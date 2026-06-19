import express from "express";
import path from "path";
import fs from "fs";
import { createServer } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { createServer as createViteServer } from "vite";

interface Room {
  pin: string;
  hostWs?: WebSocket;
  peerWs?: WebSocket;
  fileInfo?: {
    name: string;
    size: number;
    type: string;
    isFolder?: boolean;
    fileCount?: number;
  };
}

const activeRooms = new Map<string, Room>();

function generatePin(): string {
  let pin = "";
  do {
    pin = Math.floor(100000 + Math.random() * 900000).toString();
  } while (activeRooms.has(pin));
  return pin;
}

async function startServer() {
  const app = express();
  const server = createServer(app);
  const wss = new WebSocketServer({ noServer: true });

  const PORT = 3000;

  // Handle upgrade to WebSockets on the same port 3000
  server.on("upgrade", (request, socket, head) => {
    const pathname = new URL(request.url || "", `http://${request.headers.host}`).pathname;
    
    // Skip if it's the Vite HMR websocket which is handled by Vite middleware
    if (pathname === "/@vite/client" || request.headers["sec-websocket-protocol"] === "vite-hmr") {
      return;
    }
    
    wss.handleUpgrade(request, socket, head, (ws) => {
      wss.emit("connection", ws, request);
    });
  });

  const wsToRoom = new Map<WebSocket, { pin: string; role: "host" | "peer" }>();

  wss.on("connection", (ws) => {
    const extWs = ws as any;
    extWs.isAlive = true;
    
    ws.on("pong", () => {
      extWs.isAlive = true;
    });

    ws.on("message", (messageStr) => {
      try {
        const payload = JSON.parse(messageStr.toString());
        const { type } = payload;

        if (type === "create-room") {
          const pin = generatePin();
          const room: Room = {
            pin,
            hostWs: ws,
          };
          activeRooms.set(pin, room);
          wsToRoom.set(ws, { pin, role: "host" });
          
          ws.send(JSON.stringify({ type: "room-created", pin }));
          console.log(`[Signaling] Room ${pin} created by host`);
        } 
        
        else if (type === "join-room") {
          const pin = String(payload.pin).trim();
          const room = activeRooms.get(pin);
          if (!room) {
            ws.send(JSON.stringify({ type: "error", message: `PIN ${pin} is incorrect, expired, or non-existent.` }));
            return;
          }
          if (room.peerWs) {
            ws.send(JSON.stringify({ type: "error", message: `The room for PIN ${pin} is already full.` }));
            return;
          }

          room.peerWs = ws;
          wsToRoom.set(ws, { pin, role: "peer" });

          // Inform receiver they've successfully joined
          ws.send(JSON.stringify({ 
            type: "joined", 
            pin, 
            fileInfo: room.fileInfo 
          }));

          // Inform host the receiver joined
          if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
            room.hostWs.send(JSON.stringify({ type: "peer-joined" }));
          }
          console.log(`[Signaling] Peer joined room ${pin}`);
        } 
        
        else if (type === "set-file-info") {
          const meta = wsToRoom.get(ws);
          if (meta && meta.role === "host") {
            const room = activeRooms.get(meta.pin);
            if (room) {
              room.fileInfo = {
                name: payload.name,
                size: payload.size,
                type: payload.fileType,
                isFolder: payload.isFolder ?? false,
                fileCount: payload.fileCount
              };
              // Relay file info to peer if already connected
              if (room.peerWs && room.peerWs.readyState === WebSocket.OPEN) {
                room.peerWs.send(JSON.stringify({ type: "file-info", fileInfo: room.fileInfo }));
              }
            }
          }
        } 
        
        else if (type === "signal") {
          const meta = wsToRoom.get(ws);
          if (meta) {
            const room = activeRooms.get(meta.pin);
            if (room) {
              const targetWs = meta.role === "host" ? room.peerWs : room.hostWs;
              if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(JSON.stringify({ type: "signal", data: payload.data }));
              }
            }
          }
        } 
        
        else if (type === "cancel-transfer") {
          const meta = wsToRoom.get(ws);
          if (meta) {
            const room = activeRooms.get(meta.pin);
            if (room) {
              const targetWs = meta.role === "host" ? room.peerWs : room.hostWs;
              if (targetWs && targetWs.readyState === WebSocket.OPEN) {
                targetWs.send(JSON.stringify({ type: "transfer-cancelled", reason: payload.reason || "User cancelled" }));
              }
            }
          }
        }
      } catch (err) {
        console.error("Failed to parse websocket message", err);
      }
    });

    ws.on("close", () => {
      const meta = wsToRoom.get(ws);
      if (meta) {
        const { pin, role } = meta;
        const room = activeRooms.get(pin);
        if (room) {
          if (role === "host") {
            // Notify peer that host left
            if (room.peerWs && room.peerWs.readyState === WebSocket.OPEN) {
              room.peerWs.send(JSON.stringify({ type: "host-disconnected", pin }));
            }
            activeRooms.delete(pin);
            console.log(`[Signaling] Host left, room ${pin} deleted`);
          } else {
            // Notify host that peer disconnected
            if (room.hostWs && room.hostWs.readyState === WebSocket.OPEN) {
              room.hostWs.send(JSON.stringify({ type: "peer-disconnected" }));
            }
            room.peerWs = undefined;
            console.log(`[Signaling] Peer left room ${pin}`);
          }
        }
        wsToRoom.delete(ws);
      }
    });
  });

  // Keep alive ping checks
  const interval = setInterval(() => {
    wss.clients.forEach((ws) => {
      const extWs = ws as any;
      if (extWs.isAlive === false) return ws.terminate();
      extWs.isAlive = false;
      ws.ping();
    });
  }, 30000);

  wss.on("close", () => {
    clearInterval(interval);
  });

  // High-speed stream upload/download directories setup
  const UPLOAD_DIR = path.join("/tmp", "p2p_uploads");
  if (!fs.existsSync(UPLOAD_DIR)) {
    fs.mkdirSync(UPLOAD_DIR, { recursive: true });
  }

  // Upload endpoint: stream raw binary directly to memory-backed /tmp cache
  app.post(
    "/api/upload/:fileId/:fileName",
    express.raw({ limit: "500mb", type: "*/*" }),
    (req, res) => {
      const { fileId, fileName } = req.params;
      const logPath = path.join(process.cwd(), "upload_debug.log");
      
      const writeLog = (msg: string) => {
        const logLine = `${new Date().toISOString()} - [UPLOAD - ${fileId}] ${msg}\n`;
        try {
          fs.appendFileSync(logPath, logLine);
        } catch (err) {}
        console.log(logLine.trim());
      };

      writeLog(`Received upload request for ${fileName}. Content-Length: ${req.headers["content-length"]}`);

      if (!fileId || !fileName) {
        writeLog("Error: Missing fileId or fileName");
        return res.status(400).json({ error: "Missing fileId or fileName" });
      }

      const cleanFileId = fileId.replace(/[^a-zA-Z0-9_\-]/g, "");
      const fileDir = path.join(UPLOAD_DIR, cleanFileId);
      
      try {
        if (!fs.existsSync(fileDir)) {
          fs.mkdirSync(fileDir, { recursive: true });
        }
      } catch (e: any) {
        writeLog(`Error creating directory: ${e.message}`);
        return res.status(500).json({ error: "Failed to create destination directory" });
      }

      const filePath = path.join(fileDir, fileName);
      writeLog(`Writing payload buffer to filePath: ${filePath}`);

      try {
        const payload = req.body;
        if (!payload || !(payload instanceof Buffer)) {
          writeLog("Error: Request body is empty or not parsed as a Buffer");
          return res.status(400).json({ error: "Invalid or empty binary payload body" });
        }

        writeLog(`Payload size parsed successfully: ${payload.length} bytes. Writing to disk...`);
        fs.writeFileSync(filePath, payload);
        writeLog("fs.writeFileSync completed successfully. Generating download URL.");

        const downloadUrl = `/api/download/${cleanFileId}/${encodeURIComponent(fileName)}`;
        res.json({ success: true, downloadUrl });
      } catch (err: any) {
        writeLog(`Error writing file: ${err.message}`);
        res.status(500).json({ error: "File system write error" });
      }
    }
  );

  // Download endpoint: server-to-client streaming
  app.get("/api/download/:fileId/:fileName", (req, res) => {
    const { fileId, fileName } = req.params;
    const cleanFileId = fileId.replace(/[^a-zA-Z0-9_\-]/g, "");
    const filePath = path.join(UPLOAD_DIR, cleanFileId, fileName);

    if (fs.existsSync(filePath)) {
      res.download(filePath, fileName, (err) => {
        if (err) {
          console.error("[Server Download Error] download transmission error:", err);
        }
      });
    } else {
      res.status(404).send("File not found or expired on host server.");
    }
  });

  // Health check endpoint
  app.get("/api/health", (req, res) => {
    res.json({ status: "healthy", activeRoomsCount: activeRooms.size });
  });

  // Vite development middleware OR static built asset routing
  if (process.env.NODE_ENV !== "production") {
    const vite = await createViteServer({
      server: { middlewareMode: true },
      appType: "spa",
    });
    app.use(vite.middlewares);
  } else {
    const distPath = path.join(process.cwd(), "dist");
    app.use(express.static(distPath));
    app.get("*", (req, res) => {
      res.sendFile(path.join(distPath, "index.html"));
    });
  }

  server.listen(PORT, "0.0.0.0", () => {
    console.log(`[Server] Running on http://0.0.0.0:${PORT} in ${process.env.NODE_ENV || "development"} mode`);
  });
}

startServer().catch((err) => {
  console.error("Fatal error starting the server:", err);
});
