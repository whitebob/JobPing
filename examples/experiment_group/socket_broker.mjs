import http from "node:http";
import { Server } from "socket.io";

const PORT = Number(process.env.SOCKET_BROKER_PORT ?? 8890);

const server = http.createServer();
const io = new Server(server, {
  cors: {
    origin: "*",
  },
});

io.on("connection", (socket) => {
  console.log("broker: client connected", socket.id);

  socket.on("jobping:envelope", (envelope) => {
    // broadcast to all other clients
    socket.broadcast.emit("jobping:envelope", envelope);
  });

  socket.on("jobping:message", (message) => {
    socket.broadcast.emit("jobping:message", message);
  });

  socket.on("disconnect", () => {
    console.log("broker: client disconnected", socket.id);
  });
});

server.listen(PORT, () => {
  console.log(`broker: listening on http://127.0.0.1:${PORT}`);
});
