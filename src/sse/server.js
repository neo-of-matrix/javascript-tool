import express from "express";

const app = express();

// 连接管理器
class ConnectionManager {
  constructor() {
    this.clients = new Map();
    this.heartbeatInterval = null;
    this.startHeartbeat();
  }

  // 添加客户端
  addClient(req, res) {
    const clientId = this.generateClientId();

    // 设置SSE响应头
    res.writeHead(200, {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-cache, no-transform",
      Connection: "keep-alive",
      "Access-Control-Allow-Origin": "*",
      "Access-Control-Allow-Headers": "Cache-Control",
      "X-Accel-Buffering": "no", // 禁用Nginx缓冲
    });

    const client = {
      id: clientId,
      response: res,
      ip: req.ip,
      connectedAt: new Date(),
      lastActivity: new Date(),
    };

    this.clients.set(clientId, client);

    // 处理Last-Event-ID头部
    const lastEventId = req.headers["last-event-id"];
    if (lastEventId) {
      this.sendToClient(client, "catchup", {
        message: "恢复连接",
        lastEventId: lastEventId,
        timestamp: new Date().toISOString(),
      });
    }

    // 发送连接确认
    this.sendToClient(client, "connected", {
      clientId: clientId,
      message: "连接成功",
      timestamp: new Date().toISOString(),
    });

    console.log(`客户端 ${clientId} 已连接，总连接数: ${this.clients.size}`);

    // 客户端断开连接处理
    req.on("close", () => {
      this.removeClient(clientId);
    });

    req.on("error", (error) => {
      console.error(`客户端 ${clientId} 连接错误:`, error);
      this.removeClient(clientId);
    });

    return clientId;
  }

  // 发送消息到客户端
  sendToClient(client, event, data, id = null) {
    try {
      if (!client.response.writable) {
        this.removeClient(client.id);
        return false;
      }

      const message = [];

      if (event && event !== "message") {
        message.push(`event: ${event}`);
      }

      // 数据验证和序列化
      const validatedData = this.validateMessageData(data);
      message.push(`data: ${validatedData}`);

      const messageId = id || Date.now().toString();
      message.push(`id: ${messageId}`);

      // 添加时间戳用于调试
      message.push(`: timestamp: ${new Date().toISOString()}`);

      message.push("", ""); // 空行表示消息结束

      client.response.write(message.join("\n"));
      client.lastActivity = new Date();

      // 尝试刷新缓冲区
      if (typeof client.response.flush === "function") {
        client.response.flush();
      }

      return true;
    } catch (error) {
      console.error(`向客户端 ${client.id} 发送消息失败:`, error);
      this.removeClient(client.id);
      return false;
    }
  }

  // 数据验证
  validateMessageData(data) {
    if (typeof data === "object") {
      const jsonString = JSON.stringify(data);
      // 防止过大的消息
      if (jsonString.length > 10000) {
        thrownewError("Message too large");
      }
      return jsonString;
    } else if (typeof data === "string") {
      // 防止注入攻击
      if (data.includes("\0") || data.length > 10000) {
        thrownewError("Invalid message content");
      }
      return data;
    } else {
      returnString(data);
    }
  }

  // 广播消息
  broadcast(event, data, excludeClientId = null) {
    const messageId = Date.now().toString();
    let successCount = 0;
    let failCount = 0;

    this.clients.forEach((client, clientId) => {
      if (clientId !== excludeClientId) {
        const success = this.sendToClient(client, event, data, messageId);
        if (success) {
          successCount++;
        } else {
          failCount++;
        }
      }
    });

    console.log(`广播消息 ${event}: 成功 ${successCount}, 失败 ${failCount}`);
    return { successCount, failCount };
  }

  // 发送心跳
  startHeartbeat() {
    this.heartbeatInterval = setInterval(() => {
      this.clients.forEach((client) => {
        try {
          client.response.write(": heartbeat\n\n");
        } catch (error) {
          this.removeClient(client.id);
        }
      });
    }, 25000); // 25秒心跳
  }

  // 移除客户端
  removeClient(clientId) {
    const client = this.clients.get(clientId);
    if (client) {
      try {
        if (client.response.writable) {
          client.response.end();
        }
      } catch (error) {
        // 忽略关闭错误
      }
      this.clients.delete(clientId);
      console.log(`客户端 ${clientId} 已断开，剩余连接: ${this.clients.size}`);
    }
  }

  // 生成客户端ID
  generateClientId() {
    return `client_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  // 获取统计信息
  getStats() {
    return {
      totalClients: this.clients.size,
      clients: Array.from(this.clients.values()).map((client) => ({
        id: client.id,
        ip: client.ip,
        connectedAt: client.connectedAt,
        lastActivity: client.lastActivity,
      })),
    };
  }

  // 清理资源
  destroy() {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
    }
    this.clients.forEach((client) => {
      try {
        client.response.end();
      } catch (error) {
        // 忽略错误
      }
    });
    this.clients.clear();
  }
}

// 初始化连接管理器
const connectionManager = new ConnectionManager();

// SSE端点
app.get("/api/events", (req, res) => {
  connectionManager.addClient(req, res);
});
app.get("/api/stocks", (req, res) => {
  connectionManager.addClient(req, res);
});
app.get("/api/news", (req, res) => {
  connectionManager.addClient(req, res);
});

// 广播消息API
app.post("/api/broadcast", express.json(), (req, res) => {
  const { event, data, excludeClientId } = req.body;

  if (!event || !data) {
    return res.status(400).json({ error: "Missing event or data" });
  }

  try {
    const result = connectionManager.broadcast(event, data, excludeClientId);
    res.json({
      success: true,
      ...result,
      totalClients: connectionManager.getStats().totalClients,
    });
  } catch (error) {
    console.error("广播消息失败:", error);
    res.status(500).json({ error: "Broadcast failed" });
  }
});

// 获取连接统计
app.get("/api/stats", (req, res) => {
  res.json(connectionManager.getStats());
});

// 优雅关闭
process.on("SIGTERM", () => {
  console.log("收到SIGTERM信号，优雅关闭服务器...");
  connectionManager.destroy();
  process.exit(0);
});

app.listen(3000, () => {
  console.log("SSE服务器运行在端口 3000");
});
