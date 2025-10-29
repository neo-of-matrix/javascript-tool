import { AdvancedSSEClient, SSEConnectionPool } from "./sse-client";

class Dashboard {
  private connectionPool: SSEConnectionPool;
  public eventsConnection?: AdvancedSSEClient;
  public stockConnection?: AdvancedSSEClient;
  public newsConnection?: AdvancedSSEClient;

  constructor() {
    this.connectionPool = new SSEConnectionPool();
    this.initialize();
  }

  async initialize(): Promise<void> {
    // 使用连接池管理多个 SSE 连接
    this.eventsConnection = await this.connectionPool.getConnection(
      "/api/events",
      {
        withCredentials: true,
        autoReconnect: true,
        maxRetries: 10,
        retryDelay: 2000,
        backoffMultiplier: 1.5,
      }
    );
    this.stockConnection = await this.connectionPool.getConnection(
      "/api/stocks",
      { withCredentials: true }
    );
    this.newsConnection = await this.connectionPool.getConnection("/api/news", {
      withCredentials: true,
    });

    this.setupEventHandlers();
  }

  setupEventHandlers(): void {
    this.eventsConnection?.on("message", (event) => {
      const stockData = JSON.parse(event.data);
      console.log("events connection:", stockData);
      // updateUI(stock);
    });
    this.stockConnection?.on("customUpdate", (event) => {
      const stockData = JSON.parse(event.data);
      console.log("stock connection:", stockData);
      // updateUI(stock);
    });
    this.newsConnection?.on("customUpdate", (event) => {
      const stockData = JSON.parse(event.data);
      console.log("news connection:", stockData);
      // updateUI(stock);
    });

    // 错误处理
    this.eventsConnection?.on("error", (error) => {
      console.error("数据连接异常:", error);
    });
    this.stockConnection?.on("error", (error) => {
      console.error("数据连接异常:", error);
    });
    this.newsConnection?.on("error", (error) => {
      console.error("数据连接异常:", error);
    });
  }

  // 清理资源
  destroy(): void {
    if (this.eventsConnection) {
      this.eventsConnection.destroy();
    }
    if (this.stockConnection) {
      this.stockConnection.destroy();
    }
    if (this.newsConnection) {
      this.newsConnection.destroy();
    }
    this.connectionPool.closeAll();
  }
}

export { Dashboard };
