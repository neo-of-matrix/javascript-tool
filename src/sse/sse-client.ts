interface SSEOptions {
  withCredentials?: boolean;
  autoReconnect?: boolean;
  maxRetries?: number;
  retryDelay?: number;
  backoffMultiplier?: number;
  maxRetryDelay?: number;
}

type EventHandler = (data: any) => void;

class AdvancedSSEClient {
  private url: string;
  protected options: Required<SSEOptions>;
  private eventSource: EventSource | null;
  protected retryCount: number;
  protected isConnected: boolean;
  private eventHandlers: Map<string, EventHandler[]>;
  private lastEventId: string | null;
  private heartbeatInterval: number | null;

  constructor(url: string, options: SSEOptions = {}) {
    this.url = url;
    this.options = {
      withCredentials: false,
      autoReconnect: true,
      maxRetries: 5,
      retryDelay: 1000,
      backoffMultiplier: 1.5,
      maxRetryDelay: 30000,
      ...options,
    };

    this.eventSource = null;
    this.retryCount = 0;
    this.isConnected = false;
    this.eventHandlers = new Map();
    this.lastEventId = null;
    this.heartbeatInterval = null;
  }

  // 建立连接
  connect(): void {
    try {
      // 清理现有连接
      if (this.eventSource) {
        this.eventSource.close();
      }

      let url = this.url;
      if (this.lastEventId) {
        const separator = url.includes("?") ? "&" : "?";
        url += `${separator}lastEventId=${encodeURIComponent(
          this.lastEventId
        )}`;
      }

      this.eventSource = new EventSource(url, {
        withCredentials: this.options.withCredentials,
      });

      this.setupEventHandlers();
      this.startHeartbeatMonitor();
    } catch (error) {
      console.error("创建SSE连接失败:", error);
      this.handleReconnection();
    }
  }

  // 设置事件处理器
  private setupEventHandlers(): void {
    if (!this.eventSource) return;

    this.eventSource.onopen = () => {
      this.isConnected = true;
      this.retryCount = 0;
      console.log(`${this.url} SSE连接已建立`);
      this.emit("connected", { timestamp: Date.now() });
    };

    this.eventSource.onmessage = (event: MessageEvent) => {
      this.lastEventId = event.lastEventId || null;
      this.emit("message", {
        data: event.data,
        id: event.lastEventId,
        timestamp: Date.now(),
      });
    };

    this.eventSource.onerror = (event: Event) => {
      this.isConnected = false;
      console.error("SSE连接错误:", event);
      this.emit("error", {
        type: "connection_error",
        event: event,
        retryCount: this.retryCount,
      });

      if (this.options.autoReconnect) {
        this.handleReconnection();
      }
    };

    // 动态添加自定义事件监听器
    for (const [eventName] of this.eventHandlers) {
      if (
        eventName !== "message" &&
        eventName !== "error" &&
        eventName !== "connected"
      ) {
        this.eventSource.addEventListener(eventName, (event: Event) => {
          const messageEvent = event as MessageEvent;
          this.lastEventId = messageEvent.lastEventId || null;
          this.emit(eventName, {
            data: messageEvent.data,
            id: messageEvent.lastEventId,
            timestamp: Date.now(),
          });
        });
      }
    }
  }

  // 心跳监控
  private startHeartbeatMonitor(): void {
    this.heartbeatInterval = window.setInterval(() => {
      if (this.isConnected) {
        this.emit("heartbeat", { timestamp: Date.now() });
      }
    }, 30000);
  }

  // 添加事件监听器
  on(eventName: string, handler: EventHandler): void {
    if (!this.eventHandlers.has(eventName)) {
      this.eventHandlers.set(eventName, []);
    }
    this.eventHandlers.get(eventName)!.push(handler);

    // 如果已经连接，为自定义事件添加原生监听器
    if (
      this.eventSource &&
      this.isConnected &&
      eventName !== "message" &&
      eventName !== "error" &&
      eventName !== "connected"
    ) {
      this.eventSource.addEventListener(eventName, (event: Event) => {
        const messageEvent = event as MessageEvent;
        this.emit(eventName, {
          data: messageEvent.data,
          id: messageEvent.lastEventId,
          timestamp: Date.now(),
        });
      });
    }
  }

  // 触发事件
  private emit(eventName: string, data: any): void {
    const handlers = this.eventHandlers.get(eventName) || [];
    handlers.forEach((handler) => {
      try {
        handler(data);
      } catch (error) {
        console.error(`事件处理器错误 (${eventName}):`, error);
      }
    });
  }

  // 处理重连逻辑
  protected handleReconnection(): void {
    if (this.retryCount >= this.options.maxRetries) {
      console.error("达到最大重连次数，停止重连");
      this.emit("error", {
        type: "max_retries_exceeded",
        retryCount: this.retryCount,
      });
      return;
    }

    this.retryCount++;
    const baseDelay = this.options.retryDelay;
    const backoffDelay =
      baseDelay * Math.pow(this.options.backoffMultiplier, this.retryCount - 1);
    const maxDelay = this.options.maxRetryDelay;
    const actualDelay = Math.min(backoffDelay, maxDelay);

    console.log(`${actualDelay}ms后尝试第${this.retryCount}次重连...`);

    setTimeout(() => {
      this.connect();
    }, actualDelay);
  }

  // 关闭连接
  close(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }

    if (this.eventSource) {
      this.eventSource.close();
      this.isConnected = false;
      this.emit("disconnected", { timestamp: Date.now() });
    }
  }

  // 资源清理
  destroy(): void {
    this.close();
    this.eventHandlers.clear();
  }
}

class MobileSSEClient extends AdvancedSSEClient {
  private backgroundMode: boolean;

  constructor(url: string, options: SSEOptions = {}) {
    super(url, {
      autoReconnect: true,
      maxRetries: Infinity, // 移动网络需要无限重试
      retryDelay: 1000,
      backoffMultiplier: 1.5,
      maxRetryDelay: 60000, // 最大重试延迟60秒
      ...options,
    });

    this.backgroundMode = false;
    this.setupNetworkListeners();
    this.setupVisibilityHandler();
  }

  private setupNetworkListeners(): void {
    // 监听网络状态变化
    if (typeof navigator !== "undefined" && "connection" in navigator) {
      (navigator as any).connection.addEventListener(
        "change",
        this.handleNetworkChange.bind(this)
      );
    }

    window.addEventListener("online", () => {
      console.log("网络恢复，尝试重新连接");
      if (!this.isConnected) {
        this.connect();
      }
    });

    window.addEventListener("offline", () => {
      console.log("网络断开，关闭连接");
      this.close(); // 网络断开时主动关闭以节省资源
    });
  }

  private setupVisibilityHandler(): void {
    // 页面可见性变化处理
    document.addEventListener("visibilitychange", () => {
      if (document.hidden) {
        // 页面隐藏时，延长心跳间隔或暂停连接
        this.onBackground();
      } else {
        // 页面可见时恢复
        this.onForeground();
      }
    });
  }

  private onBackground(): void {
    this.backgroundMode = true;
    // 可以在这里减少心跳频率或暂停非关键连接
    console.log("应用进入后台，优化SSE连接");
  }

  private onForeground(): void {
    this.backgroundMode = false;
    if (!this.isConnected) {
      this.connect();
    }
    console.log("应用回到前台，恢复SSE连接");
  }

  private handleNetworkChange(): void {
    if ("connection" in navigator) {
      const connection = (navigator as any).connection;
      console.log("网络连接变化:", {
        effectiveType: connection.effectiveType,
        downlink: connection.downlink,
        rtt: connection.rtt,
      });

      // 根据网络质量调整重连策略
      if (
        connection.effectiveType === "2g" ||
        connection.effectiveType === "slow-2g"
      ) {
        this.options.retryDelay = 3000; // 慢网络增加重连延迟
      } else {
        this.options.retryDelay = 1000;
      }
    }
  }

  protected handleReconnection(): void {
    // 移动端使用更保守的重连策略
    const baseDelay = this.options.retryDelay;
    const backoffDelay =
      baseDelay * Math.pow(this.options.backoffMultiplier, this.retryCount - 1);
    const actualDelay = Math.min(backoffDelay, this.options.maxRetryDelay);

    console.log(`${actualDelay}ms后尝试第${this.retryCount}次重连...`);

    setTimeout(() => {
      // 重连前检查网络状态
      if (navigator.onLine) {
        this.connect();
      } else {
        console.log("网络未连接，等待网络恢复");
        this.handleReconnection(); // 继续等待
      }
    }, actualDelay);
  }
}

// 浏览器连接池管理
class SSEConnectionPool {
  private maxConnections: number;
  private connections: Map<string, AdvancedSSEClient>;
  private pendingRequests: Array<() => void>;
  private connectionCount: number;

  constructor(maxConnections: number = 6) {
    // 大多数浏览器限制为6个
    this.maxConnections = maxConnections;
    this.connections = new Map();
    this.pendingRequests = [];
    this.connectionCount = 0;
  }

  async getConnection(
    url: string,
    options: SSEOptions = {}
  ): Promise<AdvancedSSEClient> {
    // 等待可用连接槽位
    while (this.connectionCount >= this.maxConnections) {
      await this.waitForSlot();
    }

    const connection = new AdvancedSSEClient(url, options);
    const connectionId = this.generateConnectionId();

    this.connections.set(connectionId, connection);
    this.connectionCount++;

    // 连接关闭时释放槽位
    connection.on("disconnected", () => {
      this.connections.delete(connectionId);
      this.connectionCount--;
      this.processPendingRequests();
    });

    connection.on("error", () => {
      this.connections.delete(connectionId);
      this.connectionCount--;
      this.processPendingRequests();
    });

    return connection;
  }

  private waitForSlot(): Promise<void> {
    return new Promise((resolve) => {
      this.pendingRequests.push(resolve);
    });
  }

  private processPendingRequests(): void {
    if (
      this.pendingRequests.length > 0 &&
      this.connectionCount < this.maxConnections
    ) {
      const resolve = this.pendingRequests.shift();
      if (resolve) resolve();
    }
  }

  private generateConnectionId(): string {
    return `conn_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  // 关闭所有连接
  closeAll(): void {
    this.connections.forEach((connection) => {
      connection.close();
    });
    this.connections.clear();
    this.connectionCount = 0;
    this.pendingRequests = [];
  }
}

export { AdvancedSSEClient, MobileSSEClient, SSEConnectionPool };
