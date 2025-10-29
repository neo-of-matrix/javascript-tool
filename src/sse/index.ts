import { connection } from "./connection.ts";
import "./style.css";

const createSSE = (): void => {
  const app = document.querySelector("#app");
  if (!app) {
    console.error("未找到 #app 元素");
    return;
  }

  const element = document.createElement("div");
  element.innerHTML = `
  <div class="container">
    <div>
      <span>message </span>
      <button id="start-receive">开始接收数据 message 事件</button>
    </div>
    <div>
      <span>自定义事件 </span>
      <button id="updateData">自定义事件更新数据 customUpdate</button>
    </div>
    <div>
      <span>统计信息 </span>
      <button id="get-total">获取统计信息</button>
    </div>
    <div>
      <span>断开连接 </span>
      <button id="close-message">断开 message 连接</button>
      <button id="close-all">断开所有连接</button>
    </div>
  </div>
`;
  app.appendChild(element);

  const startReceiveBtn = document.querySelector(
    "#start-receive"
  ) as HTMLButtonElement;
  const updateDataBtn = document.querySelector(
    "#updateData"
  ) as HTMLButtonElement;
  const getTotalBtn = document.querySelector("#get-total") as HTMLButtonElement;
  const closeMessageBtn = document.querySelector(
    "#close-message"
  ) as HTMLButtonElement;
  const closeAllBtn = document.querySelector("#close-all") as HTMLButtonElement;

  if (
    !startReceiveBtn ||
    !updateDataBtn ||
    !getTotalBtn ||
    !closeMessageBtn ||
    !closeAllBtn
  ) {
    console.error("未找到所有按钮元素");
    return;
  }

  connection(
    startReceiveBtn,
    updateDataBtn,
    getTotalBtn,
    closeMessageBtn,
    closeAllBtn
  );
};

export { createSSE };
