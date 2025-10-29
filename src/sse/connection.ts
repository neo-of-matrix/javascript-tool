import { Dashboard } from "./dashboard";

const startReceive = (): void => {
  fetch("/api/broadcast", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      event: "message",
      data: {
        symbol: "message",
      },
    }),
  });
};

const customUpdate = (): void => {
  fetch("/api/broadcast", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      event: "customUpdate",
      data: {
        symbol: "customUpdate",
      },
    }),
  });
};

const getStats = async (): Promise<void> => {
  const data = await fetch("/api/stats");
  const json = await data.json();
  console.log(json);
};

const closeMessage = (stockDashboard: Dashboard): void => {
  stockDashboard?.eventsConnection?.close();
};

const connection = async (
  element: HTMLElement,
  stockUpdateElement: HTMLElement,
  getTotalElement: HTMLElement,
  closeMessageElement: HTMLElement,
  closeAllElement: HTMLElement
): Promise<void> => {
  const stockDashboard = await new Dashboard();
  await stockDashboard?.eventsConnection?.connect();
  await stockDashboard?.stockConnection?.connect();
  await stockDashboard?.newsConnection?.connect();

  element.addEventListener("click", () => {
    startReceive();
  });
  stockUpdateElement.addEventListener("click", () => {
    customUpdate();
  });
  getTotalElement.addEventListener("click", () => {
    getStats();
  });
  closeMessageElement.addEventListener("click", () => {
    closeMessage(stockDashboard);
  });
  closeAllElement.addEventListener("click", () => {
    stockDashboard.destroy();
  });
};

export { connection };
