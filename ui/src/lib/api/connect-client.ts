import { createClient } from "@connectrpc/connect";
import { createConnectTransport } from "@connectrpc/connect-web";
import { DashboardService } from "./gopherstack/dashboard/v1/dashboard_connect";

const transport = createConnectTransport({
  baseUrl: typeof window === "undefined" ? "http://localhost:8000" : "",
});

export const dashboardClient = createClient(DashboardService, transport);
