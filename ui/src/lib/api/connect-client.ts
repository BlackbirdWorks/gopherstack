import { createPromiseClient } from '@connectrpc/connect';
import { createConnectTransport } from '@connectrpc/connect-web';
import { DashboardService } from './gopherstack/dashboard/v1/dashboard_connect';

// When running in dev server, default to localhost:8000. In production (embedded), default to current origin.
const isBrowser = typeof window !== 'undefined';
const defaultEndpoint = isBrowser 
  ? (window.location.port === '5173' || window.location.port !== '' && window.location.port !== '8000')
     ? 'http://localhost:8000' 
     : window.location.origin
  : 'http://localhost:8000';

const transport = createConnectTransport({
  baseUrl: defaultEndpoint,
});

export const dashboardClient = createPromiseClient(DashboardService, transport);
