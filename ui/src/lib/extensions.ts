// Data logic for the Extensions page (/_gopherstack/extensions API).

export type ExtensionStatus =
  | "starting"
  | "ready"
  | "unhealthy"
  | "restarting"
  | "failed"
  | "stopped";

export type ExtensionRoutes = {
  path_prefixes?: string[];
  hosts?: string[];
  target_prefixes?: string[];
};

export type ExtensionRequestHook = {
  services: string[];
  pre?: string;
  post?: string;
};

export type ExtensionHooks = {
  on_startup?: string;
  on_ready?: string;
  on_shutdown?: string;
  request?: ExtensionRequestHook[];
};

export type ExtensionInfo = {
  name: string;
  type: "process" | "container";
  status: ExtensionStatus;
  restarts: number;
  last_error?: string;
  endpoint?: string;
  registration?: {
    name: string;
    version?: string;
    routes: ExtensionRoutes;
    hooks: ExtensionHooks;
  };
};

export const settledStatuses: ExtensionStatus[] = ["ready", "failed", "stopped"];

export function claimLabels(routes?: ExtensionRoutes): string[] {
  if (!routes) return [];
  return [
    ...(routes.path_prefixes ?? []).map((p) => `path ${p}`),
    ...(routes.hosts ?? []).map((h) => `host ${h}`),
    ...(routes.target_prefixes ?? []).map((t) => `target ${t}`),
  ];
}

export function hookLabels(hooks?: ExtensionHooks): string[] {
  if (!hooks) return [];
  const out: string[] = [];
  if (hooks.on_startup) out.push("on_startup");
  if (hooks.on_ready) out.push("on_ready");
  if (hooks.on_shutdown) out.push("on_shutdown");
  for (const rh of hooks.request ?? []) {
    const phases = [rh.pre ? "pre" : "", rh.post ? "post" : ""].filter(Boolean).join("+");
    out.push(`${phases} ${rh.services.join(",")}`);
  }
  return out;
}

export function statusStyle(status: ExtensionStatus): string {
  switch (status) {
    case "ready":
      return "bg-emerald-100 text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300";
    case "failed":
      return "bg-red-100 text-red-800 dark:bg-red-900/30 dark:text-red-300";
    case "unhealthy":
    case "restarting":
      return "bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300";
    default:
      return "bg-slate-100 text-slate-700 dark:bg-slate-700 dark:text-slate-300";
  }
}

export function needsFastPoll(exts: ExtensionInfo[]): boolean {
  return exts.some((e) => !settledStatuses.includes(e.status));
}
