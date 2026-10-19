import { toast } from "svelte-sonner";

import { describeGcpError } from "./gcprest.ts";

export const GCP_INPUT =
  "w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500";
export const GCP_MINI =
  "rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50";
export const GCP_GHOST =
  "rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800";
export const GCP_PRIMARY =
  "rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50";

/** Runs an action with success/failure toasts; resolves to whether it succeeded. */
export async function attempt(
  action: () => Promise<unknown>,
  success: string,
  failure: string,
): Promise<boolean> {
  try {
    await action();
    if (success) toast.success(success);
    return true;
  } catch (e) {
    toast.error(`${failure}: ${describeGcpError(e)}`);
    return false;
  }
}

export async function copyText(text: string): Promise<void> {
  try {
    await navigator.clipboard.writeText(text);
    toast.success("Copied to clipboard");
  } catch {
    toast.error("Clipboard is unavailable");
  }
}

/** Saves text as a file through a temporary object URL. */
export function downloadText(filename: string, text: string, mime = "application/json"): void {
  const url = URL.createObjectURL(new Blob([text], { type: mime }));
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
