export const DEFAULT_PROJECT = "gopherstack-local";
export const PROJECT_ID_RE = /^[a-z][a-z0-9-]{4,28}[a-z0-9]$/;

export function validateProjectId(id: string): string {
  if (!id) return "Project is required";
  return PROJECT_ID_RE.test(id) ? "" : "6-30 characters: lowercase letters, digits and hyphens";
}

export function readStoredProject(key: string, storage?: Pick<Storage, "getItem">): string {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    return s?.getItem(key)?.trim() || DEFAULT_PROJECT;
  } catch {
    return DEFAULT_PROJECT;
  }
}

export function writeStoredProject(
  key: string,
  value: string,
  storage?: Pick<Storage, "setItem">,
): void {
  try {
    const s = storage ?? (typeof window === "undefined" ? undefined : window.localStorage);
    s?.setItem(key, value);
  } catch {
    // storage unavailable: the choice lasts for this page only
  }
}
