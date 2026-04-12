export type ThemeMode = "light" | "dark";

export const themeStorageKey = "gopherstack-dashboard2-theme";

export function resolveTheme(savedTheme: string | null, prefersDark: boolean): ThemeMode {
  if (savedTheme === "dark" || savedTheme === "light") {
    return savedTheme;
  }

  return prefersDark ? "dark" : "light";
}

export function applyTheme(doc: Document, theme: ThemeMode): void {
  doc.documentElement.classList.toggle("dark", theme === "dark");
}

export function initializeTheme(doc: Document, storage: Storage, prefersDark: boolean): ThemeMode {
  const theme = resolveTheme(storage.getItem(themeStorageKey), prefersDark);
  applyTheme(doc, theme);

  return theme;
}

export function toggleTheme(current: ThemeMode): ThemeMode {
  return current === "dark" ? "light" : "dark";
}

export function toggleStoredTheme(doc: Document, storage: Storage, current: ThemeMode): ThemeMode {
  const next = toggleTheme(current);
  storage.setItem(themeStorageKey, next);
  applyTheme(doc, next);

  return next;
}
