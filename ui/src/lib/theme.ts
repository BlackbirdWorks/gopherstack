export type ThemeName =
  | "light"
  | "dark"
  | "github"
  | "github-light"
  | "ocean"
  | "cyberpunk-2077"
  | "aurora"
  | "solstice";

export const themeStorageKey = "gopherstack-theme";

export const themes: readonly ThemeName[] = [
  "light",
  "dark",
  "github",
  "github-light",
  "ocean",
  "cyberpunk-2077",
  "aurora",
  "solstice",
] as const;

export function isValidTheme(value: string | null): value is ThemeName {
  return (
    value === "light" ||
    value === "dark" ||
    value === "github" ||
    value === "github-light" ||
    value === "ocean" ||
    value === "cyberpunk-2077" ||
    value === "aurora" ||
    value === "solstice"
  );
}

export function resolveTheme(savedTheme: string | null, prefersDark: boolean): ThemeName {
  if (isValidTheme(savedTheme)) {
    return savedTheme;
  }

  return prefersDark ? "dark" : "light";
}

export function isDarkTheme(theme: ThemeName): boolean {
  return (
    theme === "dark" ||
    theme === "github" ||
    theme === "ocean" ||
    theme === "cyberpunk-2077" ||
    theme === "aurora"
  );
}

export function applyTheme(doc: Document, theme: ThemeName): void {
  doc.documentElement.classList.toggle("dark", isDarkTheme(theme));

  for (const t of themes) {
    doc.documentElement.classList.toggle(`theme-${t}`, t === theme);
  }
}

export function initializeTheme(doc: Document, storage: Storage, prefersDark: boolean): ThemeName {
  const theme = resolveTheme(storage.getItem(themeStorageKey), prefersDark);
  applyTheme(doc, theme);

  return theme;
}

export function setTheme(doc: Document, storage: Storage, theme: ThemeName): ThemeName {
  storage.setItem(themeStorageKey, theme);
  applyTheme(doc, theme);

  return theme;
}
