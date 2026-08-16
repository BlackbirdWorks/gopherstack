import { fileURLToPath } from "node:url";
import adapter from "@sveltejs/adapter-static";

const spaDir = fileURLToPath(new URL("../dashboard/static/spa", import.meta.url));

/** @type {import('@sveltejs/kit').Config} */
const config = {
  compilerOptions: {
    // Force runes mode for the project, except for libraries. Can be removed in svelte 6.
    runes: ({ filename }) => (filename.split(/[/\\]/).includes("node_modules") ? undefined : true),
  },
  kit: {
    adapter: adapter({
      fallback: "index.html",
      pages: spaDir,
      assets: spaDir,
    }),
    paths: {
      base: "/dashboard",
    },
  },
};

export default config;
