import adapter from "@sveltejs/adapter-static";

/** @type {import('@sveltejs/kit').Config} */
const config = {
  compilerOptions: {
    // Force runes mode for the project, except for libraries. Can be removed in svelte 6.
    runes: ({ filename }) => (filename.split(/[/\\]/).includes("node_modules") ? undefined : true),
  },
  kit: {
    adapter: adapter({
      fallback: "index.html",
      pages: "../dashboard/static/dashboard2",
      assets: "../dashboard/static/dashboard2",
    }),
    paths: {
      base: "/dashboard2",
    },
  },
};

export default config;
