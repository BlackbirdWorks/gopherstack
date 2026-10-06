import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";
import adapter from "@sveltejs/adapter-static";
import { sveltekit } from "@sveltejs/kit/vite";

const spaDir = fileURLToPath(new URL("../dashboard/static/spa", import.meta.url));

export default defineConfig({
  plugins: [
    tailwindcss(),
    sveltekit({
      compilerOptions: {
        runes: ({ filename }) =>
          filename.split(/[/\\]/).includes("node_modules") ? undefined : true,
      },
      adapter: adapter({ fallback: "index.html", pages: spaDir, assets: spaDir }),
      paths: { base: "/dashboard" },
    }),
  ],
  build: {
    rolldownOptions: {
      output: {
        // Keep AWS SDK/Smithy in one vendor chunk; the default heuristic hoisted
        // SDK helpers into a route node, giving a circular import that blanked pages.
        codeSplitting: {
          groups: [{ name: "aws-sdk", test: /node_modules[\\/](@aws-sdk|@smithy)[\\/]/ }],
        },
      },
    },
  },
  server: {
    proxy: {
      "/_api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
      "/": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
  resolve: {
    conditions: ["browser"],
  },
  test: {
    globals: true,
    expect: { requireAssertions: true },
    environment: "jsdom",
    setupFiles: ["./vitest.setup.ts"],
    include: ["src/lib/**/*.{test,spec}.{js,ts}", "src/routes/**/*.test.{js,ts}"],
    exclude: ["src/lib/vitest-examples/**"],
    coverage: {
      reporter: ["text", "html"],
      provider: "v8",
      include: ["src/lib/**/*.ts"],
      exclude: [
        "src/lib/vitest-examples/**",
        "src/**/*.d.ts",
        "src/lib/index.ts",
        "src/lib/api/**",
        "src/lib/aws-client.ts",
        "**/confirm-dialog.ts",
      ],
      thresholds: {
        branches: 90,
        functions: 90,
        lines: 90,
        statements: 90,
      },
    },
  },
});
