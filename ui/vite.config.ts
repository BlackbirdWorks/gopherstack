import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vitest/config";
import { sveltekit } from "@sveltejs/kit/vite";

export default defineConfig({
  base: "/dashboard/",
  plugins: [tailwindcss(), sveltekit()],
  build: {
    rollupOptions: {
      output: {
        // Isolate the AWS SDK / Smithy runtime into a dedicated vendor chunk.
        // Under Vite 8's Rolldown bundler, the default code-splitting heuristic
        // hoisted shared SDK helpers into an arbitrary page-route node (e.g.
        // /neptune) and then had the shared SDK chunk import them back, creating
        // a circular import. That cycle left command-factory bindings (e.g. the
        // RDS/Neptune classBuilder) uninitialized when a route's top-level
        // `class extends factory(...)` ran, throwing "TypeError: z is not a
        // function" during hydration and blanking every dashboard page.
        // Pinning all node_modules SDK code to one chunk breaks the cycle:
        // page nodes only import FROM this chunk, never the reverse.
        manualChunks(id: string) {
          if (id.includes("node_modules/@aws-sdk") || id.includes("node_modules/@smithy")) {
            return "aws-sdk";
          }
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
