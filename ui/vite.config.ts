import tailwindcss from "@tailwindcss/vite";
import { defineConfig } from "vitest/config";
import { sveltekit } from "@sveltejs/kit/vite";

export default defineConfig({
  base: "/dashboard/",
  plugins: [tailwindcss(), sveltekit()],
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
      ],
      thresholds: {
        branches: 100,
        functions: 100,
        lines: 100,
        statements: 100,
      },
    },
  },
});
