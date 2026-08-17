import { existsSync, readFileSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import {
  getCommonCategories,
  getCommonServices,
  getUncommonCategories,
  getUncommonServices,
  implementedDashboardRouteIds,
  sidebarCategories,
  type DashboardCategory,
} from "./nav";

const repoRoot = resolve(import.meta.dirname, "../../..");
const servicesDir = resolve(repoRoot, "services");
const cliGoSource = readFileSync(resolve(repoRoot, "cli.go"), "utf8");

// Route ids whose backend lives under a services/<dir> whose name differs
// from the dashboard route id (Go package-name collisions with stdlib/3rd
// party names, or historical renames). Verified against cli.go's imports.
const ROUTE_ID_TO_SERVICE_DIR: Record<string, string> = {
  config: "awsconfig",
  costexplorer: "ce",
  inspector: "inspector2",
  msk: "kafka",
  sfn: "stepfunctions",
  timestream: "timestreamwrite",
};

// Dashboard chrome carried in implementedDashboardRouteIds that isn't
// backed by an AWS service emulator at all — nothing to check a services/
// dir or cli.go registration against.
const BACKEND_CHECK_EXEMPT_ROUTE_IDS = new Set<string>(["chaos", "resources"]);

// Known gap, not swept under the rug: the dashboard advertises this route
// (nav entry + page) but no services/globalaccelerator backend exists yet
// and cli.go never registers one. Remove this entry once the backend
// lands — from that point the guard below holds it to the same standard
// as every other route.
const KNOWN_BACKEND_GAP_ROUTE_IDS = new Set<string>(["globalaccelerator"]);

// Route directories under src/routes/ that are deliberately NOT linked from
// sidebarCategories. Every entry here is a conscious exemption from the
// "every service route is reachable from the sidebar" rule below — adding to
// this list should be a deliberate act, not a way to silence a failing test.
const SIDEBAR_EXEMPT_ROUTE_IDS = new Set<string>([
  // Dashboard chrome: these are pages, but not AWS-service pages. Each is
  // linked directly from the top navbar icons in +layout.svelte instead of
  // from a sidebarCategories entry.
  //
  // Chaos-engineering control panel. It also has a sidebarCategories entry
  // today (via settings-resilience), but its chrome-ness makes it exempt
  // regardless.
  "chaos",
  // Live API console.
  "console",
  // Docs viewer.
  "docs",
  // System metrics page.
  "metrics",
  // Resource health page (top navbar "Resource Health" icon).
  "resources",
  // Settings page.
  "settings",
  // Legacy alias, not chrome: routes/awsconfig/+page.svelte is a 4-line
  // re-export of routes/config/+page.svelte, kept only so the legacy
  // `/dashboard/awsconfig` URL exercised by test/e2e/awsconfig_test.go keeps
  // resolving. Deliberately not sidebar-linked — a nav entry would just be a
  // confusing duplicate of the "Config" entry that already points at `config`.
  "awsconfig",
]);

describe("sidebarCategories", () => {
  it("contains all expected category ids", () => {
    const ids = sidebarCategories.map((c: DashboardCategory) => c.id);
    expect(ids).toContain("core-services");
    expect(ids).toContain("messaging-serverless");
    expect(ids).toContain("security-services");
    expect(ids).toContain("compute");
    expect(ids).toContain("database-analytics");
    expect(ids).toContain("devtools");
    expect(ids).toContain("app-integration");
    expect(ids).toContain("web-mobile");
    expect(ids).toContain("intelligence-ml");
    expect(ids).toContain("iot");
    expect(ids).toContain("storage");
    expect(ids).toContain("business");
    expect(ids).toContain("networking");
    expect(ids).toContain("management-observability");
    expect(ids).toContain("settings-resilience");
  });

  it("core-services category has S3 and ElastiCache", () => {
    const core = sidebarCategories.find((c: DashboardCategory) => c.id === "core-services");
    expect(core).toBeDefined();
    const routeIds = core!.routes.map((r) => r.id);
    expect(routeIds).toContain("s3");
    expect(routeIds).toContain("elasticache");
  });

  it("settings-resilience includes FIS route", () => {
    const settings = sidebarCategories.find(
      (c: DashboardCategory) => c.id === "settings-resilience",
    );
    expect(settings).toBeDefined();
    const routeIds = settings!.routes.map((r) => r.id);
    expect(routeIds).toContain("fis");
  });

  it("all routes have required fields", () => {
    for (const category of sidebarCategories) {
      for (const route of category.routes) {
        expect(route.id).toBeTruthy();
        expect(route.href).toMatch(/^\/dashboard\//);
        expect(route.label).toBeTruthy();
        expect(route.icon).toBeTruthy();
      }
    }
  });

  it("each category has an id and label", () => {
    for (const category of sidebarCategories) {
      expect(category.id).toBeTruthy();
      expect(category.label).toBeTruthy();
      expect(category.routes.length).toBeGreaterThan(0);
    }
  });

  it("getCommonServices returns only common routes", () => {
    const commonServices = getCommonServices();

    expect(commonServices.length).toBeGreaterThan(0);
    expect(commonServices.every((route) => route.common === true)).toBe(true);
    expect(commonServices.some((route) => route.id === "s3")).toBe(true);
  });

  it("getUncommonServices returns only non-common routes", () => {
    const uncommonServices = getUncommonServices();
    const totalRoutes = sidebarCategories.flatMap((category) => category.routes);
    const commonServices = getCommonServices();

    expect(uncommonServices.length).toBeGreaterThan(0);
    expect(uncommonServices.every((route) => !route.common)).toBe(true);
    expect(uncommonServices.length + commonServices.length).toBe(totalRoutes.length);
  });

  it("getCommonCategories returns only categories with common routes", () => {
    const commonCategories = getCommonCategories();

    expect(commonCategories.length).toBeGreaterThan(0);
    expect(commonCategories.every((category) => category.routes.length > 0)).toBe(true);
    expect(
      commonCategories.every((category) => category.routes.every((route) => route.common === true)),
    ).toBe(true);
  });

  it("getUncommonCategories returns only categories with non-common routes", () => {
    const uncommonCategories = getUncommonCategories();

    expect(uncommonCategories.length).toBeGreaterThan(0);
    expect(uncommonCategories.every((category) => category.routes.length > 0)).toBe(true);
    expect(
      uncommonCategories.every((category) => category.routes.every((route) => !route.common)),
    ).toBe(true);
  });
});

describe("nav catalog matches the routes directory (drift guard)", () => {
  // Vite-native glob, works under vitest without touching the filesystem by hand.
  // Matches only top-level `routes/<id>/+page.svelte` files, so nested dynamic
  // routes like `s3/[bucket]/+page.svelte` are NOT included here — that's
  // intentional, we only care about top-level service/chrome route ids.
  const routeModules = import.meta.glob("../routes/*/+page.svelte");

  const routeDirIds = Object.keys(routeModules).map((path) => {
    const match = path.match(/^\.\.\/routes\/([^/]+)\/\+page\.svelte$/);
    if (!match) {
      throw new Error(`nav.test.ts: unexpected glob path shape: ${path}`);
    }
    return match[1];
  });
  const routeDirIdSet = new Set(routeDirIds);

  it("glob discovery sanity check: found a plausible number of top-level routes", () => {
    // `ls ui/src/routes | wc -l` includes 4 non-directory files alongside the
    // route directories: +layout.svelte, +page.svelte, layout.css,
    // page.test.ts. This glob only matches directories that contain a
    // +page.svelte, so it should land close to (total entries - 4) and, in
    // particular, comfortably above the ~150 known service/chrome routes.
    // This guards against the glob pattern silently matching nothing (e.g. if
    // it were ever pointed at the wrong relative path).
    expect(routeDirIds.length).toBeGreaterThan(150);
    expect(routeDirIdSet.size).toBe(routeDirIds.length);
  });

  it("every sidebarCategories route id has a matching routes/<id>/+page.svelte directory", () => {
    const missing: string[] = [];
    for (const category of sidebarCategories) {
      for (const route of category.routes) {
        if (!routeDirIdSet.has(route.id)) {
          missing.push(route.id);
        }
      }
    }

    expect(
      missing,
      `sidebarCategories route ids with no matching routes/<id>/+page.svelte directory: ${missing.join(", ") || "(none)"}`,
    ).toEqual([]);
  });

  it("every implementedDashboardRouteIds id has a matching routes/<id>/+page.svelte directory", () => {
    const missing = [...implementedDashboardRouteIds].filter((id) => !routeDirIdSet.has(id));

    expect(
      missing,
      `implementedDashboardRouteIds ids with no matching routes/<id>/+page.svelte directory: ${missing.join(", ") || "(none)"}`,
    ).toEqual([]);
  });

  it("every implementedDashboardRouteIds id has a services/<id> backend directory and a cli.go registration", () => {
    const missingServiceDir: string[] = [];
    const missingCliRegistration: string[] = [];

    for (const id of implementedDashboardRouteIds) {
      if (BACKEND_CHECK_EXEMPT_ROUTE_IDS.has(id) || KNOWN_BACKEND_GAP_ROUTE_IDS.has(id)) {
        continue;
      }

      const serviceDir = ROUTE_ID_TO_SERVICE_DIR[id] ?? id;

      if (!existsSync(resolve(servicesDir, serviceDir))) {
        missingServiceDir.push(id);
      }

      if (!cliGoSource.includes(`gopherstack/services/${serviceDir}"`)) {
        missingCliRegistration.push(id);
      }
    }

    expect(
      missingServiceDir,
      `implementedDashboardRouteIds ids with no services/<dir> backend directory (add a ` +
        `ROUTE_ID_TO_SERVICE_DIR entry if the dir is legitimately named differently): ${missingServiceDir.join(", ") || "(none)"}`,
    ).toEqual([]);
    expect(
      missingCliRegistration,
      `implementedDashboardRouteIds ids never imported/registered in cli.go: ${missingCliRegistration.join(", ") || "(none)"}`,
    ).toEqual([]);
  });

  it("every sidebarCategories route id is in implementedDashboardRouteIds", () => {
    // The opposite direction from the "reachable from sidebarCategories"
    // check below: a sidebarCategories entry whose id is missing from
    // implementedDashboardRouteIds renders a nav link that +layout.svelte's
    // `implementedDashboardRouteIds.has(route.id)` gate then filters out of
    // the sidebar entirely (gopherstack-ks2s.5). Every sidebarCategories
    // entry is presumed to have a real, working backend by the time it's
    // added to the catalog, so this direction has no exemption list.
    const missing: string[] = [];
    for (const category of sidebarCategories) {
      for (const route of category.routes) {
        if (!implementedDashboardRouteIds.has(route.id)) {
          missing.push(route.id);
        }
      }
    }

    expect(
      missing,
      `sidebarCategories route ids missing from implementedDashboardRouteIds (the sidebar link ` +
        `exists but is filtered out): ${missing.join(", ") || "(none)"}`,
    ).toEqual([]);
  });

  it("every service route directory is reachable from sidebarCategories, unless explicitly exempt", () => {
    const navRouteIds = new Set(
      sidebarCategories.flatMap((category) => category.routes.map((r) => r.id)),
    );

    const unreachable = routeDirIds.filter(
      (id) => !navRouteIds.has(id) && !SIDEBAR_EXEMPT_ROUTE_IDS.has(id),
    );

    expect(
      unreachable,
      `route directories not linked from any sidebarCategories entry and not listed in ` +
        `SIDEBAR_EXEMPT_ROUTE_IDS: ${unreachable.join(", ") || "(none)"}. ` +
        `Either add a sidebarCategories entry for it, or add it to SIDEBAR_EXEMPT_ROUTE_IDS ` +
        `with a comment explaining why it's intentionally not a sidebar service.`,
    ).toEqual([]);
  });
});
