import { describe, expect, it } from "vitest";

import {
  getCommonCategories,
  getCommonServices,
  getUncommonCategories,
  getUncommonServices,
  sidebarCategories,
  type DashboardCategory,
} from "./nav";

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
