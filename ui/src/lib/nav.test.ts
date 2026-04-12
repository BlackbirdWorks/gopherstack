import { describe, expect, it } from "vitest";

import { sidebarCategories, type DashboardCategory } from "./nav";

describe("sidebarCategories", () => {
  it("contains all expected category ids", () => {
    const ids = sidebarCategories.map((c: DashboardCategory) => c.id);
    expect(ids).toContain("core-services");
    expect(ids).toContain("standard-services");
    expect(ids).toContain("message-services");
    expect(ids).toContain("security-services");
    expect(ids).toContain("compute");
    expect(ids).toContain("database-analytics");
    expect(ids).toContain("devtools");
    expect(ids).toContain("management");
    expect(ids).toContain("app-integration");
    expect(ids).toContain("web-mobile");
    expect(ids).toContain("iot-ai");
    expect(ids).toContain("networking-security");
    expect(ids).toContain("storage");
    expect(ids).toContain("business");
    expect(ids).toContain("networking");
    expect(ids).toContain("monitoring");
    expect(ids).toContain("settings-resilience");
  });

  it("core-services category has DynamoDB, S3, ElastiCache", () => {
    const core = sidebarCategories.find((c: DashboardCategory) => c.id === "core-services");
    expect(core).toBeDefined();
    const routeIds = core!.routes.map((r) => r.id);
    expect(routeIds).toContain("dynamodb");
    expect(routeIds).toContain("s3");
    expect(routeIds).toContain("elasticache");
  });

  it("all routes have required fields", () => {
    for (const category of sidebarCategories) {
      for (const route of category.routes) {
        expect(route.id).toBeTruthy();
        expect(route.href).toMatch(/^\/dashboard2\//);
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
});
