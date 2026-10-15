import { describe, it, expect, vi } from "vitest";
import {
  describeError,
  endpointPath,
  isApiError,
  statusBadgeClass,
  withClusterEndpoints,
} from "./arc";

const clients: { path: string; region: string }[] = [];
vi.mock("#lib/aws-client.ts", () => ({
  getRoute53RecoveryClusterClient: (path: string, region: string) => {
    const c = { path, region };
    clients.push(c);
    return c;
  },
}));

describe("arc helpers", () => {
  it.each([
    [
      "http://h:1/route53-recovery-cluster/abc/us-west-2",
      "/route53-recovery-cluster/abc/us-west-2",
    ],
    [
      "http://h:1/route53-recovery-cluster/abc/us-west-2/",
      "/route53-recovery-cluster/abc/us-west-2",
    ],
    [undefined, ""],
  ])("endpointPath(%s)", (input, want) => {
    expect(endpointPath(input)).toBe(want);
  });

  it("describeError includes code and status", () => {
    const e = Object.assign(new Error("blocked"), {
      name: "ConflictException",
      $metadata: { httpStatusCode: 409 },
    });
    expect(describeError(e)).toBe("ConflictException (HTTP 409): blocked");
    expect(isApiError(e)).toBe(true);
    expect(isApiError(new TypeError("Failed to fetch"))).toBe(false);
  });

  it("statusBadgeClass maps states", () => {
    expect(statusBadgeClass("READY")).toContain("green");
    expect(statusBadgeClass("NOT_READY")).toContain("amber");
    expect(statusBadgeClass("FAILED")).toContain("red");
    expect(statusBadgeClass("x")).toContain("gray");
  });

  it("withClusterEndpoints prefers the region, fails over on network errors only", async () => {
    clients.length = 0;
    const eps = [
      { Endpoint: "http://h/p/c/us-east-1", Region: "us-east-1" },
      { Endpoint: "http://h/p/c/us-west-2", Region: "us-west-2" },
    ];
    const seen: string[] = [];
    const out = await withClusterEndpoints(eps, "us-west-2", (c) => {
      const cl = c as unknown as { region: string };
      seen.push(cl.region);
      if (cl.region === "us-west-2") {
        return Promise.reject(new TypeError("Failed to fetch"));
      }
      return Promise.resolve("ok");
    });
    expect(out).toBe("ok");
    expect(seen).toEqual(["us-west-2", "us-east-1"]);

    const conflict = Object.assign(new Error("no"), {
      name: "ConflictException",
      $metadata: { httpStatusCode: 409 },
    });
    const tried: string[] = [];
    await expect(
      withClusterEndpoints(eps, "us-east-1", (c) => {
        tried.push((c as unknown as { region: string }).region);
        return Promise.reject(conflict);
      }),
    ).rejects.toBe(conflict);
    expect(tried).toEqual(["us-east-1"]);

    await expect(withClusterEndpoints([], "us-east-1", () => Promise.resolve(1))).rejects.toThrow(
      "no endpoints",
    );
  });
});
