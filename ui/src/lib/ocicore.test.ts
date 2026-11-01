import { describe, expect, it, vi } from "vitest";

import {
  CORE_PROXY_BASE,
  allowedActions,
  instanceAction,
  instanceShape,
  listCompartments,
  listInstanceVnics,
  listNsgRules,
  listResources,
  parseResource,
  protocolName,
  routeRules,
  securityListRules,
  shortId,
} from "./ocicore";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;

const comp = "ocid1.compartment.oc1..aaaa";

describe("parsing", () => {
  it("parses a resource and shortens OCIDs", () => {
    expect(
      parseResource({
        id: "ocid1.vcn.oc1.iad.xyz",
        displayName: "main",
        lifecycleState: "AVAILABLE",
      }),
    ).toMatchObject({ name: "main", state: "AVAILABLE" });
    expect(parseResource({ id: "ocid1.vcn" }).name).toBe("ocid1.vcn");
    expect(shortId("short")).toBe("short");
    expect(shortId("ocid1.subnet.oc1.iad.abcdefghijklmnop")).toBe("…efghijklmnop");
  });

  it.each([
    ["6", "TCP"],
    ["17", "UDP"],
    ["1", "ICMP"],
    ["all", "ALL"],
    ["99", "99"],
  ])("names protocol %s", (p, want) => {
    expect(protocolName(p)).toBe(want);
  });

  it("flattens security list rules with ports", () => {
    const rules = securityListRules({
      ingressSecurityRules: [
        {
          protocol: "6",
          source: "0.0.0.0/0",
          tcpOptions: { destinationPortRange: { min: 22, max: 22 } },
        },
        {
          protocol: "6",
          source: "10.0.0.0/8",
          tcpOptions: { destinationPortRange: { min: 8000, max: 8100 } },
        },
      ],
      egressSecurityRules: [{ protocol: "all", destination: "0.0.0.0/0", isStateless: true }],
    });
    expect(rules.map((r) => [r.direction, r.protocol, r.peer, r.ports, r.stateless])).toEqual([
      ["INGRESS", "TCP", "0.0.0.0/0", "22", false],
      ["INGRESS", "TCP", "10.0.0.0/8", "8000-8100", false],
      ["EGRESS", "ALL", "0.0.0.0/0", "all", true],
    ]);
  });

  it("reads route rules and shapes", () => {
    expect(
      routeRules({ routeRules: [{ destination: "0.0.0.0/0", networkEntityId: "ocid1.ig" }] }),
    ).toEqual([{ destination: "0.0.0.0/0", target: "ocid1.ig" }]);
    const inst = parseResource({
      id: "i",
      shape: "VM.Standard.E4.Flex",
      shapeConfig: { ocpus: 2, memoryInGBs: 32 },
    });
    expect(instanceShape(inst)).toBe("VM.Standard.E4.Flex (2 OCPU, 32 GB)");
  });

  it("allows power actions by state", () => {
    expect(allowedActions("RUNNING")).toEqual(["STOP", "SOFTRESET"]);
    expect(allowedActions("STOPPED")).toEqual(["START"]);
    expect(allowedActions("STOPPING")).toEqual([]);
  });
});

describe("requests", () => {
  it("lists compartments", async () => {
    const fetchFn = vi.fn().mockResolvedValue(json({ compartments: [comp] }));
    expect(await listCompartments(asFetch(fetchFn))).toEqual([comp]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${CORE_PROXY_BASE}/compartments`);
  });

  it("follows opc-next-page across list pages", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("page=p2")
          ? json([{ id: "b", displayName: "b" }])
          : json([{ id: "a", displayName: "a" }], 200, { "opc-next-page": "p2" }),
      ),
    );
    const vcns = await listResources("vcns", comp, "", asFetch(fetchFn));
    expect(vcns.map((v) => v.id)).toEqual(["a", "b"]);
    expect(fetchFn.mock.calls.map((c) => c[0])).toEqual([
      `${CORE_PROXY_BASE}/api/vcns?compartmentId=${encodeURIComponent(comp)}`,
      `${CORE_PROXY_BASE}/api/vcns?compartmentId=${encodeURIComponent(comp)}&page=p2`,
    ]);
  });

  it("joins VNIC attachments with their VNICs", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("/vnics/")
          ? json({ privateIp: "10.0.0.5", publicIp: "1.2.3.4", macAddress: "aa", isPrimary: true })
          : json([{ id: "att1", vnicId: "vnic1", lifecycleState: "ATTACHED", subnetId: "s" }]),
      ),
    );
    const vnics = await listInstanceVnics(comp, "inst1", asFetch(fetchFn));
    expect(vnics).toEqual([
      {
        attachmentId: "att1",
        vnicId: "vnic1",
        state: "ATTACHED",
        name: "",
        privateIp: "10.0.0.5",
        publicIp: "1.2.3.4",
        mac: "aa",
        subnetId: "s",
        isPrimary: true,
      },
    ]);
    expect(String(fetchFn.mock.calls[0]?.[0])).toContain("instanceId=inst1");
  });

  it("reads NSG rules and posts instance actions", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("securityRules")
          ? json([{ direction: "INGRESS", protocol: "6", source: "0.0.0.0/0" }])
          : json({ id: "i", displayName: "web", lifecycleState: "STOPPING" }),
      ),
    );
    expect((await listNsgRules("nsg1", asFetch(fetchFn)))[0]).toMatchObject({
      direction: "INGRESS",
      protocol: "TCP",
    });
    const r = await instanceAction("inst 1", "STOP", asFetch(fetchFn));
    expect(r.state).toBe("STOPPING");
    expect(fetchFn.mock.calls[1]).toEqual([
      `${CORE_PROXY_BASE}/api/instances/inst%201?action=STOP`,
      { method: "POST" },
    ]);
  });

  it("reports the emulator's flat error body", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ code: "NotAuthorizedOrNotFound", message: "missing" }, 404));
    await expect(listResources("vcns", comp, "", asFetch(fetchFn))).rejects.toMatchObject({
      httpStatus: 404,
      code: "NotAuthorizedOrNotFound",
      message: "missing",
    });
  });
});
