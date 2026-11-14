import { describe, expect, it, vi } from "vitest";

import {
  NETWORK_API_VERSION,
  NETWORK_PROXY_BASE,
  awaitOperation,
  deleteRule,
  emptyRule,
  gatewayAction,
  gatewayRunning,
  getBackendHealth,
  listAppGateways,
  listNsgs,
  listRuleCollectionGroups,
  listVnets,
  parseAppGateway,
  parseBackendHealth,
  parseFirewall,
  parseNsg,
  parseRuleCollectionGroup,
  parseVnet,
  putRule,
  putVnet,
  ruleBody,
  sortRules,
  splitList,
  subnetBody,
  validCidr,
  validPorts,
  validPrefixes,
  validPriority,
  validateRule,
  validateVnet,
  vnetBody,
  type RuleInput,
} from "./azurenetwork";

const sub = "00000000-0000-0000-0000-000000000000";
const net = (type: string, name: string, rg = "demo") =>
  `/subscriptions/${sub}/resourceGroups/${rg}/providers/Microsoft.Network/${type}/${name}`;
const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const noSleep = () => Promise.resolve();

const rule = (over: Partial<RuleInput> = {}): RuleInput => ({
  ...emptyRule(),
  name: "web",
  priority: "300",
  destinationPorts: "80, 443",
  ...over,
});

describe("parsers", () => {
  it("parses a vnet with subnet associations and delegations", () => {
    const v = parseVnet({
      id: net("virtualNetworks", "hub"),
      name: "hub",
      location: "eastus",
      properties: {
        addressSpace: { addressPrefixes: ["10.0.0.0/16"] },
        provisioningState: "Succeeded",
        subnets: [
          {
            name: "app",
            properties: {
              addressPrefix: "10.0.1.0/24",
              networkSecurityGroup: { id: net("networkSecurityGroups", "web") },
              natGateway: { id: net("natGateways", "egress") },
              delegations: [
                { name: "d", properties: { serviceName: "Microsoft.Web/serverFarms" } },
              ],
            },
          },
        ],
      },
    });
    expect(v.resourceGroup).toBe("demo");
    expect(v.addressSpace).toEqual(["10.0.0.0/16"]);
    expect(v.subnets[0]).toEqual({
      name: "app",
      addressPrefix: "10.0.1.0/24",
      nsg: "web",
      natGateway: "egress",
      delegations: [{ name: "d", service: "Microsoft.Web/serverFarms" }],
    });
  });

  it("sorts rules by priority and merges singular and plural fields", () => {
    const nsg = parseNsg({
      id: net("networkSecurityGroups", "web"),
      name: "web",
      location: "eastus",
      properties: {
        securityRules: [
          {
            name: "b",
            properties: {
              priority: 300,
              destinationPortRanges: ["80", "443"],
              sourceAddressPrefix: "*",
            },
          },
          {
            name: "a",
            properties: { priority: 100, destinationPortRange: "22", direction: "Inbound" },
          },
          { name: "c", properties: { priority: 100 } },
        ],
        defaultSecurityRules: [{ name: "AllowVnetInBound", properties: { priority: 65000 } }],
      },
    });
    expect(nsg.rules.map((r) => r.name)).toEqual(["a", "c", "b"]);
    expect(nsg.rules[2].destinationPorts).toEqual(["80", "443"]);
    expect(nsg.rules[0].destinationPorts).toEqual(["22"]);
    expect(nsg.defaultRules[0].isDefault).toBe(true);
    expect(
      sortRules([
        { priority: 2, name: "x" },
        { priority: 1, name: "y" },
      ])[0].name,
    ).toBe("y");
  });

  it("parses an application gateway", () => {
    const g = parseAppGateway({
      id: net("applicationGateways", "agw"),
      name: "agw",
      location: "westus",
      properties: {
        sku: { name: "Standard_v2", tier: "Standard_v2", capacity: 2 },
        operationalState: "Running",
        frontendPorts: [{ name: "p80", properties: { port: 80 } }],
        httpListeners: [
          {
            name: "l",
            properties: {
              protocol: "Http",
              frontendPort: { id: net("applicationGateways", "agw") + "/frontendPorts/p80" },
            },
          },
        ],
        backendAddressPools: [
          { name: "pool", properties: { backendAddresses: [{ ipAddress: "10.0.2.10" }] } },
        ],
        backendHttpSettingsCollection: [
          { name: "s", properties: { port: 8080, protocol: "Http" } },
        ],
        requestRoutingRules: [
          {
            name: "r2",
            properties: {
              ruleType: "Basic",
              priority: 20,
              httpListener: { id: "/x/httpListeners/l" },
            },
          },
          {
            name: "r1",
            properties: {
              ruleType: "Basic",
              priority: 10,
              httpListener: { id: "/x/httpListeners/l" },
              backendAddressPool: { id: "/x/backendAddressPools/pool" },
            },
          },
        ],
        probes: [{ name: "pr", properties: { protocol: "Http", host: "h", path: "/health" } }],
      },
    });
    expect(g).toMatchObject({ sku: "Standard_v2", capacity: 2, operationalState: "Running" });
    expect(g.listeners[0]).toMatchObject({ name: "l", port: "80" });
    expect(g.pools[0].addresses).toEqual(["10.0.2.10"]);
    expect(g.rules.map((r) => r.name)).toEqual(["r1", "r2"]);
    expect(g.rules[0].pool).toBe("pool");
    expect(g.settings[0].port).toBe(8080);
    expect(gatewayRunning(g)).toBe(true);
    expect(gatewayRunning({ operationalState: "Stopped" })).toBe(false);
  });

  it("parses firewalls and policy rule collection groups sorted by priority", () => {
    const f = parseFirewall({
      id: net("azureFirewalls", "fw"),
      name: "fw",
      properties: {
        threatIntelMode: "Alert",
        firewallPolicy: { id: net("firewallPolicies", "pol") },
        sku: { name: "AZFW_VNet", tier: "Standard" },
        ipConfigurations: [
          { name: "c", properties: { subnet: { id: "/x/subnets/AzureFirewallSubnet" } } },
        ],
      },
    });
    expect(f).toMatchObject({ threatIntel: "Alert", policy: "pol", tier: "Standard" });
    expect(f.ipConfigs[0].subnet).toBe("AzureFirewallSubnet");

    const g = parseRuleCollectionGroup({
      name: "grp",
      properties: {
        priority: 200,
        ruleCollections: [
          {
            name: "late",
            priority: 500,
            ruleCollectionType: "FirewallPolicyFilterRuleCollection",
            action: { type: "Deny" },
            rules: [],
          },
          {
            name: "early",
            priority: 100,
            ruleCollectionType: "FirewallPolicyFilterRuleCollection",
            action: { type: "Allow" },
            rules: [
              {
                ruleType: "NetworkRule",
                name: "r1",
                ipProtocols: ["TCP"],
                sourceAddresses: ["10.0.0.0/24"],
                destinationAddresses: ["*"],
                destinationPorts: ["443"],
              },
              {
                ruleType: "ApplicationRule",
                name: "r2",
                sourceAddresses: ["*"],
                targetFqdns: ["a.com"],
                protocols: [{ protocolType: "Https", port: 443 }],
              },
            ],
          },
        ],
      },
    });
    expect(g.collections.map((c) => c.name)).toEqual(["early", "late"]);
    expect(g.collections[0].type).toBe("Filter");
    expect(g.collections[0].rules[0].summary).toBe("TCP 10.0.0.0/24 -> *:443");
    expect(g.collections[0].rules[1].summary).toBe("* -> a.com (Https:443)");
  });

  it("parses backend health", () => {
    const pools = parseBackendHealth({
      backendAddressPools: [
        {
          backendAddressPool: { id: "/x/backendAddressPools/pool" },
          backendHttpSettingsCollection: [
            {
              backendHttpSettings: { id: "/x/backendHttpSettingsCollection/s" },
              servers: [{ address: "10.0.2.10", health: "Unknown", healthProbeLog: "no route" }],
            },
          ],
        },
      ],
    });
    expect(pools).toEqual([
      {
        pool: "pool",
        settings: "s",
        servers: [{ address: "10.0.2.10", health: "Unknown", log: "no route" }],
      },
    ]);
  });
});

describe("validation", () => {
  it("validates rule priority 100 to 4096", () => {
    expect(validPriority("99")).toMatch(/between 100 and 4096/);
    expect(validPriority("100")).toBeNull();
    expect(validPriority("4096")).toBeNull();
    expect(validPriority("4097")).toMatch(/between/);
    expect(validPriority("1.5")).toMatch(/whole number/);
    expect(validPriority("")).toMatch(/whole number/);
  });

  it("validates ports, prefixes and cidrs", () => {
    expect(validPorts("22")).toBeNull();
    expect(validPorts("80, 8000-8100")).toBeNull();
    expect(validPorts("*")).toBeNull();
    expect(validPorts("70000")).not.toBeNull();
    expect(validPorts("http")).not.toBeNull();
    expect(validPorts("")).not.toBeNull();
    expect(validPrefixes("*")).toBeNull();
    expect(validPrefixes("Internet, 10.0.0.0/24, 10.0.0.5")).toBeNull();
    expect(validPrefixes("bad prefix")).not.toBeNull();
    expect(validCidr("10.0.0.0/16")).toBeNull();
    expect(validCidr("10.0.0.0/33")).not.toBeNull();
    expect(validCidr("300.0.0.0/8")).not.toBeNull();
    expect(validCidr("10.0.0.0")).not.toBeNull();
  });

  it("validates whole rule and vnet drafts", () => {
    expect(validateRule(rule())).toBeNull();
    expect(validateRule(rule({ name: "" }))).toMatch(/required/);
    expect(validateRule(rule({ name: "Web" }), ["web"])).toMatch(/exists/);
    expect(validateRule(rule({ priority: "50" }))).toMatch(/between/);
    expect(validateRule(rule({ destinationPorts: "" }))).not.toBeNull();
    const v = {
      name: "hub",
      location: "eastus",
      addressSpace: "10.0.0.0/16",
      subnetName: "",
      subnetPrefix: "",
    };
    expect(validateVnet(v)).toBeNull();
    expect(validateVnet({ ...v, addressSpace: "nope" })).not.toBeNull();
    expect(validateVnet({ ...v, subnetName: "a" })).not.toBeNull();
    expect(validateVnet({ ...v, name: "-x" })).not.toBeNull();
  });
});

describe("request bodies", () => {
  it("builds rules with singular and plural fields", () => {
    expect(ruleBody(rule({ description: " dns " }))).toEqual({
      properties: {
        priority: 300,
        direction: "Inbound",
        access: "Allow",
        protocol: "Tcp",
        description: "dns",
        sourcePortRange: "*",
        destinationPortRanges: ["80", "443"],
        sourceAddressPrefix: "*",
        destinationAddressPrefix: "*",
      },
    });
    expect(splitList(" a, ,b ")).toEqual(["a", "b"]);
  });

  it("builds vnet and subnet bodies", () => {
    const v = {
      name: "hub",
      location: "westus",
      addressSpace: "10.0.0.0/16, 10.1.0.0/16",
      subnetName: "app",
      subnetPrefix: "10.0.0.0/24",
    };
    expect(vnetBody(v)).toEqual({
      location: "westus",
      properties: {
        addressSpace: { addressPrefixes: ["10.0.0.0/16", "10.1.0.0/16"] },
        subnets: [{ name: "app", properties: { addressPrefix: "10.0.0.0/24" } }],
      },
    });
    expect(vnetBody({ ...v, subnetName: "" }).properties).not.toHaveProperty("subnets");
    expect(
      subnetBody({
        name: "s",
        prefix: "10.0.1.0/24",
        nsg: "/n",
        nat: "",
        delegation: "Microsoft.Web/serverFarms",
      }),
    ).toEqual({
      properties: {
        addressPrefix: "10.0.1.0/24",
        networkSecurityGroup: { id: "/n" },
        delegations: [
          { name: "delegation", properties: { serviceName: "Microsoft.Web/serverFarms" } },
        ],
      },
    });
  });
});

describe("transport", () => {
  it("lists through the pinned proxy path", async () => {
    const f = vi
      .fn()
      .mockImplementation(() => Promise.resolve(json({ value: [{ name: "b" }, { name: "a" }] })));
    const vnets = await listVnets(sub, "demo", asFetch(f));
    expect(vnets.map((v) => v.name)).toEqual(["a", "b"]);
    expect(f.mock.calls[0][0]).toBe(
      `${NETWORK_PROXY_BASE}/subscriptions/${sub}/resourceGroups/demo/providers/Microsoft.Network/virtualNetworks?api-version=${NETWORK_API_VERSION}`,
    );
    const nsgs = await listNsgs(
      sub,
      "demo",
      asFetch(vi.fn().mockImplementation(() => Promise.resolve(json({})))),
    );
    expect(nsgs).toEqual([]);
    expect(
      await listAppGateways(sub, "demo", asFetch(vi.fn().mockResolvedValue(json({})))),
    ).toEqual([]);
  });

  it("lists rule collection groups sorted by priority", async () => {
    const f = vi.fn().mockImplementation(() =>
      Promise.resolve(
        json({
          value: [
            { name: "b", properties: { priority: 300 } },
            { name: "a", properties: { priority: 200 } },
          ],
        }),
      ),
    );
    const groups = await listRuleCollectionGroups(sub, "demo", "pol", asFetch(f));
    expect(groups.map((g) => g.name)).toEqual(["a", "b"]);
    expect(f.mock.calls[0][0]).toContain("/firewallPolicies/pol/ruleCollectionGroups?api-version=");
  });

  it("follows the async operation until it succeeds", async () => {
    const statuses = ["InProgress", "InProgress", "Succeeded"];
    const f = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === "PUT") {
        return Promise.resolve(
          json({ name: "hub" }, 201, { "Azure-AsyncOperation": `${NETWORK_PROXY_BASE}/op/1` }),
        );
      }
      expect(url).toBe(`${NETWORK_PROXY_BASE}/op/1`);
      return Promise.resolve(json({ status: statuses.shift() }));
    });
    const sleep = vi.fn(noSleep);
    const v = {
      name: "hub",
      location: "eastus",
      addressSpace: "10.0.0.0/16",
      subnetName: "",
      subnetPrefix: "",
    };
    await putVnet(sub, "demo", v, asFetch(f), sleep);
    expect(sleep).toHaveBeenCalledTimes(2);
    expect(f.mock.calls[0][1]?.body).toBe(JSON.stringify(vnetBody(v)));
  });

  it("fails when the operation fails or never ends", async () => {
    const failed = vi.fn().mockImplementation(() => Promise.resolve(json({ status: "Failed" })));
    await expect(awaitOperation("/op", asFetch(failed), noSleep)).rejects.toMatchObject({
      code: "Failed",
    });
    const stuck = vi.fn().mockImplementation(() => Promise.resolve(json({ status: "InProgress" })));
    await expect(awaitOperation("/op", asFetch(stuck), noSleep)).rejects.toMatchObject({
      code: "Timeout",
    });
    expect(stuck).toHaveBeenCalledTimes(30);
    await expect(awaitOperation("", asFetch(vi.fn()), noSleep)).resolves.toBeUndefined();
  });

  it("surfaces ARM error codes", async () => {
    const f = vi
      .fn()
      .mockImplementation(() =>
        Promise.resolve(
          json({ error: { code: "InvalidRequestFormat", message: "bad rule" } }, 400),
        ),
      );
    await expect(putRule(sub, "demo", "web", rule(), asFetch(f), noSleep)).rejects.toMatchObject({
      httpStatus: 400,
      code: "InvalidRequestFormat",
      message: "bad rule",
    });
    const down = vi.fn().mockRejectedValue(new Error("offline"));
    await expect(deleteRule(sub, "demo", "web", "r", asFetch(down), noSleep)).rejects.toMatchObject(
      { code: "NetworkError" },
    );
  });

  it("addresses rules and gateway actions", async () => {
    const f = vi.fn().mockImplementation(() => Promise.resolve(json({}, 202)));
    await putRule(sub, "demo", "web", rule(), asFetch(f), noSleep);
    await deleteRule(sub, "demo", "web", "web", asFetch(f), noSleep);
    await gatewayAction(sub, "demo", "agw", "stop", asFetch(f), noSleep);
    expect(
      f.mock.calls.map((c) => [
        c[1]?.method,
        String(c[0]).split("Microsoft.Network/")[1].split("?")[0],
      ]),
    ).toEqual([
      ["PUT", "networkSecurityGroups/web/securityRules/web"],
      ["DELETE", "networkSecurityGroups/web/securityRules/web"],
      ["POST", "applicationGateways/agw/stop"],
    ]);
  });

  it("reads backend health from the operation result", async () => {
    const f = vi.fn((url: string, init?: RequestInit) => {
      if (init?.method === "POST") {
        return Promise.resolve(
          json({}, 202, { "Azure-AsyncOperation": "/op/1", Location: "/res/1" }),
        );
      }
      if (url === "/op/1") return Promise.resolve(json({ status: "Succeeded" }));
      return Promise.resolve(
        json({
          backendAddressPools: [
            {
              backendAddressPool: { id: "/p/pool" },
              backendHttpSettingsCollection: [
                {
                  backendHttpSettings: { id: "/s/set" },
                  servers: [{ address: "127.0.0.1", health: "Healthy" }],
                },
              ],
            },
          ],
        }),
      );
    });
    const pools = await getBackendHealth(sub, "demo", "agw", asFetch(f), noSleep);
    expect(pools[0].servers[0]).toMatchObject({ address: "127.0.0.1", health: "Healthy" });
  });
});
