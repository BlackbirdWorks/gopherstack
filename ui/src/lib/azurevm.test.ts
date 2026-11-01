import { describe, expect, it, vi } from "vitest";

import {
  COMPUTE_API_VERSION,
  NETWORK_API_VERSION,
  VM_PROXY_BASE,
  allowedActions,
  listDisks,
  listNics,
  listNsgs,
  listPublicIps,
  listVms,
  listVnets,
  parseNic,
  parseNsg,
  parseVm,
  parseVnet,
  powerAction,
  powerLabel,
  powerStateOf,
  vmAddresses,
} from "./azurevm";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;

const sub = "00000000-0000-0000-0000-000000000000";
const rgPath = (rg: string, ns: string, type: string, name: string) =>
  `/subscriptions/${sub}/resourceGroups/${rg}/providers/Microsoft.${ns}/${type}/${name}`;
const vm = (name: string, power = "running", rg = "demo") => ({
  id: rgPath(rg, "Compute", "virtualMachines", name),
  name,
  location: "eastus",
  properties: {
    hardwareProfile: { vmSize: "Standard_B2s" },
    osProfile: { computerName: name, adminUsername: "azureuser" },
    storageProfile: {
      imageReference: { publisher: "Canonical", offer: "ubuntu", sku: "22_04" },
      osDisk: {
        name: `${name}-os`,
        osType: "Linux",
        diskSizeGB: 30,
        managedDisk: { storageAccountType: "Premium_LRS" },
      },
      dataDisks: [{ name: `${name}-data`, diskSizeGB: 64 }],
    },
    networkProfile: {
      networkInterfaces: [{ id: rgPath(rg, "Network", "networkInterfaces", `${name}-nic`) }],
    },
    instanceView: {
      statuses: [{ code: "ProvisioningState/succeeded" }, { code: `PowerState/${power}` }],
    },
  },
});
const nic = (name: string) => ({
  id: rgPath("demo", "Network", "networkInterfaces", name),
  name,
  properties: {
    ipConfigurations: [
      {
        properties: {
          privateIPAddress: "10.0.0.4",
          publicIPAddress: { id: rgPath("demo", "Network", "publicIPAddresses", "pip") },
          subnet: { id: "subnet-id" },
        },
      },
    ],
  },
});
const computeApi = `?api-version=${COMPUTE_API_VERSION}`;

describe("parsing", () => {
  it("parses a VM with disks and power state", () => {
    expect(parseVm(vm("web"))).toMatchObject({
      name: "web",
      resourceGroup: "demo",
      size: "Standard_B2s",
      osType: "Linux",
      power: "running",
      adminUsername: "azureuser",
      image: "Canonical:ubuntu:22_04",
      disks: [
        { name: "web-os", kind: "OS", sizeGb: 30, type: "Premium_LRS" },
        { name: "web-data", kind: "Data", sizeGb: 64 },
      ],
    });
  });

  it("reads the power state from the instance view", () => {
    expect(powerStateOf(vm("a", "deallocated"))).toBe("deallocated");
    expect(powerStateOf({ properties: {} })).toBe("");
  });

  it("resolves a VM's addresses through its NICs and public IPs", () => {
    const v = parseVm(vm("web"));
    const addr = vmAddresses(
      v,
      [parseNic(nic("web-nic")), parseNic(nic("other"))],
      [
        {
          id: rgPath("demo", "Network", "publicIPAddresses", "pip"),
          name: "pip",
          address: "20.1.2.3",
          allocation: "Static",
        },
      ],
    );
    expect(addr).toEqual({ privateIps: ["10.0.0.4"], publicIps: ["20.1.2.3"] });
  });

  it("parses a network and a security group sorted by priority", () => {
    expect(
      parseVnet({
        id: rgPath("demo", "Network", "virtualNetworks", "vnet"),
        name: "vnet",
        properties: {
          addressSpace: { addressPrefixes: ["10.0.0.0/16"] },
          subnets: [
            {
              name: "default",
              properties: {
                addressPrefix: "10.0.0.0/24",
                networkSecurityGroup: { id: "/x/nsgs/web-nsg" },
              },
            },
          ],
        },
      }),
    ).toMatchObject({
      prefixes: ["10.0.0.0/16"],
      subnets: [{ name: "default", prefix: "10.0.0.0/24", nsg: "web-nsg" }],
    });
    const g = parseNsg({
      id: rgPath("demo", "Network", "networkSecurityGroups", "nsg"),
      name: "nsg",
      properties: {
        securityRules: [
          {
            name: "b",
            properties: { priority: 200, destinationPortRange: "443", access: "Allow" },
          },
          {
            name: "a",
            properties: { priority: 100, destinationPortRanges: ["22", "80"], access: "Deny" },
          },
        ],
      },
    });
    expect(g.rules.map((r) => [r.name, r.ports])).toEqual([
      ["a", "22, 80"],
      ["b", "443"],
    ]);
  });

  it.each([
    ["running", ["restart", "powerOff", "deallocate"]],
    ["stopped", ["start", "deallocate"]],
    ["deallocated", ["start"]],
    ["", []],
  ])("allows %j actions", (power, want) => {
    expect(allowedActions(power)).toEqual(want);
  });

  it("labels power states", () => {
    expect(powerLabel("running")).toBe("Running");
    expect(powerLabel("")).toBe("Unknown");
  });
});

describe("client", () => {
  it("lists VMs with their instance view, sorted", async () => {
    const fetchFn = vi.fn((_url: string) =>
      Promise.resolve(json({ value: [vm("b", "running", "z"), vm("a")] })),
    );
    const out = await listVms(sub, asFetch(fetchFn));
    expect(out.map((v) => `${v.resourceGroup}/${v.name}`)).toEqual(["demo/a", "z/b"]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${VM_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.Compute/virtualMachines${computeApi}&statusOnly=true`,
    );
  });

  it("lists disks and the network resources", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json({
          value: url.includes("/disks")
            ? [
                {
                  id: rgPath("demo", "Compute", "disks", "d"),
                  name: "d",
                  managedBy: "/x/virtualMachines/web",
                  sku: { name: "Premium_LRS" },
                  properties: { diskSizeGB: 30, diskState: "Attached" },
                },
              ]
            : [],
        }),
      ),
    );
    expect((await listDisks(sub, asFetch(fetchFn)))[0]).toMatchObject({
      attachedTo: "web",
      sku: "Premium_LRS",
      sizeGb: 30,
    });
    await listNics(sub, asFetch(fetchFn));
    await listPublicIps(sub, asFetch(fetchFn));
    await listVnets(sub, asFetch(fetchFn));
    await listNsgs(sub, asFetch(fetchFn));
    expect(fetchFn.mock.calls.slice(1).map((c) => c[0])).toEqual(
      ["networkInterfaces", "publicIPAddresses", "virtualNetworks", "networkSecurityGroups"].map(
        (t) =>
          `${VM_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.Network/${t}?api-version=${NETWORK_API_VERSION}`,
      ),
    );
  });

  it("posts a power action to the VM", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    await powerAction(sub, { resourceGroup: "demo", name: "web" }, "powerOff", asFetch(fetchFn));
    expect([fetchFn.mock.calls[0]?.[0], (fetchFn.mock.calls[0]![1] as RequestInit).method]).toEqual(
      [
        `${VM_PROXY_BASE}${rgPath("demo", "Compute", "virtualMachines", "web")}/powerOff${computeApi}`,
        "POST",
      ],
    );
  });

  it("surfaces an ARM failure", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(
        json({ error: { code: "OperationNotAllowed", message: "VM is stopped" } }, 409),
      );
    await expect(
      powerAction(sub, { resourceGroup: "demo", name: "web" }, "restart", asFetch(fetchFn)),
    ).rejects.toMatchObject({
      httpStatus: 409,
      message: "VM is stopped",
    });
  });
});
