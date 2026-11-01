import { describe, expect, it, vi } from "vitest";

import {
  CI_PROXY_BASE,
  actionEnabled,
  createBody,
  createInstance,
  deleteInstance,
  getEngine,
  getInstance,
  inProgress,
  instanceAction,
  isBusy,
  listAvailabilityDomains,
  listContainers,
  listInstances,
  listShapes,
  listSubnets,
  listWorkRequestErrors,
  listWorkRequests,
  memoryRange,
  overallHealth,
  parseContainer,
  parseDetail,
  parseShape,
  renameInstance,
  retrieveLogs,
  splitArgs,
  validateNew,
  type NewInstance,
} from "./ocicontainerinstances";

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
const asFetch = (f: unknown) => f as typeof fetch;
const comp = "ocid1.compartment.oc1..aaaa";

const instRaw = {
  id: "ci1",
  displayName: "web",
  lifecycleState: "ACTIVE",
  shape: "CI.Standard.E4.Flex",
  shapeConfig: { ocpus: 2, memoryInGBs: 8 },
  containerCount: 1,
  volumeCount: 1,
  containerRestartPolicy: "ALWAYS",
  availabilityDomain: "AD-1",
  timeCreated: "2026-10-31T00:00:00Z",
  gracefulShutdownTimeoutInSeconds: 30,
  containers: [{ containerId: "c1", displayName: "main" }],
  vnics: [{ vnicId: "vnic1" }],
  volumes: [
    {
      name: "cfg",
      volumeType: "CONFIGFILE",
      configs: [{ fileName: "a.conf", path: "etc", sizeInBytes: 12 }],
    },
    { name: "tmp", volumeType: "EMPTYDIR", backingStore: "MEMORY" },
  ],
  imagePullSecrets: [{ secretType: "BASIC", registryEndpoint: "r.example.com" }],
};

const ctrRaw = {
  id: "c1",
  displayName: "main",
  imageUrl: "alpine:3.20",
  lifecycleState: "ACTIVE",
  exitCode: 0,
  containerRestartAttemptCount: 2,
  resourceConfig: { vcpusLimit: 0.5, memoryLimitInGBs: 1 },
  command: ["sh"],
  arguments: ["-c", "echo hi"],
  environmentVariableNames: ["B", "A"],
  volumeMounts: [{ volumeName: "cfg", mountPath: "/etc/app", isReadOnly: true }],
  healthChecks: [
    { name: "h1", healthCheckType: "HTTP", status: "HEALTHY" },
    { name: "h2", healthCheckType: "TCP", status: "UNHEALTHY", statusDetails: "refused" },
  ],
};

describe("parsing", () => {
  it("parses an instance with volume files and pull secrets", () => {
    const d = parseDetail(instRaw);
    expect(d).toMatchObject({
      id: "ci1",
      name: "web",
      state: "ACTIVE",
      ocpus: 2,
      memoryGBs: 8,
      gracefulShutdownSeconds: 30,
      vnicIds: ["vnic1"],
      containers: [{ id: "c1", name: "main" }],
      pullSecrets: [{ type: "BASIC", registry: "r.example.com" }],
    });
    expect(d.volumes[0]).toEqual({
      name: "cfg",
      type: "CONFIGFILE",
      backingStore: "",
      files: [{ fileName: "a.conf", path: "etc", sizeBytes: 12 }],
    });
    expect(d.volumes[1].backingStore).toBe("MEMORY");
  });

  it("leaves the graceful shutdown unset when absent", () => {
    expect(parseDetail({ id: "x" }).gracefulShutdownSeconds).toBeUndefined();
  });

  it("parses a container with names only for environment", () => {
    const c = parseContainer(ctrRaw);
    expect(c).toMatchObject({
      name: "main",
      image: "alpine:3.20",
      exitCode: 0,
      restartCount: 2,
      vcpusLimit: 0.5,
      memoryLimitGBs: 1,
      envNames: ["B", "A"],
      health: "UNHEALTHY",
    });
    expect(c.mounts).toEqual([{ volume: "cfg", path: "/etc/app", readOnly: true }]);
    expect(JSON.stringify(c)).not.toContain("environmentVariables");
  });

  it("combines health statuses", () => {
    expect(overallHealth([])).toBe("");
    expect(overallHealth([{ status: "HEALTHY" }])).toBe("HEALTHY");
    expect(overallHealth([{ status: "HEALTHY" }, { status: "UNKNOWN" }])).toBe("UNKNOWN");
    expect(overallHealth([{ status: "UNKNOWN" }, { status: "UNHEALTHY" }])).toBe("UNHEALTHY");
  });

  it("parses shapes and computes the memory range", () => {
    const s = parseShape({
      name: "CI.Standard.E4.Flex",
      ocpuOptions: { min: 1, max: 64 },
      memoryOptions: { minInGBs: 1, maxInGBs: 1024, defaultPerOcpuInGBs: 16, maxPerOcpuInGBs: 64 },
    });
    expect(memoryRange(s, 1)).toEqual({ min: 1, max: 64 });
    expect(memoryRange(s, 32)).toEqual({ min: 32, max: 1024 });
  });
});

describe("state helpers", () => {
  it("enables actions by state and work request", () => {
    expect(actionEnabled("stop", "ACTIVE", false)).toBe(true);
    expect(actionEnabled("restart", "ACTIVE", false)).toBe(true);
    expect(actionEnabled("start", "ACTIVE", false)).toBe(false);
    expect(actionEnabled("start", "INACTIVE", false)).toBe(true);
    expect(actionEnabled("stop", "INACTIVE", false)).toBe(false);
    expect(actionEnabled("stop", "ACTIVE", true)).toBe(false);
    expect(actionEnabled("start", "INACTIVE", true)).toBe(false);
  });

  it("detects busy states and work in progress", () => {
    expect(isBusy("UPDATING")).toBe(true);
    expect(isBusy("ACTIVE")).toBe(false);
    expect(inProgress([{ status: "SUCCEEDED" }, { status: "IN_PROGRESS" }])).toBe(true);
    expect(inProgress([{ status: "SUCCEEDED" }, { status: "FAILED" }])).toBe(false);
  });
});

describe("create form", () => {
  const shape = parseShape({
    name: "CI.Standard.E4.Flex",
    ocpuOptions: { min: 1, max: 4 },
    memoryOptions: { minInGBs: 1, maxInGBs: 64, defaultPerOcpuInGBs: 16, maxPerOcpuInGBs: 16 },
  });
  const base: NewInstance = {
    compartmentId: comp,
    availabilityDomain: "AD-1",
    displayName: " web ",
    shape: shape.name,
    ocpus: 1,
    memoryInGBs: 8,
    restartPolicy: "ALWAYS",
    subnetId: " ocid1.subnet.oc1..s ",
    image: " alpine:3.20 ",
    containerName: "",
    command: "sh",
    args: `-c 'echo hello; sleep 3600'`,
    env: [
      { name: "TOKEN", value: "s3cret" },
      { name: " ", value: "ignored" },
    ],
  };

  it("splits command lines with quotes", () => {
    expect(splitArgs(`-c 'echo hello; sleep 3600'`)).toEqual(["-c", "echo hello; sleep 3600"]);
    expect(splitArgs(`a "b c" ''  d`)).toEqual(["a", "b c", "", "d"]);
    expect(splitArgs("")).toEqual([]);
  });

  it("builds the request body from fixed fields", () => {
    expect(createBody(base)).toEqual({
      compartmentId: comp,
      availabilityDomain: "AD-1",
      shape: shape.name,
      displayName: "web",
      shapeConfig: { ocpus: 1, memoryInGBs: 8 },
      containerRestartPolicy: "ALWAYS",
      vnics: [{ subnetId: "ocid1.subnet.oc1..s" }],
      containers: [
        {
          imageUrl: "alpine:3.20",
          command: ["sh"],
          arguments: ["-c", "echo hello; sleep 3600"],
          environmentVariables: { TOKEN: "s3cret" },
        },
      ],
    });
  });

  it("validates ranges and required fields", () => {
    expect(validateNew(base, shape)).toBe("");
    expect(validateNew(base, void 0)).toMatch(/shape/i);
    expect(validateNew({ ...base, image: " " }, shape)).toMatch(/image/i);
    expect(validateNew({ ...base, subnetId: "" }, shape)).toMatch(/subnet/i);
    expect(validateNew({ ...base, availabilityDomain: "" }, shape)).toMatch(/availability/i);
    expect(validateNew({ ...base, ocpus: 5 }, shape)).toMatch(/OCPUs/);
    expect(validateNew({ ...base, memoryInGBs: 17 }, shape)).toMatch(
      /Memory must be between 1 and 16/,
    );
    expect(validateNew({ ...base, env: [{ name: "a b", value: "" }] }, shape)).toMatch(
      /environment/i,
    );
  });
});

describe("requests", () => {
  it("lists instances across pages and hides deleted ones", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(json({ items: [instRaw] }, 200, { "opc-next-page": "p2" }))
      .mockResolvedValueOnce(
        json({ items: [{ ...instRaw, id: "ci2", lifecycleState: "DELETED" }] }),
      );
    const out = await listInstances(comp, asFetch(f));
    expect(out.map((i) => i.id)).toEqual(["ci1"]);
    expect(f.mock.calls[0][0]).toBe(
      `${CI_PROXY_BASE}/api/containerInstances?compartmentId=${encodeURIComponent(comp)}`,
    );
    expect(f.mock.calls[1][0]).toContain("page=p2");
  });

  it("reads one instance", async () => {
    const f = vi.fn().mockResolvedValue(json(instRaw));
    expect((await getInstance("ci1", asFetch(f))).name).toBe("web");
    expect(f.mock.calls[0][0]).toBe(`${CI_PROXY_BASE}/api/containerInstances/ci1`);
  });

  it("lists containers with their full record", async () => {
    const f = vi.fn((url: string) =>
      Promise.resolve(
        url.includes("/api/containers/c1") ? json(ctrRaw) : json({ items: [{ id: "c1" }] }),
      ),
    );
    const out = await listContainers(comp, "ci1", asFetch(f));
    expect(out[0].envNames).toEqual(["B", "A"]);
    expect(f.mock.calls[0][0]).toContain("containerInstanceId=ci1");
  });

  it("lists work requests for the instance and their errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValueOnce(
        json({
          items: [
            {
              id: "w1",
              operationType: "CREATE_CONTAINER_INSTANCE",
              status: "FAILED",
              percentComplete: 50,
              resources: [{ actionType: "CREATED", entityType: "containerInstance" }],
            },
          ],
        }),
      )
      .mockResolvedValueOnce(
        json({ items: [{ code: "ImagePull", message: "denied", timestamp: "t" }] }),
      );
    const wrs = await listWorkRequests(comp, "ci1", asFetch(f));
    expect(wrs[0]).toMatchObject({
      status: "FAILED",
      percent: 50,
      resources: "CREATED containerInstance",
    });
    expect(f.mock.calls[0][0]).toContain("resourceId=ci1");
    expect(await listWorkRequestErrors("w1", asFetch(f))).toEqual([
      { code: "ImagePull", message: "denied", timestamp: "t" },
    ]);
  });

  it("lists shapes, subnets, domains and the engine", async () => {
    const f = vi.fn((url: string) => {
      if (url.includes("containerInstanceShapes"))
        return Promise.resolve(json({ items: [{ name: "S" }] }));
      if (url.includes("/subnets"))
        return Promise.resolve(
          json({ items: [{ id: "s1", displayName: "a", cidrBlock: "10.0.0.0/24", vcnId: "v" }] }),
        );
      if (url.includes("/availabilitydomains"))
        return Promise.resolve(json({ items: [{ name: "AD-1" }] }));
      return Promise.resolve(json({ engine: "docker" }));
    });
    expect((await listShapes(comp, asFetch(f)))[0].name).toBe("S");
    expect(await listSubnets(comp, asFetch(f))).toEqual([
      { id: "s1", name: "a", cidr: "10.0.0.0/24", vcnId: "v" },
    ]);
    expect(await listAvailabilityDomains(asFetch(f))).toEqual(["AD-1"]);
    expect(await getEngine(asFetch(f))).toBe("docker");
  });

  it("sends actions, rename, delete and create", async () => {
    const f = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({}, 202)));
    await instanceAction("ci1", "stop", asFetch(f));
    await renameInstance("ci1", "new", asFetch(f));
    await deleteInstance("ci1", asFetch(f));
    await createInstance(
      {
        compartmentId: comp,
        availabilityDomain: "AD",
        displayName: "",
        shape: "S",
        ocpus: 1,
        memoryInGBs: 2,
        restartPolicy: "NEVER",
        subnetId: "s",
        image: "i",
        containerName: "",
        command: "",
        args: "",
        env: [],
      },
      asFetch(f),
    );
    expect(f.mock.calls.map((c) => [c[1]?.method, c[0]])).toEqual([
      ["POST", `${CI_PROXY_BASE}/api/containerInstances/ci1/actions/stop`],
      ["PUT", `${CI_PROXY_BASE}/api/containerInstances/ci1`],
      ["DELETE", `${CI_PROXY_BASE}/api/containerInstances/ci1`],
      ["POST", `${CI_PROXY_BASE}/api/containerInstances`],
    ]);
    expect(JSON.parse(String(f.mock.calls[1][1]?.body))).toEqual({ displayName: "new" });
  });

  it("surfaces proxy errors", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(
        json({ error: { code: 503, status: "UNAVAILABLE", message: "not enabled" } }, 503),
      );
    await expect(listInstances(comp, asFetch(f))).rejects.toMatchObject({
      httpStatus: 503,
      message: "not enabled",
    });
  });
});

describe("logs", () => {
  it("returns text with the truncation headers", async () => {
    const f = vi.fn().mockResolvedValue(
      new Response("<b>hello</b>\n", {
        status: 200,
        headers: { "X-Logs-Truncated": "true", "X-Logs-Total-Bytes": "999" },
      }),
    );
    const r = await retrieveLogs("c1", true, asFetch(f));
    expect(r).toEqual({ text: "<b>hello</b>\n", truncated: true, totalBytes: 999 });
    expect(f.mock.calls[0][0]).toBe(
      `${CI_PROXY_BASE}/api/containers/c1/actions/retrieveLogs?isPrevious=true`,
    );
    expect(f.mock.calls[0][1]).toMatchObject({ method: "POST" });
  });

  it("raises the state-engine 409 message", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(
        json({ code: "IncorrectState", message: "container logs need the docker engine" }, 409),
      );
    await expect(retrieveLogs("c1", false, asFetch(f))).rejects.toMatchObject({
      httpStatus: 409,
      code: "IncorrectState",
      message: "container logs need the docker engine",
    });
  });

  it("falls back to the status text for a non-JSON failure", async () => {
    const f = vi
      .fn()
      .mockResolvedValue(new Response("boom", { status: 502, statusText: "Bad Gateway" }));
    await expect(retrieveLogs("c1", false, asFetch(f))).rejects.toMatchObject({
      httpStatus: 502,
      message: "Bad Gateway",
    });
  });
});
