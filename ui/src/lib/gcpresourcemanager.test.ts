import { describe, expect, it, vi } from "vitest";

import {
  addMember,
  buildProjectBody,
  createFolder,
  deleteNode,
  disableService,
  enableService,
  filterServices,
  flatten,
  getEnforcement,
  getIamPolicy,
  groupBindings,
  listOrphanProjects,
  listServices,
  listTagBindings,
  listTagKeys,
  loadTree,
  memberKind,
  moveNode,
  nodePath,
  orphanRoot,
  parentOptions,
  parseFolder,
  parseOrganization,
  parsePolicy,
  parseProject,
  parseService,
  removeMember,
  statePill,
  undeleteNode,
  validFolderName,
  validMember,
  validProjectId,
  validProjectName,
  validRole,
  type RmNode,
} from "./gcpresourcemanager";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;
const urlOf = (f: { mock: { calls: unknown[][] } }, i: number): string =>
  String((f.mock.calls[i] ?? [])[0]);
const initOf = (f: { mock: { calls: unknown[][] } }, i: number): RequestInit =>
  (f.mock.calls[i] ?? [])[1] as RequestInit;

const org = parseOrganization({ name: "organizations/1", displayName: "gopherstack.local" });
const project = parseProject({
  name: "projects/123456789012",
  projectId: "alpha-proj",
  displayName: "Alpha",
  parent: "folders/10",
  state: "DELETE_REQUESTED",
  deleteTime: "2026-01-02T00:00:00Z",
  labels: { env: "dev" },
});

describe("parsers", () => {
  it("parses organizations, folders and projects", () => {
    expect(org).toMatchObject({ kind: "organization", id: "1", label: "gopherstack.local" });
    expect(
      parseFolder({ name: "folders/10", displayName: "Team", parent: "organizations/1" }),
    ).toMatchObject({
      kind: "folder",
      id: "10",
      state: "ACTIVE",
      parent: "organizations/1",
    });
    expect(project).toMatchObject({
      kind: "project",
      id: "alpha-proj",
      number: "123456789012",
      state: "DELETE_REQUESTED",
      labels: { env: "dev" },
    });
  });

  it("parses services and policies", () => {
    expect(
      parseService({
        name: "projects/1/services/pubsub.googleapis.com",
        config: {
          name: "pubsub.googleapis.com",
          title: "Pub/Sub",
          documentation: { summary: "s" },
        },
        state: "ENABLED",
      }),
    ).toEqual({
      name: "projects/1/services/pubsub.googleapis.com",
      id: "pubsub.googleapis.com",
      title: "Pub/Sub",
      summary: "s",
      state: "ENABLED",
    });
    const pol = parsePolicy({
      etag: "BwX=",
      bindings: [
        { role: "roles/viewer", members: ["user:a@x.io"] },
        { role: "roles/editor", members: ["group:g@x.io"], condition: { title: "temp" } },
      ],
    });
    expect(pol.etag).toBe("BwX=");
    expect(groupBindings(pol).map((b) => b.role)).toEqual(["roles/viewer", "roles/editor"]);
    expect(pol.bindings[1]?.condition).toBe("temp");
  });
});

describe("validation", () => {
  it.each([
    ["alpha-proj", true],
    ["ab", false],
    ["Alpha-proj", false],
    ["1alpha-proj", false],
    ["alpha-proj-", false],
    ["my-google-project", false],
    ["a".repeat(31), false],
  ])("project id %s", (v, ok) => {
    expect(validProjectId(v)).toBe(ok);
  });

  it.each([
    ["Team Alpha", true],
    ["ab", false],
    [" Team", false],
    ["Team-1_x", true],
    ["Team!", false],
  ])("folder name %s", (v, ok) => {
    expect(validFolderName(v)).toBe(ok);
  });

  it("project names, roles and members", () => {
    expect(validProjectName("")).toBe(true);
    expect(validProjectName("abc")).toBe(false);
    expect(validProjectName("My Project!")).toBe(true);
    expect(validRole("roles/viewer")).toBe(true);
    expect(validRole("projects/p/roles/custom.one")).toBe(true);
    expect(validRole("viewer")).toBe(false);
    expect(validMember("user:a@x.io")).toBe(true);
    expect(validMember("user: a")).toBe(false);
    expect(memberKind("serviceAccount:x@y")).toBe("serviceAccount");
    expect(memberKind("allUsers")).toBe("allUsers");
  });

  it("state pills", () => {
    expect(statePill("DELETE_REQUESTED")).toContain("red");
    expect(statePill("ACTIVE")).toContain("green");
    expect(statePill("")).toContain("gray");
  });
});

describe("tree", () => {
  const folder: RmNode = {
    ...parseFolder({ name: "folders/10", displayName: "Team", parent: "organizations/1" }),
  };
  const dead: RmNode = {
    ...parseFolder({ name: "folders/11", displayName: "Old", state: "DELETE_REQUESTED" }),
  };
  const root: RmNode = { ...org, children: [{ ...folder, children: [{ ...project }] }, dead] };

  it("flattens and builds parent options without deleted nodes or the skipped subtree", () => {
    expect(flatten(root).map((n) => n.name)).toEqual([
      "organizations/1",
      "folders/10",
      "projects/123456789012",
      "folders/11",
    ]);
    expect(parentOptions(root)).toEqual([
      { name: "organizations/1", label: "gopherstack.local" },
      { name: "folders/10", label: "gopherstack.local / Team" },
    ]);
    expect(parentOptions(root, "folders/10").map((o) => o.name)).toEqual(["organizations/1"]);
  });

  it("orphan root is unselectable", () => {
    expect(orphanRoot([project]).name).toBe("");
    expect(orphanRoot([project]).children).toHaveLength(1);
  });

  it("loads the subtree recursively", async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes("/folders?parent=organizations%2F1")) {
        return Promise.resolve(json({ folders: [{ name: "folders/10", displayName: "Team" }] }));
      }
      if (url.includes("/projects?parent=folders%2F10")) {
        return Promise.resolve(json({ projects: [{ name: "projects/5", projectId: "p-five" }] }));
      }
      return Promise.resolve(json({}));
    });
    const tree = await loadTree(org, asFetch(fetchMock));
    expect(tree.children.map((c) => c.id)).toEqual(["10"]);
    expect(tree.children[0]?.children.map((c) => c.id)).toEqual(["p-five"]);
  });

  it("keeps only parentless projects", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(
        json({
          projects: [
            { name: "projects/1", projectId: "solo-proj" },
            { name: "projects/2", parent: "folders/1" },
          ],
        }),
      ),
    );
    const got = await listOrphanProjects(asFetch(fetchMock));
    expect(got.map((p) => p.id)).toEqual(["solo-proj"]);
  });
});

describe("services", () => {
  const all = [
    {
      name: "n1",
      id: "pubsub.googleapis.com",
      title: "Pub/Sub",
      summary: "messaging",
      state: "ENABLED",
    },
    {
      name: "n2",
      id: "run.googleapis.com",
      title: "Cloud Run",
      summary: "containers",
      state: "DISABLED",
    },
  ];

  it("filters by text and state", () => {
    expect(filterServices(all, "run", "").map((s) => s.id)).toEqual(["run.googleapis.com"]);
    expect(filterServices(all, "", "ENABLED").map((s) => s.id)).toEqual(["pubsub.googleapis.com"]);
    expect(filterServices(all, "MESSAGING", "DISABLED")).toEqual([]);
  });

  it("lists with a state filter and pages", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        json({ services: [{ name: "a", state: "ENABLED" }], nextPageToken: "t2" }),
      )
      .mockResolvedValueOnce(json({ services: [{ name: "b", state: "ENABLED" }] }));
    const got = await listServices("p1", "ENABLED", asFetch(fetchMock));
    expect(got).toHaveLength(2);
    expect(urlOf(fetchMock, 0)).toContain("/projects/p1/services?pageSize=200&state=ENABLED");
    expect(urlOf(fetchMock, 1)).toContain("pageToken=t2");
  });

  it("enables and disables with fixed bodies", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({})));
    await enableService("p1", "pubsub.googleapis.com", asFetch(fetchMock));
    await disableService("p1", "pubsub.googleapis.com", true, asFetch(fetchMock));
    expect(urlOf(fetchMock, 0)).toMatch(
      /\/projects\/p1\/services\/pubsub\.googleapis\.com:enable$/,
    );
    expect(urlOf(fetchMock, 1)).toMatch(/:disable$/);
    expect(JSON.parse(String(initOf(fetchMock, 1).body))).toEqual({
      disableDependentServices: true,
    });
  });

  it("surfaces the precondition message", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(
        json({ error: { message: "depends on this", status: "FAILED_PRECONDITION" } }, 400),
      ),
    );
    await expect(
      disableService("p1", "compute.googleapis.com", false, asFetch(fetchMock)),
    ).rejects.toMatchObject({
      code: "FAILED_PRECONDITION",
      message: "depends on this",
    });
  });
});

describe("mutations", () => {
  it("builds project bodies from fixed fields", () => {
    expect(
      buildProjectBody({
        projectId: " my-proj-1 ",
        displayName: "",
        parent: "folders/1",
        labels: [
          { key: "a", value: "b" },
          { key: " ", value: "x" },
        ],
      }),
    ).toEqual({ projectId: "my-proj-1", parent: "folders/1", labels: { a: "b" } });
  });

  it("calls the expected verbs", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({})));
    const f = asFetch(fetchMock);
    await createFolder("organizations/1", "Team", f);
    await moveNode(project, "organizations/1", f);
    await deleteNode(project, f);
    await undeleteNode(project, f);
    expect(initOf(fetchMock, 0).method).toBe("POST");
    expect(JSON.parse(String(initOf(fetchMock, 0).body))).toEqual({
      parent: "organizations/1",
      displayName: "Team",
    });
    expect(urlOf(fetchMock, 1)).toMatch(/\/projects\/alpha-proj:move$/);
    expect(initOf(fetchMock, 2).method).toBe("DELETE");
    expect(urlOf(fetchMock, 3)).toMatch(/:undelete$/);
    expect(nodePath(org)).toBe("organizations/1");
  });
});

describe("iam", () => {
  it("reads and edits with the etag", async () => {
    const fetchMock = vi.fn(() => Promise.resolve(json({ etag: "e2", bindings: [] })));
    const f = asFetch(fetchMock);
    await getIamPolicy(project, f);
    const next = await addMember(project, "roles/viewer", "user:a@x.io", "e1", f);
    await removeMember(project, "roles/viewer", "user:a@x.io", "e2", f);
    expect(urlOf(fetchMock, 0)).toMatch(/:getIamPolicy$/);
    expect(JSON.parse(String(initOf(fetchMock, 1).body))).toEqual({
      role: "roles/viewer",
      member: "user:a@x.io",
      etag: "e1",
    });
    expect(urlOf(fetchMock, 2)).toMatch(/:removeMember$/);
    expect(next.etag).toBe("e2");
  });

  it("passes an etag conflict through", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(json({ error: { message: "policy changed", status: "ABORTED" } }, 409)),
    );
    await expect(
      addMember(project, "roles/viewer", "user:a", "old", asFetch(fetchMock)),
    ).rejects.toMatchObject({
      httpStatus: 409,
      code: "ABORTED",
    });
  });
});

describe("tags and enforcement", () => {
  it("lists keys with their values and bindings across parent forms", async () => {
    const fetchMock = vi.fn((url: string) => {
      if (url.includes("/tagKeys"))
        return Promise.resolve(json({ tagKeys: [{ name: "tagKeys/1", shortName: "env" }] }));
      if (url.includes("/tagValues"))
        return Promise.resolve(json({ tagValues: [{ name: "tagValues/2", shortName: "prod" }] }));
      return Promise.resolve(json({ tagBindings: [{ name: "b1", tagValue: "tagValues/2" }] }));
    });
    const f = asFetch(fetchMock);
    const keys = await listTagKeys("organizations/1", f);
    expect(keys[0]?.values.map((v) => v.shortName)).toEqual(["prod"]);
    const bindings = await listTagBindings(["//x/projects/a", "//x/projects/1"], f);
    expect(bindings).toHaveLength(2);
  });

  it("reads the enforcement switches", async () => {
    const fetchMock = vi.fn(() =>
      Promise.resolve(json({ projects: true, serviceEnablement: false })),
    );
    expect(await getEnforcement(asFetch(fetchMock))).toEqual({
      projects: true,
      serviceEnablement: false,
    });
  });
});
