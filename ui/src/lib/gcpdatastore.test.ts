import { describe, expect, it, vi } from "vitest";

import {
  DATASTORE_PROXY_BASE,
  formatKey,
  formatValue,
  listEntities,
  listIndexes,
  listKinds,
  listNamespaces,
  nsLabel,
  parseEntity,
  parseKey,
  runGql,
  valueType,
} from "./gcpdatastore";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;

const key = (kind: string, name: string, ns = "") => ({
  partitionId: { projectId: "p", ...(ns ? { namespaceId: ns } : {}) },
  path: [{ kind, name }],
});
const bodyOf = (call: unknown[] | undefined): Record<string, unknown> =>
  JSON.parse(String((call?.[1] as RequestInit | undefined)?.body)) as Record<string, unknown>;

describe("values and keys", () => {
  it("formats keys with names, ids and namespaces", () => {
    expect(formatKey(parseKey(key("Task", "t1")))).toBe('Task("t1")');
    expect(
      formatKey(
        parseKey({
          partitionId: { namespaceId: "tenant" },
          path: [
            { kind: "User", id: "7" },
            { kind: "Task", name: "t" },
          ],
        }),
      ),
    ).toBe('[tenant] User(7) / Task("t")');
  });

  it.each([
    [{ stringValue: "hi" }, "string", "hi"],
    [{ integerValue: "42" }, "integer", "42"],
    [{ doubleValue: 1.5 }, "double", "1.5"],
    [{ booleanValue: false }, "boolean", "false"],
    [{ nullValue: "NULL_VALUE" }, "null", "null"],
    [{ timestampValue: "2026-01-01T00:00:00Z" }, "timestamp", "2026-01-01T00:00:00Z"],
    [{ geoPointValue: { latitude: 1, longitude: 2 } }, "geopoint", "1, 2"],
    [{ keyValue: key("Task", "t") }, "key", 'Task("t")'],
    [{ arrayValue: { values: [{ stringValue: "a" }, { integerValue: "2" }] } }, "array", "[a, 2]"],
    [{ entityValue: { properties: { x: { stringValue: "y" } } } }, "entity", "{x: y}"],
  ])("formats %j", (v, type, text) => {
    expect(valueType(v)).toBe(type);
    expect(formatValue(v)).toBe(text);
  });

  it("parses an entity with excluded-from-indexes properties", () => {
    const e = parseEntity({
      version: "3",
      entity: {
        key: key("Task", "t1"),
        properties: {
          title: { stringValue: "write" },
          note: { stringValue: "n", excludeFromIndexes: true },
        },
      },
    });
    expect(e.label).toBe('Task("t1")');
    expect(e.properties.map((p) => [p.name, p.excludeFromIndexes])).toEqual([
      ["note", true],
      ["title", false],
    ]);
  });

  it("labels the default namespace", () => {
    expect(nsLabel("")).toBe("(default)");
    expect(nsLabel("tenant")).toBe("tenant");
  });
});

const entityResults = (names: string[]) =>
  names.map((n) => ({ entity: { key: key("Task", n), properties: {} } }));

describe("requests", () => {
  it("lists namespaces from the __namespace__ metadata kind", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        batch: {
          entityResults: [
            { entity: { key: { path: [{ kind: "__namespace__", id: "1" }] } } },
            { entity: { key: { path: [{ kind: "__namespace__", name: "tenant" }] } } },
          ],
        },
      }),
    );
    expect(await listNamespaces("p", asFetch(fetchFn))).toEqual(["", "tenant"]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(`${DATASTORE_PROXY_BASE}/projects/p:runQuery`);
  });

  it("lists kinds in a namespace", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        batch: {
          entityResults: [
            { entity: { key: { path: [{ kind: "__kind__", name: "Task" }] } } },
            { entity: { key: { path: [{ kind: "__kind__", name: "Note" }] } } },
          ],
        },
      }),
    );
    expect(await listKinds("p", "tenant", asFetch(fetchFn))).toEqual(["Note", "Task"]);
    expect(bodyOf(fetchFn.mock.calls[0]).partitionId).toEqual({
      projectId: "p",
      namespaceId: "tenant",
    });
  });

  it("pages entities with the end cursor", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        batch: {
          entityResults: entityResults(["a"]),
          endCursor: "c1",
          moreResults: "NOT_FINISHED",
        },
      }),
    );
    const page = await listEntities("p", "", "Task", "c0", 10, asFetch(fetchFn));
    expect(page).toMatchObject({ endCursor: "c1", moreResults: "NOT_FINISHED" });
    expect(bodyOf(fetchFn.mock.calls[0])).toEqual({
      partitionId: { projectId: "p" },
      query: { kind: [{ name: "Task" }], limit: 10, startCursor: "c0" },
    });
  });

  it("runs GQL with literals allowed and reports query errors", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(json({ batch: { entityResults: entityResults(["a", "b"]) } }))
      .mockResolvedValueOnce(
        json({ error: { status: "INVALID_ARGUMENT", message: "bad GQL" } }, 400),
      );
    const ok = await runGql("p", "", "  SELECT * FROM Task ", asFetch(fetchFn));
    expect(ok.entities).toHaveLength(2);
    expect(bodyOf(fetchFn.mock.calls[0]).gqlQuery).toEqual({
      queryString: "SELECT * FROM Task",
      allowLiterals: true,
    });
    await expect(runGql("p", "", "SELEKT", asFetch(fetchFn))).rejects.toMatchObject({
      code: "INVALID_ARGUMENT",
      message: "bad GQL",
    });
  });

  it("lists indexes across pages", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValueOnce(
        json({
          indexes: [
            {
              indexId: "1",
              kind: "Task",
              ancestor: "NONE",
              state: "READY",
              properties: [{ name: "title", direction: "ASCENDING" }],
            },
          ],
          nextPageToken: "n",
        }),
      )
      .mockResolvedValueOnce(
        json({ indexes: [{ indexId: "2", kind: "Note", state: "CREATING" }] }),
      );
    const idx = await listIndexes("p", asFetch(fetchFn));
    expect(idx.map((i) => [i.id, i.kind, i.state])).toEqual([
      ["1", "Task", "READY"],
      ["2", "Note", "CREATING"],
    ]);
    expect(fetchFn.mock.calls[1]?.[0]).toBe(
      `${DATASTORE_PROXY_BASE}/projects/p/indexes?pageToken=n`,
    );
  });
});
