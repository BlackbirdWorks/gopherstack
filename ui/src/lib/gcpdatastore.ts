/** Client for the Datastore emulator (namespaces, kinds, entities, GQL, indexes) via the dashboard's in-process REST proxy. */

import { restCall, str, type FetchFn, type Json } from "./gcprest.ts";

export const DATASTORE_PROXY_BASE = "/dashboard/api/v1/gcp/datastore/v1";
export const DS_PROJECT_KEY = "gopherstack_datastore_project";
export const DEFAULT_DS_PROJECT = "gopherstack-local";
export const DEFAULT_PAGE_SIZE = 50;

const enc = encodeURIComponent;
const list = (v: unknown): Json[] => (Array.isArray(v) ? (v as Json[]) : []);
const obj = (v: unknown): Json => (v && typeof v === "object" ? (v as Json) : {});

export type PathElement = { kind: string; id: string; name: string };

export type DsKey = { namespace: string; path: PathElement[] };

export type DsProperty = {
  name: string;
  type: string;
  display: string;
  excludeFromIndexes: boolean;
  meaning: number;
};

export type DsEntity = { key: DsKey; label: string; properties: DsProperty[]; version: string };

export type QueryPage = { entities: DsEntity[]; endCursor: string; moreResults: string };

export type IndexInfo = {
  id: string;
  kind: string;
  ancestor: string;
  state: string;
  properties: { name: string; direction: string }[];
};

export function parseKey(raw: unknown): DsKey {
  const k = obj(raw);
  return {
    namespace: str(obj(k.partitionId).namespaceId),
    path: list(k.path).map((p) => ({ kind: str(p.kind), id: str(p.id), name: str(p.name) })),
  };
}

/** Human form of a key: Kind("name") / Kind(123), prefixed with its namespace when set. */
export function formatKey(key: DsKey): string {
  const path = key.path.map((p) => (p.name ? `${p.kind}("${p.name}")` : `${p.kind}(${p.id})`));
  const text = path.join(" / ");
  return key.namespace ? `[${key.namespace}] ${text}` : text;
}

const VALUE_TYPES: Record<string, string> = {
  nullValue: "null",
  booleanValue: "boolean",
  integerValue: "integer",
  doubleValue: "double",
  timestampValue: "timestamp",
  keyValue: "key",
  stringValue: "string",
  blobValue: "blob",
  geoPointValue: "geopoint",
  entityValue: "entity",
  arrayValue: "array",
};

export function valueType(v: Json): string {
  for (const field of Object.keys(VALUE_TYPES))
    if (field in v) return VALUE_TYPES[field] ?? "unknown";
  return "unknown";
}

/** Short text for a Datastore JSON Value. */
export function formatValue(raw: unknown): string {
  const v = obj(raw);
  switch (valueType(v)) {
    case "null":
      return "null";
    case "key":
      return formatKey(parseKey(v.keyValue));
    case "geopoint": {
      const g = obj(v.geoPointValue);
      return `${String(g.latitude ?? 0)}, ${String(g.longitude ?? 0)}`;
    }
    case "entity": {
      const props = obj(obj(v.entityValue).properties);
      return `{${Object.entries(props)
        .map(([k, x]) => `${k}: ${formatValue(x)}`)
        .join(", ")}}`;
    }
    case "array":
      return `[${list(obj(v.arrayValue).values)
        .map((x) => formatValue(x))
        .join(", ")}]`;
    case "blob":
      return `blob(${str(v.blobValue).length} base64 chars)`;
    case "unknown":
      return "";
    default:
      return String(Object.values(v).find((x) => typeof x !== "object" && x !== undefined) ?? "");
  }
}

export function parseEntity(raw: Json): DsEntity {
  const e = obj(raw.entity);
  const key = parseKey(e.key);
  const props = obj(e.properties);
  return {
    key,
    label: formatKey(key),
    version: str(raw.version),
    properties: Object.entries(props)
      .toSorted(([a], [b]) => a.localeCompare(b))
      .map(([name, value]) => {
        const v = obj(value);
        return {
          name,
          type: valueType(v),
          display: formatValue(v),
          excludeFromIndexes: v.excludeFromIndexes === true,
          meaning: typeof v.meaning === "number" ? v.meaning : 0,
        };
      }),
  };
}

function parsePage(body: Json): QueryPage {
  const batch = obj(body.batch);
  return {
    entities: list(batch.entityResults).map((r) => parseEntity(r)),
    endCursor: str(batch.endCursor),
    moreResults: str(batch.moreResults),
  };
}

const projectPath = (project: string, verb: string) => `/projects/${enc(project)}:${verb}`;

const partition = (project: string, namespace: string): Json => ({
  projectId: project,
  ...(namespace ? { namespaceId: namespace } : {}),
});

/** Namespaces: the empty string stands for the default namespace. */
export async function listNamespaces(project: string, fetchFn: FetchFn = fetch): Promise<string[]> {
  const body = await restCall(
    DATASTORE_PROXY_BASE,
    "POST",
    projectPath(project, "runQuery"),
    { partitionId: { projectId: project }, query: { kind: [{ name: "__namespace__" }] } },
    fetchFn,
  );
  const names = parsePage(body).entities.map((e) => e.key.path[0]?.name ?? "");
  return [...new Set(names)].toSorted();
}

export async function listKinds(
  project: string,
  namespace: string,
  fetchFn: FetchFn = fetch,
): Promise<string[]> {
  const body = await restCall(
    DATASTORE_PROXY_BASE,
    "POST",
    projectPath(project, "runQuery"),
    { partitionId: partition(project, namespace), query: { kind: [{ name: "__kind__" }] } },
    fetchFn,
  );
  return parsePage(body)
    .entities.map((e) => e.key.path[0]?.name ?? "")
    .filter(Boolean)
    .toSorted();
}

export async function listEntities(
  project: string,
  namespace: string,
  kind: string,
  startCursor = "",
  limit = DEFAULT_PAGE_SIZE,
  fetchFn: FetchFn = fetch,
): Promise<QueryPage> {
  const query: Json = { kind: [{ name: kind }], limit };
  if (startCursor) query.startCursor = startCursor;
  return parsePage(
    await restCall(
      DATASTORE_PROXY_BASE,
      "POST",
      projectPath(project, "runQuery"),
      { partitionId: partition(project, namespace), query },
      fetchFn,
    ),
  );
}

export async function runGql(
  project: string,
  namespace: string,
  gql: string,
  fetchFn: FetchFn = fetch,
): Promise<QueryPage> {
  return parsePage(
    await restCall(
      DATASTORE_PROXY_BASE,
      "POST",
      projectPath(project, "runQuery"),
      {
        partitionId: partition(project, namespace),
        gqlQuery: { queryString: gql.trim(), allowLiterals: true },
      },
      fetchFn,
    ),
  );
}

export async function listIndexes(project: string, fetchFn: FetchFn = fetch): Promise<IndexInfo[]> {
  const out: IndexInfo[] = [];
  let token = "";
  do {
    const qs = token ? `?pageToken=${enc(token)}` : "";
    const body = await restCall(
      DATASTORE_PROXY_BASE,
      "GET",
      `/projects/${enc(project)}/indexes${qs}`,
      undefined,
      fetchFn,
    );
    for (const i of list(body.indexes)) {
      out.push({
        id: str(i.indexId),
        kind: str(i.kind),
        ancestor: str(i.ancestor),
        state: str(i.state),
        properties: list(i.properties).map((p) => ({
          name: str(p.name),
          direction: str(p.direction),
        })),
      });
    }
    token = str(body.nextPageToken);
  } while (token);
  return out;
}

export const nsLabel = (ns: string) => ns || "(default)";

function readStored(key: string, fallback: string): string {
  try {
    return localStorage.getItem(key)?.trim() || fallback;
  } catch {
    return fallback;
  }
}

export const readStoredDsProject = () => readStored(DS_PROJECT_KEY, DEFAULT_DS_PROJECT);

export function writeStoredDsProject(project: string): void {
  try {
    localStorage.setItem(DS_PROJECT_KEY, project);
  } catch {
    // storage unavailable: scope applies for this session only
  }
}
