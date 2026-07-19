import type { AttributeValue, KeySchemaElement, TableDescription } from "@aws-sdk/client-dynamodb";

/** Convert a single DynamoDB AttributeValue to a plain JS value. */
export function avToJson(av: AttributeValue): unknown {
  if (av.S !== undefined) return av.S;
  if (av.N !== undefined) return Number(av.N);
  if (av.BOOL !== undefined) return av.BOOL;
  if (av.NULL) return null;
  if (av.L) return av.L.map((item) => avToJson(item));
  if (av.M) {
    const obj: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(av.M)) obj[k] = avToJson(v);
    return obj;
  }
  if (av.SS) return av.SS;
  if (av.NS) return av.NS?.map(Number);
  if (av.B) return "(binary)";
  if (av.BS) return "(binary set)";
  return JSON.stringify(av);
}

/** Convert a DynamoDB item (map of AttributeValues) to a plain JS object. */
export function itemToJson(item: Record<string, AttributeValue>): Record<string, unknown> {
  const obj: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(item)) obj[k] = avToJson(v);
  return obj;
}

/** Convert a plain JS value to a DynamoDB AttributeValue. */
export function jsonToAv(val: unknown): AttributeValue {
  if (typeof val === "string") return { S: val };
  if (typeof val === "number") return { N: String(val) };
  if (typeof val === "boolean") return { BOOL: val };
  if (val === null || val === undefined) return { NULL: true };
  if (Array.isArray(val)) return { L: val.map((item) => jsonToAv(item)) };
  if (typeof val === "object") {
    const m: Record<string, AttributeValue> = {};
    for (const [k, v] of Object.entries(val as Record<string, unknown>)) m[k] = jsonToAv(v);
    return { M: m };
  }
  return { S: String(val) };
}

/** Convert a plain JS object to a DynamoDB item. */
export function jsonToItem(json: Record<string, unknown>): Record<string, AttributeValue> {
  const item: Record<string, AttributeValue> = {};
  for (const [k, v] of Object.entries(json)) item[k] = jsonToAv(v);
  return item;
}

/** Extract unique column names from a list of plain objects. */
export function getColumns(items: Record<string, unknown>[]): string[] {
  const cols = new Set<string>();
  for (const item of items) for (const key of Object.keys(item)) cols.add(key);
  return Array.from(cols);
}

/** Format a table's key schema for display. */
export function getKeySchema(desc?: TableDescription): string {
  if (!desc?.KeySchema) return "";
  return desc.KeySchema.map((k) => `${k.AttributeName} (${k.KeyType})`).join(", ");
}

export interface ResolvedKeySchema {
  pkName: string;
  skName: string;
  pkType: string;
  skType: string;
}

/**
 * Pick the KeySchema to use: the named GSI's, the named LSI's, or (if no
 * index name is given, or it doesn't match any GSI/LSI) the base table's.
 */
function pickKeySchema(desc: TableDescription, indexName?: string): KeySchemaElement[] {
  const baseKeySchema = desc.KeySchema ?? [];
  if (!indexName) return baseKeySchema;
  const gsi = (desc.GlobalSecondaryIndexes ?? []).find((i) => i.IndexName === indexName);
  if (gsi) return gsi.KeySchema ?? [];
  const lsi = (desc.LocalSecondaryIndexes ?? []).find((i) => i.IndexName === indexName);
  if (lsi) return lsi.KeySchema ?? [];
  return baseKeySchema;
}

/** Extract the HASH/RANGE attribute names from a KeySchema. */
function keyNamesFromSchema(keySchema: KeySchemaElement[]): { pkName: string; skName: string } {
  let pkName = "",
    skName = "";
  for (const k of keySchema) {
    if (k.KeyType === "HASH") pkName = k.AttributeName ?? "";
    if (k.KeyType === "RANGE") skName = k.AttributeName ?? "";
  }
  return { pkName, skName };
}

/** Look up the attribute types for the given key names, defaulting to "S". */
function keyTypesFromAttributeDefinitions(
  desc: TableDescription,
  pkName: string,
  skName: string,
): { pkType: string; skType: string } {
  let pkType = "S",
    skType = "S";
  for (const attr of desc.AttributeDefinitions ?? []) {
    if (attr.AttributeName === pkName) pkType = attr.AttributeType ?? "S";
    if (attr.AttributeName === skName) skType = attr.AttributeType ?? "S";
  }
  return { pkType, skType };
}

/** Resolve key schema names and types from a table description and optional index. */
export function resolveKeySchema(
  desc: TableDescription | null,
  indexName?: string,
): ResolvedKeySchema {
  if (!desc) return { pkName: "", skName: "", pkType: "S", skType: "S" };
  const keySchema = pickKeySchema(desc, indexName);
  const { pkName, skName } = keyNamesFromSchema(keySchema);
  const { pkType, skType } = keyTypesFromAttributeDefinitions(desc, pkName, skName);
  return { pkName, skName, pkType, skType };
}

export interface QueryKeyCondition {
  expression: string;
  names: Record<string, string>;
  values: Record<string, AttributeValue>;
}

/** Build a KeyConditionExpression with attribute names/values. */
export function buildKeyCondition(
  pkName: string,
  pkValue: string,
  pkType: string,
  skName: string,
  skOperator: string,
  skValue: string,
  skValue2: string,
  skType: string,
): QueryKeyCondition {
  const names: Record<string, string> = { "#pk": pkName };
  const values: Record<string, AttributeValue> = {
    ":pkval": pkType === "N" ? { N: pkValue } : { S: pkValue },
  };
  let expression = "#pk = :pkval";
  if (skOperator && skName && skValue) {
    names["#sk"] = skName;
    values[":skval"] = skType === "N" ? { N: skValue } : { S: skValue };
    if (skOperator === "BETWEEN" && skValue2) {
      values[":skval2"] = skType === "N" ? { N: skValue2 } : { S: skValue2 };
      expression += " AND #sk BETWEEN :skval AND :skval2";
    } else if (skOperator === "begins_with") {
      expression += " AND begins_with(#sk, :skval)";
    } else {
      expression += ` AND #sk ${skOperator} :skval`;
    }
  }
  return { expression, names, values };
}
