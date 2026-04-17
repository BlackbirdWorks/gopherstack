import { describe, expect, it } from "vitest";

import {
	avToJson,
	buildKeyCondition,
	getColumns,
	getKeySchema,
	itemToJson,
	jsonToAv,
	jsonToItem,
	resolveKeySchema,
} from "./dynamodb";

describe("avToJson", () => {
	it("converts string values", () => {
		expect(avToJson({ S: "hello" })).toBe("hello");
	});

	it("converts number values", () => {
		expect(avToJson({ N: "42" })).toBe(42);
	});

	it("converts boolean values", () => {
		expect(avToJson({ BOOL: true })).toBe(true);
		expect(avToJson({ BOOL: false })).toBe(false);
	});

	it("converts null values", () => {
		expect(avToJson({ NULL: true })).toBeNull();
	});

	it("converts list values recursively", () => {
		expect(avToJson({ L: [{ S: "a" }, { N: "1" }] })).toEqual(["a", 1]);
	});

	it("converts map values recursively", () => {
		expect(
			avToJson({ M: { name: { S: "Alice" }, age: { N: "30" } } }),
		).toEqual({ name: "Alice", age: 30 });
	});

	it("converts string sets", () => {
		expect(avToJson({ SS: ["a", "b", "c"] })).toEqual(["a", "b", "c"]);
	});

	it("converts number sets", () => {
		expect(avToJson({ NS: ["1", "2", "3"] })).toEqual([1, 2, 3]);
	});

	it("converts binary values to placeholder", () => {
		expect(avToJson({ B: new Uint8Array([1, 2]) })).toBe("(binary)");
	});

	it("converts binary sets to placeholder", () => {
		expect(avToJson({ BS: [new Uint8Array([1])] })).toBe("(binary set)");
	});

	it("falls back to JSON string for unknown shapes", () => {
		const result = avToJson({});
		expect(typeof result).toBe("string");
	});
});

describe("itemToJson", () => {
	it("converts a full DynamoDB item", () => {
		expect(
			itemToJson({
				id: { S: "user-1" },
				age: { N: "25" },
				active: { BOOL: true },
			}),
		).toEqual({ id: "user-1", age: 25, active: true });
	});

	it("handles empty item", () => {
		expect(itemToJson({})).toEqual({});
	});

	it("handles nested maps and lists", () => {
		expect(
			itemToJson({
				data: {
					M: {
						tags: { L: [{ S: "go" }, { S: "ts" }] },
						nested: { M: { x: { N: "1" } } },
					},
				},
			}),
		).toEqual({
			data: { tags: ["go", "ts"], nested: { x: 1 } },
		});
	});
});

describe("jsonToAv", () => {
	it("converts strings", () => {
		expect(jsonToAv("hello")).toEqual({ S: "hello" });
	});

	it("converts numbers", () => {
		expect(jsonToAv(42)).toEqual({ N: "42" });
	});

	it("converts booleans", () => {
		expect(jsonToAv(true)).toEqual({ BOOL: true });
	});

	it("converts null", () => {
		expect(jsonToAv(null)).toEqual({ NULL: true });
	});

	it("converts undefined", () => {
		expect(jsonToAv()).toEqual({ NULL: true });
	});

	it("converts arrays", () => {
		expect(jsonToAv(["a", 1])).toEqual({
			L: [{ S: "a" }, { N: "1" }],
		});
	});

	it("converts objects", () => {
		expect(jsonToAv({ name: "Bob" })).toEqual({
			M: { name: { S: "Bob" } },
		});
	});

	it("converts nested structures", () => {
		expect(jsonToAv({ list: [1, "two", null] })).toEqual({
			M: {
				list: {
					L: [{ N: "1" }, { S: "two" }, { NULL: true }],
				},
			},
		});
	});

	it("falls back to string for unrecognized types", () => {
		expect(jsonToAv(BigInt(42))).toEqual({ S: "42" });
	});
});

describe("jsonToItem", () => {
	it("converts a plain object to DynamoDB item", () => {
		expect(jsonToItem({ id: "pk-1", count: 5 })).toEqual({
			id: { S: "pk-1" },
			count: { N: "5" },
		});
	});

	it("handles empty object", () => {
		expect(jsonToItem({})).toEqual({});
	});
});

describe("roundtrip avToJson <-> jsonToAv", () => {
	it("roundtrips scalar types", () => {
		expect(avToJson(jsonToAv("hello"))).toBe("hello");
		expect(avToJson(jsonToAv(42))).toBe(42);
		expect(avToJson(jsonToAv(true))).toBe(true);
		expect(avToJson(jsonToAv(null))).toBeNull();
	});

	it("roundtrips nested structures", () => {
		const original = { name: "Alice", scores: [100, 200], meta: { x: 1 } };
		const converted = avToJson(jsonToAv(original));
		expect(converted).toEqual(original);
	});
});

describe("getColumns", () => {
	it("extracts unique columns from items", () => {
		const items = [
			{ id: "1", name: "Alice" },
			{ id: "2", email: "bob@test.com" },
		];
		expect(getColumns(items)).toEqual(["id", "name", "email"]);
	});

	it("returns empty array for empty list", () => {
		expect(getColumns([])).toEqual([]);
	});

	it("deduplicates column names", () => {
		const items = [{ a: 1, b: 2 }, { b: 3, a: 4 }];
		expect(getColumns(items)).toEqual(["a", "b"]);
	});
});

describe("getKeySchema", () => {
	it("formats key schema from table description", () => {
		expect(
			getKeySchema({
				KeySchema: [
					{ AttributeName: "pk", KeyType: "HASH" },
					{ AttributeName: "sk", KeyType: "RANGE" },
				],
			}),
		).toBe("pk (HASH), sk (RANGE)");
	});

	it("returns empty string for undefined", () => {
		expect(getKeySchema()).toBe("");
	});

	it("returns empty string when no KeySchema", () => {
		expect(getKeySchema({})).toBe("");
	});

	it("handles hash-only tables", () => {
		expect(
			getKeySchema({
				KeySchema: [{ AttributeName: "id", KeyType: "HASH" }],
			}),
		).toBe("id (HASH)");
	});
});

describe("resolveKeySchema", () => {
	const desc = {
		KeySchema: [
			{ AttributeName: "pk", KeyType: "HASH" as const },
			{ AttributeName: "sk", KeyType: "RANGE" as const },
		],
		AttributeDefinitions: [
			{ AttributeName: "pk", AttributeType: "S" as const },
			{ AttributeName: "sk", AttributeType: "N" as const },
			{ AttributeName: "gsiPk", AttributeType: "S" as const },
			{ AttributeName: "gsiSk", AttributeType: "S" as const },
			{ AttributeName: "lsiSk", AttributeType: "N" as const },
		],
		GlobalSecondaryIndexes: [
			{
				IndexName: "gsi-1",
				KeySchema: [
					{ AttributeName: "gsiPk", KeyType: "HASH" as const },
					{ AttributeName: "gsiSk", KeyType: "RANGE" as const },
				],
			},
		],
		LocalSecondaryIndexes: [
			{
				IndexName: "lsi-1",
				KeySchema: [
					{ AttributeName: "pk", KeyType: "HASH" as const },
					{ AttributeName: "lsiSk", KeyType: "RANGE" as const },
				],
			},
		],
	};

	it("returns defaults for null description", () => {
		expect(resolveKeySchema(null)).toEqual({
			pkName: "",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});

	it("resolves table key schema", () => {
		expect(resolveKeySchema(desc)).toEqual({
			pkName: "pk",
			skName: "sk",
			pkType: "S",
			skType: "N",
		});
	});

	it("resolves GSI key schema", () => {
		expect(resolveKeySchema(desc, "gsi-1")).toEqual({
			pkName: "gsiPk",
			skName: "gsiSk",
			pkType: "S",
			skType: "S",
		});
	});

	it("resolves LSI key schema", () => {
		expect(resolveKeySchema(desc, "lsi-1")).toEqual({
			pkName: "pk",
			skName: "lsiSk",
			pkType: "S",
			skType: "N",
		});
	});

	it("falls back to table key schema for unknown index", () => {
		expect(resolveKeySchema(desc, "nonexistent")).toEqual({
			pkName: "pk",
			skName: "sk",
			pkType: "S",
			skType: "N",
		});
	});

	it("handles hash-only tables", () => {
		const hashOnly = {
			KeySchema: [{ AttributeName: "id", KeyType: "HASH" as const }],
			AttributeDefinitions: [
				{ AttributeName: "id", AttributeType: "S" as const },
			],
		};
		expect(resolveKeySchema(hashOnly)).toEqual({
			pkName: "id",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});

	it("handles missing KeySchema on description", () => {
		expect(resolveKeySchema({})).toEqual({
			pkName: "",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});

	it("handles missing AttributeDefinitions", () => {
		const noAttrDefs = {
			KeySchema: [{ AttributeName: "pk", KeyType: "HASH" as const }],
		};
		expect(resolveKeySchema(noAttrDefs)).toEqual({
			pkName: "pk",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});

	it("handles undefined AttributeName and AttributeType", () => {
		const weirdDesc = {
			KeySchema: [
				{ KeyType: "HASH" as const },
				{ KeyType: "RANGE" as const },
			],
			AttributeDefinitions: [{ AttributeName: "" }],
		};
		expect(resolveKeySchema(weirdDesc)).toEqual({
			pkName: "",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});

	it("handles index with no KeySchema", () => {
		const descNoGsiKS = {
			KeySchema: [{ AttributeName: "pk", KeyType: "HASH" as const }],
			AttributeDefinitions: [
				{ AttributeName: "pk", AttributeType: "S" as const },
			],
			GlobalSecondaryIndexes: [{ IndexName: "gsi-empty" }],
		};
		expect(resolveKeySchema(descNoGsiKS, "gsi-empty")).toEqual({
			pkName: "",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});

	it("handles LSI with no KeySchema", () => {
		const descNoLsiKS = {
			KeySchema: [{ AttributeName: "pk", KeyType: "HASH" as const }],
			AttributeDefinitions: [
				{ AttributeName: "pk", AttributeType: "S" as const },
			],
			LocalSecondaryIndexes: [{ IndexName: "lsi-empty" }],
		};
		expect(resolveKeySchema(descNoLsiKS, "lsi-empty")).toEqual({
			pkName: "",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});

	it("falls back to table schema when no GSIs or LSIs exist", () => {
		const noIndexes = {
			KeySchema: [{ AttributeName: "pk", KeyType: "HASH" as const }],
			AttributeDefinitions: [
				{ AttributeName: "pk", AttributeType: "S" as const },
			],
		};
		expect(resolveKeySchema(noIndexes, "some-index")).toEqual({
			pkName: "pk",
			skName: "",
			pkType: "S",
			skType: "S",
		});
	});
});

describe("buildKeyCondition", () => {
	it("builds partition key only condition", () => {
		const result = buildKeyCondition("pk", "user-1", "S", "", "", "", "", "S");
		expect(result.expression).toBe("#pk = :pkval");
		expect(result.names).toEqual({ "#pk": "pk" });
		expect(result.values).toEqual({ ":pkval": { S: "user-1" } });
	});

	it("builds partition key with numeric type", () => {
		const result = buildKeyCondition("id", "42", "N", "", "", "", "", "S");
		expect(result.values).toEqual({ ":pkval": { N: "42" } });
	});

	it("builds equals sort key condition", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "=", "2024-01-01", "", "S",
		);
		expect(result.expression).toBe("#pk = :pkval AND #sk = :skval");
		expect(result.names).toEqual({ "#pk": "pk", "#sk": "sk" });
		expect(result.values[":skval"]).toEqual({ S: "2024-01-01" });
	});

	it("builds less-than sort key condition", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "<", "100", "", "N",
		);
		expect(result.expression).toBe("#pk = :pkval AND #sk < :skval");
		expect(result.values[":skval"]).toEqual({ N: "100" });
	});

	it("builds greater-than-or-equal sort key condition", () => {
		const result = buildKeyCondition(
			"pk", "val", "S",
			"sk", ">=", "50", "", "N",
		);
		expect(result.expression).toBe("#pk = :pkval AND #sk >= :skval");
	});

	it("builds BETWEEN sort key condition", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "BETWEEN", "A", "Z", "S",
		);
		expect(result.expression).toBe(
			"#pk = :pkval AND #sk BETWEEN :skval AND :skval2",
		);
		expect(result.values[":skval"]).toEqual({ S: "A" });
		expect(result.values[":skval2"]).toEqual({ S: "Z" });
	});

	it("builds BETWEEN with numeric sort key", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "BETWEEN", "10", "20", "N",
		);
		expect(result.values[":skval"]).toEqual({ N: "10" });
		expect(result.values[":skval2"]).toEqual({ N: "20" });
	});

	it("builds begins_with sort key condition", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "begins_with", "2024-", "", "S",
		);
		expect(result.expression).toBe(
			"#pk = :pkval AND begins_with(#sk, :skval)",
		);
	});

	it("skips sort key when operator is empty", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "", "value", "", "S",
		);
		expect(result.expression).toBe("#pk = :pkval");
		expect(result.names).not.toHaveProperty("#sk");
	});

	it("skips sort key when sk name is empty", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"", "=", "value", "", "S",
		);
		expect(result.expression).toBe("#pk = :pkval");
	});

	it("skips sort key when sk value is empty", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "=", "", "", "S",
		);
		expect(result.expression).toBe("#pk = :pkval");
	});

	it("skips BETWEEN second value when empty", () => {
		const result = buildKeyCondition(
			"pk", "user-1", "S",
			"sk", "BETWEEN", "A", "", "S",
		);
		// falls through to generic operator since value2 is empty
		expect(result.expression).toBe("#pk = :pkval AND #sk BETWEEN :skval");
	});
});
