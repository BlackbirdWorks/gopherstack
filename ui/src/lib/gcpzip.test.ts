import { describe, expect, it } from "vitest";

import { crc32, zipStore } from "./gcpzip";

const text = (s: string) => new TextEncoder().encode(s);
const u32 = (b: Uint8Array, at: number) => new DataView(b.buffer, b.byteOffset).getUint32(at, true);

describe("zipStore", () => {
  it("computes the standard crc32", () => {
    expect(crc32(text("123456789"))).toBe(0xcbf43926);
  });

  it("writes local header, data, central directory and end record", () => {
    const data = text("hello");
    const zip = zipStore([{ name: "index.js", data }]);
    expect(u32(zip, 0)).toBe(0x04034b50);
    expect(u32(zip, 14)).toBe(crc32(data));
    expect(new TextDecoder().decode(zip.slice(30, 38))).toBe("index.js");
    expect(new TextDecoder().decode(zip.slice(38, 43))).toBe("hello");
    const end = zip.length - 22;
    expect(u32(zip, end)).toBe(0x06054b50);
    expect(u32(zip, end + 16)).toBe(43);
    expect(u32(zip, 43)).toBe(0x02014b50);
  });

  it("records every file in the central directory", () => {
    const zip = zipStore([
      { name: "a", data: text("1") },
      { name: "b", data: text("22") },
    ]);
    const end = zip.length - 22;
    expect(new DataView(zip.buffer).getUint16(end + 10, true)).toBe(2);
  });
});
