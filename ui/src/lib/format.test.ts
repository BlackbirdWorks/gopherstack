import { describe, expect, it } from "vitest";

import { formatBytes, formatDate, formatNumber } from "./format";

describe("formatDate", () => {
  it("returns em dash for undefined", () => {
    const empty: { value?: string } = {};
    expect(formatDate(empty.value)).toBe("—");
  });

  it("returns em dash for null", () => {
    expect(formatDate(null)).toBe("—");
  });

  it("returns em dash for empty string", () => {
    expect(formatDate("")).toBe("—");
  });

  it("returns em dash for an invalid date string", () => {
    expect(formatDate("not-a-date")).toBe("—");
  });

  it("formats a Date instance via toLocaleString", () => {
    const date = new Date(2024, 0, 15, 9, 30);
    expect(formatDate(date)).toBe(date.toLocaleString());
  });

  it("formats a date string the same as the equivalent Date", () => {
    const iso = "2024-03-05T12:00:00.000Z";
    expect(formatDate(iso)).toBe(new Date(iso).toLocaleString());
  });
});

describe("formatBytes", () => {
  it("returns em dash for undefined", () => {
    const empty: { value?: number } = {};
    expect(formatBytes(empty.value)).toBe("—");
  });

  it("returns em dash for null", () => {
    expect(formatBytes(null)).toBe("—");
  });

  it("returns em dash for NaN", () => {
    expect(formatBytes(Number.NaN)).toBe("—");
  });

  it("formats sub-1024 byte counts with no decimal", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(512)).toBe("512 B");
  });

  it("formats kilobytes", () => {
    expect(formatBytes(1536)).toBe("1.5 KB");
  });

  it("formats megabytes", () => {
    expect(formatBytes(5 * 1024 * 1024)).toBe("5.0 MB");
  });

  it("formats gigabytes", () => {
    expect(formatBytes(2 * 1024 * 1024 * 1024)).toBe("2.0 GB");
  });

  it("formats terabytes", () => {
    expect(formatBytes(3 * 1024 * 1024 * 1024 * 1024)).toBe("3.0 TB");
  });
});

describe("formatNumber", () => {
  it("returns em dash for undefined", () => {
    const empty: { value?: number } = {};
    expect(formatNumber(empty.value)).toBe("—");
  });

  it("returns em dash for null", () => {
    expect(formatNumber(null)).toBe("—");
  });

  it("returns em dash for NaN", () => {
    expect(formatNumber(Number.NaN)).toBe("—");
  });

  it("formats with locale thousands separators", () => {
    expect(formatNumber(1234567)).toBe((1234567).toLocaleString());
  });

  it("formats zero", () => {
    expect(formatNumber(0)).toBe("0");
  });
});
