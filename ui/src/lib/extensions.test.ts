import { describe, expect, it } from "vitest";

import {
  claimLabels,
  hookLabels,
  needsFastPoll,
  statusStyle,
  type ExtensionInfo,
} from "./extensions";

const ext = (status: ExtensionInfo["status"]): ExtensionInfo => ({
  name: "x",
  type: "process",
  status,
  restarts: 0,
});

describe("extensions helpers", () => {
  it("claimLabels flattens routes", () => {
    expect(
      claimLabels({ path_prefixes: ["/hello"], hosts: ["a.test"], target_prefixes: ["Svc."] }),
    ).toEqual(["path /hello", "host a.test", "target Svc."]);
    expect(claimLabels()).toEqual([]);
  });

  it("hookLabels lists lifecycle and request hooks", () => {
    expect(
      hookLabels({
        on_startup: "/s",
        on_shutdown: "/d",
        request: [{ services: ["S3", "SQS"], pre: "/pre", post: "/post" }],
      }),
    ).toEqual(["on_startup", "on_shutdown", "pre+post S3,SQS"]);
    expect(hookLabels()).toEqual([]);
  });

  it("statusStyle distinguishes states", () => {
    expect(statusStyle("ready")).toContain("emerald");
    expect(statusStyle("failed")).toContain("red");
    expect(statusStyle("restarting")).toContain("amber");
    expect(statusStyle("stopped")).toContain("slate");
  });

  it("needsFastPoll is true while any extension is transitional", () => {
    expect(needsFastPoll([ext("ready"), ext("failed")])).toBe(false);
    expect(needsFastPoll([ext("ready"), ext("starting")])).toBe(true);
  });
});
