import { readdirSync } from "node:fs";
import { resolve } from "node:path";

import { describe, expect, it } from "vitest";

import {
  AZURE_SERVICES,
  GCP_SERVICES,
  OCI_SERVICES,
  azureRoute,
  gcpRoute,
  ociRoute,
} from "./cloud";
import { ICON_IDS } from "./icon-manifest";
import { sidebarCategories } from "./nav";

const iconsDir = resolve(import.meta.dirname, "../../../dashboard/static/icons");
const onDisk = readdirSync(iconsDir)
  .filter((f) => f.endsWith(".svg"))
  .map((f) => f.slice(0, -4))
  .toSorted();

describe("icon manifest", () => {
  it("matches the icon files on disk", () => {
    expect([...ICON_IDS].toSorted()).toEqual(onDisk);
  });

  it("covers every service icon the sidebar can request", () => {
    const routes = [
      ...sidebarCategories.flatMap((c) => c.routes),
      ...AZURE_SERVICES.map((s) => azureRoute(s)),
      ...GCP_SERVICES.map((s) => gcpRoute(s)),
      ...OCI_SERVICES.map((s) => ociRoute(s)),
    ];
    const missing = routes.map((r) => r.icon).filter((i) => !ICON_IDS.has(i));
    expect([...new Set(missing)]).toEqual([]);
  });
});
