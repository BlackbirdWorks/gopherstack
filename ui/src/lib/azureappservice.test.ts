import { describe, expect, it, vi } from "vitest";

import {
  APPSERVICE_API_VERSION,
  APPSERVICE_PROXY_BASE,
  getStickySettings,
  isWebApp,
  listSlots,
  listWebApps,
  parseSite,
  parseSticky,
  siteAction,
  siteUrl,
  stickyCount,
  swapSlots,
  swapTargets,
} from "./azureappservice";

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
const asFetch = (f: unknown) => f as typeof fetch;

const sub = "00000000-0000-0000-0000-000000000000";
const idOf = (rg: string, name: string) =>
  `/subscriptions/${sub}/resourceGroups/${rg}/providers/Microsoft.Web/sites/${name}`;
const site = (name: string, rg = "demo", extra: Record<string, unknown> = {}) => ({
  id: idOf(rg, name.split("/").join("/slots/")),
  name,
  kind: "app,linux",
  location: "eastus",
  properties: {
    state: "Running",
    hostNames: [`${name.split("/")[0]}.azurewebsites.net`],
    defaultHostName: `${name.split("/")[0]}.azurewebsites.net`,
    siteConfig: { linuxFxVersion: "NODE|20-lts" },
    ...extra,
  },
});
const api = `?api-version=${APPSERVICE_API_VERSION}`;
const base = `${APPSERVICE_PROXY_BASE}${idOf("demo", "shop")}`;

describe("parsing", () => {
  it("parses a web app", () => {
    expect(parseSite(site("shop"))).toMatchObject({
      app: "shop",
      slot: "",
      resourceGroup: "demo",
      state: "Running",
      runtime: "NODE|20-lts",
      defaultHostName: "shop.azurewebsites.net",
      previewTarget: "",
    });
  });

  it("parses a slot with a pending preview and the last swap", () => {
    const s = parseSite(
      site("shop/staging", "demo", {
        targetSwapSlot: "production",
        slotSwapStatus: {
          sourceSlotName: "staging",
          destinationSlotName: "production",
          timestampUtc: "t",
        },
      }),
    );
    expect(s).toMatchObject({ app: "shop", slot: "staging", previewTarget: "production" });
    expect(s.lastSwap).toBe("staging → production at t");
  });

  it("tells function apps from web apps and builds the hostname link", () => {
    expect(isWebApp({ kind: "functionapp,linux" })).toBe(false);
    expect(isWebApp({ kind: "app,linux" })).toBe(true);
    expect(siteUrl({ defaultHostName: "a.example", httpsOnly: true })).toBe("https://a.example");
    expect(siteUrl({ defaultHostName: "a.example", httpsOnly: false })).toBe("http://a.example");
    expect(siteUrl({ defaultHostName: "", httpsOnly: false })).toBe("");
  });

  it("parses sticky names", () => {
    const sticky = parseSticky({
      properties: { appSettingNames: ["A"], connectionStringNames: ["C", "D"] },
    });
    expect(sticky).toEqual({ appSettings: ["A"], connectionStrings: ["C", "D"], azureStorage: [] });
    expect(stickyCount(sticky)).toBe(3);
    expect(stickyCount(parseSticky({}))).toBe(0);
  });

  it("offers every other slot, production included, as swap target", () => {
    const slots = [{ slot: "staging" }, { slot: "qa" }];
    expect(swapTargets(slots, "production")).toEqual(["staging", "qa"]);
    expect(swapTargets(slots, "staging")).toEqual(["production", "qa"]);
  });
});

describe("requests", () => {
  it("lists web apps only, without slots and function apps, sorted", async () => {
    const fetchFn = vi.fn().mockResolvedValue(
      json({
        value: [
          site("zeta", "b-rg"),
          { ...site("fn"), kind: "functionapp,linux" },
          site("alpha", "b-rg"),
          site("shop"),
          site("shop/staging"),
        ],
      }),
    );
    const apps = await listWebApps(sub, asFetch(fetchFn));
    expect(apps.map((a) => `${a.resourceGroup}/${a.app}`)).toEqual([
      "b-rg/alpha",
      "b-rg/zeta",
      "demo/shop",
    ]);
    expect(fetchFn.mock.calls[0]?.[0]).toBe(
      `${APPSERVICE_PROXY_BASE}/subscriptions/${sub}/providers/Microsoft.Web/sites${api}`,
    );
  });

  it("lists slots and reads sticky settings", async () => {
    const fetchFn = vi.fn((url: string) =>
      Promise.resolve(
        json(
          url.includes("slotConfigNames")
            ? { properties: { appSettingNames: ["X"] } }
            : { value: [site("shop/staging"), site("shop/qa")] },
        ),
      ),
    );
    const app = { resourceGroup: "demo", app: "shop" };
    expect((await listSlots(sub, app, asFetch(fetchFn))).map((s) => s.slot)).toEqual([
      "qa",
      "staging",
    ]);
    expect((await getStickySettings(sub, app, asFetch(fetchFn))).appSettings).toEqual(["X"]);
    expect(fetchFn.mock.calls.map((c) => c[0])).toEqual([
      `${base}/slots${api}`,
      `${base}/config/slotConfigNames${api}`,
    ]);
  });

  it("posts lifecycle actions to the app and to a slot", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    const app = { resourceGroup: "demo", app: "shop" };
    await siteAction(sub, { ...app, slot: "" }, "stop", asFetch(fetchFn));
    await siteAction(sub, { ...app, slot: "staging" }, "restart", asFetch(fetchFn));
    expect(fetchFn.mock.calls.map((c) => [c[0], (c[1] as RequestInit).method])).toEqual([
      [`${base}/stop${api}`, "POST"],
      [`${base}/slots/staging/restart${api}`, "POST"],
    ]);
  });

  it("swaps, previews, completes and cancels from the source slot", async () => {
    const fetchFn = vi.fn((_url: string, _init?: RequestInit) => Promise.resolve(json({})));
    const app = { resourceGroup: "demo", app: "shop" };
    await swapSlots(sub, app, "staging", "production", "swap", asFetch(fetchFn));
    await swapSlots(sub, app, "staging", "production", "preview", asFetch(fetchFn));
    await swapSlots(sub, app, "staging", "production", "complete", asFetch(fetchFn));
    await swapSlots(sub, app, "staging", "production", "cancel", asFetch(fetchFn));
    await swapSlots(sub, app, "production", "staging", "swap", asFetch(fetchFn));
    const calls = fetchFn.mock.calls.map((c) => [c[0], (c[1] as RequestInit).body]);
    expect(calls).toEqual([
      [`${base}/slots/staging/slotsswap${api}`, '{"targetSlot":"production"}'],
      [`${base}/slots/staging/applySlotConfig${api}`, '{"targetSlot":"production"}'],
      [`${base}/slots/staging/slotsswap${api}`, '{"targetSlot":"production"}'],
      [`${base}/slots/staging/resetSlotConfig${api}`, undefined],
      [`${base}/slotsswap${api}`, '{"targetSlot":"staging"}'],
    ]);
  });

  it("surfaces an ARM failure", async () => {
    const fetchFn = vi
      .fn()
      .mockResolvedValue(json({ error: { code: "Conflict", message: "swap pending" } }, 409));
    await expect(
      siteAction(sub, { resourceGroup: "demo", app: "shop", slot: "" }, "stop", asFetch(fetchFn)),
    ).rejects.toMatchObject({ httpStatus: 409, message: "swap pending" });
  });
});
