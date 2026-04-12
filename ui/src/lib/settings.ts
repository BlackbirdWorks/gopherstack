export type DashboardSettingsUpdate = {
  accountID: string;
  region: string;
  latencyMs: number;
  enforceIAM: boolean;
};

export function toSettingsForm(update: DashboardSettingsUpdate): URLSearchParams {
  const form = new URLSearchParams();
  form.set("accountID", update.accountID);
  form.set("region", update.region);
  form.set("latencyMs", String(update.latencyMs));
  form.set("enforceIAM", update.enforceIAM ? "true" : "false");

  return form;
}

export async function saveSettings(
  fetcher: (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>,
  update: DashboardSettingsUpdate,
): Promise<string> {
  const response = await fetcher("/dashboard/settings/update", {
    method: "POST",
    headers: {
      "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
    },
    body: toSettingsForm(update).toString(),
  });

  const payload = (await response.json()) as { message?: string };
  if (!response.ok) {
    throw new Error(payload.message ?? "settings update failed");
  }

  return payload.message ?? "Settings updated";
}
