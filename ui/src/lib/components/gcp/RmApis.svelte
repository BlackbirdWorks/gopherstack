<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { attempt, GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { disableService, enableService, filterServices, listServices, statePill, type RmService } from '#lib/gcpresourcemanager.ts';

	let { project }: { project: string } = $props();

	let all = $state<RmService[]>([]);
	let query = $state('');
	let stateFilter = $state('');
	let loading = $state(true);
	let error = $state('');
	let disabling = $state('');
	let dependents = $state(false);
	let disableError = $state('');
	let busy = $state(false);

	const shown = $derived(filterServices(all, query, stateFilter));
	const enabledCount = $derived(all.filter((s) => s.state === 'ENABLED').length);

	async function load() {
		try {
			all = await listServices(project);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void project;
		disabling = '';
		loading = true;
		void load();
	});

	async function enable(s: RmService) {
		if (await attempt(() => enableService(project, s.id), `Enabled ${s.id}`, `Failed to enable ${s.id}`)) await load();
	}

	function startDisable(s: RmService) {
		disabling = s.id;
		dependents = false;
		disableError = '';
	}

	async function confirmDisable() {
		busy = true;
		try {
			await disableService(project, disabling, dependents);
			disabling = '';
			await load();
		} catch (e) {
			disableError = describeGcpError(e);
		} finally {
			busy = false;
		}
	}

</script>

<div class="space-y-3" data-testid="rm-apis">
	<div class="flex flex-wrap items-center gap-3 text-sm">
		<input class="{GCP_INPUT} !w-64" placeholder="Search the API catalog" bind:value={query} aria-label="Search APIs" data-testid="rm-api-search" />
		<select class="{GCP_INPUT} !w-40" bind:value={stateFilter} aria-label="Filter by state" data-testid="rm-api-filter">
			<option value="">All</option>
			<option value="ENABLED">Enabled</option>
			<option value="DISABLED">Disabled</option>
		</select>
		<span class="text-xs text-gray-500 dark:text-gray-400" data-testid="rm-api-count">{enabledCount} of {all.length} enabled</span>
	</div>

	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="rm-api-error">{error}</p>
	{:else if loading}
		<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
	{:else if shown.length === 0}
		<p class="py-4 text-center text-sm text-gray-500" data-testid="rm-api-empty">No APIs match.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="rm-api-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class="px-3 py-2">API</th><th class="px-3 py-2">State</th><th class="px-3 py-2 text-right">Action</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each shown as s (s.id)}
						<tr data-testid="rm-api" data-service={s.id}>
							<td class="px-3 py-2"><div class="font-mono text-xs">{s.id}</div><div class="text-xs text-gray-500">{s.title}{s.summary ? ` — ${s.summary}` : ''}</div></td>
							<td class="px-3 py-2"><span class="rounded-full px-2 py-0.5 text-xs {statePill(s.state)}" data-testid="rm-api-state">{s.state}</span></td>
							<td class="px-3 py-2 text-right">
								{#if s.state === 'ENABLED'}
									<button class={GCP_MINI} onclick={() => startDisable(s)} data-testid="rm-api-disable">Disable</button>
								{:else}
									<button class={GCP_MINI} onclick={() => enable(s)} data-testid="rm-api-enable">Enable</button>
								{/if}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	{#if disabling}
		<div class="space-y-3 rounded-lg border border-amber-300 bg-amber-50 p-4 text-sm dark:border-amber-800 dark:bg-amber-950/30" role="alertdialog" aria-label="Disable API" data-testid="rm-disable-panel">
			<p class="font-medium">Disable <span class="font-mono">{disabling}</span>?</p>
			<p class="text-xs text-gray-600 dark:text-gray-300">Other enabled APIs may depend on this one. Disabling it fails unless dependent services are disabled with it.</p>
			<label class="flex items-center gap-2"><input type="checkbox" bind:checked={dependents} data-testid="rm-disable-dependents" /> Also disable dependent services</label>
			{#if disableError}<p class="text-red-600" role="alert" data-testid="rm-disable-error">{disableError}</p>{/if}
			<div class="flex gap-2">
				<button class={GCP_PRIMARY} disabled={busy} onclick={confirmDisable} data-testid="rm-disable-confirm">Disable</button>
				<button class={GCP_GHOST} onclick={() => (disabling = '')} data-testid="rm-disable-cancel">Cancel</button>
			</div>
		</div>
	{/if}
</div>
