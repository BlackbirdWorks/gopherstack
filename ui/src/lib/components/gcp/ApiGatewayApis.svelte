<script lang="ts">
	import { attempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		createGateway,
		diagnosticsFor,
		getConfig,
		GW_REGIONS,
		isActive,
		listApis,
		listConfigs,
		validGatewayId,
		type GwApi,
		type GwConfig,
		type GwOperation
	} from '#lib/gcpapigateway.ts';

	let { project, operations, refreshKey, onChanged }: { project: string; operations: GwOperation[]; refreshKey: number; onChanged: () => void } = $props();

	let apis = $state<GwApi[]>([]);
	let configs = $state<GwConfig[]>([]);
	let selectedApi = $state('');
	let viewing = $state<GwConfig | null>(null);
	let creating = $state('');
	let gwId = $state('');
	let gwName = $state('');
	let gwRegion = $state(GW_REGIONS[0]);
	let error = $state('');

	async function loadApis() {
		try {
			apis = await listApis(project);
			error = '';
			if (!apis.some((a) => a.name === selectedApi)) selectedApi = '';
			await loadConfigs();
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function loadConfigs() {
		viewing = null;
		if (!selectedApi) {
			configs = [];
			return;
		}
		try {
			configs = await listConfigs(selectedApi);
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	function pickApi(a: GwApi) {
		selectedApi = selectedApi === a.name ? '' : a.name;
		creating = '';
		void loadConfigs();
	}

	async function view(c: GwConfig) {
		if (viewing?.name === c.name) {
			viewing = null;
			return;
		}
		try {
			viewing = await getConfig(c.name);
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function create(event: Event, c: GwConfig) {
		event.preventDefault();
		if (!validGatewayId(gwId)) return;
		const ok = await attempt(() => createGateway(project, gwRegion, gwId, gwName, c.name), `Gateway ${gwId} requested`, `Failed to create ${gwId}`);
		if (ok) {
			creating = gwId = gwName = '';
			onChanged();
		}
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';

	$effect(() => {
		void refreshKey;
		void project;
		void loadApis();
	});
</script>

<div class="space-y-4" data-testid="agw-apis">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="agw-apis-error">{error}</p>{/if}
	{#if apis.length === 0}
		<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="agw-apis-empty">No APIs in this project.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="agw-api-table">
				<thead class={thead}><tr><th class={cell}>API</th><th class={cell}>Display name</th><th class={cell}>State</th><th class={cell}>Managed service</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each apis as a (a.name)}
						<tr class={selectedApi === a.name ? 'bg-blue-50/60 dark:bg-blue-950/20' : ''} data-testid="agw-api">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => pickApi(a)} aria-expanded={selectedApi === a.name} data-testid="agw-api-select">{a.id}</button></td>
							<td class="{cell} text-xs">{a.displayName || '—'}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(a.state)}">{a.state || '—'}</span></td>
							<td class="{cell} font-mono text-xs break-all">{a.managedService || '—'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	{#if selectedApi}
		<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="agw-configs">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">API configs</h2>
			{#if configs.length === 0}
				<p class="text-sm text-gray-500" data-testid="agw-configs-empty">No configs for this API.</p>
			{:else}
				<table class="w-full text-sm" data-testid="agw-config-table">
					<thead class="text-left text-gray-500"><tr><th class={cell}>Config</th><th class={cell}>State</th><th class={cell}>Service config</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each configs as c (c.name)}
							{@const diags = c.state === 'FAILED' ? diagnosticsFor(c.name, operations) : []}
							<tr data-testid="agw-config">
								<td class="{cell} font-mono text-xs">{c.id}</td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(c.state)}" data-testid="agw-config-state">{c.state || '—'}</span></td>
								<td class="{cell} font-mono text-xs">{c.serviceConfigId || '—'}</td>
								<td class="{cell} text-xs">{c.createTime || '—'}</td>
								<td class="{cell} text-right whitespace-nowrap space-x-1">
									<button class={GCP_MINI} onclick={() => view(c)} data-testid="agw-config-view">{viewing?.name === c.name ? 'Hide OpenAPI' : 'View OpenAPI'}</button>
									<button class={GCP_MINI} onclick={() => (creating = creating === c.name ? '' : c.name)} disabled={!isActive(c)} data-testid="agw-config-deploy">Create gateway</button>
								</td>
							</tr>
							{#if diags.length > 0}
								<tr><td colspan="5" class="{cell} text-xs">
									<ul class="space-y-1 text-red-700 dark:text-red-300" data-testid="agw-diagnostics">
										{#each diags as d, i (i)}<li><span class="font-mono">{d.location || 'config'}</span>: {d.message}</li>{/each}
									</ul>
								</td></tr>
							{/if}
							{#if creating === c.name}
								<tr><td colspan="5" class={cell}>
									<form class="flex flex-wrap items-end gap-2" onsubmit={(e) => create(e, c)} data-testid="agw-create-form">
										<label class="text-xs text-gray-500">Gateway ID<input class="{GCP_INPUT} mt-1" bind:value={gwId} placeholder="edge" data-testid="agw-create-id" /></label>
										<label class="text-xs text-gray-500">Display name (optional)<input class="{GCP_INPUT} mt-1" bind:value={gwName} data-testid="agw-create-name" /></label>
										<label class="text-xs text-gray-500">Region
											<select class="{GCP_INPUT} mt-1" bind:value={gwRegion} data-testid="agw-create-region">{#each GW_REGIONS as r (r)}<option value={r}>{r}</option>{/each}</select>
										</label>
										<button class={GCP_PRIMARY} type="submit" disabled={!validGatewayId(gwId)} data-testid="agw-create-submit">Create</button>
									</form>
								</td></tr>
							{/if}
						{/each}
					</tbody>
				</table>
			{/if}

			{#if viewing}
				<div class="space-y-2" data-testid="agw-openapi">
					<h3 class="text-sm font-medium">OpenAPI documents of {viewing.id}</h3>
					{#if viewing.documents.length === 0}<p class="text-xs text-gray-500">This config has no OpenAPI documents.</p>{/if}
					{#each viewing.documents as d (d.path)}
						<p class="font-mono text-xs text-gray-500">{d.path}{d.decoded ? '' : ' (not text, shown as base64)'}</p>
						<pre class="max-h-96 overflow-auto rounded-md bg-gray-50 p-3 text-xs dark:bg-slate-800" data-testid="agw-openapi-doc">{d.contents}</pre>
					{/each}
				</div>
			{/if}
		</div>
	{/if}
</div>
