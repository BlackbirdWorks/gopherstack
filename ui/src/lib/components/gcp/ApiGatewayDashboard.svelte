<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw, Waypoints } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import TryItPanel from '#lib/components/TryItPanel.svelte';
	import ApiGatewayApis from '#lib/components/gcp/ApiGatewayApis.svelte';
	import { confirmedAttempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError, shortName } from '#lib/gcprest.ts';
	import {
		deleteGateway,
		isActive,
		listGateways,
		listOperations,
		listProjects,
		readStoredProject,
		tryGateway,
		writeStoredProject,
		type GwGateway,
		type GwOperation
	} from '#lib/gcpapigateway.ts';

	const TABS = [
		{ id: 'apis', label: 'APIs & configs' },
		{ id: 'gateways', label: 'Gateways' },
		{ id: 'operations', label: 'Operations' },
		{ id: 'try', label: 'Try it' }
	];

	let projects = $state<string[]>([]);
	let project = $state('');
	let tab = $state('apis');
	let gateways = $state<GwGateway[]>([]);
	let operations = $state<GwOperation[]>([]);
	let loading = $state(true);
	let error = $state('');
	let refreshKey = $state(0);
	let tryGatewayName = $state('');
	let apiKey = $state('');
	let method = $state('GET');
	let path = $state('/');

	const activeGateways = $derived(gateways.filter((g) => isActive(g)));
	const target = $derived(gateways.find((g) => g.name === tryGatewayName));

	async function boot() {
		try {
			projects = await listProjects();
			const s = readStoredProject();
			project = projects.includes(s) ? s : (projects[0] ?? '');
			error = '';
			await loadScope();
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadScope() {
		if (!project) return;
		try {
			gateways = await listGateways(project);
			operations = await listOperations(project, gateways.map((g) => g.location));
			error = '';
			if (!gateways.some((g) => g.name === tryGatewayName)) tryGatewayName = activeGateways[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function refresh() {
		refreshKey += 1;
		await loadScope();
	}

	async function pickProject(p: string) {
		project = p;
		writeStoredProject(p);
		await refresh();
	}

	async function remove(g: GwGateway) {
		const ok = await confirmedAttempt({ title: 'Delete gateway', message: `Delete gateway "${g.id}"? Requests to ${g.defaultHostname || 'it'} stop being served.`, confirmLabel: 'Delete' }, () => deleteGateway(g.name), `Deleting ${g.id}`, `Failed to delete ${g.id}`);
		if (ok) await refresh();
	}

	function tryOn(g: GwGateway) {
		tryGatewayName = g.name;
		tab = 'try';
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' || s === 'SUCCEEDED' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';

	onMount(boot);
</script>

<div class="space-y-6 pb-20" data-testid="agw-dashboard">
	<PageHeader icon={Waypoints} title="API Gateway" description="APIs, configs, gateways and operations" color="blue" onRefresh={refresh} />

	{#if error && !project}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="agw-error">
			<p class="font-medium">Could not load API Gateway</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={boot}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if !project}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="agw-empty">No API Gateway resources yet. Create an API, a config and a gateway with the Google Cloud API Gateway client.</p>
	{:else}
		<div class="flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
			<label for="agw-project">Project</label>
			<select id="agw-project" class="{GCP_INPUT} !w-56 !py-1 font-mono text-xs" value={project} onchange={(e) => pickProject(e.currentTarget.value)} data-testid="agw-project">
				{#each projects as p (p)}<option value={p}>{p}</option>{/each}
			</select>
		</div>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="agw-scope-error">{error}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

		{#if tab === 'apis'}
			<ApiGatewayApis {project} {operations} {refreshKey} onChanged={() => { tab = 'gateways'; void refresh(); }} />
		{:else if tab === 'gateways'}
			{#if gateways.length === 0}
				<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="agw-gateways-empty">No gateways. Create one from an active API config.</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="agw-gateway-table">
						<thead class={thead}><tr><th class={cell}>Gateway</th><th class={cell}>State</th><th class={cell}>Region</th><th class={cell}>Default hostname</th><th class={cell}>API config</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each gateways as g (g.name)}
								<tr data-testid="agw-gateway">
									<td class="{cell} font-mono text-xs">{g.id}{g.displayName && g.displayName !== g.id ? ` (${g.displayName})` : ''}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(g.state)}" data-testid="agw-gateway-state">{g.state || '—'}</span></td>
									<td class="{cell} font-mono text-xs" data-testid="agw-gateway-region">{g.location}</td>
									<td class="{cell} font-mono text-xs break-all" data-testid="agw-gateway-host">{g.defaultHostname || '—'}</td>
									<td class="{cell} font-mono text-xs break-all" data-testid="agw-gateway-config">{g.apiConfig ? shortName(g.apiConfig) : '—'}</td>
									<td class="{cell} text-right whitespace-nowrap space-x-1">
										<button class={GCP_MINI} onclick={() => tryOn(g)} disabled={!isActive(g)} data-testid="agw-gateway-try">Try it</button>
										<button class={GCP_MINI} onclick={() => remove(g)} data-testid="agw-gateway-delete">Delete</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if tab === 'operations'}
			{#if operations.length === 0}
				<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="agw-ops-empty">No operations.</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="agw-op-table">
						<thead class={thead}><tr><th class={cell}>Operation</th><th class={cell}>Action</th><th class={cell}>Target</th><th class={cell}>Status</th><th class={cell}>Started</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each operations as o (o.name)}
								<tr data-testid="agw-op">
									<td class="{cell} font-mono text-xs">{o.id}</td>
									<td class="{cell} text-xs" data-testid="agw-op-verb">{o.verb || '—'}</td>
									<td class="{cell} font-mono text-xs break-all">{o.target ? o.target.replace(/^projects\/[^/]+\/locations\//, '') : '—'}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(o.status)}" data-testid="agw-op-status">{o.status}</span>{#if o.statusMessage && o.status === 'FAILED'}<span class="ml-2 text-xs text-red-600">{o.statusMessage}</span>{/if}</td>
									<td class="{cell} text-xs">{o.createTime || '—'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if activeGateways.length === 0}
			<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="agw-try-none">No ACTIVE gateway to send requests to.</p>
		{:else}
			<TryItPanel
				prefix="agw"
				bind:method
				bind:path
				resetKey={tryGatewayName}
				disabled={!target || !isActive(target)}
				note="Sent in-process to the data plane of the selected gateway; the destination cannot be changed. The API key is sent as the key query parameter and is never stored or shown again."
				send={(r) => tryGateway(tryGatewayName, { ...r, apiKey })}
			>
				{#snippet fields()}
					<div class="flex flex-wrap items-end gap-3 text-sm">
						<label>Gateway
							<select class="{GCP_INPUT} !py-1 font-mono text-xs" bind:value={tryGatewayName} data-testid="agw-try-gateway">
								{#each activeGateways as g (g.name)}<option value={g.name}>{g.location}/{g.id}</option>{/each}
							</select>
						</label>
						<label>API key (optional)
							<input class="{GCP_INPUT} !py-1 font-mono text-xs" type="password" autocomplete="off" bind:value={apiKey} data-testid="agw-try-key" />
						</label>
					</div>
				{/snippet}
			</TryItPanel>
		{/if}
	{/if}
</div>
