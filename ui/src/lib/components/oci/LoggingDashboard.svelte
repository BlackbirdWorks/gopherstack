<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw, ScrollText } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		TIME_RANGES,
		defaultQuery,
		listCompartments,
		listLogGroups,
		listLogs,
		pushTestEntry,
		readStoredLoggingCompartment,
		searchLogs,
		setLogEnabled,
		writeStoredLoggingCompartment,
		type LogGroup,
		type LogInfo,
		type SearchResult
	} from '#lib/ocilogging.ts';

	const TABS = [
		{ id: 'groups', label: 'Log groups' },
		{ id: 'search', label: 'Search' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredLoggingCompartment());
	let groups = $state<LogGroup[]>([]);
	let logs = $state<LogInfo[]>([]);
	let everyLog = $state<LogInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let tab = $state('groups');
	let logsError = $state('');
	let query = $state('');
	let range = $state('1h');
	let result = $state<SearchResult | null>(null);
	let searching = $state(false);
	let searchError = $state('');
	let testData = $state('hello from the dashboard');
	let pushLogId = $state('');

	async function loadGroups() {
		if (!compartment) {
			groups = [];
			return;
		}
		try {
			groups = await listLogGroups(compartment);
			everyLog = (await Promise.all(groups.map((g) => listLogs(g.id)))).flat();
			error = '';
			if (!groups.some((g) => g.id === selected)) selected = '';
			if (!query) query = defaultQuery(compartment);
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadLogs();
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadGroups();
		loading = false;
	}

	async function loadLogs() {
		logsError = '';
		if (!selected) {
			logs = [];
			return;
		}
		try {
			logs = await listLogs(selected);
			everyLog = [...everyLog.filter((l) => l.groupId !== selected), ...logs];
		} catch (e) {
			logsError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredLoggingCompartment(compartment);
		selected = '';
		query = defaultQuery(compartment);
		loading = true;
		void loadGroups();
	}

	function select(g: LogGroup) {
		selected = selected === g.id ? '' : g.id;
		void loadLogs();
	}

	async function toggle(l: LogInfo) {
		const ok = await attempt(() => setLogEnabled(l.groupId || selected, l.id, !l.enabled), `${l.enabled ? 'Disabled' : 'Enabled'} ${l.name}`, `Failed to update ${l.name}`);
		if (ok) await loadLogs();
	}

	function searchLog(g: LogGroup, l: LogInfo) {
		query = defaultQuery(compartment, g.name, l.name);
		pushLogId = l.id;
		tab = 'search';
	}

	async function run() {
		searching = true;
		searchError = '';
		try {
			const seconds = TIME_RANGES.find((r) => r.id === range)?.seconds ?? 3600;
			const end = new Date(Date.now() + 60_000);
			result = await searchLogs(query, new Date(end.getTime() - (seconds + 60) * 1000), end);
		} catch (e) {
			searchError = describeGcpError(e);
			result = null;
		} finally {
			searching = false;
		}
	}

	async function push() {
		const ok = await attempt(() => pushTestEntry(pushLogId, testData), 'Test entry ingested', 'Failed to ingest entry');
		if (ok) await run();
	}

	const allLogs = $derived(everyLog.filter((l) => l.state === 'ACTIVE' && l.enabled));
	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : s === 'INACTIVE' ? 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="logging-dashboard">
	<PageHeader icon={ScrollText} title="Logging" description="Oracle Cloud Logging: log groups, logs and log search" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="logging-error">
			<p class="font-medium">Could not load log groups</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="logging-empty">No log groups yet. Create one with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="logging-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="logging-list-error">{error}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

		{#if tab === 'groups'}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="logging-groups">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Log group</th><th class={cell}>State</th><th class={cell}>Description</th><th class={cell}>Created</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each groups as g (g.id)}
							<tr class={selected === g.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="logging-group">
								<td class="{cell} font-mono text-xs" title={g.id}><button class="underline decoration-dotted" onclick={() => select(g)} aria-expanded={selected === g.id} data-testid="logging-select">{g.name}</button></td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(g.state)}">{g.state}</span></td>
								<td class="{cell} text-xs">{g.description || '—'}</td>
								<td class="{cell} text-xs">{g.created}</td>
							</tr>
						{:else}
							<tr><td colspan="4" class="{cell} text-center text-gray-500" data-testid="logging-none">No log groups in this compartment</td></tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if selected}
				{@const group = groups.find((g) => g.id === selected)}
				<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="logging-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Logs in {group?.name}</h2>
					{#if logsError}<p class="text-sm text-red-600" role="alert" data-testid="logging-logs-error">{logsError}</p>{/if}
					{#if logs.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="logging-logs-empty">This log group has no logs</p>
					{:else}
						<table class="w-full text-sm" data-testid="logging-logs">
							<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Log</th><th class={cell}>Type</th><th class={cell}>State</th><th class={cell}>Enabled</th><th class={cell}>Retention</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each logs as l (l.id)}
									<tr data-testid="logging-log">
										<td class="{cell} font-mono text-xs" title={l.id}>{l.name}</td>
										<td class="{cell} text-xs">{l.type}</td>
										<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(l.state)}">{l.state}</span></td>
										<td class="{cell} text-xs" data-testid="logging-log-enabled">{l.enabled ? 'Enabled' : 'Disabled'}</td>
										<td class="{cell} text-xs">{l.retentionDays} days</td>
										<td class="{cell} text-right whitespace-nowrap space-x-1">
											<button class={GCP_MINI} onclick={() => toggle(l)} disabled={l.state !== 'ACTIVE' && l.state !== 'INACTIVE'} data-testid="logging-toggle">{l.enabled ? 'Disable' : 'Enable'}</button>
											{#if group}<button class={GCP_MINI} onclick={() => searchLog(group, l)} data-testid="logging-search-log">Search</button>{/if}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					{/if}
				</div>
			{/if}
		{:else}
			<div class="space-y-3" data-testid="logging-search">
				<label class="block text-xs text-gray-500 dark:text-gray-400" for="logging-query">Query (scope, then where, sort by, top, limit and count)</label>
				<textarea id="logging-query" class="{GCP_INPUT} font-mono text-xs" rows="3" bind:value={query} data-testid="logging-query"></textarea>
				<div class="flex flex-wrap items-center gap-2">
					<select class="{GCP_INPUT} !w-auto" bind:value={range} aria-label="Time range" data-testid="logging-range">
						{#each TIME_RANGES as r (r.id)}<option value={r.id}>{r.label}</option>{/each}
					</select>
					<button class={GCP_PRIMARY} onclick={run} disabled={searching || !query.trim()} data-testid="logging-run">Search</button>
					{#if searching}<RefreshCw class="h-4 w-4 animate-spin text-gray-400" />{/if}
				</div>
				{#if searchError}<p class="text-sm text-red-600" role="alert" data-testid="logging-search-error">{searchError}</p>{/if}
				{#if result}
					<p class="text-xs text-gray-500" data-testid="logging-count">{result.count} result{result.count === 1 ? '' : 's'}{result.more ? ' (more available, narrow the query)' : ''}</p>
					{#if result.rows.length === 0}
						<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="logging-results-empty">No matching log entries</p>
					{:else}
						<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
							<table class="w-full text-xs" data-testid="logging-results">
								<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr>{#each result.columns as c (c)}<th class={cell}>{c}</th>{/each}</tr></thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each result.rows as row, i (i)}
										<tr data-testid="logging-result">{#each result.columns as c (c)}<td class="{cell} font-mono break-all">{row[c]}</td>{/each}</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				{/if}

				<div class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Ingest a test entry</h3>
					<p class="text-xs text-gray-500">Pick a log with its Search button on the Log groups tab, or choose one here.</p>
					<div class="flex flex-wrap items-center gap-2">
						<select class="{GCP_INPUT} !w-auto !py-1 text-xs" bind:value={pushLogId} aria-label="Log to ingest into" data-testid="logging-push-log">
							<option value="">Select a log</option>
							{#each allLogs as l (l.id)}<option value={l.id}>{l.name}</option>{/each}
							{#if pushLogId && !allLogs.some((l) => l.id === pushLogId)}<option value={pushLogId}>{pushLogId.slice(-12)}</option>{/if}
						</select>
						<input class="{GCP_INPUT} !w-80 !py-1 text-xs" type="text" bind:value={testData} aria-label="Entry data" data-testid="logging-push-data" />
						<button class={GCP_MINI} onclick={push} disabled={!pushLogId || !testData.trim()} data-testid="logging-push">Ingest</button>
					</div>
				</div>
			</div>
		{/if}
	{/if}
</div>
