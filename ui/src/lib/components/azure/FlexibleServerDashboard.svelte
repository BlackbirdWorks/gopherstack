<script lang="ts">
	import { onMount } from 'svelte';
	import { Database, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import {
		addDatabase,
		addFirewallRule,
		canAct,
		deleteDatabase,
		deleteFirewallRule,
		engineLabel,
		isNonDefault,
		listConfigurations,
		listDatabases,
		listFirewallRules,
		listServers,
		powerAction,
		validChildName,
		validCharset,
		validIpv4,
		type FlexConfig,
		type FlexDatabase,
		type FlexFirewallRule,
		type FlexServer,
		type PowerAction
	} from '#lib/azureflexdb.ts';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'databases', label: 'Databases' },
		{ id: 'firewall', label: 'Firewall rules' },
		{ id: 'config', label: 'Configuration' }
	];

	let subscription = $state('');
	let servers = $state<FlexServer[]>([]);
	let loading = $state(true);
	let error = $state('');
	let group = $state('');
	let selected = $state('');
	let tab = $state('databases');
	let databases = $state<FlexDatabase[]>([]);
	let rules = $state<FlexFirewallRule[]>([]);
	let configs = $state<FlexConfig[]>([]);
	let showAll = $state(false);
	let detailError = $state('');
	let dbName = $state('');
	let dbCharset = $state('');
	let dbCollation = $state('');
	let ruleName = $state('');
	let ruleStart = $state('');
	let ruleEnd = $state('');

	const groups = $derived([...new Set(servers.map((s) => s.resourceGroup))].toSorted());
	const visible = $derived(group ? servers.filter((s) => s.resourceGroup === group) : servers);
	const current = $derived(servers.find((s) => s.id === selected));
	const shownConfigs = $derived(showAll ? configs : configs.filter((c) => isNonDefault(c)));
	const canAddDb = $derived(validChildName(dbName) && validCharset(dbCharset) && validCharset(dbCollation));
	const canAddRule = $derived(validChildName(ruleName) && validIpv4(ruleStart) && validIpv4(ruleEnd));

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			servers = await listServers(subscription);
			error = '';
			if (!servers.some((s) => s.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadDetail();
	}

	async function loadDetail() {
		const s = current;
		if (!s) return;
		detailError = '';
		try {
			[databases, rules, configs] = await Promise.all([listDatabases(subscription, s), listFirewallRules(subscription, s), listConfigurations(subscription, s)]);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function select(s: FlexServer) {
		selected = selected === s.id ? '' : s.id;
		databases = [];
		rules = [];
		configs = [];
		if (selected) void loadDetail();
	}

	async function power(s: FlexServer, action: PowerAction) {
		const run = () => powerAction(subscription, s, action);
		const past = { start: 'Start', stop: 'Stop', restart: 'Restart' }[action];
		const ok =
			action === 'stop'
				? await confirmedAttempt({ title: 'Stop server', message: `Stop "${s.name}"? Connections are dropped until it is started again.`, confirmLabel: 'Stop' }, run, `${past} requested for ${s.name}`, `Failed to stop ${s.name}`)
				: await attempt(run, `${past} requested for ${s.name}`, `Failed to ${action} ${s.name}`);
		if (ok) await load();
	}

	async function createDb(event: Event, s: FlexServer) {
		event.preventDefault();
		if (!canAddDb) return;
		const ok = await attempt(() => addDatabase(subscription, s, dbName.trim(), dbCharset, dbCollation), `Database ${dbName} added`, `Failed to add database ${dbName}`);
		if (ok) {
			dbName = dbCharset = dbCollation = '';
			await loadDetail();
		}
	}

	async function removeDb(s: FlexServer, d: FlexDatabase) {
		const ok = await confirmedAttempt({ title: 'Delete database', message: `Delete database "${d.name}" from ${s.name}?`, confirmLabel: 'Delete' }, () => deleteDatabase(subscription, s, d.name), `Database ${d.name} deleted`, `Failed to delete ${d.name}`);
		if (ok) await loadDetail();
	}

	async function createRule(event: Event, s: FlexServer) {
		event.preventDefault();
		if (!canAddRule) return;
		const ok = await attempt(() => addFirewallRule(subscription, s, ruleName.trim(), ruleStart, ruleEnd), `Rule ${ruleName} added`, `Failed to add rule ${ruleName}`);
		if (ok) {
			ruleName = ruleStart = ruleEnd = '';
			await loadDetail();
		}
	}

	async function removeRule(s: FlexServer, r: FlexFirewallRule) {
		const ok = await confirmedAttempt({ title: 'Delete firewall rule', message: `Delete rule "${r.name}" from ${s.name}?`, confirmLabel: 'Delete' }, () => deleteFirewallRule(subscription, s, r.name), `Rule ${r.name} deleted`, `Failed to delete ${r.name}`);
		if (ok) await loadDetail();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'Ready' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'Failed' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="flexdb-dashboard">
	<PageHeader icon={Database} title="Flexible Server" description="Azure Database for PostgreSQL and MySQL flexible servers" color="sky" onRefresh={load} />

	{#if error && servers.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="flexdb-error">
			<p class="font-medium">Could not load servers</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if servers.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="flexdb-empty">No flexible servers. Create one through Resource Manager (Microsoft.DBforPostgreSQL or Microsoft.DBforMySQL flexibleServers).</p>
	{:else}
		<div class="flex items-center gap-2 text-xs text-gray-500">
			<label for="flexdb-group">Resource group</label>
			<select id="flexdb-group" class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={group} data-testid="flexdb-group">
				<option value="">All resource groups</option>
				{#each groups as g (g)}<option value={g}>{g}</option>{/each}
			</select>
		</div>

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="flexdb-servers">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Server</th><th class={cell}>Engine</th><th class={cell}>Resource group</th><th class={cell}>State</th><th class={cell}>Version</th><th class={cell}>SKU</th><th class={cell}>Storage</th><th class={cell}>HA</th><th class={cell}>FQDN</th><th class={cell}>Endpoint</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each visible as s (s.id)}
						<tr class={selected === s.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="flexdb-server">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(s)} aria-expanded={selected === s.id} data-testid="flexdb-select">{s.name}</button></td>
							<td class="{cell} text-xs" data-testid="flexdb-engine">{engineLabel(s.engine)}</td>
							<td class="{cell} font-mono text-xs">{s.resourceGroup}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(s.state)}" data-testid="flexdb-state">{s.state || '—'}</span></td>
							<td class="{cell} font-mono text-xs" data-testid="flexdb-version">{s.version || '—'}</td>
							<td class="{cell} text-xs" data-testid="flexdb-sku">{s.sku || '—'}{s.tier ? ` (${s.tier})` : ''}</td>
							<td class="{cell} text-xs" data-testid="flexdb-storage">{s.storageGb} GiB{s.iops ? `, ${s.iops} IOPS` : ''}</td>
							<td class="{cell} text-xs" data-testid="flexdb-ha">{s.haMode || '—'}</td>
							<td class="{cell} font-mono text-xs break-all" data-testid="flexdb-fqdn">{s.fqdn || '—'}</td>
							<td class="{cell} font-mono text-xs" data-testid="flexdb-endpoint">{s.endpoint || '—'}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								<button class={GCP_MINI} onclick={() => power(s, 'start')} disabled={!canAct(s.state, 'start')} data-testid="flexdb-start">Start</button>
								<button class={GCP_MINI} onclick={() => power(s, 'stop')} disabled={!canAct(s.state, 'stop')} data-testid="flexdb-stop">Stop</button>
								<button class={GCP_MINI} onclick={() => power(s, 'restart')} disabled={!canAct(s.state, 'restart')} data-testid="flexdb-restart">Restart</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="flexdb-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name}</h2>
				<p class="text-xs text-gray-500">Administrator login <span class="font-mono" data-testid="flexdb-login">{current.login || '—'}</span>. The administrator password is never shown.</p>
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="flexdb-detail-error">{detailError}</p>{/if}
				<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="sky" />

				{#if tab === 'databases'}
					<form class="flex flex-wrap items-end gap-2" onsubmit={(e) => createDb(e, current)} data-testid="flexdb-db-form">
						<label class="text-xs text-gray-500">Name<input class="{GCP_INPUT} mt-1" bind:value={dbName} data-testid="flexdb-db-name" /></label>
						<label class="text-xs text-gray-500">Charset (optional)<input class="{GCP_INPUT} mt-1" bind:value={dbCharset} data-testid="flexdb-db-charset" /></label>
						<label class="text-xs text-gray-500">Collation (optional)<input class="{GCP_INPUT} mt-1" bind:value={dbCollation} data-testid="flexdb-db-collation" /></label>
						<button class={GCP_PRIMARY} type="submit" disabled={!canAddDb} data-testid="flexdb-db-add">Add database</button>
					</form>
					<table class="w-full text-sm" data-testid="flexdb-databases">
						<thead class="text-left text-gray-500"><tr><th class={cell}>Name</th><th class={cell}>Charset</th><th class={cell}>Collation</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each databases as d (d.name)}
								<tr data-testid="flexdb-database"><td class="{cell} font-mono text-xs">{d.name}</td><td class="{cell} text-xs">{d.charset || '—'}</td><td class="{cell} text-xs">{d.collation || '—'}</td><td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeDb(current, d)} data-testid="flexdb-db-delete">Delete</button></td></tr>
							{/each}
						</tbody>
					</table>
				{:else if tab === 'firewall'}
					<form class="flex flex-wrap items-end gap-2" onsubmit={(e) => createRule(e, current)} data-testid="flexdb-rule-form">
						<label class="text-xs text-gray-500">Rule name<input class="{GCP_INPUT} mt-1" bind:value={ruleName} data-testid="flexdb-rule-name" /></label>
						<label class="text-xs text-gray-500">Start IP<input class="{GCP_INPUT} mt-1" bind:value={ruleStart} placeholder="10.0.0.1" data-testid="flexdb-rule-start" /></label>
						<label class="text-xs text-gray-500">End IP<input class="{GCP_INPUT} mt-1" bind:value={ruleEnd} placeholder="10.0.0.9" data-testid="flexdb-rule-end" /></label>
						<button class={GCP_PRIMARY} type="submit" disabled={!canAddRule} data-testid="flexdb-rule-add">Add rule</button>
					</form>
					{#if rules.length === 0}
						<p class="text-sm text-gray-500" data-testid="flexdb-rules-empty">No firewall rules.</p>
					{:else}
						<table class="w-full text-sm" data-testid="flexdb-rules">
							<thead class="text-left text-gray-500"><tr><th class={cell}>Name</th><th class={cell}>Start IP</th><th class={cell}>End IP</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each rules as r (r.name)}
									<tr data-testid="flexdb-rule"><td class="{cell} font-mono text-xs">{r.name}</td><td class="{cell} font-mono text-xs">{r.start}</td><td class="{cell} font-mono text-xs">{r.end}</td><td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeRule(current, r)} data-testid="flexdb-rule-delete">Delete</button></td></tr>
								{/each}
							</tbody>
						</table>
					{/if}
				{:else}
					<label class="flex items-center gap-2 text-xs text-gray-500"><input type="checkbox" bind:checked={showAll} data-testid="flexdb-config-all" />Show all {configs.length} parameters (default view: changed or pending restart)</label>
					{#if shownConfigs.length === 0}
						<p class="text-sm text-gray-500" data-testid="flexdb-configs-empty">All parameters are at their defaults.</p>
					{:else}
						<table class="w-full text-sm" data-testid="flexdb-configs">
							<thead class="text-left text-gray-500"><tr><th class={cell}>Parameter</th><th class={cell}>Value</th><th class={cell}>Default</th><th class={cell}>Source</th><th class={cell}>Restart</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each shownConfigs as c (c.name)}
									<tr data-testid="flexdb-config"><td class="{cell} font-mono text-xs">{c.name}</td><td class="{cell} font-mono text-xs" data-testid="flexdb-config-value">{c.value}</td><td class="{cell} font-mono text-xs">{c.defaultValue}</td><td class="{cell} text-xs">{c.source}</td><td class="{cell} text-xs" data-testid="flexdb-config-pending">{c.pendingRestart ? 'Pending restart' : '—'}</td></tr>
								{/each}
							</tbody>
						</table>
					{/if}
				{/if}
			</div>
		{/if}
	{/if}
</div>
