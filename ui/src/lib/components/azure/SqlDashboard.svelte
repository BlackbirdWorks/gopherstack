<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Database, Inbox, Trash2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import SqlQuery from '#lib/components/azure/SqlQuery.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import { attempt, confirmedAttempt, GCP_GHOST, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		deleteFirewallRule,
		formatMaxSize,
		listDatabases,
		listFirewallRules,
		listServers,
		putFirewallRule,
		validateRule,
		type SqlDatabase,
		type SqlFirewallRule,
		type SqlServer
	} from '#lib/azuresql.ts';

	let sub = $state('');
	let servers = $state<SqlServer[]>([]);
	let selectedId = $state('');
	let dbs = $state<SqlDatabase[]>([]);
	let rules = $state<SqlFirewallRule[]>([]);
	let tab = $state<'databases' | 'firewall' | 'query'>('databases');
	let loading = $state(true);
	let loadError = $state('');
	let fName = $state('');
	let fStart = $state('');
	let fEnd = $state('');
	let fTouched = $state(false);

	const server = $derived(servers.find((s) => s.id === selectedId));
	const ruleError = $derived(validateRule(fName, fStart, fEnd));
	const TABS = [
		['databases', 'Databases'],
		['firewall', 'Firewall rules'],
		['query', 'Query']
	] as const;

	async function loadChildren() {
		if (!server) {
			dbs = [];
			rules = [];
			return;
		}
		[dbs, rules] = await Promise.all([listDatabases(server, sub), listFirewallRules(server, sub)]);
	}

	async function load() {
		loading = true;
		loadError = '';
		try {
			if (!sub) sub = await getSubscriptionId();
			servers = await listServers(sub);
			if (!servers.some((s) => s.id === selectedId)) selectedId = servers[0]?.id ?? '';
			await loadChildren();
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Azure SQL: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function pick(id: string) {
		selectedId = id;
		await attempt(loadChildren, '', 'Failed to load server details');
	}

	async function addRule(e: Event) {
		e.preventDefault();
		fTouched = true;
		if (!server || ruleError) return;
		const ok = await attempt(() => putFirewallRule(server, sub, fName, fStart, fEnd), `Saved rule ${fName}`, `Failed to save ${fName}`);
		if (ok) {
			fName = fStart = fEnd = '';
			fTouched = false;
			await attempt(loadChildren, '', 'Failed to reload rules');
		}
	}

	async function removeRule(r: SqlFirewallRule) {
		if (!server) return;
		const ok = await confirmedAttempt(
			{ title: 'Delete firewall rule', message: `Delete rule "${r.name}"?` },
			() => deleteFirewallRule(server, sub, r.name),
			`Deleted ${r.name}`,
			`Failed to delete ${r.name}`
		);
		if (ok) await attempt(loadChildren, '', 'Failed to reload rules');
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="sql-dashboard">
	<PageHeader icon={Database} title="SQL Database" description="Azure SQL logical servers, databases, firewall rules and a read-only query runner" color="sky" onRefresh={load} />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="sql-error">
			<p class="font-medium">Could not reach Azure SQL</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if servers.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="sql-empty">
			<Inbox class="mb-3 h-10 w-10 opacity-30" />
			<p>No SQL servers. Create one with the Azure CLI, Terraform or ARM.</p>
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="sql-servers">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Server</th><th class="px-4 py-2">Resource group</th><th class="px-4 py-2">Location</th><th class="px-4 py-2">State</th><th class="px-4 py-2">Admin login</th><th class="px-4 py-2">Min TLS</th><th class="px-4 py-2">Endpoint</th></tr></thead>
				<tbody>
					{#each servers as s (s.id)}
						<tr class="border-t border-gray-100 dark:border-gray-800 {s.id === selectedId ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''}" data-testid="sql-server">
							<td class="px-4 py-2 font-mono"><button class="underline decoration-dotted" onclick={() => pick(s.id)} data-testid="sql-select">{s.name}</button></td>
							<td class="px-4 py-2">{s.resourceGroup}</td>
							<td class="px-4 py-2">{s.location}</td>
							<td class="px-4 py-2" data-testid="sql-state">{s.state}</td>
							<td class="px-4 py-2 font-mono">{s.login || '(Entra only)'}</td>
							<td class="px-4 py-2">{s.minTls}</td>
							<td class="px-4 py-2 font-mono text-xs">{s.endpoint || s.fqdn}{s.endpoint ? '' : ' (state-only)'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if server}
			<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700" role="tablist">
				{#each TABS as [id, label] (id)}
					<button role="tab" aria-selected={tab === id} class="px-3 py-2 text-sm {tab === id ? 'border-b-2 border-blue-600 font-medium text-blue-600' : 'text-gray-500'}" onclick={() => (tab = id)} data-testid="sql-tab-{id}">{label}</button>
				{/each}
			</div>

			{#if tab === 'databases'}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="sql-databases">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Database</th><th class="px-4 py-2">SKU</th><th class="px-4 py-2">Tier</th><th class="px-4 py-2">Status</th><th class="px-4 py-2">Max size</th><th class="px-4 py-2">Collation</th></tr></thead>
						<tbody>
							{#each dbs as d (d.name)}
								<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="sql-database">
									<td class="px-4 py-2 font-mono">{d.name}</td>
									<td class="px-4 py-2" data-testid="sql-db-sku">{d.sku || '-'}</td>
									<td class="px-4 py-2">{d.tier || '-'}</td>
									<td class="px-4 py-2" data-testid="sql-db-status">{d.status}</td>
									<td class="px-4 py-2">{formatMaxSize(d.maxSizeBytes)}</td>
									<td class="px-4 py-2 text-xs">{d.collation}</td>
								</tr>
							{:else}
								<tr><td class="px-4 py-6 text-center text-gray-500" colspan="6">No databases.</td></tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else if tab === 'firewall'}
				<form class="flex flex-wrap items-end gap-3" onsubmit={addRule} data-testid="sql-rule-form">
					<label class="text-sm">Name<input class={GCP_INPUT} bind:value={fName} data-testid="sql-rule-name" /></label>
					<label class="text-sm">Start IP<input class={GCP_INPUT} bind:value={fStart} placeholder="10.0.0.1" data-testid="sql-rule-start" /></label>
					<label class="text-sm">End IP<input class={GCP_INPUT} bind:value={fEnd} placeholder="10.0.0.255" data-testid="sql-rule-end" /></label>
					<button type="submit" class={GCP_GHOST} data-testid="sql-rule-save">Add rule</button>
					{#if fTouched && ruleError}<span class="text-sm text-red-600">{ruleError}</span>{/if}
				</form>
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="sql-rules">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Rule</th><th class="px-4 py-2">Start IP</th><th class="px-4 py-2">End IP</th><th class="px-4 py-2"></th></tr></thead>
						<tbody>
							{#each rules as r (r.name)}
								<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="sql-rule">
									<td class="px-4 py-2 font-mono">{r.name}</td>
									<td class="px-4 py-2 font-mono">{r.start}</td>
									<td class="px-4 py-2 font-mono">{r.end}</td>
									<td class="px-4 py-2 text-right"><button class={GCP_MINI} onclick={() => removeRule(r)} aria-label="Delete rule {r.name}"><Trash2 class="inline h-3 w-3" /></button></td>
								</tr>
							{:else}
								<tr><td class="px-4 py-6 text-center text-gray-500" colspan="4">No firewall rules.</td></tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				{#key server.id}<SqlQuery {server} databases={dbs.map((d) => d.name)} />{/key}
			{/if}
		{/if}
	{/if}
</div>
