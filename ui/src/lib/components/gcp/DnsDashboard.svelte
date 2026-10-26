<script lang="ts">
	import { onMount } from 'svelte';
	import { Globe, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		deleteRecordSet,
		deleteZone,
		listChanges,
		listRecordSets,
		listZones,
		readStoredDnsProject,
		summarizeChange,
		writeStoredDnsProject,
		type ChangeInfo,
		type RecordSet,
		type ZoneInfo
	} from '#lib/gcpdns.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'records', label: 'Record sets' },
		{ id: 'changes', label: 'Change history' }
	];

	let project = $state(readStoredDnsProject());
	let zones = $state<ZoneInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let zone = $state('');
	let tab = $state('records');
	let records = $state<RecordSet[]>([]);
	let changes = $state<ChangeInfo[]>([]);
	let childLoading = $state(false);
	let childError = $state('');

	async function load() {
		try {
			zones = (await listZones(project)).toSorted((a, b) => a.name.localeCompare(b.name));
			error = '';
			if (!zones.some((z) => z.name === zone)) zone = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChild() {
		if (!zone) return;
		childLoading = true;
		childError = '';
		try {
			if (tab === 'records') records = await listRecordSets(project, zone);
			else changes = await listChanges(project, zone);
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	function applyScope(p: string) {
		project = p;
		writeStoredDnsProject(p);
		zone = '';
		loading = true;
		void load();
	}

	function selectZone(row: ResourceRow) {
		zone = zone === row.id ? '' : row.id;
		tab = 'records';
		void loadChild();
	}

	function selectTab(id: string) {
		tab = id;
		void loadChild();
	}

	async function removeZone(row: ResourceRow) {
		if (!(await confirmDestructive({ title: 'Delete managed zone', message: `Delete managed zone "${row.id}"? It must have no record sets other than the default NS and SOA.` }))) return;
		if (await attempt(() => deleteZone(project, row.id), `Deleted ${row.id}`, `Failed to delete ${row.id}`)) await load();
	}

	async function removeRecord(rr: RecordSet) {
		if (!(await confirmDestructive({ title: 'Delete record set', message: `Delete ${rr.type} record set "${rr.name}"?` }))) return;
		if (await attempt(() => deleteRecordSet(project, zone, rr), `Deleted ${rr.name}`, `Failed to delete ${rr.name}`)) await loadChild();
	}

	const rows = $derived(zones.map((z) => ({ id: z.name, cells: [z.name, z.dnsName, z.visibility, z.description || '—', z.creationTime], raw: z.raw })));
	const cell = 'px-4 py-2';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="dns-dashboard">
	<PageHeader icon={Globe} title="Cloud DNS" description="Google Cloud DNS: managed zones, record sets and change history" color="blue" onRefresh={load} />
	<GcpScopeBar prefix="dns" {project} hint="Zones are listed per project." onApply={applyScope} />
	<GcpResourceTable testId="dns-zone" noun="managed zones" columns={['Zone', 'DNS name', 'Visibility', 'Description', 'Created']} {rows} {loading} {error} selected={zone} onRetry={load} onSelect={selectZone} onDelete={removeZone} />

	{#if zone}
		<div class="space-y-3" data-testid="dns-detail">
			<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />
			{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="dns-detail-error">{childError}</p>{/if}
			{#if childLoading}
				<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
			{:else if tab === 'records'}
				{#if records.length === 0}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="dns-records-empty">No record sets</p>
				{:else}
					<table class="w-full text-sm" data-testid="dns-records">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>TTL</th><th class={cell}>Data</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each records as r (`${r.name}|${r.type}`)}
								<tr data-testid="dns-record">
									<td class="{cell} font-mono text-xs break-all">{r.name}</td><td class="{cell} text-xs">{r.type}</td><td class="{cell} text-xs">{r.ttl}</td>
									<td class="{cell} font-mono text-xs break-all">{r.rrdatas.join(', ')}</td>
									<td class="{cell} text-right"><button class="{GCP_MINI} text-red-600" onclick={() => removeRecord(r)} aria-label="Delete {r.name} {r.type}" data-testid="dns-record-delete">Delete</button></td>
								</tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{:else if changes.length === 0}
				<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="dns-changes-empty">No changes</p>
			{:else}
				<table class="w-full text-sm" data-testid="dns-changes">
					<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Change</th><th class={cell}>Status</th><th class={cell}>Started</th><th class={cell}>Modifications</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each changes as c (c.id)}
							<tr data-testid="dns-change"><td class="{cell} font-mono text-xs">{c.id}</td><td class="{cell} text-xs">{c.status}</td><td class="{cell} text-xs">{c.startTime}</td>
								<td class="{cell} font-mono text-xs break-all">{#each summarizeChange(c) as line, i (i)}<div>{line}</div>{/each}</td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{/if}
</div>
