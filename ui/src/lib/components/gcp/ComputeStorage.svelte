<script lang="ts">
	import { untrack } from 'svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { attempt, confirmedAttempt, GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { createSnapshot, deleteDisk, deleteSnapshot, inZoneScope, labelText, listDisks, listImages, listSnapshots, validResourceName, type ComputeImage, type Disk, type Snapshot } from '#lib/gcpcompute.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { project: string; zone: string; region: string; refresh: number };
	let { project, zone, region, refresh }: Props = $props();

	const TABS = [
		{ id: 'disks', label: 'Disks' },
		{ id: 'images', label: 'Images' },
		{ id: 'snapshots', label: 'Snapshots' }
	];
	let tab = $state('disks');
	let disks = $state<Disk[]>([]);
	let images = $state<ComputeImage[]>([]);
	let snapshots = $state<Snapshot[]>([]);
	let loading = $state(true);
	let error = $state('');
	let snapOf = $state<Disk | null>(null);
	let snapName = $state('');

	const diskById = $derived(new Map(disks.map((d) => [`${d.zone}/${d.name}`, d])));
	const diskRows = $derived(
		disks
			.filter((d) => inZoneScope(d.zone, zone, region))
			.map((d) => ({ id: `${d.zone}/${d.name}`, cells: [d.name, d.zone, d.status, `${d.sizeGb} GB`, d.type, d.users.join(', '), labelText(d.labels)], raw: d.raw }))
	);
	const imageRows = $derived(images.map((i) => ({ id: `${i.project}/${i.name}`, cells: [i.name, i.project, i.family, i.status, i.sizeGb ? `${i.sizeGb} GB` : '', i.created], raw: i.raw })));
	const snapshotRows = $derived(snapshots.map((s) => ({ id: s.name, cells: [s.name, s.status, s.sourceDisk, `${s.sizeGb} GB`, s.created], raw: s.raw })));

	async function load(quiet = false) {
		if (!quiet) loading = true;
		try {
			if (tab === 'disks') disks = await listDisks(project);
			else if (tab === 'images') images = await listImages(project);
			else snapshots = await listSnapshots(project);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void project;
		void refresh;
		untrack(() => void load());
	});

	function selectTab(id: string) {
		tab = id;
		void load();
	}

	async function snapshot() {
		const d = snapOf;
		if (!d || !validResourceName(snapName)) return;
		if (await attempt(() => createSnapshot(project, d.zone, d.name, snapName), `Snapshot ${snapName} requested`, `Failed to snapshot ${d.name}`)) {
			snapOf = null;
			snapName = '';
			await load(true);
		}
	}

	async function removeDisk(d: Disk) {
		if (await confirmedAttempt({ title: 'Delete disk', message: `Delete disk "${d.name}"?` }, () => deleteDisk(project, d.zone, d.name), `Deleted ${d.name}`, `Failed to delete ${d.name}`)) await load(true);
	}

	async function removeSnapshot(name: string) {
		if (await confirmedAttempt({ title: 'Delete snapshot', message: `Delete snapshot "${name}"?` }, () => deleteSnapshot(project, name), `Deleted ${name}`, `Failed to delete ${name}`)) await load(true);
	}
</script>

<div class="space-y-4">
	<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />
	{#if tab === 'disks'}
		{#if snapOf}
			<form class="flex flex-wrap items-end gap-2 rounded-lg border border-gray-200 p-3 text-sm dark:border-gray-700" onsubmit={(e) => { e.preventDefault(); void snapshot(); }} data-testid="compute-snapshot-form">
				<label class="flex-1">Snapshot of {snapOf.name}
					<input class="{GCP_INPUT} font-mono" bind:value={snapName} placeholder="my-snapshot" data-testid="compute-snapshot-name" />
				</label>
				<button type="submit" class={GCP_PRIMARY} disabled={!validResourceName(snapName)} data-testid="compute-snapshot-submit">Create snapshot</button>
				<button type="button" class={GCP_GHOST} onclick={() => (snapOf = null)}>Cancel</button>
			</form>
		{/if}
		<GcpResourceTable testId="compute-disk" noun="disks" columns={['Name', 'Zone', 'Status', 'Size', 'Type', 'In use by', 'Labels']} rows={diskRows} {loading} {error} onRetry={() => load()} onDelete={(r) => { const d = diskById.get(r.id); if (d) void removeDisk(d); }}>
			{#snippet actions(row: ResourceRow)}
				{@const d = diskById.get(row.id)}
				{#if d}<button class={GCP_MINI} onclick={() => { snapOf = d; snapName = ''; }} data-testid="compute-disk-snapshot">Snapshot</button>{/if}
			{/snippet}
		</GcpResourceTable>
	{:else if tab === 'images'}
		<GcpResourceTable testId="compute-image" noun="images" columns={['Name', 'Project', 'Family', 'Status', 'Size', 'Created']} rows={imageRows} {loading} {error} onRetry={() => load()} />
	{:else}
		<GcpResourceTable testId="compute-snapshot" noun="snapshots" columns={['Name', 'Status', 'Source disk', 'Size', 'Created']} rows={snapshotRows} {loading} {error} onRetry={() => load()} onDelete={(r) => void removeSnapshot(r.id)} />
	{/if}
</div>
