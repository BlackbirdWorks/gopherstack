<script lang="ts">
	import { onMount } from 'svelte';
	import { Server } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import ComputeTabContent from '#lib/components/gcp/ComputeTabContent.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { ALL_REGIONS, ALL_ZONES, listRegionNames, listZoneNames, readStoredComputeScope, regionOfZone, writeStoredComputeScope } from '#lib/gcpcompute.ts';

	const TABS = [
		{ id: 'instances', label: 'Instances' },
		{ id: 'storage', label: 'Disks, images & snapshots' },
		{ id: 'networks', label: 'Networks' },
		{ id: 'lb', label: 'Load balancing' },
		{ id: 'armor', label: 'Cloud Armor' },
		{ id: 'operations', label: 'Operations' }
	];

	const stored = readStoredComputeScope();
	let project = $state(stored.project);
	let zone = $state(stored.zone);
	let region = $state(stored.region);
	let zones = $state<string[]>([]);
	let regions = $state<string[]>([]);
	let tab = $state('instances');
	let refresh = $state(0);

	const zoneChoices = $derived(region ? zones.filter((z) => regionOfZone(z) === region) : zones);

	async function loadScope() {
		try {
			[zones, regions] = await Promise.all([listZoneNames(project), listRegionNames(project)]);
		} catch {
			zones = [];
			regions = [];
		}
	}

	function persist() {
		writeStoredComputeScope(project, zone, region);
	}

	function applyScope(p: string) {
		project = p;
		persist();
		void loadScope();
	}

	function pickZone(event: Event) {
		zone = (event.currentTarget as HTMLSelectElement).value;
		if (zone) region = regionOfZone(zone);
		persist();
	}

	function pickRegion(event: Event) {
		region = (event.currentTarget as HTMLSelectElement).value;
		if (zone && regionOfZone(zone) !== region) zone = ALL_ZONES;
		persist();
	}

	onMount(() => void loadScope());
</script>

<div class="space-y-6 pb-20" data-testid="compute-dashboard">
	<PageHeader icon={Server} title="Compute Engine" description="Google Compute Engine: instances, disks, networks, load balancing, Cloud Armor and operations" color="blue" onRefresh={() => (refresh += 1)} />
	<div class="flex flex-wrap items-center gap-4">
		<GcpScopeBar prefix="compute" {project} hint="Resources are listed per project." onApply={applyScope} />
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Region
			<select class="{GCP_INPUT} w-40 !py-1 font-mono text-xs" value={region} onchange={pickRegion} data-testid="compute-region">
				<option value={ALL_REGIONS}>All regions</option>
				{#each regions as r (r)}<option value={r}>{r}</option>{/each}
			</select>
		</label>
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Zone
			<select class="{GCP_INPUT} w-48 !py-1 font-mono text-xs" value={zone} onchange={pickZone} data-testid="compute-zone">
				<option value={ALL_ZONES}>All zones</option>
				{#each zoneChoices as z (z)}<option value={z}>{z}</option>{/each}
			</select>
		</label>
	</div>
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	<ComputeTabContent {tab} {project} {zone} {region} zones={zoneChoices} {refresh} />
</div>
