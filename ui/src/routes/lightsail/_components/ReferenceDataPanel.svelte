<script lang="ts">
	// Reference data -- family A (9 ops): blueprints, bundles, regions,
	// container powers, relational-database blueprints/bundles, bucket
	// bundles, distribution bundles, and container API metadata.
	//
	// This is small, synthetic SEED data this emulator ships with -- an
	// emulator decision, not AWS's real catalog (services/lightsail/
	// PARITY.md family A / referencedata.go). Every list here is labeled as
	// such so a user does not mistake it for AWS's authoritative catalog.
	import {
		GetBlueprintsCommand,
		GetBundlesCommand,
		GetRegionsCommand,
		GetContainerServicePowersCommand,
		GetRelationalDatabaseBlueprintsCommand,
		GetRelationalDatabaseBundlesCommand,
		GetBucketBundlesCommand,
		GetDistributionBundlesCommand,
		GetContainerAPIMetadataCommand,
		type Blueprint,
		type Bundle,
		type Region,
		type ContainerServicePower,
		type RelationalDatabaseBlueprint,
		type RelationalDatabaseBundle,
		type BucketBundle,
		type DistributionBundle,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	type SubTab =
		| 'blueprints'
		| 'bundles'
		| 'regions'
		| 'containerPowers'
		| 'dbBlueprints'
		| 'dbBundles'
		| 'bucketBundles'
		| 'distributionBundles'
		| 'containerApiMetadata';

	const subTabs: TabDef[] = [
		{ id: 'blueprints', label: 'Blueprints' },
		{ id: 'bundles', label: 'Instance bundles' },
		{ id: 'regions', label: 'Regions' },
		{ id: 'containerPowers', label: 'Container powers' },
		{ id: 'dbBlueprints', label: 'Database blueprints' },
		{ id: 'dbBundles', label: 'Database bundles' },
		{ id: 'bucketBundles', label: 'Bucket bundles' },
		{ id: 'distributionBundles', label: 'Distribution bundles' },
		{ id: 'containerApiMetadata', label: 'Container API metadata' }
	];

	let active = $state<SubTab>('blueprints');
	let loading = $state(false);
	let error = $state<string | null>(null);

	let blueprints = $state<Blueprint[]>([]);
	let bundles = $state<Bundle[]>([]);
	let regions = $state<Region[]>([]);
	let containerPowers = $state<ContainerServicePower[]>([]);
	let dbBlueprints = $state<RelationalDatabaseBlueprint[]>([]);
	let dbBundles = $state<RelationalDatabaseBundle[]>([]);
	let bucketBundles = $state<BucketBundle[]>([]);
	let distributionBundles = $state<DistributionBundle[]>([]);
	let containerApiMetadata = $state<Record<string, string>[]>([]);

	async function loadActive(): Promise<void> {
		loading = true;
		error = null;
		try {
			switch (active) {
				case 'blueprints': {
					const r = await client().send(new GetBlueprintsCommand({}));
					blueprints = r.blueprints ?? [];
					break;
				}
				case 'bundles': {
					const r = await client().send(new GetBundlesCommand({}));
					bundles = r.bundles ?? [];
					break;
				}
				case 'regions': {
					const r = await client().send(new GetRegionsCommand({}));
					regions = r.regions ?? [];
					break;
				}
				case 'containerPowers': {
					const r = await client().send(new GetContainerServicePowersCommand({}));
					containerPowers = r.powers ?? [];
					break;
				}
				case 'dbBlueprints': {
					const r = await client().send(new GetRelationalDatabaseBlueprintsCommand({}));
					dbBlueprints = r.blueprints ?? [];
					break;
				}
				case 'dbBundles': {
					const r = await client().send(new GetRelationalDatabaseBundlesCommand({}));
					dbBundles = r.bundles ?? [];
					break;
				}
				case 'bucketBundles': {
					const r = await client().send(new GetBucketBundlesCommand({}));
					bucketBundles = r.bundles ?? [];
					break;
				}
				case 'distributionBundles': {
					const r = await client().send(new GetDistributionBundlesCommand({}));
					distributionBundles = r.bundles ?? [];
					break;
				}
				case 'containerApiMetadata': {
					const r = await client().send(new GetContainerAPIMetadataCommand({}));
					containerApiMetadata = r.metadata ?? [];
					break;
				}
			}
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	export async function refresh(): Promise<void> {
		await loadActive();
	}

	onRegionChange(() => void loadActive());

	function switchTab(id: string): void {
		active = id as SubTab;
		void loadActive().catch((e) => toast.error(describeError(e)));
	}

	const filteredBlueprints = $derived(
		blueprints.filter((b) => (b.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredBundles = $derived(
		bundles.filter((b) => (b.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
</script>

<div class="space-y-3">
	<div class="rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-4 py-2 text-sm text-amber-800 dark:text-amber-300">
		This tab is small, synthetic seed data this emulator ships with -- not AWS's real, authoritative
		catalog. Use it as a source of valid IDs for the create forms elsewhere on this page.
	</div>

	<Tabs tabs={subTabs} {active} onSelect={switchTab} color="cyan" />

	{#if error}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load data</p>
			<p>{error}</p>
		</div>
	{/if}

	{#if active === 'blueprints'}
		<DataTable
			rows={filteredBlueprints}
			rowKey={(b) => b.blueprintId ?? ''}
			columns={defineColumns<Blueprint>([
				{ key: 'blueprintId', label: 'Blueprint ID' },
				{ key: 'name', label: 'Name' },
				{ key: 'group', label: 'Group' },
				{ key: 'platform', label: 'Platform' },
				{ key: 'isActive', label: 'Active' }
			])}
			{loading}
			emptyMessage="No blueprints found"
		/>
	{:else if active === 'bundles'}
		<DataTable
			rows={filteredBundles}
			rowKey={(b) => b.bundleId ?? ''}
			columns={defineColumns<Bundle>([
				{ key: 'bundleId', label: 'Bundle ID' },
				{ key: 'name', label: 'Name' },
				{ key: 'cpuCount', label: 'vCPU' },
				{ key: 'ramSizeInGb', label: 'RAM (GB)' },
				{ key: 'price', label: 'Price/mo' }
			])}
			{loading}
			emptyMessage="No bundles found"
		/>
	{:else if active === 'regions'}
		<DataTable
			rows={regions}
			rowKey={(r) => r.name ?? ''}
			columns={defineColumns<Region>([
				{ key: 'name', label: 'Region' },
				{ key: 'displayName', label: 'Display name' },
				{ key: 'continentCode', label: 'Continent' }
			])}
			{loading}
			emptyMessage="No regions found"
		/>
	{:else if active === 'containerPowers'}
		<DataTable
			rows={containerPowers}
			rowKey={(p) => p.powerId ?? ''}
			columns={defineColumns<ContainerServicePower>([
				{ key: 'powerId', label: 'Power ID' },
				{ key: 'name', label: 'Name' },
				{ key: 'cpuCount', label: 'vCPU' },
				{ key: 'ramSizeInGb', label: 'RAM (GB)' },
				{ key: 'price', label: 'Price/mo' }
			])}
			{loading}
			emptyMessage="No container powers found"
		/>
	{:else if active === 'dbBlueprints'}
		<DataTable
			rows={dbBlueprints}
			rowKey={(b) => b.blueprintId ?? ''}
			columns={defineColumns<RelationalDatabaseBlueprint>([
				{ key: 'blueprintId', label: 'Blueprint ID' },
				{ key: 'engine', label: 'Engine' },
				{ key: 'engineVersion', label: 'Version' }
			])}
			{loading}
			emptyMessage="No database blueprints found"
		/>
	{:else if active === 'dbBundles'}
		<DataTable
			rows={dbBundles}
			rowKey={(b) => b.bundleId ?? ''}
			columns={defineColumns<RelationalDatabaseBundle>([
				{ key: 'bundleId', label: 'Bundle ID' },
				{ key: 'name', label: 'Name' },
				{ key: 'cpuCount', label: 'vCPU' },
				{ key: 'ramSizeInGb', label: 'RAM (GB)' },
				{ key: 'price', label: 'Price/mo' }
			])}
			{loading}
			emptyMessage="No database bundles found"
		/>
	{:else if active === 'bucketBundles'}
		<DataTable
			rows={bucketBundles}
			rowKey={(b) => b.bundleId ?? ''}
			columns={defineColumns<BucketBundle>([
				{ key: 'bundleId', label: 'Bundle ID' },
				{ key: 'name', label: 'Name' },
				{ key: 'storagePerMonthInGb', label: 'Storage (GB)' },
				{ key: 'price', label: 'Price/mo' }
			])}
			{loading}
			emptyMessage="No bucket bundles found"
		/>
	{:else if active === 'distributionBundles'}
		<DataTable
			rows={distributionBundles}
			rowKey={(b) => b.bundleId ?? ''}
			columns={defineColumns<DistributionBundle>([
				{ key: 'bundleId', label: 'Bundle ID' },
				{ key: 'name', label: 'Name' },
				{ key: 'transferPerMonthInGb', label: 'Transfer (GB)' },
				{ key: 'price', label: 'Price/mo' }
			])}
			{loading}
			emptyMessage="No distribution bundles found"
		/>
	{:else if active === 'containerApiMetadata'}
		<div class="rounded-lg border border-slate-200 dark:border-slate-700 p-4 text-sm">
			{#if loading}
				Loading…
			{:else if containerApiMetadata.length === 0}
				No container API metadata found
			{:else}
				<ul class="space-y-2">
					{#each containerApiMetadata as entry, i (i)}
						<li>{JSON.stringify(entry)}</li>
					{/each}
				</ul>
			{/if}
		</div>
	{/if}
</div>
