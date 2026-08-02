<script lang="ts">
	// Account -- family BB (2 ops: GetActiveNames, GetCostEstimate).
	//
	// GetActiveNames is an account-wide name-uniqueness listing across ALL
	// resource kinds (Lightsail enforces one global namespace for resource
	// names, services/lightsail/store.go's registerNameLocked) -- shown here
	// as a flat searchable list, not tied to any one resource family.
	//
	// GetCostEstimate always returns an honest EMPTY resourcesBudgetEstimate
	// in this emulator (services/lightsail/tagging_vpc_misc.go's
	// handleGetCostEstimate) -- there is no real billing/metering signal to
	// synthesize a plausible-looking cost estimate from, so this panel says
	// so rather than fabricating a dollar figure.
	import { GetActiveNamesCommand, GetCostEstimateCommand, type LightsailClient } from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let activeNames = $state<string[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchNames(reset: boolean): Promise<void> {
		const resp = await client().send(new GetActiveNamesCommand({ pageToken: reset ? undefined : nextToken }));
		activeNames = reset ? (resp.activeNames ?? []) : [...activeNames, ...(resp.activeNames ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchNames(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchNames(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(activeNames.filter((n) => n.toLowerCase().includes(searchQuery.toLowerCase())));

	// ------------------------------ Cost estimate ---------------------------

	let costResourceName = $state('');
	let costChecked = $state(false);
	let costEmpty = $state(true);
	let costBusy = $state(false);

	async function checkCostEstimate(): Promise<void> {
		if (!costResourceName) return;
		costBusy = true;
		try {
			const resp = await client().send(
				new GetCostEstimateCommand({
					resourceName: costResourceName,
					startTime: new Date(Date.now() - 30 * 24 * 3600_000),
					endTime: new Date()
				})
			);
			costEmpty = (resp.resourcesBudgetEstimate ?? []).length === 0;
			costChecked = true;
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			costBusy = false;
		}
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Active resource names (account-wide, across every resource kind)</p>
	{#if loading}
		<p class="text-sm text-slate-500">Loading…</p>
	{:else if filtered.length === 0}
		<p class="text-sm text-slate-500">No active names found</p>
	{:else}
		<ul class="text-sm space-y-1 max-h-64 overflow-y-auto">
			{#each filtered as n (n)}
				<li>{n}</li>
			{/each}
		</ul>
	{/if}
	<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />
</div>

<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Cost estimate</p>
	<div class="flex items-center gap-2">
		<input
			bind:value={costResourceName}
			placeholder="Resource name"
			aria-label="Resource name for cost estimate"
			class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 flex-1"
		/>
		<button onclick={checkCostEstimate} disabled={costBusy || !costResourceName} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">
			Check cost estimate
		</button>
	</div>
	{#if costChecked && costEmpty}
		<p class="text-xs text-slate-500">
			This emulator does not produce real cost/billing data -- GetCostEstimate returned an honest empty
			resourcesBudgetEstimate.
		</p>
	{/if}
</div>
