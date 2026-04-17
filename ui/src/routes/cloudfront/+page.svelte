<script lang="ts">
	import { onMount } from 'svelte';
	import { getCloudFrontClient } from '$lib/aws-client';
	import {
		ListDistributionsCommand,
		GetDistributionCommand,
		CreateInvalidationCommand,
		ListInvalidationsCommand,
		GetDistributionConfigCommand,
		type DistributionSummary,
		type Distribution,
		type InvalidationSummary
	} from '@aws-sdk/client-cloudfront';
	import { toast } from 'svelte-sonner';
	import { Globe, Search, RefreshCw, Plus, ChevronRight, Zap, Settings } from 'lucide-svelte';

	const cf = getCloudFrontClient();

	let loading = $state(false);
	let distributions = $state<DistributionSummary[]>([]);
	let selectedDist = $state<Distribution | null>(null);
	let activeTab = $state<'overview' | 'origins' | 'behaviors' | 'invalidations'>('overview');
	let searchQuery = $state('');

	// Invalidations
	let invalidations = $state<InvalidationSummary[]>([]);
	let loadingInvalidations = $state(false);

	// Create Invalidation
	let showInvalidate = $state(false);
	let creatingInvalidation = $state(false);
	let invalidationPaths = $state('/\n');

	const filteredDists = $derived(
		distributions.filter((d) =>
			!searchQuery ||
			(d.DomainName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
			(d.Comment ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
			(d.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const statusColor = (enabled: boolean | undefined, status: string | undefined) => {
		if (status === 'Deployed' && enabled) return 'green';
		if (status === 'InProgress') return 'yellow';
		if (!enabled) return 'gray';
		return 'blue';
	};

	async function loadDistributions() {
		loading = true;
		try {
			const resp = await cf.send(new ListDistributionsCommand({}));
			distributions = resp.DistributionList?.Items ?? [];
		} catch (e) {
			toast.error('Failed to load distributions: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectDistribution(id: string) {
		activeTab = 'overview';
		invalidations = [];
		try {
			const resp = await cf.send(new GetDistributionCommand({ Id: id }));
			selectedDist = resp.Distribution ?? null;
		} catch (e) {
			toast.error('Failed to load distribution details: ' + String(e));
		}
	}

	async function handleTabChange(tab: 'overview' | 'origins' | 'behaviors' | 'invalidations') {
		activeTab = tab;
		if (tab === 'invalidations' && selectedDist && invalidations.length === 0) {
			await loadInvalidations();
		}
	}

	async function loadInvalidations() {
		if (!selectedDist) return;
		loadingInvalidations = true;
		try {
			const resp = await cf.send(new ListInvalidationsCommand({ DistributionId: selectedDist.Id ?? '' }));
			invalidations = resp.InvalidationList?.Items ?? [];
		} catch (e) {
			toast.error('Failed to load invalidations: ' + String(e));
		} finally {
			loadingInvalidations = false;
		}
	}

	async function createInvalidation() {
		if (!selectedDist) return;
		const paths = invalidationPaths.split('\n').map((p) => p.trim()).filter(Boolean);
		if (paths.length === 0) return;
		creatingInvalidation = true;
		try {
			await cf.send(new CreateInvalidationCommand({
				DistributionId: selectedDist.Id ?? '',
				InvalidationBatch: {
					CallerReference: `invalidate-${Date.now()}`,
					Paths: {
						Quantity: paths.length,
						Items: paths
					}
				}
			}));
			toast.success(`Invalidation created for ${paths.length} paths`);
			showInvalidate = false;
			invalidationPaths = '/\n';
			invalidations = [];
			activeTab = 'invalidations';
			await loadInvalidations();
		} catch (e) {
			toast.error('Failed to create invalidation: ' + String(e));
		} finally {
			creatingInvalidation = false;
		}
	}

	function httpMethodsList(methods: string[] | undefined): string {
		return (methods ?? []).join(', ') || '-';
	}

	onMount(loadDistributions);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Globe class="w-7 h-7 text-violet-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon CloudFront</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Global content delivery network</p>
			</div>
		</div>
		<button onclick={loadDistributions} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	{#if selectedDist}
		<!-- Distribution Detail -->
		<div class="flex items-center justify-between">
			<div class="flex items-center gap-2 text-sm">
				<button onclick={() => { selectedDist = null; invalidations = []; }} class="text-violet-600 hover:underline">Distributions</button>
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<span class="font-medium">{selectedDist.DomainName}</span>
				<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedDist.DistributionConfig?.Enabled, selectedDist.Status)}-100 text-${statusColor(selectedDist.DistributionConfig?.Enabled, selectedDist.Status)}-700`}>
					{selectedDist.Status}
				</span>
			</div>
			<button onclick={() => (showInvalidate = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm font-medium">
				<Zap class="w-4 h-4" /> Create Invalidation
			</button>
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['overview', 'Overview'], ['origins', 'Origins'], ['behaviors', 'Cache Behaviors'], ['invalidations', 'Invalidations']] as [tab, label]}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'origins' | 'behaviors' | 'invalidations')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-violet-500 text-violet-600 dark:text-violet-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
				>
					{label}
				</button>
			{/each}
		</div>

		{#if activeTab === 'overview'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'Distribution ID', value: selectedDist.Id },
					{ label: 'Domain Name', value: selectedDist.DomainName },
					{ label: 'Status', value: selectedDist.Status },
					{ label: 'Enabled', value: selectedDist.DistributionConfig?.Enabled ? 'Yes' : 'No' },
					{ label: 'Price Class', value: selectedDist.DistributionConfig?.PriceClass ?? '-' },
					{ label: 'HTTP Version', value: selectedDist.DistributionConfig?.HttpVersion ?? '-' },
					{ label: 'IPv6 Enabled', value: selectedDist.DistributionConfig?.IsIPV6Enabled ? 'Yes' : 'No' },
					{ label: 'Comment', value: selectedDist.DistributionConfig?.Comment || 'None' }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
						<div class="text-xs text-gray-500 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1 truncate">{row.value}</div>
					</div>
				{/each}
			</div>

			{#if (selectedDist.DistributionConfig?.Aliases?.Items ?? []).length > 0}
				<div class="bg-violet-50 dark:bg-violet-900/20 rounded-xl border border-violet-200 dark:border-violet-800 p-4">
					<div class="text-sm font-semibold text-violet-800 dark:text-violet-300 mb-2">Alternate Domain Names (CNAMEs)</div>
					<div class="flex flex-wrap gap-2">
						{#each (selectedDist.DistributionConfig?.Aliases?.Items ?? []) as alias}
							<span class="px-3 py-1 bg-violet-100 dark:bg-violet-900/40 text-violet-700 dark:text-violet-300 rounded-full text-xs font-mono">{alias}</span>
						{/each}
					</div>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'origins'}
			{#if (selectedDist.DistributionConfig?.Origins?.Items ?? []).length === 0}
				<div class="text-center py-12 text-gray-500">No origins configured</div>
			{:else}
				<div class="space-y-3">
					{#each (selectedDist.DistributionConfig?.Origins?.Items ?? []) as origin}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
							<div class="flex items-start justify-between">
								<div>
									<div class="font-semibold text-sm">{origin.Id}</div>
									<div class="text-xs text-gray-500 mt-1 font-mono">{origin.DomainName}</div>
									{#if origin.OriginPath}
										<div class="text-xs text-gray-500">Path: {origin.OriginPath}</div>
									{/if}
								</div>
								<span class="px-2 py-0.5 rounded text-xs font-medium bg-violet-100 text-violet-700">
									{origin.S3OriginConfig ? 'S3' : origin.CustomOriginConfig ? 'Custom' : 'Origin'}
								</span>
							</div>
							{#if origin.CustomOriginConfig}
								<div class="mt-2 grid grid-cols-2 gap-2 text-xs text-gray-500">
									<span>HTTP Port: {origin.CustomOriginConfig.HTTPPort ?? 80}</span>
									<span>HTTPS Port: {origin.CustomOriginConfig.HTTPSPort ?? 443}</span>
									<span>Protocol: {origin.CustomOriginConfig.OriginProtocolPolicy}</span>
								</div>
							{/if}
						</div>
					{/each}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'behaviors'}
			{#if (selectedDist.DistributionConfig?.CacheBehaviors?.Items ?? []).length === 0 && !selectedDist.DistributionConfig?.DefaultCacheBehavior}
				<div class="text-center py-12 text-gray-500">No cache behaviors configured</div>
			{:else}
				<div class="space-y-3">
					<!-- Default behavior -->
					{#if selectedDist.DistributionConfig?.DefaultCacheBehavior}
						{@const dcb = selectedDist.DistributionConfig.DefaultCacheBehavior}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-violet-200 dark:border-violet-800 p-4">
							<div class="flex items-center gap-2 mb-2">
								<span class="px-2 py-0.5 rounded text-xs font-medium bg-violet-100 text-violet-700">Default</span>
								<span class="text-sm font-medium">/*</span>
							</div>
							<div class="grid grid-cols-2 gap-2 text-xs text-gray-500">
								<span>Origin: {dcb.TargetOriginId}</span>
								<span>Viewer Protocol: {dcb.ViewerProtocolPolicy}</span>
								<span>HTTP Methods: {httpMethodsList(dcb.AllowedMethods?.Items)}</span>
								<span>Compress: {dcb.Compress ? 'Yes' : 'No'}</span>
							</div>
						</div>
					{/if}
					<!-- Custom behaviors -->
					{#each (selectedDist.DistributionConfig?.CacheBehaviors?.Items ?? []) as behavior}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
							<div class="text-sm font-medium mb-2 font-mono">{behavior.PathPattern}</div>
							<div class="grid grid-cols-2 gap-2 text-xs text-gray-500">
								<span>Origin: {behavior.TargetOriginId}</span>
								<span>Viewer Protocol: {behavior.ViewerProtocolPolicy}</span>
								<span>HTTP Methods: {httpMethodsList(behavior.AllowedMethods?.Items)}</span>
								<span>Compress: {behavior.Compress ? 'Yes' : 'No'}</span>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'invalidations'}
			{#if loadingInvalidations}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"></div></div>
			{:else if invalidations.length === 0}
				<div class="text-center py-12 text-gray-500"><Zap class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No invalidations</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Invalidation ID</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Created</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each invalidations as inv}
								<tr>
									<td class="px-4 py-3 font-mono text-xs">{inv.Id}</td>
									<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${inv.Status === 'Completed' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}>{inv.Status}</span></td>
									<td class="px-4 py-3 text-xs text-gray-500">{inv.CreateTime?.toLocaleString() ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

	{:else}
		<!-- Distribution List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search distributions..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-violet-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredDists.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Globe class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No distributions found</p>
				<p class="text-sm mt-1">Create a CloudFront distribution to serve content globally</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">ID</th>
							<th class="px-4 py-3 text-left">Domain Name</th>
							<th class="px-4 py-3 text-left">Status</th>
							<th class="px-4 py-3 text-left">Comment</th>
							<th class="px-4 py-3 text-left">Origins</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredDists as dist}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectDistribution(dist.Id ?? '')} class="text-violet-600 dark:text-violet-400 hover:underline font-mono text-xs">{dist.Id}</button>
								</td>
								<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-400">{dist.DomainName}</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(dist.Enabled, dist.Status)}-100 text-${statusColor(dist.Enabled, dist.Status)}-700`}>
										{dist.Status}
									</span>
								</td>
								<td class="px-4 py-3 text-xs text-gray-500">{dist.Comment || '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{dist.Origins?.Quantity ?? 0}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Invalidation Modal -->
{#if showInvalidate && selectedDist}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Invalidation</h2>
			<p class="text-sm text-gray-500">Distribution: {selectedDist.DomainName}</p>
			<div>
				<label for="invalidation-paths" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Paths to Invalidate (one per line)</label>
				<textarea id="invalidation-paths" bind:value={invalidationPaths} rows={6} placeholder="/images/*&#10;/static/app.js&#10;/" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
				<p class="text-xs text-gray-400 mt-1">Use /* to invalidate everything. Wildcards are supported.</p>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showInvalidate = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={createInvalidation} disabled={creatingInvalidation || !invalidationPaths.trim()} class="flex-1 px-4 py-2 rounded-lg bg-violet-600 text-white text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
					{creatingInvalidation ? 'Creating...' : 'Create Invalidation'}
				</button>
			</div>
		</div>
	</div>
{/if}
