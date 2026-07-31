<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAppStreamClient } from '$lib/aws-client';
	import {
		DescribeStacksCommand,
		DescribeFleetsCommand,
		DescribeImagesCommand,
		type Stack,
		type Fleet,
		type Image as AppStreamImage
	} from '@aws-sdk/client-appstream';
	import { toast } from 'svelte-sonner';
	import { Monitor, RefreshCw, Search, Server, Image, CheckCircle } from 'lucide-svelte';

	const as2 = regionalClient(getAppStreamClient);

	let loading = $state(false);
	let activeTab = $state<'stacks' | 'fleets' | 'images'>('stacks');
	let searchQuery = $state('');
	let stacks = $state<Stack[]>([]);
	let fleets = $state<Fleet[]>([]);
	let images = $state<AppStreamImage[]>([]);

	const filteredStacks = $derived(stacks.filter((s) => (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredFleets = $derived(fleets.filter((f) => (f.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredImages = $derived(images.filter((i) => (i.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const runningFleets = $derived(fleets.filter((f) => f.State === 'RUNNING').length);

	async function loadData() {
		loading = true;
		try {
			const [stackResp, fleetResp, imgResp] = await Promise.all([
				as2().send(new DescribeStacksCommand({})),
				as2().send(new DescribeFleetsCommand({})),
				as2().send(new DescribeImagesCommand({}))
			]);
			stacks = stackResp.Stacks ?? [];
			fleets = fleetResp.Fleets ?? [];
			images = imgResp.Images ?? [];
		} catch (e) {
			toast.error('Failed to load AppStream data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Monitor class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon AppStream 2.0</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fully managed application and desktop streaming service</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Monitor class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{stacks.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Stacks</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Server class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{fleets.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Fleets</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-emerald-600 dark:text-emerald-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{runningFleets}</p><p class="text-sm text-gray-500 dark:text-gray-400">Running</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Image class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{images.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Images</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['stacks', 'Stacks'], ['fleets', 'Fleets'], ['images', 'Images']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'stacks'}
				{#if filteredStacks.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No stacks found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredStacks as stack}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Monitor class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{stack.Name}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{stack.Description ?? stack.Arn}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'fleets'}
				{#if filteredFleets.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No fleets found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredFleets as fleet}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{fleet.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{fleet.InstanceType} · {fleet.MaxUserDurationInSeconds}s max</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {fleet.State === 'RUNNING' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{fleet.State}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'images'}
				{#if filteredImages.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No images found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredImages as img}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Image class="w-5 h-5 text-purple-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{img.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{img.Platform} · {img.BaseImageArn?.split('/').pop()}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{img.State}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
