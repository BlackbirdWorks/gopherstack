<script lang="ts">
	import { onMount } from 'svelte';
	import { getEFSClient } from '$lib/aws-client';
	import {
		DescribeFileSystemsCommand,
		DescribeMountTargetsCommand,
		type FileSystemDescription,
		type MountTargetDescription
	} from '@aws-sdk/client-efs';
	import { toast } from 'svelte-sonner';
	import { HardDrive, Search, RefreshCw, ChevronRight, Server } from 'lucide-svelte';

	const client = getEFSClient();

	let loading = $state(false);
	let fileSystems = $state<FileSystemDescription[]>([]);
	let mountTargets = $state<MountTargetDescription[]>([]);
	let selectedFS = $state<FileSystemDescription | null>(null);
	let activeTab = $state<'filesystems' | 'mounttargets'>('filesystems');
	let searchQuery = $state('');

	const filteredFS = $derived(
		fileSystems.filter(
			(f) => !searchQuery || (f.FileSystemId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) || (f.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredMountTargets = $derived(
		mountTargets.filter(
			(m) => !searchQuery || (m.MountTargetId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(fileSystems.filter((f) => f.LifeCycleState === 'available').length);
	const totalSizeGB = $derived(
		Math.round(fileSystems.reduce((acc, f) => acc + (f.SizeInBytes?.Value ?? 0), 0) / 1073741824)
	);

	async function loadData() {
		loading = true;
		try {
			const fsResp = await client.send(new DescribeFileSystemsCommand({}));
			fileSystems = fsResp.FileSystems ?? [];
			if (fileSystems.length > 0) {
				const mtResp = await client.send(
					new DescribeMountTargetsCommand({ FileSystemId: fileSystems[0].FileSystemId })
				);
				mountTargets = mtResp.MountTargets ?? [];
			}
		} catch (e) {
			toast.error('Failed to load EFS data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<HardDrive class="w-7 h-7 text-teal-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon EFS</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Scalable, elastic, cloud-native NFS file system</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Stat Cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<HardDrive class="w-5 h-5 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{fileSystems.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">File Systems</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<HardDrive class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{availableCount}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Available</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<HardDrive class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{totalSizeGB}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Total Size (GB)</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Server class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{mountTargets.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Mount Targets</p>
			</div>
		</div>
	</div>

	{#if selectedFS}
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedFS = null; }} class="text-teal-600 hover:underline">File Systems</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="font-medium text-gray-700 dark:text-gray-300">{selectedFS.FileSystemId}</span>
		</div>
		<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
			{#each [
				{ label: 'File System ID', value: selectedFS.FileSystemId ?? '-' },
				{ label: 'Lifecycle State', value: selectedFS.LifeCycleState ?? '-' },
				{ label: 'Performance Mode', value: selectedFS.PerformanceMode ?? '-' },
				{ label: 'Throughput Mode', value: selectedFS.ThroughputMode ?? '-' },
				{ label: 'Size (Bytes)', value: String(selectedFS.SizeInBytes?.Value ?? 0) },
				{ label: 'Number of Mount Targets', value: String(selectedFS.NumberOfMountTargets ?? 0) }
			] as row}
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 font-medium">{row.label}</div>
					<div class="text-sm text-gray-900 dark:text-white mt-1 font-mono truncate">{row.value}</div>
				</div>
			{/each}
		</div>
	{:else}
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['filesystems', 'File Systems'], ['mounttargets', 'Mount Targets']] as [tab, label]}
				<button
					onclick={() => { activeTab = tab as 'filesystems' | 'mounttargets'; searchQuery = ''; }}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-teal-500 text-teal-600 dark:text-teal-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
				>{label}</button>
			{/each}
		</div>

		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-teal-600 border-t-transparent rounded-full"></div></div>
		{:else if activeTab === 'filesystems'}
			{#if filteredFS.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<HardDrive class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No file systems found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">ID</th>
								<th class="px-4 py-3 text-left">State</th>
								<th class="px-4 py-3 text-left">Performance Mode</th>
								<th class="px-4 py-3 text-left">Throughput Mode</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredFS as fs}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button onclick={() => { selectedFS = fs; }} class="text-teal-600 dark:text-teal-400 hover:underline font-medium">{fs.FileSystemId ?? '-'}</button>
									</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${fs.LifeCycleState === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{fs.LifeCycleState ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{fs.PerformanceMode ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{fs.ThroughputMode ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else}
			{#if filteredMountTargets.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Server class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No mount targets found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Mount Target ID</th>
								<th class="px-4 py-3 text-left">State</th>
								<th class="px-4 py-3 text-left">Subnet</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredMountTargets as mt}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{mt.MountTargetId ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${mt.LifeCycleState === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{mt.LifeCycleState ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{mt.SubnetId ?? '-'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
