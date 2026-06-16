<script lang="ts">
	import { onMount } from 'svelte';
	import { getFSxClient } from '$lib/aws-client';
	import {
		DescribeFileSystemsCommand,
		DescribeBackupsCommand,
		type FileSystem,
		type Backup
	} from '@aws-sdk/client-fsx';
	import { toast } from 'svelte-sonner';
	import { HardDrive, RefreshCw, Search, Database, Shield } from 'lucide-svelte';

	const fsx = getFSxClient();

	let loading = $state(false);
	let activeTab = $state<'filesystems' | 'backups'>('filesystems');
	let searchQuery = $state('');
	let fileSystems = $state<FileSystem[]>([]);
	let backups = $state<Backup[]>([]);

	const filteredFS = $derived(fileSystems.filter((f) => (f.FileSystemId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredBackups = $derived(backups.filter((b) => (b.BackupId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const availableFS = $derived(fileSystems.filter((f) => f.Lifecycle === 'AVAILABLE').length);

	async function loadData() {
		loading = true;
		try {
			const [fsResp, backupResp] = await Promise.all([
				fsx.send(new DescribeFileSystemsCommand({})),
				fsx.send(new DescribeBackupsCommand({}))
			]);
			fileSystems = fsResp.FileSystems ?? [];
			backups = backupResp.Backups ?? [];
		} catch (e) {
			toast.error('Failed to load FSx data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<HardDrive class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon FSx</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fully managed third-party file systems</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><HardDrive class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{fileSystems.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">File Systems</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Database class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{availableFS}</p><p class="text-sm text-gray-500 dark:text-gray-400">Available</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Shield class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{backups.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Backups</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['filesystems', 'File Systems'], ['backups', 'Backups']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-orange-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'filesystems'}
				{#if filteredFS.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No file systems found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredFS as fs}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<HardDrive class="w-5 h-5 text-orange-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{fs.FileSystemId}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{fs.FileSystemType} · {fs.StorageCapacity} GiB</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {fs.Lifecycle === 'AVAILABLE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400'}">{fs.Lifecycle}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'backups'}
				{#if filteredBackups.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No backups found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredBackups as backup}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Shield class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{backup.BackupId}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{backup.Type}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {backup.Lifecycle === 'AVAILABLE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{backup.Lifecycle}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
