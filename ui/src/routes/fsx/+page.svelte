<script lang="ts">
	import { onMount } from 'svelte';
	import { getFSxClient } from '$lib/aws-client';
	import {
		FSxClient,
		DescribeFileSystemsCommand,
		DescribeBackupsCommand,
		CreateFileSystemCommand,
		DeleteFileSystemCommand,
		CreateBackupCommand,
		DeleteBackupCommand,
		type FileSystem,
		type Backup
	} from '@aws-sdk/client-fsx';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { HardDrive, RefreshCw, Search, Database, Shield, Plus, Trash2, ChevronRight, Copy } from 'lucide-svelte';

	let fsx: FSxClient | undefined;
	function client(): FSxClient {
		return (fsx ??= getFSxClient());
	}

	let loading = $state(false);
	let activeTab = $state<'filesystems' | 'backups'>('filesystems');
	let searchQuery = $state('');
	let fileSystems = $state<FileSystem[]>([]);
	let backups = $state<Backup[]>([]);
	let selectedFS = $state<FileSystem | null>(null);

	// Create file system
	let showCreate = $state(false);
	let creating = $state(false);
	let newFSType = $state<'WINDOWS' | 'LUSTRE' | 'ONTAP' | 'OPENZFS'>('LUSTRE');
	let newStorageCapacity = $state(1200);
	let newSubnetId = $state('subnet-00000000');
	let newDeploymentType = $state('SCRATCH_1');

	const filteredFS = $derived(fileSystems.filter((f) => (f.FileSystemId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredBackups = $derived(backups.filter((b) => (b.BackupId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const availableFS = $derived(fileSystems.filter((f) => f.Lifecycle === 'AVAILABLE').length);

	function getThroughput(fs: FileSystem): string {
		let t: number | undefined;
		let unit = 'MB/s';
		if (fs.FileSystemType === 'LUSTRE') {
			t = fs.LustreConfiguration?.PerUnitStorageThroughput;
			unit = 'MB/s/TiB';
		} else if (fs.FileSystemType === 'WINDOWS') {
			t = fs.WindowsConfiguration?.ThroughputCapacity;
		} else if (fs.FileSystemType === 'ONTAP') {
			t = fs.OntapConfiguration?.ThroughputCapacity;
		} else if (fs.FileSystemType === 'OPENZFS') {
			t = fs.OpenZFSConfiguration?.ThroughputCapacity;
		}
		return t === undefined ? '—' : `${t} ${unit}`;
	}

	function getMountName(fs: FileSystem): string {
		if (fs.FileSystemType === 'LUSTRE') {
			return fs.LustreConfiguration?.MountName ?? fs.DNSName ?? '—';
		}
		return fs.DNSName ?? '—';
	}

	async function loadData() {
		loading = true;
		try {
			const [fsResp, backupResp] = await Promise.all([
				client().send(new DescribeFileSystemsCommand({})),
				client().send(new DescribeBackupsCommand({}))
			]);
			fileSystems = fsResp.FileSystems ?? [];
			backups = backupResp.Backups ?? [];
		} catch (e) {
			toast.error('Failed to load FSx data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function createFileSystem() {
		if (!newSubnetId.trim()) return;
		creating = true;
		try {
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const input: any = {
				FileSystemType: newFSType,
				StorageCapacity: newStorageCapacity,
				SubnetIds: [newSubnetId.trim()]
			};
			if (newFSType === 'LUSTRE') {
				input.LustreConfiguration = { DeploymentType: newDeploymentType };
			} else if (newFSType === 'WINDOWS') {
				input.WindowsConfiguration = { ThroughputCapacity: 32 };
			} else if (newFSType === 'OPENZFS') {
				input.OpenZFSConfiguration = { DeploymentType: 'SINGLE_AZ_1', ThroughputCapacity: 64 };
			} else if (newFSType === 'ONTAP') {
				input.OntapConfiguration = { DeploymentType: 'SINGLE_AZ_1', ThroughputCapacity: 128 };
			}
			const resp = await client().send(new CreateFileSystemCommand(input));
			toast.success(`File system "${resp.FileSystem?.FileSystemId}" creation initiated`);
			showCreate = false;
			newSubnetId = 'subnet-00000000';
			await loadData();
		} catch (e) {
			toast.error('Failed to create file system: ' + String(e));
		} finally {
			creating = false;
		}
	}

	async function deleteFileSystem(id: string) {
		if (!(await confirmDestructive({ title: 'Delete File System', message: `Delete file system "${id}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteFileSystemCommand({ FileSystemId: id }));
			toast.success(`File system "${id}" deletion initiated`);
			if (selectedFS?.FileSystemId === id) selectedFS = null;
			await loadData();
		} catch (e) {
			toast.error('Failed to delete file system: ' + String(e));
		}
	}

	async function backupFileSystem(id: string) {
		try {
			const resp = await client().send(new CreateBackupCommand({ FileSystemId: id }));
			toast.success(`Backup "${resp.Backup?.BackupId}" created`);
			await loadData();
		} catch (e) {
			toast.error('Failed to create backup: ' + String(e));
		}
	}

	async function deleteBackup(id: string) {
		if (!(await confirmDestructive({ title: 'Delete Backup', message: `Delete backup "${id}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteBackupCommand({ BackupId: id }));
			toast.success(`Backup "${id}" deleted`);
			await loadData();
		} catch (e) {
			toast.error('Failed to delete backup: ' + String(e));
		}
	}

	function copyToClipboard(text: string, label: string) {
		navigator.clipboard.writeText(text).then(() => toast.success(`${label} copied`)).catch(() => toast.error('Copy failed'));
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
		<div class="flex items-center gap-2">
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreate = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create File System
			</button>
		</div>
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

	{#if selectedFS}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
			<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
				<div class="flex items-center gap-2 text-sm">
					<button onclick={() => (selectedFS = null)} class="text-orange-600 hover:underline">File Systems</button>
					<ChevronRight class="w-4 h-4 text-gray-400" />
					<span class="font-medium text-gray-900 dark:text-white">{selectedFS.FileSystemId}</span>
				</div>
				<div class="flex items-center gap-2">
					<button onclick={() => backupFileSystem(selectedFS?.FileSystemId ?? '')} class="px-3 py-1.5 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700 flex items-center gap-1.5">
						<Shield class="w-3.5 h-3.5" /> Create Backup
					</button>
					<button onclick={() => deleteFileSystem(selectedFS?.FileSystemId ?? '')} class="px-3 py-1.5 rounded-lg bg-red-600 text-white text-sm hover:bg-red-700 flex items-center gap-1.5">
						<Trash2 class="w-3.5 h-3.5" /> Delete
					</button>
				</div>
			</div>
			<div class="p-4 grid grid-cols-2 sm:grid-cols-3 gap-3">
				{#each [
					['File System ID', selectedFS.FileSystemId ?? '-'],
					['Type', selectedFS.FileSystemType ?? '-'],
					['Lifecycle', selectedFS.Lifecycle ?? '-'],
					['Storage Capacity', `${selectedFS.StorageCapacity ?? '-'} GiB`],
					['Storage Type', selectedFS.StorageType ?? '-'],
					['VPC', selectedFS.VpcId ?? '-'],
					['DNS Name', selectedFS.DNSName ?? '-'],
					['Owner Account', selectedFS.OwnerId ?? '-'],
					['Created', selectedFS.CreationTime ? new Date(selectedFS.CreationTime).toLocaleString() : '-']
				] as [label, value]}
					<div class="bg-gray-50 dark:bg-slate-700/50 rounded-lg p-3">
						<p class="text-xs text-gray-500 dark:text-gray-400">{label}</p>
						<p class="text-sm font-medium text-gray-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
					</div>
				{/each}
			</div>
			{#if selectedFS.ResourceARN}
				<div class="px-4 pb-4">
					<div class="flex items-center justify-between bg-gray-50 dark:bg-slate-700/30 rounded-lg px-3 py-2">
						<span class="text-xs font-mono text-gray-700 dark:text-gray-300 truncate" title={selectedFS.ResourceARN}>{selectedFS.ResourceARN}</span>
						<button onclick={() => copyToClipboard(selectedFS?.ResourceARN ?? '', 'ARN')} class="ml-2 p-1.5 text-gray-400 hover:text-gray-600 flex-shrink-0" title="Copy ARN"><Copy class="w-3.5 h-3.5" /></button>
					</div>
				</div>
			{/if}
		</div>
	{:else}
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
									<button onclick={() => (selectedFS = fs)} class="flex items-center gap-3 text-left min-w-0">
										<HardDrive class="w-5 h-5 text-orange-500 flex-shrink-0" />
										<div class="min-w-0">
											<p class="font-medium text-orange-600 dark:text-orange-400 hover:underline truncate">{fs.FileSystemId}</p>
											<p class="text-xs text-gray-500 dark:text-gray-400">{fs.FileSystemType} · {fs.StorageCapacity} GiB</p>
										</div>
									</button>
									<div class="flex items-center gap-2">
										<span class="text-xs px-2 py-1 rounded-full {fs.Lifecycle === 'AVAILABLE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400'}">{fs.Lifecycle}</span>
										<button onclick={() => backupFileSystem(fs.FileSystemId ?? '')} title="Create backup" class="p-1.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900/30 text-blue-500"><Shield class="w-3.5 h-3.5" /></button>
										<button onclick={() => deleteFileSystem(fs.FileSystemId ?? '')} title="Delete" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500"><Trash2 class="w-3.5 h-3.5" /></button>
									</div>
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
											<p class="text-xs text-gray-500 dark:text-gray-400">{backup.Type} · {backup.FileSystem?.FileSystemId ?? '-'}</p>
										</div>
									</div>
									<div class="flex items-center gap-2">
										<span class="text-xs px-2 py-1 rounded-full {backup.Lifecycle === 'AVAILABLE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{backup.Lifecycle}</span>
										<button onclick={() => deleteBackup(backup.BackupId ?? '')} title="Delete backup" class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500"><Trash2 class="w-3.5 h-3.5" /></button>
									</div>
								</div>
							{/each}
						</div>
					{/if}
				{/if}
			</div>
		</div>
	{/if}
</div>

<!-- Create File System Modal -->
{#if showCreate}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create File System</h2>
			<div>
				<label for="fsx-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">File System Type</label>
				<select id="fsx-type" bind:value={newFSType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="LUSTRE">Lustre</option>
					<option value="WINDOWS">Windows File Server</option>
					<option value="ONTAP">NetApp ONTAP</option>
					<option value="OPENZFS">OpenZFS</option>
				</select>
			</div>
			<div>
				<label for="fsx-capacity" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Storage Capacity (GiB)</label>
				<input id="fsx-capacity" bind:value={newStorageCapacity} type="number" min="1" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="fsx-subnet" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Subnet ID</label>
				<input id="fsx-subnet" bind:value={newSubnetId} type="text" placeholder="subnet-00000000" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" />
			</div>
			{#if newFSType === 'LUSTRE'}
				<div>
					<label for="fsx-deploy" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Deployment Type</label>
					<select id="fsx-deploy" bind:value={newDeploymentType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="SCRATCH_1">SCRATCH_1</option>
						<option value="SCRATCH_2">SCRATCH_2</option>
						<option value="PERSISTENT_1">PERSISTENT_1</option>
						<option value="PERSISTENT_2">PERSISTENT_2</option>
					</select>
				</div>
			{/if}
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreate = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createFileSystem} disabled={creating || !newSubnetId.trim()} class="flex-1 px-4 py-2 rounded-lg bg-orange-600 text-white text-sm font-medium hover:bg-orange-700 disabled:opacity-50">
					{creating ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}
