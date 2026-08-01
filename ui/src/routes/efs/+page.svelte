<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getEFSClient } from '$lib/aws-client';
	import {
		CreateFileSystemCommand,
		DeleteFileSystemCommand,
		DescribeFileSystemsCommand,
		CreateMountTargetCommand,
		DeleteMountTargetCommand,
		DescribeMountTargetsCommand,
		DescribeAccessPointsCommand,
		type FileSystemDescription,
		type MountTargetDescription,
		type AccessPointDescription
	} from '@aws-sdk/client-efs';
	import { toast } from 'svelte-sonner';
	import { HardDrive, Search, RefreshCw, ChevronRight, Server, Plus, Trash2 } from 'lucide-svelte';

	const client = regionalClient(getEFSClient);

	let loading = $state(false);
	let fileSystems = $state<FileSystemDescription[]>([]);
	let mountTargets = $state<MountTargetDescription[]>([]);
	let selectedFS = $state<FileSystemDescription | null>(null);
	let accessPoints = $state<AccessPointDescription[]>([]);
	let loadingAccessPoints = $state(false);
	let activeTab = $state<'filesystems' | 'mounttargets'>('filesystems');
	let searchQuery = $state('');

	// Create file system
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newCreationToken = $state('');
	let newPerformanceMode = $state('generalPurpose');
	let newThroughputMode = $state('bursting');
	let newEncrypted = $state(false);

	// Create mount target
	let showCreateMTModal = $state(false);
	let creatingMT = $state(false);
	let newMTSubnetId = $state('');

	const filteredFS = $derived(
		fileSystems.filter(
			(f) =>
				!searchQuery ||
				(f.FileSystemId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(f.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredMountTargets = $derived(
		mountTargets.filter(
			(m) =>
				!searchQuery || (m.MountTargetId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(fileSystems.filter((f) => f.LifeCycleState === 'available').length);
	const totalSizeGB = $derived(
		Math.round(fileSystems.reduce((acc, f) => acc + (f.SizeInBytes?.Value ?? 0), 0) / 1073741824)
	);

	async function loadData() {
		loading = true;
		try {
			const fsResp = await client().send(new DescribeFileSystemsCommand({}));
			fileSystems = fsResp.FileSystems ?? [];
			if (fileSystems.length > 0) {
				const mtResp = await client().send(
					new DescribeMountTargetsCommand({ FileSystemId: fileSystems[0].FileSystemId })
				);
				mountTargets = mtResp.MountTargets ?? [];
			} else {
				mountTargets = [];
			}
		} catch (e) {
			toast.error('Failed to load EFS data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadMountTargets(fsId: string) {
		try {
			const mtResp = await client().send(new DescribeMountTargetsCommand({ FileSystemId: fsId }));
			mountTargets = mtResp.MountTargets ?? [];
		} catch (e) {
			toast.error('Failed to load mount targets: ' + String(e));
		}
	}

	async function loadAccessPoints(fsId: string) {
		loadingAccessPoints = true;
		try {
			const res = await client().send(new DescribeAccessPointsCommand({ FileSystemId: fsId }));
			accessPoints = res.AccessPoints ?? [];
		} catch (e) {
			toast.error('Failed to load access points: ' + String(e));
		} finally {
			loadingAccessPoints = false;
		}
	}

	async function createFileSystem() {
		if (!newCreationToken.trim()) {
			toast.error('Creation token is required');
			return;
		}
		creating = true;
		try {
			await client().send(
				new CreateFileSystemCommand({
					CreationToken: newCreationToken.trim(),
					PerformanceMode: newPerformanceMode as 'generalPurpose' | 'maxIO',
					ThroughputMode: newThroughputMode as 'bursting' | 'provisioned' | 'elastic',
					Encrypted: newEncrypted
				})
			);
			toast.success('File system created');
			showCreateModal = false;
			newCreationToken = '';
			newPerformanceMode = 'generalPurpose';
			newThroughputMode = 'bursting';
			newEncrypted = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to create file system: ' + String(e));
		} finally {
			creating = false;
		}
	}

	async function deleteFileSystem(fs: FileSystemDescription) {
		const id = fs.FileSystemId ?? '';
		const label = fs.Name ?? id;
		if (
			!(await confirmDestructive({
				title: 'Delete File System',
				message: `Delete file system "${label}"? All data will be permanently lost.`
			}))
		)
			return;
		try {
			await client().send(new DeleteFileSystemCommand({ FileSystemId: id }));
			toast.success(`File system ${id} deleted`);
			if (selectedFS?.FileSystemId === id) { selectedFS = null; accessPoints = []; }
			await loadData();
		} catch (e) {
			toast.error('Failed to delete file system: ' + String(e));
		}
	}

	async function createMountTarget() {
		if (!selectedFS?.FileSystemId) return;
		if (!newMTSubnetId.trim()) {
			toast.error('Subnet ID is required');
			return;
		}
		creatingMT = true;
		try {
			await client().send(
				new CreateMountTargetCommand({
					FileSystemId: selectedFS.FileSystemId,
					SubnetId: newMTSubnetId.trim()
				})
			);
			toast.success('Mount target created');
			showCreateMTModal = false;
			newMTSubnetId = '';
			await loadMountTargets(selectedFS.FileSystemId);
		} catch (e) {
			toast.error('Failed to create mount target: ' + String(e));
		} finally {
			creatingMT = false;
		}
	}

	async function deleteMountTarget(mt: MountTargetDescription) {
		const id = mt.MountTargetId ?? '';
		if (
			!(await confirmDestructive({
				title: 'Delete Mount Target',
				message: `Delete mount target "${id}"?`
			}))
		)
			return;
		try {
			await client().send(new DeleteMountTargetCommand({ MountTargetId: id }));
			toast.success(`Mount target ${id} deleted`);
			if (selectedFS?.FileSystemId) await loadMountTargets(selectedFS.FileSystemId);
		} catch (e) {
			toast.error('Failed to delete mount target: ' + String(e));
		}
	}

	onRegionChange(loadData);
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
		<div class="flex items-center gap-2">
			<button
				onclick={() => { showCreateModal = true; }}
				class="flex items-center gap-2 px-4 py-2 bg-teal-600 text-white rounded-lg hover:bg-teal-700 text-sm font-medium"
			>
				<Plus class="w-4 h-4" /> Create File System
			</button>
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
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
			<button onclick={() => { selectedFS = null; accessPoints = []; }} class="text-teal-600 hover:underline">File Systems</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="font-medium text-gray-700 dark:text-gray-300">{selectedFS.FileSystemId}</span>
		</div>
		<!-- Info grid -->
		<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
			{#each [
				{ label: 'Lifecycle State', value: selectedFS.LifeCycleState ?? '-' },
				{ label: 'Performance Mode', value: selectedFS.PerformanceMode ?? '-' },
				{ label: 'Throughput Mode', value: selectedFS.ThroughputMode ?? '-' },
				{ label: 'Size (GB)', value: String(Math.round((selectedFS.SizeInBytes?.Value ?? 0) / 1073741824)) },
				{ label: 'Mount Targets', value: String(selectedFS.NumberOfMountTargets ?? 0) },
				{ label: 'Encrypted', value: selectedFS.Encrypted ? 'Yes' : 'No' },
				{ label: 'AZ', value: selectedFS.AvailabilityZoneName ?? 'Regional' },
				{ label: 'Creation Token', value: selectedFS.CreationToken ?? '-' },
			] as row}
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-3">
					<div class="text-xs text-gray-500 font-medium">{row.label}</div>
					<div class="text-sm text-gray-900 dark:text-white mt-0.5 font-medium truncate">{row.value}</div>
				</div>
			{/each}
		</div>

		{#if selectedFS.FileSystemArn}
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-3">
				<div class="text-xs text-gray-500 font-medium mb-0.5">File System ARN</div>
				<div class="text-xs font-mono text-gray-900 dark:text-white break-all">{selectedFS.FileSystemArn}</div>
			</div>
		{/if}

		{#if selectedFS.KmsKeyId}
			<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-3">
				<div class="text-xs text-gray-500 font-medium mb-0.5">KMS Key ID</div>
				<div class="text-xs font-mono text-gray-900 dark:text-white break-all">{selectedFS.KmsKeyId}</div>
			</div>
		{/if}

		<!-- Mount Target Management -->
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700">
			<div class="flex items-center justify-between px-4 py-3 border-b border-gray-200 dark:border-gray-700">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Mount Targets per AZ</h2>
				<button
					onclick={() => { showCreateMTModal = true; }}
					class="flex items-center gap-1 px-3 py-1.5 bg-teal-600 text-white rounded-lg hover:bg-teal-700 text-xs font-medium"
				>
					<Plus class="w-3 h-3" /> Add Mount Target
				</button>
			</div>
			{#if mountTargets.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">
					<Server class="w-8 h-8 mx-auto mb-2 opacity-40" />
					<p class="text-sm">No mount targets</p>
				</div>
			{:else}
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Mount Target ID</th>
							<th class="px-4 py-3 text-left">AZ</th>
							<th class="px-4 py-3 text-left">State</th>
							<th class="px-4 py-3 text-left">IP Address</th>
							<th class="px-4 py-3 text-left">Subnet</th>
							<th class="px-4 py-3 text-left"></th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each mountTargets as mt}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3 font-medium text-gray-900 dark:text-white font-mono text-xs">{mt.MountTargetId ?? '-'}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-300 text-xs">{mt.AvailabilityZoneName ?? mt.AvailabilityZoneId ?? '-'}</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium ${mt.LifeCycleState === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{mt.LifeCycleState ?? '-'}</span>
								</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-300 font-mono text-xs">{mt.IpAddress ?? '-'}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-300 text-xs">{mt.SubnetId ?? '-'}</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteMountTarget(mt)}
										class="p-1.5 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 rounded"
										title="Delete mount target"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>

		<!-- Access Points -->
		<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700">
			<div class="px-4 py-3 border-b border-gray-200 dark:border-gray-700">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Access Points</h2>
			</div>
			{#if loadingAccessPoints}
				<div class="text-center py-8"><div class="inline-block w-5 h-5 border-2 border-teal-600 border-t-transparent rounded-full animate-spin"></div></div>
			{:else if accessPoints.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">
					<p class="text-sm">No access points</p>
				</div>
			{:else}
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Access Point ID</th>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Root Path</th>
							<th class="px-4 py-3 text-left">POSIX UID/GID</th>
							<th class="px-4 py-3 text-left">State</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each accessPoints as ap}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-white">{ap.AccessPointId ?? '-'}</td>
								<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{ap.Name ?? '-'}</td>
								<td class="px-4 py-3 font-mono text-xs text-gray-600 dark:text-gray-300">{ap.RootDirectory?.Path ?? '/'}</td>
								<td class="px-4 py-3 text-xs text-gray-600 dark:text-gray-300">
									{#if ap.PosixUser}
										{ap.PosixUser.Uid}/{ap.PosixUser.Gid}
									{:else}
										-
									{/if}
								</td>
								<td class="px-4 py-3">
									<span class={`px-2 py-0.5 rounded text-xs font-medium ${ap.LifeCycleState === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{ap.LifeCycleState ?? '-'}</span>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
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
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Token</th>
								<th class="px-4 py-3 text-left">State</th>
								<th class="px-4 py-3 text-left">Performance Mode</th>
								<th class="px-4 py-3 text-left">Throughput Mode</th>
								<th class="px-4 py-3 text-left"></th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredFS as fs}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button
											onclick={async () => { selectedFS = fs; accessPoints = []; await Promise.all([loadMountTargets(fs.FileSystemId ?? ''), loadAccessPoints(fs.FileSystemId ?? '')]); }}
											class="text-teal-600 dark:text-teal-400 hover:underline font-medium"
										>{fs.FileSystemId ?? '-'}</button>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{fs.Name ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{fs.CreationToken ?? '-'}</td>
									<td class="px-4 py-3">
										<span class={`px-2 py-0.5 rounded text-xs font-medium ${fs.LifeCycleState === 'available' ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-700'}`}>{fs.LifeCycleState ?? '-'}</span>
									</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{fs.PerformanceMode ?? '-'}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{fs.ThroughputMode ?? '-'}</td>
									<td class="px-4 py-3 text-right">
										<button
											onclick={() => deleteFileSystem(fs)}
											class="p-1.5 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 rounded"
											title="Delete file system"
										>
											<Trash2 class="w-4 h-4" />
										</button>
									</td>
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

<!-- Create File System Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md mx-4 p-6">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Create File System</h2>
			<div class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Creation Token <span class="text-red-500">*</span></label>
					<input
						bind:value={newCreationToken}
						type="text"
						placeholder="my-fs-token"
						class="w-full px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Performance Mode</label>
					<select bind:value={newPerformanceMode} class="w-full px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm">
						<option value="generalPurpose">General Purpose</option>
						<option value="maxIO">Max I/O</option>
					</select>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Throughput Mode</label>
					<select bind:value={newThroughputMode} class="w-full px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm">
						<option value="bursting">Bursting</option>
						<option value="provisioned">Provisioned</option>
						<option value="elastic">Elastic</option>
					</select>
				</div>
				<div class="flex items-center gap-2">
					<input id="encrypted" type="checkbox" bind:checked={newEncrypted} class="rounded" />
					<label for="encrypted" class="text-sm text-gray-700 dark:text-gray-300">Encrypted</label>
				</div>
			</div>
			<div class="flex justify-end gap-3 mt-6">
				<button
					onclick={() => { showCreateModal = false; newCreationToken = ''; newPerformanceMode = 'generalPurpose'; newThroughputMode = 'bursting'; newEncrypted = false; }}
					class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white"
				>Cancel</button>
				<button
					onclick={createFileSystem}
					disabled={creating}
					class="px-4 py-2 bg-teal-600 text-white rounded-lg hover:bg-teal-700 text-sm font-medium disabled:opacity-50"
				>{creating ? 'Creating...' : 'Create'}</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Mount Target Modal -->
{#if showCreateMTModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md mx-4 p-6">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Add Mount Target</h2>
			<div class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">File System</label>
					<p class="text-sm font-mono text-gray-600 dark:text-gray-400">{selectedFS?.FileSystemId}</p>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Subnet ID <span class="text-red-500">*</span></label>
					<input
						bind:value={newMTSubnetId}
						type="text"
						placeholder="subnet-12345678"
						class="w-full px-3 py-2 rounded-lg border border-gray-300 dark:border-gray-600 bg-white dark:bg-gray-800 text-sm"
					/>
				</div>
			</div>
			<div class="flex justify-end gap-3 mt-6">
				<button
					onclick={() => { showCreateMTModal = false; newMTSubnetId = ''; }}
					class="px-4 py-2 text-sm text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-white"
				>Cancel</button>
				<button
					onclick={createMountTarget}
					disabled={creatingMT}
					class="px-4 py-2 bg-teal-600 text-white rounded-lg hover:bg-teal-700 text-sm font-medium disabled:opacity-50"
				>{creatingMT ? 'Creating...' : 'Create'}</button>
			</div>
		</div>
	</div>
{/if}
