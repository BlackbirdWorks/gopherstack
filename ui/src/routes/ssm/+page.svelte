<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSSMClient } from '$lib/aws-client';
	import {
		DescribeParametersCommand,
		GetParameterCommand,
		PutParameterCommand,
		DeleteParameterCommand,
		GetParametersByPathCommand,
		CreateMaintenanceWindowCommand,
		DeleteMaintenanceWindowCommand,
		DescribeMaintenanceWindowsCommand,
		type ParameterMetadata,
		type MaintenanceWindowIdentity
	} from '@aws-sdk/client-ssm';
	import { toast } from 'svelte-sonner';
	import { Settings, Search, RefreshCw, Plus, Trash2, Eye, EyeOff, Edit2, Check, X, Calendar, FileText, Folder, ChevronRight, ChevronDown } from 'lucide-svelte';

	const ssm = regionalClient(getSSMClient);

	let loading = $state(false);
	let parameters = $state<ParameterMetadata[]>([]);
	let searchPath = $state('');
	let selectedParam = $state<ParameterMetadata | null>(null);
	let selectedValue = $state<string | null>(null);
	let loadingValue = $state(false);
	let showValue = $state(false);
	// Flat list vs. /-path folder tree
	let listView = $state<'flat' | 'tree'>('flat');
	let collapsedFolders = $state(new Set<string>());

	type TreeNode = { name: string; path: string; children: TreeNode[]; param?: ParameterMetadata };

	// Build a folder tree from parameter names delimited by "/".
	const paramTree = $derived.by(() => {
		const root: TreeNode = { name: '', path: '', children: [] };
		for (const p of parameters) {
			const name = p.Name ?? '';
			const segments = name.replace(/^\//, '').split('/');
			let node = root;
			let acc = '';
			for (let i = 0; i < segments.length; i++) {
				const seg = segments[i];
				acc = `${acc}/${seg}`;
				const isLeaf = i === segments.length - 1;
				let child = node.children.find((c) => c.name === seg && (isLeaf ? !!c.param === false : true));
				if (!child) {
					child = { name: seg, path: acc, children: [] };
					node.children.push(child);
				}
				if (isLeaf) child.param = p;
				node = child;
			}
		}
		return root;
	});

	function toggleFolder(path: string) {
		const next = new Set(collapsedFolders);
		if (next.has(path)) next.delete(path);
		else next.add(path);
		collapsedFolders = next;
	}

	// Create/Edit modal
	let showModal = $state(false);
	let saving = $state(false);
	let isEditing = $state(false);
	let modalName = $state('');
	let modalValue = $state('');
	let modalType = $state<'String' | 'StringList' | 'SecureString'>('String');
	let modalDescription = $state('');
	let modalTier = $state<'Standard' | 'Advanced'>('Standard');
	let modalOverwrite = $state(false);

	const typeColors: Record<string, string> = {
		String: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300',
		StringList: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300',
		SecureString: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300'
	};

	async function loadParameters() {
		loading = true;
		// `searchPath` is read with `untrack` so it never becomes a dependency
		// of the `onRegionChange` effect below -- otherwise every keystroke in
		// the bound search input would re-trigger that effect too, since the
		// effect would end up reading `searchPath` on every run.
		const currentSearchPath = untrack(() => searchPath.trim());
		try {
			if (currentSearchPath.startsWith('/')) {
				// Path-based search
				const res = await ssm().send(new GetParametersByPathCommand({
					Path: currentSearchPath,
					Recursive: true,
					WithDecryption: false,
					MaxResults: 50
				}));
				parameters = (res.Parameters ?? []).map((p) => ({
					Name: p.Name,
					Type: p.Type,
					LastModifiedDate: p.LastModifiedDate,
					ARN: p.ARN,
					DataType: p.DataType,
					Version: p.Version
				}));
			} else {
				const filters = currentSearchPath
					? [{ Key: 'Name', Option: 'Contains', Values: [currentSearchPath] }]
					: undefined;
				const res = await ssm().send(new DescribeParametersCommand({
					ParameterFilters: filters,
					MaxResults: 50
				}));
				parameters = res.Parameters ?? [];
			}
		} catch (err: unknown) {
			toast.error(`Failed to load parameters: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function getValue(param: ParameterMetadata) {
		selectedParam = param;
		selectedValue = null;
		showValue = false;
		loadingValue = true;
		try {
			const res = await ssm().send(new GetParameterCommand({
				Name: param.Name,
				WithDecryption: true
			}));
			selectedValue = res.Parameter?.Value ?? '';
		} catch (err: unknown) {
			toast.error(`Failed to get value: ${(err as Error).message}`);
		} finally {
			loadingValue = false;
		}
	}

	function openCreate() {
		isEditing = false;
		modalName = '';
		modalValue = '';
		modalType = 'String';
		modalDescription = '';
		modalTier = 'Standard';
		modalOverwrite = false;
		showModal = true;
	}

	function openEdit(param: ParameterMetadata) {
		isEditing = true;
		modalName = param.Name ?? '';
		modalValue = selectedValue ?? '';
		modalType = (param.Type as 'String' | 'StringList' | 'SecureString') ?? 'String';
		modalDescription = '';
		modalTier = 'Standard';
		modalOverwrite = true;
		showModal = true;
	}

	async function save() {
		if (!modalName.trim() || !modalValue.trim()) return;
		saving = true;
		try {
			await ssm().send(new PutParameterCommand({
				Name: modalName.trim(),
				Value: modalValue,
				Type: modalType,
				Description: modalDescription || undefined,
				Tier: modalTier,
				Overwrite: modalOverwrite
			}));
			toast.success(`Parameter "${modalName.trim()}" ${isEditing ? 'updated' : 'created'}`);
			showModal = false;
			await loadParameters();
		} catch (err: unknown) {
			toast.error(`Save failed: ${(err as Error).message}`);
		} finally {
			saving = false;
		}
	}

	async function deleteParameter(name: string) {
		if (!await confirmDestructive({ title: 'Delete Parameter', message: `Delete parameter "${name}"? Any applications reading this parameter will receive an error.` })) return;
		try {
			await ssm().send(new DeleteParameterCommand({ Name: name }));
			toast.success(`Parameter "${name}" deleted`);
			if (selectedParam?.Name === name) { selectedParam = null; selectedValue = null; }
			await loadParameters();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function formatDate(d?: Date): string {
		return d ? new Date(d).toLocaleString() : 'N/A';
	}

	// Maintenance Windows
	let activeTab = $state<'parameters' | 'maintenance-windows'>('parameters');
	let mwLoading = $state(false);
	let maintenanceWindows = $state<MaintenanceWindowIdentity[]>([]);

	// Create maintenance window modal
	let showMWModal = $state(false);
	let mwSaving = $state(false);
	let mwName = $state('');
	let mwDescription = $state('');
	let mwSchedule = $state('');
	let mwDuration = $state(4);
	let mwCutoff = $state(1);
	let mwAllowUnassociated = $state(false);

	async function loadMaintenanceWindows() {
		mwLoading = true;
		try {
			const res = await ssm().send(new DescribeMaintenanceWindowsCommand({ MaxResults: 50 }));
			maintenanceWindows = (res.WindowIdentities ?? []) as MaintenanceWindowIdentity[];
		} catch (err: unknown) {
			toast.error(`Failed to load maintenance windows: ${(err as Error).message}`);
		} finally {
			mwLoading = false;
		}
	}

	function openCreateMW() {
		mwName = '';
		mwDescription = '';
		mwSchedule = 'cron(0 2 ? * SUN *)';
		mwDuration = 4;
		mwCutoff = 1;
		mwAllowUnassociated = false;
		showMWModal = true;
	}

	async function saveMW() {
		if (!mwName.trim() || !mwSchedule.trim()) return;
		mwSaving = true;
		try {
			await ssm().send(new CreateMaintenanceWindowCommand({
				Name: mwName.trim(),
				Description: mwDescription || undefined,
				Schedule: mwSchedule.trim(),
				Duration: mwDuration,
				Cutoff: mwCutoff,
				AllowUnassociatedTargets: mwAllowUnassociated
			}));
			toast.success(`Maintenance window "${mwName.trim()}" created`);
			showMWModal = false;
			await loadMaintenanceWindows();
		} catch (err: unknown) {
			toast.error(`Failed to create maintenance window: ${(err as Error).message}`);
		} finally {
			mwSaving = false;
		}
	}

	async function deleteMW(windowId: string, name: string) {
		if (!await confirmDestructive({ title: 'Delete Maintenance Window', message: `Delete maintenance window "${name}"?` })) return;
		try {
			await ssm().send(new DeleteMaintenanceWindowCommand({ WindowId: windowId }));
			toast.success(`Maintenance window "${name}" deleted`);
			await loadMaintenanceWindows();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	onRegionChange(() => {
		loadParameters();
		loadMaintenanceWindows();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<Settings class="w-6 h-6 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">AWS Systems Manager</h1>
				<p class="text-slate-600 dark:text-slate-300">Secure configuration, secrets, and maintenance management</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			{#if activeTab === 'parameters'}
				<button onclick={() => loadParameters()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
					<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
				</button>
				<button onclick={openCreate} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
					<Plus class="w-4 h-4" />
					Create Parameter
				</button>
			{:else}
				<button onclick={() => loadMaintenanceWindows()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
					<RefreshCw class="w-5 h-5 {mwLoading ? 'animate-spin' : ''}" />
				</button>
				<button onclick={openCreateMW} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
					<Plus class="w-4 h-4" />
					Create Window
				</button>
			{/if}
		</div>
	</div>

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<nav class="flex gap-0" aria-label="SSM Tabs">
			<button
				onclick={() => { activeTab = 'parameters'; }}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === 'parameters' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
			>
				<div class="flex items-center gap-2"><Settings class="w-4 h-4" />Parameter Store</div>
			</button>
			<button
				onclick={() => { activeTab = 'maintenance-windows'; }}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === 'maintenance-windows' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
			>
				<div class="flex items-center gap-2"><Calendar class="w-4 h-4" />Maintenance Windows</div>
			</button>
		</nav>
	</div>

	{#if activeTab === 'parameters'}
	<!-- Search -->
	<div class="flex gap-2">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input
				type="text"
				bind:value={searchPath}
				placeholder="Search by name or /path/prefix..."
				class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
			/>
		</div>
		<button onclick={() => loadParameters()} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-lg hover:bg-slate-200 dark:hover:bg-slate-600">
			Search
		</button>
		<div class="flex items-center gap-1" title="List view">
			<button type="button" onclick={() => { listView = 'flat'; }} class="px-2.5 py-2 text-xs rounded-lg {listView === 'flat' ? 'bg-indigo-600 text-white' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'}">Flat</button>
			<button type="button" onclick={() => { listView = 'tree'; }} class="px-2.5 py-2 text-xs rounded-lg {listView === 'tree' ? 'bg-indigo-600 text-white' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'}">Tree</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Parameter List -->
		<div class="lg:col-span-1">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading parameters...</p>
				</div>
			{:else if parameters.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<Settings class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No parameters found</p>
				</div>
			{:else if listView === 'flat'}
				<div class="space-y-2">
					{#each parameters as param}
						<button
							onclick={() => getValue(param)}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-indigo-400 transition-colors {selectedParam?.Name === param.Name ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-start justify-between">
								<div class="min-w-0 flex-1">
									<p class="font-medium text-slate-900 dark:text-white text-sm truncate">{param.Name}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{formatDate(param.LastModifiedDate)}</p>
								</div>
								<span class="ml-2 flex-shrink-0 px-2 py-0.5 text-xs rounded-full {typeColors[param.Type ?? 'String'] ?? typeColors.String}">{param.Type}</span>
							</div>
						</button>
					{/each}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-2">
					{#each paramTree.children as child}
						{@render treeNode(child, 0)}
					{/each}
				</div>
			{/if}

			{#snippet treeNode(node: TreeNode, depth: number)}
				{#if node.param && node.children.length === 0}
					<button
						onclick={() => node.param && getValue(node.param)}
						class="w-full text-left flex items-center gap-2 px-2 py-1.5 rounded hover:bg-slate-100 dark:hover:bg-slate-700/50 {selectedParam?.Name === node.param.Name ? 'bg-indigo-50 dark:bg-indigo-900/30' : ''}"
						style="padding-left: {depth * 14 + 8}px"
					>
						<FileText class="w-3.5 h-3.5 text-slate-400 flex-shrink-0" />
						<span class="text-sm text-slate-700 dark:text-slate-200 truncate flex-1">{node.name}</span>
						<span class="flex-shrink-0 px-1.5 py-0.5 text-[10px] rounded-full {typeColors[node.param.Type ?? 'String'] ?? typeColors.String}">{node.param.Type}</span>
					</button>
				{:else}
					<button
						type="button"
						onclick={() => toggleFolder(node.path)}
						class="w-full text-left flex items-center gap-1.5 px-2 py-1.5 rounded hover:bg-slate-100 dark:hover:bg-slate-700/50"
						style="padding-left: {depth * 14 + 8}px"
					>
						{#if collapsedFolders.has(node.path)}
							<ChevronRight class="w-3.5 h-3.5 text-slate-400 flex-shrink-0" />
						{:else}
							<ChevronDown class="w-3.5 h-3.5 text-slate-400 flex-shrink-0" />
						{/if}
						<Folder class="w-3.5 h-3.5 text-amber-500 flex-shrink-0" />
						<span class="text-sm font-medium text-slate-700 dark:text-slate-200 truncate">{node.name}</span>
					</button>
					{#if !collapsedFolders.has(node.path)}
						{#each node.children as child}
							{@render treeNode(child, depth + 1)}
						{/each}
						{#if node.param}
							<button
								onclick={() => node.param && getValue(node.param)}
								class="w-full text-left flex items-center gap-2 px-2 py-1.5 rounded hover:bg-slate-100 dark:hover:bg-slate-700/50 {selectedParam?.Name === node.param.Name ? 'bg-indigo-50 dark:bg-indigo-900/30' : ''}"
								style="padding-left: {(depth + 1) * 14 + 8}px"
							>
								<FileText class="w-3.5 h-3.5 text-slate-400 flex-shrink-0" />
								<span class="text-sm text-slate-700 dark:text-slate-200 truncate flex-1">(self)</span>
								<span class="flex-shrink-0 px-1.5 py-0.5 text-[10px] rounded-full {typeColors[node.param.Type ?? 'String'] ?? typeColors.String}">{node.param.Type}</span>
							</button>
						{/if}
					{/if}
				{/if}
			{/snippet}
		</div>

		<!-- Parameter Detail -->
		<div class="lg:col-span-2">
			{#if selectedParam}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-lg font-bold text-slate-900 dark:text-white font-mono break-all">{selectedParam.Name}</h2>
							<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {typeColors[selectedParam.Type ?? 'String'] ?? typeColors.String}">{selectedParam.Type}</span>
						</div>
						<div class="flex gap-2">
							<button
								onclick={() => openEdit(selectedParam!)}
								class="px-3 py-1.5 bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 rounded-lg hover:bg-indigo-200 dark:hover:bg-indigo-900/50 flex items-center gap-1.5 text-sm"
							>
								<Edit2 class="w-4 h-4" />Edit
							</button>
							<button
								onclick={() => deleteParameter(selectedParam?.Name ?? '')}
								class="px-3 py-1.5 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 rounded-lg hover:bg-red-200 flex items-center gap-1.5 text-sm"
							>
								<Trash2 class="w-4 h-4" />Delete
							</button>
						</div>
					</div>

					<div class="grid grid-cols-2 gap-3">
						<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
							<p class="text-xs text-slate-500 dark:text-slate-400">Last Modified</p>
							<p class="text-sm font-medium text-slate-900 dark:text-white mt-0.5">{formatDate(selectedParam.LastModifiedDate)}</p>
						</div>
						<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
							<p class="text-xs text-slate-500 dark:text-slate-400">Version</p>
							<p class="text-sm font-medium text-slate-900 dark:text-white mt-0.5">{selectedParam.Version ?? 'N/A'}</p>
						</div>
					</div>

					<!-- Value -->
					<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4">
						<div class="flex items-center justify-between mb-2">
							<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Value</p>
							{#if selectedParam.Type === 'SecureString'}
								<button onclick={() => { showValue = !showValue; }} class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500">
									{#if showValue}
										<EyeOff class="w-4 h-4" />Hide
									{:else}
										<Eye class="w-4 h-4" />Show
									{/if}
								</button>
							{/if}
						</div>
						{#if loadingValue}
							<div class="inline-block animate-spin rounded-full h-4 w-4 border-b-2 border-indigo-500"></div>
						{:else if selectedValue !== null}
							{#if selectedParam.Type === 'SecureString' && !showValue}
								<p class="font-mono text-sm text-slate-500 dark:text-slate-400">••••••••••••</p>
							{:else}
								<pre class="font-mono text-sm text-slate-900 dark:text-white whitespace-pre-wrap break-all">{selectedValue}</pre>
							{/if}
						{/if}
					</div>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Settings class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a parameter to view details</p>
				</div>
			{/if}
		</div>
	</div>
	{:else}
	<!-- Maintenance Windows tab -->
	{#if mwLoading}
		<div class="text-center py-12">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p class="text-slate-500 dark:text-slate-400">Loading maintenance windows...</p>
		</div>
	{:else if maintenanceWindows.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Calendar class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
			<p class="text-slate-500 dark:text-slate-400 mb-4">No maintenance windows found</p>
			<button onclick={openCreateMW} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2 mx-auto">
				<Plus class="w-4 h-4" />Create your first window
			</button>
		</div>
	{:else}
		<div class="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
			{#each maintenanceWindows as mw}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-3">
					<div class="flex items-start justify-between">
						<div class="min-w-0 flex-1">
							<p class="font-semibold text-slate-900 dark:text-white truncate">{mw.Name}</p>
							{#if mw.Description}
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5 truncate">{mw.Description}</p>
							{/if}
						</div>
						<span class="ml-2 flex-shrink-0 px-2 py-0.5 text-xs rounded-full {mw.Enabled ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-500 dark:text-slate-400'}">
							{mw.Enabled ? 'Enabled' : 'Disabled'}
						</span>
					</div>
					<div class="grid grid-cols-2 gap-2 text-xs">
						<div class="bg-slate-50 dark:bg-slate-700/50 rounded p-2">
							<p class="text-slate-500 dark:text-slate-400">Duration</p>
							<p class="font-medium text-slate-900 dark:text-white">{mw.Duration}h</p>
						</div>
						<div class="bg-slate-50 dark:bg-slate-700/50 rounded p-2">
							<p class="text-slate-500 dark:text-slate-400">Cutoff</p>
							<p class="font-medium text-slate-900 dark:text-white">{mw.Cutoff}h</p>
						</div>
					</div>
					<div class="bg-slate-50 dark:bg-slate-700/50 rounded p-2 text-xs">
						<p class="text-slate-500 dark:text-slate-400">Schedule</p>
						<p class="font-mono text-slate-900 dark:text-white truncate">{mw.Schedule}</p>
					</div>
					<div class="flex justify-end">
						<button
							onclick={() => deleteMW(mw.WindowId ?? '', mw.Name ?? '')}
							class="px-3 py-1.5 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 rounded-lg hover:bg-red-200 flex items-center gap-1.5 text-sm"
						>
							<Trash2 class="w-4 h-4" />Delete
						</button>
					</div>
				</div>
			{/each}
		</div>
	{/if}
	{/if}
</div>

<!-- Create/Edit Modal -->
{#if showModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">{isEditing ? 'Edit Parameter' : 'Create Parameter'}</h2>
			<form onsubmit={(e) => { e.preventDefault(); save(); }} class="space-y-4">
				<div>
					<label for="ssm-param-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Name</label>
					<input
						id="ssm-param-name"
						type="text"
						bind:value={modalName}
						placeholder="e.g. /myapp/database/password"
						disabled={isEditing}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-60"
						required
					/>
				</div>
				<div>
					<label for="ssm-param-value" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Value</label>
					<textarea
						id="ssm-param-value"
						bind:value={modalValue}
						rows={3}
						placeholder="Parameter value..."
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
						required
					></textarea>
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="ssm-param-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Type</label>
						<select id="ssm-param-type" bind:value={modalType} disabled={isEditing} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-60">
							<option value="String">String</option>
							<option value="StringList">StringList</option>
							<option value="SecureString">SecureString</option>
						</select>
					</div>
					<div>
						<label for="ssm-param-tier" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Tier</label>
						<select id="ssm-param-tier" bind:value={modalTier} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							<option value="Standard">Standard</option>
							<option value="Advanced">Advanced</option>
						</select>
					</div>
				</div>
				<div>
					<label for="ssm-param-desc" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description (optional)</label>
					<input id="ssm-param-desc" type="text" bind:value={modalDescription} placeholder="Parameter description..." class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white flex items-center gap-1.5">
						<X class="w-4 h-4" />Cancel
					</button>
					<button type="submit" disabled={saving} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
						<Check class="w-4 h-4" />
						{saving ? 'Saving...' : (isEditing ? 'Update' : 'Create')}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Maintenance Window Modal -->
{#if showMWModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Maintenance Window</h2>
			<form onsubmit={(e) => { e.preventDefault(); saveMW(); }} class="space-y-4">
				<div>
					<label for="mw-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Name</label>
					<input
						id="mw-name"
						type="text"
						bind:value={mwName}
						placeholder="e.g. weekly-patching"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
						required
					/>
				</div>
				<div>
					<label for="mw-desc" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description (optional)</label>
					<input id="mw-desc" type="text" bind:value={mwDescription} placeholder="Window description..." class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
				</div>
				<div>
					<label for="mw-schedule" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Schedule (cron or rate expression)</label>
					<input
						id="mw-schedule"
						type="text"
						bind:value={mwSchedule}
						placeholder="cron(0 2 ? * SUN *)"
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm"
						required
					/>
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="mw-duration" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Duration (hours, 1–24)</label>
						<input id="mw-duration" type="number" min="1" max="24" bind:value={mwDuration} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
					</div>
					<div>
						<label for="mw-cutoff" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Cutoff (hours before end)</label>
						<input id="mw-cutoff" type="number" min="0" max="23" bind:value={mwCutoff} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
					</div>
				</div>
				<div class="flex items-center gap-2">
					<input id="mw-unassociated" type="checkbox" bind:checked={mwAllowUnassociated} class="rounded" />
					<label for="mw-unassociated" class="text-sm text-slate-700 dark:text-slate-300">Allow unassociated targets</label>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showMWModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white flex items-center gap-1.5">
						<X class="w-4 h-4" />Cancel
					</button>
					<button type="submit" disabled={mwSaving} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
						<Check class="w-4 h-4" />
						{mwSaving ? 'Creating...' : 'Create'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
