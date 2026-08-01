<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { getMediaStoreClient } from '$lib/aws-client';
	import {
		ListContainersCommand,
		CreateContainerCommand,
		DeleteContainerCommand,
		DescribeContainerCommand,
		GetContainerPolicyCommand,
		PutContainerPolicyCommand,
		DeleteContainerPolicyCommand,
		GetCorsPolicyCommand,
		PutCorsPolicyCommand,
		DeleteCorsPolicyCommand,
		GetLifecyclePolicyCommand,
		PutLifecyclePolicyCommand,
		DeleteLifecyclePolicyCommand,
		GetMetricPolicyCommand,
		PutMetricPolicyCommand,
		DeleteMetricPolicyCommand,
		StartAccessLoggingCommand,
		StopAccessLoggingCommand,
		TagResourceCommand,
		UntagResourceCommand,
		ListTagsForResourceCommand,
		type Container,
		type CorsRule,
		type MetricPolicy,
	} from '@aws-sdk/client-mediastore';
	import { toast } from 'svelte-sonner';
	import {
		Database,
		RefreshCw,
		Plus,
		Trash2,
		ChevronRight,
		Shield,
		Globe,
		Clock,
		BarChart3,
		Tag,
		Info,
		Search,
		X,
		Activity,
	} from 'lucide-svelte';

	const mediaStore = regionalClient(getMediaStoreClient);

	// --- State ---
	let loading = $state(false);
	let containers = $state<Container[]>([]);
	let searchQuery = $state('');
	let selectedContainer = $state<Container | null>(null);
	let loadingDetails = $state(false);
	let activeDetailTab = $state<'overview' | 'policy' | 'cors' | 'lifecycle' | 'metrics' | 'tags'>('overview');

	// Create modal
	let showCreateModal = $state(false);
	let newContainerName = $state('');
	let creating = $state(false);

	// Policy
	let containerPolicy = $state('');
	let loadingPolicy = $state(false);
	let editingPolicy = $state(false);
	let policyDraft = $state('');

	// CORS
	let corsRules = $state<CorsRule[]>([]);
	let loadingCors = $state(false);
	let editingCors = $state(false);
	let corsDraft = $state('');

	// Lifecycle
	let lifecyclePolicy = $state('');
	let loadingLifecycle = $state(false);
	let editingLifecycle = $state(false);
	let lifecycleDraft = $state('');

	// Metric Policy
	let metricPolicy = $state<MetricPolicy | null>(null);
	let loadingMetrics = $state(false);
	let editingMetrics = $state(false);
	let metricsDraft = $state('');

	// Tags
	let tags = $state<Record<string, string>>({});
	let loadingTags = $state(false);
	let newTagKey = $state('');
	let newTagValue = $state('');
	let taggingInFlight = $state(false);

	// Derived
	const filteredContainers = $derived(
		containers.filter(c =>
			(c.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	// --- Helpers ---
	function fmtTime(val: unknown): string {
		if (!val) return '—';
		const unix = val instanceof Date ? val.getTime() / 1000 : Number(val);
		return new Date(unix * 1000).toLocaleString();
	}

	// --- API ---
	async function loadContainers() {
		loading = true;
		try {
			const res = await mediaStore().send(new ListContainersCommand({ MaxResults: 100 }));
			containers = res.Containers ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load containers: ${String(err)}`);
		} finally {
			loading = false;
		}
	}

	async function createContainer() {
		if (!newContainerName.trim()) return;
		creating = true;
		try {
			await mediaStore().send(new CreateContainerCommand({ ContainerName: newContainerName.trim() }));
			toast.success(`Container "${newContainerName.trim()}" created`);
			newContainerName = '';
			showCreateModal = false;
			await loadContainers();
		} catch (err: unknown) {
			toast.error(`Failed to create container: ${String(err)}`);
		} finally {
			creating = false;
		}
	}

	async function deleteContainer(name: string | undefined) {
		if (!name) return;
		if (!(await confirmDestructive({ title: 'Delete Container', message: `Delete container "${name}"? This cannot be undone.` }))) return;
		try {
			await mediaStore().send(new DeleteContainerCommand({ ContainerName: name }));
			toast.success(`Container "${name}" deleted`);
			if (selectedContainer?.Name === name) selectedContainer = null;
			await loadContainers();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${String(err)}`);
		}
	}

	async function selectContainer(c: Container) {
		selectedContainer = c;
		activeDetailTab = 'overview';
		loadingDetails = true;
		try {
			const res = await mediaStore().send(new DescribeContainerCommand({ ContainerName: c.Name! }));
			selectedContainer = res.Container ?? c;
		} catch (err: unknown) {
			toast.error(`Failed to describe container: ${String(err)}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function switchTab(tab: typeof activeDetailTab) {
		activeDetailTab = tab;
		editingPolicy = false;
		editingCors = false;
		editingLifecycle = false;
		editingMetrics = false;

		if (!selectedContainer?.Name) return;
		const name = selectedContainer.Name;

		if (tab === 'policy') await loadPolicy(name);
		else if (tab === 'cors') await loadCors(name);
		else if (tab === 'lifecycle') await loadLifecycle(name);
		else if (tab === 'metrics') await loadMetrics(name);
		else if (tab === 'tags') await loadTags(selectedContainer.ARN!);
	}

	// --- Policy ---
	async function loadPolicy(name: string) {
		loadingPolicy = true;
		try {
			const res = await mediaStore().send(new GetContainerPolicyCommand({ ContainerName: name }));
			containerPolicy = res.Policy ?? '';
		} catch (err: unknown) {
			const msg = String(err);
			if (msg.includes('PolicyNotFoundException') || msg.includes('not found')) {
				containerPolicy = '';
			} else {
				toast.error(`Failed to load policy: ${msg}`);
			}
		} finally {
			loadingPolicy = false;
		}
	}

	async function savePolicy() {
		if (!selectedContainer?.Name) return;
		try {
			await mediaStore().send(new PutContainerPolicyCommand({ ContainerName: selectedContainer.Name, Policy: policyDraft }));
			containerPolicy = policyDraft;
			editingPolicy = false;
			toast.success('Container policy saved');
		} catch (err: unknown) {
			toast.error(`Failed to save policy: ${String(err)}`);
		}
	}

	async function deletePolicy() {
		if (!selectedContainer?.Name) return;
		if (!(await confirmDestructive({ title: 'Delete Policy', message: 'Remove the container policy?' }))) return;
		try {
			await mediaStore().send(new DeleteContainerPolicyCommand({ ContainerName: selectedContainer.Name }));
			containerPolicy = '';
			toast.success('Container policy deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete policy: ${String(err)}`);
		}
	}

	// --- CORS ---
	async function loadCors(name: string) {
		loadingCors = true;
		try {
			const res = await mediaStore().send(new GetCorsPolicyCommand({ ContainerName: name }));
			corsRules = res.CorsPolicy ?? [];
		} catch (err: unknown) {
			const msg = String(err);
			if (msg.includes('CorsPolicyNotFoundException') || msg.includes('not found')) {
				corsRules = [];
			} else {
				toast.error(`Failed to load CORS: ${msg}`);
			}
		} finally {
			loadingCors = false;
		}
	}

	async function saveCors() {
		if (!selectedContainer?.Name) return;
		try {
			const parsed = JSON.parse(corsDraft) as CorsRule[];
			await mediaStore().send(new PutCorsPolicyCommand({ ContainerName: selectedContainer.Name, CorsPolicy: parsed }));
			corsRules = parsed;
			editingCors = false;
			toast.success('CORS policy saved');
		} catch (err: unknown) {
			toast.error(`Failed to save CORS: ${String(err)}`);
		}
	}

	async function deleteCors() {
		if (!selectedContainer?.Name) return;
		if (!(await confirmDestructive({ title: 'Delete CORS Policy', message: 'Remove the CORS policy?' }))) return;
		try {
			await mediaStore().send(new DeleteCorsPolicyCommand({ ContainerName: selectedContainer.Name }));
			corsRules = [];
			toast.success('CORS policy deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete CORS: ${String(err)}`);
		}
	}

	// --- Lifecycle ---
	async function loadLifecycle(name: string) {
		loadingLifecycle = true;
		try {
			const res = await mediaStore().send(new GetLifecyclePolicyCommand({ ContainerName: name }));
			lifecyclePolicy = res.LifecyclePolicy ?? '';
		} catch (err: unknown) {
			const msg = String(err);
			if (msg.includes('PolicyNotFoundException') || msg.includes('not found')) {
				lifecyclePolicy = '';
			} else {
				toast.error(`Failed to load lifecycle: ${msg}`);
			}
		} finally {
			loadingLifecycle = false;
		}
	}

	async function saveLifecycle() {
		if (!selectedContainer?.Name) return;
		try {
			await mediaStore().send(new PutLifecyclePolicyCommand({ ContainerName: selectedContainer.Name, LifecyclePolicy: lifecycleDraft }));
			lifecyclePolicy = lifecycleDraft;
			editingLifecycle = false;
			toast.success('Lifecycle policy saved');
		} catch (err: unknown) {
			toast.error(`Failed to save lifecycle: ${String(err)}`);
		}
	}

	async function deleteLifecycle() {
		if (!selectedContainer?.Name) return;
		if (!(await confirmDestructive({ title: 'Delete Lifecycle Policy', message: 'Remove the lifecycle policy?' }))) return;
		try {
			await mediaStore().send(new DeleteLifecyclePolicyCommand({ ContainerName: selectedContainer.Name }));
			lifecyclePolicy = '';
			toast.success('Lifecycle policy deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete lifecycle: ${String(err)}`);
		}
	}

	// --- Metrics ---
	async function loadMetrics(name: string) {
		loadingMetrics = true;
		try {
			const res = await mediaStore().send(new GetMetricPolicyCommand({ ContainerName: name }));
			metricPolicy = res.MetricPolicy ?? null;
		} catch (err: unknown) {
			const msg = String(err);
			if (msg.includes('PolicyNotFoundException') || msg.includes('not found')) {
				metricPolicy = null;
			} else {
				toast.error(`Failed to load metric policy: ${msg}`);
			}
		} finally {
			loadingMetrics = false;
		}
	}

	async function saveMetrics() {
		if (!selectedContainer?.Name) return;
		try {
			const parsed = JSON.parse(metricsDraft) as MetricPolicy;
			await mediaStore().send(new PutMetricPolicyCommand({ ContainerName: selectedContainer.Name, MetricPolicy: parsed }));
			metricPolicy = parsed;
			editingMetrics = false;
			toast.success('Metric policy saved');
		} catch (err: unknown) {
			toast.error(`Failed to save metric policy: ${String(err)}`);
		}
	}

	async function deleteMetrics() {
		if (!selectedContainer?.Name) return;
		if (!(await confirmDestructive({ title: 'Delete Metric Policy', message: 'Remove the metric policy?' }))) return;
		try {
			await mediaStore().send(new DeleteMetricPolicyCommand({ ContainerName: selectedContainer.Name }));
			metricPolicy = null;
			toast.success('Metric policy deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete metric policy: ${String(err)}`);
		}
	}

	// --- Access Logging ---
	async function toggleAccessLogging(enable: boolean) {
		if (!selectedContainer?.Name) return;
		try {
			if (enable) {
				await mediaStore().send(new StartAccessLoggingCommand({ ContainerName: selectedContainer.Name }));
				toast.success('Access logging enabled');
			} else {
				await mediaStore().send(new StopAccessLoggingCommand({ ContainerName: selectedContainer.Name }));
				toast.success('Access logging disabled');
			}
			selectedContainer = { ...selectedContainer, AccessLoggingEnabled: enable };
		} catch (err: unknown) {
			toast.error(`Failed to toggle access logging: ${String(err)}`);
		}
	}

	// --- Tags ---
	async function loadTags(arn: string) {
		loadingTags = true;
		try {
			const res = await mediaStore().send(new ListTagsForResourceCommand({ Resource: arn }));
			tags = Object.fromEntries((res.Tags ?? []).map(t => [t.Key!, t.Value ?? '']));
		} catch (err: unknown) {
			toast.error(`Failed to load tags: ${String(err)}`);
		} finally {
			loadingTags = false;
		}
	}

	async function addTag() {
		if (!selectedContainer?.ARN || !newTagKey.trim()) return;
		taggingInFlight = true;
		try {
			await mediaStore().send(new TagResourceCommand({ Resource: selectedContainer.ARN, Tags: [{ Key: newTagKey.trim(), Value: newTagValue.trim() }] }));
			tags = { ...tags, [newTagKey.trim()]: newTagValue.trim() };
			newTagKey = '';
			newTagValue = '';
			toast.success('Tag added');
		} catch (err: unknown) {
			toast.error(`Failed to add tag: ${String(err)}`);
		} finally {
			taggingInFlight = false;
		}
	}

	async function removeTag(key: string) {
		if (!selectedContainer?.ARN) return;
		try {
			await mediaStore().send(new UntagResourceCommand({ Resource: selectedContainer.ARN, TagKeys: [key] }));
			const next = { ...tags };
			delete next[key];
			tags = next;
			toast.success('Tag removed');
		} catch (err: unknown) {
			toast.error(`Failed to remove tag: ${String(err)}`);
		}
	}

	// Container names are only unique within a region, so a container
	// selected in the old region must not survive a region switch -- clear
	// it (and its policy/CORS/lifecycle/metrics/tags detail state) and fall
	// back to the list.
	onRegionChange(() => {
		selectedContainer = null;
		activeDetailTab = 'overview';
		containerPolicy = '';
		corsRules = [];
		lifecyclePolicy = '';
		metricPolicy = null;
		tags = {};
		void loadContainers();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-purple-600/20 rounded-xl shadow-inner">
				<Database class="w-8 h-8 text-purple-600" />
			</div>
			<div>
				<h1 class="text-3xl font-bold bg-gradient-to-r from-purple-600 to-indigo-500 dark:from-purple-400 dark:to-indigo-400 bg-clip-text text-transparent italic tracking-tight">
					MediaStore
				</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Media container storage and policy management</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button
				onclick={() => loadContainers()}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 hover:bg-white dark:hover:bg-slate-700 border border-slate-200 dark:border-slate-600 transition-all active:scale-95 shadow-sm"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 px-5 py-2.5 bg-purple-600 hover:bg-purple-700 text-white rounded-xl font-black shadow-lg shadow-purple-600/20 transition-all active:scale-95 uppercase text-xs tracking-widest"
			>
				<Plus class="w-5 h-5" />
				New Container
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<!-- Container List -->
		<div class="lg:col-span-4 space-y-3">
			<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
				<div class="p-3 border-b border-slate-200 dark:border-slate-700/50">
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
						<input
							type="text"
							bind:value={searchQuery}
							placeholder="Search containers..."
							class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-purple-500 outline-none transition-all"
						/>
					</div>
				</div>

				<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
					{#if loading && !containers.length}
						{#each Array(3) as _}
							<div class="p-4 animate-pulse">
								<div class="h-10 bg-slate-200/50 dark:bg-slate-700/30 rounded-lg"></div>
							</div>
						{/each}
					{:else if filteredContainers.length === 0}
						<div class="p-12 text-center">
							<Database class="w-10 h-10 text-slate-200 dark:text-slate-700 mx-auto mb-3" />
							<p class="text-slate-400 text-sm italic font-bold">No containers found.</p>
							{#if !containers.length}
								<button onclick={() => (showCreateModal = true)} class="mt-3 text-xs text-purple-500 font-black underline">
									Create your first container
								</button>
							{/if}
						</div>
					{:else}
						{#each filteredContainers as container}
							<div
								role="button"
								tabindex="0"
								onclick={() => selectContainer(container)}
								onkeydown={(e) => e.key === 'Enter' && selectContainer(container)}
								class="p-4 flex items-center justify-between hover:bg-purple-500/5 dark:hover:bg-purple-500/10 cursor-pointer transition-all {selectedContainer?.Name === container.Name ? 'bg-purple-500/10 border-l-4 border-purple-500 shadow-inner' : 'border-l-4 border-transparent'}"
							>
								<div class="flex items-center gap-3 min-w-0">
									<Activity class="w-4 h-4 text-purple-500 flex-shrink-0" />
									<div class="min-w-0">
										<div class="font-black text-slate-900 dark:text-white tracking-tight text-sm truncate">
											{container.Name}
										</div>
										<div class="text-[10px] text-slate-400 font-mono mt-0.5">{container.Status ?? 'UNKNOWN'}</div>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-slate-300 flex-shrink-0" />
							</div>
						{/each}
					{/if}
				</div>
			</div>
		</div>

		<!-- Detail View -->
		<div class="lg:col-span-8">
			{#if loadingDetails}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-12 flex items-center justify-center">
					<RefreshCw class="w-8 h-8 text-slate-400 animate-spin" />
				</div>
			{:else if selectedContainer}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
					<!-- Detail header -->
					<div class="p-6 border-b border-slate-100 dark:border-slate-700/50 bg-gradient-to-br from-purple-500/5 to-indigo-500/5">
						<div class="flex justify-between items-start">
							<div>
								<h2 class="text-2xl font-black text-slate-900 dark:text-white tracking-tight">{selectedContainer.Name}</h2>
								<div class="flex items-center gap-2 mt-2 flex-wrap">
									<span class="px-2 py-0.5 rounded-lg bg-emerald-500/10 text-emerald-600 text-[9px] font-black uppercase tracking-widest border border-emerald-500/20">
										{selectedContainer.Status ?? 'UNKNOWN'}
									</span>
									{#if selectedContainer.AccessLoggingEnabled}
										<span class="px-2 py-0.5 rounded-lg bg-blue-500/10 text-blue-600 text-[9px] font-black uppercase tracking-widest border border-blue-500/20">
											Logging ON
										</span>
									{/if}
								</div>
							</div>
							<button
								onclick={() => deleteContainer(selectedContainer?.Name)}
								class="p-2 bg-rose-500/10 text-rose-600 hover:bg-rose-500/20 rounded-xl transition-all border border-rose-500/20"
								title="Delete Container"
							>
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>

					<!-- Tabs -->
					<div class="flex overflow-x-auto border-b border-slate-100 dark:border-slate-700/50 bg-white/20 dark:bg-slate-900/20">
						{#each [
							{ id: 'overview', label: 'Overview', icon: Info },
							{ id: 'policy', label: 'Access Policy', icon: Shield },
							{ id: 'cors', label: 'CORS', icon: Globe },
							{ id: 'lifecycle', label: 'Lifecycle', icon: Clock },
							{ id: 'metrics', label: 'Metrics', icon: BarChart3 },
							{ id: 'tags', label: 'Tags', icon: Tag },
						] as tab}
							{@const Icon = tab.icon}
							<button
								onclick={() => switchTab(tab.id as typeof activeDetailTab)}
								class="flex items-center gap-1.5 px-4 py-3 text-xs font-black uppercase tracking-widest whitespace-nowrap transition-all border-b-2 {activeDetailTab === tab.id ? 'border-purple-500 text-purple-600 dark:text-purple-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}"
							>
								<Icon class="w-3.5 h-3.5" />
								{tab.label}
							</button>
						{/each}
					</div>

					<!-- Tab Content -->
					<div class="p-6">
						<!-- Overview -->
						{#if activeDetailTab === 'overview'}
							<div class="space-y-4">
								<div class="grid grid-cols-1 sm:grid-cols-2 gap-4">
									{#each [
										{ label: 'ARN', value: selectedContainer.ARN ?? '—' },
										{ label: 'Endpoint', value: selectedContainer.Endpoint ?? '—' },
										{ label: 'Status', value: selectedContainer.Status ?? '—' },
										{ label: 'Created', value: fmtTime(selectedContainer.CreationTime) },
									] as field}
										<div class="bg-slate-50 dark:bg-slate-900/40 rounded-xl p-3 border border-slate-100 dark:border-slate-700/50">
											<div class="text-[9px] font-black uppercase tracking-widest text-slate-400 mb-1">{field.label}</div>
											<div class="text-sm text-slate-800 dark:text-slate-200 font-mono break-all">{field.value}</div>
										</div>
									{/each}
								</div>

								<div class="bg-slate-50 dark:bg-slate-900/40 rounded-xl p-4 border border-slate-100 dark:border-slate-700/50">
									<div class="flex items-center justify-between">
										<div>
											<div class="text-[9px] font-black uppercase tracking-widest text-slate-400 mb-1">Access Logging</div>
											<div class="text-sm text-slate-700 dark:text-slate-300">
												{selectedContainer.AccessLoggingEnabled ? 'Enabled' : 'Disabled'}
											</div>
										</div>
										<button
											onclick={() => toggleAccessLogging(!selectedContainer?.AccessLoggingEnabled)}
											class="px-4 py-2 text-xs font-black uppercase tracking-widest rounded-xl transition-all {selectedContainer.AccessLoggingEnabled
												? 'bg-amber-500/10 text-amber-600 hover:bg-amber-500/20 border border-amber-500/20'
												: 'bg-emerald-500/10 text-emerald-600 hover:bg-emerald-500/20 border border-emerald-500/20'}"
										>
											{selectedContainer.AccessLoggingEnabled ? 'Disable' : 'Enable'}
										</button>
									</div>
								</div>
							</div>

						<!-- Container Policy -->
						{:else if activeDetailTab === 'policy'}
							<div class="space-y-4">
								{#if loadingPolicy}
									<div class="flex justify-center py-8"><RefreshCw class="w-6 h-6 animate-spin text-slate-400" /></div>
								{:else if editingPolicy}
									<textarea
										bind:value={policyDraft}
										rows={12}
										class="w-full font-mono text-xs bg-slate-900 text-green-300 rounded-xl p-4 border border-slate-700 focus:ring-2 focus:ring-purple-500 outline-none resize-y"
										placeholder={`{"Version":"2012-10-17","Statement":[]}`}
									></textarea>
									<div class="flex gap-2">
										<button onclick={savePolicy} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Save Policy
										</button>
										<button onclick={() => (editingPolicy = false)} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-slate-200 dark:hover:bg-slate-600 transition-all">
											Cancel
										</button>
									</div>
								{:else if containerPolicy}
									<pre class="font-mono text-xs bg-slate-900 text-green-300 rounded-xl p-4 overflow-auto max-h-72 border border-slate-700">{JSON.stringify(JSON.parse(containerPolicy), null, 2)}</pre>
									<div class="flex gap-2">
										<button onclick={() => { policyDraft = containerPolicy; editingPolicy = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Edit
										</button>
										<button onclick={deletePolicy} class="px-4 py-2 bg-rose-500/10 text-rose-600 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-rose-500/20 border border-rose-500/20 transition-all">
											Delete
										</button>
									</div>
								{:else}
									<div class="text-center py-8">
										<Shield class="w-10 h-10 text-slate-200 dark:text-slate-700 mx-auto mb-3" />
										<p class="text-slate-400 text-sm italic mb-4">No container policy set</p>
										<button onclick={() => { policyDraft = ''; editingPolicy = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Add Policy
										</button>
									</div>
								{/if}
							</div>

						<!-- CORS Policy -->
						{:else if activeDetailTab === 'cors'}
							<div class="space-y-4">
								{#if loadingCors}
									<div class="flex justify-center py-8"><RefreshCw class="w-6 h-6 animate-spin text-slate-400" /></div>
								{:else if editingCors}
									<textarea
										bind:value={corsDraft}
										rows={14}
										class="w-full font-mono text-xs bg-slate-900 text-green-300 rounded-xl p-4 border border-slate-700 focus:ring-2 focus:ring-purple-500 outline-none resize-y"
										placeholder={`[{"AllowedOrigins":["*"],"AllowedMethods":["GET"],"AllowedHeaders":["*"],"MaxAgeSeconds":3000}]`}
									></textarea>
									<div class="flex gap-2">
										<button onclick={saveCors} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Save CORS
										</button>
										<button onclick={() => (editingCors = false)} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-slate-200 dark:hover:bg-slate-600 transition-all">
											Cancel
										</button>
									</div>
								{:else if corsRules.length > 0}
									<div class="space-y-3">
										{#each corsRules as rule, i}
											<div class="bg-slate-50 dark:bg-slate-900/40 rounded-xl p-4 border border-slate-100 dark:border-slate-700/50">
												<div class="text-[9px] font-black uppercase tracking-widest text-slate-400 mb-2">Rule {i + 1}</div>
												<div class="grid grid-cols-2 gap-2 text-xs">
													<div>
														<span class="text-slate-400">Origins:</span>
														<span class="ml-1 font-mono text-slate-700 dark:text-slate-300">{(rule.AllowedOrigins ?? []).join(', ')}</span>
													</div>
													<div>
														<span class="text-slate-400">Methods:</span>
														<span class="ml-1 font-mono text-slate-700 dark:text-slate-300">{(rule.AllowedMethods ?? []).join(', ')}</span>
													</div>
													<div>
														<span class="text-slate-400">Headers:</span>
														<span class="ml-1 font-mono text-slate-700 dark:text-slate-300">{(rule.AllowedHeaders ?? []).join(', ')}</span>
													</div>
													{#if rule.MaxAgeSeconds}
														<div>
															<span class="text-slate-400">Max Age:</span>
															<span class="ml-1 font-mono text-slate-700 dark:text-slate-300">{rule.MaxAgeSeconds}s</span>
														</div>
													{/if}
												</div>
											</div>
										{/each}
									</div>
									<div class="flex gap-2">
										<button onclick={() => { corsDraft = JSON.stringify(corsRules, null, 2); editingCors = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Edit
										</button>
										<button onclick={deleteCors} class="px-4 py-2 bg-rose-500/10 text-rose-600 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-rose-500/20 border border-rose-500/20 transition-all">
											Delete
										</button>
									</div>
								{:else}
									<div class="text-center py-8">
										<Globe class="w-10 h-10 text-slate-200 dark:text-slate-700 mx-auto mb-3" />
										<p class="text-slate-400 text-sm italic mb-4">No CORS policy set</p>
										<button onclick={() => { corsDraft = ''; editingCors = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Add CORS Policy
										</button>
									</div>
								{/if}
							</div>

						<!-- Lifecycle Policy -->
						{:else if activeDetailTab === 'lifecycle'}
							<div class="space-y-4">
								{#if loadingLifecycle}
									<div class="flex justify-center py-8"><RefreshCw class="w-6 h-6 animate-spin text-slate-400" /></div>
								{:else if editingLifecycle}
									<textarea
										bind:value={lifecycleDraft}
										rows={12}
										class="w-full font-mono text-xs bg-slate-900 text-green-300 rounded-xl p-4 border border-slate-700 focus:ring-2 focus:ring-purple-500 outline-none resize-y"
										placeholder={`{"rules":[{"definition":{"path":[{"wildcard":"*"}],"seconds":[{"numeric":["<",300]}]},"action":"EXPIRE"}]}`}
									></textarea>
									<div class="flex gap-2">
										<button onclick={saveLifecycle} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Save Lifecycle
										</button>
										<button onclick={() => (editingLifecycle = false)} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-slate-200 dark:hover:bg-slate-600 transition-all">
											Cancel
										</button>
									</div>
								{:else if lifecyclePolicy}
									<pre class="font-mono text-xs bg-slate-900 text-green-300 rounded-xl p-4 overflow-auto max-h-72 border border-slate-700">{JSON.stringify(JSON.parse(lifecyclePolicy), null, 2)}</pre>
									<div class="flex gap-2">
										<button onclick={() => { lifecycleDraft = lifecyclePolicy; editingLifecycle = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Edit
										</button>
										<button onclick={deleteLifecycle} class="px-4 py-2 bg-rose-500/10 text-rose-600 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-rose-500/20 border border-rose-500/20 transition-all">
											Delete
										</button>
									</div>
								{:else}
									<div class="text-center py-8">
										<Clock class="w-10 h-10 text-slate-200 dark:text-slate-700 mx-auto mb-3" />
										<p class="text-slate-400 text-sm italic mb-4">No lifecycle policy set</p>
										<button onclick={() => { lifecycleDraft = ''; editingLifecycle = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Add Lifecycle Policy
										</button>
									</div>
								{/if}
							</div>

						<!-- Metric Policy -->
						{:else if activeDetailTab === 'metrics'}
							<div class="space-y-4">
								{#if loadingMetrics}
									<div class="flex justify-center py-8"><RefreshCw class="w-6 h-6 animate-spin text-slate-400" /></div>
								{:else if editingMetrics}
									<textarea
										bind:value={metricsDraft}
										rows={12}
										class="w-full font-mono text-xs bg-slate-900 text-green-300 rounded-xl p-4 border border-slate-700 focus:ring-2 focus:ring-purple-500 outline-none resize-y"
										placeholder={`{"ContainerLevelMetrics":"ENABLED","MetricPolicyRules":[]}`}
									></textarea>
									<div class="flex gap-2">
										<button onclick={saveMetrics} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Save Metric Policy
										</button>
										<button onclick={() => (editingMetrics = false)} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-slate-200 dark:hover:bg-slate-600 transition-all">
											Cancel
										</button>
									</div>
								{:else if metricPolicy}
									<div class="space-y-3">
										<div class="bg-slate-50 dark:bg-slate-900/40 rounded-xl p-4 border border-slate-100 dark:border-slate-700/50">
											<div class="text-[9px] font-black uppercase tracking-widest text-slate-400 mb-1">Container Level Metrics</div>
											<div class="text-sm font-mono text-slate-700 dark:text-slate-300">{metricPolicy.ContainerLevelMetrics ?? '—'}</div>
										</div>
										{#if metricPolicy.MetricPolicyRules && metricPolicy.MetricPolicyRules.length > 0}
											<div class="text-[9px] font-black uppercase tracking-widest text-slate-400 mt-4 mb-2">Rules</div>
											{#each metricPolicy.MetricPolicyRules as rule}
												<div class="bg-slate-50 dark:bg-slate-900/40 rounded-xl p-3 border border-slate-100 dark:border-slate-700/50 text-xs">
													<span class="text-slate-400">Group:</span>
													<span class="ml-1 font-mono text-slate-700 dark:text-slate-300">{rule.ObjectGroupName}</span>
													<span class="mx-2 text-slate-300">|</span>
													<span class="text-slate-400">Pattern:</span>
													<span class="ml-1 font-mono text-slate-700 dark:text-slate-300">{rule.ObjectGroup}</span>
												</div>
											{/each}
										{/if}
									</div>
									<div class="flex gap-2">
										<button onclick={() => { metricsDraft = JSON.stringify(metricPolicy, null, 2); editingMetrics = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Edit
										</button>
										<button onclick={deleteMetrics} class="px-4 py-2 bg-rose-500/10 text-rose-600 text-xs font-black uppercase tracking-widest rounded-xl hover:bg-rose-500/20 border border-rose-500/20 transition-all">
											Delete
										</button>
									</div>
								{:else}
									<div class="text-center py-8">
										<BarChart3 class="w-10 h-10 text-slate-200 dark:text-slate-700 mx-auto mb-3" />
										<p class="text-slate-400 text-sm italic mb-4">No metric policy set</p>
										<button onclick={() => { metricsDraft = ''; editingMetrics = true; }} class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 transition-all">
											Add Metric Policy
										</button>
									</div>
								{/if}
							</div>

						<!-- Tags -->
						{:else if activeDetailTab === 'tags'}
							<div class="space-y-4">
								{#if loadingTags}
									<div class="flex justify-center py-8"><RefreshCw class="w-6 h-6 animate-spin text-slate-400" /></div>
								{:else}
									<!-- Add tag -->
									<div class="flex gap-2">
										<input
											type="text"
											bind:value={newTagKey}
											placeholder="Key"
											class="flex-1 px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-purple-500 outline-none"
										/>
										<input
											type="text"
											bind:value={newTagValue}
											placeholder="Value"
											class="flex-1 px-3 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm focus:ring-2 focus:ring-purple-500 outline-none"
										/>
										<button
											onclick={addTag}
											disabled={taggingInFlight || !newTagKey.trim()}
											class="px-4 py-2 bg-purple-600 text-white text-xs font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 disabled:opacity-50 transition-all"
										>
											{taggingInFlight ? '...' : 'Add'}
										</button>
									</div>

									<!-- Tag list -->
									{#if Object.keys(tags).length === 0}
										<div class="text-center py-8">
											<Tag class="w-10 h-10 text-slate-200 dark:text-slate-700 mx-auto mb-3" />
											<p class="text-slate-400 text-sm italic">No tags set</p>
										</div>
									{:else}
										<div class="space-y-2">
											{#each Object.entries(tags) as [key, value]}
												<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-900/40 rounded-xl px-4 py-2.5 border border-slate-100 dark:border-slate-700/50">
													<div class="text-sm">
														<span class="font-mono font-bold text-slate-700 dark:text-slate-300">{key}</span>
														{#if value}
															<span class="text-slate-400 mx-2">=</span>
															<span class="font-mono text-slate-500 dark:text-slate-400">{value}</span>
														{/if}
													</div>
													<button
														onclick={() => removeTag(key)}
														class="p-1 text-rose-400 hover:text-rose-600 hover:bg-rose-50 dark:hover:bg-rose-900/20 rounded-lg transition-all"
														title="Remove tag"
													>
														<X class="w-3.5 h-3.5" />
													</button>
												</div>
											{/each}
										</div>
									{/if}
								{/if}
							</div>
						{/if}
					</div>
				</div>
			{:else}
				<div class="bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-12 flex flex-col items-center justify-center text-center">
					<Database class="w-12 h-12 text-slate-200 dark:text-slate-700 mb-4" />
					<p class="text-slate-400 text-sm italic font-bold">Select a container to inspect</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Container Modal -->
{#if showCreateModal}
	<div
		role="dialog"
		aria-modal="true"
		tabindex="-1"
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm"
		onclick={(e) => e.target === e.currentTarget && (showCreateModal = false)}
		onkeydown={(e) => e.key === 'Escape' && (showCreateModal = false)}
	>
		<div class="bg-white dark:bg-slate-800 rounded-2xl shadow-2xl p-6 w-full max-w-md mx-4">
			<h2 class="text-lg font-black uppercase tracking-tight text-slate-900 dark:text-white mb-4">New Container</h2>
			<input
				type="text"
				bind:value={newContainerName}
				placeholder="Container name"
				class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900 rounded-xl text-sm focus:ring-2 focus:ring-purple-500 outline-none mb-4"
				onkeydown={(e) => e.key === 'Enter' && createContainer()}
			/>
			<div class="flex gap-2 justify-end">
				<button onclick={() => (showCreateModal = false)} class="px-4 py-2 text-sm font-bold text-slate-500 hover:text-slate-700 transition-all">
					Cancel
				</button>
				<button
					onclick={createContainer}
					disabled={creating || !newContainerName.trim()}
					class="px-5 py-2 bg-purple-600 text-white text-sm font-black uppercase tracking-widest rounded-xl hover:bg-purple-700 disabled:opacity-50 transition-all"
				>
					{creating ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}
