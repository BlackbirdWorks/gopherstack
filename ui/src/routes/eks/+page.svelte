<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getEKSClient } from '$lib/aws-client';
	import {
		ListClustersCommand,
		DescribeClusterCommand,
		CreateClusterCommand,
		DeleteClusterCommand,
		ListNodegroupsCommand,
		DescribeNodegroupCommand,
		CreateNodegroupCommand,
		DeleteNodegroupCommand,
		ListAddonsCommand,
		DescribeAddonCommand,
		CreateAddonCommand,
		DeleteAddonCommand,
		ListFargateProfilesCommand,
		DescribeFargateProfileCommand,
		CreateFargateProfileCommand,
		DeleteFargateProfileCommand,
		ListPodIdentityAssociationsCommand,
		CreatePodIdentityAssociationCommand,
		DeletePodIdentityAssociationCommand,
		ListAccessEntriesCommand,
		DescribeAccessEntryCommand,
		CreateAccessEntryCommand,
		DeleteAccessEntryCommand,
		UpdateNodegroupConfigCommand,
		type Cluster,
		type Nodegroup,
		type Addon,
		type FargateProfile,
		type PodIdentityAssociationSummary,
		type AccessEntry,
		AMITypes
	} from '@aws-sdk/client-eks';
	import { toast } from 'svelte-sonner';
	import { Box, Search, RefreshCw, Plus, Trash2, Server, Layers, Shield, Package, Cloud, Key } from 'lucide-svelte';

	const eks = getEKSClient();

	let loading = $state(false);
	let clusters = $state<string[]>([]);
	let searchQuery = $state('');
	let selectedCluster = $state<Cluster | null>(null);
	let loadingCluster = $state(false);
	let nodeGroups = $state<string[]>([]);
	let nodeGroupDetails = $state<Nodegroup[]>([]);
	let loadingNodeGroups = $state(false);
	// Nodegroup scaling editor
	let scalingNG = $state<string | null>(null);
	let scaleMin = $state(1);
	let scaleDesired = $state(2);
	let scaleMax = $state(3);
	let scalingUpdate = $state(false);
	let detailTab = $state<'overview' | 'nodegroups' | 'addons' | 'fargate' | 'podidentity' | 'access'>('overview');

	// Addon state
	let addonNames = $state<string[]>([]);
	let addonDetails = $state<Addon[]>([]);
	let loadingAddons = $state(false);
	let showCreateAddon = $state(false);
	let creatingAddon = $state(false);
	let newAddonName = $state('vpc-cni');

	// Fargate state
	let fargateNames = $state<string[]>([]);
	let fargateDetails = $state<FargateProfile[]>([]);
	let loadingFargate = $state(false);
	let showCreateFargate = $state(false);
	let creatingFargate = $state(false);
	let newFargateName = $state('');
	let newFargateNamespace = $state('default');

	// Pod Identity state
	let podIdentities = $state<PodIdentityAssociationSummary[]>([]);
	let loadingPodIdentity = $state(false);
	let showCreatePodIdentity = $state(false);
	let creatingPodIdentity = $state(false);
	let newPodIdNamespace = $state('default');
	let newPodIdServiceAccount = $state('');
	let newPodIdRoleArn = $state('');

	// Access Entry state
	let accessEntryArns = $state<string[]>([]);
	let accessEntryDetails = $state<AccessEntry[]>([]);
	let loadingAccessEntries = $state(false);
	let showCreateAccessEntry = $state(false);
	let creatingAccessEntry = $state(false);
	let newAccessPrincipalArn = $state('');
	let newAccessType = $state('STANDARD');

	// Create cluster modal
	let showCreateCluster = $state(false);
	let creatingCluster = $state(false);
	let newClusterName = $state('');
	let newK8sVersion = $state('1.31');
	let newRoleArn = $state('');

	// Create nodegroup modal
	let showCreateNG = $state(false);
	let creatingNG = $state(false);
	let newNGName = $state('');
	let newNGInstanceType = $state('t3.medium');
	let newNGAmiType = $state('AL2_x86_64');
	let newNGMin = $state(1);
	let newNGMax = $state(3);
	let newNGDesired = $state(2);

	const detailTabs = [
		{ id: 'overview', label: 'Overview', icon: Server },
		{ id: 'nodegroups', label: 'Node Groups', icon: Layers },
		{ id: 'addons', label: 'Addons', icon: Package },
		{ id: 'fargate', label: 'Fargate', icon: Cloud },
		{ id: 'podidentity', label: 'Pod Identity', icon: Key },
		{ id: 'access', label: 'Access', icon: Shield }
	];

	const filteredClusters = $derived(
		clusters.filter((c) => c.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function statusColor(status: string | undefined): string {
		if (status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'CREATING') return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'DELETING' || status === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	async function loadClusters() {
		loading = true;
		try {
			const res = await eks.send(new ListClustersCommand({}));
			clusters = res.clusters ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load clusters: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectCluster(name: string) {
		loadingCluster = true;
		detailTab = 'overview';
		nodeGroups = [];
		nodeGroupDetails = [];
		addonNames = [];
		addonDetails = [];
		fargateNames = [];
		fargateDetails = [];
		podIdentities = [];
		accessEntryArns = [];
		accessEntryDetails = [];
		try {
			const res = await eks.send(new DescribeClusterCommand({ name }));
			selectedCluster = res.cluster ?? null;
			await loadNodeGroups(name);
		} catch (err: unknown) {
			toast.error(`Failed to describe cluster: ${(err as Error).message}`);
		} finally {
			loadingCluster = false;
		}
	}

	async function loadNodeGroups(clusterName: string) {
		loadingNodeGroups = true;
		try {
			const res = await eks.send(new ListNodegroupsCommand({ clusterName }));
			nodeGroups = res.nodegroups ?? [];
			const details = await Promise.all(
				nodeGroups.slice(0, 10).map(async (ng) => {
					const d = await eks.send(new DescribeNodegroupCommand({ clusterName, nodegroupName: ng }));
					return d.nodegroup!;
				})
			);
			nodeGroupDetails = details.filter(Boolean);
		} catch (err: unknown) {
			toast.error(`Failed to load node groups: ${(err as Error).message}`);
		} finally {
			loadingNodeGroups = false;
		}
	}

	async function loadAddons(clusterName: string) {
		loadingAddons = true;
		try {
			const res = await eks.send(new ListAddonsCommand({ clusterName }));
			addonNames = res.addons ?? [];
			const details = await Promise.all(
				addonNames.slice(0, 20).map(async (name) => {
					const d = await eks.send(new DescribeAddonCommand({ clusterName, addonName: name }));
					return d.addon!;
				})
			);
			addonDetails = details.filter(Boolean);
		} catch (err: unknown) {
			toast.error(`Failed to load addons: ${(err as Error).message}`);
		} finally {
			loadingAddons = false;
		}
	}

	async function loadFargateProfiles(clusterName: string) {
		loadingFargate = true;
		try {
			const res = await eks.send(new ListFargateProfilesCommand({ clusterName }));
			fargateNames = res.fargateProfileNames ?? [];
			const details = await Promise.all(
				fargateNames.slice(0, 20).map(async (name) => {
					const d = await eks.send(new DescribeFargateProfileCommand({ clusterName, fargateProfileName: name }));
					return d.fargateProfile!;
				})
			);
			fargateDetails = details.filter(Boolean);
		} catch (err: unknown) {
			toast.error(`Failed to load Fargate profiles: ${(err as Error).message}`);
		} finally {
			loadingFargate = false;
		}
	}

	async function loadPodIdentities(clusterName: string) {
		loadingPodIdentity = true;
		try {
			const res = await eks.send(new ListPodIdentityAssociationsCommand({ clusterName }));
			podIdentities = res.associations ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load pod identities: ${(err as Error).message}`);
		} finally {
			loadingPodIdentity = false;
		}
	}

	async function loadAccessEntries(clusterName: string) {
		loadingAccessEntries = true;
		try {
			const res = await eks.send(new ListAccessEntriesCommand({ clusterName }));
			accessEntryArns = res.accessEntries ?? [];
			const details = await Promise.all(
				accessEntryArns.slice(0, 20).map(async (arn) => {
					const d = await eks.send(new DescribeAccessEntryCommand({ clusterName, principalArn: arn }));
					return d.accessEntry!;
				})
			);
			accessEntryDetails = details.filter(Boolean);
		} catch (err: unknown) {
			toast.error(`Failed to load access entries: ${(err as Error).message}`);
		} finally {
			loadingAccessEntries = false;
		}
	}

	async function createCluster() {
		if (!newClusterName.trim() || !newRoleArn.trim()) return;
		creatingCluster = true;
		try {
			await eks.send(new CreateClusterCommand({
				name: newClusterName.trim(),
				version: newK8sVersion,
				roleArn: newRoleArn.trim(),
				resourcesVpcConfig: { subnetIds: [], securityGroupIds: [] }
			}));
			toast.success(`Cluster "${newClusterName.trim()}" creating`);
			showCreateCluster = false;
			newClusterName = '';
			newRoleArn = '';
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creatingCluster = false;
		}
	}

	async function deleteCluster(name: string) {
		if (!await confirmDestructive({ title: 'Delete EKS Cluster', message: `Delete cluster "${name}"? All node groups and workloads will be terminated.` })) return;
		try {
			await eks.send(new DeleteClusterCommand({ name }));
			toast.success(`Cluster "${name}" deleting`);
			if (selectedCluster?.name === name) selectedCluster = null;
			await loadClusters();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createNodeGroup() {
		if (!selectedCluster?.name || !newNGName.trim()) return;
		creatingNG = true;
		try {
			await eks.send(new CreateNodegroupCommand({
				clusterName: selectedCluster.name,
				nodegroupName: newNGName.trim(),
				instanceTypes: [newNGInstanceType],
				amiType: newNGAmiType as AMITypes,
				scalingConfig: { minSize: newNGMin, maxSize: newNGMax, desiredSize: newNGDesired },
				nodeRole: 'arn:aws:iam::123456789012:role/EKSNodeRole',
				subnets: []
			}));
			toast.success(`Node group "${newNGName.trim()}" creating`);
			showCreateNG = false;
			newNGName = '';
			await loadNodeGroups(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Create node group failed: ${(err as Error).message}`);
		} finally {
			creatingNG = false;
		}
	}

	async function deleteNodeGroup(ngName: string) {
		if (!selectedCluster?.name || !await confirmDestructive({ title: 'Delete Node Group', message: `Delete node group "${ngName}"? All nodes will be drained and terminated.` })) return;
		try {
			await eks.send(new DeleteNodegroupCommand({ clusterName: selectedCluster.name, nodegroupName: ngName }));
			toast.success(`Node group "${ngName}" deleting`);
			await loadNodeGroups(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function startScaleNodeGroup(ng: Nodegroup) {
		if (scalingNG === ng.nodegroupName) {
			scalingNG = null;
			return;
		}
		scalingNG = ng.nodegroupName ?? null;
		scaleMin = ng.scalingConfig?.minSize ?? 1;
		scaleDesired = ng.scalingConfig?.desiredSize ?? 1;
		scaleMax = ng.scalingConfig?.maxSize ?? 1;
	}

	async function scaleNodeGroup(ngName: string) {
		if (!selectedCluster?.name) return;
		if (scaleMin > scaleDesired || scaleDesired > scaleMax) {
			toast.error('Require min ≤ desired ≤ max');
			return;
		}
		scalingUpdate = true;
		try {
			await eks.send(new UpdateNodegroupConfigCommand({
				clusterName: selectedCluster.name,
				nodegroupName: ngName,
				scalingConfig: { minSize: scaleMin, desiredSize: scaleDesired, maxSize: scaleMax }
			}));
			toast.success(`Node group "${ngName}" scaling updated`);
			scalingNG = null;
			await loadNodeGroups(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Scaling update failed: ${(err as Error).message}`);
		} finally {
			scalingUpdate = false;
		}
	}

	function kubeconfigCmd(): string {
		const region = selectedCluster?.arn?.split(':')[3] ?? 'us-east-1';
		return `aws eks update-kubeconfig --name ${selectedCluster?.name ?? ''} --region ${region}`;
	}

	function copyKubeconfigCmd() {
		if (!selectedCluster?.name) return;
		navigator.clipboard.writeText(kubeconfigCmd()).then(() => toast.success('Command copied')).catch(() => toast.error('Copy failed'));
	}

	async function createAddon() {
		if (!selectedCluster?.name) return;
		creatingAddon = true;
		try {
			await eks.send(new CreateAddonCommand({ clusterName: selectedCluster.name, addonName: newAddonName }));
			toast.success(`Addon "${newAddonName}" installing`);
			showCreateAddon = false;
			await loadAddons(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Create addon failed: ${(err as Error).message}`);
		} finally {
			creatingAddon = false;
		}
	}

	async function deleteAddon(addonName: string) {
		if (!selectedCluster?.name || !await confirmDestructive({ title: 'Delete Addon', message: `Remove addon "${addonName}" from the cluster?` })) return;
		try {
			await eks.send(new DeleteAddonCommand({ clusterName: selectedCluster.name, addonName }));
			toast.success(`Addon "${addonName}" removing`);
			await loadAddons(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Delete addon failed: ${(err as Error).message}`);
		}
	}

	async function createFargateProfile() {
		if (!selectedCluster?.name || !newFargateName.trim()) return;
		creatingFargate = true;
		try {
			await eks.send(new CreateFargateProfileCommand({
				clusterName: selectedCluster.name,
				fargateProfileName: newFargateName.trim(),
				podExecutionRoleArn: 'arn:aws:iam::123456789012:role/FargatePodRole',
				selectors: [{ namespace: newFargateNamespace }]
			}));
			toast.success(`Fargate profile "${newFargateName.trim()}" creating`);
			showCreateFargate = false;
			newFargateName = '';
			await loadFargateProfiles(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Create Fargate profile failed: ${(err as Error).message}`);
		} finally {
			creatingFargate = false;
		}
	}

	async function deleteFargateProfile(profileName: string) {
		if (!selectedCluster?.name || !await confirmDestructive({ title: 'Delete Fargate Profile', message: `Delete Fargate profile "${profileName}"?` })) return;
		try {
			await eks.send(new DeleteFargateProfileCommand({ clusterName: selectedCluster.name, fargateProfileName: profileName }));
			toast.success(`Fargate profile "${profileName}" deleting`);
			await loadFargateProfiles(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createPodIdentity() {
		if (!selectedCluster?.name || !newPodIdServiceAccount.trim() || !newPodIdRoleArn.trim()) return;
		creatingPodIdentity = true;
		try {
			await eks.send(new CreatePodIdentityAssociationCommand({
				clusterName: selectedCluster.name,
				namespace: newPodIdNamespace,
				serviceAccount: newPodIdServiceAccount.trim(),
				roleArn: newPodIdRoleArn.trim()
			}));
			toast.success('Pod identity association created');
			showCreatePodIdentity = false;
			newPodIdServiceAccount = '';
			newPodIdRoleArn = '';
			await loadPodIdentities(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creatingPodIdentity = false;
		}
	}

	async function deletePodIdentity(assocId: string) {
		if (!selectedCluster?.name || !await confirmDestructive({ title: 'Delete Pod Identity', message: 'Delete this pod identity association?' })) return;
		try {
			await eks.send(new DeletePodIdentityAssociationCommand({ clusterName: selectedCluster.name, associationId: assocId }));
			toast.success('Pod identity deleted');
			await loadPodIdentities(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function createAccessEntry() {
		if (!selectedCluster?.name || !newAccessPrincipalArn.trim()) return;
		creatingAccessEntry = true;
		try {
			await eks.send(new CreateAccessEntryCommand({
				clusterName: selectedCluster.name,
				principalArn: newAccessPrincipalArn.trim(),
				type: newAccessType
			}));
			toast.success('Access entry created');
			showCreateAccessEntry = false;
			newAccessPrincipalArn = '';
			await loadAccessEntries(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creatingAccessEntry = false;
		}
	}

	async function deleteAccessEntry(principalArn: string) {
		if (!selectedCluster?.name || !await confirmDestructive({ title: 'Delete Access Entry', message: `Delete access entry for "${principalArn}"?` })) return;
		try {
			await eks.send(new DeleteAccessEntryCommand({ clusterName: selectedCluster.name, principalArn }));
			toast.success('Access entry deleted');
			await loadAccessEntries(selectedCluster.name);
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function onTabSwitch(tab: string) {
		detailTab = tab as typeof detailTab;
		if (!selectedCluster?.name) return;
		if (tab === 'addons' && addonDetails.length === 0 && !loadingAddons) loadAddons(selectedCluster.name);
		if (tab === 'fargate' && fargateDetails.length === 0 && !loadingFargate) loadFargateProfiles(selectedCluster.name);
		if (tab === 'podidentity' && podIdentities.length === 0 && !loadingPodIdentity) loadPodIdentities(selectedCluster.name);
		if (tab === 'access' && accessEntryDetails.length === 0 && !loadingAccessEntries) loadAccessEntries(selectedCluster.name);
	}

	onMount(() => { loadClusters(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-cyan-100 dark:bg-cyan-900/30 rounded-lg">
				<Box class="w-6 h-6 text-cyan-600 dark:text-cyan-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">EKS Clusters</h1>
				<p class="text-slate-600 dark:text-slate-300">Elastic Kubernetes Service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadClusters()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button onclick={() => { showCreateCluster = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />Create Cluster
			</button>
		</div>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input type="text" bind:value={searchQuery} placeholder="Search clusters..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Cluster List -->
		<div class="lg:col-span-1 space-y-2 max-h-[70vh] overflow-y-auto">
			{#if loading}
				<div class="text-center py-8"><div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500"></div></div>
			{:else if filteredClusters.length === 0}
				<div class="text-center py-8 text-slate-500 dark:text-slate-400">No clusters found</div>
			{:else}
				{#each filteredClusters as cluster}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectCluster(cluster)}
						onkeydown={(e) => { if (e.key === 'Enter') selectCluster(cluster); }}
						class="w-full text-left p-4 rounded-lg border transition-all cursor-pointer {selectedCluster?.name === cluster ? 'border-indigo-500 bg-indigo-50 dark:bg-indigo-900/20' : 'border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 hover:border-indigo-300'}"
					>
						<div class="flex items-center justify-between">
							<div class="flex items-center gap-3">
								<Server class="w-5 h-5 text-slate-400" />
								<span class="font-medium text-slate-900 dark:text-white">{cluster}</span>
							</div>
							<button onclick={(e) => { e.stopPropagation(); deleteCluster(cluster); }} class="p-1 text-slate-400 hover:text-red-500">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>
				{/each}
			{/if}
		</div>

		<!-- Cluster Detail -->
		<div class="lg:col-span-2">
			{#if selectedCluster}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
					<div class="p-4 border-b border-slate-200 dark:border-slate-700">
						<div class="flex items-center justify-between">
							<div class="flex items-center gap-3">
								<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedCluster.name}</h2>
								<span class="px-2 py-0.5 text-xs rounded-full {statusColor(selectedCluster.status)}">{selectedCluster.status}</span>
							</div>
						</div>
					</div>

					<div class="p-4 space-y-4">
						<!-- Tabs -->
						<div class="flex border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
							{#each detailTabs as tab}
								<button
									onclick={() => onTabSwitch(tab.id)}
									class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors whitespace-nowrap {detailTab === tab.id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400'}"
								>
									<tab.icon class="w-4 h-4" />{tab.label}
								</button>
							{/each}
						</div>

						{#if detailTab === 'overview'}
							{#if loadingCluster}
								<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
							{:else}
								<div class="grid grid-cols-2 gap-3">
									{#each [
										['K8s Version', selectedCluster.version ?? 'N/A'],
										['Endpoint', selectedCluster.endpoint ? selectedCluster.endpoint.slice(0, 30) + '...' : 'N/A'],
										['Role ARN', selectedCluster.roleArn?.split('/').pop() ?? 'N/A'],
										['Created', selectedCluster.createdAt ? new Date(selectedCluster.createdAt).toLocaleDateString() : 'N/A']
									] as [label, value]}
										<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
											<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
											<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate">{value}</p>
										</div>
									{/each}
								</div>
								<div class="mt-3 bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
									<div class="flex items-center justify-between mb-1.5">
										<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide">Connect (kubeconfig)</p>
										<button onclick={copyKubeconfigCmd} class="text-xs text-indigo-600 dark:text-indigo-400 hover:underline">Copy</button>
									</div>
									<code class="block text-xs font-mono text-slate-700 dark:text-slate-300 break-all">{kubeconfigCmd()}</code>
								</div>
							{/if}
						{:else if detailTab === 'nodegroups'}
							<div class="space-y-3">
								<div class="flex justify-end">
									<button onclick={() => { showCreateNG = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
										<Plus class="w-4 h-4" />Create Node Group
									</button>
								</div>
								{#if loadingNodeGroups}
									<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
								{:else if nodeGroupDetails.length === 0}
									<p class="text-slate-500 dark:text-slate-400 text-sm text-center py-4">No node groups</p>
								{:else}
									{#each nodeGroupDetails as ng}
										<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4">
											<div class="flex items-start justify-between">
												<div>
													<div class="flex items-center gap-2 mb-1">
														<p class="font-medium text-slate-900 dark:text-white">{ng.nodegroupName}</p>
														<span class="px-2 py-0.5 text-xs rounded-full {statusColor(ng.status)}">{ng.status}</span>
													</div>
													<p class="text-xs text-slate-500 dark:text-slate-400">
														{ng.instanceTypes?.join(', ')} · min {ng.scalingConfig?.minSize ?? 0} / desired {ng.scalingConfig?.desiredSize ?? 0} / max {ng.scalingConfig?.maxSize ?? 0}
													</p>
												</div>
												<div class="flex items-center gap-1">
													<button onclick={() => startScaleNodeGroup(ng)} class="text-xs px-2 py-1 rounded border border-indigo-200 dark:border-indigo-800 text-indigo-600 dark:text-indigo-400 hover:bg-indigo-50 dark:hover:bg-indigo-900/20">{scalingNG === ng.nodegroupName ? 'Cancel' : 'Scale'}</button>
													<button onclick={() => deleteNodeGroup(ng.nodegroupName ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
														<Trash2 class="w-4 h-4" />
													</button>
												</div>
											</div>
											{#if scalingNG === ng.nodegroupName}
												<div class="mt-3 pt-3 border-t border-slate-200 dark:border-slate-600 flex flex-wrap items-end gap-2">
													<div>
														<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="ng-min-{ng.nodegroupName}">Min</label>
														<input id="ng-min-{ng.nodegroupName}" type="number" min="0" bind:value={scaleMin} class="w-16 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
													</div>
													<div>
														<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="ng-des-{ng.nodegroupName}">Desired</label>
														<input id="ng-des-{ng.nodegroupName}" type="number" min="0" bind:value={scaleDesired} class="w-16 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
													</div>
													<div>
														<label class="block text-xs text-slate-500 dark:text-slate-400 mb-1" for="ng-max-{ng.nodegroupName}">Max</label>
														<input id="ng-max-{ng.nodegroupName}" type="number" min="0" bind:value={scaleMax} class="w-16 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
													</div>
													<button disabled={scalingUpdate} onclick={() => scaleNodeGroup(ng.nodegroupName ?? '')} class="px-3 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50">{scalingUpdate ? 'Updating...' : 'Apply'}</button>
												</div>
											{/if}
										</div>
									{/each}
								{/if}
							</div>
						{:else if detailTab === 'addons'}
							<div class="space-y-3">
								<div class="flex justify-end">
									<button onclick={() => { showCreateAddon = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
										<Plus class="w-4 h-4" />Install Addon
									</button>
								</div>
								{#if loadingAddons}
									<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
								{:else if addonDetails.length === 0}
									<p class="text-slate-500 dark:text-slate-400 text-sm text-center py-4">No addons installed</p>
								{:else}
									{#each addonDetails as addon}
										<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4 flex items-start justify-between">
											<div>
												<div class="flex items-center gap-2 mb-1">
													<Package class="w-4 h-4 text-cyan-500" />
													<p class="font-medium text-slate-900 dark:text-white">{addon.addonName}</p>
													<span class="px-2 py-0.5 text-xs rounded-full {statusColor(addon.status)}">{addon.status}</span>
												</div>
												<p class="text-xs text-slate-500 dark:text-slate-400">
													Version: {addon.addonVersion ?? 'N/A'}
												</p>
											</div>
											<button onclick={() => deleteAddon(addon.addonName ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									{/each}
								{/if}
							</div>
						{:else if detailTab === 'fargate'}
							<div class="space-y-3">
								<div class="flex justify-end">
									<button onclick={() => { showCreateFargate = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
										<Plus class="w-4 h-4" />Create Profile
									</button>
								</div>
								{#if loadingFargate}
									<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
								{:else if fargateDetails.length === 0}
									<p class="text-slate-500 dark:text-slate-400 text-sm text-center py-4">No Fargate profiles</p>
								{:else}
									{#each fargateDetails as fp}
										<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4 flex items-start justify-between">
											<div>
												<div class="flex items-center gap-2 mb-1">
													<Cloud class="w-4 h-4 text-purple-500" />
													<p class="font-medium text-slate-900 dark:text-white">{fp.fargateProfileName}</p>
													<span class="px-2 py-0.5 text-xs rounded-full {statusColor(fp.status)}">{fp.status}</span>
												</div>
												<p class="text-xs text-slate-500 dark:text-slate-400">
													Selectors: {fp.selectors?.map(s => s.namespace).join(', ') ?? 'None'}
												</p>
											</div>
											<button onclick={() => deleteFargateProfile(fp.fargateProfileName ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									{/each}
								{/if}
							</div>
						{:else if detailTab === 'podidentity'}
							<div class="space-y-3">
								<div class="flex justify-end">
									<button onclick={() => { showCreatePodIdentity = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
										<Plus class="w-4 h-4" />Create Association
									</button>
								</div>
								{#if loadingPodIdentity}
									<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
								{:else if podIdentities.length === 0}
									<p class="text-slate-500 dark:text-slate-400 text-sm text-center py-4">No pod identity associations</p>
								{:else}
									{#each podIdentities as pi}
										<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4 flex items-start justify-between">
											<div>
												<div class="flex items-center gap-2 mb-1">
													<Key class="w-4 h-4 text-amber-500" />
													<p class="font-medium text-slate-900 dark:text-white">{pi.namespace}/{pi.serviceAccount}</p>
												</div>
												<p class="text-xs text-slate-500 dark:text-slate-400">
													ID: {pi.associationId} · ARN: {pi.associationArn?.split('/').pop() ?? ''}
												</p>
											</div>
											<button onclick={() => deletePodIdentity(pi.associationId ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									{/each}
								{/if}
							</div>
						{:else if detailTab === 'access'}
							<div class="space-y-3">
								<div class="flex justify-end">
									<button onclick={() => { showCreateAccessEntry = true; }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-1.5 text-sm">
										<Plus class="w-4 h-4" />Create Entry
									</button>
								</div>
								{#if loadingAccessEntries}
									<div class="text-center py-4"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
								{:else if accessEntryDetails.length === 0}
									<p class="text-slate-500 dark:text-slate-400 text-sm text-center py-4">No access entries</p>
								{:else}
									{#each accessEntryDetails as ae}
										<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4 flex items-start justify-between">
											<div>
												<div class="flex items-center gap-2 mb-1">
													<Shield class="w-4 h-4 text-emerald-500" />
													<p class="font-medium text-slate-900 dark:text-white truncate max-w-sm">{ae.principalArn?.split('/').pop() ?? ae.principalArn}</p>
													<span class="px-2 py-0.5 text-xs rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{ae.type ?? 'STANDARD'}</span>
												</div>
												<p class="text-xs text-slate-500 dark:text-slate-400 truncate max-w-md">
													{ae.principalArn}
												</p>
											</div>
											<button onclick={() => deleteAccessEntry(ae.principalArn ?? '')} class="p-1.5 text-slate-400 hover:text-red-500">
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									{/each}
								{/if}
							</div>
						{/if}
					</div>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Box class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a cluster to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Cluster Modal -->
{#if showCreateCluster}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create EKS Cluster</h2>
			<form onsubmit={(e) => { e.preventDefault(); createCluster(); }} class="space-y-4">
				<div>
					<label for="eks-cluster-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Cluster Name</label>
					<input id="eks-cluster-name" type="text" bind:value={newClusterName} placeholder="e.g. production-cluster" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div>
					<label for="eks-k8s-version" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Kubernetes Version</label>
					<select id="eks-k8s-version" bind:value={newK8sVersion} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						{#each ['1.31', '1.30', '1.29', '1.28'] as v}
							<option value={v}>{v}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="eks-role-arn" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Cluster Role ARN</label>
					<input id="eks-role-arn" type="text" bind:value={newRoleArn} placeholder="arn:aws:iam::123:role/EKSClusterRole" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm" required />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateCluster = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingCluster} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingCluster ? 'Creating...' : 'Create Cluster'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Node Group Modal -->
{#if showCreateNG}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Node Group</h2>
			<form onsubmit={(e) => { e.preventDefault(); createNodeGroup(); }} class="space-y-4">
				<div>
					<label for="eks-ng-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Node Group Name</label>
					<input id="eks-ng-name" type="text" bind:value={newNGName} placeholder="e.g. general-workers" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="eks-instance-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Type</label>
						<select id="eks-instance-type" bind:value={newNGInstanceType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							{#each ['t3.small', 't3.medium', 't3.large', 'm5.large', 'm5.xlarge', 'c5.large'] as it}
								<option value={it}>{it}</option>
							{/each}
						</select>
					</div>
					<div>
						<label for="eks-ami-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">AMI Type</label>
						<select id="eks-ami-type" bind:value={newNGAmiType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							{#each ['AL2_x86_64', 'AL2_x86_64_GPU', 'AL2_ARM_64', 'BOTTLEROCKET_x86_64'] as at}
								<option value={at}>{at}</option>
							{/each}
						</select>
					</div>
				</div>
				<div class="grid grid-cols-3 gap-3">
					<div>
						<label for="eks-ng-min" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Min</label>
						<input id="eks-ng-min" type="number" bind:value={newNGMin} min="0" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label for="eks-ng-desired" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Desired</label>
						<input id="eks-ng-desired" type="number" bind:value={newNGDesired} min="0" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
					<div>
						<label for="eks-ng-max" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Max</label>
						<input id="eks-ng-max" type="number" bind:value={newNGMax} min="1" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
					</div>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateNG = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingNG} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingNG ? 'Creating...' : 'Create Node Group'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Addon Modal -->
{#if showCreateAddon}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Install Addon</h2>
			<form onsubmit={(e) => { e.preventDefault(); createAddon(); }} class="space-y-4">
				<div>
					<label for="eks-addon-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Addon Name</label>
					<select id="eks-addon-name" bind:value={newAddonName} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						{#each ['vpc-cni', 'coredns', 'kube-proxy', 'aws-ebs-csi-driver', 'aws-efs-csi-driver', 'snapshot-controller', 'adot', 'aws-guardduty-agent', 'amazon-cloudwatch-observability'] as addon}
							<option value={addon}>{addon}</option>
						{/each}
					</select>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateAddon = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingAddon} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingAddon ? 'Installing...' : 'Install Addon'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Fargate Profile Modal -->
{#if showCreateFargate}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Fargate Profile</h2>
			<form onsubmit={(e) => { e.preventDefault(); createFargateProfile(); }} class="space-y-4">
				<div>
					<label for="eks-fargate-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Profile Name</label>
					<input id="eks-fargate-name" type="text" bind:value={newFargateName} placeholder="e.g. my-fargate-profile" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div>
					<label for="eks-fargate-ns" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Namespace Selector</label>
					<input id="eks-fargate-ns" type="text" bind:value={newFargateNamespace} placeholder="default" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateFargate = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingFargate} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingFargate ? 'Creating...' : 'Create Profile'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Pod Identity Modal -->
{#if showCreatePodIdentity}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Pod Identity Association</h2>
			<form onsubmit={(e) => { e.preventDefault(); createPodIdentity(); }} class="space-y-4">
				<div>
					<label for="eks-pid-ns" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Namespace</label>
					<input id="eks-pid-ns" type="text" bind:value={newPodIdNamespace} placeholder="default" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div>
					<label for="eks-pid-sa" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Service Account</label>
					<input id="eks-pid-sa" type="text" bind:value={newPodIdServiceAccount} placeholder="my-service-account" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div>
					<label for="eks-pid-role" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Role ARN</label>
					<input id="eks-pid-role" type="text" bind:value={newPodIdRoleArn} placeholder="arn:aws:iam::123:role/PodRole" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm" required />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreatePodIdentity = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingPodIdentity} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingPodIdentity ? 'Creating...' : 'Create Association'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Access Entry Modal -->
{#if showCreateAccessEntry}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Access Entry</h2>
			<form onsubmit={(e) => { e.preventDefault(); createAccessEntry(); }} class="space-y-4">
				<div>
					<label for="eks-ae-principal" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Principal ARN</label>
					<input id="eks-ae-principal" type="text" bind:value={newAccessPrincipalArn} placeholder="arn:aws:iam::123:role/MyRole" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm" required />
				</div>
				<div>
					<label for="eks-ae-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Type</label>
					<select id="eks-ae-type" bind:value={newAccessType} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						{#each ['STANDARD', 'EC2_LINUX', 'EC2_WINDOWS', 'FARGATE_LINUX'] as t}
							<option value={t}>{t}</option>
						{/each}
					</select>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateAccessEntry = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button type="submit" disabled={creatingAccessEntry} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creatingAccessEntry ? 'Creating...' : 'Create Entry'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
