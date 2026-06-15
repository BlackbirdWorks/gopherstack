<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getOpenSearchClient } from '$lib/aws-client';
	import {
		ListDomainNamesCommand,
		DescribeDomainCommand,
		CreateDomainCommand,
		DeleteDomainCommand,
		UpdateDomainConfigCommand,
		ListTagsCommand,
		AddTagsCommand,
		DescribePackagesCommand,
		ListPackagesForDomainCommand,
		CreatePackageCommand,
		DeletePackageCommand,
		AssociatePackageCommand,
		DissociatePackageCommand,
		type DomainStatus
	} from '@aws-sdk/client-opensearch';
	import { toast } from 'svelte-sonner';
	import { Search, RefreshCw, Plus, Trash2, ChevronRight, Database, Tag, Package } from 'lucide-svelte';

	const opensearch = getOpenSearchClient();

	let loading = $state(false);
	let domainNames = $state<{ DomainName: string }[]>([]);
	let selectedDomain = $state<DomainStatus | null>(null);
	let loadingDetail = $state(false);
	let activeTab = $state<'overview' | 'config' | 'tags' | 'packages'>('overview');
	let searchQuery = $state('');

	// Tags
	let tags = $state<{ Key: string; Value: string }[]>([]);
	let loadingTags = $state(false);

	// Packages (all available)
	let packages = $state<{ PackageID: string; PackageName: string; PackageType: string; PackageDescription: string; PackageStatus: string }[]>([]);
	let loadingPackages = $state(false);

	// Domain-associated packages
	let domainPackages = $state<{ PackageID: string; PackageName: string; PackageType: string; DomainPackageStatus: string }[]>([]);
	let loadingDomainPackages = $state(false);

	// Create Package
	let showCreatePackage = $state(false);
	let creatingPackage = $state(false);
	let newPackageName = $state('');
	let newPackageType = $state('TXT-DICTIONARY');
	let newPackageDescription = $state('');

	// Create Domain
	let showCreateDomain = $state(false);
	let creatingDomain = $state(false);
	let newDomainName = $state('');
	let newEngineVersion = $state('OpenSearch_2.11');
	let newInstanceType = $state('t3.small.search');
	let newInstanceCount = $state(1);
	let newStorageSize = $state(10);
	let newMultiAZ = $state(false);

	// Update Config
	let showUpdateConfig = $state(false);
	let updatingConfig = $state(false);
	let updateInstanceType = $state('');
	let updateInstanceCount = $state(1);

	// Add Tag
	let showAddTag = $state(false);
	let newTagKey = $state('');
	let newTagValue = $state('');

	const filteredDomains = $derived(
		domainNames.filter((d) => !searchQuery || d.DomainName.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const statusColor = (status: boolean | undefined) => {
		if (status === true) return 'green';
		if (status === false) return 'red';
		return 'yellow';
	};

	async function loadDomains() {
		loading = true;
		try {
			const resp = await opensearch.send(new ListDomainNamesCommand({}));
			domainNames = (resp.DomainNames ?? []).map(d => ({ DomainName: d.DomainName ?? '' }));
		} catch (e) {
			toast.error('Failed to load domains: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectDomain(name: string) {
		loadingDetail = true;
		activeTab = 'overview';
		domainPackages = [];
		try {
			const resp = await opensearch.send(new DescribeDomainCommand({ DomainName: name }));
			selectedDomain = resp.DomainStatus ?? null;
			updateInstanceType = selectedDomain?.ClusterConfig?.InstanceType ?? '';
			updateInstanceCount = selectedDomain?.ClusterConfig?.InstanceCount ?? 1;
		} catch (e) {
			toast.error('Failed to load domain details: ' + String(e));
		} finally {
			loadingDetail = false;
		}
		await loadDomainPackages(name);
	}

	async function loadDomainPackages(domainName: string) {
		loadingDomainPackages = true;
		try {
			const resp = await opensearch.send(new ListPackagesForDomainCommand({ DomainName: domainName }));
			domainPackages = (resp.DomainPackageDetailsList ?? []).map((p) => ({
				PackageID: p.PackageID ?? '',
				PackageName: p.PackageName ?? '',
				PackageType: p.PackageType ?? '',
				DomainPackageStatus: p.DomainPackageStatus ?? ''
			}));
		} catch (e) {
			toast.error('Failed to load domain packages: ' + String(e));
		} finally {
			loadingDomainPackages = false;
		}
	}

	async function handleTabChange(tab: 'overview' | 'config' | 'tags' | 'packages') {
		activeTab = tab;
		if (tab === 'tags' && selectedDomain && tags.length === 0) await loadTags();
		if (tab === 'packages' && packages.length === 0) await loadPackages();
	}

	async function loadPackages() {
		loadingPackages = true;
		try {
			const resp = await opensearch.send(new DescribePackagesCommand({}));
			packages = (resp.PackageDetailsList ?? []).map((p) => ({
				PackageID: p.PackageID ?? '',
				PackageName: p.PackageName ?? '',
				PackageType: p.PackageType ?? '',
				PackageDescription: p.PackageDescription ?? '',
				PackageStatus: p.PackageStatus ?? ''
			}));
		} catch (e) {
			toast.error('Failed to load packages: ' + String(e));
		} finally {
			loadingPackages = false;
		}
	}

	async function createPackage() {
		if (!newPackageName.trim()) return;
		creatingPackage = true;
		try {
			await opensearch.send(new CreatePackageCommand({
				PackageName: newPackageName.trim(),
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				PackageType: newPackageType as any,
				PackageDescription: newPackageDescription.trim(),
				PackageSource: { S3BucketName: 'gopherstack-packages', S3Key: `${newPackageName.trim()}.txt` }
			}));
			toast.success(`Package "${newPackageName}" created`);
			showCreatePackage = false;
			newPackageName = '';
			newPackageDescription = '';
			packages = [];
			await loadPackages();
		} catch (e) {
			toast.error('Failed to create package: ' + String(e));
		} finally {
			creatingPackage = false;
		}
	}

	async function deletePackage(packageID: string, packageName: string) {
		if (!await confirmDestructive({ title: 'Delete Package', message: `Delete package "${packageName}"?` })) return;
		try {
			await opensearch.send(new DeletePackageCommand({ PackageID: packageID }));
			toast.success(`Package "${packageName}" deleted`);
			packages = [];
			await loadPackages();
		} catch (e) {
			toast.error('Failed to delete package: ' + String(e));
		}
	}

	async function associatePackage(packageID: string) {
		if (!selectedDomain?.DomainName) return;
		try {
			await opensearch.send(new AssociatePackageCommand({ PackageID: packageID, DomainName: selectedDomain.DomainName }));
			toast.success('Package associated with domain');
		} catch (e) {
			toast.error('Failed to associate package: ' + String(e));
		}
	}

	async function dissociatePackage(packageID: string) {
		if (!selectedDomain?.DomainName) return;
		try {
			await opensearch.send(new DissociatePackageCommand({ PackageID: packageID, DomainName: selectedDomain.DomainName }));
			toast.success('Package dissociated from domain');
		} catch (e) {
			toast.error('Failed to dissociate package: ' + String(e));
		}
	}

	async function loadTags() {
		if (!selectedDomain?.ARN) return;
		loadingTags = true;
		try {
			const resp = await opensearch.send(new ListTagsCommand({ ARN: selectedDomain.ARN }));
			tags = (resp.TagList ?? []).map((t) => ({ Key: t.Key ?? '', Value: t.Value ?? '' }));
		} catch (e) {
			toast.error('Failed to load tags: ' + String(e));
		} finally {
			loadingTags = false;
		}
	}

	async function addTag() {
		if (!selectedDomain?.ARN || !newTagKey.trim()) return;
		try {
			await opensearch.send(new AddTagsCommand({
				ARN: selectedDomain.ARN,
				TagList: [{ Key: newTagKey.trim(), Value: newTagValue.trim() }]
			}));
			toast.success('Tag added');
			showAddTag = false;
			newTagKey = '';
			newTagValue = '';
			tags = [];
			await loadTags();
		} catch (e) {
			toast.error('Failed to add tag: ' + String(e));
		}
	}

	async function createDomain() {
		if (!newDomainName.trim()) return;
		creatingDomain = true;
		try {
			await opensearch.send(new CreateDomainCommand({
				DomainName: newDomainName.trim(),
				EngineVersion: newEngineVersion,
				ClusterConfig: {
					InstanceType: newInstanceType as 't3.small.search',
					InstanceCount: newInstanceCount,
					MultiAZWithStandbyEnabled: newMultiAZ
				},
				EBSOptions: {
					EBSEnabled: true,
					VolumeSize: newStorageSize,
					VolumeType: 'gp3'
				}
			}));
			toast.success(`Domain "${newDomainName}" creation initiated`);
			showCreateDomain = false;
			resetCreateForm();
			await loadDomains();
		} catch (e) {
			toast.error('Failed to create domain: ' + String(e));
		} finally {
			creatingDomain = false;
		}
	}

	function resetCreateForm() {
		newDomainName = '';
		newEngineVersion = 'OpenSearch_2.11';
		newInstanceType = 't3.small.search';
		newInstanceCount = 1;
		newStorageSize = 10;
		newMultiAZ = false;
	}

	async function updateConfig() {
		if (!selectedDomain) return;
		updatingConfig = true;
		try {
			await opensearch.send(new UpdateDomainConfigCommand({
				DomainName: selectedDomain.DomainName ?? '',
				ClusterConfig: {
					InstanceType: updateInstanceType as 't3.small.search',
					InstanceCount: updateInstanceCount
				}
			}));
			toast.success('Domain config update initiated');
			showUpdateConfig = false;
			await selectDomain(selectedDomain.DomainName ?? '');
		} catch (e) {
			toast.error('Failed to update config: ' + String(e));
		} finally {
			updatingConfig = false;
		}
	}

	async function deleteDomain(name: string) {
		if (!await confirmDestructive({ title: 'Delete OpenSearch Domain', message: `Delete domain "${name}"? All indices and data will be permanently lost.` })) return;
		try {
			await opensearch.send(new DeleteDomainCommand({ DomainName: name }));
			toast.success(`Domain "${name}" deletion initiated`);
			if (selectedDomain?.DomainName === name) selectedDomain = null;
			await loadDomains();
		} catch (e) {
			toast.error('Failed to delete domain: ' + String(e));
		}
	}

	const instanceTypes = [
		't3.small.search', 't3.medium.search',
		'm6g.large.search', 'm6g.xlarge.search', 'm6g.2xlarge.search',
		'r6g.large.search', 'r6g.xlarge.search', 'r6g.2xlarge.search',
		'c6g.large.search', 'c6g.xlarge.search'
	];

	onMount(loadDomains);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="w-7 h-7 text-cyan-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon OpenSearch Service</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Managed search and analytics</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={loadDomains} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateDomain = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Domain
			</button>
		</div>
	</div>

	{#if selectedDomain}
		<!-- Domain Detail -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedDomain = null; tags = []; }} class="text-cyan-600 hover:underline">Domains</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedDomain.DomainName}</span>
			{#if loadingDetail}
				<span class="text-gray-400 text-xs">(loading...)</span>
			{:else}
				<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedDomain.Processing === false)}-100 text-${statusColor(selectedDomain.Processing === false)}-700`}>
					{selectedDomain.Processing ? 'Processing' : 'Active'}
				</span>
			{/if}
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['overview', 'Overview'], ['config', 'Configuration'], ['tags', 'Tags'], ['packages', 'Packages']] as [tab, label]}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'config' | 'tags' | 'packages')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-cyan-500 text-cyan-600 dark:text-cyan-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
				>
					{label}
				</button>
			{/each}
			<button onclick={() => { showUpdateConfig = true; }} class="ml-auto px-4 py-2 text-sm text-cyan-600 hover:underline">Update Config</button>
			<button onclick={() => deleteDomain(selectedDomain?.DomainName ?? '')} class="px-4 py-2 text-sm text-red-500 hover:underline">Delete</button>
		</div>

		{#if activeTab === 'overview'}
			{#if loadingDetail}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-cyan-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
					{#each [
						{ label: 'Domain ARN', value: selectedDomain.ARN },
						{ label: 'Engine Version', value: selectedDomain.EngineVersion },
						{ label: 'Instance Type', value: selectedDomain.ClusterConfig?.InstanceType },
						{ label: 'Instance Count', value: String(selectedDomain.ClusterConfig?.InstanceCount ?? '-') },
						{ label: 'Dedicated Master', value: selectedDomain.ClusterConfig?.DedicatedMasterEnabled ? 'Enabled' : 'Disabled' },
						{ label: 'Zone Awareness', value: selectedDomain.ClusterConfig?.ZoneAwarenessEnabled ? 'Enabled' : 'Disabled' },
						{ label: 'EBS Volume Size', value: `${selectedDomain.EBSOptions?.VolumeSize ?? '-'} GB` },
						{ label: 'Endpoint', value: selectedDomain.Endpoint ?? selectedDomain.Endpoints?.vpc ?? 'N/A' }
					] as row}
						<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
							<div class="text-xs text-gray-500 font-medium">{row.label}</div>
							<div class="text-sm text-gray-900 dark:text-white mt-1 truncate font-mono">{row.value}</div>
						</div>
					{/each}
				</div>
				{#if selectedDomain.Endpoint}
					<div class="bg-cyan-50 dark:bg-cyan-900/20 rounded-xl border border-cyan-200 dark:border-cyan-800 p-4">
						<div class="text-sm font-semibold text-cyan-800 dark:text-cyan-300 mb-1">Kibana/OpenSearch Dashboards</div>
						<a href="{selectedDomain.Endpoint}/_dashboards" target="_blank" rel="noopener noreferrer" class="text-cyan-600 dark:text-cyan-400 text-sm hover:underline">{selectedDomain.Endpoint}/_dashboards</a>
					</div>
				{/if}

				<!-- Package Associations -->
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Package Associations</h3>
					{#if loadingDomainPackages}
						<div class="flex items-center gap-2 text-sm text-gray-400 py-2"><div class="animate-spin w-4 h-4 border-2 border-cyan-600 border-t-transparent rounded-full"></div> Loading...</div>
					{:else if domainPackages.length === 0}
						<p class="text-sm text-gray-400 italic">No packages associated with this domain</p>
					{:else}
						<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 overflow-hidden">
							<table class="w-full text-sm">
								<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
									<tr>
										<th class="px-4 py-2 text-left">Package</th>
										<th class="px-4 py-2 text-left">Type</th>
										<th class="px-4 py-2 text-left">Status</th>
									</tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each domainPackages as pkg}
										<tr>
											<td class="px-4 py-2 font-medium">{pkg.PackageName}</td>
											<td class="px-4 py-2 text-gray-500">{pkg.PackageType}</td>
											<td class="px-4 py-2">
												<span class={`px-2 py-0.5 rounded text-xs font-medium ${pkg.DomainPackageStatus === 'ACTIVE' ? 'bg-green-100 text-green-700' : 'bg-yellow-100 text-yellow-700'}`}>
													{pkg.DomainPackageStatus}
												</span>
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'config'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#if selectedDomain.SnapshotOptions}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4 col-span-2">
						<div class="text-xs text-gray-500 font-medium mb-2">Snapshot Options</div>
						<div class="text-sm">Automated Snapshot Start Hour: {selectedDomain.SnapshotOptions.AutomatedSnapshotStartHour ?? 0}:00</div>
					</div>
				{/if}
				{#each [
					{ label: 'Encryption at Rest', value: selectedDomain.EncryptionAtRestOptions?.Enabled ? 'Enabled' : 'Disabled' },
					{ label: 'Node-to-Node Encryption', value: selectedDomain.NodeToNodeEncryptionOptions?.Enabled ? 'Enabled' : 'Disabled' },
					{ label: 'HTTPS Required', value: selectedDomain.DomainEndpointOptions?.EnforceHTTPS ? 'Yes' : 'No' },
					{ label: 'TLS Policy', value: selectedDomain.DomainEndpointOptions?.TLSSecurityPolicy ?? 'N/A' },
					{ label: 'Cognito Auth', value: selectedDomain.CognitoOptions?.Enabled ? 'Enabled' : 'Disabled' }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
						<div class="text-xs text-gray-500 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1">{row.value}</div>
					</div>
				{/each}
			</div>
		{/if}

		{#if activeTab === 'tags'}
			<div class="flex justify-end mb-4">
				<button onclick={() => (showAddTag = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm font-medium">
					<Tag class="w-4 h-4" /> Add Tag
				</button>
			</div>
			{#if loadingTags}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-cyan-600 border-t-transparent rounded-full"></div></div>
			{:else if tags.length === 0}
				<div class="text-center py-12 text-gray-500"><Tag class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No tags</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Key</th>
								<th class="px-4 py-3 text-left">Value</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each tags as tag}
								<tr><td class="px-4 py-3 font-medium">{tag.Key}</td><td class="px-4 py-3 text-gray-600 dark:text-gray-400">{tag.Value}</td></tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		{#if activeTab === 'packages'}
			<div class="flex justify-end mb-4">
				<button onclick={() => (showCreatePackage = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm font-medium">
					<Package class="w-4 h-4" /> Create Package
				</button>
			</div>
			{#if loadingPackages}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-cyan-600 border-t-transparent rounded-full"></div></div>
			{:else if packages.length === 0}
				<div class="text-center py-12 text-gray-500"><Package class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No packages</p></div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
							<tr>
								<th class="px-4 py-3 text-left">Name</th>
								<th class="px-4 py-3 text-left">Type</th>
								<th class="px-4 py-3 text-left">Status</th>
								<th class="px-4 py-3 text-left">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each packages as pkg}
								<tr>
									<td class="px-4 py-3 font-medium">{pkg.PackageName}</td>
									<td class="px-4 py-3 text-gray-500">{pkg.PackageType}</td>
									<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700">{pkg.PackageStatus}</span></td>
									<td class="px-4 py-3">
										<div class="flex gap-2">
											<button onclick={() => associatePackage(pkg.PackageID)} class="text-xs text-cyan-600 hover:underline">Associate</button>
											<button onclick={() => dissociatePackage(pkg.PackageID)} class="text-xs text-gray-500 hover:underline">Dissociate</button>
											<button onclick={() => deletePackage(pkg.PackageID, pkg.PackageName)} class="text-xs text-red-500 hover:underline">Delete</button>
										</div>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

	{:else}
		<!-- Domain List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search domains..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-cyan-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredDomains.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No domains found</p>
				<p class="text-sm mt-1">Create an OpenSearch domain to get started</p>
			</div>
		{:else}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each filteredDomains as domain}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectDomain(domain.DomainName)}
						onkeypress={(e) => e.key === 'Enter' && selectDomain(domain.DomainName)}
						class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 text-left hover:border-cyan-400 hover:shadow-md transition-all group cursor-pointer"
					>
						<div class="flex items-start justify-between">
							<div>
								<div class="font-semibold text-cyan-600 dark:text-cyan-400 group-hover:underline">{domain.DomainName}</div>
							</div>
							<button onclick={(e) => { e.stopPropagation(); deleteDomain(domain.DomainName); }} class="text-red-400 hover:text-red-600 opacity-0 group-hover:opacity-100 transition-opacity">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}
</div>

<!-- Create Domain Modal -->
{#if showCreateDomain}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create OpenSearch Domain</h2>
			<div>
				<label for="os-domain-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Domain Name</label>
				<input id="os-domain-name" bind:value={newDomainName} type="text" placeholder="my-opensearch-domain" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="engine-version" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Engine Version</label>
				<select id="engine-version" bind:value={newEngineVersion} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="OpenSearch_2.11">OpenSearch 2.11</option>
					<option value="OpenSearch_2.9">OpenSearch 2.9</option>
					<option value="OpenSearch_2.7">OpenSearch 2.7</option>
					<option value="Elasticsearch_8.11">Elasticsearch 8.11</option>
				</select>
			</div>
			<div>
				<label for="instance-type-os" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Type</label>
				<select id="instance-type-os" bind:value={newInstanceType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each instanceTypes as t}<option value={t}>{t}</option>{/each}
				</select>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="instance-count-os" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Count</label>
					<input id="instance-count-os" bind:value={newInstanceCount} type="number" min="1" max="80" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="storage-size-os" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Storage (GB)</label>
					<input id="storage-size-os" bind:value={newStorageSize} type="number" min="10" max="3072" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => { showCreateDomain = false; resetCreateForm(); }} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createDomain} disabled={creatingDomain || !newDomainName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
					{creatingDomain ? 'Creating...' : 'Create Domain'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Update Config Modal -->
{#if showUpdateConfig && selectedDomain}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Update Configuration</h2>
			<div>
				<label for="update-instance-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Type</label>
				<select id="update-instance-type" bind:value={updateInstanceType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each instanceTypes as t}<option value={t}>{t}</option>{/each}
				</select>
			</div>
			<div>
				<label for="update-instance-count" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Count</label>
				<input id="update-instance-count" bind:value={updateInstanceCount} type="number" min="1" max="80" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showUpdateConfig = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={updateConfig} disabled={updatingConfig} class="flex-1 px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
					{updatingConfig ? 'Updating...' : 'Update'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Package Modal -->
{#if showCreatePackage}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Package</h2>
			<div>
				<label for="pkg-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Package Name</label>
				<input id="pkg-name" bind:value={newPackageName} type="text" placeholder="my-dictionary" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="pkg-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Package Type</label>
				<select id="pkg-type" bind:value={newPackageType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="TXT-DICTIONARY">TXT-DICTIONARY</option>
					<option value="ZIP-PLUGIN">ZIP-PLUGIN</option>
				</select>
			</div>
			<div>
				<label for="pkg-desc" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description</label>
				<input id="pkg-desc" bind:value={newPackageDescription} type="text" placeholder="Custom dictionary for analysis" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => { showCreatePackage = false; }} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createPackage} disabled={creatingPackage || !newPackageName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
					{creatingPackage ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Add Tag Modal -->
{#if showAddTag}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-sm p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Add Tag</h2>
			<div>
				<label for="tag-key" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Key</label>
				<input id="tag-key" bind:value={newTagKey} type="text" placeholder="Environment" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="tag-value" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Value</label>
				<input id="tag-value" bind:value={newTagValue} type="text" placeholder="production" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showAddTag = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={addTag} disabled={!newTagKey.trim()} class="flex-1 px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">Add</button>
			</div>
		</div>
	</div>
{/if}
