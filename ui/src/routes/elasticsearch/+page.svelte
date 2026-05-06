<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getElasticsearchServiceClient } from '$lib/aws-client';
	import {
		ListDomainNamesCommand,
		DescribeElasticsearchDomainCommand,
		CreateElasticsearchDomainCommand,
		DeleteElasticsearchDomainCommand,
		UpdateElasticsearchDomainConfigCommand,
		ListTagsCommand,
		AddTagsCommand,
		type ElasticsearchDomainStatus
	} from '@aws-sdk/client-elasticsearch-service';
	import { toast } from 'svelte-sonner';
	import { Search, RefreshCw, Plus, Trash2, ChevronRight, Database, Tag } from 'lucide-svelte';

	const elasticsearch = getElasticsearchServiceClient();

	let loading = $state(false);
	let domainNames = $state<{ DomainName: string }[]>([]);
	let selectedDomain = $state<ElasticsearchDomainStatus | null>(null);
	let loadingDetail = $state(false);
	let activeTab = $state<'overview' | 'config' | 'tags'>('overview');
	let searchQuery = $state('');

	// Tags
	let tags = $state<{ Key: string; Value: string }[]>([]);
	let loadingTags = $state(false);

	// Create Domain
	let showCreateDomain = $state(false);
	let creatingDomain = $state(false);
	let newDomainName = $state('');
	let newEsVersion = $state('7.10');
	let newInstanceType = $state('t3.small.elasticsearch');
	let newInstanceCount = $state(1);
	let newStorageSize = $state(10);

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

	async function loadDomains() {
		loading = true;
		try {
			const resp = await elasticsearch.send(new ListDomainNamesCommand({}));
			domainNames = (resp.DomainNames ?? []).map((d) => ({ DomainName: d.DomainName ?? '' }));
		} catch (e) {
			toast.error('Failed to load domains: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectDomain(name: string) {
		loadingDetail = true;
		activeTab = 'overview';
		tags = [];
		try {
			const resp = await elasticsearch.send(new DescribeElasticsearchDomainCommand({ DomainName: name }));
			selectedDomain = resp.DomainStatus ?? null;
			updateInstanceType = selectedDomain?.ElasticsearchClusterConfig?.InstanceType ?? '';
			updateInstanceCount = selectedDomain?.ElasticsearchClusterConfig?.InstanceCount ?? 1;
		} catch (e) {
			toast.error('Failed to load domain details: ' + String(e));
		} finally {
			loadingDetail = false;
		}
	}

	async function handleTabChange(tab: 'overview' | 'config' | 'tags') {
		activeTab = tab;
		if (tab === 'tags' && selectedDomain && tags.length === 0) await loadTags();
	}

	async function loadTags() {
		if (!selectedDomain?.ARN) return;
		loadingTags = true;
		try {
			const resp = await elasticsearch.send(new ListTagsCommand({ ARN: selectedDomain.ARN }));
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
			await elasticsearch.send(new AddTagsCommand({
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
			await elasticsearch.send(new CreateElasticsearchDomainCommand({
				DomainName: newDomainName.trim(),
				ElasticsearchVersion: newEsVersion,
				ElasticsearchClusterConfig: {
					InstanceType: newInstanceType as 't3.small.elasticsearch',
					InstanceCount: newInstanceCount
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
		newEsVersion = '7.10';
		newInstanceType = 't3.small.elasticsearch';
		newInstanceCount = 1;
		newStorageSize = 10;
	}

	async function updateConfig() {
		if (!selectedDomain) return;
		updatingConfig = true;
		try {
			await elasticsearch.send(new UpdateElasticsearchDomainConfigCommand({
				DomainName: selectedDomain.DomainName ?? '',
				ElasticsearchClusterConfig: {
					InstanceType: updateInstanceType as 't3.small.elasticsearch',
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
		if (!await confirmDestructive({ title: 'Delete Elasticsearch Domain', message: `Delete domain "${name}"? All indices and data will be permanently lost.` })) return;
		try {
			await elasticsearch.send(new DeleteElasticsearchDomainCommand({ DomainName: name }));
			toast.success(`Domain "${name}" deletion initiated`);
			if (selectedDomain?.DomainName === name) selectedDomain = null;
			await loadDomains();
		} catch (e) {
			toast.error('Failed to delete domain: ' + String(e));
		}
	}

	const instanceTypes = [
		't3.small.elasticsearch', 't3.medium.elasticsearch',
		'm6g.large.elasticsearch', 'm6g.xlarge.elasticsearch', 'm6g.2xlarge.elasticsearch',
		'r6g.large.elasticsearch', 'r6g.xlarge.elasticsearch', 'r6g.2xlarge.elasticsearch',
		'c6g.large.elasticsearch', 'c6g.xlarge.elasticsearch'
	];

	onMount(loadDomains);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Database class="w-7 h-7 text-yellow-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Elasticsearch Service</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Managed search and analytics</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={loadDomains} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateDomain = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-yellow-600 text-white hover:bg-yellow-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Domain
			</button>
		</div>
	</div>

	{#if selectedDomain}
		<!-- Domain Detail -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedDomain = null; tags = []; }} class="text-yellow-600 hover:underline">Domains</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedDomain.DomainName}</span>
			{#if loadingDetail}
				<span class="text-gray-400 text-xs">(loading...)</span>
			{:else}
				<span class="ml-2 px-2 py-0.5 rounded text-xs font-medium bg-green-100 text-green-700">
					{selectedDomain.Processing ? 'Processing' : 'Active'}
				</span>
			{/if}
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['overview', 'Overview'], ['config', 'Configuration'], ['tags', 'Tags']] as [tab, label]}
				<button
					onclick={() => handleTabChange(tab as 'overview' | 'config' | 'tags')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-yellow-500 text-yellow-600 dark:text-yellow-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
				>
					{label}
				</button>
			{/each}
			<button onclick={() => { showUpdateConfig = true; }} class="ml-auto px-4 py-2 text-sm text-yellow-600 hover:underline">Update Config</button>
			<button onclick={() => deleteDomain(selectedDomain?.DomainName ?? '')} class="px-4 py-2 text-sm text-red-500 hover:underline">Delete</button>
		</div>

		{#if activeTab === 'overview'}
			{#if loadingDetail}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-yellow-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
					{#each [
						{ label: 'Domain ARN', value: selectedDomain.ARN ?? 'N/A' },
						{ label: 'Elasticsearch Version', value: selectedDomain.ElasticsearchVersion ?? 'N/A' },
						{ label: 'Instance Type', value: selectedDomain.ElasticsearchClusterConfig?.InstanceType ?? 'N/A' },
						{ label: 'Instance Count', value: String(selectedDomain.ElasticsearchClusterConfig?.InstanceCount ?? '-') },
						{ label: 'Dedicated Master', value: selectedDomain.ElasticsearchClusterConfig?.DedicatedMasterEnabled ? 'Enabled' : 'Disabled' },
						{ label: 'Zone Awareness', value: selectedDomain.ElasticsearchClusterConfig?.ZoneAwarenessEnabled ? 'Enabled' : 'Disabled' },
						{ label: 'EBS Volume Size', value: `${selectedDomain.EBSOptions?.VolumeSize ?? '-'} GB` },
						{ label: 'Endpoint', value: selectedDomain.Endpoint ?? 'N/A' }
					] as row}
						<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
							<div class="text-xs text-gray-500 font-medium">{row.label}</div>
							<div class="text-sm text-gray-900 dark:text-white mt-1 truncate font-mono">{row.value}</div>
						</div>
					{/each}
				</div>
				{#if selectedDomain.Endpoint}
					<div class="bg-yellow-50 dark:bg-yellow-900/20 rounded-xl border border-yellow-200 dark:border-yellow-800 p-4">
						<div class="text-sm font-semibold text-yellow-800 dark:text-yellow-300 mb-1">Kibana</div>
						<a href="{selectedDomain.Endpoint}/_plugin/kibana" target="_blank" rel="noopener noreferrer" class="text-yellow-600 dark:text-yellow-400 text-sm hover:underline">{selectedDomain.Endpoint}/_plugin/kibana</a>
					</div>
				{/if}
			{/if}
		{/if}

		{#if activeTab === 'config'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'Encryption at Rest', value: selectedDomain.EncryptionAtRestOptions?.Enabled ? 'Enabled' : 'Disabled' },
					{ label: 'Node-to-Node Encryption', value: selectedDomain.NodeToNodeEncryptionOptions?.Enabled ? 'Enabled' : 'Disabled' },
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
				<button onclick={() => (showAddTag = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-yellow-600 text-white hover:bg-yellow-700 text-sm font-medium">
					<Tag class="w-4 h-4" /> Add Tag
				</button>
			</div>
			{#if loadingTags}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-yellow-600 border-t-transparent rounded-full"></div></div>
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

	{:else}
		<!-- Domain List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search domains..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-yellow-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredDomains.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No domains found</p>
				<p class="text-sm mt-1">Create an Elasticsearch domain to get started</p>
			</div>
		{:else}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each filteredDomains as domain}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectDomain(domain.DomainName)}
						onkeypress={(e) => e.key === 'Enter' && selectDomain(domain.DomainName)}
						class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 text-left hover:border-yellow-400 hover:shadow-md transition-all group cursor-pointer"
					>
						<div class="flex items-start justify-between">
							<div>
								<div class="font-semibold text-yellow-600 dark:text-yellow-400 group-hover:underline">{domain.DomainName}</div>
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
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Elasticsearch Domain</h2>
			<div>
				<label for="es-domain-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Domain Name</label>
				<input id="es-domain-name" bind:value={newDomainName} type="text" placeholder="my-elasticsearch-domain" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="es-version" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Elasticsearch Version</label>
				<select id="es-version" bind:value={newEsVersion} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="7.10">7.10</option>
					<option value="7.9">7.9</option>
					<option value="7.8">7.8</option>
					<option value="6.8">6.8</option>
				</select>
			</div>
			<div>
				<label for="es-instance-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Type</label>
				<select id="es-instance-type" bind:value={newInstanceType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each instanceTypes as t}<option value={t}>{t}</option>{/each}
				</select>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="es-instance-count" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Count</label>
					<input id="es-instance-count" bind:value={newInstanceCount} type="number" min="1" max="80" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="es-storage-size" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Storage (GB)</label>
					<input id="es-storage-size" bind:value={newStorageSize} type="number" min="10" max="3072" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => { showCreateDomain = false; resetCreateForm(); }} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createDomain} disabled={creatingDomain || !newDomainName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-yellow-600 text-white text-sm font-medium hover:bg-yellow-700 disabled:opacity-50">
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
				<label for="update-es-instance-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Type</label>
				<select id="update-es-instance-type" bind:value={updateInstanceType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each instanceTypes as t}<option value={t}>{t}</option>{/each}
				</select>
			</div>
			<div>
				<label for="update-es-instance-count" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Instance Count</label>
				<input id="update-es-instance-count" bind:value={updateInstanceCount} type="number" min="1" max="80" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showUpdateConfig = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={updateConfig} disabled={updatingConfig} class="flex-1 px-4 py-2 rounded-lg bg-yellow-600 text-white text-sm font-medium hover:bg-yellow-700 disabled:opacity-50">
					{updatingConfig ? 'Updating...' : 'Update'}
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
				<label for="es-tag-key" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Key</label>
				<input id="es-tag-key" bind:value={newTagKey} type="text" placeholder="Environment" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="es-tag-value" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Value</label>
				<input id="es-tag-value" bind:value={newTagValue} type="text" placeholder="production" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showAddTag = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50">Cancel</button>
				<button onclick={addTag} disabled={!newTagKey.trim()} class="flex-1 px-4 py-2 rounded-lg bg-yellow-600 text-white text-sm font-medium hover:bg-yellow-700 disabled:opacity-50">Add</button>
			</div>
		</div>
	</div>
{/if}
