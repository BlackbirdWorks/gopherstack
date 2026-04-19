<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getAPIGatewayClient } from '$lib/aws-client';
	import {
		GetRestApisCommand,
		GetResourcesCommand,
		CreateRestApiCommand,
		DeleteRestApiCommand,
		GetApiKeysCommand,
		GetDeploymentsCommand,
		GetStagesCommand,
		type RestApi,
		type Resource,
		type ApiKey,
		type Deployment,
		type Stage
	} from '@aws-sdk/client-api-gateway';
	import { toast } from 'svelte-sonner';
	import {
		Globe,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		KeyRound,
		ChevronRight,
		Layers
	} from 'lucide-svelte';

	const apigw = getAPIGatewayClient();

	let loading = $state(false);
	let activeTab = $state<'apis' | 'apikeys'>('apis');
	let searchQuery = $state('');

	// REST APIs
	let restApis = $state<RestApi[]>([]);
	let selectedApi = $state<RestApi | null>(null);
	let apiResources = $state<Resource[]>([]);
	let apiStages = $state<Stage[]>([]);
	let loadingApiDetail = $state(false);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newApiName = $state('');
	let newApiDescription = $state('');
	let newApiEndpointType = $state<'REGIONAL' | 'EDGE' | 'PRIVATE'>('REGIONAL');

	// API Keys
	let apiKeys = $state<ApiKey[]>([]);

	const filteredApis = $derived(
		restApis.filter(
			(a) =>
				(a.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(a.description ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredKeys = $derived(
		apiKeys.filter((k) => (k.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadApis() {
		loading = true;
		try {
			const res = await apigw.send(new GetRestApisCommand({ limit: 100 }));
			restApis = res.items ?? [];
		} catch (e) {
			toast.error(`Failed to load APIs: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewApi(api: RestApi) {
		selectedApi = api;
		if (!api.id) return;
		loadingApiDetail = true;
		apiResources = [];
		apiStages = [];
		try {
			const [resourcesRes, stagesRes] = await Promise.all([
				apigw.send(new GetResourcesCommand({ restApiId: api.id, limit: 100 })),
				apigw.send(new GetStagesCommand({ restApiId: api.id }))
			]);
			apiResources = resourcesRes.items ?? [];
			apiStages = stagesRes.item ?? [];
		} catch (e) {
			toast.error(`Failed to load API details: ${e}`);
		} finally {
			loadingApiDetail = false;
		}
	}

	async function loadApiKeys() {
		loading = true;
		try {
			const res = await apigw.send(new GetApiKeysCommand({ limit: 100 }));
			apiKeys = res.items ?? [];
		} catch (e) {
			toast.error(`Failed to load API keys: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createApi() {
		if (!newApiName.trim()) return;
		creating = true;
		try {
			await apigw.send(
				new CreateRestApiCommand({
					name: newApiName.trim(),
					description: newApiDescription.trim() || undefined,
					endpointConfiguration: { types: [newApiEndpointType] }
				})
			);
			toast.success(`API "${newApiName}" created`);
			showCreateModal = false;
			newApiName = '';
			newApiDescription = '';
			await loadApis();
		} catch (e) {
			toast.error(`Failed to create API: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApi(api: RestApi) {
		if (!api.id || !await confirmDestructive(`Delete API "${api.name}"?`)) return;
		try {
			await apigw.send(new DeleteRestApiCommand({ restApiId: api.id }));
			toast.success(`API "${api.name}" deleted`);
			if (selectedApi?.id === api.id) selectedApi = null;
			await loadApis();
		} catch (e) {
			toast.error(`Failed to delete API: ${e}`);
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedApi = null;
		if (tab === 'apis') await loadApis();
		else await loadApiKeys();
	}

	onMount(() => loadApis());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Globe class="h-8 w-8 text-blue-600" />
			<div>
				<h1 class="text-2xl font-bold">API Gateway</h1>
				<p class="text-sm text-muted-foreground">Create, publish, and manage REST and WebSocket APIs</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'apis', label: 'REST APIs' }, { id: 'apikeys', label: 'API Keys' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- REST APIs Tab -->
	{#if activeTab === 'apis'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search APIs..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create API
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredApis.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Globe class="h-12 w-12 mb-3 opacity-30" />
				<p>No REST APIs found</p>
				<p class="text-sm">Create an API to expose your services</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Description</th>
							<th class="px-4 py-3 text-left font-medium">Endpoint</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredApis as api}
							<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewApi(api)}>
								<td class="px-4 py-3 font-medium">{api.name}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs truncate max-w-[200px]">
									{api.description ?? '—'}
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{api.endpointConfiguration?.types?.join(', ') ?? '—'}
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{api.createdDate ? new Date(api.createdDate).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => { e.stopPropagation(); viewApi(api); }}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View resources"
									>
										<Eye class="h-4 w-4" />
									</button>
									<button
										onclick={(e) => { e.stopPropagation(); deleteApi(api); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete API"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- API Detail Panel -->
			{#if selectedApi}
				<div class="rounded-lg border p-4 space-y-4">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold">{selectedApi.name}</h3>
						<button onclick={() => (selectedApi = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>

					{#if loadingApiDetail}
						<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
					{:else}
						<div class="grid grid-cols-2 gap-4">
							<!-- Resources -->
							<div>
								<p class="text-sm font-medium mb-2 flex items-center gap-1">
									<Layers class="h-4 w-4" />
									Resources ({apiResources.length})
								</p>
								{#if apiResources.length === 0}
									<p class="text-sm text-muted-foreground">No resources</p>
								{:else}
									<div class="divide-y rounded border overflow-hidden max-h-48 overflow-y-auto">
										{#each apiResources as resource}
											<div class="px-3 py-2 text-xs">
												<span class="font-mono">{resource.path ?? '/'}</span>
												{#if resource.resourceMethods && Object.keys(resource.resourceMethods).length > 0}
													<span class="ml-2 text-muted-foreground">
														{Object.keys(resource.resourceMethods).join(', ')}
													</span>
												{/if}
											</div>
										{/each}
									</div>
								{/if}
							</div>

							<!-- Stages -->
							<div>
								<p class="text-sm font-medium mb-2 flex items-center gap-1">
									<ChevronRight class="h-4 w-4" />
									Stages ({apiStages.length})
								</p>
								{#if apiStages.length === 0}
									<p class="text-sm text-muted-foreground">No stages deployed</p>
								{:else}
									<div class="divide-y rounded border overflow-hidden">
										{#each apiStages as stage}
											<div class="px-3 py-2 text-xs">
												<span class="font-medium">{stage.stageName}</span>
												<span class="ml-2 text-muted-foreground">{stage.description ?? ''}</span>
												<div class="text-muted-foreground">
													Last deploy: {stage.lastUpdatedDate ? new Date(stage.lastUpdatedDate).toLocaleDateString() : '—'}
												</div>
											</div>
										{/each}
									</div>
								{/if}
							</div>
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- API Keys Tab -->
	{#if activeTab === 'apikeys'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search API keys..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredKeys.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<KeyRound class="h-12 w-12 mb-3 opacity-30" />
				<p>No API keys found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">ID</th>
							<th class="px-4 py-3 text-left font-medium">Enabled</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredKeys as key}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{key.name}</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{key.id}</td>
								<td class="px-4 py-3">
									{#if key.enabled}
										<span class="text-green-600">✓</span>
									{:else}
										<span class="text-muted-foreground">✗</span>
									{/if}
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{key.createdDate ? new Date(key.createdDate).toLocaleDateString() : '—'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create API Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create REST API</h2>
			<div class="space-y-3">
				<div>
					<label for="api-name" class="block text-sm font-medium mb-1">API Name *</label>
					<input
						id="api-name"
						type="text"
						bind:value={newApiName}
						placeholder="my-api"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="api-desc" class="block text-sm font-medium mb-1">Description</label>
					<input
						id="api-desc"
						type="text"
						bind:value={newApiDescription}
						placeholder="My REST API description"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="endpoint-type" class="block text-sm font-medium mb-1">Endpoint Type</label>
					<select
						id="endpoint-type"
						bind:value={newApiEndpointType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="REGIONAL">Regional</option>
						<option value="EDGE">Edge-optimized</option>
						<option value="PRIVATE">Private</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createApi}
					disabled={creating || !newApiName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create API'}
				</button>
			</div>
		</div>
	</div>
{/if}
