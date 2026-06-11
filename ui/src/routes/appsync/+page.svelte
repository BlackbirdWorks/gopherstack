<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getAppSyncClient } from '$lib/aws-client';
	import {
		ListGraphqlApisCommand,
		GetGraphqlApiCommand,
		CreateGraphqlApiCommand,
		DeleteGraphqlApiCommand,
		ListDataSourcesCommand,
		CreateDataSourceCommand,
		DeleteDataSourceCommand,
		StartSchemaCreationCommand,
		ListFunctionsCommand,
		ListResolversCommand,
		GetResolverCommand,
		UpdateResolverCommand,
		GetIntrospectionSchemaCommand,
		type GraphqlApi,
		type DataSource,
		type FunctionConfiguration,
		type Resolver,
		DataSourceType
	} from '@aws-sdk/client-appsync';
	import { toast } from 'svelte-sonner';
	import {
		Zap,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Database,
		Code,
		Eye,
		ChevronRight,
		Play,
		Edit
	} from 'lucide-svelte';

	const appsync = getAppSyncClient();

	let loading = $state(false);
	let activeTab = $state<'apis' | 'datasources' | 'functions' | 'resolvers' | 'graphql'>('apis');
	let searchQuery = $state('');

	// APIs
	let apis = $state<GraphqlApi[]>([]);
	let selectedApi = $state<GraphqlApi | null>(null);
	let apiSchema = $state<string>('');
	let loadingSchema = $state(false);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newApiName = $state('');
	let newApiAuthType = $state<'API_KEY' | 'AWS_IAM' | 'AMAZON_COGNITO_USER_POOLS' | 'OPENID_CONNECT'>('API_KEY');

	// Data sources
	let dataSources = $state<DataSource[]>([]);
	let selectedApiId = $state('');

	// Functions
	let functions = $state<FunctionConfiguration[]>([]);

	// Resolvers
	let resolvers = $state<Resolver[]>([]);
	let resolverTypeName = $state('');
	let editingResolver = $state<Resolver | null>(null);
	let resolverRequestVTL = $state('');
	let resolverResponseVTL = $state('');
	let savingResolver = $state(false);

	// GraphQL executor
	let gqlQuery = $state('{\n  __typename\n}');
	let gqlVariables = $state('{}');
	let gqlResult = $state('');
	let executingGql = $state(false);

	const filteredApis = $derived(
		apis.filter((a) => (a.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredDataSources = $derived(
		dataSources.filter(
			(d) =>
				(d.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(d.type ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredFunctions = $derived(
		functions.filter((f) => (f.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredResolvers = $derived(
		resolvers.filter(
			(r) =>
				(r.typeName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(r.fieldName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function authTypeBadge(authType?: string) {
		if (authType === 'API_KEY') return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
		if (authType === 'AWS_IAM') return 'text-purple-700 bg-purple-100 dark:text-purple-300 dark:bg-purple-900';
		if (authType === 'AMAZON_COGNITO_USER_POOLS')
			return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadApis() {
		loading = true;
		try {
			const res = await appsync.send(new ListGraphqlApisCommand({ maxResults: 100 }));
			apis = res.graphqlApis ?? [];
		} catch (e) {
			toast.error(`Failed to load APIs: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewApi(api: GraphqlApi) {
		selectedApi = api;
		apiSchema = '';
		if (!api.apiId) return;
		selectedApiId = api.apiId;
		loadingSchema = true;
		try {
			const res = await appsync.send(
				new GetIntrospectionSchemaCommand({ apiId: api.apiId, format: 'SDL' })
			);
			if (res.schema) {
				const decoder = new TextDecoder();
				apiSchema = decoder.decode(res.schema);
			}
		} catch {
			// Schema may not be available.
			apiSchema = '';
		} finally {
			loadingSchema = false;
		}
	}

	async function loadDataSources(apiId?: string) {
		loading = true;
		const id = apiId ?? selectedApiId;
		if (!id) {
			loading = false;
			return;
		}
		try {
			const res = await appsync.send(new ListDataSourcesCommand({ apiId: id, maxResults: 100 }));
			dataSources = res.dataSources ?? [];
		} catch (e) {
			toast.error(`Failed to load data sources: ${e}`);
		} finally {
			loading = false;
		}
	}

	// Create data source
	let showCreateDS = $state(false);
	let creatingDS = $state(false);
	let dsName = $state('');
	let dsDescription = $state('');
	let dsType = $state<string>('AMAZON_DYNAMODB');
	let dsServiceRoleArn = $state('');
	let dsTableName = $state('');
	let dsAwsRegion = $state('us-east-1');
	let dsLambdaArn = $state('');
	let dsHttpEndpoint = $state('');

	async function createDataSource() {
		if (!selectedApiId) return;
		if (!dsName.trim()) {
			toast.error('Data source name is required');
			return;
		}
		creatingDS = true;
		try {
			await appsync.send(
				new CreateDataSourceCommand({
					apiId: selectedApiId,
					name: dsName.trim(),
					description: dsDescription.trim() || undefined,
					type: dsType as DataSourceType,
					serviceRoleArn: dsServiceRoleArn.trim() || undefined,
					dynamodbConfig:
						dsType === DataSourceType.AMAZON_DYNAMODB
							? { tableName: dsTableName.trim(), awsRegion: dsAwsRegion.trim() }
							: undefined,
					lambdaConfig:
						dsType === DataSourceType.AWS_LAMBDA
							? { lambdaFunctionArn: dsLambdaArn.trim() }
							: undefined,
					httpConfig:
						dsType === DataSourceType.HTTP ? { endpoint: dsHttpEndpoint.trim() } : undefined
				})
			);
			toast.success(`Data source "${dsName}" created`);
			showCreateDS = false;
			dsName = '';
			dsDescription = '';
			dsServiceRoleArn = '';
			dsTableName = '';
			dsLambdaArn = '';
			dsHttpEndpoint = '';
			await loadDataSources();
		} catch (e) {
			toast.error(`Failed to create data source: ${e}`);
		} finally {
			creatingDS = false;
		}
	}

	async function deleteDataSource(name: string | undefined) {
		if (!name || !selectedApiId) return;
		if (
			!(await confirmDestructive({
				title: 'Delete Data Source',
				message: `Delete data source "${name}"?`
			}))
		)
			return;
		try {
			await appsync.send(new DeleteDataSourceCommand({ apiId: selectedApiId, name }));
			toast.success('Data source deleted');
			await loadDataSources();
		} catch (e) {
			toast.error(`Failed to delete data source: ${e}`);
		}
	}

	// Schema upload (SDL)
	let showSchemaUpload = $state(false);
	let schemaSdl = $state('');
	let uploadingSchema = $state(false);

	async function uploadSchema() {
		if (!selectedApiId || !schemaSdl.trim()) {
			toast.error('Schema SDL is required');
			return;
		}
		uploadingSchema = true;
		try {
			await appsync.send(
				new StartSchemaCreationCommand({
					apiId: selectedApiId,
					definition: new TextEncoder().encode(schemaSdl)
				})
			);
			toast.success('Schema creation started');
			showSchemaUpload = false;
		} catch (e) {
			toast.error(`Failed to upload schema: ${e}`);
		} finally {
			uploadingSchema = false;
		}
	}

	async function loadFunctions(apiId?: string) {
		loading = true;
		const id = apiId ?? selectedApiId;
		if (!id) {
			loading = false;
			return;
		}
		try {
			const res = await appsync.send(
				new ListFunctionsCommand({ apiId: id, maxResults: 100 })
			);
			functions = res.functions ?? [];
		} catch (e) {
			toast.error(`Failed to load functions: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadResolvers(typeName?: string) {
		loading = true;
		const id = selectedApiId;
		const type = typeName ?? resolverTypeName;
		if (!id || !type) {
			loading = false;
			return;
		}
		try {
			const res = await appsync.send(
				new ListResolversCommand({ apiId: id, typeName: type, maxResults: 100 })
			);
			resolvers = res.resolvers ?? [];
		} catch (e) {
			toast.error(`Failed to load resolvers: ${e}`);
		} finally {
			loading = false;
		}
	}

	function openResolverEditor(resolver: Resolver) {
		editingResolver = resolver;
		resolverRequestVTL = resolver.requestMappingTemplate ?? '';
		resolverResponseVTL = resolver.responseMappingTemplate ?? '';
	}

	function closeResolverEditor() {
		editingResolver = null;
		resolverRequestVTL = '';
		resolverResponseVTL = '';
	}

	async function saveResolver() {
		if (!editingResolver || !selectedApiId) return;
		savingResolver = true;
		try {
			await appsync.send(
				new UpdateResolverCommand({
					apiId: selectedApiId,
					typeName: editingResolver.typeName,
					fieldName: editingResolver.fieldName,
					dataSourceName: editingResolver.dataSourceName,
					requestMappingTemplate: resolverRequestVTL || undefined,
					responseMappingTemplate: resolverResponseVTL || undefined
				})
			);
			toast.success('Resolver updated');
			closeResolverEditor();
			await loadResolvers();
		} catch (e) {
			toast.error(`Failed to save resolver: ${e}`);
		} finally {
			savingResolver = false;
		}
	}

	async function executeGraphQL() {
		if (!selectedApiId) {
			toast.error('Select an API first');
			return;
		}
		executingGql = true;
		gqlResult = '';
		try {
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			let variables: Record<string, any> = {};
			try {
				variables = JSON.parse(gqlVariables || '{}');
			} catch {
				toast.error('Invalid variables JSON');
				return;
			}
			// Use raw fetch to POST to the GraphQL endpoint
			const endpoint = `/appsync/v1/apis/${selectedApiId}/graphql`;
			const res = await fetch(endpoint, {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ query: gqlQuery, variables })
			});
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const json: any = await res.json();
			gqlResult = JSON.stringify(json, null, 2);
		} catch (e) {
			gqlResult = `Error: ${e}`;
		} finally {
			executingGql = false;
		}
	}

	async function createApi() {
		if (!newApiName.trim()) return;
		creating = true;
		try {
			await appsync.send(
				new CreateGraphqlApiCommand({
					name: newApiName.trim(),
					authenticationType: newApiAuthType
				})
			);
			toast.success(`API "${newApiName}" created`);
			showCreateModal = false;
			newApiName = '';
			await loadApis();
		} catch (e) {
			toast.error(`Failed to create API: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApi(api: GraphqlApi) {
		if (!api.apiId || !await confirmDestructive({ title: 'Delete AppSync API', message: `Delete API "${api.name}"? All resolvers and schemas will be removed.` })) return;
		try {
			await appsync.send(new DeleteGraphqlApiCommand({ apiId: api.apiId }));
			toast.success(`API "${api.name}" deleted`);
			if (selectedApi?.apiId === api.apiId) selectedApi = null;
			await loadApis();
		} catch (e) {
			toast.error(`Failed to delete API: ${e}`);
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'apis') await loadApis();
		else if (tab === 'datasources') {
			if (selectedApiId) await loadDataSources();
			else toast.error('Select an API first to view data sources');
		} else if (tab === 'functions') {
			if (selectedApiId) await loadFunctions();
			else toast.error('Select an API first to view functions');
		} else if (tab === 'resolvers' && selectedApiId && resolverTypeName) {
			await loadResolvers();
		}
		// graphql tab needs no initial load
	}

	onMount(() => loadApis());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Zap class="h-8 w-8 text-purple-600" />
			<div>
				<h1 class="text-2xl font-bold">AppSync</h1>
				<p class="text-sm text-muted-foreground">Managed GraphQL APIs for real-time data</p>
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
	<div class="flex border-b overflow-x-auto">
		{#each [
			{ id: 'apis', label: 'GraphQL APIs' },
			{ id: 'datasources', label: 'Data Sources' },
			{ id: 'functions', label: 'Functions' },
			{ id: 'resolvers', label: 'Resolver Editor' },
			{ id: 'graphql', label: 'GraphQL Executor' }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- APIs Tab -->
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
				<Zap class="h-12 w-12 mb-3 opacity-30" />
				<p>No GraphQL APIs found</p>
				<p class="text-sm">Create an API to get started</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Auth Type</th>
							<th class="px-4 py-3 text-left font-medium">API ID</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredApis as api}
							<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewApi(api)}>
								<td class="px-4 py-3 font-medium">{api.name}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {authTypeBadge(api.authenticationType)}">
										{api.authenticationType?.replace(/_/g, ' ') ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{api.apiId}</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => { e.stopPropagation(); viewApi(api); }}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View details"
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

			<!-- API Details -->
			{#if selectedApi}
				<div class="rounded-lg border p-4 space-y-4">
					<div class="flex items-center justify-between">
						<h3 class="font-semibold">{selectedApi.name}</h3>
						<div class="flex gap-2">
							<button
								onclick={() => { selectedApiId = selectedApi?.apiId ?? ''; loadDataSources(); onTabChange('datasources'); }}
								class="flex items-center gap-1.5 text-xs rounded-md border px-3 py-1.5 hover:bg-accent"
							>
								<Database class="h-3.5 w-3.5" />
								Data Sources
							</button>
							<button
								onclick={() => { selectedApiId = selectedApi?.apiId ?? ''; loadFunctions(); onTabChange('functions'); }}
								class="flex items-center gap-1.5 text-xs rounded-md border px-3 py-1.5 hover:bg-accent"
							>
								<Code class="h-3.5 w-3.5" />
								Functions
							</button>
							<button
								onclick={() => { selectedApiId = selectedApi?.apiId ?? ''; onTabChange('graphql'); }}
								class="flex items-center gap-1.5 text-xs rounded-md border px-3 py-1.5 hover:bg-accent"
							>
								<Play class="h-3.5 w-3.5" />
								Execute GraphQL
							</button>
							<button onclick={() => (selectedApi = null)} class="text-xs text-muted-foreground hover:text-foreground">
								Close
							</button>
						</div>
					</div>
					<div class="grid grid-cols-2 gap-3 text-sm">
						<div>
							<span class="text-muted-foreground">API ID:</span>
							<span class="ml-1 font-mono">{selectedApi.apiId}</span>
						</div>
						<div>
							<span class="text-muted-foreground">Region:</span>
							<span class="ml-1">{selectedApi.arn?.split(':')[3] ?? '—'}</span>
						</div>
					</div>
					{#if selectedApi.uris && Object.keys(selectedApi.uris).length > 0}
						<div>
							<p class="text-sm font-medium mb-2">Endpoints</p>
							<div class="space-y-1">
								{#each Object.entries(selectedApi.uris) as [type, uri]}
									<div class="flex items-center gap-2 text-xs">
										<span class="w-16 font-medium">{type}:</span>
										<span class="text-muted-foreground font-mono truncate">{uri}</span>
									</div>
								{/each}
							</div>
						</div>
					{/if}
					{#if loadingSchema}
						<div class="flex items-center gap-2 text-sm text-muted-foreground">
							<RefreshCw class="h-4 w-4 animate-spin" />
							Loading schema…
						</div>
					{:else if apiSchema}
						<div>
							<p class="text-sm font-medium mb-2">Schema (SDL)</p>
							<pre class="rounded bg-muted p-3 text-xs overflow-auto max-h-48">{apiSchema.slice(0, 2000)}{apiSchema.length > 2000 ? '\n…(truncated)' : ''}</pre>
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Data Sources Tab -->
	{#if activeTab === 'datasources'}
		{#if selectedApiId}
			<div class="flex items-center justify-end gap-2">
				<button onclick={() => { showSchemaUpload = true; }} class="flex items-center gap-2 rounded-md border px-3 py-1.5 text-sm hover:bg-muted">
					<Code class="h-4 w-4" /> Upload Schema (SDL)
				</button>
				<button onclick={() => { showCreateDS = true; }} class="flex items-center gap-2 rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90">
					<Plus class="h-4 w-4" /> Create Data Source
				</button>
			</div>
		{/if}
		{#if !selectedApiId}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Database class="h-12 w-12 mb-3 opacity-30" />
				<p>Select an API from the APIs tab to view data sources</p>
			</div>
		{:else if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredDataSources.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Database class="h-12 w-12 mb-3 opacity-30" />
				<p>No data sources found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">ARN</th>
							<th class="px-4 py-3 text-right font-medium"></th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredDataSources as ds}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{ds.name}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs bg-muted">{ds.type ?? '—'}</span>
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[300px]">
									{ds.dataSourceArn ?? '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button onclick={() => deleteDataSource(ds.name)} class="p-1.5 text-red-500 hover:bg-red-50 dark:hover:bg-red-900/20 rounded" title="Delete data source">
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Functions Tab -->
	{#if activeTab === 'functions'}
		{#if !selectedApiId}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Code class="h-12 w-12 mb-3 opacity-30" />
				<p>Select an API from the APIs tab to view functions</p>
			</div>
		{:else if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredFunctions.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Code class="h-12 w-12 mb-3 opacity-30" />
				<p>No functions found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Data Source</th>
							<th class="px-4 py-3 text-left font-medium">Runtime</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredFunctions as fn}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{fn.name}</td>
								<td class="px-4 py-3 text-muted-foreground">{fn.dataSourceName ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{fn.runtime?.name ?? fn.syncConfig?.conflictHandler ?? '—'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Resolver Editor Tab -->
	{#if activeTab === 'resolvers'}
		{#if !selectedApiId}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Edit class="h-12 w-12 mb-3 opacity-30" />
				<p>Select an API from the APIs tab first</p>
			</div>
		{:else}
			<div class="flex items-center gap-3 mb-4">
				<input
					type="text"
					placeholder="Type name (e.g. Query)"
					bind:value={resolverTypeName}
					class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary w-56"
				/>
				<button
					onclick={() => loadResolvers()}
					disabled={!resolverTypeName}
					class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				>
					<Search class="h-4 w-4" />
					Load Resolvers
				</button>
			</div>

			{#if loading}
				<div class="flex justify-center py-12">
					<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
				</div>
			{:else if resolvers.length === 0 && resolverTypeName}
				<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
					<Edit class="h-12 w-12 mb-3 opacity-30" />
					<p>No resolvers found for type "{resolverTypeName}"</p>
				</div>
			{:else if resolvers.length > 0}
				<div class="flex gap-4">
					<!-- Resolver list -->
					<div class="w-64 shrink-0 rounded-lg border overflow-hidden">
						<div class="bg-muted/50 px-3 py-2 text-xs font-medium">Resolvers</div>
						<div class="divide-y">
							{#each filteredResolvers as r}
								<button
									onclick={() => openResolverEditor(r)}
									class="w-full text-left px-3 py-2 text-sm hover:bg-accent {editingResolver?.fieldName === r.fieldName ? 'bg-accent' : ''}"
								>
									<div class="font-medium">{r.fieldName}</div>
									<div class="text-xs text-muted-foreground">{r.kind ?? 'UNIT'}</div>
								</button>
							{/each}
						</div>
					</div>

					<!-- VTL Editor -->
					{#if editingResolver}
						<div class="flex-1 space-y-4">
							<div class="flex items-center justify-between">
								<h3 class="font-semibold text-sm">
									{editingResolver.typeName}.{editingResolver.fieldName}
								</h3>
								<div class="flex gap-2">
									<button
										onclick={saveResolver}
										disabled={savingResolver}
										class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
									>
										{savingResolver ? 'Saving…' : 'Save'}
									</button>
									<button
										onclick={closeResolverEditor}
										class="text-xs text-muted-foreground hover:text-foreground"
									>
										Cancel
									</button>
								</div>
							</div>

							<div>
								<label class="block text-xs font-medium mb-1 text-muted-foreground">Request Mapping Template (VTL)</label>
								<textarea
									bind:value={resolverRequestVTL}
									rows={8}
									placeholder="&#123;&#10;  &quot;version&quot;: &quot;2018-05-29&quot;,&#10;  &quot;operation&quot;: &quot;Invoke&quot;&#10;&#125;"
									class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary resize-y"
								></textarea>
							</div>

							<div>
								<label class="block text-xs font-medium mb-1 text-muted-foreground">Response Mapping Template (VTL)</label>
								<textarea
									bind:value={resolverResponseVTL}
									rows={8}
									placeholder="$util.toJson($context.result)"
									class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary resize-y"
								></textarea>
							</div>

							<div class="rounded-md bg-muted/50 p-3 text-xs text-muted-foreground space-y-1">
								<p class="font-medium">VTL quick reference</p>
								<p><code>$ctx.args.key</code> — resolver argument</p>
								<p><code>$ctx.result</code> — resolver result</p>
								<p><code>$util.toJson($ctx.result)</code> — JSON-encode result</p>
								<p><code>$util.dynamodb.toDynamoDBJson($ctx.args.id)</code> — DynamoDB format</p>
							</div>
						</div>
					{:else}
						<div class="flex-1 flex items-center justify-center text-muted-foreground text-sm">
							Select a resolver to edit its VTL templates
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- GraphQL Executor Tab -->
	{#if activeTab === 'graphql'}
		{#if !selectedApiId}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Play class="h-12 w-12 mb-3 opacity-30" />
				<p>Select an API from the APIs tab first</p>
			</div>
		{:else}
			<div class="space-y-4">
				<div class="flex items-center justify-between">
					<p class="text-sm text-muted-foreground">
						Executing against API: <span class="font-mono text-foreground">{selectedApiId}</span>
					</p>
					<button
						onclick={executeGraphQL}
						disabled={executingGql}
						class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
					>
						{#if executingGql}
							<RefreshCw class="h-4 w-4 animate-spin" />
							Running…
						{:else}
							<Play class="h-4 w-4" />
							Execute
						{/if}
					</button>
				</div>

				<div class="grid grid-cols-2 gap-4">
					<div class="space-y-2">
						<label class="block text-sm font-medium">Query / Mutation</label>
						<textarea
							bind:value={gqlQuery}
							rows={14}
							class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary resize-y"
						></textarea>
					</div>
					<div class="space-y-2">
						<label class="block text-sm font-medium">Variables (JSON)</label>
						<textarea
							bind:value={gqlVariables}
							rows={6}
							class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary resize-y"
						></textarea>
					</div>
				</div>

				{#if gqlResult}
					<div>
						<p class="text-sm font-medium mb-2">Result</p>
						<pre class="rounded-lg bg-muted p-4 text-xs font-mono overflow-auto max-h-96">{gqlResult}</pre>
					</div>
				{/if}
			</div>
		{/if}
	{/if}
</div>

<!-- Create API Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create GraphQL API</h2>
			<div class="space-y-3">
				<div>
					<label for="api-name" class="block text-sm font-medium mb-1">API Name *</label>
					<input
						id="api-name"
						type="text"
						bind:value={newApiName}
						placeholder="my-graphql-api"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="api-auth" class="block text-sm font-medium mb-1">Authentication Type</label>
					<select
						id="api-auth"
						bind:value={newApiAuthType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="API_KEY">API Key</option>
						<option value="AWS_IAM">AWS IAM</option>
						<option value="AMAZON_COGNITO_USER_POOLS">Cognito User Pools</option>
						<option value="OPENID_CONNECT">OpenID Connect</option>
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

<!-- Create Data Source Modal -->
{#if showCreateDS}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-lg border bg-background p-6 shadow-xl">
			<h2 class="mb-4 text-lg font-semibold">Create Data Source</h2>
			<div class="space-y-3">
				<div>
					<label for="ds-name" class="mb-1 block text-sm font-medium">Name</label>
					<input id="ds-name" bind:value={dsName} type="text" placeholder="myDataSource" class="w-full rounded-md border bg-background px-3 py-2 text-sm" />
				</div>
				<div>
					<label for="ds-type" class="mb-1 block text-sm font-medium">Type</label>
					<select id="ds-type" bind:value={dsType} class="w-full rounded-md border bg-background px-3 py-2 text-sm">
						<option value="AMAZON_DYNAMODB">Amazon DynamoDB</option>
						<option value="AWS_LAMBDA">AWS Lambda</option>
						<option value="HTTP">HTTP</option>
						<option value="NONE">None (local)</option>
						<option value="RELATIONAL_DATABASE">Relational Database</option>
					</select>
				</div>
				{#if dsType === 'AMAZON_DYNAMODB'}
					<div>
						<label for="ds-table" class="mb-1 block text-sm font-medium">Table Name</label>
						<input id="ds-table" bind:value={dsTableName} type="text" placeholder="MyTable" class="w-full rounded-md border bg-background px-3 py-2 text-sm" />
					</div>
					<div>
						<label for="ds-region" class="mb-1 block text-sm font-medium">AWS Region</label>
						<input id="ds-region" bind:value={dsAwsRegion} type="text" placeholder="us-east-1" class="w-full rounded-md border bg-background px-3 py-2 text-sm" />
					</div>
				{:else if dsType === 'AWS_LAMBDA'}
					<div>
						<label for="ds-lambda" class="mb-1 block text-sm font-medium">Lambda Function ARN</label>
						<input id="ds-lambda" bind:value={dsLambdaArn} type="text" placeholder="arn:aws:lambda:..." class="w-full rounded-md border bg-background px-3 py-2 text-sm" />
					</div>
				{:else if dsType === 'HTTP'}
					<div>
						<label for="ds-http" class="mb-1 block text-sm font-medium">Endpoint</label>
						<input id="ds-http" bind:value={dsHttpEndpoint} type="text" placeholder="https://api.example.com" class="w-full rounded-md border bg-background px-3 py-2 text-sm" />
					</div>
				{/if}
				<div>
					<label for="ds-role" class="mb-1 block text-sm font-medium">Service Role ARN (optional)</label>
					<input id="ds-role" bind:value={dsServiceRoleArn} type="text" placeholder="arn:aws:iam::..." class="w-full rounded-md border bg-background px-3 py-2 text-sm" />
				</div>
			</div>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => { showCreateDS = false; }} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createDataSource} disabled={creatingDS || !dsName.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creatingDS ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Upload Schema (SDL) Modal -->
{#if showSchemaUpload}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-2xl rounded-lg border bg-background p-6 shadow-xl">
			<h2 class="mb-4 text-lg font-semibold">Upload GraphQL Schema (SDL)</h2>
			<textarea
				bind:value={schemaSdl}
				rows="14"
				placeholder={'type Query {\n  hello: String\n}'}
				class="w-full rounded-md border bg-background px-3 py-2 font-mono text-xs"
			></textarea>
			<p class="mt-2 text-xs text-muted-foreground">Submits the SDL via StartSchemaCreation. Schema processing runs asynchronously on the API.</p>
			<div class="mt-6 flex justify-end gap-3">
				<button onclick={() => { showSchemaUpload = false; }} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={uploadSchema} disabled={uploadingSchema || !schemaSdl.trim()} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{uploadingSchema ? 'Uploading...' : 'Upload Schema'}
				</button>
			</div>
		</div>
	</div>
{/if}
