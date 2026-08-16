<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getAPIGatewayClient } from '$lib/aws-client';
	import {
		GetRestApisCommand,
		GetResourcesCommand,
		CreateRestApiCommand,
		DeleteRestApiCommand,
		GetApiKeysCommand,
		CreateApiKeyCommand,
		DeleteApiKeyCommand,
		UpdateApiKeyCommand,
		GetDeploymentsCommand,
		GetStagesCommand,
		CreateDeploymentCommand,
		GetUsagePlansCommand,
		CreateUsagePlanCommand,
		DeleteUsagePlanCommand,
		GetUsagePlanKeysCommand,
		CreateUsagePlanKeyCommand,
		GetDomainNamesCommand,
		CreateDomainNameCommand,
		DeleteDomainNameCommand,
		GetAuthorizersCommand,
		GetModelsCommand,
		type RestApi,
		type Resource,
		type ApiKey,
		type Deployment,
		type Stage,
		type UsagePlan,
		type UsagePlanKey,
		type DomainName,
		type Authorizer,
		type Model
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
		Layers,
		Copy,
		Check,
		Rocket,
		BookOpen,
		BarChart2,
		Map,
		Shield,
		Code2,
		Link,
		ToggleLeft,
		ToggleRight,
		Database
	} from 'lucide-svelte';

	const apigw = regionalClient(getAPIGatewayClient);

	// Every row carries the region its Get*Command call was made against.
	// Row actions (delete/toggle/deploy) must use THIS region, not the page's
	// shared `apigw()` client -- in All mode the same name can legitimately
	// exist in two different regions.
	type Regioned<T> = T & { region: string };

	// ─── Top-level tab ───────────────────────────────────────────────────────────
	let topTab = $state<'apis' | 'apikeys' | 'usageplans' | 'domainnames' | 'metrics' | 'docs'>('apis');
	let searchQuery = $state('');
	let loading = $state(false);

	async function loadRegioned<TResponse, TItem>(
		label: string,
		regionCall: (region: string) => Promise<TResponse>,
		extractItems: (r: TResponse) => TItem[],
		assign: (items: Regioned<TItem>[]) => void
	) {
		loading = true;
		try {
			const result = await multiRegionList(regionCall, extractItems);
			assign(result.items.map(({ region, item }) => ({ ...item, region }) as Regioned<TItem>));
			if (result.errors.length > 0) toast.error(`Failed to load ${label} from ${result.errors.length} region(s)`);
		} catch (e) {
			toast.error(`Failed to load ${label}: ${e}`);
		} finally {
			loading = false;
		}
	}

	// ─── REST APIs ───────────────────────────────────────────────────────────────
	let restApis = $state<Regioned<RestApi>[]>([]);
	let selectedApi = $state<Regioned<RestApi> | null>(null);
	let apiResources = $state<Resource[]>([]);
	let apiStages = $state<Stage[]>([]);
	let apiDeployments = $state<Deployment[]>([]);
	let apiAuthorizers = $state<Authorizer[]>([]);
	let apiModels = $state<Model[]>([]);
	let loadingApiDetail = $state(false);
	let apiDetailTab = $state<'resources' | 'stages' | 'deployments' | 'authorizers' | 'models'>('resources');
	let showCreateApiModal = $state(false);
	let creating = $state(false);
	let newApiName = $state('');
	let newApiDescription = $state('');
	let newApiEndpointType = $state<'REGIONAL' | 'EDGE' | 'PRIVATE'>('REGIONAL');
	// Deploy modal
	let showDeployModal = $state(false);
	let deployStageName = $state('v1');
	let deployDescription = $state('');
	let deploying = $state(false);

	const filteredApis = $derived(
		restApis.filter(
			(a) =>
				(a.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(a.description ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	// ─── API Keys ────────────────────────────────────────────────────────────────
	let apiKeys = $state<Regioned<ApiKey>[]>([]);
	let showCreateKeyModal = $state(false);
	let newKeyName = $state('');
	let newKeyDescription = $state('');
	let newKeyEnabled = $state(true);
	let copiedKeyId = $state<string | null>(null);

	const filteredKeys = $derived(
		apiKeys.filter((k) => (k.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ─── Usage Plans ─────────────────────────────────────────────────────────────
	let usagePlans = $state<Regioned<UsagePlan>[]>([]);
	let selectedPlan = $state<Regioned<UsagePlan> | null>(null);
	let planKeys = $state<UsagePlanKey[]>([]);
	let loadingPlanKeys = $state(false);
	let showCreatePlanModal = $state(false);
	let newPlanName = $state('');
	let newPlanDescription = $state('');
	let showAddKeyModal = $state(false);
	let addKeyId = $state('');
	let addKeyType = $state('API_KEY');

	const filteredPlans = $derived(
		usagePlans.filter((p) => (p.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ─── Domain Names ────────────────────────────────────────────────────────────
	let domainNames = $state<Regioned<DomainName>[]>([]);
	let showCreateDomainModal = $state(false);
	let newDomainName = $state('');
	let newDomainCertArn = $state('');
	let newDomainEndpointType = $state<'REGIONAL' | 'EDGE'>('REGIONAL');

	const filteredDomains = $derived(
		domainNames.filter((d) => (d.domainName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ─── Supported operations list ───────────────────────────────────────────────
	const supportedOperations = [
		'CreateRestApi', 'DeleteRestApi', 'GetRestApi', 'GetRestApis', 'UpdateRestApi',
		'CreateResource', 'DeleteResource', 'GetResource', 'GetResources', 'UpdateResource',
		'PutMethod', 'GetMethod', 'DeleteMethod',
		'PutMethodResponse', 'GetMethodResponse', 'DeleteMethodResponse',
		'PutIntegration', 'GetIntegration', 'DeleteIntegration',
		'PutIntegrationResponse', 'GetIntegrationResponse', 'DeleteIntegrationResponse',
		'CreateDeployment', 'GetDeployment', 'GetDeployments', 'DeleteDeployment', 'UpdateDeployment',
		'GetStage', 'GetStages', 'DeleteStage', 'CreateStage', 'UpdateStage',
		'FlushStageCache', 'FlushStageAuthorizersCache',
		'CreateAuthorizer', 'GetAuthorizer', 'GetAuthorizers', 'UpdateAuthorizer', 'DeleteAuthorizer',
		'CreateRequestValidator', 'GetRequestValidator', 'GetRequestValidators',
		'UpdateRequestValidator', 'DeleteRequestValidator',
		'CreateApiKey', 'GetApiKey', 'GetApiKeys', 'UpdateApiKey', 'DeleteApiKey',
		'CreateDomainName', 'GetDomainName', 'GetDomainNames', 'DeleteDomainName',
		'CreateBasePathMapping', 'GetBasePathMapping', 'GetBasePathMappings', 'DeleteBasePathMapping',
		'CreateModel', 'GetModel', 'GetModels', 'UpdateModel', 'DeleteModel',
		'CreateUsagePlan', 'GetUsagePlan', 'GetUsagePlans', 'DeleteUsagePlan',
		'CreateUsagePlanKey', 'GetUsagePlanKey', 'GetUsagePlanKeys', 'DeleteUsagePlanKey',
		'CreateDocumentationPart', 'GetDocumentationPart', 'GetDocumentationParts', 'DeleteDocumentationPart',
		'CreateDocumentationVersion', 'GetDocumentationVersion', 'GetDocumentationVersions', 'DeleteDocumentationVersion',
		'GetAccount', 'GetTags', 'TagResource', 'UntagResource',
		'TestInvokeMethod',
	];

	// ─── Data loaders ────────────────────────────────────────────────────────────
	async function loadApis() {
		await loadRegioned('APIs',
			(region) => getAPIGatewayClient(region).send(new GetRestApisCommand({ limit: 100 })),
			(r) => r.items ?? [],
			(items) => restApis = items);
	}

	async function loadApiDetail(api: Regioned<RestApi>) {
		selectedApi = api;
		if (!api.id) return;
		loadingApiDetail = true;
		apiResources = [];
		apiStages = [];
		apiDeployments = [];
		apiAuthorizers = [];
		apiModels = [];
		apiDetailTab = 'resources';
		try {
			const client = getAPIGatewayClient(api.region);
			const [resourcesRes, stagesRes, deplsRes, authRes, modelsRes] = await Promise.all([
				client.send(new GetResourcesCommand({ restApiId: api.id, limit: 100 })),
				client.send(new GetStagesCommand({ restApiId: api.id })),
				client.send(new GetDeploymentsCommand({ restApiId: api.id, limit: 100 })),
				client.send(new GetAuthorizersCommand({ restApiId: api.id })),
				client.send(new GetModelsCommand({ restApiId: api.id, limit: 100 })),
			]);
			apiResources = resourcesRes.items ?? [];
			apiStages = stagesRes.item ?? [];
			apiDeployments = deplsRes.items ?? [];
			apiAuthorizers = authRes.items ?? [];
			apiModels = modelsRes.items ?? [];
		} catch (e) {
			toast.error(`Failed to load API details: ${e}`);
		} finally {
			loadingApiDetail = false;
		}
	}

	async function loadApiKeys() {
		await loadRegioned('API keys',
			(region) => getAPIGatewayClient(region).send(new GetApiKeysCommand({ limit: 100, includeValues: true })),
			(r) => r.items ?? [],
			(items) => apiKeys = items);
	}

	async function loadUsagePlans() {
		await loadRegioned('usage plans',
			(region) => getAPIGatewayClient(region).send(new GetUsagePlansCommand({ limit: 100 })),
			(r) => r.items ?? [],
			(items) => usagePlans = items);
	}

	async function loadDomainNames() {
		await loadRegioned('domain names',
			(region) => getAPIGatewayClient(region).send(new GetDomainNamesCommand({ limit: 100 })),
			(r) => r.items ?? [],
			(items) => domainNames = items);
	}

	async function loadPlanKeys(planId: string, region: string) {
		loadingPlanKeys = true;
		try {
			const res = await getAPIGatewayClient(region).send(new GetUsagePlanKeysCommand({ usagePlanId: planId, limit: 100 }));
			planKeys = res.items ?? [];
		} catch (e) {
			toast.error(`Failed to load plan keys: ${e}`);
		} finally {
			loadingPlanKeys = false;
		}
	}

	// ─── CRUD ────────────────────────────────────────────────────────────────────
	async function createApi() {
		if (!newApiName.trim()) return;
		creating = true;
		try {
			await apigw().send(
				new CreateRestApiCommand({
					name: newApiName.trim(),
					description: newApiDescription.trim() || undefined,
					endpointConfiguration: { types: [newApiEndpointType] }
				})
			);
			toast.success(`API "${newApiName}" created`);
			showCreateApiModal = false;
			newApiName = '';
			newApiDescription = '';
			await loadApis();
		} catch (e) {
			toast.error(`Failed to create API: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApi(api: Regioned<RestApi>) {
		if (!api.id || !(await confirmDestructive({ title: 'Delete API', message: `Delete API "${api.name}"?` }))) return;
		try {
			await getAPIGatewayClient(api.region).send(new DeleteRestApiCommand({ restApiId: api.id }));
			toast.success(`API "${api.name}" deleted`);
			if (selectedApi?.id === api.id) selectedApi = null;
			await loadApis();
		} catch (e) {
			toast.error(`Failed to delete API: ${e}`);
		}
	}

	async function deployApi() {
		if (!selectedApi?.id || !deployStageName.trim()) return;
		deploying = true;
		try {
			await getAPIGatewayClient(selectedApi.region).send(new CreateDeploymentCommand({
				restApiId: selectedApi.id,
				stageName: deployStageName.trim(),
				description: deployDescription.trim() || undefined,
			}));
			toast.success(`Deployed to stage "${deployStageName}"`);
			showDeployModal = false;
			deployStageName = 'v1';
			deployDescription = '';
			await loadApiDetail(selectedApi);
		} catch (e) {
			toast.error(`Deploy failed: ${e}`);
		} finally {
			deploying = false;
		}
	}

	async function createApiKey() {
		if (!newKeyName.trim()) return;
		creating = true;
		try {
			await apigw().send(new CreateApiKeyCommand({
				name: newKeyName.trim(),
				description: newKeyDescription.trim() || undefined,
				enabled: newKeyEnabled,
			}));
			toast.success(`API Key "${newKeyName}" created`);
			showCreateKeyModal = false;
			newKeyName = '';
			newKeyDescription = '';
			newKeyEnabled = true;
			await loadApiKeys();
		} catch (e) {
			toast.error(`Failed to create API key: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApiKey(key: Regioned<ApiKey>) {
		if (!key.id || !(await confirmDestructive({ title: 'Delete API Key', message: `Delete key "${key.name}"?` }))) return;
		try {
			await getAPIGatewayClient(key.region).send(new DeleteApiKeyCommand({ apiKey: key.id }));
			toast.success(`API Key "${key.name}" deleted`);
			await loadApiKeys();
		} catch (e) {
			toast.error(`Failed to delete API key: ${e}`);
		}
	}

	async function toggleApiKeyEnabled(key: Regioned<ApiKey>) {
		if (!key.id) return;
		try {
			await getAPIGatewayClient(key.region).send(new UpdateApiKeyCommand({
				apiKey: key.id,
				patchOperations: [{ op: 'replace', path: '/enabled', value: key.enabled ? 'false' : 'true' }],
			}));
			toast.success(`API Key "${key.name}" ${key.enabled ? 'disabled' : 'enabled'}`);
			await loadApiKeys();
		} catch (e) {
			toast.error(`Failed to update API key: ${e}`);
		}
	}

	async function copyToClipboard(text: string, keyId: string) {
		await navigator.clipboard.writeText(text);
		copiedKeyId = keyId;
		setTimeout(() => { copiedKeyId = null; }, 2000);
	}

	async function createUsagePlan() {
		if (!newPlanName.trim()) return;
		creating = true;
		try {
			await apigw().send(new CreateUsagePlanCommand({
				name: newPlanName.trim(),
				description: newPlanDescription.trim() || undefined,
			}));
			toast.success(`Usage Plan "${newPlanName}" created`);
			showCreatePlanModal = false;
			newPlanName = '';
			newPlanDescription = '';
			await loadUsagePlans();
		} catch (e) {
			toast.error(`Failed to create usage plan: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteUsagePlan(plan: Regioned<UsagePlan>) {
		if (!plan.id || !(await confirmDestructive({ title: 'Delete Usage Plan', message: `Delete usage plan "${plan.name}"?` }))) return;
		try {
			await getAPIGatewayClient(plan.region).send(new DeleteUsagePlanCommand({ usagePlanId: plan.id }));
			toast.success(`Usage Plan "${plan.name}" deleted`);
			if (selectedPlan?.id === plan.id) { selectedPlan = null; planKeys = []; }
			await loadUsagePlans();
		} catch (e) {
			toast.error(`Failed to delete usage plan: ${e}`);
		}
	}

	async function selectPlan(plan: Regioned<UsagePlan>) {
		selectedPlan = plan;
		if (plan.id) await loadPlanKeys(plan.id, plan.region);
	}

	async function addKeyToPlan() {
		if (!selectedPlan?.id || !addKeyId.trim()) return;
		try {
			await getAPIGatewayClient(selectedPlan.region).send(new CreateUsagePlanKeyCommand({
				usagePlanId: selectedPlan.id,
				keyId: addKeyId.trim(),
				keyType: addKeyType,
			}));
			toast.success('API key added to usage plan');
			showAddKeyModal = false;
			addKeyId = '';
			await loadPlanKeys(selectedPlan.id, selectedPlan.region);
		} catch (e) {
			toast.error(`Failed to add key to plan: ${e}`);
		}
	}

	async function createDomainName() {
		if (!newDomainName.trim()) return;
		creating = true;
		try {
			await apigw().send(new CreateDomainNameCommand({
				domainName: newDomainName.trim(),
				regionalCertificateArn: newDomainCertArn.trim() || undefined,
				endpointConfiguration: { types: [newDomainEndpointType] },
			}));
			toast.success(`Domain "${newDomainName}" created`);
			showCreateDomainModal = false;
			newDomainName = '';
			newDomainCertArn = '';
			await loadDomainNames();
		} catch (e) {
			toast.error(`Failed to create domain: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteDomainName(domain: Regioned<DomainName>) {
		if (!domain.domainName || !(await confirmDestructive({ title: 'Delete Domain', message: `Delete domain "${domain.domainName}"?` }))) return;
		try {
			await getAPIGatewayClient(domain.region).send(new DeleteDomainNameCommand({ domainName: domain.domainName }));
			toast.success(`Domain "${domain.domainName}" deleted`);
			await loadDomainNames();
		} catch (e) {
			toast.error(`Failed to delete domain: ${e}`);
		}
	}

	// ─── Demo data ───────────────────────────────────────────────────────────────
	async function seedDemoData() {
		creating = true;
		try {
			const apiRes = await apigw().send(new CreateRestApiCommand({
				name: 'petstore-demo',
				description: 'Demo PetStore REST API',
				endpointConfiguration: { types: ['REGIONAL'] },
			}));
			if (apiRes.id) {
				await apigw().send(new CreateDeploymentCommand({ restApiId: apiRes.id, stageName: 'dev', description: 'Demo deployment' }));
				await apigw().send(new CreateDeploymentCommand({ restApiId: apiRes.id, stageName: 'prod' }));
			}
			await apigw().send(new CreateApiKeyCommand({ name: 'demo-key', description: 'Demo API key', enabled: true }));
			await apigw().send(new CreateUsagePlanCommand({ name: 'demo-plan', description: 'Demo usage plan' }));
			await apigw().send(new CreateDomainNameCommand({
				domainName: 'api.example.internal',
				endpointConfiguration: { types: ['REGIONAL'] },
			}));
			toast.success('Demo data seeded');
			await loadApis();
		} catch (e) {
			toast.error(`Demo seed failed: ${e}`);
		} finally {
			creating = false;
		}
	}

	// ─── Tab switching ────────────────────────────────────────────────────────────
	async function switchTopTab(tab: typeof topTab) {
		topTab = tab;
		searchQuery = '';
		selectedApi = null;
		selectedPlan = null;
		planKeys = [];
		if (tab === 'apis') await loadApis();
		else if (tab === 'apikeys') await loadApiKeys();
		else if (tab === 'usageplans') await loadUsagePlans();
		else if (tab === 'domainnames') await loadDomainNames();
	}

	// ─── Helpers ─────────────────────────────────────────────────────────────────
	type ResourceNode = Resource & { depth: number };

	function buildResourceTree(resources: Resource[]): ResourceNode[] {
		const byId: Record<string, ResourceNode> = {};
		for (const r of resources) {
			if (r.id) byId[r.id] = { ...r, depth: 0 };
		}
		const result: ResourceNode[] = [];
		const visited = new Set<string>();

		function visit(id: string, depth: number) {
			if (visited.has(id)) return;
			visited.add(id);
			const node = byId[id];
			if (!node) return;
			node.depth = depth;
			result.push(node);
			for (const child of Object.values(byId)) {
				if (child.parentId === id) visit(child.id!, depth + 1);
			}
		}

		// Start from roots (no parent or parent not in map)
		for (const [id, r] of Object.entries(byId)) {
			if (!r.parentId || !byId[r.parentId]) visit(id, 0);
		}

		return result;
	}

	const resourceTree = $derived(buildResourceTree(apiResources));

	// Only one top-level tab's data is loaded at a time; on a region change
	// reset everything region-scoped and reload whichever tab is active.
	// `topTab` is read via untrack() because switchTopTab() also writes it:
	// without untrack, every tab switch would re-trigger this region effect
	// and double-fetch.
	onRegionChange(() => {
		restApis = [];
		selectedApi = null;
		apiResources = [];
		apiStages = [];
		apiDeployments = [];
		apiAuthorizers = [];
		apiModels = [];
		apiKeys = [];
		usagePlans = [];
		selectedPlan = null;
		planKeys = [];
		domainNames = [];
		const tab = untrack(() => topTab);
		if (tab === 'apis') void loadApis();
		else if (tab === 'apikeys') void loadApiKeys();
		else if (tab === 'usageplans') void loadUsagePlans();
		else if (tab === 'domainnames') void loadDomainNames();
	});
</script>

<div class="space-y-4">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Globe class="h-8 w-8 text-blue-600" />
			<div>
				<h1 class="text-2xl font-bold">API Gateway</h1>
				<p class="text-sm text-muted-foreground">Create, publish and manage REST APIs</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button
				onclick={seedDemoData}
				disabled={creating}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent disabled:opacity-50"
				title="Seed demo data"
			>
				<Database class="h-4 w-4" />
				Demo Data
			</button>
			<button
				onclick={() => switchTopTab(topTab)}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
			>
				<RefreshCw class="h-4 w-4" />
				Refresh
			</button>
		</div>
	</div>

	<!-- Top Tabs -->
	<div class="flex border-b overflow-x-auto">
		<button onclick={() => switchTopTab('apis')} class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'apis' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
			<Globe class="h-4 w-4" />REST APIs
		</button>
		<button onclick={() => switchTopTab('apikeys')} class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'apikeys' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
			<KeyRound class="h-4 w-4" />API Keys
		</button>
		<button onclick={() => switchTopTab('usageplans')} class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'usageplans' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
			<BarChart2 class="h-4 w-4" />Usage Plans
		</button>
		<button onclick={() => switchTopTab('domainnames')} class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'domainnames' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
			<Link class="h-4 w-4" />Domain Names
		</button>
		<button onclick={() => switchTopTab('metrics')} class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'metrics' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
			<BarChart2 class="h-4 w-4" />Metrics
		</button>
		<button onclick={() => switchTopTab('docs')} class="flex items-center gap-1.5 whitespace-nowrap px-4 py-2 text-sm font-medium border-b-2 transition-colors {topTab === 'docs' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
			<BookOpen class="h-4 w-4" />Docs
		</button>
	</div>

	<!-- ═══════════════════════════ REST APIs Tab ═══════════════════════════ -->
	{#if topTab === 'apis'}
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
				onclick={() => (showCreateApiModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create API
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if filteredApis.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Globe class="h-12 w-12 mb-3 opacity-30" />
				<p>No REST APIs found</p>
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
						{#each filteredApis as api (api.region + '::' + api.id)}
							<tr
								class="hover:bg-muted/30 cursor-pointer {selectedApi && selectedApi.id === api.id && selectedApi.region === api.region ? 'bg-muted/50' : ''}"
								onclick={() => loadApiDetail(api)}
							>
								<td class="px-4 py-3 font-medium"><div class="flex items-center gap-2"><span>{api.name}</span><RegionChip region={api.region} /></div></td>
								<td class="px-4 py-3 text-muted-foreground text-xs truncate max-w-[200px]">{api.description ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">{api.endpointConfiguration?.types?.join(', ') ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">{api.createdDate ? new Date(api.createdDate).toLocaleDateString() : '—'}</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => { e.stopPropagation(); loadApiDetail(api); }}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View details"
									><Eye class="h-4 w-4" /></button>
									<button
										onclick={(e) => { e.stopPropagation(); deleteApi(api); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete API"
									><Trash2 class="h-4 w-4" /></button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- API Detail Panel -->
		{#if selectedApi}
			<div class="rounded-lg border p-4 space-y-4">
				<div class="flex items-center justify-between">
					<div>
						<h3 class="font-semibold text-lg flex items-center gap-2"><span>{selectedApi.name}</span><RegionChip region={selectedApi.region} /></h3>
						<p class="text-xs text-muted-foreground font-mono">{selectedApi.id}</p>
						{#if selectedApi.description}
							<p class="text-sm text-muted-foreground">{selectedApi.description}</p>
						{/if}
					</div>
					<div class="flex gap-2">
						<button
							onclick={() => (showDeployModal = true)}
							class="flex items-center gap-1 rounded-md bg-green-600 px-3 py-1.5 text-sm text-white hover:bg-green-700"
						>
							<Rocket class="h-4 w-4" />
							Deploy
						</button>
						<button onclick={() => (selectedApi = null)} class="text-xs text-muted-foreground hover:text-foreground">Close</button>
					</div>
				</div>

				<!-- Detail sub-tabs -->
				<div class="flex border-b gap-1">
					<button onclick={() => { apiDetailTab = 'resources'; }} class="flex items-center gap-1 px-3 py-1.5 text-xs font-medium border-b-2 transition-colors {apiDetailTab === 'resources' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
						<Layers class="h-3.5 w-3.5" />Resources
					</button>
					<button onclick={() => { apiDetailTab = 'stages'; }} class="flex items-center gap-1 px-3 py-1.5 text-xs font-medium border-b-2 transition-colors {apiDetailTab === 'stages' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
						<Rocket class="h-3.5 w-3.5" />Stages
					</button>
					<button onclick={() => { apiDetailTab = 'deployments'; }} class="flex items-center gap-1 px-3 py-1.5 text-xs font-medium border-b-2 transition-colors {apiDetailTab === 'deployments' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
						<ChevronRight class="h-3.5 w-3.5" />Deployments
					</button>
					<button onclick={() => { apiDetailTab = 'authorizers'; }} class="flex items-center gap-1 px-3 py-1.5 text-xs font-medium border-b-2 transition-colors {apiDetailTab === 'authorizers' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
						<Shield class="h-3.5 w-3.5" />Authorizers
					</button>
					<button onclick={() => { apiDetailTab = 'models'; }} class="flex items-center gap-1 px-3 py-1.5 text-xs font-medium border-b-2 transition-colors {apiDetailTab === 'models' ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}">
						<Code2 class="h-3.5 w-3.5" />Models
					</button>
				</div>

				{#if loadingApiDetail}
					<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
				{:else}
					<!-- Resources sub-tab -->
					{#if apiDetailTab === 'resources'}
						{#if resourceTree.length === 0}
							<p class="text-sm text-muted-foreground">No resources</p>
						{:else}
							<div class="divide-y rounded border overflow-hidden max-h-60 overflow-y-auto text-xs">
								{#each resourceTree as resource}
									<div class="px-3 py-2 flex items-center gap-1" style="padding-left: {(resource.depth * 16) + 12}px">
										<ChevronRight class="h-3 w-3 text-muted-foreground shrink-0" />
										<span class="font-mono text-blue-600 dark:text-blue-400">{resource.pathPart || '/'}</span>
										<span class="text-muted-foreground ml-1">({resource.path})</span>
										{#if resource.resourceMethods && Object.keys(resource.resourceMethods).length > 0}
											<div class="ml-auto flex gap-1">
												{#each Object.keys(resource.resourceMethods) as m}
													<span class="rounded px-1 bg-blue-100 dark:bg-blue-900 text-blue-700 dark:text-blue-300 font-medium">{m}</span>
												{/each}
											</div>
										{/if}
									</div>
								{/each}
							</div>
						{/if}

					<!-- Stages sub-tab -->
					{:else if apiDetailTab === 'stages'}
						{#if apiStages.length === 0}
							<p class="text-sm text-muted-foreground">No stages — click Deploy to create one</p>
						{:else}
							<div class="divide-y rounded border overflow-hidden">
								{#each apiStages as stage}
									<div class="px-3 py-2.5">
										<div class="flex items-center justify-between">
											<span class="font-medium text-sm">{stage.stageName}</span>
											<span class="text-xs text-muted-foreground">
												{stage.lastUpdatedDate ? new Date(stage.lastUpdatedDate).toLocaleDateString() : '—'}
											</span>
										</div>
										{#if (stage as any).invokeUrl}
											<div class="mt-1 flex items-center gap-2">
												<span class="text-xs font-mono text-green-600 dark:text-green-400 truncate">{(stage as any).invokeUrl}</span>
												<button
													onclick={() => copyToClipboard((stage as any).invokeUrl, stage.stageName ?? '')}
													class="shrink-0 rounded p-0.5 text-muted-foreground hover:text-foreground"
													title="Copy invoke URL"
												>
													{#if copiedKeyId === stage.stageName}
														<Check class="h-3 w-3 text-green-500" />
													{:else}
														<Copy class="h-3 w-3" />
													{/if}
												</button>
											</div>
										{/if}
										{#if stage.description}
											<p class="text-xs text-muted-foreground">{stage.description}</p>
										{/if}
									</div>
								{/each}
							</div>
						{/if}

					<!-- Deployments sub-tab -->
					{:else if apiDetailTab === 'deployments'}
						{#if apiDeployments.length === 0}
							<p class="text-sm text-muted-foreground">No deployments</p>
						{:else}
							<div class="divide-y rounded border overflow-hidden">
								{#each apiDeployments as depl}
									<div class="px-3 py-2 text-xs">
										<span class="font-mono text-muted-foreground">{depl.id}</span>
										{#if depl.description}
											<span class="ml-2">{depl.description}</span>
										{/if}
										<span class="ml-2 text-muted-foreground">
											{depl.createdDate ? new Date(depl.createdDate).toLocaleDateString() : ''}
										</span>
									</div>
								{/each}
							</div>
						{/if}

					<!-- Authorizers sub-tab -->
					{:else if apiDetailTab === 'authorizers'}
						{#if apiAuthorizers.length === 0}
							<p class="text-sm text-muted-foreground">No authorizers</p>
						{:else}
							<div class="divide-y rounded border overflow-hidden">
								{#each apiAuthorizers as auth}
									<div class="px-3 py-2 text-xs">
										<div class="flex items-center justify-between">
											<span class="font-medium">{auth.name}</span>
											<span class="rounded px-1 bg-purple-100 dark:bg-purple-900 text-purple-700 dark:text-purple-300">{auth.type}</span>
										</div>
										{#if auth.authorizerUri}
											<div class="text-muted-foreground font-mono truncate">{auth.authorizerUri}</div>
										{/if}
									</div>
								{/each}
							</div>
						{/if}

					<!-- Models sub-tab -->
					{:else if apiDetailTab === 'models'}
						{#if apiModels.length === 0}
							<p class="text-sm text-muted-foreground">No models</p>
						{:else}
							<div class="divide-y rounded border overflow-hidden">
								{#each apiModels as model}
									<div class="px-3 py-2 text-xs">
										<span class="font-medium">{model.name}</span>
										<span class="ml-2 text-muted-foreground">{model.contentType}</span>
										{#if model.description}
											<p class="text-muted-foreground">{model.description}</p>
										{/if}
									</div>
								{/each}
							</div>
						{/if}
					{/if}
				{/if}
			</div>
		{/if}
	{/if}

	<!-- ═══════════════════════════ API Keys Tab ═══════════════════════════ -->
	{#if topTab === 'apikeys'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search API keys..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateKeyModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Key
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
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
							<th class="px-4 py-3 text-left font-medium">Value</th>
							<th class="px-4 py-3 text-left font-medium">Enabled</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredKeys as key (key.region + '::' + key.id)}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium"><div class="flex items-center gap-2"><span>{key.name}</span><RegionChip region={key.region} /></div></td>
								<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{key.id}</td>
								<td class="px-4 py-3">
									{#if key.value}
										<div class="flex items-center gap-1">
											<span class="font-mono text-xs text-muted-foreground">{key.value.slice(0, 8)}…</span>
											<button
												onclick={() => copyToClipboard(key.value!, key.id!)}
												class="rounded p-0.5 text-muted-foreground hover:text-foreground"
												title="Copy API key value"
											>
												{#if copiedKeyId === key.id}
													<Check class="h-3.5 w-3.5 text-green-500" />
												{:else}
													<Copy class="h-3.5 w-3.5" />
												{/if}
											</button>
										</div>
									{:else}
										<span class="text-xs text-muted-foreground">—</span>
									{/if}
								</td>
								<td class="px-4 py-3">
									<button
										onclick={() => toggleApiKeyEnabled(key)}
										class="text-sm"
										title={key.enabled ? 'Disable key' : 'Enable key'}
									>
										{#if key.enabled}
											<ToggleRight class="h-5 w-5 text-green-500" />
										{:else}
											<ToggleLeft class="h-5 w-5 text-muted-foreground" />
										{/if}
									</button>
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{key.createdDate ? new Date(key.createdDate).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteApiKey(key)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete key"
									><Trash2 class="h-4 w-4" /></button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ═══════════════════════════ Usage Plans Tab ═══════════════════════════ -->
	{#if topTab === 'usageplans'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search usage plans..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreatePlanModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Plan
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if filteredPlans.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<BarChart2 class="h-12 w-12 mb-3 opacity-30" />
				<p>No usage plans found</p>
			</div>
		{:else}
			<div class="grid gap-4 {selectedPlan ? 'grid-cols-2' : 'grid-cols-1'}">
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Name</th>
								<th class="px-4 py-3 text-left font-medium">Description</th>
								<th class="px-4 py-3 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each filteredPlans as plan (plan.region + '::' + plan.id)}
								<tr
									class="hover:bg-muted/30 cursor-pointer {selectedPlan && selectedPlan.id === plan.id && selectedPlan.region === plan.region ? 'bg-muted/50' : ''}"
									onclick={() => selectPlan(plan)}
								>
									<td class="px-4 py-3 font-medium"><div class="flex items-center gap-2"><span>{plan.name}</span><RegionChip region={plan.region} /></div></td>
									<td class="px-4 py-3 text-muted-foreground text-xs">{plan.description ?? '—'}</td>
									<td class="px-4 py-3 text-right">
										<button
											onclick={(e) => { e.stopPropagation(); deleteUsagePlan(plan); }}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											title="Delete plan"
										><Trash2 class="h-4 w-4" /></button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>

				<!-- Plan Keys Panel -->
				{#if selectedPlan}
					<div class="rounded-lg border p-4 space-y-3">
						<div class="flex items-center justify-between">
							<h4 class="font-medium flex items-center gap-2">Keys for: {selectedPlan.name}<RegionChip region={selectedPlan.region} /></h4>
							<button
								onclick={() => (showAddKeyModal = true)}
								class="flex items-center gap-1 rounded-md bg-primary px-2 py-1 text-xs text-primary-foreground"
							>
								<Plus class="h-3.5 w-3.5" />
								Add Key
							</button>
						</div>
						{#if loadingPlanKeys}
							<RefreshCw class="h-4 w-4 animate-spin" />
						{:else if planKeys.length === 0}
							<p class="text-sm text-muted-foreground">No keys associated</p>
						{:else}
							<div class="divide-y rounded border overflow-hidden">
								{#each planKeys as k}
									<div class="px-3 py-2 text-xs flex items-center justify-between">
										<div>
											<span class="font-medium">{k.name}</span>
											<span class="ml-2 text-muted-foreground font-mono">{k.id}</span>
										</div>
										<span class="text-muted-foreground">{k.type}</span>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{/if}
			</div>
		{/if}
	{/if}

	<!-- ═══════════════════════════ Domain Names Tab ═══════════════════════════ -->
	{#if topTab === 'domainnames'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search domain names..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateDomainModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Domain
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" /></div>
		{:else if filteredDomains.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Link class="h-12 w-12 mb-3 opacity-30" />
				<p>No custom domain names</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Domain</th>
							<th class="px-4 py-3 text-left font-medium">Endpoint</th>
							<th class="px-4 py-3 text-left font-medium">Regional Domain</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredDomains as domain (domain.region + '::' + domain.domainName)}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium"><div class="flex items-center gap-2"><span>{domain.domainName}</span><RegionChip region={domain.region} /></div></td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{domain.endpointConfiguration?.types?.join(', ') ?? '—'}
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs font-mono">
									{domain.regionalDomainName ?? '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteDomainName(domain)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete domain"
									><Trash2 class="h-4 w-4" /></button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ═══════════════════════════ Metrics Tab ═══════════════════════════ -->
	{#if topTab === 'metrics'}
		<div class="space-y-6">
			<h2 class="text-lg font-semibold">Service Metrics</h2>
			<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
				<div class="p-5 rounded-xl border bg-card shadow-sm">
					<div class="text-xs font-medium text-muted-foreground uppercase tracking-wider">REST APIs</div>
					<div class="text-3xl font-bold text-blue-600 dark:text-blue-400 mt-2">{restApis.length}</div>
				</div>
				<div class="p-5 rounded-xl border bg-card shadow-sm">
					<div class="text-xs font-medium text-muted-foreground uppercase tracking-wider">API Keys</div>
					<div class="text-3xl font-bold text-green-600 dark:text-green-400 mt-2">{apiKeys.length}</div>
					<div class="text-xs text-muted-foreground mt-1">
						{apiKeys.filter((k) => k.enabled).length} enabled
					</div>
				</div>
				<div class="p-5 rounded-xl border bg-card shadow-sm">
					<div class="text-xs font-medium text-muted-foreground uppercase tracking-wider">Usage Plans</div>
					<div class="text-3xl font-bold text-purple-600 dark:text-purple-400 mt-2">{usagePlans.length}</div>
				</div>
				<div class="p-5 rounded-xl border bg-card shadow-sm">
					<div class="text-xs font-medium text-muted-foreground uppercase tracking-wider">Custom Domains</div>
					<div class="text-3xl font-bold text-orange-600 dark:text-orange-400 mt-2">{domainNames.length}</div>
				</div>
			</div>
			<div class="p-4 rounded-lg border bg-muted/30">
				<h3 class="text-sm font-medium mb-2">Supported Operations ({supportedOperations.length})</h3>
				<div class="flex flex-wrap gap-1.5">
					{#each supportedOperations as op}
						<span class="rounded-full bg-primary/10 px-2 py-0.5 text-xs font-medium text-primary">{op}</span>
					{/each}
				</div>
			</div>
		</div>
	{/if}

	<!-- ═══════════════════════════ Docs Tab ═══════════════════════════ -->
	{#if topTab === 'docs'}
		<div class="space-y-4 max-w-3xl">
			<h2 class="text-lg font-semibold">API Gateway — Supported Operations</h2>
			<p class="text-sm text-muted-foreground">
				Gopherstack implements {supportedOperations.length} API Gateway operations. These can be used
				with the AWS SDK v2 client pointed at your gopherstack endpoint.
			</p>
			{#each [
				{ group: 'REST APIs', ops: ['CreateRestApi', 'DeleteRestApi', 'GetRestApi', 'GetRestApis', 'UpdateRestApi'] },
				{ group: 'Resources', ops: ['CreateResource', 'DeleteResource', 'GetResource', 'GetResources', 'UpdateResource'] },
				{ group: 'Methods', ops: ['PutMethod', 'GetMethod', 'DeleteMethod', 'PutMethodResponse', 'GetMethodResponse', 'DeleteMethodResponse'] },
				{ group: 'Integrations', ops: ['PutIntegration', 'GetIntegration', 'DeleteIntegration', 'PutIntegrationResponse', 'GetIntegrationResponse', 'DeleteIntegrationResponse'] },
				{ group: 'Deployments & Stages', ops: ['CreateDeployment', 'GetDeployment', 'GetDeployments', 'DeleteDeployment', 'UpdateDeployment', 'GetStage', 'GetStages', 'CreateStage', 'DeleteStage', 'UpdateStage', 'FlushStageCache', 'FlushStageAuthorizersCache'] },
				{ group: 'Authorizers', ops: ['CreateAuthorizer', 'GetAuthorizer', 'GetAuthorizers', 'UpdateAuthorizer', 'DeleteAuthorizer'] },
				{ group: 'API Keys', ops: ['CreateApiKey', 'GetApiKey', 'GetApiKeys', 'UpdateApiKey', 'DeleteApiKey'] },
				{ group: 'Domain Names', ops: ['CreateDomainName', 'GetDomainName', 'GetDomainNames', 'DeleteDomainName'] },
				{ group: 'Usage Plans', ops: ['CreateUsagePlan', 'GetUsagePlan', 'GetUsagePlans', 'DeleteUsagePlan', 'CreateUsagePlanKey', 'GetUsagePlanKey', 'GetUsagePlanKeys', 'DeleteUsagePlanKey'] },
				{ group: 'Models', ops: ['CreateModel', 'GetModel', 'GetModels', 'UpdateModel', 'DeleteModel'] },
				{ group: 'Documentation', ops: ['CreateDocumentationPart', 'GetDocumentationPart', 'GetDocumentationParts', 'DeleteDocumentationPart', 'CreateDocumentationVersion', 'GetDocumentationVersion', 'GetDocumentationVersions', 'DeleteDocumentationVersion'] },
				{ group: 'Account & Tags', ops: ['GetAccount', 'GetTags', 'TagResource', 'UntagResource'] },
				{ group: 'Test Invoke', ops: ['TestInvokeMethod'] },
			] as section}
				<div class="space-y-2">
					<h3 class="text-sm font-semibold text-muted-foreground uppercase tracking-wider">{section.group}</h3>
					<div class="flex flex-wrap gap-1.5">
						{#each section.ops as op}
							<span class="rounded-full bg-secondary px-2.5 py-0.5 text-xs font-medium">{op}</span>
						{/each}
					</div>
				</div>
			{/each}
		</div>
	{/if}
</div>

<!-- ═══════════════════════════ Modals ═══════════════════════════ -->

<!-- Create API Modal -->
{#if showCreateApiModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold">Create REST API</h2>
				<WriteRegionHint />
			</div>
			<div class="space-y-3">
				<div>
					<label for="api-name" class="block text-sm font-medium mb-1">API Name *</label>
					<input id="api-name" type="text" bind:value={newApiName} placeholder="my-api"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="api-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="api-desc" type="text" bind:value={newApiDescription} placeholder="My REST API"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="endpoint-type" class="block text-sm font-medium mb-1">Endpoint Type</label>
					<select id="endpoint-type" bind:value={newApiEndpointType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="REGIONAL">Regional</option>
						<option value="EDGE">Edge-optimized</option>
						<option value="PRIVATE">Private</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateApiModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createApi} disabled={creating || !newApiName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create API'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Deploy Modal -->
{#if showDeployModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-sm rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Deploy API</h2>
			<div class="space-y-3">
				<div>
					<label for="stage-name" class="block text-sm font-medium mb-1">Stage Name *</label>
					<input id="stage-name" type="text" bind:value={deployStageName} placeholder="v1"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="deploy-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="deploy-desc" type="text" bind:value={deployDescription} placeholder="Initial deployment"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showDeployModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={deployApi} disabled={deploying || !deployStageName.trim()}
					class="rounded-md bg-green-600 px-4 py-2 text-sm text-white hover:bg-green-700 disabled:opacity-50">
					{deploying ? 'Deploying…' : 'Deploy'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create API Key Modal -->
{#if showCreateKeyModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-sm rounded-lg bg-background p-6 shadow-xl">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold">Create API Key</h2>
				<WriteRegionHint />
			</div>
			<div class="space-y-3">
				<div>
					<label for="key-name" class="block text-sm font-medium mb-1">Key Name *</label>
					<input id="key-name" type="text" bind:value={newKeyName} placeholder="my-api-key"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="key-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="key-desc" type="text" bind:value={newKeyDescription} placeholder="Description"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<label class="flex items-center gap-2 text-sm">
					<input type="checkbox" bind:checked={newKeyEnabled} class="rounded" />
					Enabled
				</label>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateKeyModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createApiKey} disabled={creating || !newKeyName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create Key'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Usage Plan Modal -->
{#if showCreatePlanModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-sm rounded-lg bg-background p-6 shadow-xl">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold">Create Usage Plan</h2>
				<WriteRegionHint />
			</div>
			<div class="space-y-3">
				<div>
					<label for="plan-name" class="block text-sm font-medium mb-1">Plan Name *</label>
					<input id="plan-name" type="text" bind:value={newPlanName} placeholder="my-plan"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="plan-desc" class="block text-sm font-medium mb-1">Description</label>
					<input id="plan-desc" type="text" bind:value={newPlanDescription} placeholder="Description"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreatePlanModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createUsagePlan} disabled={creating || !newPlanName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create Plan'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Add Key to Plan Modal -->
{#if showAddKeyModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-sm rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add API Key to Plan</h2>
			<div class="space-y-3">
				<div>
					<label for="key-id" class="block text-sm font-medium mb-1">API Key ID *</label>
					<input id="key-id" type="text" bind:value={addKeyId} placeholder="abc123xyz"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="key-type" class="block text-sm font-medium mb-1">Key Type</label>
					<select id="key-type" bind:value={addKeyType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="API_KEY">API_KEY</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showAddKeyModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={addKeyToPlan} disabled={!addKeyId.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					Add Key
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Domain Name Modal -->
{#if showCreateDomainModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-sm rounded-lg bg-background p-6 shadow-xl">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold">Create Custom Domain</h2>
				<WriteRegionHint />
			</div>
			<div class="space-y-3">
				<div>
					<label for="domain-name" class="block text-sm font-medium mb-1">Domain Name *</label>
					<input id="domain-name" type="text" bind:value={newDomainName} placeholder="api.example.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="cert-arn" class="block text-sm font-medium mb-1">Certificate ARN (optional)</label>
					<input id="cert-arn" type="text" bind:value={newDomainCertArn} placeholder="arn:aws:acm:..."
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="domain-endpoint" class="block text-sm font-medium mb-1">Endpoint Type</label>
					<select id="domain-endpoint" bind:value={newDomainEndpointType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="REGIONAL">Regional</option>
						<option value="EDGE">Edge-optimized</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button onclick={() => (showCreateDomainModal = false)} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
				<button onclick={createDomainName} disabled={creating || !newDomainName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
					{creating ? 'Creating…' : 'Create Domain'}
				</button>
			</div>
		</div>
	</div>
{/if}
