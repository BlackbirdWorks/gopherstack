<script lang="ts">
	import { onMount } from 'svelte';
	import { getVerifiedPermissionsClient } from '$lib/aws-client';
	import {
		ListPolicyStoresCommand,
		ListPoliciesCommand,
		ListIdentitySourcesCommand,
		ListPolicyTemplatesCommand,
		GetSchemaCommand,
		PutSchemaCommand,
		GetPolicyCommand,
		CreatePolicyCommand,
		UpdatePolicyCommand,
		DeletePolicyCommand,
		CreatePolicyStoreCommand,
		UpdatePolicyStoreCommand,
		CreatePolicyTemplateCommand,
		DeletePolicyTemplateCommand,
		IsAuthorizedCommand,
		CreateIdentitySourceCommand,
		DeleteIdentitySourceCommand,
		type PolicyStoreItem,
		type PolicyItem,
		type IdentitySourceItem,
		type PolicyTemplateItem
	} from '@aws-sdk/client-verifiedpermissions';
	import { toast } from 'svelte-sonner';
	import {
		Shield,
		RefreshCw,
		Search,
		Lock,
		FileText,
		Users,
		Code,
		Play,
		Database,
		ChevronDown,
		ChevronRight,
		Pencil,
		Trash2,
		Plus,
		X,
		Check,
		Tag,
		Layout
	} from 'lucide-svelte';

	const vp = getVerifiedPermissionsClient();

	let loading = $state(false);
	let activeTab = $state<'stores' | 'policies' | 'templates' | 'identity' | 'schema' | 'authz'>(
		'stores'
	);
	let searchQuery = $state('');
	let stores = $state<PolicyStoreItem[]>([]);
	let policies = $state<PolicyItem[]>([]);
	let identitySources = $state<IdentitySourceItem[]>([]);
	let policyTemplates = $state<PolicyTemplateItem[]>([]);
	let selectedStoreId = $state<string | null>(null);
	let selectedStore = $state<PolicyStoreItem | null>(null);
	let expandedPolicyId = $state<string | null>(null);
	let policyStatements = $state<Record<string, string>>({});
	let deletingPolicyId = $state<string | null>(null);
	let deletingTemplateId = $state<string | null>(null);
	let deletingIdentitySourceId = $state<string | null>(null);

	// Schema state
	let schemaText = $state('');
	let schemaNamespaces = $state<string[]>([]);
	let schemaLoading = $state(false);
	let schemaSaving = $state(false);
	let schemaEditing = $state(false);
	let schemaDraft = $state('');
	let schemaCached = $state<{ storeId: string; text: string } | null>(null);

	// Cedar editor state (create/edit policy)
	let policyEditorOpen = $state(false);
	let policyEditorMode = $state<'create' | 'edit' | 'create-template-linked'>('create');
	let policyEditorId = $state('');
	let policyEditorStatement = $state('');
	let policyEditorSaving = $state(false);
	let policyEditorDescription = $state('');
	// Template-linked fields
	let policyEditorTemplateId = $state('');
	let policyEditorPrincipalType = $state('');
	let policyEditorPrincipalId = $state('');
	let policyEditorResourceType = $state('');
	let policyEditorResourceId = $state('');

	// Policy template editor
	let templateEditorOpen = $state(false);
	let templateEditorMode = $state<'create' | 'edit'>('create');
	let templateEditorId = $state('');
	let templateEditorStatement = $state('');
	let templateEditorDescription = $state('');
	let templateEditorSaving = $state(false);

	// Policy store create/edit
	let storeEditorOpen = $state(false);
	let storeEditorMode = $state<'create' | 'edit'>('create');
	let storeEditorId = $state('');
	let storeEditorDescription = $state('');
	let storeEditorValidationMode = $state<'OFF' | 'STRICT'>('OFF');
	let storeEditorDeletionProtection = $state<'DISABLED' | 'ENABLED'>('DISABLED');
	let storeEditorSaving = $state(false);

	// Identity source create
	let identitySourceEditorOpen = $state(false);
	let identitySourceEditorType = $state<'cognito' | 'oidc'>('cognito');
	let identitySourceEditorPrincipalType = $state('');
	let identitySourceEditorUserPoolArn = $state('');
	let identitySourceEditorClientIds = $state('');
	let identitySourceEditorGroupEntityType = $state('');
	let identitySourceEditorIssuer = $state('');
	let identitySourceEditorEntityIdPrefix = $state('');
	let identitySourceEditorSaving = $state(false);

	// Authorization tester state
	let authzPrincipalType = $state('');
	let authzPrincipalId = $state('');
	let authzActionType = $state('');
	let authzActionId = $state('');
	let authzResourceType = $state('');
	let authzResourceId = $state('');
	let authzContext = $state('{}');
	let authzResult = $state<{
		decision: string;
		determiningPolicies: string[];
		errors: string[];
	} | null>(null);
	let authzLoading = $state(false);

	const filteredStores = $derived(
		stores.filter(
			(s) =>
				(s.policyStoreId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.arn ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.description ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredPolicies = $derived(
		policies.filter(
			(p) =>
				(p.policyId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(policyStatements[p.policyId ?? ''] ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredTemplates = $derived(
		policyTemplates.filter(
			(pt) =>
				(pt.policyTemplateId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(pt.description ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadData() {
		loading = true;
		try {
			const resp = await vp.send(new ListPolicyStoresCommand({}));
			stores = resp.policyStores ?? [];
		} catch (e) {
			toast.error('Failed to load Verified Permissions data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadPolicies(storeId: string) {
		selectedStoreId = storeId;
		selectedStore = stores.find((s) => s.policyStoreId === storeId) ?? null;
		schemaText = '';
		schemaCached = null;
		try {
			const [polResp, idResp, tmplResp] = await Promise.all([
				vp.send(new ListPoliciesCommand({ policyStoreId: storeId })),
				vp.send(new ListIdentitySourcesCommand({ policyStoreId: storeId })),
				vp.send(new ListPolicyTemplatesCommand({ policyStoreId: storeId }))
			]);
			policies = polResp.policies ?? [];
			identitySources = idResp.identitySources ?? [];
			policyTemplates = tmplResp.policyTemplates ?? [];
			activeTab = 'policies';
		} catch (e) {
			toast.error('Failed to load policies: ' + String(e));
		}
	}

	async function loadPolicyStatement(policyId: string) {
		if (!selectedStoreId || policyStatements[policyId] !== undefined) return;
		try {
			const resp = await vp.send(
				new GetPolicyCommand({ policyStoreId: selectedStoreId, policyId })
			);
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const stmt = (resp as any).definition?.static?.statement ?? '';
			policyStatements = { ...policyStatements, [policyId]: stmt };
		} catch {
			policyStatements = { ...policyStatements, [policyId]: '' };
		}
	}

	function togglePolicyExpand(policyId: string) {
		if (expandedPolicyId === policyId) {
			expandedPolicyId = null;
		} else {
			expandedPolicyId = policyId;
			loadPolicyStatement(policyId);
		}
	}

	function openCreatePolicy() {
		policyEditorMode = 'create';
		policyEditorId = '';
		policyEditorStatement = `permit(\n  principal,\n  action,\n  resource\n);`;
		policyEditorDescription = '';
		policyEditorOpen = true;
	}

	function openCreateTemplateLinkedPolicy() {
		policyEditorMode = 'create-template-linked';
		policyEditorTemplateId = '';
		policyEditorPrincipalType = '';
		policyEditorPrincipalId = '';
		policyEditorResourceType = '';
		policyEditorResourceId = '';
		policyEditorOpen = true;
	}

	function openEditPolicy(policy: PolicyItem) {
		policyEditorMode = 'edit';
		policyEditorId = policy.policyId ?? '';
		policyEditorStatement = policyStatements[policy.policyId ?? ''] ?? '';
		policyEditorDescription = '';
		policyEditorOpen = true;
	}

	async function savePolicy() {
		if (!selectedStoreId) return;
		policyEditorSaving = true;
		try {
			if (policyEditorMode === 'create') {
				await vp.send(
					new CreatePolicyCommand({
						policyStoreId: selectedStoreId,
						definition: {
							static: {
								statement: policyEditorStatement,
								description: policyEditorDescription || undefined
							}
						}
					})
				);
				toast.success('Policy created');
			} else if (policyEditorMode === 'create-template-linked') {
				await vp.send(
					new CreatePolicyCommand({
						policyStoreId: selectedStoreId,
						definition: {
							templateLinked: {
								policyTemplateId: policyEditorTemplateId,
								principal: policyEditorPrincipalType
									? { entityType: policyEditorPrincipalType, entityId: policyEditorPrincipalId }
									: undefined,
								resource: policyEditorResourceType
									? { entityType: policyEditorResourceType, entityId: policyEditorResourceId }
									: undefined
							}
						}
					})
				);
				toast.success('Template-linked policy created');
			} else {
				await vp.send(
					new UpdatePolicyCommand({
						policyStoreId: selectedStoreId,
						policyId: policyEditorId,
						definition: {
							static: { statement: policyEditorStatement }
						}
					})
				);
				policyStatements = { ...policyStatements, [policyEditorId]: policyEditorStatement };
				toast.success('Policy updated');
			}
			policyEditorOpen = false;
			await loadPolicies(selectedStoreId);
		} catch (e) {
			toast.error('Failed to save policy: ' + String(e));
		} finally {
			policyEditorSaving = false;
		}
	}

	async function deletePolicy(policyId: string) {
		if (!selectedStoreId) return;
		deletingPolicyId = null;
		try {
			await vp.send(new DeletePolicyCommand({ policyStoreId: selectedStoreId, policyId }));
			toast.success('Policy deleted');
			await loadPolicies(selectedStoreId);
		} catch (e) {
			toast.error('Failed to delete policy: ' + String(e));
		}
	}

	// Template operations
	function openCreateTemplate() {
		templateEditorMode = 'create';
		templateEditorId = '';
		templateEditorStatement = `permit(\n  principal == ?principal,\n  action,\n  resource\n);`;
		templateEditorDescription = '';
		templateEditorOpen = true;
	}

	function openEditTemplate(tmpl: PolicyTemplateItem) {
		templateEditorMode = 'edit';
		templateEditorId = tmpl.policyTemplateId ?? '';
		templateEditorStatement = ((tmpl as unknown as Record<string, unknown>).statement as string) ?? '';
		templateEditorDescription = tmpl.description ?? '';
		templateEditorOpen = true;
	}

	async function saveTemplate() {
		if (!selectedStoreId) return;
		templateEditorSaving = true;
		try {
			if (templateEditorMode === 'create') {
				await vp.send(
					new CreatePolicyTemplateCommand({
						policyStoreId: selectedStoreId,
						statement: templateEditorStatement,
						description: templateEditorDescription || undefined
					})
				);
				toast.success('Policy template created');
			}
			templateEditorOpen = false;
			await loadPolicies(selectedStoreId);
		} catch (e) {
			toast.error('Failed to save template: ' + String(e));
		} finally {
			templateEditorSaving = false;
		}
	}

	async function deleteTemplate(templateId: string) {
		if (!selectedStoreId) return;
		deletingTemplateId = null;
		try {
			await vp.send(
				new DeletePolicyTemplateCommand({
					policyStoreId: selectedStoreId,
					policyTemplateId: templateId
				})
			);
			toast.success('Template deleted');
			await loadPolicies(selectedStoreId);
		} catch (e) {
			toast.error('Failed to delete template: ' + String(e));
		}
	}

	// Store create/edit
	function openCreateStore() {
		storeEditorMode = 'create';
		storeEditorId = '';
		storeEditorDescription = '';
		storeEditorValidationMode = 'OFF';
		storeEditorDeletionProtection = 'DISABLED';
		storeEditorOpen = true;
	}

	function openEditStore(store: PolicyStoreItem) {
		type StoreExtra = { description?: string; validationSettings?: { mode?: string }; deletionProtection?: string };
		const s = store as unknown as StoreExtra;
		storeEditorMode = 'edit';
		storeEditorId = store.policyStoreId ?? '';
		storeEditorDescription = s.description ?? '';
		storeEditorValidationMode = (s.validationSettings?.mode as 'OFF' | 'STRICT') ?? 'OFF';
		storeEditorDeletionProtection = (s.deletionProtection as 'DISABLED' | 'ENABLED') ?? 'DISABLED';
		storeEditorOpen = true;
	}

	async function saveStore() {
		storeEditorSaving = true;
		try {
			if (storeEditorMode === 'create') {
				await vp.send(
					new CreatePolicyStoreCommand({
						description: storeEditorDescription || undefined,
						validationSettings: { mode: storeEditorValidationMode as 'OFF' | 'STRICT' },
						// eslint-disable-next-line @typescript-eslint/no-explicit-any
						...(storeEditorDeletionProtection !== 'DISABLED' && {
							deletionProtection: storeEditorDeletionProtection
						})
					})
				);
				toast.success('Policy store created');
			} else {
				await vp.send(
					new UpdatePolicyStoreCommand({
						policyStoreId: storeEditorId,
						description: storeEditorDescription,
						validationSettings: { mode: storeEditorValidationMode as 'OFF' | 'STRICT' }
					})
				);
				toast.success('Policy store updated');
			}
			storeEditorOpen = false;
			await loadData();
		} catch (e) {
			toast.error('Failed to save store: ' + String(e));
		} finally {
			storeEditorSaving = false;
		}
	}

	// Identity source create
	async function saveIdentitySource() {
		if (!selectedStoreId) return;
		identitySourceEditorSaving = true;
		try {
			if (identitySourceEditorType === 'cognito') {
				const clientIds = identitySourceEditorClientIds
					.split(',')
					.map((s) => s.trim())
					.filter(Boolean);
				await vp.send(
					new CreateIdentitySourceCommand({
						policyStoreId: selectedStoreId,
						principalEntityType: identitySourceEditorPrincipalType || undefined,
						configuration: {
							cognitoUserPoolConfiguration: {
								userPoolArn: identitySourceEditorUserPoolArn,
								clientIds: clientIds.length > 0 ? clientIds : undefined,
								groupConfiguration: identitySourceEditorGroupEntityType
									? { groupEntityType: identitySourceEditorGroupEntityType }
									: undefined
							}
						}
					})
				);
			} else {
				await vp.send(
					new CreateIdentitySourceCommand({
						policyStoreId: selectedStoreId,
						principalEntityType: identitySourceEditorPrincipalType || undefined,
						configuration: {
							openIdConnectConfiguration: {
								issuer: identitySourceEditorIssuer,
								entityIdPrefix: identitySourceEditorEntityIdPrefix || undefined,
								tokenSelection: {
									identityTokenOnly: { principalIdClaim: 'sub' }
								}
							}
						}
					})
				);
			}
			toast.success('Identity source created');
			identitySourceEditorOpen = false;
			await loadPolicies(selectedStoreId);
		} catch (e) {
			toast.error('Failed to create identity source: ' + String(e));
		} finally {
			identitySourceEditorSaving = false;
		}
	}

	async function deleteIdentitySource(identitySourceId: string) {
		if (!selectedStoreId) return;
		deletingIdentitySourceId = null;
		try {
			await vp.send(
				new DeleteIdentitySourceCommand({
					policyStoreId: selectedStoreId,
					identitySourceId
				})
			);
			toast.success('Identity source deleted');
			await loadPolicies(selectedStoreId);
		} catch (e) {
			toast.error('Failed to delete identity source: ' + String(e));
		}
	}

	async function loadSchema() {
		if (!selectedStoreId) return;
		if (schemaCached?.storeId === selectedStoreId) {
			schemaText = schemaCached.text;
			return;
		}
		schemaLoading = true;
		try {
			const resp = await vp.send(new GetSchemaCommand({ policyStoreId: selectedStoreId }));
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const raw = (resp as any).schema ?? (resp as any).definition?.cedarJson ?? '';
			schemaText = raw;
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			schemaNamespaces = (resp as any).namespaces ?? [];
			schemaCached = { storeId: selectedStoreId, text: raw };
		} catch (e) {
			const msg = String(e);
			if (msg.includes('ResourceNotFoundException') || msg.includes('not found')) {
				schemaText = '';
				schemaNamespaces = [];
				schemaCached = { storeId: selectedStoreId, text: '' };
			} else {
				toast.error('Failed to load schema: ' + msg);
			}
		} finally {
			schemaLoading = false;
		}
	}

	async function openSchemaTab() {
		activeTab = 'schema';
		schemaEditing = false;
		await loadSchema();
	}

	function startEditSchema() {
		schemaDraft = schemaText;
		schemaEditing = true;
	}

	function cancelEditSchema() {
		schemaEditing = false;
		schemaDraft = '';
	}

	async function saveSchema() {
		if (!selectedStoreId) return;
		schemaSaving = true;
		try {
			const resp = await vp.send(
				new PutSchemaCommand({
					policyStoreId: selectedStoreId,
					definition: { cedarJson: schemaDraft }
				})
			);
			schemaText = schemaDraft;
			schemaCached = { storeId: selectedStoreId, text: schemaDraft };
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			schemaNamespaces = (resp as any).namespaces ?? [];
			schemaEditing = false;
			toast.success('Schema saved');
		} catch (e) {
			toast.error('Failed to save schema: ' + String(e));
		} finally {
			schemaSaving = false;
		}
	}

	async function runAuthz() {
		if (!selectedStoreId) return;
		authzLoading = true;
		authzResult = null;
		try {
			const resp = await vp.send(
				new IsAuthorizedCommand({
					policyStoreId: selectedStoreId,
					principal: authzPrincipalType
						? { entityType: authzPrincipalType, entityId: authzPrincipalId }
						: undefined,
					action: authzActionType
						? { actionType: authzActionType, actionId: authzActionId }
						: undefined,
					resource: authzResourceType
						? { entityType: authzResourceType, entityId: authzResourceId }
						: undefined
				})
			);
			authzResult = {
				decision: resp.decision ?? 'DENY',
				determiningPolicies: (resp.determiningPolicies ?? []).map((dp) => dp.policyId ?? ''),
				errors: (resp.errors ?? []).map((e) => e.errorDescription ?? '')
			};
		} catch (e) {
			toast.error('Authorization check failed: ' + String(e));
		} finally {
			authzLoading = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">
					Amazon Verified Permissions
				</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">
					Fine-grained authorization and permissions management
				</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={openCreateStore}
				class="flex items-center gap-1 px-3 py-2 rounded-lg bg-green-600 text-white text-sm hover:bg-green-700"
			>
				<Plus class="w-4 h-4" /> New Store
			</button>
			<button
				onclick={loadData}
				title="Refresh"
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Lock class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{stores.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Policy Stores</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<FileText class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{policies.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Policies</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Users class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{identitySources.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Identity Sources</p>
			</div>
		</div>
		<div
			class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3"
		>
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Code class="w-5 h-5 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{policyTemplates.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Templates</p>
			</div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<div class="flex gap-2 flex-wrap">
				{#each ([['stores', 'Policy Stores', Lock], ['policies', 'Policies', FileText], ['templates', 'Templates', Layout], ['identity', 'Identity Sources', Users], ['schema', 'Schema', Database], ['authz', 'Authz Tester', Play]] as const) as [tab, label, Icon]}
					<button
						onclick={() => {
							if (tab === 'schema') {
								openSchemaTab();
							} else {
								activeTab = tab;
								searchQuery = '';
							}
						}}
						class="px-4 py-2 rounded-lg text-sm font-medium flex items-center gap-1.5 {activeTab === tab ? 'bg-green-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}"
					>
						<Icon class="w-3.5 h-3.5" />{label}
					</button>
				{/each}
			</div>
			<div class="flex items-center gap-2">
				{#if activeTab === 'policies' && selectedStoreId}
					<button
						onclick={openCreateTemplateLinkedPolicy}
						class="flex items-center gap-1 px-3 py-2 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700"
					>
						<Plus class="w-4 h-4" /> Template-Linked
					</button>
					<button
						onclick={openCreatePolicy}
						class="flex items-center gap-1 px-3 py-2 rounded-lg bg-green-600 text-white text-sm hover:bg-green-700"
					>
						<Plus class="w-4 h-4" /> New Policy
					</button>
				{/if}
				{#if activeTab === 'templates' && selectedStoreId}
					<button
						onclick={openCreateTemplate}
						class="flex items-center gap-1 px-3 py-2 rounded-lg bg-green-600 text-white text-sm hover:bg-green-700"
					>
						<Plus class="w-4 h-4" /> New Template
					</button>
				{/if}
				{#if activeTab === 'identity' && selectedStoreId}
					<button
						onclick={() => (identitySourceEditorOpen = true)}
						class="flex items-center gap-1 px-3 py-2 rounded-lg bg-green-600 text-white text-sm hover:bg-green-700"
					>
						<Plus class="w-4 h-4" /> New Identity Source
					</button>
				{/if}
				{#if activeTab !== 'schema' && activeTab !== 'authz'}
					<div class="relative">
						<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
						<input
							bind:value={searchQuery}
							placeholder="Search..."
							class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64"
						/>
					</div>
				{/if}
			</div>
		</div>

		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'stores'}
				{#if filteredStores.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No policy stores found.
						<button onclick={openCreateStore} class="ml-2 text-green-500 hover:underline"
							>Create one now</button
						>
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredStores as store}
							{@const storeX = store as unknown as { description?: string; validationSettings?: { mode?: string }; deletionProtection?: string }}
							<div
								class="rounded-lg bg-gray-50 dark:bg-slate-700/50 {selectedStoreId === store.policyStoreId ? 'ring-2 ring-green-500' : ''}"
							>
								<div class="flex items-center justify-between p-3">
									<button
										onclick={() => loadPolicies(store.policyStoreId ?? '')}
										class="flex items-center gap-3 flex-1 text-left"
									>
										<Lock class="w-5 h-5 text-green-500 shrink-0" />
										<div>
											<p class="font-medium text-gray-900 dark:text-white">
												{store.policyStoreId}
											</p>
											<p class="text-xs text-gray-500 dark:text-gray-400">{store.arn}</p>
											{#if storeX.description}
												<p class="text-xs text-gray-400 italic">
													{storeX.description}
												</p>
											{/if}
											<div class="flex gap-2 mt-1">
												{#if storeX.validationSettings}
													<span
														class="text-xs px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300"
													>
														Validation: {storeX.validationSettings.mode ?? 'OFF'}
													</span>
												{/if}
												{#if storeX.deletionProtection === 'ENABLED'}
													<span
														class="text-xs px-1.5 py-0.5 rounded bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300"
													>
														Deletion Protected
													</span>
												{/if}
											</div>
										</div>
									</button>
									<div class="flex items-center gap-1">
										<button
											onclick={() => openEditStore(store)}
											title="Edit"
											class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500"
										>
											<Pencil class="w-4 h-4" />
										</button>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'policies'}
				{#if !selectedStoreId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						Select a policy store to view policies.
					</div>
				{:else if filteredPolicies.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No policies found.
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPolicies as policy}
							<div class="rounded-lg bg-gray-50 dark:bg-slate-700/50 overflow-hidden">
								<div class="flex items-center justify-between p-3">
									<button
										class="flex items-center gap-3 flex-1 text-left"
										onclick={() => togglePolicyExpand(policy.policyId ?? '')}
									>
										{#if expandedPolicyId === policy.policyId}
											<ChevronDown class="w-4 h-4 text-gray-400 shrink-0" />
										{:else}
											<ChevronRight class="w-4 h-4 text-gray-400 shrink-0" />
										{/if}
										<FileText class="w-5 h-5 text-blue-500 shrink-0" />
										<div>
											<p class="font-medium text-gray-900 dark:text-white">{policy.policyId}</p>
											<p class="text-xs text-gray-500 dark:text-gray-400">{policy.policyType}</p>
										</div>
									</button>
									<div class="flex items-center gap-1">
										{#if policy.policyType === 'STATIC'}
											<button
												onclick={() => openEditPolicy(policy)}
												title="Edit"
												class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500 dark:text-gray-400"
											>
												<Pencil class="w-4 h-4" />
											</button>
										{/if}
										{#if deletingPolicyId === policy.policyId}
											<button
												onclick={() => deletePolicy(policy.policyId ?? '')}
												class="px-2 py-1 rounded bg-red-600 text-white text-xs">Confirm</button
											>
											<button
												onclick={() => {
													deletingPolicyId = null;
												}}
												class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500"
												><X class="w-4 h-4" /></button
											>
										{:else}
											<button
												onclick={() => {
													deletingPolicyId = policy.policyId ?? '';
												}}
												title="Delete"
												class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500"
											>
												<Trash2 class="w-4 h-4" />
											</button>
										{/if}
									</div>
								</div>
								{#if expandedPolicyId === policy.policyId}
									<div class="px-3 pb-3 border-t border-gray-200 dark:border-slate-600 pt-2">
										{#if policyStatements[policy.policyId ?? ''] !== undefined}
											<pre
												class="text-xs font-mono bg-gray-900 text-green-300 p-3 rounded overflow-x-auto">{policyStatements[policy.policyId ?? ''] || '(no statement — template-linked policy)'}</pre>
										{:else}
											<div class="text-xs text-gray-400 py-2">Loading...</div>
										{/if}
									</div>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'templates'}
				{#if !selectedStoreId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						Select a policy store to view templates.
					</div>
				{:else if filteredTemplates.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No policy templates found.
						<button onclick={openCreateTemplate} class="ml-2 text-green-500 hover:underline"
							>Create one now</button
						>
					</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTemplates as tmpl}
							<div class="rounded-lg bg-gray-50 dark:bg-slate-700/50 overflow-hidden">
								<div class="flex items-center justify-between p-3">
									<div class="flex items-center gap-3 flex-1">
										<Layout class="w-5 h-5 text-orange-500 shrink-0" />
										<div>
											<p class="font-medium text-gray-900 dark:text-white">
												{tmpl.policyTemplateId}
											</p>
											{#if tmpl.description}
												<p class="text-xs text-gray-500 dark:text-gray-400">{tmpl.description}</p>
											{/if}
										</div>
									</div>
									<div class="flex items-center gap-1">
										{#if deletingTemplateId === tmpl.policyTemplateId}
											<button
												onclick={() => deleteTemplate(tmpl.policyTemplateId ?? '')}
												class="px-2 py-1 rounded bg-red-600 text-white text-xs">Confirm</button
											>
											<button
												onclick={() => {
													deletingTemplateId = null;
												}}
												class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500"
												><X class="w-4 h-4" /></button
											>
										{:else}
											<button
												onclick={() => {
													deletingTemplateId = tmpl.policyTemplateId ?? '';
												}}
												title="Delete"
												class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500"
											>
												<Trash2 class="w-4 h-4" />
											</button>
										{/if}
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'identity'}
				{#if !selectedStoreId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						Select a policy store to view identity sources.
					</div>
				{:else if identitySources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						No identity sources found.
						<button
							onclick={() => (identitySourceEditorOpen = true)}
							class="ml-2 text-green-500 hover:underline">Create one now</button
						>
					</div>
				{:else}
					<div class="space-y-2">
						{#each identitySources as src}
							<div class="rounded-lg bg-gray-50 dark:bg-slate-700/50 overflow-hidden">
								<div class="flex items-center justify-between p-3">
									<div class="flex items-center gap-3 flex-1">
										<Users class="w-5 h-5 text-purple-500" />
										<div>
											<p class="font-medium text-gray-900 dark:text-white">
												{src.identitySourceId}
											</p>
											<p class="text-xs text-gray-500 dark:text-gray-400">
												{src.principalEntityType}
											</p>
											{#if src.configuration}
												{#if 'cognitoUserPoolConfiguration' in src.configuration}
													<p class="text-xs text-gray-400">Cognito: {src.configuration.cognitoUserPoolConfiguration?.userPoolArn}</p>
												{:else if 'openIdConnectConfiguration' in src.configuration}
													<p class="text-xs text-gray-400">OIDC: {src.configuration.openIdConnectConfiguration?.issuer}</p>
												{/if}
											{/if}
										</div>
									</div>
									<div class="flex items-center gap-1">
										{#if deletingIdentitySourceId === src.identitySourceId}
											<button
												onclick={() => deleteIdentitySource(src.identitySourceId ?? '')}
												class="px-2 py-1 rounded bg-red-600 text-white text-xs">Confirm</button
											>
											<button
												onclick={() => {
													deletingIdentitySourceId = null;
												}}
												class="p-1.5 rounded hover:bg-gray-200 dark:hover:bg-slate-600 text-gray-500"
												><X class="w-4 h-4" /></button
											>
										{:else}
											<button
												onclick={() => {
													deletingIdentitySourceId = src.identitySourceId ?? '';
												}}
												title="Delete"
												class="p-1.5 rounded hover:bg-red-100 dark:hover:bg-red-900/30 text-red-500"
											>
												<Trash2 class="w-4 h-4" />
											</button>
										{/if}
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'schema'}
				{#if !selectedStoreId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						Select a policy store first to view or edit its schema.
					</div>
				{:else if schemaLoading}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading schema...</div>
				{:else}
					<div class="space-y-3">
						<div class="flex items-center justify-between">
							<div>
								<p class="text-sm text-gray-600 dark:text-gray-400">
									Cedar JSON schema for policy store <span
										class="font-mono text-xs bg-gray-100 dark:bg-slate-700 px-1 py-0.5 rounded"
										>{selectedStoreId}</span
									>
								</p>
								{#if schemaNamespaces.length > 0}
									<div class="flex flex-wrap gap-1 mt-1">
										{#each schemaNamespaces as ns}
											<span
												class="text-xs px-1.5 py-0.5 rounded bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300"
												>{ns}</span
											>
										{/each}
									</div>
								{/if}
							</div>
							{#if !schemaEditing}
								<button
									onclick={startEditSchema}
									class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700"
								>
									<Pencil class="w-3.5 h-3.5" /> Edit Schema
								</button>
							{:else}
								<div class="flex items-center gap-2">
									<button
										onclick={cancelEditSchema}
										class="flex items-center gap-1 px-3 py-1.5 rounded-lg border border-gray-300 dark:border-slate-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
									>
										<X class="w-3.5 h-3.5" /> Cancel
									</button>
									<button
										onclick={saveSchema}
										disabled={schemaSaving}
										class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-green-600 text-white text-sm hover:bg-green-700 disabled:opacity-50"
									>
										<Check class="w-3.5 h-3.5" />
										{schemaSaving ? 'Saving...' : 'Save Schema'}
									</button>
								</div>
							{/if}
						</div>
						{#if schemaEditing}
							<textarea
								bind:value={schemaDraft}
								rows={20}
								placeholder='&#123;"namespace": &#123;"entityTypes": &#123;&#125;, "actions": &#123;&#125;&#125;&#125;'
								class="w-full font-mono text-xs bg-gray-900 text-green-300 p-3 rounded border border-gray-700 focus:outline-none focus:ring-2 focus:ring-green-500 resize-y"
							></textarea>
							<p class="text-xs text-gray-500 dark:text-gray-400">
								Enter a valid Cedar JSON schema. Changes are applied immediately on save.
							</p>
						{:else if schemaText}
							<pre
								class="text-xs font-mono bg-gray-900 text-green-300 p-3 rounded overflow-x-auto whitespace-pre-wrap break-all">{schemaText}</pre>
						{:else}
							<div class="text-center py-8 text-gray-500 dark:text-gray-400">
								No schema defined for this policy store.
								<button onclick={startEditSchema} class="ml-2 text-blue-500 hover:underline"
									>Add one now</button
								>
							</div>
						{/if}
					</div>
				{/if}
			{:else if activeTab === 'authz'}
				{#if !selectedStoreId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						Select a policy store first to test authorization.
					</div>
				{:else}
					<div class="space-y-6">
						<div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
							<div class="space-y-2">
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Principal</h3>
								<input
									bind:value={authzPrincipalType}
									placeholder="Entity type (e.g. User)"
									class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
								<input
									bind:value={authzPrincipalId}
									placeholder="Entity ID (e.g. alice)"
									class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
							</div>
							<div class="space-y-2">
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Action</h3>
								<input
									bind:value={authzActionType}
									placeholder="Action type (e.g. Action)"
									class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
								<input
									bind:value={authzActionId}
									placeholder="Action ID (e.g. read)"
									class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
							</div>
							<div class="space-y-2">
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Resource</h3>
								<input
									bind:value={authzResourceType}
									placeholder="Entity type (e.g. Document)"
									class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
								<input
									bind:value={authzResourceId}
									placeholder="Entity ID (e.g. doc-123)"
									class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
							</div>
						</div>

						<div class="space-y-2">
							<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">
								Context (JSON, optional)
							</h3>
							<textarea
								bind:value={authzContext}
								rows={3}
								placeholder="&#123;&#125;"
								class="w-full font-mono text-xs bg-gray-900 text-green-300 p-3 rounded border border-gray-700 focus:outline-none focus:ring-2 focus:ring-green-500 resize-y"
							></textarea>
						</div>

						<div class="flex items-center gap-3">
							<button
								onclick={runAuthz}
								disabled={authzLoading}
								class="flex items-center gap-2 px-4 py-2 rounded-lg bg-green-600 text-white font-medium text-sm hover:bg-green-700 disabled:opacity-50"
							>
								<Play class="w-4 h-4" />
								{authzLoading ? 'Checking...' : 'Check Authorization'}
							</button>
							<p class="text-xs text-gray-500 dark:text-gray-400">
								Against store: <span class="font-mono">{selectedStoreId}</span>
							</p>
						</div>

						{#if authzResult}
							<div
								class="rounded-lg border {authzResult.decision === 'ALLOW' ? 'border-green-300 dark:border-green-700 bg-green-50 dark:bg-green-900/20' : 'border-red-300 dark:border-red-700 bg-red-50 dark:bg-red-900/20'} p-4 space-y-2"
							>
								<div class="flex items-center gap-2">
									<span
										class="text-lg font-bold {authzResult.decision === 'ALLOW' ? 'text-green-700 dark:text-green-400' : 'text-red-700 dark:text-red-400'}"
										>{authzResult.decision}</span
									>
								</div>
								{#if authzResult.determiningPolicies.length > 0}
									<div>
										<p class="text-xs font-semibold text-gray-600 dark:text-gray-400 mb-1">
											Determining Policies:
										</p>
										<ul class="space-y-1">
											{#each authzResult.determiningPolicies as pid}
												<li
													class="text-xs font-mono bg-gray-100 dark:bg-slate-700 px-2 py-1 rounded"
												>
													{pid}
												</li>
											{/each}
										</ul>
									</div>
								{/if}
								{#if authzResult.errors.length > 0}
									<div>
										<p class="text-xs font-semibold text-red-600 dark:text-red-400 mb-1">Errors:</p>
										<ul class="space-y-1">
											{#each authzResult.errors as err}
												<li class="text-xs text-red-600 dark:text-red-400">{err}</li>
											{/each}
										</ul>
									</div>
								{/if}
							</div>
						{/if}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Cedar Policy Editor Modal -->
{#if policyEditorOpen}
	<div class="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
		<div
			class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-2xl flex flex-col max-h-[90vh]"
		>
			<div
				class="flex items-center justify-between p-4 border-b border-gray-200 dark:border-slate-700"
			>
				<div class="flex items-center gap-2">
					<Code class="w-5 h-5 text-green-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
						{#if policyEditorMode === 'create'}New Cedar Policy
						{:else if policyEditorMode === 'create-template-linked'}New Template-Linked Policy
						{:else}Edit Cedar Policy{/if}
					</h2>
				</div>
				<button
					onclick={() => {
						policyEditorOpen = false;
					}}
					class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
				>
					<X class="w-5 h-5" />
				</button>
			</div>
			<div class="flex-1 overflow-auto p-4 space-y-3">
				{#if policyEditorMode === 'create-template-linked'}
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Template ID</label
						>
						<select
							bind:value={policyEditorTemplateId}
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						>
							<option value="">— select template —</option>
							{#each policyTemplates as tmpl}
								<option value={tmpl.policyTemplateId}>{tmpl.policyTemplateId}</option>
							{/each}
						</select>
					</div>
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
								>Principal Entity Type</label
							>
							<input
								bind:value={policyEditorPrincipalType}
								placeholder="User"
								class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
						</div>
						<div>
							<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
								>Principal Entity ID</label
							>
							<input
								bind:value={policyEditorPrincipalId}
								placeholder="alice"
								class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
						</div>
						<div>
							<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
								>Resource Entity Type</label
							>
							<input
								bind:value={policyEditorResourceType}
								placeholder="Document"
								class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
						</div>
						<div>
							<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
								>Resource Entity ID</label
							>
							<input
								bind:value={policyEditorResourceId}
								placeholder="doc-123"
								class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
						</div>
					</div>
				{:else if policyEditorMode === 'create'}
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Description (optional)</label
						>
						<input
							bind:value={policyEditorDescription}
							placeholder="Policy description..."
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Cedar Statement</label
						>
						<textarea
							bind:value={policyEditorStatement}
							rows={16}
							class="w-full font-mono text-xs bg-gray-900 text-green-300 p-3 rounded border border-gray-700 focus:outline-none focus:ring-2 focus:ring-green-500 resize-y"
						></textarea>
					</div>
				{:else}
					<p class="text-sm text-gray-500 dark:text-gray-400">
						Policy ID: <span class="font-mono text-xs">{policyEditorId}</span>
					</p>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Cedar Statement</label
						>
						<textarea
							bind:value={policyEditorStatement}
							rows={16}
							class="w-full font-mono text-xs bg-gray-900 text-green-300 p-3 rounded border border-gray-700 focus:outline-none focus:ring-2 focus:ring-green-500 resize-y"
						></textarea>
					</div>
				{/if}
			</div>
			<div
				class="flex items-center justify-end gap-2 p-4 border-t border-gray-200 dark:border-slate-700"
			>
				<button
					onclick={() => {
						policyEditorOpen = false;
					}}
					class="px-4 py-2 rounded-lg border border-gray-300 dark:border-slate-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
				>
					Cancel
				</button>
				<button
					onclick={savePolicy}
					disabled={policyEditorSaving ||
						(policyEditorMode !== 'create-template-linked' && !policyEditorStatement.trim())}
					class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50"
				>
					{policyEditorSaving
						? 'Saving...'
						: policyEditorMode === 'create' || policyEditorMode === 'create-template-linked'
							? 'Create Policy'
							: 'Update Policy'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Policy Template Editor Modal -->
{#if templateEditorOpen}
	<div class="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
		<div
			class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-2xl flex flex-col max-h-[90vh]"
		>
			<div
				class="flex items-center justify-between p-4 border-b border-gray-200 dark:border-slate-700"
			>
				<div class="flex items-center gap-2">
					<Layout class="w-5 h-5 text-orange-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
						{templateEditorMode === 'create' ? 'New Policy Template' : 'Edit Policy Template'}
					</h2>
				</div>
				<button
					onclick={() => {
						templateEditorOpen = false;
					}}
					class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
				>
					<X class="w-5 h-5" />
				</button>
			</div>
			<div class="flex-1 overflow-auto p-4 space-y-3">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Description (optional)</label
					>
					<input
						bind:value={templateEditorDescription}
						placeholder="Template description..."
						class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Cedar Template Statement</label
					>
					<textarea
						bind:value={templateEditorStatement}
						rows={16}
						class="w-full font-mono text-xs bg-gray-900 text-green-300 p-3 rounded border border-gray-700 focus:outline-none focus:ring-2 focus:ring-green-500 resize-y"
					></textarea>
				</div>
			</div>
			<div
				class="flex items-center justify-end gap-2 p-4 border-t border-gray-200 dark:border-slate-700"
			>
				<button
					onclick={() => {
						templateEditorOpen = false;
					}}
					class="px-4 py-2 rounded-lg border border-gray-300 dark:border-slate-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
				>
					Cancel
				</button>
				<button
					onclick={saveTemplate}
					disabled={templateEditorSaving || !templateEditorStatement.trim()}
					class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50"
				>
					{templateEditorSaving ? 'Saving...' : 'Create Template'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Policy Store Create/Edit Modal -->
{#if storeEditorOpen}
	<div class="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-lg flex flex-col">
			<div
				class="flex items-center justify-between p-4 border-b border-gray-200 dark:border-slate-700"
			>
				<div class="flex items-center gap-2">
					<Lock class="w-5 h-5 text-green-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
						{storeEditorMode === 'create' ? 'New Policy Store' : 'Edit Policy Store'}
					</h2>
				</div>
				<button
					onclick={() => {
						storeEditorOpen = false;
					}}
					class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
				>
					<X class="w-5 h-5" />
				</button>
			</div>
			<div class="p-4 space-y-4">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Description (optional)</label
					>
					<input
						bind:value={storeEditorDescription}
						placeholder="Policy store description..."
						class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Validation Mode</label
					>
					<select
						bind:value={storeEditorValidationMode}
						class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="OFF">OFF — No schema validation</option>
						<option value="STRICT">STRICT — Schema-aware policy validation</option>
					</select>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Deletion Protection</label
					>
					<select
						bind:value={storeEditorDeletionProtection}
						class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="DISABLED">DISABLED</option>
						<option value="ENABLED">ENABLED — Prevents deletion</option>
					</select>
				</div>
			</div>
			<div
				class="flex items-center justify-end gap-2 p-4 border-t border-gray-200 dark:border-slate-700"
			>
				<button
					onclick={() => {
						storeEditorOpen = false;
					}}
					class="px-4 py-2 rounded-lg border border-gray-300 dark:border-slate-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
				>
					Cancel
				</button>
				<button
					onclick={saveStore}
					disabled={storeEditorSaving}
					class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50"
				>
					{storeEditorSaving
						? 'Saving...'
						: storeEditorMode === 'create'
							? 'Create Store'
							: 'Update Store'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Identity Source Create Modal -->
{#if identitySourceEditorOpen}
	<div class="fixed inset-0 bg-black/60 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-lg flex flex-col">
			<div
				class="flex items-center justify-between p-4 border-b border-gray-200 dark:border-slate-700"
			>
				<div class="flex items-center gap-2">
					<Tag class="w-5 h-5 text-purple-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">New Identity Source</h2>
				</div>
				<button
					onclick={() => {
						identitySourceEditorOpen = false;
					}}
					class="text-gray-400 hover:text-gray-600 dark:hover:text-gray-200"
				>
					<X class="w-5 h-5" />
				</button>
			</div>
			<div class="p-4 space-y-4">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Provider Type</label
					>
					<div class="flex gap-3">
						<label class="flex items-center gap-2 cursor-pointer">
							<input
								type="radio"
								bind:group={identitySourceEditorType}
								value="cognito"
								class="text-green-600"
							/>
							<span class="text-sm text-gray-700 dark:text-gray-300">Cognito User Pool</span>
						</label>
						<label class="flex items-center gap-2 cursor-pointer">
							<input
								type="radio"
								bind:group={identitySourceEditorType}
								value="oidc"
								class="text-green-600"
							/>
							<span class="text-sm text-gray-700 dark:text-gray-300">OpenID Connect</span>
						</label>
					</div>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
						>Principal Entity Type</label
					>
					<input
						bind:value={identitySourceEditorPrincipalType}
						placeholder="User"
						class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				{#if identitySourceEditorType === 'cognito'}
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>User Pool ARN</label
						>
						<input
							bind:value={identitySourceEditorUserPoolArn}
							placeholder="arn:aws:cognito-idp:us-east-1:123456789012:userpool/..."
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Client IDs (comma-separated, optional)</label
						>
						<input
							bind:value={identitySourceEditorClientIds}
							placeholder="client-id-1, client-id-2"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Group Entity Type (optional)</label
						>
						<input
							bind:value={identitySourceEditorGroupEntityType}
							placeholder="UserGroup"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				{:else}
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Issuer URL</label
						>
						<input
							bind:value={identitySourceEditorIssuer}
							placeholder="https://auth.example.com"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
							>Entity ID Prefix (optional)</label
						>
						<input
							bind:value={identitySourceEditorEntityIdPrefix}
							placeholder="https://auth.example.com/users/"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				{/if}
			</div>
			<div
				class="flex items-center justify-end gap-2 p-4 border-t border-gray-200 dark:border-slate-700"
			>
				<button
					onclick={() => {
						identitySourceEditorOpen = false;
					}}
					class="px-4 py-2 rounded-lg border border-gray-300 dark:border-slate-600 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700"
				>
					Cancel
				</button>
				<button
					onclick={saveIdentitySource}
					disabled={identitySourceEditorSaving}
					class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50"
				>
					{identitySourceEditorSaving ? 'Creating...' : 'Create Identity Source'}
				</button>
			</div>
		</div>
	</div>
{/if}
