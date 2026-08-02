<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getLakeFormationClient } from '$lib/aws-client';
	import {
		ListResourcesCommand,
		RegisterResourceCommand,
		DeregisterResourceCommand,
		ListLFTagsCommand,
		CreateLFTagCommand,
		DeleteLFTagCommand,
		UpdateLFTagCommand,
		ListPermissionsCommand,
		GrantPermissionsCommand,
		RevokePermissionsCommand,
		ListTransactionsCommand,
		StartTransactionCommand,
		CommitTransactionCommand,
		CancelTransactionCommand,
		ListDataCellsFilterCommand,
		CreateDataCellsFilterCommand,
		DeleteDataCellsFilterCommand,
		ListLFTagExpressionsCommand,
		CreateLFTagExpressionCommand,
		DeleteLFTagExpressionCommand,
		type ResourceInfo,
		type LFTagPair,
		type PrincipalResourcePermissions,
		type TransactionDescription,
		type DataCellsFilter,
		type LFTagExpression,
		type Resource,
		Permission,
		TransactionStatusFilter,
	} from '@aws-sdk/client-lakeformation';
	import { toast } from 'svelte-sonner';
	import { Database, RefreshCw, Plus, Trash2, Tag, Shield, ArrowLeftRight, Copy, Filter, BookOpen, Layers } from 'lucide-svelte';

	const lf = regionalClient(getLakeFormationClient);

	type Tab = 'resources' | 'lftags' | 'permissions' | 'transactions' | 'datafilters' | 'expressions';
	let activeTab = $state<Tab>('resources');

	// --- Shared confirm dialog ---
	let confirmMsg = $state('');
	let confirmAction = $state<(() => void) | null>(null);

	function showConfirm(message: string, action: () => void) {
		confirmMsg = message;
		confirmAction = () => action();
	}

	function doConfirm() {
		if (confirmAction) confirmAction();
		confirmAction = null;
		confirmMsg = '';
	}

	function cancelConfirm() {
		confirmAction = null;
		confirmMsg = '';
	}

	// --- Copy to clipboard ---
	function copyToClipboard(text: string) {
		navigator.clipboard.writeText(text).then(() => {
			toast.success('Copied to clipboard');
		}).catch(() => {
			toast.error('Failed to copy');
		});
	}

	// --- Spinner snippet ---
	const spinnerSvg = `<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-teal-600" viewBox="0 0 100 101" fill="none"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" /></svg>`;

	// =========== RESOURCES ===========
	let loadingResources = $state(false);
	let resources = $state<ResourceInfo[]>([]);
	let showRegisterModal = $state(false);
	let registering = $state(false);
	let newResourceArn = $state('');
	let newRoleArn = $state('');

	async function loadResources() {
		loadingResources = true;
		try {
			const res = await lf().send(new ListResourcesCommand({}));
			resources = res.ResourceInfoList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load resources: ${(err as Error).message}`);
		} finally {
			loadingResources = false;
		}
	}

	async function registerResource() {
		const arn = newResourceArn.trim();
		const role = newRoleArn.trim();
		if (!arn || !role) return;
		registering = true;
		try {
			await lf().send(new RegisterResourceCommand({ ResourceArn: arn, RoleArn: role }));
			toast.success('Resource registered');
			showRegisterModal = false;
			newResourceArn = '';
			newRoleArn = '';
			await loadResources();
		} catch (err: unknown) {
			toast.error(`Failed to register: ${(err as Error).message}`);
		} finally {
			registering = false;
		}
	}

	function confirmDeregister(arn: string) {
		showConfirm(`Deregister resource "${arn}"? This cannot be undone.`, async () => {
			try {
				await lf().send(new DeregisterResourceCommand({ ResourceArn: arn }));
				toast.success('Resource deregistered');
				await loadResources();
			} catch (err: unknown) {
				toast.error(`Failed to deregister: ${(err as Error).message}`);
			}
		});
	}

	// =========== LF TAGS ===========
	let loadingTags = $state(false);
	let lfTags = $state<LFTagPair[]>([]);
	let tagSearch = $state('');
	let showCreateTagModal = $state(false);
	let creatingTag = $state(false);
	let newTagKey = $state('');
	let newTagValues = $state('');
	let showEditTagModal = $state(false);
	let editTagKey = $state('');
	let editTagAddValues = $state('');
	let editTagRemoveValues = $state('');
	let savingTag = $state(false);

	let filteredTags = $derived(
		tagSearch.trim()
			? lfTags.filter(t => t.TagKey?.toLowerCase().includes(tagSearch.toLowerCase()))
			: lfTags
	);

	async function loadLFTags() {
		loadingTags = true;
		try {
			const res = await lf().send(new ListLFTagsCommand({}));
			lfTags = (res.LFTags ?? []) as LFTagPair[];
		} catch (err: unknown) {
			toast.error(`Failed to load LF tags: ${(err as Error).message}`);
		} finally {
			loadingTags = false;
		}
	}

	async function createLFTag() {
		if (!newTagKey.trim() || !newTagValues.trim()) return;
		creatingTag = true;
		try {
			const values = newTagValues.split(',').map(v => v.trim()).filter(Boolean);
			await lf().send(new CreateLFTagCommand({ TagKey: newTagKey.trim(), TagValues: values }));
			toast.success('LF tag created');
			showCreateTagModal = false;
			newTagKey = '';
			newTagValues = '';
			await loadLFTags();
		} catch (err: unknown) {
			toast.error(`Failed to create tag: ${(err as Error).message}`);
		} finally {
			creatingTag = false;
		}
	}

	function openEditTag(tagKey: string) {
		editTagKey = tagKey;
		editTagAddValues = '';
		editTagRemoveValues = '';
		showEditTagModal = true;
	}

	async function updateLFTag() {
		if (!editTagKey) return;
		savingTag = true;
		try {
			const toAdd = editTagAddValues.split(',').map(v => v.trim()).filter(Boolean);
			const toRemove = editTagRemoveValues.split(',').map(v => v.trim()).filter(Boolean);
			await lf().send(new UpdateLFTagCommand({
				TagKey: editTagKey,
				TagValuesToAdd: toAdd.length > 0 ? toAdd : undefined,
				TagValuesToDelete: toRemove.length > 0 ? toRemove : undefined,
			}));
			toast.success('LF tag updated');
			showEditTagModal = false;
			await loadLFTags();
		} catch (err: unknown) {
			toast.error(`Failed to update tag: ${(err as Error).message}`);
		} finally {
			savingTag = false;
		}
	}

	function confirmDeleteTag(tagKey: string) {
		showConfirm(`Delete LF tag "${tagKey}"? All resources tagged with this key will lose the tag.`, async () => {
			try {
				await lf().send(new DeleteLFTagCommand({ TagKey: tagKey }));
				toast.success('LF tag deleted');
				await loadLFTags();
			} catch (err: unknown) {
				toast.error(`Failed to delete tag: ${(err as Error).message}`);
			}
		});
	}

	// =========== PERMISSIONS ===========
	let loadingPerms = $state(false);
	let permissions = $state<PrincipalResourcePermissions[]>([]);
	let showGrantModal = $state(false);
	let granting = $state(false);
	let grantPrincipal = $state('');
	let grantResourceType = $state<'catalog' | 'database' | 'table' | 'datalocation'>('database');
	let grantResourceDb = $state('');
	let grantResourceTable = $state('');
	let grantResourceArn = $state('');
	let grantPermList = $state('SELECT');
	let grantWithOption = $state('');
	let permPrincipalFilter = $state('');

	let filteredPerms = $derived(
		permPrincipalFilter.trim()
			? permissions.filter(p => p.Principal?.DataLakePrincipalIdentifier?.toLowerCase().includes(permPrincipalFilter.toLowerCase()))
			: permissions
	);

	async function loadPermissions() {
		loadingPerms = true;
		try {
			const res = await lf().send(new ListPermissionsCommand({}));
			permissions = res.PrincipalResourcePermissions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load permissions: ${(err as Error).message}`);
		} finally {
			loadingPerms = false;
		}
	}

	async function grantPermission() {
		if (!grantPrincipal.trim()) return;
		granting = true;
		try {
			let resource: Resource;
			switch (grantResourceType) {
				case 'catalog':
					resource = { Catalog: {} };
					break;
				case 'database':
					if (!grantResourceDb.trim()) { toast.error('Database name required'); return; }
					resource = { Database: { Name: grantResourceDb.trim() } };
					break;
				case 'table':
					if (!grantResourceDb.trim() || !grantResourceTable.trim()) { toast.error('Database and table name required'); return; }
					resource = { Table: { DatabaseName: grantResourceDb.trim(), Name: grantResourceTable.trim() } };
					break;
				case 'datalocation':
					if (!grantResourceArn.trim()) { toast.error('Resource ARN required'); return; }
					resource = { DataLocation: { ResourceArn: grantResourceArn.trim() } };
					break;
			}
			const perms = grantPermList.split(',').map(p => p.trim()).filter(Boolean) as Permission[];
			const withOption = grantWithOption.split(',').map(p => p.trim()).filter(Boolean) as Permission[];
			await lf().send(new GrantPermissionsCommand({
				Principal: { DataLakePrincipalIdentifier: grantPrincipal.trim() },
				Resource: resource,
				Permissions: perms,
				PermissionsWithGrantOption: withOption.length > 0 ? withOption : undefined,
			}));
			toast.success('Permission granted');
			showGrantModal = false;
			grantPrincipal = '';
			grantResourceDb = '';
			grantResourceTable = '';
			grantResourceArn = '';
			grantPermList = 'SELECT';
			grantWithOption = '';
			await loadPermissions();
		} catch (err: unknown) {
			toast.error(`Failed to grant permission: ${(err as Error).message}`);
		} finally {
			granting = false;
		}
	}

	function confirmRevoke(entry: PrincipalResourcePermissions) {
		const who = entry.Principal?.DataLakePrincipalIdentifier ?? 'principal';
		showConfirm(`Revoke permissions for "${who}"?`, async () => {
			try {
				await lf().send(new RevokePermissionsCommand({
					Principal: entry.Principal,
					Resource: entry.Resource,
					Permissions: entry.Permissions,
				}));
				toast.success('Permission revoked');
				await loadPermissions();
			} catch (err: unknown) {
				toast.error(`Failed to revoke: ${(err as Error).message}`);
			}
		});
	}

	function describeResource(resource: Resource | undefined): string {
		if (!resource) return '—';
		if (resource.Catalog !== undefined) return 'Catalog';
		if (resource.Database) return `DB: ${resource.Database.Name}`;
		if (resource.Table) return `Table: ${resource.Table.DatabaseName}.${resource.Table.Name}`;
		if (resource.DataLocation) return resource.DataLocation.ResourceArn ?? '—';
		return '—';
	}

	// =========== TRANSACTIONS ===========
	let loadingTxns = $state(false);
	let transactions = $state<TransactionDescription[]>([]);
	let txnStatusFilter = $state('');

	let filteredTxns = $derived(
		txnStatusFilter
			? transactions.filter(t => t.TransactionStatus === txnStatusFilter)
			: transactions
	);

	async function loadTransactions() {
		loadingTxns = true;
		try {
			const res = await lf().send(new ListTransactionsCommand({ StatusFilter: (txnStatusFilter || undefined) as TransactionStatusFilter | undefined }));
			transactions = res.Transactions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load transactions: ${(err as Error).message}`);
		} finally {
			loadingTxns = false;
		}
	}

	async function startTransaction() {
		try {
			const res = await lf().send(new StartTransactionCommand({}));
			toast.success(`Started: ${res.TransactionId}`);
			await loadTransactions();
		} catch (err: unknown) {
			toast.error(`Failed to start: ${(err as Error).message}`);
		}
	}

	async function commitTransaction(id: string) {
		try {
			await lf().send(new CommitTransactionCommand({ TransactionId: id }));
			toast.success('Transaction committed');
			await loadTransactions();
		} catch (err: unknown) {
			toast.error(`Failed to commit: ${(err as Error).message}`);
		}
	}

	function confirmCancelTxn(id: string) {
		showConfirm(`Cancel transaction "${id}"? Active writes will be rolled back.`, async () => {
			try {
				await lf().send(new CancelTransactionCommand({ TransactionId: id }));
				toast.success('Transaction cancelled');
				await loadTransactions();
			} catch (err: unknown) {
				toast.error(`Failed to cancel: ${(err as Error).message}`);
			}
		});
	}

	// =========== DATA CELLS FILTERS ===========
	let loadingFilters = $state(false);
	let dataFilters = $state<DataCellsFilter[]>([]);
	let showCreateFilterModal = $state(false);
	let creatingFilter = $state(false);
	let filterCatalogId = $state('');
	let filterDbName = $state('');
	let filterTableName = $state('');
	let filterName = $state('');

	async function loadDataFilters() {
		loadingFilters = true;
		try {
			const res = await lf().send(new ListDataCellsFilterCommand({}));
			dataFilters = (res.DataCellsFilters ?? []) as DataCellsFilter[];
		} catch (err: unknown) {
			toast.error(`Failed to load filters: ${(err as Error).message}`);
		} finally {
			loadingFilters = false;
		}
	}

	async function createDataFilter() {
		if (!filterCatalogId.trim() || !filterDbName.trim() || !filterTableName.trim() || !filterName.trim()) return;
		creatingFilter = true;
		try {
			await lf().send(new CreateDataCellsFilterCommand({
				TableData: {
					TableCatalogId: filterCatalogId.trim(),
					DatabaseName: filterDbName.trim(),
					TableName: filterTableName.trim(),
					Name: filterName.trim(),
				},
			}));
			toast.success('Data cells filter created');
			showCreateFilterModal = false;
			filterCatalogId = '';
			filterDbName = '';
			filterTableName = '';
			filterName = '';
			await loadDataFilters();
		} catch (err: unknown) {
			toast.error(`Failed to create filter: ${(err as Error).message}`);
		} finally {
			creatingFilter = false;
		}
	}

	function confirmDeleteFilter(f: DataCellsFilter) {
		showConfirm(`Delete data cells filter "${f.Name}"?`, async () => {
			try {
				await lf().send(new DeleteDataCellsFilterCommand({
					TableCatalogId: f.TableCatalogId,
					DatabaseName: f.DatabaseName,
					TableName: f.TableName,
					Name: f.Name,
				}));
				toast.success('Filter deleted');
				await loadDataFilters();
			} catch (err: unknown) {
				toast.error(`Failed to delete: ${(err as Error).message}`);
			}
		});
	}

	// =========== LF TAG EXPRESSIONS ===========
	let loadingExprs = $state(false);
	let lfExpressions = $state<LFTagExpression[]>([]);
	let showCreateExprModal = $state(false);
	let creatingExpr = $state(false);
	let exprName = $state('');
	let exprDescription = $state('');
	let exprTagKey = $state('');
	let exprTagValues = $state('');

	async function loadExpressions() {
		loadingExprs = true;
		try {
			const res = await lf().send(new ListLFTagExpressionsCommand({}));
			lfExpressions = (res.LFTagExpressions ?? []) as LFTagExpression[];
		} catch (err: unknown) {
			toast.error(`Failed to load expressions: ${(err as Error).message}`);
		} finally {
			loadingExprs = false;
		}
	}

	async function createExpression() {
		if (!exprName.trim() || !exprTagKey.trim()) return;
		creatingExpr = true;
		try {
			const values = exprTagValues.split(',').map(v => v.trim()).filter(Boolean);
			await lf().send(new CreateLFTagExpressionCommand({
				Name: exprName.trim(),
				Description: exprDescription.trim() || undefined,
				Expression: [{ TagKey: exprTagKey.trim(), TagValues: values }],
			}));
			toast.success('Expression created');
			showCreateExprModal = false;
			exprName = '';
			exprDescription = '';
			exprTagKey = '';
			exprTagValues = '';
			await loadExpressions();
		} catch (err: unknown) {
			toast.error(`Failed to create expression: ${(err as Error).message}`);
		} finally {
			creatingExpr = false;
		}
	}

	function confirmDeleteExpr(name: string) {
		showConfirm(`Delete LF tag expression "${name}"?`, async () => {
			try {
				await lf().send(new DeleteLFTagExpressionCommand({ Name: name }));
				toast.success('Expression deleted');
				await loadExpressions();
			} catch (err: unknown) {
				toast.error(`Failed to delete: ${(err as Error).message}`);
			}
		});
	}

	// =========== TAB SWITCHING ===========
	function switchTab(tab: Tab) {
		activeTab = tab;
		if (tab === 'resources' && resources.length === 0) loadResources();
		else if (tab === 'lftags' && lfTags.length === 0) loadLFTags();
		else if (tab === 'permissions' && permissions.length === 0) loadPermissions();
		else if (tab === 'transactions' && transactions.length === 0) loadTransactions();
		else if (tab === 'datafilters' && dataFilters.length === 0) loadDataFilters();
		else if (tab === 'expressions' && lfExpressions.length === 0) loadExpressions();
	}

	function refreshCurrent() {
		if (activeTab === 'resources') loadResources();
		else if (activeTab === 'lftags') loadLFTags();
		else if (activeTab === 'permissions') loadPermissions();
		else if (activeTab === 'transactions') loadTransactions();
		else if (activeTab === 'datafilters') loadDataFilters();
		else if (activeTab === 'expressions') loadExpressions();
	}

	const isLoading = $derived(loadingResources || loadingTags || loadingPerms || loadingTxns || loadingFilters || loadingExprs);

	// Each tab's list is only loaded when first visited and cached
	// thereafter (see switchTab above) — clear every cached list and
	// reload whichever tab is active rather than only refetching
	// resources. `activeTab` is read via untrack() because switchTab also
	// writes it: without untrack, every tab switch would re-trigger this
	// region effect and double-fetch.
	onRegionChange(() => {
		resources = [];
		lfTags = [];
		permissions = [];
		transactions = [];
		dataFilters = [];
		lfExpressions = [];
		const tab = untrack(() => activeTab);
		if (tab === 'lftags') void loadLFTags();
		else if (tab === 'permissions') void loadPermissions();
		else if (tab === 'transactions') void loadTransactions();
		else if (tab === 'datafilters') void loadDataFilters();
		else if (tab === 'expressions') void loadExpressions();
		else void loadResources();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<Database class="w-6 h-6 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Lake Formation</h1>
				<p class="text-slate-600 dark:text-slate-300">Data lake permissions and governance</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			{#if activeTab === 'resources'}
				<button onclick={() => { showRegisterModal = true; }} class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors">
					<Plus class="w-4 h-4" />Register Resource
				</button>
			{:else if activeTab === 'lftags'}
				<button onclick={() => { showCreateTagModal = true; }} class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors">
					<Plus class="w-4 h-4" />Create Tag
				</button>
			{:else if activeTab === 'permissions'}
				<button onclick={() => { showGrantModal = true; }} class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors">
					<Plus class="w-4 h-4" />Grant Permission
				</button>
			{:else if activeTab === 'transactions'}
				<button onclick={() => startTransaction()} class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors">
					<Plus class="w-4 h-4" />Start Transaction
				</button>
			{:else if activeTab === 'datafilters'}
				<button onclick={() => { showCreateFilterModal = true; }} class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors">
					<Plus class="w-4 h-4" />Create Filter
				</button>
			{:else if activeTab === 'expressions'}
				<button onclick={() => { showCreateExprModal = true; }} class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg transition-colors">
					<Plus class="w-4 h-4" />Create Expression
				</button>
			{/if}
			<button onclick={() => refreshCurrent()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {isLoading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- Tab Navigation -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<ul class="flex flex-wrap -mb-px text-sm font-medium text-center">
			{#each [
				{ id: 'resources', label: 'Resources', icon: Database },
				{ id: 'lftags', label: 'LF Tags', icon: Tag },
				{ id: 'permissions', label: 'Permissions', icon: Shield },
				{ id: 'transactions', label: 'Transactions', icon: ArrowLeftRight },
				{ id: 'datafilters', label: 'Data Filters', icon: Filter },
				{ id: 'expressions', label: 'Expressions', icon: BookOpen },
			] as t}
				{@const TabIcon = t.icon}
				<li class="mr-2">
					<button
						onclick={() => switchTab(t.id as Tab)}
						class="inline-flex items-center gap-2 p-4 border-b-2 rounded-t-lg transition-colors {activeTab === t.id ? 'text-teal-600 border-teal-600 dark:text-teal-400 dark:border-teal-400' : 'border-transparent text-slate-500 hover:text-slate-600 hover:border-slate-300 dark:text-slate-400 dark:hover:text-slate-300'}"
					>
						<TabIcon class="w-4 h-4" />
						{t.label}
					</button>
				</li>
			{/each}
		</ul>
	</div>

	<!-- ======== RESOURCES ======== -->
	{#if activeTab === 'resources'}
		{#if loadingResources}
			<div class="flex items-center justify-center p-8">{@html spinnerSvg}</div>
		{:else if resources.length === 0}
			<div class="text-center py-12 text-slate-500">
				<Database class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" />
				<p class="text-lg font-medium">No resources registered</p>
				<p class="text-sm mt-1">Register an S3 location to manage data lake access</p>
				<button onclick={() => { showRegisterModal = true; }} class="mt-4 px-4 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg">Register Resource</button>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Resource ARN</th>
							<th class="px-6 py-3">Role ARN</th>
							<th class="px-6 py-3">Last Modified</th>
							<th class="px-6 py-3 text-right">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#each resources as resource}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
								<td class="px-6 py-4 font-mono text-xs text-slate-900 dark:text-white max-w-xs">
									<div class="flex items-center gap-1">
										<span class="truncate" title={resource.ResourceArn}>{resource.ResourceArn}</span>
										<button onclick={() => copyToClipboard(resource.ResourceArn ?? '')} class="shrink-0 text-slate-400 hover:text-slate-600" title="Copy ARN">
											<Copy class="w-3 h-3" />
										</button>
									</div>
								</td>
								<td class="px-6 py-4 font-mono text-xs truncate max-w-xs" title={resource.RoleArn ?? ''}>{resource.RoleArn ?? '—'}</td>
								<td class="px-6 py-4">{resource.LastModified ? new Date(resource.LastModified).toLocaleDateString() : '—'}</td>
								<td class="px-6 py-4 text-right">
									<button onclick={() => confirmDeregister(resource.ResourceArn ?? '')} class="text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Deregister">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ======== LF TAGS ======== -->
	{#if activeTab === 'lftags'}
		<div class="flex items-center gap-2 mb-2">
			<input
				type="search"
				bind:value={tagSearch}
				placeholder="Filter by tag key…"
				class="w-64 px-3 py-2 text-sm bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white"
			/>
		</div>
		{#if loadingTags}
			<div class="flex items-center justify-center p-8">{@html spinnerSvg}</div>
		{:else if filteredTags.length === 0}
			<div class="text-center py-12 text-slate-500">
				<Tag class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" />
				<p class="text-lg font-medium">{tagSearch ? 'No matching tags' : 'No LF tags defined'}</p>
				<p class="text-sm mt-1">Create LF tags to govern access to your data lake resources</p>
				{#if !tagSearch}
					<button onclick={() => { showCreateTagModal = true; }} class="mt-4 px-4 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg">Create Tag</button>
				{/if}
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Tag Key</th>
							<th class="px-6 py-3">Catalog ID</th>
							<th class="px-6 py-3">Allowed Values</th>
							<th class="px-6 py-3 text-right">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#each filteredTags as tag}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
								<td class="px-6 py-4 font-medium text-slate-900 dark:text-white font-mono">{tag.TagKey}</td>
								<td class="px-6 py-4 font-mono text-xs text-slate-400">{tag.CatalogId ?? '—'}</td>
								<td class="px-6 py-4">
									<div class="flex flex-wrap gap-1">
										{#each tag.TagValues ?? [] as val}
											<span class="px-2 py-0.5 text-xs bg-teal-100 dark:bg-teal-900/30 text-teal-700 dark:text-teal-300 rounded">{val}</span>
										{/each}
									</div>
								</td>
								<td class="px-6 py-4 text-right flex justify-end gap-2">
									<button onclick={() => openEditTag(tag.TagKey ?? '')} class="text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200 text-xs px-2 py-1 border border-slate-300 dark:border-slate-600 rounded" title="Edit values">Edit</button>
									<button onclick={() => confirmDeleteTag(tag.TagKey ?? '')} class="text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Delete tag">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ======== PERMISSIONS ======== -->
	{#if activeTab === 'permissions'}
		<div class="flex items-center gap-2 mb-2">
			<input
				type="search"
				bind:value={permPrincipalFilter}
				placeholder="Filter by principal…"
				class="w-72 px-3 py-2 text-sm bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white"
			/>
		</div>
		{#if loadingPerms}
			<div class="flex items-center justify-center p-8">{@html spinnerSvg}</div>
		{:else if filteredPerms.length === 0}
			<div class="text-center py-12 text-slate-500">
				<Shield class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" />
				<p class="text-lg font-medium">{permPrincipalFilter ? 'No matching permissions' : 'No permissions granted'}</p>
				<p class="text-sm mt-1">Grant permissions to allow principals to access data lake resources</p>
				{#if !permPrincipalFilter}
					<button onclick={() => { showGrantModal = true; }} class="mt-4 px-4 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg">Grant Permission</button>
				{/if}
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Principal</th>
							<th class="px-6 py-3">Resource</th>
							<th class="px-6 py-3">Permissions</th>
							<th class="px-6 py-3">With Grant Option</th>
							<th class="px-6 py-3 text-right">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#each filteredPerms as perm}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
								<td class="px-6 py-4 font-mono text-xs text-slate-900 dark:text-white max-w-xs">
									<div class="flex items-center gap-1">
										<span class="truncate" title={perm.Principal?.DataLakePrincipalIdentifier}>{perm.Principal?.DataLakePrincipalIdentifier ?? '—'}</span>
										{#if perm.Principal?.DataLakePrincipalIdentifier}
											<button onclick={() => copyToClipboard(perm.Principal!.DataLakePrincipalIdentifier!)} class="shrink-0 text-slate-400 hover:text-slate-600" title="Copy">
												<Copy class="w-3 h-3" />
											</button>
										{/if}
									</div>
								</td>
								<td class="px-6 py-4 text-xs text-slate-700 dark:text-slate-300">{describeResource(perm.Resource)}</td>
								<td class="px-6 py-4">
									<div class="flex flex-wrap gap-1">
										{#each perm.Permissions ?? [] as p}
											<span class="px-2 py-0.5 text-xs bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 rounded">{p}</span>
										{/each}
									</div>
								</td>
								<td class="px-6 py-4">
									<div class="flex flex-wrap gap-1">
										{#each perm.PermissionsWithGrantOption ?? [] as p}
											<span class="px-2 py-0.5 text-xs bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 rounded">{p}</span>
										{/each}
									</div>
								</td>
								<td class="px-6 py-4 text-right">
									<button onclick={() => confirmRevoke(perm)} class="text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Revoke">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ======== TRANSACTIONS ======== -->
	{#if activeTab === 'transactions'}
		<div class="flex items-center gap-2 mb-2">
			<select
				bind:value={txnStatusFilter}
				onchange={() => loadTransactions()}
				class="px-3 py-2 text-sm bg-white dark:bg-slate-800 border border-slate-300 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white"
			>
				<option value="">All statuses</option>
				<option value="ACTIVE">ACTIVE</option>
				<option value="COMMITTED">COMMITTED</option>
				<option value="ABORTED">ABORTED</option>
				<option value="COMMIT_IN_PROGRESS">COMMIT_IN_PROGRESS</option>
			</select>
		</div>
		{#if loadingTxns}
			<div class="flex items-center justify-center p-8">{@html spinnerSvg}</div>
		{:else if filteredTxns.length === 0}
			<div class="text-center py-12 text-slate-500">
				<ArrowLeftRight class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" />
				<p class="text-lg font-medium">{txnStatusFilter ? `No ${txnStatusFilter} transactions` : 'No transactions'}</p>
				<p class="text-sm mt-1">Start a transaction to govern governed table writes</p>
				{#if !txnStatusFilter}
					<button onclick={() => startTransaction()} class="mt-4 px-4 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg">Start Transaction</button>
				{/if}
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Transaction ID</th>
							<th class="px-6 py-3">Status</th>
							<th class="px-6 py-3 text-right">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#each filteredTxns as tx}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
								<td class="px-6 py-4 font-mono text-xs text-slate-900 dark:text-white">
									<div class="flex items-center gap-1">
										<span>{tx.TransactionId}</span>
										<button onclick={() => copyToClipboard(tx.TransactionId ?? '')} class="text-slate-400 hover:text-slate-600" title="Copy ID">
											<Copy class="w-3 h-3" />
										</button>
									</div>
								</td>
								<td class="px-6 py-4">
									<span class="px-2 py-0.5 text-xs rounded font-medium
										{tx.TransactionStatus === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' :
										 tx.TransactionStatus === 'COMMITTED' ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' :
										 tx.TransactionStatus === 'COMMIT_IN_PROGRESS' ? 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300' :
										 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300'}"
									>{tx.TransactionStatus}</span>
								</td>
								<td class="px-6 py-4 text-right">
									{#if tx.TransactionStatus === 'ACTIVE'}
										<div class="flex justify-end gap-2">
											<button onclick={() => commitTransaction(tx.TransactionId ?? '')} class="px-2 py-1 text-xs font-medium text-white bg-blue-600 hover:bg-blue-700 rounded">Commit</button>
											<button onclick={() => confirmCancelTxn(tx.TransactionId ?? '')} class="px-2 py-1 text-xs font-medium text-white bg-red-600 hover:bg-red-700 rounded">Cancel</button>
										</div>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ======== DATA CELLS FILTERS ======== -->
	{#if activeTab === 'datafilters'}
		{#if loadingFilters}
			<div class="flex items-center justify-center p-8">{@html spinnerSvg}</div>
		{:else if dataFilters.length === 0}
			<div class="text-center py-12 text-slate-500">
				<Filter class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" />
				<p class="text-lg font-medium">No data cells filters</p>
				<p class="text-sm mt-1">Create filters to restrict row/column access on governed tables</p>
				<button onclick={() => { showCreateFilterModal = true; }} class="mt-4 px-4 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg">Create Filter</button>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Name</th>
							<th class="px-6 py-3">Database</th>
							<th class="px-6 py-3">Table</th>
							<th class="px-6 py-3">Catalog ID</th>
							<th class="px-6 py-3 text-right">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#each dataFilters as f}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
								<td class="px-6 py-4 font-medium text-slate-900 dark:text-white">{f.Name}</td>
								<td class="px-6 py-4">{f.DatabaseName}</td>
								<td class="px-6 py-4">{f.TableName}</td>
								<td class="px-6 py-4 font-mono text-xs">{f.TableCatalogId ?? '—'}</td>
								<td class="px-6 py-4 text-right">
									<button onclick={() => confirmDeleteFilter(f)} class="text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Delete">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- ======== LF TAG EXPRESSIONS ======== -->
	{#if activeTab === 'expressions'}
		{#if loadingExprs}
			<div class="flex items-center justify-center p-8">{@html spinnerSvg}</div>
		{:else if lfExpressions.length === 0}
			<div class="text-center py-12 text-slate-500">
				<BookOpen class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" />
				<p class="text-lg font-medium">No LF tag expressions</p>
				<p class="text-sm mt-1">Save reusable tag expressions to simplify access control policies</p>
				<button onclick={() => { showCreateExprModal = true; }} class="mt-4 px-4 py-2 text-sm font-medium text-white bg-teal-600 hover:bg-teal-700 rounded-lg">Create Expression</button>
			</div>
		{:else}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Name</th>
							<th class="px-6 py-3">Description</th>
							<th class="px-6 py-3">Expression</th>
							<th class="px-6 py-3 text-right">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#each lfExpressions as expr}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
								<td class="px-6 py-4 font-medium text-slate-900 dark:text-white">{expr.Name}</td>
								<td class="px-6 py-4 text-slate-500">{expr.Description ?? '—'}</td>
								<td class="px-6 py-4">
									<div class="flex flex-wrap gap-1">
										{#each expr.Expression ?? [] as tag}
											<span class="px-2 py-0.5 text-xs bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300 rounded">{tag.TagKey}: {tag.TagValues?.join(', ')}</span>
										{/each}
									</div>
								</td>
								<td class="px-6 py-4 text-right">
									<button onclick={() => confirmDeleteExpr(expr.Name ?? '')} class="text-red-600 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Delete">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- ======== CONFIRM DIALOG ======== -->
{#if confirmAction}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" role="dialog" aria-modal="true">
		<div class="bg-white dark:bg-slate-800 rounded-lg shadow-xl p-6 max-w-sm w-full mx-4">
			<p class="text-slate-900 dark:text-white font-medium mb-4">{confirmMsg}</p>
			<div class="flex gap-3 justify-end">
				<button onclick={() => cancelConfirm()} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-700 dark:text-slate-300 dark:border-slate-600">Cancel</button>
				<button onclick={() => doConfirm()} class="py-2 px-4 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg">Confirm</button>
			</div>
		</div>
	</div>
{/if}

<!-- ======== MODALS ======== -->

<!-- Register Resource -->
{#if showRegisterModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showRegisterModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showRegisterModal = false)} role="dialog" aria-modal="true" tabindex="-1">
		<div class="relative p-4 w-full max-w-md">
			<div class="bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Register Resource</h3>
					<button aria-label="Close" onclick={() => { showRegisterModal = false; }} class="text-slate-400 hover:bg-slate-200 hover:text-slate-900 rounded-lg w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); registerResource(); }}>
						<div>
							<label for="resource-arn" class="block mb-1 text-sm font-medium text-slate-900 dark:text-white">Resource ARN</label>
							<p class="text-xs text-slate-500 mb-2">The ARN of the S3 location to register (e.g. arn:aws:s3:::my-bucket)</p>
							<input type="text" id="resource-arn" bind:value={newResourceArn} placeholder="arn:aws:s3:::my-bucket" required pattern="arn:aws:s3:::.*" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="role-arn" class="block mb-1 text-sm font-medium text-slate-900 dark:text-white">Role ARN</label>
							<p class="text-xs text-slate-500 mb-2">IAM role ARN that Lake Formation will use to access the resource</p>
							<input type="text" id="role-arn" bind:value={newRoleArn} placeholder="arn:aws:iam::000000000000:role/my-role" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div class="flex gap-3 justify-end pt-2">
							<button type="button" onclick={() => { showRegisterModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600">Cancel</button>
							<button type="submit" disabled={registering} class="text-white bg-teal-600 hover:bg-teal-700 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">{registering ? 'Registering…' : 'Register'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Create LF Tag -->
{#if showCreateTagModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showCreateTagModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showCreateTagModal = false)} role="dialog" aria-modal="true" tabindex="-1">
		<div class="relative p-4 w-full max-w-md">
			<div class="bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create LF Tag</h3>
					<button aria-label="Close" onclick={() => { showCreateTagModal = false; }} class="text-slate-400 hover:bg-slate-200 hover:text-slate-900 rounded-lg w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createLFTag(); }}>
						<div>
							<label for="tag-key" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Tag Key</label>
							<input type="text" id="tag-key" bind:value={newTagKey} placeholder="e.g. environment" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="tag-values" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Allowed Values <span class="text-slate-400 font-normal">(comma-separated)</span></label>
							<input type="text" id="tag-values" bind:value={newTagValues} placeholder="e.g. dev,staging,prod" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div class="flex gap-3 justify-end pt-2">
							<button type="button" onclick={() => { showCreateTagModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600">Cancel</button>
							<button type="submit" disabled={creatingTag} class="text-white bg-teal-600 hover:bg-teal-700 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">{creatingTag ? 'Creating…' : 'Create'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Edit LF Tag values -->
{#if showEditTagModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showEditTagModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showEditTagModal = false)} role="dialog" aria-modal="true" tabindex="-1">
		<div class="relative p-4 w-full max-w-md">
			<div class="bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Edit LF Tag: <span class="font-mono text-teal-600">{editTagKey}</span></h3>
					<button aria-label="Close" onclick={() => { showEditTagModal = false; }} class="text-slate-400 hover:bg-slate-200 hover:text-slate-900 rounded-lg w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); updateLFTag(); }}>
						<div>
							<label for="tag-add-values" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Values to Add <span class="text-slate-400 font-normal">(comma-separated)</span></label>
							<input type="text" id="tag-add-values" bind:value={editTagAddValues} placeholder="e.g. staging,uat" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="tag-remove-values" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Values to Remove <span class="text-slate-400 font-normal">(comma-separated)</span></label>
							<input type="text" id="tag-remove-values" bind:value={editTagRemoveValues} placeholder="e.g. old-value" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div class="flex gap-3 justify-end pt-2">
							<button type="button" onclick={() => { showEditTagModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600">Cancel</button>
							<button type="submit" disabled={savingTag} class="text-white bg-teal-600 hover:bg-teal-700 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">{savingTag ? 'Saving…' : 'Save'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Grant Permission -->
{#if showGrantModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showGrantModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showGrantModal = false)} role="dialog" aria-modal="true" tabindex="-1">
		<div class="relative p-4 w-full max-w-lg">
			<div class="bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Grant Permission</h3>
					<button aria-label="Close" onclick={() => { showGrantModal = false; }} class="text-slate-400 hover:bg-slate-200 hover:text-slate-900 rounded-lg w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); grantPermission(); }}>
						<div>
							<label for="grant-principal" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Principal ARN <span class="text-slate-400 font-normal">(IAM user/role/group)</span></label>
							<input type="text" id="grant-principal" bind:value={grantPrincipal} placeholder="arn:aws:iam::000000000000:user/alice" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<p class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Resource Type</p>
							<div class="flex gap-2 flex-wrap">
								{#each ['catalog', 'database', 'table', 'datalocation'] as rt}
									<button
										type="button"
										onclick={() => { grantResourceType = rt as any; }}
										class="px-3 py-1.5 text-sm rounded-lg border transition-colors {grantResourceType === rt ? 'bg-teal-600 text-white border-teal-600' : 'bg-white dark:bg-slate-600 text-slate-700 dark:text-slate-200 border-slate-300 dark:border-slate-500 hover:bg-slate-50'}"
									>{rt === 'datalocation' ? 'Data Location' : rt.charAt(0).toUpperCase() + rt.slice(1)}</button>
								{/each}
							</div>
						</div>
						{#if grantResourceType === 'database' || grantResourceType === 'table'}
							<div>
								<label for="grant-db" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Database Name</label>
								<input type="text" id="grant-db" bind:value={grantResourceDb} placeholder="my_database" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
							</div>
						{/if}
						{#if grantResourceType === 'table'}
							<div>
								<label for="grant-table" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Table Name</label>
								<input type="text" id="grant-table" bind:value={grantResourceTable} placeholder="my_table" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
							</div>
						{/if}
						{#if grantResourceType === 'datalocation'}
							<div>
								<label for="grant-arn" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Resource ARN</label>
								<input type="text" id="grant-arn" bind:value={grantResourceArn} placeholder="arn:aws:s3:::my-bucket" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
							</div>
						{/if}
						<div>
							<label for="grant-perms" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Permissions <span class="text-slate-400 font-normal">(comma-separated, e.g. SELECT,DESCRIBE)</span></label>
							<input type="text" id="grant-perms" bind:value={grantPermList} placeholder="SELECT,DESCRIBE" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="grant-with-option" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">With Grant Option <span class="text-slate-400 font-normal">(comma-separated, optional)</span></label>
							<input type="text" id="grant-with-option" bind:value={grantWithOption} placeholder="SELECT" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div class="flex gap-3 justify-end pt-2">
							<button type="button" onclick={() => { showGrantModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600">Cancel</button>
							<button type="submit" disabled={granting} class="text-white bg-teal-600 hover:bg-teal-700 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">{granting ? 'Granting…' : 'Grant'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Create Data Cells Filter -->
{#if showCreateFilterModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showCreateFilterModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showCreateFilterModal = false)} role="dialog" aria-modal="true" tabindex="-1">
		<div class="relative p-4 w-full max-w-md">
			<div class="bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Data Cells Filter</h3>
					<button aria-label="Close" onclick={() => { showCreateFilterModal = false; }} class="text-slate-400 hover:bg-slate-200 hover:text-slate-900 rounded-lg w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createDataFilter(); }}>
						<div>
							<label for="filter-catalog" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Catalog ID</label>
							<input type="text" id="filter-catalog" bind:value={filterCatalogId} placeholder="123456789012" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="filter-db" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Database Name</label>
							<input type="text" id="filter-db" bind:value={filterDbName} placeholder="my_database" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="filter-table" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Table Name</label>
							<input type="text" id="filter-table" bind:value={filterTableName} placeholder="my_table" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="filter-name" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Filter Name</label>
							<input type="text" id="filter-name" bind:value={filterName} placeholder="my_row_filter" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div class="flex gap-3 justify-end pt-2">
							<button type="button" onclick={() => { showCreateFilterModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600">Cancel</button>
							<button type="submit" disabled={creatingFilter} class="text-white bg-teal-600 hover:bg-teal-700 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">{creatingFilter ? 'Creating…' : 'Create'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Create LF Tag Expression -->
{#if showCreateExprModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showCreateExprModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showCreateExprModal = false)} role="dialog" aria-modal="true" tabindex="-1">
		<div class="relative p-4 w-full max-w-md">
			<div class="bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create LF Tag Expression</h3>
					<button aria-label="Close" onclick={() => { showCreateExprModal = false; }} class="text-slate-400 hover:bg-slate-200 hover:text-slate-900 rounded-lg w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createExpression(); }}>
						<div>
							<label for="expr-name" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Expression Name</label>
							<input type="text" id="expr-name" bind:value={exprName} placeholder="e.g. prod-data-access" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="expr-desc" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Description <span class="text-slate-400 font-normal">(optional)</span></label>
							<input type="text" id="expr-desc" bind:value={exprDescription} placeholder="Access to production data" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="expr-tag-key" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Tag Key</label>
							<input type="text" id="expr-tag-key" bind:value={exprTagKey} placeholder="environment" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div>
							<label for="expr-tag-values" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Tag Values <span class="text-slate-400 font-normal">(comma-separated)</span></label>
							<input type="text" id="expr-tag-values" bind:value={exprTagValues} placeholder="prod,staging" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
						</div>
						<div class="flex gap-3 justify-end pt-2">
							<button type="button" onclick={() => { showCreateExprModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600">Cancel</button>
							<button type="submit" disabled={creatingExpr} class="text-white bg-teal-600 hover:bg-teal-700 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">{creatingExpr ? 'Creating…' : 'Create'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}
