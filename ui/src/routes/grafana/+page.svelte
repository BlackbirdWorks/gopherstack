<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getGrafanaClient } from '$lib/aws-client';
	import {
		ListWorkspacesCommand,
		CreateWorkspaceCommand,
		DescribeWorkspaceCommand,
		UpdateWorkspaceCommand,
		DeleteWorkspaceCommand,
		AssociateLicenseCommand,
		DisassociateLicenseCommand,
		ListWorkspaceServiceAccountsCommand,
		CreateWorkspaceServiceAccountCommand,
		DeleteWorkspaceServiceAccountCommand,
		type WorkspaceSummary,
		type WorkspaceDescription,
		type ServiceAccountSummary,
		type AccountAccessType,
		type PermissionType,
		type AuthenticationProviderTypes,
		type LicenseType,
		type Role
	} from '@aws-sdk/client-grafana';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { BarChart3, Plus, Trash2, Eye, Pencil, X } from 'lucide-svelte';

	// Amazon Managed Grafana has exactly one top-level listable resource
	// family (Workspaces). ListWorkspaceServiceAccounts/ListPermissions/
	// ListWorkspaceServiceAccountTokens all require a workspaceId (and the
	// last also a serviceAccountId), so they are not independently listable
	// families -- service accounts live inside the workspace detail modal,
	// the same way dlm nests tag management inside its policy detail modal.
	//
	// Deliberately NOT built here (see project report, not invented as UI):
	//  - Workspace API keys (CreateWorkspaceApiKeyCommand/
	//    DeleteWorkspaceApiKeyCommand): the SDK has no
	//    ListWorkspaceApiKeysCommand at all, so an existing key can never be
	//    read back -- only a one-shot create that returns a secret and a
	//    delete by name typed blind. Also legacy/superseded by service
	//    accounts + tokens per AWS's own docs.
	//  - Service account tokens (workspaceId + serviceAccountId nesting,
	//    3 levels deep) and workspace Permissions (IAM Identity Center/SAML
	//    role-assignment editing, not a create/delete-shaped resource).
	const grafana = regionalClient(getGrafanaClient);

	type TabId = 'workspaces';
	const tabs: TabDef[] = [{ id: 'workspaces', label: 'Workspaces' }];
	let activeTab = $state<TabId>('workspaces');
	let searchQuery = $state('');

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function statusClass(status: string | undefined): string {
		if (status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let workspaces = $state<WorkspaceSummary[]>([]);

	async function fetchWorkspaces(): Promise<void> {
		const resp = await grafana().send(new ListWorkspacesCommand({}));
		workspaces = resp.workspaces ?? [];
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	const tabLoader = createTabLoader<TabId>({
		workspaces: () => fetchWorkspaces().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// selectedWorkspace and its service-account list key off a workspaceId
	// that is only unique within the region it was fetched from -- clear
	// both on every region change, same as workspaces itself.
	onRegionChange(() => {
		selectedWorkspace = null;
		serviceAccounts = [];
		tabLoader.refresh('workspaces');
	});

	const filteredWorkspaces = $derived(
		workspaces.filter((w) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(w.name ?? '').toLowerCase().includes(q) ||
				(w.id ?? '').toLowerCase().includes(q) ||
				(w.status ?? '').toLowerCase().includes(q) ||
				(w.endpoint ?? '').toLowerCase().includes(q)
			);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Create ---

	let createModal = $state<Modal | null>(null);
	let creating = $state(false);
	let createError = $state<string | null>(null);
	let newName = $state('');
	let newDescription = $state('');
	let newAccountAccessType = $state<AccountAccessType>('CURRENT_ACCOUNT');
	let newPermissionType = $state<PermissionType>('CUSTOMER_MANAGED');
	let newWorkspaceRoleArn = $state('');
	let newAuthSso = $state(true);
	let newAuthSaml = $state(false);

	function openCreateModal(): void {
		createError = null;
		newName = '';
		newDescription = '';
		newAccountAccessType = 'CURRENT_ACCOUNT';
		newPermissionType = 'CUSTOMER_MANAGED';
		newWorkspaceRoleArn = '';
		newAuthSso = true;
		newAuthSaml = false;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!newName.trim()) {
			createError = 'Workspace name is required.';
			return;
		}
		const authenticationProviders: AuthenticationProviderTypes[] = [];
		if (newAuthSso) authenticationProviders.push('AWS_SSO');
		if (newAuthSaml) authenticationProviders.push('SAML');
		if (authenticationProviders.length === 0) {
			createError = 'At least one authentication provider is required.';
			return;
		}
		creating = true;
		createError = null;
		try {
			await grafana().send(
				new CreateWorkspaceCommand({
					workspaceName: newName.trim(),
					workspaceDescription: newDescription.trim() || undefined,
					accountAccessType: newAccountAccessType,
					permissionType: newPermissionType,
					workspaceRoleArn: newWorkspaceRoleArn.trim() || undefined,
					authenticationProviders
				})
			);
			toast.success(`Workspace "${newName}" created`);
			createModal?.close();
			await tabLoader.refresh('workspaces');
		} catch (e) {
			const msg = describeError(e);
			createError = msg;
			toast.error(msg);
		} finally {
			creating = false;
		}
	}

	// --- Delete ---

	async function handleDelete(w: WorkspaceSummary): Promise<void> {
		if (!w.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete workspace',
			message: `Delete Grafana workspace "${w.name ?? w.id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await grafana().send(new DeleteWorkspaceCommand({ workspaceId: w.id }));
			toast.success('Workspace deleted');
			if (selectedWorkspace?.id === w.id) {
				selectedWorkspace = null;
				serviceAccounts = [];
				detailModal?.close();
			}
			await tabLoader.refresh('workspaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Detail (with service accounts + license) ---

	let detailModal = $state<Modal | null>(null);
	let selectedWorkspace = $state<WorkspaceDescription | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let serviceAccounts = $state<ServiceAccountSummary[]>([]);
	let serviceAccountsError = $state<string | null>(null);
	let newSaName = $state('');
	let newSaRole = $state<Role>('VIEWER');
	let licenseBusy = $state(false);

	async function loadServiceAccounts(workspaceId: string): Promise<void> {
		serviceAccountsError = null;
		try {
			const resp = await grafana().send(new ListWorkspaceServiceAccountsCommand({ workspaceId }));
			serviceAccounts = resp.serviceAccounts ?? [];
		} catch (e) {
			serviceAccountsError = describeError(e);
		}
	}

	async function openDetail(w: WorkspaceSummary): Promise<void> {
		selectedWorkspace = null;
		serviceAccounts = [];
		detailError = null;
		serviceAccountsError = null;
		newSaName = '';
		newSaRole = 'VIEWER';
		detailModal?.open();
		if (!w.id) return;
		detailLoading = true;
		try {
			const resp = await grafana().send(new DescribeWorkspaceCommand({ workspaceId: w.id }));
			selectedWorkspace = resp.workspace ?? null;
			if (selectedWorkspace?.id) await loadServiceAccounts(selectedWorkspace.id);
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function refreshDetail(): Promise<void> {
		if (!selectedWorkspace?.id) return;
		try {
			const resp = await grafana().send(new DescribeWorkspaceCommand({ workspaceId: selectedWorkspace.id }));
			selectedWorkspace = resp.workspace ?? selectedWorkspace;
		} catch (e) {
			detailError = describeError(e);
		}
	}

	async function addServiceAccount(): Promise<void> {
		if (!selectedWorkspace?.id || !newSaName.trim()) return;
		serviceAccountsError = null;
		try {
			await grafana().send(
				new CreateWorkspaceServiceAccountCommand({
					workspaceId: selectedWorkspace.id,
					name: newSaName.trim(),
					grafanaRole: newSaRole
				})
			);
			newSaName = '';
			toast.success('Service account created');
			await loadServiceAccounts(selectedWorkspace.id);
		} catch (e) {
			const msg = describeError(e);
			serviceAccountsError = msg;
			toast.error(msg);
		}
	}

	async function removeServiceAccount(sa: ServiceAccountSummary): Promise<void> {
		if (!selectedWorkspace?.id || !sa.id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete service account',
			message: `Delete service account "${sa.name}"?`
		});
		if (!confirmed) return;
		try {
			await grafana().send(
				new DeleteWorkspaceServiceAccountCommand({ workspaceId: selectedWorkspace.id, serviceAccountId: sa.id })
			);
			toast.success('Service account deleted');
			await loadServiceAccounts(selectedWorkspace.id);
		} catch (e) {
			const msg = describeError(e);
			serviceAccountsError = msg;
			toast.error(msg);
		}
	}

	async function toggleLicense(): Promise<void> {
		if (!selectedWorkspace?.id) return;
		licenseBusy = true;
		try {
			if (selectedWorkspace.licenseType) {
				await grafana().send(new DisassociateLicenseCommand({ workspaceId: selectedWorkspace.id, licenseType: selectedWorkspace.licenseType }));
				toast.success('License disassociated');
			} else {
				await grafana().send(new AssociateLicenseCommand({ workspaceId: selectedWorkspace.id, licenseType: 'ENTERPRISE_FREE_TRIAL' as LicenseType }));
				toast.success('License associated');
			}
			await refreshDetail();
			await tabLoader.refresh('workspaces');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			licenseBusy = false;
		}
	}

	// --- Edit ---

	let editModal = $state<Modal | null>(null);
	let editing = $state(false);
	let editError = $state<string | null>(null);
	let editWorkspaceId = $state('');
	let editName = $state('');
	let editDescription = $state('');

	function openEditModal(w: WorkspaceDescription): void {
		editError = null;
		editWorkspaceId = w.id ?? '';
		editName = w.name ?? '';
		editDescription = w.description ?? '';
		editModal?.open();
	}

	async function submitEdit(): Promise<void> {
		if (!editWorkspaceId) return;
		editing = true;
		editError = null;
		try {
			await grafana().send(
				new UpdateWorkspaceCommand({
					workspaceId: editWorkspaceId,
					workspaceName: editName.trim() || undefined,
					workspaceDescription: editDescription.trim() || undefined
				})
			);
			toast.success('Workspace updated');
			editModal?.close();
			await tabLoader.refresh('workspaces');
			await refreshDetail();
		} catch (e) {
			const msg = describeError(e);
			editError = msg;
			toast.error(msg);
		} finally {
			editing = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={BarChart3}
		title="Amazon Managed Grafana"
		description="Fully managed Grafana workspaces for operational observability"
		onRefresh={handleRefresh}
		color="orange"
	>
		{#snippet actions()}
			<button
				onclick={openCreateModal}
				class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm"
			>
				<Plus class="w-4 h-4" /> Create workspace
			</button>
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="orange" />
			<SearchInput bind:value={searchQuery} placeholder="Search workspaces..." />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'workspaces'}
				{#snippet statusCell(w: WorkspaceSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(w.status)}">{w.status ?? '—'}</span>
				{/snippet}
				{#snippet authCell(w: WorkspaceSummary)}
					<div class="flex gap-1">
						{#each w.authentication?.providers ?? [] as provider (provider)}
							<span class="text-xs px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400">{provider}</span>
						{/each}
					</div>
				{/snippet}
				{#snippet createdCell(w: WorkspaceSummary)}
					{formatDate(w.created)}
				{/snippet}
				{#snippet actionsCell(w: WorkspaceSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDetail(w)} title="View" aria-label="View workspace {w.name}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDelete(w)} title="Delete" aria-label="Delete workspace {w.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const columns = defineColumns<WorkspaceSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'id', label: 'ID' },
					{ key: 'status', label: 'Status', render: statusCell },
					{ key: 'grafanaVersion', label: 'Version' },
					{ key: 'authentication', label: 'Auth', render: authCell },
					{ key: 'created', label: 'Created', render: createdCell },
					{ key: 'actions', label: '', render: actionsCell }
				])}
				<DataTable
					rows={filteredWorkspaces}
					rowKey={(w) => w.id ?? ''}
					columns={columns}
					loading={tabLoader.isLoading('workspaces')}
					emptyMessage="No workspaces found"
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createModal} title="Create Workspace">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="grafana-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="grafana-new-name" bind:value={newName} placeholder="my-workspace" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="grafana-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="grafana-new-desc" bind:value={newDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="grafana-new-access" class="text-sm text-slate-600 dark:text-slate-300">Account access type</label>
				<select id="grafana-new-access" bind:value={newAccountAccessType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="CURRENT_ACCOUNT">CURRENT_ACCOUNT</option>
					<option value="ORGANIZATION">ORGANIZATION</option>
				</select>
			</div>
			<div>
				<label for="grafana-new-perm" class="text-sm text-slate-600 dark:text-slate-300">Permission type</label>
				<select id="grafana-new-perm" bind:value={newPermissionType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="CUSTOMER_MANAGED">CUSTOMER_MANAGED</option>
					<option value="SERVICE_MANAGED">SERVICE_MANAGED</option>
				</select>
				<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">AWS recommends CUSTOMER_MANAGED for API/CLI-created workspaces; SERVICE_MANAGED IAM role provisioning is console-only.</p>
			</div>
			<div>
				<label for="grafana-new-role-arn" class="text-sm text-slate-600 dark:text-slate-300">Workspace role ARN (for CUSTOMER_MANAGED)</label>
				<input id="grafana-new-role-arn" bind:value={newWorkspaceRoleArn} placeholder="arn:aws:iam::123456789012:role/GrafanaRole" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<span class="text-sm text-slate-600 dark:text-slate-300">Authentication providers</span>
				<div class="mt-1 flex gap-4">
					<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={newAuthSso} /> AWS SSO</label>
					<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={newAuthSaml} /> SAML</label>
				</div>
			</div>
			{#if createError}
				<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={creating} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editModal} title="Edit Workspace">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="grafana-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="grafana-edit-name" bind:value={editName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="grafana-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="grafana-edit-desc" bind:value={editDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editError}
				<p class="text-sm text-red-600 dark:text-red-400">{editError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEdit} disabled={editing} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{editing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Workspace">
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if selectedWorkspace}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{selectedWorkspace.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{selectedWorkspace.id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Endpoint</dt><dd class="break-all text-slate-900 dark:text-white">{selectedWorkspace.endpoint ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{selectedWorkspace.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Grafana version</dt><dd class="text-slate-900 dark:text-white">{selectedWorkspace.grafanaVersion ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(selectedWorkspace.created)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Modified</dt><dd class="text-slate-900 dark:text-white">{formatDate(selectedWorkspace.modified)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">License</dt>
					<dd class="text-slate-900 dark:text-white flex items-center gap-2">
						{selectedWorkspace.licenseType ?? 'None'}
						<button type="button" onclick={toggleLicense} disabled={licenseBusy} class="text-xs px-2 py-1 rounded-lg bg-orange-600/10 text-orange-600 hover:bg-orange-600/20 disabled:opacity-50">
							{selectedWorkspace.licenseType ? 'Disassociate' : 'Associate free trial'}
						</button>
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Service accounts</dt>
					<dd class="text-slate-900 dark:text-white">
						{#if serviceAccountsError}
							<p class="text-sm text-red-600 dark:text-red-400">{serviceAccountsError}</p>
						{/if}
						{#if serviceAccounts.length === 0}
							<span class="text-slate-500 dark:text-slate-400">No service accounts</span>
						{:else}
							<ul class="space-y-1">
								{#each serviceAccounts as sa (sa.id)}
									<li class="flex items-center gap-2">
										<span class="px-2 py-0.5 rounded-full bg-gray-100 dark:bg-slate-700 text-xs">{sa.name} · {sa.grafanaRole}</span>
										<button onclick={() => removeServiceAccount(sa)} aria-label="Delete service account {sa.name}" class="text-gray-400 hover:text-red-500"><X class="w-3 h-3" /></button>
									</li>
								{/each}
							</ul>
						{/if}
						<div class="mt-2 flex items-center gap-2">
							<input bind:value={newSaName} placeholder="Service account name" aria-label="New service account name" class="w-1/2 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							<select bind:value={newSaRole} aria-label="New service account role" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
								<option value="VIEWER">VIEWER</option>
								<option value="EDITOR">EDITOR</option>
								<option value="ADMIN">ADMIN</option>
							</select>
							<button type="button" onclick={addServiceAccount} class="px-2 py-1 text-xs rounded-lg bg-orange-600 text-white hover:bg-orange-700">Add</button>
						</div>
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if selectedWorkspace}
			<button type="button" onclick={() => selectedWorkspace && openEditModal(selectedWorkspace)} class="flex items-center gap-2 rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>
