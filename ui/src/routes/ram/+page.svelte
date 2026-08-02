<script lang="ts">
	// AWS RAM (Resource Access Manager) is about *sharing* resources you
	// already own with other principals (accounts/OUs/IAM roles), not owning
	// resources itself -- Resources and Principals are read-only derived
	// views of what a ResourceShare currently covers, not independently
	// creatable things. The CRUD floor here is therefore: ResourceShare
	// (create/update/delete + associate/disassociate resources & principals),
	// Permission (create/delete a customer-managed permission + version
	// management, already partly present), and Invitation (accept/reject,
	// already present).
	//
	// ListTagsForResource is deliberately NOT called anywhere on this page --
	// it is not a real AWS RAM SDK operation (see services/ram/PARITY.md and
	// handler.go's opListTagsForResource doc comment); tags are read back via
	// GetResourceShares' `tags` field, which is already used below.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getRAMClient } from '$lib/aws-client';
	import {
		GetResourceSharesCommand,
		ListResourcesCommand,
		ListPrincipalsCommand,
		ListPermissionsCommand,
		ListPermissionVersionsCommand,
		GetResourceShareAssociationsCommand,
		GetResourceShareInvitationsCommand,
		RejectResourceShareInvitationCommand,
		AcceptResourceShareInvitationCommand,
		SetDefaultPermissionVersionCommand,
		CreateResourceShareCommand,
		UpdateResourceShareCommand,
		DeleteResourceShareCommand,
		AssociateResourceShareCommand,
		DisassociateResourceShareCommand,
		CreatePermissionCommand,
		CreatePermissionVersionCommand,
		DeletePermissionCommand,
		DeletePermissionVersionCommand,
		type ResourceShare,
		type Resource,
		type Principal,
		type ResourceSharePermissionSummary,
		type ResourceShareInvitation,
		type ResourceShareAssociation
	} from '@aws-sdk/client-ram';
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
	import LoadMore from '$lib/components/LoadMore.svelte';
	import { Share2, RefreshCw, Plus, Trash2, Eye, Pencil, Users, Box, CheckCircle, Key, Bell } from 'lucide-svelte';

	const ram = regionalClient(getRAMClient);

	type TabId = 'shares' | 'resources' | 'principals' | 'permissions' | 'invitations';

	const tabs: TabDef[] = [
		{ id: 'shares', label: 'Resource Shares' },
		{ id: 'resources', label: 'Resources' },
		{ id: 'principals', label: 'Principals' },
		{ id: 'permissions', label: 'Permissions' },
		{ id: 'invitations', label: 'Invitations' }
	];

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

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	function statusClass(status: string | undefined): string {
		if (status === 'ACTIVE' || status === 'ASSOCIATED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status?.includes('FAILED')) return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	function splitList(s: string): string[] {
		return s
			.split(',')
			.map((v) => v.trim())
			.filter(Boolean);
	}

	let activeTab = $state<TabId>('shares');
	let searchQuery = $state('');

	let shares = $state<ResourceShare[]>([]);
	let resources = $state<Resource[]>([]);
	let principals = $state<Principal[]>([]);
	let permissions = $state<ResourceSharePermissionSummary[]>([]);
	let invitations = $state<ResourceShareInvitation[]>([]);

	let sharesNextToken = $state<string | null>(null);
	let resourcesNextToken = $state<string | null>(null);
	let principalsNextToken = $state<string | null>(null);
	let permissionsNextToken = $state<string | null>(null);

	async function fetchShares(): Promise<void> {
		const resp = await ram().send(new GetResourceSharesCommand({ resourceOwner: 'SELF' }));
		shares = resp.resourceShares ?? [];
		sharesNextToken = resp.nextToken ?? null;
	}
	async function fetchResources(): Promise<void> {
		const resp = await ram().send(new ListResourcesCommand({ resourceOwner: 'SELF' }));
		resources = resp.resources ?? [];
		resourcesNextToken = resp.nextToken ?? null;
	}
	async function fetchPrincipals(): Promise<void> {
		const resp = await ram().send(new ListPrincipalsCommand({ resourceOwner: 'SELF' }));
		principals = resp.principals ?? [];
		principalsNextToken = resp.nextToken ?? null;
	}
	async function fetchPermissions(): Promise<void> {
		const resp = await ram().send(new ListPermissionsCommand({}));
		permissions = resp.permissions ?? [];
		permissionsNextToken = resp.nextToken ?? null;
	}
	async function fetchInvitations(): Promise<void> {
		const resp = await ram().send(new GetResourceShareInvitationsCommand({}));
		invitations = resp.resourceShareInvitations ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		shares: () => fetchShares().catch(rethrowDescribed),
		resources: () => fetchResources().catch(rethrowDescribed),
		principals: () => fetchPrincipals().catch(rethrowDescribed),
		permissions: () => fetchPermissions().catch(rethrowDescribed),
		invitations: () => fetchInvitations().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		selectedPermission = null;
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		detailModal?.close();
		selectedShareArn = '';
		tabLoader.refresh(untrack(() => activeTab));
	});

	async function loadMoreShares(): Promise<void> {
		if (!sharesNextToken) return;
		try {
			const resp = await ram().send(new GetResourceSharesCommand({ resourceOwner: 'SELF', nextToken: sharesNextToken }));
			shares = [...shares, ...(resp.resourceShares ?? [])];
			sharesNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more shares: ' + describeError(e));
		}
	}
	async function loadMoreResources(): Promise<void> {
		if (!resourcesNextToken) return;
		try {
			const resp = await ram().send(new ListResourcesCommand({ resourceOwner: 'SELF', nextToken: resourcesNextToken }));
			resources = [...resources, ...(resp.resources ?? [])];
			resourcesNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more resources: ' + describeError(e));
		}
	}
	async function loadMorePrincipals(): Promise<void> {
		if (!principalsNextToken) return;
		try {
			const resp = await ram().send(new ListPrincipalsCommand({ resourceOwner: 'SELF', nextToken: principalsNextToken }));
			principals = [...principals, ...(resp.principals ?? [])];
			principalsNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more principals: ' + describeError(e));
		}
	}
	async function loadMorePermissions(): Promise<void> {
		if (!permissionsNextToken) return;
		try {
			const resp = await ram().send(new ListPermissionsCommand({ nextToken: permissionsNextToken }));
			permissions = [...permissions, ...(resp.permissions ?? [])];
			permissionsNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more permissions: ' + describeError(e));
		}
	}

	function matches(q: string, ...fields: (string | undefined)[]): boolean {
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}

	const filteredShares = $derived(shares.filter((s) => matches(searchQuery.toLowerCase(), s.name, s.resourceShareArn)));
	const filteredResources = $derived(resources.filter((r) => matches(searchQuery.toLowerCase(), r.arn, r.type)));
	const filteredPrincipals = $derived(principals.filter((p) => matches(searchQuery.toLowerCase(), p.id)));
	const filteredPermissions = $derived(permissions.filter((p) => matches(searchQuery.toLowerCase(), p.name, p.resourceType)));
	const filteredInvitations = $derived(invitations.filter((i) => matches(searchQuery.toLowerCase(), i.resourceShareName)));

	const activeShares = $derived(shares.filter((s) => s.status === 'ACTIVE').length);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Resource Share: create / update / delete / associate / disassociate / detail ---

	let createShareModal = $state<Modal | null>(null);
	let creatingShare = $state(false);
	let createShareError = $state<string | null>(null);
	let newShareName = $state('');
	let newShareResourceArns = $state('');
	let newSharePrincipals = $state('');
	let newSharePermissionArns = $state('');
	let newShareAllowExternal = $state(false);

	function openCreateShareModal(): void {
		createShareError = null;
		newShareName = '';
		newShareResourceArns = '';
		newSharePrincipals = '';
		newSharePermissionArns = '';
		newShareAllowExternal = false;
		createShareModal?.open();
	}

	async function submitCreateShare(): Promise<void> {
		if (!newShareName) {
			createShareError = 'Name is required.';
			return;
		}
		creatingShare = true;
		createShareError = null;
		try {
			const resourceArns = splitList(newShareResourceArns);
			const sharePrincipals = splitList(newSharePrincipals);
			const permissionArns = splitList(newSharePermissionArns);
			await ram().send(
				new CreateResourceShareCommand({
					name: newShareName,
					resourceArns: resourceArns.length > 0 ? resourceArns : undefined,
					principals: sharePrincipals.length > 0 ? sharePrincipals : undefined,
					permissionArns: permissionArns.length > 0 ? permissionArns : undefined,
					allowExternalPrincipals: newShareAllowExternal
				})
			);
			toast.success('Resource share created');
			createShareModal?.close();
			await tabLoader.refresh('shares');
		} catch (e) {
			const msg = describeError(e);
			createShareError = msg;
			toast.error(msg);
		} finally {
			creatingShare = false;
		}
	}

	async function deleteShare(s: ResourceShare): Promise<void> {
		if (!s.resourceShareArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete resource share',
			message: `Delete resource share "${s.name ?? s.resourceShareArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await ram().send(new DeleteResourceShareCommand({ resourceShareArn: s.resourceShareArn }));
			toast.success('Resource share deleted');
			await tabLoader.refresh('shares');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editShareModal = $state<Modal | null>(null);
	let editingShare = $state(false);
	let editShareError = $state<string | null>(null);
	let editShareName = $state('');
	let editShareAllowExternal = $state(false);

	function openEditShare(): void {
		editShareError = null;
		editShareName = selectedShare?.name ?? '';
		editShareAllowExternal = selectedShare?.allowExternalPrincipals ?? false;
		editShareModal?.open();
	}

	async function submitEditShare(): Promise<void> {
		editingShare = true;
		editShareError = null;
		try {
			await ram().send(
				new UpdateResourceShareCommand({
					resourceShareArn: selectedShareArn,
					name: editShareName || undefined,
					allowExternalPrincipals: editShareAllowExternal
				})
			);
			toast.success('Resource share updated');
			editShareModal?.close();
			await tabLoader.refresh('shares');
			await refreshShareDetail();
		} catch (e) {
			const msg = describeError(e);
			editShareError = msg;
			toast.error(msg);
		} finally {
			editingShare = false;
		}
	}

	let detailModal = $state<Modal | null>(null);
	let selectedShareArn = $state('');
	let selectedShare = $state<ResourceShare | null>(null);
	let shareResourceAssociations = $state<ResourceShareAssociation[]>([]);
	let sharePrincipalAssociations = $state<ResourceShareAssociation[]>([]);
	let loadingShareDetail = $state(false);
	let shareDetailError = $state<string | null>(null);
	let associateResourceArns = $state('');
	let associatePrincipals = $state('');
	let associating = $state(false);

	async function openShareDetail(s: ResourceShare): Promise<void> {
		selectedShare = s;
		selectedShareArn = s.resourceShareArn ?? '';
		shareResourceAssociations = [];
		sharePrincipalAssociations = [];
		shareDetailError = null;
		detailModal?.open();
		if (selectedShareArn) await refreshShareDetail();
	}

	async function refreshShareDetail(): Promise<void> {
		if (!selectedShareArn) return;
		loadingShareDetail = true;
		shareDetailError = null;
		try {
			const [resResp, prinResp] = await Promise.all([
				ram().send(new GetResourceShareAssociationsCommand({ associationType: 'RESOURCE', resourceShareArns: [selectedShareArn] })),
				ram().send(new GetResourceShareAssociationsCommand({ associationType: 'PRINCIPAL', resourceShareArns: [selectedShareArn] }))
			]);
			shareResourceAssociations = resResp.resourceShareAssociations ?? [];
			sharePrincipalAssociations = prinResp.resourceShareAssociations ?? [];
			const refreshed = shares.find((s) => s.resourceShareArn === selectedShareArn);
			if (refreshed) selectedShare = refreshed;
		} catch (e) {
			shareDetailError = describeError(e);
		} finally {
			loadingShareDetail = false;
		}
	}

	async function submitAssociate(): Promise<void> {
		const resourceArns = splitList(associateResourceArns);
		const principals2 = splitList(associatePrincipals);
		if (resourceArns.length === 0 && principals2.length === 0) {
			toast.error('Enter at least one resource ARN or principal to associate');
			return;
		}
		associating = true;
		try {
			await ram().send(
				new AssociateResourceShareCommand({
					resourceShareArn: selectedShareArn,
					resourceArns: resourceArns.length > 0 ? resourceArns : undefined,
					principals: principals2.length > 0 ? principals2 : undefined
				})
			);
			toast.success('Associated');
			associateResourceArns = '';
			associatePrincipals = '';
			await refreshShareDetail();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			associating = false;
		}
	}

	async function disassociateResource(resourceArn: string): Promise<void> {
		try {
			await ram().send(new DisassociateResourceShareCommand({ resourceShareArn: selectedShareArn, resourceArns: [resourceArn] }));
			toast.success('Disassociated');
			await refreshShareDetail();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function disassociatePrincipal(principal: string): Promise<void> {
		try {
			await ram().send(new DisassociateResourceShareCommand({ resourceShareArn: selectedShareArn, principals: [principal] }));
			toast.success('Disassociated');
			await refreshShareDetail();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Permission: create / delete / versions (existing) ---

	let createPermissionModal = $state<Modal | null>(null);
	let creatingPermission = $state(false);
	let createPermissionError = $state<string | null>(null);
	let newPermissionName = $state('');
	let newPermissionResourceType = $state('');
	let newPermissionPolicyTemplate = $state('{\n  "Effect": "Allow",\n  "Action": []\n}');

	function openCreatePermissionModal(): void {
		createPermissionError = null;
		newPermissionName = '';
		newPermissionResourceType = '';
		newPermissionPolicyTemplate = '{\n  "Effect": "Allow",\n  "Action": []\n}';
		createPermissionModal?.open();
	}

	async function submitCreatePermission(): Promise<void> {
		if (!newPermissionName || !newPermissionResourceType || !newPermissionPolicyTemplate) {
			createPermissionError = 'Name, resource type, and policy template are required.';
			return;
		}
		let policyTemplate: unknown;
		try {
			policyTemplate = JSON.parse(newPermissionPolicyTemplate);
		} catch {
			createPermissionError = 'Policy template must be valid JSON.';
			return;
		}
		creatingPermission = true;
		createPermissionError = null;
		try {
			await ram().send(
				new CreatePermissionCommand({
					name: newPermissionName,
					resourceType: newPermissionResourceType,
					policyTemplate: JSON.stringify(policyTemplate)
				})
			);
			toast.success('Permission created');
			createPermissionModal?.close();
			await tabLoader.refresh('permissions');
		} catch (e) {
			const msg = describeError(e);
			createPermissionError = msg;
			toast.error(msg);
		} finally {
			creatingPermission = false;
		}
	}

	async function deletePermission(p: ResourceSharePermissionSummary): Promise<void> {
		if (!p.arn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete permission',
			message: `Delete permission "${p.name ?? p.arn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await ram().send(new DeletePermissionCommand({ permissionArn: p.arn }));
			toast.success('Permission deleted');
			await tabLoader.refresh('permissions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let selectedPermission = $state<ResourceSharePermissionSummary | null>(null);
	let permissionVersions = $state<ResourceSharePermissionSummary[]>([]);
	let loadingVersions = $state(false);

	async function selectPermission(perm: ResourceSharePermissionSummary): Promise<void> {
		selectedPermission = perm;
		permissionVersions = [];
		if (!perm.arn) return;
		loadingVersions = true;
		try {
			const resp = await ram().send(new ListPermissionVersionsCommand({ permissionArn: perm.arn }));
			permissionVersions = resp.permissions ?? [];
		} catch (e) {
			toast.error('Failed to load permission versions: ' + describeError(e));
		} finally {
			loadingVersions = false;
		}
	}

	async function setDefaultVersion(permArn: string, version: number): Promise<void> {
		try {
			await ram().send(new SetDefaultPermissionVersionCommand({ permissionArn: permArn, permissionVersion: version }));
			toast.success('Default permission version updated');
			if (selectedPermission) await selectPermission(selectedPermission);
			await tabLoader.refresh('permissions');
		} catch (e) {
			toast.error('Failed to set default version: ' + describeError(e));
		}
	}

	let createVersionModal = $state<Modal | null>(null);
	let creatingVersion = $state(false);
	let createVersionError = $state<string | null>(null);
	let newVersionPolicyTemplate = $state('');

	function openCreateVersionModal(): void {
		createVersionError = null;
		newVersionPolicyTemplate = '';
		createVersionModal?.open();
	}

	async function submitCreateVersion(): Promise<void> {
		if (!selectedPermission?.arn) return;
		let policyTemplate: unknown;
		try {
			policyTemplate = JSON.parse(newVersionPolicyTemplate);
		} catch {
			createVersionError = 'Policy template must be valid JSON.';
			return;
		}
		creatingVersion = true;
		createVersionError = null;
		try {
			await ram().send(
				new CreatePermissionVersionCommand({ permissionArn: selectedPermission.arn, policyTemplate: JSON.stringify(policyTemplate) })
			);
			toast.success('Permission version created');
			createVersionModal?.close();
			await selectPermission(selectedPermission);
		} catch (e) {
			const msg = describeError(e);
			createVersionError = msg;
			toast.error(msg);
		} finally {
			creatingVersion = false;
		}
	}

	async function deleteVersion(permArn: string, version: string | undefined): Promise<void> {
		if (!version) return;
		const confirmed = await confirmDestructive({
			title: 'Delete permission version',
			message: `Delete version ${version}?`
		});
		if (!confirmed) return;
		try {
			await ram().send(new DeletePermissionVersionCommand({ permissionArn: permArn, permissionVersion: Number(version) }));
			toast.success('Permission version deleted');
			if (selectedPermission) await selectPermission(selectedPermission);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Invitations (existing accept/reject) ---

	async function acceptInvitation(invArn: string): Promise<void> {
		try {
			await ram().send(new AcceptResourceShareInvitationCommand({ resourceShareInvitationArn: invArn }));
			toast.success('Invitation accepted');
			await tabLoader.refresh('invitations');
		} catch (e) {
			toast.error('Failed to accept invitation: ' + describeError(e));
		}
	}

	async function rejectInvitation(invArn: string): Promise<void> {
		try {
			await ram().send(new RejectResourceShareInvitationCommand({ resourceShareInvitationArn: invArn }));
			toast.success('Invitation rejected');
			await tabLoader.refresh('invitations');
		} catch (e) {
			toast.error('Failed to reject invitation: ' + describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader icon={Share2} title="AWS Resource Access Manager" description="Share AWS resources across accounts and organizations" onRefresh={handleRefresh} color="cyan">
		{#snippet actions()}
			{#if activeTab === 'shares'}
				<button onclick={openCreateShareModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Plus class="w-4 h-4" /> Create resource share
				</button>
			{:else if activeTab === 'permissions' && !selectedPermission}
				<button onclick={openCreatePermissionModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Plus class="w-4 h-4" /> Create permission
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="grid grid-cols-2 sm:grid-cols-5 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-cyan-100 dark:bg-cyan-900/30 rounded-lg"><Share2 class="w-5 h-5 text-cyan-600 dark:text-cyan-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{shares.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resource Shares</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{activeShares}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active Shares</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Box class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{resources.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Shared Resources</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Users class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{principals.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Principals</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Bell class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{invitations.filter((i) => i.status === 'PENDING').length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Pending Invites</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="cyan" />
			<SearchInput bind:value={searchQuery} />
		</div>
		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'shares'}
				{#snippet shareStatusCell(s: ResourceShare)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.status)}">{s.status ?? '—'}</span>
				{/snippet}
				{#snippet shareActionsCell(s: ResourceShare)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openShareDetail(s)} title="View" aria-label="View resource share {s.name}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteShare(s)} title="Delete" aria-label="Delete resource share {s.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const shareColumns = defineColumns<ResourceShare>([
					{ key: 'name', label: 'Name' },
					{ key: 'status', label: 'Status', render: shareStatusCell },
					{ key: 'actions', label: '', render: shareActionsCell }
				])}
				<DataTable rows={filteredShares} rowKey={(s) => s.resourceShareArn ?? ''} columns={shareColumns} loading={tabLoader.isLoading('shares')} emptyMessage="No resource shares found" />
				<LoadMore hasMore={!!sharesNextToken} loading={false} onLoadMore={loadMoreShares} />
			{:else if activeTab === 'resources'}
				{@const resourceColumns = defineColumns<Resource>([
					{ key: 'arn', label: 'ARN' },
					{ key: 'type', label: 'Type' },
					{ key: 'status', label: 'Status' }
				])}
				<DataTable rows={filteredResources} rowKey={(r) => r.arn ?? ''} columns={resourceColumns} loading={tabLoader.isLoading('resources')} emptyMessage="No shared resources found" />
				<LoadMore hasMore={!!resourcesNextToken} loading={false} onLoadMore={loadMoreResources} />
			{:else if activeTab === 'principals'}
				{@const principalColumns = defineColumns<Principal>([{ key: 'id', label: 'Principal' }])}
				<DataTable rows={filteredPrincipals} rowKey={(p) => p.id ?? ''} columns={principalColumns} loading={tabLoader.isLoading('principals')} emptyMessage="No principals found" />
				<LoadMore hasMore={!!principalsNextToken} loading={false} onLoadMore={loadMorePrincipals} />
			{:else if activeTab === 'permissions'}
				{#if selectedPermission}
					<div class="mb-4">
						<button onclick={() => { selectedPermission = null; permissionVersions = []; }} class="text-sm text-cyan-600 dark:text-cyan-400 hover:underline">← Back to permissions</button>
						<div class="mt-2 flex items-center justify-between">
							<div>
								<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selectedPermission.name}</h2>
								<p class="text-xs text-gray-500 dark:text-gray-400">{selectedPermission.arn}</p>
							</div>
							<button onclick={openCreateVersionModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
								<Plus class="w-4 h-4" /> Create version
							</button>
						</div>
						{#if loadingVersions}
							<div class="text-center py-4 text-gray-500 dark:text-gray-400">Loading versions...</div>
						{:else if permissionVersions.length === 0}
							<div class="text-center py-4 text-gray-500 dark:text-gray-400">No versions found</div>
						{:else}
							<div class="space-y-2 mt-3">
								{#each permissionVersions as ver (ver.version)}
									<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
										<div class="flex items-center gap-3">
											<Key class="w-4 h-4 text-cyan-500" />
											<div>
												<p class="text-sm font-medium text-gray-900 dark:text-white">Version {ver.version}</p>
												{#if ver.defaultVersion}
													<span class="text-xs text-green-600 dark:text-green-400">Default</span>
												{/if}
											</div>
										</div>
										<div class="flex items-center gap-2">
											{#if !ver.defaultVersion && selectedPermission?.arn}
												<button onclick={() => setDefaultVersion(selectedPermission!.arn!, Number(ver.version))} class="text-xs px-3 py-1 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700">
													Set as Default
												</button>
												<button onclick={() => deleteVersion(selectedPermission!.arn!, ver.version)} title="Delete version" aria-label="Delete version {ver.version}" class="text-gray-400 hover:text-red-500">
													<Trash2 class="w-4 h-4" />
												</button>
											{/if}
										</div>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{:else}
					{#snippet permissionActionsCell(p: ResourceSharePermissionSummary)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => selectPermission(p)} title="Manage versions" aria-label="Manage versions for {p.name}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
							{#if p.permissionType !== 'AWS_MANAGED'}
								<button onclick={() => deletePermission(p)} title="Delete" aria-label="Delete permission {p.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
							{/if}
						</div>
					{/snippet}
					{@const permissionColumns = defineColumns<ResourceSharePermissionSummary>([
						{ key: 'name', label: 'Name' },
						{ key: 'resourceType', label: 'Resource Type' },
						{ key: 'version', label: 'Version' },
						{ key: 'actions', label: '', render: permissionActionsCell }
					])}
					<DataTable rows={filteredPermissions} rowKey={(p) => p.arn ?? ''} columns={permissionColumns} loading={tabLoader.isLoading('permissions')} emptyMessage="No permissions found" />
					<LoadMore hasMore={!!permissionsNextToken} loading={false} onLoadMore={loadMorePermissions} />
				{/if}
			{:else if activeTab === 'invitations'}
				{#snippet invitationActionsCell(inv: ResourceShareInvitation)}
					{#if inv.status === 'PENDING' && inv.resourceShareInvitationArn}
						<div class="flex gap-2 justify-end">
							<button onclick={() => acceptInvitation(inv.resourceShareInvitationArn!)} class="text-xs px-3 py-1 rounded-lg bg-green-600 text-white hover:bg-green-700">Accept</button>
							<button onclick={() => rejectInvitation(inv.resourceShareInvitationArn!)} class="text-xs px-3 py-1 rounded-lg bg-red-600 text-white hover:bg-red-700">Reject</button>
						</div>
					{:else}
						<span class="text-xs px-2 py-1 rounded-full {statusClass(inv.status)}">{inv.status}</span>
					{/if}
				{/snippet}
				{@const invitationColumns = defineColumns<ResourceShareInvitation>([
					{ key: 'resourceShareName', label: 'Share Name' },
					{ key: 'senderAccountId', label: 'From' },
					{ key: 'actions', label: 'Status / Actions', render: invitationActionsCell }
				])}
				<DataTable rows={filteredInvitations} rowKey={(i) => i.resourceShareInvitationArn ?? ''} columns={invitationColumns} loading={tabLoader.isLoading('invitations')} emptyMessage="No invitations found" />
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createShareModal} title="Create Resource Share">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ram-share-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ram-share-name" bind:value={newShareName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ram-share-resources" class="text-sm text-slate-600 dark:text-slate-300">Resource ARNs (comma-separated, optional)</label>
				<input id="ram-share-resources" bind:value={newShareResourceArns} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ram-share-principals" class="text-sm text-slate-600 dark:text-slate-300">Principals (comma-separated account IDs/ARNs, optional)</label>
				<input id="ram-share-principals" bind:value={newSharePrincipals} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ram-share-permissions" class="text-sm text-slate-600 dark:text-slate-300">Permission ARNs (comma-separated, optional -- defaults are auto-attached)</label>
				<input id="ram-share-permissions" bind:value={newSharePermissionArns} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newShareAllowExternal} /> Allow external (non-organization) principals
			</label>
			{#if createShareError}<p class="text-sm text-red-600 dark:text-red-400">{createShareError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createShareModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateShare} disabled={creatingShare} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{creatingShare ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editShareModal} title="Update Resource Share">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ram-edit-share-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ram-edit-share-name" bind:value={editShareName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editShareAllowExternal} /> Allow external (non-organization) principals
			</label>
			{#if editShareError}<p class="text-sm text-red-600 dark:text-red-400">{editShareError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editShareModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditShare} disabled={editingShare} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{editingShare ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createPermissionModal} title="Create Permission">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ram-perm-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ram-perm-name" bind:value={newPermissionName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ram-perm-type" class="text-sm text-slate-600 dark:text-slate-300">Resource type (e.g. ec2:Subnet)</label>
				<input id="ram-perm-type" bind:value={newPermissionResourceType} placeholder="ec2:Subnet" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ram-perm-policy" class="text-sm text-slate-600 dark:text-slate-300">Policy template (JSON)</label>
				<textarea id="ram-perm-policy" bind:value={newPermissionPolicyTemplate} rows="6" class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createPermissionError}<p class="text-sm text-red-600 dark:text-red-400">{createPermissionError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createPermissionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreatePermission} disabled={creatingPermission} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{creatingPermission ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createVersionModal} title="Create Permission Version">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ram-version-policy" class="text-sm text-slate-600 dark:text-slate-300">Policy template (JSON)</label>
				<textarea id="ram-version-policy" bind:value={newVersionPolicyTemplate} rows="6" class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createVersionError}<p class="text-sm text-red-600 dark:text-red-400">{createVersionError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createVersionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateVersion} disabled={creatingVersion} class="rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700 disabled:opacity-50">{creatingVersion ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Resource Share">
	{#snippet children()}
		{#if shareDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{shareDetailError}</p>
		{:else if selectedShare}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{selectedShare.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{selectedShare.resourceShareArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(selectedShare.status)}">{selectedShare.status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Allow external principals</dt><dd class="text-slate-900 dark:text-white">{selectedShare.allowExternalPrincipals ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(selectedShare.creationTime)}</dd></div>
			</dl>

			<div class="mt-4 space-y-2">
				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Associate resources / principals</h3>
				<div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
					<input bind:value={associateResourceArns} placeholder="Resource ARNs (comma-separated)" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					<input bind:value={associatePrincipals} placeholder="Principals (comma-separated)" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<button onclick={submitAssociate} disabled={associating} class="px-3 py-1.5 text-sm rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 disabled:opacity-50">{associating ? 'Associating…' : 'Associate'}</button>
			</div>

			{#if loadingShareDetail}
				<p class="mt-4 text-sm text-slate-500 dark:text-slate-400">Loading associations…</p>
			{:else}
				<div class="mt-4">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Associated resources ({shareResourceAssociations.length})</h3>
					{#if shareResourceAssociations.length === 0}
						<p class="text-xs text-slate-500 dark:text-slate-400">None</p>
					{:else}
						<div class="space-y-1">
							{#each shareResourceAssociations as a (a.associatedEntity)}
								<div class="flex items-center justify-between text-xs p-2 rounded bg-gray-50 dark:bg-slate-700/50">
									<span class="truncate">{a.associatedEntity}</span>
									<button onclick={() => disassociateResource(a.associatedEntity ?? '')} class="text-red-500 hover:underline shrink-0 ml-2">Disassociate</button>
								</div>
							{/each}
						</div>
					{/if}
				</div>
				<div class="mt-4">
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Associated principals ({sharePrincipalAssociations.length})</h3>
					{#if sharePrincipalAssociations.length === 0}
						<p class="text-xs text-slate-500 dark:text-slate-400">None</p>
					{:else}
						<div class="space-y-1">
							{#each sharePrincipalAssociations as a (a.associatedEntity)}
								<div class="flex items-center justify-between text-xs p-2 rounded bg-gray-50 dark:bg-slate-700/50">
									<span class="truncate">{a.associatedEntity}</span>
									<button onclick={() => disassociatePrincipal(a.associatedEntity ?? '')} class="text-red-500 hover:underline shrink-0 ml-2">Disassociate</button>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		<button type="button" onclick={refreshShareDetail} class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><RefreshCw class="w-4 h-4" /> Refresh</button>
		{#if selectedShare}
			<button type="button" onclick={openEditShare} class="flex items-center gap-2 rounded-lg bg-cyan-600 px-4 py-2 text-sm font-semibold text-white hover:bg-cyan-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>
