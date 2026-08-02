<script lang="ts">
	// WorkMail's real domain model is organization-scoped: an Organization
	// contains Users, Groups and Resources, and mailbox permissions are
	// granted on a specific user/resource's mailbox within an organization.
	// This page follows that shape -- Users/Groups/Resources/Mailbox
	// Permissions all read the currently selected organization, the same way
	// Timestream's Tables tab reads the selected database. The backend has
	// ~90 operations across many more families (aliases, mail domains,
	// access control rules, impersonation roles, mobile device access,
	// availability configurations, retention policies, mailbox export jobs,
	// personal access tokens, identity center/provider config, inbound
	// DMARC) -- all real and backend-supported, but out of scope for this
	// pass, which covers the four families named in the task brief
	// (organizations, users, groups, resources) plus mailbox permissions.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getWorkMailClient } from '$lib/aws-client';
	import {
		ListOrganizationsCommand,
		DescribeOrganizationCommand,
		CreateOrganizationCommand,
		DeleteOrganizationCommand,
		ListUsersCommand,
		DescribeUserCommand,
		CreateUserCommand,
		UpdateUserCommand,
		DeleteUserCommand,
		RegisterToWorkMailCommand,
		DeregisterFromWorkMailCommand,
		ListGroupsCommand,
		DescribeGroupCommand,
		CreateGroupCommand,
		UpdateGroupCommand,
		DeleteGroupCommand,
		ListResourcesCommand,
		DescribeResourceCommand,
		CreateResourceCommand,
		UpdateResourceCommand,
		DeleteResourceCommand,
		ListMailboxPermissionsCommand,
		PutMailboxPermissionsCommand,
		DeleteMailboxPermissionsCommand,
		type OrganizationSummary,
		type User,
		type Group,
		type Resource,
		type Permission,
		type UserRole,
		type ResourceType,
		type PermissionType
	} from '@aws-sdk/client-workmail';
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
	import { Mail, Plus, Trash2, Pencil, Eye, UserPlus, UserMinus, Key, Building2 } from 'lucide-svelte';

	const client = regionalClient(getWorkMailClient);

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

	function stateClass(s: string | undefined): string {
		if (s === 'ENABLED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (s === 'DELETED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	type TabId = 'organizations' | 'users' | 'groups' | 'resources' | 'mailboxPermissions';

	const tabs: TabDef[] = [
		{ id: 'organizations', label: 'Organizations' },
		{ id: 'users', label: 'Users' },
		{ id: 'groups', label: 'Groups' },
		{ id: 'resources', label: 'Resources' },
		{ id: 'mailboxPermissions', label: 'Mailbox Permissions' }
	];

	// Tabs scoped by the selected organization (or, for mailboxPermissions,
	// also by the selected mailbox entity) always force a reload on switch
	// rather than relying on tab-loader's cache-once-per-tab semantics --
	// otherwise switching organizations and then revisiting an
	// already-visited tab would silently show the PREVIOUS organization's
	// data. Only 'organizations' itself uses the cache-once default.
	const scopedTabs = new Set<TabId>(['users', 'groups', 'resources', 'mailboxPermissions']);

	let activeTab = $state<TabId>('organizations');
	let searchQuery = $state('');

	// ==================== Organizations ====================

	let organizations = $state<OrganizationSummary[]>([]);

	async function fetchOrganizations(): Promise<void> {
		const resp = await client().send(new ListOrganizationsCommand({}));
		organizations = resp.OrganizationSummaries ?? [];
	}

	const filteredOrganizations = $derived(
		organizations.filter((o) => (o.Alias ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ==================== Users / Groups / Resources (org-scoped) ====================

	let selectedOrgId = $state<string | null>(null);
	let users = $state<User[]>([]);
	let groups = $state<Group[]>([]);
	let resources = $state<Resource[]>([]);

	async function fetchUsers(): Promise<void> {
		const orgId = untrack(() => selectedOrgId);
		if (!orgId) {
			users = [];
			return;
		}
		const resp = await client().send(new ListUsersCommand({ OrganizationId: orgId }));
		users = resp.Users ?? [];
	}

	async function fetchGroups(): Promise<void> {
		const orgId = untrack(() => selectedOrgId);
		if (!orgId) {
			groups = [];
			return;
		}
		const resp = await client().send(new ListGroupsCommand({ OrganizationId: orgId }));
		groups = resp.Groups ?? [];
	}

	async function fetchResources(): Promise<void> {
		const orgId = untrack(() => selectedOrgId);
		if (!orgId) {
			resources = [];
			return;
		}
		const resp = await client().send(new ListResourcesCommand({ OrganizationId: orgId }));
		resources = resp.Resources ?? [];
	}

	function selectOrgAndSwitchTo(orgId: string, tab: TabId): void {
		selectedOrgId = orgId;
		activeTab = tab;
		searchQuery = '';
		tabLoader.refresh(tab);
	}

	function onOrgSelectorChange(orgId: string): void {
		selectedOrgId = orgId;
		selectedMailboxEntity = null;
		tabLoader.refresh(activeTab);
	}

	const filteredUsers = $derived(
		users.filter((u) => (u.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
			(u.Email ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredGroups = $derived(
		groups.filter((g) => (g.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
			(g.Email ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredResources = $derived(
		resources.filter((r) => (r.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
			(r.Email ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ==================== Mailbox Permissions (scoped to selectedMailboxEntity) ====================

	let selectedMailboxEntity = $state<{ id: string; label: string } | null>(null);
	let mailboxPermissions = $state<Permission[]>([]);

	async function fetchMailboxPermissions(): Promise<void> {
		const orgId = untrack(() => selectedOrgId);
		const entity = untrack(() => selectedMailboxEntity);
		if (!orgId || !entity) {
			mailboxPermissions = [];
			return;
		}
		const resp = await client().send(
			new ListMailboxPermissionsCommand({ OrganizationId: orgId, EntityId: entity.id })
		);
		mailboxPermissions = resp.Permissions ?? [];
	}

	function selectEntityForPermissions(id: string, label: string): void {
		selectedMailboxEntity = { id, label };
		activeTab = 'mailboxPermissions';
		searchQuery = '';
		tabLoader.refresh('mailboxPermissions');
	}

	// ==================== Tab loader ====================

	const tabLoader = createTabLoader<TabId>({
		organizations: () => fetchOrganizations().catch(rethrowDescribed),
		users: () => fetchUsers().catch(rethrowDescribed),
		groups: () => fetchGroups().catch(rethrowDescribed),
		resources: () => fetchResources().catch(rethrowDescribed),
		mailboxPermissions: () => fetchMailboxPermissions().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		if (scopedTabs.has(activeTab)) {
			tabLoader.refresh(activeTab);
		} else {
			tabLoader.load(activeTab);
		}
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		selectedOrgId = null;
		selectedMailboxEntity = null;
		users = [];
		groups = [];
		resources = [];
		mailboxPermissions = [];
		tabLoader.refresh('organizations');
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// ==================== Organization detail (Describe) ====================

	let orgDetailModal = $state<Modal | null>(null);
	let orgDetail = $state<{
		OrganizationId?: string;
		Alias?: string;
		State?: string;
		DefaultMailDomain?: string;
		CompletedDate?: Date;
	} | null>(null);
	let orgDetailLoading = $state(false);

	async function openOrgDetail(o: OrganizationSummary): Promise<void> {
		if (!o.OrganizationId) return;
		orgDetail = null;
		orgDetailLoading = true;
		orgDetailModal?.open();
		try {
			const resp = await client().send(new DescribeOrganizationCommand({ OrganizationId: o.OrganizationId }));
			orgDetail = resp;
		} catch (e) {
			toast.error(describeError(e));
			orgDetailModal?.close();
		} finally {
			orgDetailLoading = false;
		}
	}

	// ==================== Create / Delete Organization ====================
	// (no UpdateOrganization operation exists on the real API)

	let orgCreateModal = $state<Modal | null>(null);
	let orgCreating = $state(false);
	let orgCreateError = $state<string | null>(null);
	let newOrgAlias = $state('');
	let newOrgDomain = $state('');

	function openOrgCreateModal(): void {
		orgCreateError = null;
		newOrgAlias = '';
		newOrgDomain = '';
		orgCreateModal?.open();
	}

	async function submitCreateOrg(): Promise<void> {
		if (!newOrgAlias) {
			orgCreateError = 'Organization alias is required.';
			return;
		}
		orgCreating = true;
		orgCreateError = null;
		try {
			await client().send(
				new CreateOrganizationCommand({
					Alias: newOrgAlias,
					Domains: newOrgDomain ? [{ DomainName: newOrgDomain }] : undefined
				})
			);
			toast.success(`Organization "${newOrgAlias}" created`);
			orgCreateModal?.close();
			await tabLoader.refresh('organizations');
		} catch (e) {
			const msg = describeError(e);
			orgCreateError = msg;
			toast.error(msg);
		} finally {
			orgCreating = false;
		}
	}

	async function deleteOrganization(o: OrganizationSummary): Promise<void> {
		if (!o.OrganizationId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete organization',
			message: `Delete organization "${o.Alias ?? o.OrganizationId}"? All users, groups and resources within it will be permanently lost.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteOrganizationCommand({ OrganizationId: o.OrganizationId, DeleteDirectory: false }));
			toast.success('Organization deleted');
			if (selectedOrgId === o.OrganizationId) selectedOrgId = null;
			await tabLoader.refresh('organizations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Create / Edit / Delete User ====================

	let userCreateModal = $state<Modal | null>(null);
	let userCreating = $state(false);
	let userCreateError = $state<string | null>(null);
	let newUserName = $state('');
	let newUserDisplayName = $state('');
	let newUserRole = $state<UserRole>('USER');

	function openUserCreateModal(): void {
		if (!selectedOrgId) {
			toast.error('Select an organization first');
			return;
		}
		userCreateError = null;
		newUserName = '';
		newUserDisplayName = '';
		newUserRole = 'USER';
		userCreateModal?.open();
	}

	async function submitCreateUser(): Promise<void> {
		if (!selectedOrgId || !newUserName || !newUserDisplayName) {
			userCreateError = 'Name and display name are required.';
			return;
		}
		userCreating = true;
		userCreateError = null;
		try {
			await client().send(
				new CreateUserCommand({
					OrganizationId: selectedOrgId,
					Name: newUserName,
					DisplayName: newUserDisplayName,
					Role: newUserRole
				})
			);
			toast.success(`User "${newUserName}" created`);
			userCreateModal?.close();
			await tabLoader.refresh('users');
		} catch (e) {
			const msg = describeError(e);
			userCreateError = msg;
			toast.error(msg);
		} finally {
			userCreating = false;
		}
	}

	let userDetailModal = $state<Modal | null>(null);
	let userDetail = $state<{
		UserId?: string;
		Name?: string;
		Email?: string;
		DisplayName?: string;
		State?: string;
		UserRole?: string;
	} | null>(null);
	let userDetailLoading = $state(false);

	async function openUserDetail(u: User): Promise<void> {
		if (!selectedOrgId || !u.Id) return;
		userDetail = null;
		userDetailLoading = true;
		userDetailModal?.open();
		try {
			const resp = await client().send(new DescribeUserCommand({ OrganizationId: selectedOrgId, UserId: u.Id }));
			userDetail = resp;
		} catch (e) {
			toast.error(describeError(e));
			userDetailModal?.close();
		} finally {
			userDetailLoading = false;
		}
	}

	let userEditModal = $state<Modal | null>(null);
	let userEditing = $state(false);
	let userEditError = $state<string | null>(null);
	let editUserId = $state('');
	let editUserDisplayName = $state('');

	function openUserEditModal(u: User): void {
		userEditError = null;
		editUserId = u.Id ?? '';
		editUserDisplayName = u.DisplayName ?? '';
		userEditModal?.open();
	}

	async function submitEditUser(): Promise<void> {
		if (!selectedOrgId || !editUserId) return;
		userEditing = true;
		userEditError = null;
		try {
			await client().send(
				new UpdateUserCommand({ OrganizationId: selectedOrgId, UserId: editUserId, DisplayName: editUserDisplayName })
			);
			toast.success('User updated');
			userEditModal?.close();
			await tabLoader.refresh('users');
		} catch (e) {
			const msg = describeError(e);
			userEditError = msg;
			toast.error(msg);
		} finally {
			userEditing = false;
		}
	}

	async function deleteUser(u: User): Promise<void> {
		if (!selectedOrgId || !u.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete user',
			message: `Delete user "${u.Name ?? u.Id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteUserCommand({ OrganizationId: selectedOrgId, UserId: u.Id }));
			toast.success('User deleted');
			await tabLoader.refresh('users');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Create / Edit / Delete Group ====================

	let groupCreateModal = $state<Modal | null>(null);
	let groupCreating = $state(false);
	let groupCreateError = $state<string | null>(null);
	let newGroupName = $state('');

	function openGroupCreateModal(): void {
		if (!selectedOrgId) {
			toast.error('Select an organization first');
			return;
		}
		groupCreateError = null;
		newGroupName = '';
		groupCreateModal?.open();
	}

	async function submitCreateGroup(): Promise<void> {
		if (!selectedOrgId || !newGroupName) {
			groupCreateError = 'Group name is required.';
			return;
		}
		groupCreating = true;
		groupCreateError = null;
		try {
			await client().send(new CreateGroupCommand({ OrganizationId: selectedOrgId, Name: newGroupName }));
			toast.success(`Group "${newGroupName}" created`);
			groupCreateModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			groupCreateError = msg;
			toast.error(msg);
		} finally {
			groupCreating = false;
		}
	}

	let groupDetailModal = $state<Modal | null>(null);
	let groupDetail = $state<{ GroupId?: string; Name?: string; Email?: string; State?: string } | null>(null);
	let groupDetailLoading = $state(false);

	async function openGroupDetail(g: Group): Promise<void> {
		if (!selectedOrgId || !g.Id) return;
		groupDetail = null;
		groupDetailLoading = true;
		groupDetailModal?.open();
		try {
			const resp = await client().send(new DescribeGroupCommand({ OrganizationId: selectedOrgId, GroupId: g.Id }));
			groupDetail = resp;
		} catch (e) {
			toast.error(describeError(e));
			groupDetailModal?.close();
		} finally {
			groupDetailLoading = false;
		}
	}

	let groupEditModal = $state<Modal | null>(null);
	let groupEditing = $state(false);
	let groupEditError = $state<string | null>(null);
	let editGroupId = $state('');
	let editGroupHidden = $state(false);

	function openGroupEditModal(g: Group): void {
		groupEditError = null;
		editGroupId = g.Id ?? '';
		editGroupHidden = false;
		groupEditModal?.open();
	}

	async function submitEditGroup(): Promise<void> {
		if (!selectedOrgId || !editGroupId) return;
		groupEditing = true;
		groupEditError = null;
		try {
			await client().send(
				new UpdateGroupCommand({
					OrganizationId: selectedOrgId,
					GroupId: editGroupId,
					HiddenFromGlobalAddressList: editGroupHidden
				})
			);
			toast.success('Group updated');
			groupEditModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			groupEditError = msg;
			toast.error(msg);
		} finally {
			groupEditing = false;
		}
	}

	async function deleteGroup(g: Group): Promise<void> {
		if (!selectedOrgId || !g.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete group',
			message: `Delete group "${g.Name ?? g.Id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteGroupCommand({ OrganizationId: selectedOrgId, GroupId: g.Id }));
			toast.success('Group deleted');
			await tabLoader.refresh('groups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Create / Edit / Delete Resource ====================

	let resourceCreateModal = $state<Modal | null>(null);
	let resourceCreating = $state(false);
	let resourceCreateError = $state<string | null>(null);
	let newResourceName = $state('');
	let newResourceType = $state<ResourceType>('ROOM');
	let newResourceDescription = $state('');

	function openResourceCreateModal(): void {
		if (!selectedOrgId) {
			toast.error('Select an organization first');
			return;
		}
		resourceCreateError = null;
		newResourceName = '';
		newResourceType = 'ROOM';
		newResourceDescription = '';
		resourceCreateModal?.open();
	}

	async function submitCreateResource(): Promise<void> {
		if (!selectedOrgId || !newResourceName) {
			resourceCreateError = 'Resource name is required.';
			return;
		}
		resourceCreating = true;
		resourceCreateError = null;
		try {
			await client().send(
				new CreateResourceCommand({
					OrganizationId: selectedOrgId,
					Name: newResourceName,
					Type: newResourceType,
					Description: newResourceDescription || undefined
				})
			);
			toast.success(`Resource "${newResourceName}" created`);
			resourceCreateModal?.close();
			await tabLoader.refresh('resources');
		} catch (e) {
			const msg = describeError(e);
			resourceCreateError = msg;
			toast.error(msg);
		} finally {
			resourceCreating = false;
		}
	}

	let resourceDetailModal = $state<Modal | null>(null);
	let resourceDetail = $state<{ ResourceId?: string; Name?: string; Email?: string; Type?: string; State?: string } | null>(
		null
	);
	let resourceDetailLoading = $state(false);

	async function openResourceDetail(r: Resource): Promise<void> {
		if (!selectedOrgId || !r.Id) return;
		resourceDetail = null;
		resourceDetailLoading = true;
		resourceDetailModal?.open();
		try {
			const resp = await client().send(new DescribeResourceCommand({ OrganizationId: selectedOrgId, ResourceId: r.Id }));
			resourceDetail = resp;
		} catch (e) {
			toast.error(describeError(e));
			resourceDetailModal?.close();
		} finally {
			resourceDetailLoading = false;
		}
	}

	let resourceEditModal = $state<Modal | null>(null);
	let resourceEditing = $state(false);
	let resourceEditError = $state<string | null>(null);
	let editResourceId = $state('');
	let editResourceName = $state('');
	let editResourceDescription = $state('');

	function openResourceEditModal(r: Resource): void {
		resourceEditError = null;
		editResourceId = r.Id ?? '';
		editResourceName = r.Name ?? '';
		editResourceDescription = '';
		resourceEditModal?.open();
	}

	async function submitEditResource(): Promise<void> {
		if (!selectedOrgId || !editResourceId) return;
		resourceEditing = true;
		resourceEditError = null;
		try {
			await client().send(
				new UpdateResourceCommand({
					OrganizationId: selectedOrgId,
					ResourceId: editResourceId,
					Name: editResourceName || undefined,
					Description: editResourceDescription || undefined
				})
			);
			toast.success('Resource updated');
			resourceEditModal?.close();
			await tabLoader.refresh('resources');
		} catch (e) {
			const msg = describeError(e);
			resourceEditError = msg;
			toast.error(msg);
		} finally {
			resourceEditing = false;
		}
	}

	async function deleteResource(r: Resource): Promise<void> {
		if (!selectedOrgId || !r.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete resource',
			message: `Delete resource "${r.Name ?? r.Id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteResourceCommand({ OrganizationId: selectedOrgId, ResourceId: r.Id }));
			toast.success('Resource deleted');
			await tabLoader.refresh('resources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Register / Deregister (users, groups, resources) ====================

	let registerModal = $state<Modal | null>(null);
	let registering = $state(false);
	let registerError = $state<string | null>(null);
	let registerEntityId = $state('');
	let registerEntityLabel = $state('');
	let registerEmail = $state('');
	let registerRefreshTab = $state<TabId>('users');

	function openRegisterModal(entityId: string, label: string, tab: TabId): void {
		registerError = null;
		registerEntityId = entityId;
		registerEntityLabel = label;
		registerEmail = '';
		registerRefreshTab = tab;
		registerModal?.open();
	}

	async function submitRegister(): Promise<void> {
		if (!selectedOrgId || !registerEntityId || !registerEmail) {
			registerError = 'Email is required.';
			return;
		}
		registering = true;
		registerError = null;
		try {
			await client().send(
				new RegisterToWorkMailCommand({ OrganizationId: selectedOrgId, EntityId: registerEntityId, Email: registerEmail })
			);
			toast.success(`"${registerEntityLabel}" registered to WorkMail`);
			registerModal?.close();
			await tabLoader.refresh(registerRefreshTab);
		} catch (e) {
			const msg = describeError(e);
			registerError = msg;
			toast.error(msg);
		} finally {
			registering = false;
		}
	}

	async function deregisterEntity(entityId: string, label: string, tab: TabId): Promise<void> {
		if (!selectedOrgId) return;
		try {
			await client().send(new DeregisterFromWorkMailCommand({ OrganizationId: selectedOrgId, EntityId: entityId }));
			toast.success(`"${label}" deregistered from WorkMail`);
			await tabLoader.refresh(tab);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Mailbox Permissions: create (Put) / delete ====================

	let permCreateModal = $state<Modal | null>(null);
	let permCreating = $state(false);
	let permCreateError = $state<string | null>(null);
	let newPermGranteeId = $state('');
	let newPermFullAccess = $state(true);
	let newPermSendAs = $state(false);
	let newPermSendOnBehalf = $state(false);

	function openPermCreateModal(): void {
		permCreateError = null;
		newPermGranteeId = '';
		newPermFullAccess = true;
		newPermSendAs = false;
		newPermSendOnBehalf = false;
		permCreateModal?.open();
	}

	async function submitCreatePermission(): Promise<void> {
		if (!selectedOrgId || !selectedMailboxEntity || !newPermGranteeId) {
			permCreateError = 'Grantee ID is required.';
			return;
		}
		const values = [
			newPermFullAccess ? 'FULL_ACCESS' : null,
			newPermSendAs ? 'SEND_AS' : null,
			newPermSendOnBehalf ? 'SEND_ON_BEHALF' : null
		].filter(Boolean) as PermissionType[];
		if (values.length === 0) {
			permCreateError = 'Select at least one permission.';
			return;
		}
		permCreating = true;
		permCreateError = null;
		try {
			await client().send(
				new PutMailboxPermissionsCommand({
					OrganizationId: selectedOrgId,
					EntityId: selectedMailboxEntity.id,
					GranteeId: newPermGranteeId,
					PermissionValues: values
				})
			);
			toast.success('Mailbox permission granted');
			permCreateModal?.close();
			await tabLoader.refresh('mailboxPermissions');
		} catch (e) {
			const msg = describeError(e);
			permCreateError = msg;
			toast.error(msg);
		} finally {
			permCreating = false;
		}
	}

	async function deletePermission(p: Permission): Promise<void> {
		if (!selectedOrgId || !selectedMailboxEntity) return;
		const confirmed = await confirmDestructive({
			title: 'Revoke mailbox permission',
			message: `Revoke all permissions granted to "${p.GranteeId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteMailboxPermissionsCommand({
					OrganizationId: selectedOrgId,
					EntityId: selectedMailboxEntity.id,
					GranteeId: p.GranteeId
				})
			);
			toast.success('Mailbox permission revoked');
			await tabLoader.refresh('mailboxPermissions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

{#snippet orgStateCell(o: OrganizationSummary)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(o.State)}">{o.State}</span>
{/snippet}
{#snippet orgActionsCell(o: OrganizationSummary)}
	<div class="flex items-center gap-2 justify-end">
		<button onclick={() => selectOrgAndSwitchTo(o.OrganizationId ?? '', 'users')} title="Manage users" aria-label="Manage users in {o.Alias}" class="text-gray-400 hover:text-green-500"><Building2 class="w-4 h-4" /></button>
		<button onclick={() => openOrgDetail(o)} title="View" aria-label="View organization {o.Alias}" class="text-gray-400 hover:text-green-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => deleteOrganization(o)} title="Delete" aria-label="Delete organization {o.Alias}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}

{#snippet userStateCell(u: User)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(u.State)}">{u.State}</span>
{/snippet}
{#snippet userActionsCell(u: User)}
	<div class="flex items-center gap-2 justify-end">
		{#if u.State === 'ENABLED'}
			<button onclick={() => deregisterEntity(u.Id ?? '', u.Name ?? '', 'users')} title="Deregister" aria-label="Deregister {u.Name}" class="text-gray-400 hover:text-amber-500"><UserMinus class="w-4 h-4" /></button>
		{:else}
			<button onclick={() => openRegisterModal(u.Id ?? '', u.Name ?? '', 'users')} title="Register" aria-label="Register {u.Name}" class="text-gray-400 hover:text-green-500"><UserPlus class="w-4 h-4" /></button>
		{/if}
		<button onclick={() => selectEntityForPermissions(u.Id ?? '', u.Name ?? '')} title="Mailbox permissions" aria-label="Mailbox permissions for {u.Name}" class="text-gray-400 hover:text-green-500"><Key class="w-4 h-4" /></button>
		<button onclick={() => openUserDetail(u)} title="View" aria-label="View user {u.Name}" class="text-gray-400 hover:text-green-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => openUserEditModal(u)} title="Edit" aria-label="Edit user {u.Name}" class="text-gray-400 hover:text-green-500"><Pencil class="w-4 h-4" /></button>
		<button onclick={() => deleteUser(u)} title="Delete" aria-label="Delete user {u.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}

{#snippet groupStateCell(g: Group)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(g.State)}">{g.State}</span>
{/snippet}
{#snippet groupActionsCell(g: Group)}
	<div class="flex items-center gap-2 justify-end">
		{#if g.State === 'ENABLED'}
			<button onclick={() => deregisterEntity(g.Id ?? '', g.Name ?? '', 'groups')} title="Deregister" aria-label="Deregister {g.Name}" class="text-gray-400 hover:text-amber-500"><UserMinus class="w-4 h-4" /></button>
		{:else}
			<button onclick={() => openRegisterModal(g.Id ?? '', g.Name ?? '', 'groups')} title="Register" aria-label="Register {g.Name}" class="text-gray-400 hover:text-green-500"><UserPlus class="w-4 h-4" /></button>
		{/if}
		<button onclick={() => openGroupDetail(g)} title="View" aria-label="View group {g.Name}" class="text-gray-400 hover:text-green-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => openGroupEditModal(g)} title="Edit" aria-label="Edit group {g.Name}" class="text-gray-400 hover:text-green-500"><Pencil class="w-4 h-4" /></button>
		<button onclick={() => deleteGroup(g)} title="Delete" aria-label="Delete group {g.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}

{#snippet resourceStateCell(r: Resource)}
	<span class="text-xs px-2 py-1 rounded-full {stateClass(r.State)}">{r.State}</span>
{/snippet}
{#snippet resourceActionsCell(r: Resource)}
	<div class="flex items-center gap-2 justify-end">
		{#if r.State === 'ENABLED'}
			<button onclick={() => deregisterEntity(r.Id ?? '', r.Name ?? '', 'resources')} title="Deregister" aria-label="Deregister {r.Name}" class="text-gray-400 hover:text-amber-500"><UserMinus class="w-4 h-4" /></button>
		{:else}
			<button onclick={() => openRegisterModal(r.Id ?? '', r.Name ?? '', 'resources')} title="Register" aria-label="Register {r.Name}" class="text-gray-400 hover:text-green-500"><UserPlus class="w-4 h-4" /></button>
		{/if}
		<button onclick={() => selectEntityForPermissions(r.Id ?? '', r.Name ?? '')} title="Mailbox permissions" aria-label="Mailbox permissions for {r.Name}" class="text-gray-400 hover:text-green-500"><Key class="w-4 h-4" /></button>
		<button onclick={() => openResourceDetail(r)} title="View" aria-label="View resource {r.Name}" class="text-gray-400 hover:text-green-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => openResourceEditModal(r)} title="Edit" aria-label="Edit resource {r.Name}" class="text-gray-400 hover:text-green-500"><Pencil class="w-4 h-4" /></button>
		<button onclick={() => deleteResource(r)} title="Delete" aria-label="Delete resource {r.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}

{#snippet permValuesCell(p: Permission)}
	<span class="text-xs font-mono">{p.PermissionValues?.join(', ')}</span>
{/snippet}
{#snippet permActionsCell(p: Permission)}
	<div class="flex items-center gap-2 justify-end">
		<button onclick={() => deletePermission(p)} title="Delete" aria-label="Revoke permission for {p.GranteeId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}

<div class="p-6 space-y-6">
	<PageHeader
		icon={Mail}
		title="Amazon WorkMail"
		description="Managed business email and calendaring, organized by organization"
		onRefresh={handleRefresh}
		color="green"
	>
		{#snippet actions()}
			{#if activeTab === 'organizations'}
				<button onclick={openOrgCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm">
					<Plus class="w-4 h-4" /> Create organization
				</button>
			{:else if activeTab === 'users'}
				<button onclick={openUserCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm">
					<Plus class="w-4 h-4" /> Create user
				</button>
			{:else if activeTab === 'groups'}
				<button onclick={openGroupCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm">
					<Plus class="w-4 h-4" /> Create group
				</button>
			{:else if activeTab === 'resources'}
				<button onclick={openResourceCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm">
					<Plus class="w-4 h-4" /> Create resource
				</button>
			{:else if activeTab === 'mailboxPermissions'}
				<button onclick={openPermCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-green-600 text-white hover:bg-green-700 text-sm">
					<Plus class="w-4 h-4" /> Grant permission
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="green" />
			<SearchInput bind:value={searchQuery} />
		</div>

		{#if activeTab === 'users' || activeTab === 'groups' || activeTab === 'resources' || activeTab === 'mailboxPermissions'}
			<div class="px-4 pt-4 flex items-center gap-2 text-sm">
				<Building2 class="w-4 h-4 text-gray-400" />
				<label for="wm-org-select" class="text-gray-500 dark:text-gray-400">Organization</label>
				<select
					id="wm-org-select"
					value={selectedOrgId ?? ''}
					onchange={(e) => onOrgSelectorChange((e.target as HTMLSelectElement).value)}
					class="px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
				>
					<option value="" disabled>Select an organization…</option>
					{#each organizations as o (o.OrganizationId)}
						<option value={o.OrganizationId}>{o.Alias}</option>
					{/each}
				</select>
				{#if activeTab === 'mailboxPermissions' && selectedMailboxEntity}
					<span class="text-gray-400">/</span>
					<span class="font-medium text-gray-700 dark:text-gray-300">{selectedMailboxEntity.label}</span>
				{/if}
			</div>
		{/if}

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'organizations'}
				{@const orgColumns = defineColumns<OrganizationSummary>([
					{ key: 'Alias', label: 'Alias' },
					{ key: 'OrganizationId', label: 'Organization ID' },
					{ key: 'DefaultMailDomain', label: 'Default Domain' },
					{ key: 'State', label: 'State', render: orgStateCell },
					{ key: 'actions', label: '', render: orgActionsCell }
				])}
				<DataTable
					rows={filteredOrganizations}
					rowKey={(o) => o.OrganizationId ?? ''}
					columns={orgColumns}
					loading={tabLoader.isLoading('organizations')}
					emptyMessage="No organizations found"
				/>
			{:else if activeTab === 'users'}
				{#if !selectedOrgId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select an organization to view its users</div>
				{:else}
					{@const userColumns = defineColumns<User>([
						{ key: 'Name', label: 'Name' },
						{ key: 'Email', label: 'Email' },
						{ key: 'UserRole', label: 'Role' },
						{ key: 'State', label: 'State', render: userStateCell },
						{ key: 'actions', label: '', render: userActionsCell }
					])}
					<DataTable
						rows={filteredUsers}
						rowKey={(u) => u.Id ?? ''}
						columns={userColumns}
						loading={tabLoader.isLoading('users')}
						emptyMessage="No users found in this organization"
					/>
				{/if}
			{:else if activeTab === 'groups'}
				{#if !selectedOrgId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select an organization to view its groups</div>
				{:else}
					{@const groupColumns = defineColumns<Group>([
						{ key: 'Name', label: 'Name' },
						{ key: 'Email', label: 'Email' },
						{ key: 'State', label: 'State', render: groupStateCell },
						{ key: 'actions', label: '', render: groupActionsCell }
					])}
					<DataTable
						rows={filteredGroups}
						rowKey={(g) => g.Id ?? ''}
						columns={groupColumns}
						loading={tabLoader.isLoading('groups')}
						emptyMessage="No groups found in this organization"
					/>
				{/if}
			{:else if activeTab === 'resources'}
				{#if !selectedOrgId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select an organization to view its resources</div>
				{:else}
					{@const resourceColumns = defineColumns<Resource>([
						{ key: 'Name', label: 'Name' },
						{ key: 'Email', label: 'Email' },
						{ key: 'Type', label: 'Type' },
						{ key: 'State', label: 'State', render: resourceStateCell },
						{ key: 'actions', label: '', render: resourceActionsCell }
					])}
					<DataTable
						rows={filteredResources}
						rowKey={(r) => r.Id ?? ''}
						columns={resourceColumns}
						loading={tabLoader.isLoading('resources')}
						emptyMessage="No resources found in this organization"
					/>
				{/if}
			{:else if activeTab === 'mailboxPermissions'}
				{#if !selectedOrgId || !selectedMailboxEntity}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">
						Select a user or resource's "Mailbox permissions" action (Users / Resources tabs) to manage its permissions
					</div>
				{:else}
					{@const permColumns = defineColumns<Permission>([
						{ key: 'GranteeId', label: 'Grantee' },
						{ key: 'GranteeType', label: 'Grantee Type' },
						{ key: 'PermissionValues', label: 'Permissions', render: permValuesCell },
						{ key: 'actions', label: '', render: permActionsCell }
					])}
					<DataTable
						rows={mailboxPermissions}
						rowKey={(p) => p.GranteeId ?? ''}
						columns={permColumns}
						loading={tabLoader.isLoading('mailboxPermissions')}
						emptyMessage="No mailbox permissions granted"
					/>
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Create Organization -->
<Modal bind:this={orgCreateModal} title="Create Organization">
	{#snippet children()}
		<div class="space-y-3">
			{#if orgCreateError}<p class="text-sm text-red-600 dark:text-red-400">{orgCreateError}</p>{/if}
			<div>
				<label for="new-org-alias" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Alias</label>
				<input id="new-org-alias" bind:value={newOrgAlias} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-org-domain" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Email Domain <span class="text-gray-400">(optional)</span></label>
				<input id="new-org-domain" bind:value={newOrgDomain} placeholder="example.com" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => orgCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateOrg} disabled={orgCreating} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{orgCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Organization Detail -->
<Modal bind:this={orgDetailModal} title="Organization Detail">
	{#snippet children()}
		{#if orgDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if orgDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Organization ID</span><span class="font-mono text-xs">{orgDetail.OrganizationId}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Alias</span><span>{orgDetail.Alias}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">State</span><span>{orgDetail.State}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Default Domain</span><span>{orgDetail.DefaultMailDomain ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Completed</span><span>{formatDate(orgDetail.CompletedDate)}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => orgDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Create User -->
<Modal bind:this={userCreateModal} title="Create User">
	{#snippet children()}
		<div class="space-y-3">
			{#if userCreateError}<p class="text-sm text-red-600 dark:text-red-400">{userCreateError}</p>{/if}
			<div>
				<label for="new-user-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Name</label>
				<input id="new-user-name" bind:value={newUserName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-user-display" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Display Name</label>
				<input id="new-user-display" bind:value={newUserDisplayName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-user-role" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Role</label>
				<select id="new-user-role" bind:value={newUserRole} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
					<option value="USER">USER</option>
					<option value="REMOTE_USER">REMOTE_USER</option>
				</select>
			</div>
			<p class="text-xs text-gray-400">Use the Register action after creating to give the user a mailbox and email address.</p>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => userCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateUser} disabled={userCreating} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{userCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Edit User -->
<Modal bind:this={userEditModal} title="Edit User">
	{#snippet children()}
		<div class="space-y-3">
			{#if userEditError}<p class="text-sm text-red-600 dark:text-red-400">{userEditError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400 font-mono">{editUserId}</p>
			<div>
				<label for="edit-user-display" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Display Name</label>
				<input id="edit-user-display" bind:value={editUserDisplayName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => userEditModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitEditUser} disabled={userEditing} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{userEditing ? 'Saving…' : 'Save'}
		</button>
	{/snippet}
</Modal>

<!-- User Detail -->
<Modal bind:this={userDetailModal} title="User Detail">
	{#snippet children()}
		{#if userDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if userDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">User ID</span><span class="font-mono text-xs">{userDetail.UserId}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Name</span><span>{userDetail.Name}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Display Name</span><span>{userDetail.DisplayName}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Email</span><span>{userDetail.Email ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Role</span><span>{userDetail.UserRole}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">State</span><span>{userDetail.State}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => userDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Create Group -->
<Modal bind:this={groupCreateModal} title="Create Group">
	{#snippet children()}
		<div class="space-y-3">
			{#if groupCreateError}<p class="text-sm text-red-600 dark:text-red-400">{groupCreateError}</p>{/if}
			<div>
				<label for="new-group-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Name</label>
				<input id="new-group-name" bind:value={newGroupName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => groupCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateGroup} disabled={groupCreating} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{groupCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Edit Group -->
<Modal bind:this={groupEditModal} title="Edit Group">
	{#snippet children()}
		<div class="space-y-3">
			{#if groupEditError}<p class="text-sm text-red-600 dark:text-red-400">{groupEditError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400 font-mono">{editGroupId}</p>
			<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
				<input type="checkbox" bind:checked={editGroupHidden} class="rounded" /> Hidden from global address list
			</label>
			<p class="text-xs text-gray-400">
				WorkMail does not return the group's current value for this field (it is write-only on
				DescribeGroup/ListGroups) -- check or uncheck to set it explicitly; it does not reflect the
				existing state.
			</p>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => groupEditModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitEditGroup} disabled={groupEditing} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{groupEditing ? 'Saving…' : 'Save'}
		</button>
	{/snippet}
</Modal>

<!-- Group Detail -->
<Modal bind:this={groupDetailModal} title="Group Detail">
	{#snippet children()}
		{#if groupDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if groupDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Group ID</span><span class="font-mono text-xs">{groupDetail.GroupId}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Name</span><span>{groupDetail.Name}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Email</span><span>{groupDetail.Email ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">State</span><span>{groupDetail.State}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => groupDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Create Resource -->
<Modal bind:this={resourceCreateModal} title="Create Resource">
	{#snippet children()}
		<div class="space-y-3">
			{#if resourceCreateError}<p class="text-sm text-red-600 dark:text-red-400">{resourceCreateError}</p>{/if}
			<div>
				<label for="new-resource-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Name</label>
				<input id="new-resource-name" bind:value={newResourceName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-resource-type" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Type</label>
				<select id="new-resource-type" bind:value={newResourceType} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
					<option value="ROOM">ROOM</option>
					<option value="EQUIPMENT">EQUIPMENT</option>
				</select>
			</div>
			<div>
				<label for="new-resource-desc" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Description <span class="text-gray-400">(optional)</span></label>
				<input id="new-resource-desc" bind:value={newResourceDescription} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => resourceCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateResource} disabled={resourceCreating} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{resourceCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Edit Resource -->
<Modal bind:this={resourceEditModal} title="Edit Resource">
	{#snippet children()}
		<div class="space-y-3">
			{#if resourceEditError}<p class="text-sm text-red-600 dark:text-red-400">{resourceEditError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400 font-mono">{editResourceId}</p>
			<div>
				<label for="edit-resource-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Name</label>
				<input id="edit-resource-name" bind:value={editResourceName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="edit-resource-desc" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Description</label>
				<input id="edit-resource-desc" bind:value={editResourceDescription} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => resourceEditModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitEditResource} disabled={resourceEditing} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{resourceEditing ? 'Saving…' : 'Save'}
		</button>
	{/snippet}
</Modal>

<!-- Resource Detail -->
<Modal bind:this={resourceDetailModal} title="Resource Detail">
	{#snippet children()}
		{#if resourceDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if resourceDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Resource ID</span><span class="font-mono text-xs">{resourceDetail.ResourceId}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Name</span><span>{resourceDetail.Name}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Email</span><span>{resourceDetail.Email ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Type</span><span>{resourceDetail.Type}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">State</span><span>{resourceDetail.State}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => resourceDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Register to WorkMail -->
<Modal bind:this={registerModal} title="Register to WorkMail">
	{#snippet children()}
		<div class="space-y-3">
			{#if registerError}<p class="text-sm text-red-600 dark:text-red-400">{registerError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400">{registerEntityLabel}</p>
			<div>
				<label for="register-email" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Email</label>
				<input id="register-email" bind:value={registerEmail} placeholder="name@example.com" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => registerModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitRegister} disabled={registering} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{registering ? 'Registering…' : 'Register'}
		</button>
	{/snippet}
</Modal>

<!-- Grant Mailbox Permission -->
<Modal bind:this={permCreateModal} title="Grant Mailbox Permission">
	{#snippet children()}
		<div class="space-y-3">
			{#if permCreateError}<p class="text-sm text-red-600 dark:text-red-400">{permCreateError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400">Mailbox: {selectedMailboxEntity?.label}</p>
			<div>
				<label for="new-perm-grantee" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Grantee ID (user, group or email)</label>
				<input id="new-perm-grantee" bind:value={newPermGranteeId} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div class="flex flex-wrap gap-4">
				<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={newPermFullAccess} class="rounded" /> Full Access</label>
				<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={newPermSendAs} class="rounded" /> Send As</label>
				<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={newPermSendOnBehalf} class="rounded" /> Send On Behalf</label>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => permCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreatePermission} disabled={permCreating} class="px-4 py-2 rounded-lg bg-green-600 text-white text-sm font-medium hover:bg-green-700 disabled:opacity-50">
			{permCreating ? 'Granting…' : 'Grant'}
		</button>
	{/snippet}
</Modal>
