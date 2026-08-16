<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import { isAllRegions, currentRegion } from '$lib/region.svelte';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getCognitoIDPClient } from '$lib/aws-client';
	import {
		ListUserPoolsCommand,
		CreateUserPoolCommand,
		DescribeUserPoolCommand,
		UpdateUserPoolCommand,
		DeleteUserPoolCommand,
		ListUsersCommand,
		AdminCreateUserCommand,
		AdminGetUserCommand,
		type AdminGetUserCommandOutput,
		AdminUpdateUserAttributesCommand,
		AdminDeleteUserCommand,
		AdminEnableUserCommand,
		AdminDisableUserCommand,
		ListGroupsCommand,
		CreateGroupCommand,
		GetGroupCommand,
		UpdateGroupCommand,
		DeleteGroupCommand,
		ListUsersInGroupCommand,
		ListUserPoolClientsCommand,
		CreateUserPoolClientCommand,
		DescribeUserPoolClientCommand,
		UpdateUserPoolClientCommand,
		DeleteUserPoolClientCommand,
		ListIdentityProvidersCommand,
		CreateIdentityProviderCommand,
		DescribeIdentityProviderCommand,
		UpdateIdentityProviderCommand,
		DeleteIdentityProviderCommand,
		ListResourceServersCommand,
		CreateResourceServerCommand,
		UpdateResourceServerCommand,
		DeleteResourceServerCommand,
		type UserPoolDescriptionType,
		type UserPoolType,
		type UserType,
		type GroupType,
		type UserPoolClientDescription,
		type UserPoolClientType,
		type ProviderDescription,
		type IdentityProviderType,
		type IdentityProviderTypeType,
		type ResourceServerType,
		type ResourceServerScopeType
	} from '@aws-sdk/client-cognito-identity-provider';
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
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { UserCog, Plus, Trash2, Eye, Pencil, Check, X } from 'lucide-svelte';

	const client = regionalClient(getCognitoIDPClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
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

	// Replaces the JSON.stringify(x).toLowerCase().includes(...) antipattern:
	// search only over the named fields that are actually meaningful for each
	// resource.
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	type TabId = 'pools' | 'users' | 'groups' | 'clients' | 'identityProviders' | 'resourceServers';

	const tabs: TabDef[] = [
		{ id: 'pools', label: 'User Pools' },
		{ id: 'users', label: 'Users' },
		{ id: 'groups', label: 'Groups' },
		{ id: 'clients', label: 'App Clients' },
		{ id: 'identityProviders', label: 'Identity Providers' },
		{ id: 'resourceServers', label: 'Resource Servers' }
	];

	// Every child tab's real List operation requires UserPoolId (confirmed
	// against the installed @aws-sdk/client-cognito-identity-provider compiled
	// types: ListUsersRequest.UserPoolId, ListGroupsRequest.UserPoolId,
	// ListUserPoolClientsRequest.UserPoolId, ListIdentityProvidersRequest.UserPoolId,
	// and ListResourceServersRequest.UserPoolId are all non-optional). Unlike
	// directoryservice there is no "optionally scoped" tier here -- every
	// child tab is strictly scoped to the selected pool.
	const poolScopedTabs: TabId[] = ['users', 'groups', 'clients', 'identityProviders', 'resourceServers'];

	// Pool IDs are already region-prefixed by construction (e.g.
	// "us-east-1_AbCdEfG"), but every child tab still needs to know WHICH
	// region to send its List/Admin* calls to -- `client()` (the page's
	// shared regional client) tracks the picker's region, not the selected
	// pool's, so a pool selected from a fanned-out list under All mode would
	// otherwise route child calls to the wrong region.
	type Regioned<T> = T & { region: string };

	let activeTab = $state<TabId>('pools');
	let searchQuery = $state('');
	let selectedPoolId = $state('');
	let selectedPoolRegion = $state('');

	let pools = $state<Regioned<UserPoolDescriptionType>[]>([]);
	let poolsNextToken = $state<string | undefined>();
	let loadingMorePools = $state(false);

	let users = $state<UserType[]>([]);
	let usersPaginationToken = $state<string | undefined>();
	let loadingMoreUsers = $state(false);

	let groups = $state<GroupType[]>([]);
	let groupsNextToken = $state<string | undefined>();
	let loadingMoreGroups = $state(false);

	let clients = $state<UserPoolClientDescription[]>([]);
	let clientsNextToken = $state<string | undefined>();
	let loadingMoreClients = $state(false);

	let idps = $state<ProviderDescription[]>([]);
	let idpsNextToken = $state<string | undefined>();
	let loadingMoreIdps = $state(false);

	let resourceServers = $state<ResourceServerType[]>([]);
	let resourceServersNextToken = $state<string | undefined>();
	let loadingMoreResourceServers = $state(false);

	// ListUserPoolsRequest.MaxResults is NOT optional in the installed SDK
	// (unlike every other MaxResults/Limit in this API) -- omitting it is a
	// TypeScript compile error, and against the real service it is a required
	// parameter. 60 is the real, documented AWS maximum for this operation.
	const LIST_USER_POOLS_MAX_RESULTS = 60;

	// In All mode, fans ListUserPools out across every region with data and
	// tags each row; pagination (NextToken) is inherently single-region, so
	// "Load More" only appends within the currently selected region.
	async function fetchPools(reset: boolean): Promise<void> {
		if (reset && isAllRegions()) {
			const result = await multiRegionList(
				(region) =>
					getCognitoIDPClient(region).send(
						new ListUserPoolsCommand({ MaxResults: LIST_USER_POOLS_MAX_RESULTS })
					),
				(r) => r.UserPools ?? []
			);
			pools = result.items.map(({ region, item }) => ({ ...item, region }) as Regioned<UserPoolDescriptionType>);
			poolsNextToken = undefined;
			if (result.errors.length > 0) {
				toast.error(`Failed to load user pools from ${result.errors.length} region(s)`);
			}
		} else {
			const resp = await client().send(
				new ListUserPoolsCommand({
					MaxResults: LIST_USER_POOLS_MAX_RESULTS,
					NextToken: reset ? undefined : poolsNextToken
				})
			);
			const region = currentRegion();
			const tagged = (resp.UserPools ?? []).map((p) => ({ ...p, region }) as Regioned<UserPoolDescriptionType>);
			pools = reset ? tagged : [...pools, ...tagged];
			poolsNextToken = resp.NextToken;
		}
		if (!selectedPoolId && pools.length > 0) {
			selectedPoolId = pools[0].Id ?? '';
			selectedPoolRegion = pools[0].region;
		}
	}

	// ListUsers is the one op in this family that paginates on
	// PaginationToken instead of NextToken (confirmed against
	// ListUsersRequest/ListUsersResponse in the installed SDK) -- every other
	// list op below uses NextToken.
	async function fetchUsers(reset: boolean): Promise<void> {
		if (!selectedPoolId) {
			users = [];
			usersPaginationToken = undefined;
			return;
		}
		const resp = await getCognitoIDPClient(selectedPoolRegion).send(
			new ListUsersCommand({
				UserPoolId: selectedPoolId,
				PaginationToken: reset ? undefined : usersPaginationToken
			})
		);
		users = reset ? (resp.Users ?? []) : [...users, ...(resp.Users ?? [])];
		usersPaginationToken = resp.PaginationToken;
	}

	async function fetchGroups(reset: boolean): Promise<void> {
		if (!selectedPoolId) {
			groups = [];
			groupsNextToken = undefined;
			return;
		}
		const resp = await getCognitoIDPClient(selectedPoolRegion).send(
			new ListGroupsCommand({
				UserPoolId: selectedPoolId,
				NextToken: reset ? undefined : groupsNextToken
			})
		);
		groups = reset ? (resp.Groups ?? []) : [...groups, ...(resp.Groups ?? [])];
		groupsNextToken = resp.NextToken;
	}

	async function fetchClients(reset: boolean): Promise<void> {
		if (!selectedPoolId) {
			clients = [];
			clientsNextToken = undefined;
			return;
		}
		const resp = await getCognitoIDPClient(selectedPoolRegion).send(
			new ListUserPoolClientsCommand({
				UserPoolId: selectedPoolId,
				NextToken: reset ? undefined : clientsNextToken
			})
		);
		clients = reset ? (resp.UserPoolClients ?? []) : [...clients, ...(resp.UserPoolClients ?? [])];
		clientsNextToken = resp.NextToken;
	}

	async function fetchIdps(reset: boolean): Promise<void> {
		if (!selectedPoolId) {
			idps = [];
			idpsNextToken = undefined;
			return;
		}
		const resp = await getCognitoIDPClient(selectedPoolRegion).send(
			new ListIdentityProvidersCommand({
				UserPoolId: selectedPoolId,
				NextToken: reset ? undefined : idpsNextToken
			})
		);
		idps = reset ? (resp.Providers ?? []) : [...idps, ...(resp.Providers ?? [])];
		idpsNextToken = resp.NextToken;
	}

	async function fetchResourceServers(reset: boolean): Promise<void> {
		if (!selectedPoolId) {
			resourceServers = [];
			resourceServersNextToken = undefined;
			return;
		}
		const resp = await getCognitoIDPClient(selectedPoolRegion).send(
			new ListResourceServersCommand({
				UserPoolId: selectedPoolId,
				NextToken: reset ? undefined : resourceServersNextToken
			})
		);
		resourceServers = reset
			? (resp.ResourceServers ?? [])
			: [...resourceServers, ...(resp.ResourceServers ?? [])];
		resourceServersNextToken = resp.NextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		pools: () => fetchPools(true).catch(rethrowDescribed),
		users: () => fetchUsers(true).catch(rethrowDescribed),
		groups: () => fetchGroups(true).catch(rethrowDescribed),
		clients: () => fetchClients(true).catch(rethrowDescribed),
		identityProviders: () => fetchIdps(true).catch(rethrowDescribed),
		resourceServers: () => fetchResourceServers(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onPoolSelect(id: string): void {
		selectedPoolId = id;
		selectedPoolRegion = pools.find((p) => p.Id === id)?.region ?? currentRegion();
		if (poolScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	// User Pools is the parent resource for every other tab: on a region
	// change the previously selected pool ID belongs to the old region and
	// must not be reused, so reload pools first (which re-selects a pool for
	// the new region) before reloading whichever tab is active.
	onRegionChange(() => {
		selectedPoolId = '';
		selectedPoolRegion = '';
		pools = [];
		poolsNextToken = undefined;
		void tabLoader.refresh('pools').then(() => {
			if (activeTab !== 'pools') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredPools = $derived(pools.filter((p) => matches(searchQuery, p.Id, p.Name, p.Status)));
	const filteredUsers = $derived(users.filter((u) => matches(searchQuery, u.Username, u.UserStatus)));
	const filteredGroups = $derived(
		groups.filter((g) => matches(searchQuery, g.GroupName, g.Description, g.RoleArn))
	);
	const filteredClients = $derived(
		clients.filter((c) => matches(searchQuery, c.ClientName, c.ClientId))
	);
	const filteredIdps = $derived(
		idps.filter((p) => matches(searchQuery, p.ProviderName, p.ProviderType))
	);
	const filteredResourceServers = $derived(
		resourceServers.filter((r) => matches(searchQuery, r.Identifier, r.Name))
	);

	async function loadMorePools(): Promise<void> {
		loadingMorePools = true;
		try {
			await fetchPools(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMorePools = false;
		}
	}
	async function loadMoreUsers(): Promise<void> {
		loadingMoreUsers = true;
		try {
			await fetchUsers(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreUsers = false;
		}
	}
	async function loadMoreGroups(): Promise<void> {
		loadingMoreGroups = true;
		try {
			await fetchGroups(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGroups = false;
		}
	}
	async function loadMoreClients(): Promise<void> {
		loadingMoreClients = true;
		try {
			await fetchClients(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreClients = false;
		}
	}
	async function loadMoreIdps(): Promise<void> {
		loadingMoreIdps = true;
		try {
			await fetchIdps(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreIdps = false;
		}
	}
	async function loadMoreResourceServers(): Promise<void> {
		loadingMoreResourceServers = true;
		try {
			await fetchResourceServers(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreResourceServers = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// ==================== User Pools: create / delete / update / detail ====================

	let createPoolModal = $state<Modal | null>(null);
	let creatingPool = $state(false);
	let createPoolError = $state<string | null>(null);
	let newPoolName = $state('');
	let newPoolMfa = $state<'OFF' | 'OPTIONAL' | 'ON'>('OFF');
	let newPoolDeletionProtection = $state(false);

	function openCreatePoolModal(): void {
		createPoolError = null;
		newPoolName = '';
		newPoolMfa = 'OFF';
		newPoolDeletionProtection = false;
		createPoolModal?.open();
	}

	async function submitCreatePool(): Promise<void> {
		if (!newPoolName) {
			createPoolError = 'Pool name is required.';
			return;
		}
		creatingPool = true;
		createPoolError = null;
		try {
			await client().send(
				new CreateUserPoolCommand({
					PoolName: newPoolName,
					MfaConfiguration: newPoolMfa,
					DeletionProtection: newPoolDeletionProtection ? 'ACTIVE' : 'INACTIVE'
				})
			);
			toast.success('User pool created');
			createPoolModal?.close();
			await tabLoader.refresh('pools');
		} catch (e) {
			const msg = describeError(e);
			createPoolError = msg;
			toast.error(msg);
		} finally {
			creatingPool = false;
		}
	}

	async function handleDeletePool(p: Regioned<UserPoolDescriptionType>): Promise<void> {
		if (!p.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete user pool',
			message: `Delete user pool ${p.Name ?? p.Id}? This deletes all of its users, groups, app clients, and identity providers.`
		});
		if (!confirmed) return;
		try {
			await getCognitoIDPClient(p.region).send(new DeleteUserPoolCommand({ UserPoolId: p.Id }));
			toast.success('User pool deleted');
			if (selectedPoolId === p.Id) {
				selectedPoolId = '';
				selectedPoolRegion = '';
			}
			await tabLoader.refresh('pools');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// UpdateUserPool's MfaConfiguration/DeletionProtection aren't present on
	// the ListUserPools row (UserPoolDescriptionType) -- only on the full
	// UserPoolType from DescribeUserPool -- so both the update form and the
	// detail view fetch the pool fresh rather than reusing row data.
	let updatePoolModal = $state<Modal | null>(null);
	let updatingPool = $state(false);
	let updatePoolLoading = $state(false);
	let updatePoolError = $state<string | null>(null);
	let updatePoolTarget = $state<Regioned<UserPoolDescriptionType> | null>(null);
	let updatePoolMfa = $state<'OFF' | 'OPTIONAL' | 'ON'>('OFF');
	let updatePoolDeletionProtection = $state(false);

	async function openUpdatePoolModal(p: Regioned<UserPoolDescriptionType>): Promise<void> {
		if (!p.Id) return;
		updatePoolTarget = p;
		updatePoolError = null;
		updatePoolLoading = true;
		updatePoolModal?.open();
		try {
			const resp = await getCognitoIDPClient(p.region).send(new DescribeUserPoolCommand({ UserPoolId: p.Id }));
			updatePoolMfa = (resp.UserPool?.MfaConfiguration as 'OFF' | 'OPTIONAL' | 'ON' | undefined) ?? 'OFF';
			updatePoolDeletionProtection = resp.UserPool?.DeletionProtection === 'ACTIVE';
		} catch (e) {
			updatePoolError = describeError(e);
		} finally {
			updatePoolLoading = false;
		}
	}

	async function submitUpdatePool(): Promise<void> {
		if (!updatePoolTarget?.Id) return;
		updatingPool = true;
		updatePoolError = null;
		try {
			await getCognitoIDPClient(updatePoolTarget.region).send(
				new UpdateUserPoolCommand({
					UserPoolId: updatePoolTarget.Id,
					MfaConfiguration: updatePoolMfa,
					DeletionProtection: updatePoolDeletionProtection ? 'ACTIVE' : 'INACTIVE'
				})
			);
			toast.success('User pool updated');
			updatePoolModal?.close();
			await tabLoader.refresh('pools');
		} catch (e) {
			const msg = describeError(e);
			updatePoolError = msg;
			toast.error(msg);
		} finally {
			updatingPool = false;
		}
	}

	let poolDetailModal = $state<Modal | null>(null);
	let poolDetailLoading = $state(false);
	let poolDetailError = $state<string | null>(null);
	let viewedPool = $state<UserPoolType | null>(null);

	async function openPoolDetail(p: Regioned<UserPoolDescriptionType>): Promise<void> {
		if (!p.Id) return;
		viewedPool = null;
		poolDetailError = null;
		poolDetailLoading = true;
		poolDetailModal?.open();
		try {
			const resp = await getCognitoIDPClient(p.region).send(new DescribeUserPoolCommand({ UserPoolId: p.Id }));
			viewedPool = resp.UserPool ?? null;
		} catch (e) {
			poolDetailError = describeError(e);
		} finally {
			poolDetailLoading = false;
		}
	}

	// ==================== Users: create / delete / update / detail (Admin* verbs) ====================
	//
	// This dashboard uses the Admin* operations (AdminCreateUser,
	// AdminDeleteUser, AdminGetUser, AdminUpdateUserAttributes) throughout,
	// not the self-service SignUp/DeleteUser/UpdateUserAttributes flows: those
	// act on the CALLER's own account via an access token from a completed
	// sign-in, which has no meaning for an operator console acting on behalf
	// of arbitrary users in a pool. The Admin* family is the one that takes
	// UserPoolId + Username directly, which is what a dashboard needs.

	let createUserModal = $state<Modal | null>(null);
	let creatingUser = $state(false);
	let createUserError = $state<string | null>(null);
	let newUsername = $state('');
	let newUserEmail = $state('');
	let newUserTempPassword = $state('');
	let newUserSuppressMessage = $state(false);

	function openCreateUserModal(): void {
		createUserError = selectedPoolId ? null : 'Select a user pool first.';
		newUsername = '';
		newUserEmail = '';
		newUserTempPassword = '';
		newUserSuppressMessage = false;
		createUserModal?.open();
	}

	async function submitCreateUser(): Promise<void> {
		if (!selectedPoolId) {
			createUserError = 'Select a user pool first.';
			return;
		}
		if (!newUsername) {
			createUserError = 'Username is required.';
			return;
		}
		creatingUser = true;
		createUserError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new AdminCreateUserCommand({
					UserPoolId: selectedPoolId,
					Username: newUsername,
					UserAttributes: newUserEmail ? [{ Name: 'email', Value: newUserEmail }] : undefined,
					TemporaryPassword: newUserTempPassword || undefined,
					MessageAction: newUserSuppressMessage ? 'SUPPRESS' : undefined
				})
			);
			toast.success('User created');
			createUserModal?.close();
			await tabLoader.refresh('users');
		} catch (e) {
			const msg = describeError(e);
			createUserError = msg;
			toast.error(msg);
		} finally {
			creatingUser = false;
		}
	}

	async function handleDeleteUser(u: UserType): Promise<void> {
		if (!selectedPoolId || !u.Username) return;
		const confirmed = await confirmDestructive({
			title: 'Delete user',
			message: `Delete user ${u.Username}?`
		});
		if (!confirmed) return;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(new AdminDeleteUserCommand({ UserPoolId: selectedPoolId, Username: u.Username }));
			toast.success('User deleted');
			await tabLoader.refresh('users');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleToggleUserEnabled(u: UserType): Promise<void> {
		if (!selectedPoolId || !u.Username) return;
		try {
			if (u.Enabled) {
				await getCognitoIDPClient(selectedPoolRegion).send(new AdminDisableUserCommand({ UserPoolId: selectedPoolId, Username: u.Username }));
			} else {
				await getCognitoIDPClient(selectedPoolRegion).send(new AdminEnableUserCommand({ UserPoolId: selectedPoolId, Username: u.Username }));
			}
			await tabLoader.refresh('users');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let updateUserModal = $state<Modal | null>(null);
	let updatingUser = $state(false);
	let updateUserError = $state<string | null>(null);
	let updateUserTarget = $state<UserType | null>(null);
	let updateUserAttrRows = $state<{ name: string; value: string }[]>([]);

	function openUpdateUserModal(u: UserType): void {
		updateUserTarget = u;
		updateUserError = null;
		// "sub" is immutable and never a legal target of AdminUpdateUserAttributes.
		updateUserAttrRows = (u.Attributes ?? [])
			.filter((a) => a.Name !== 'sub')
			.map((a) => ({ name: a.Name ?? '', value: a.Value ?? '' }));
		if (updateUserAttrRows.length === 0) {
			updateUserAttrRows = [{ name: '', value: '' }];
		}
		updateUserModal?.open();
	}

	function addUpdateUserAttrRow(): void {
		updateUserAttrRows = [...updateUserAttrRows, { name: '', value: '' }];
	}

	function removeUpdateUserAttrRow(index: number): void {
		updateUserAttrRows = updateUserAttrRows.filter((_, i) => i !== index);
	}

	async function submitUpdateUser(): Promise<void> {
		if (!selectedPoolId || !updateUserTarget?.Username) return;
		const attrs = updateUserAttrRows.filter((r) => r.name.trim().length > 0);
		if (attrs.length === 0) {
			updateUserError = 'At least one attribute is required.';
			return;
		}
		updatingUser = true;
		updateUserError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new AdminUpdateUserAttributesCommand({
					UserPoolId: selectedPoolId,
					Username: updateUserTarget.Username,
					UserAttributes: attrs.map((r) => ({ Name: r.name, Value: r.value }))
				})
			);
			toast.success('User attributes updated');
			updateUserModal?.close();
			await tabLoader.refresh('users');
		} catch (e) {
			const msg = describeError(e);
			updateUserError = msg;
			toast.error(msg);
		} finally {
			updatingUser = false;
		}
	}

	let userDetailModal = $state<Modal | null>(null);
	let userDetailLoading = $state(false);
	let userDetailError = $state<string | null>(null);
	let viewedUser = $state<AdminGetUserCommandOutput | null>(null);

	async function openUserDetail(u: UserType): Promise<void> {
		if (!selectedPoolId || !u.Username) return;
		viewedUser = null;
		userDetailError = null;
		userDetailLoading = true;
		userDetailModal?.open();
		try {
			const resp = await getCognitoIDPClient(selectedPoolRegion).send(
				new AdminGetUserCommand({ UserPoolId: selectedPoolId, Username: u.Username })
			);
			viewedUser = resp;
		} catch (e) {
			userDetailError = describeError(e);
		} finally {
			userDetailLoading = false;
		}
	}

	// ==================== Groups: create / delete / update / detail ====================

	let createGroupModal = $state<Modal | null>(null);
	let creatingGroup = $state(false);
	let createGroupError = $state<string | null>(null);
	let newGroupName = $state('');
	let newGroupDescription = $state('');
	let newGroupPrecedence = $state('');
	let newGroupRoleArn = $state('');

	function openCreateGroupModal(): void {
		createGroupError = selectedPoolId ? null : 'Select a user pool first.';
		newGroupName = '';
		newGroupDescription = '';
		newGroupPrecedence = '';
		newGroupRoleArn = '';
		createGroupModal?.open();
	}

	async function submitCreateGroup(): Promise<void> {
		if (!selectedPoolId) {
			createGroupError = 'Select a user pool first.';
			return;
		}
		if (!newGroupName) {
			createGroupError = 'Group name is required.';
			return;
		}
		creatingGroup = true;
		createGroupError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new CreateGroupCommand({
					UserPoolId: selectedPoolId,
					GroupName: newGroupName,
					Description: newGroupDescription || undefined,
					Precedence: newGroupPrecedence ? Number(newGroupPrecedence) : undefined,
					RoleArn: newGroupRoleArn || undefined
				})
			);
			toast.success('Group created');
			createGroupModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			createGroupError = msg;
			toast.error(msg);
		} finally {
			creatingGroup = false;
		}
	}

	async function handleDeleteGroup(g: GroupType): Promise<void> {
		if (!selectedPoolId || !g.GroupName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete group',
			message: `Delete group ${g.GroupName}?`
		});
		if (!confirmed) return;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(new DeleteGroupCommand({ UserPoolId: selectedPoolId, GroupName: g.GroupName }));
			toast.success('Group deleted');
			await tabLoader.refresh('groups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let updateGroupModal = $state<Modal | null>(null);
	let updatingGroup = $state(false);
	let updateGroupError = $state<string | null>(null);
	let updateGroupTarget = $state<GroupType | null>(null);
	let updateGroupDescription = $state('');
	let updateGroupPrecedence = $state('');
	let updateGroupRoleArn = $state('');

	function openUpdateGroupModal(g: GroupType): void {
		updateGroupTarget = g;
		updateGroupError = null;
		updateGroupDescription = g.Description ?? '';
		updateGroupPrecedence = g.Precedence === undefined ? '' : String(g.Precedence);
		updateGroupRoleArn = g.RoleArn ?? '';
		updateGroupModal?.open();
	}

	async function submitUpdateGroup(): Promise<void> {
		if (!selectedPoolId || !updateGroupTarget?.GroupName) return;
		updatingGroup = true;
		updateGroupError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new UpdateGroupCommand({
					UserPoolId: selectedPoolId,
					GroupName: updateGroupTarget.GroupName,
					Description: updateGroupDescription || undefined,
					Precedence: updateGroupPrecedence ? Number(updateGroupPrecedence) : undefined,
					RoleArn: updateGroupRoleArn || undefined
				})
			);
			toast.success('Group updated');
			updateGroupModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			updateGroupError = msg;
			toast.error(msg);
		} finally {
			updatingGroup = false;
		}
	}

	// The detail view also shows the group's first page of members
	// (ListUsersInGroup) -- informational only, not paginated further, since
	// this is a nested view inside a modal rather than its own tab.
	let groupDetailModal = $state<Modal | null>(null);
	let groupDetailLoading = $state(false);
	let groupDetailError = $state<string | null>(null);
	let viewedGroup = $state<GroupType | null>(null);
	let viewedGroupMembers = $state<UserType[]>([]);
	let viewedGroupMembersMore = $state(false);

	async function openGroupDetail(g: GroupType): Promise<void> {
		if (!selectedPoolId || !g.GroupName) return;
		viewedGroup = null;
		viewedGroupMembers = [];
		viewedGroupMembersMore = false;
		groupDetailError = null;
		groupDetailLoading = true;
		groupDetailModal?.open();
		try {
			const [groupResp, membersResp] = await Promise.all([
				getCognitoIDPClient(selectedPoolRegion).send(new GetGroupCommand({ UserPoolId: selectedPoolId, GroupName: g.GroupName })),
				getCognitoIDPClient(selectedPoolRegion).send(new ListUsersInGroupCommand({ UserPoolId: selectedPoolId, GroupName: g.GroupName }))
			]);
			viewedGroup = groupResp.Group ?? null;
			viewedGroupMembers = membersResp.Users ?? [];
			viewedGroupMembersMore = !!membersResp.NextToken;
		} catch (e) {
			groupDetailError = describeError(e);
		} finally {
			groupDetailLoading = false;
		}
	}

	// ==================== App Clients: create / delete / update / detail ====================

	let createClientModal = $state<Modal | null>(null);
	let creatingClient = $state(false);
	let createClientError = $state<string | null>(null);
	let newClientName = $state('');
	let newClientGenerateSecret = $state(false);

	function openCreateClientModal(): void {
		createClientError = selectedPoolId ? null : 'Select a user pool first.';
		newClientName = '';
		newClientGenerateSecret = false;
		createClientModal?.open();
	}

	async function submitCreateClient(): Promise<void> {
		if (!selectedPoolId) {
			createClientError = 'Select a user pool first.';
			return;
		}
		if (!newClientName) {
			createClientError = 'Client name is required.';
			return;
		}
		creatingClient = true;
		createClientError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new CreateUserPoolClientCommand({
					UserPoolId: selectedPoolId,
					ClientName: newClientName,
					GenerateSecret: newClientGenerateSecret
				})
			);
			toast.success('App client created');
			createClientModal?.close();
			await tabLoader.refresh('clients');
		} catch (e) {
			const msg = describeError(e);
			createClientError = msg;
			toast.error(msg);
		} finally {
			creatingClient = false;
		}
	}

	async function handleDeleteClient(c: UserPoolClientDescription): Promise<void> {
		if (!selectedPoolId || !c.ClientId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete app client',
			message: `Delete app client ${c.ClientName ?? c.ClientId}?`
		});
		if (!confirmed) return;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(new DeleteUserPoolClientCommand({ UserPoolId: selectedPoolId, ClientId: c.ClientId }));
			toast.success('App client deleted');
			await tabLoader.refresh('clients');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// PreventUserExistenceErrors isn't on the ListUserPoolClients row
	// (UserPoolClientDescription only has ClientId/ClientName/UserPoolId) --
	// only on the full UserPoolClientType from DescribeUserPoolClient -- so
	// the update form fetches the client fresh, same reasoning as the pool
	// update form above.
	let updateClientModal = $state<Modal | null>(null);
	let updatingClient = $state(false);
	let updateClientLoading = $state(false);
	let updateClientError = $state<string | null>(null);
	let updateClientTarget = $state<UserPoolClientDescription | null>(null);
	let updateClientName = $state('');
	let updateClientPreventUserExistenceErrors = $state<'LEGACY' | 'ENABLED'>('LEGACY');

	async function openUpdateClientModal(c: UserPoolClientDescription): Promise<void> {
		if (!selectedPoolId || !c.ClientId) return;
		updateClientTarget = c;
		updateClientError = null;
		updateClientName = c.ClientName ?? '';
		updateClientLoading = true;
		updateClientModal?.open();
		try {
			const resp = await getCognitoIDPClient(selectedPoolRegion).send(
				new DescribeUserPoolClientCommand({ UserPoolId: selectedPoolId, ClientId: c.ClientId })
			);
			updateClientName = resp.UserPoolClient?.ClientName ?? updateClientName;
			updateClientPreventUserExistenceErrors =
				(resp.UserPoolClient?.PreventUserExistenceErrors as 'LEGACY' | 'ENABLED' | undefined) ?? 'LEGACY';
		} catch (e) {
			updateClientError = describeError(e);
		} finally {
			updateClientLoading = false;
		}
	}

	async function submitUpdateClient(): Promise<void> {
		if (!selectedPoolId || !updateClientTarget?.ClientId) return;
		if (!updateClientName) {
			updateClientError = 'Client name is required.';
			return;
		}
		updatingClient = true;
		updateClientError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new UpdateUserPoolClientCommand({
					UserPoolId: selectedPoolId,
					ClientId: updateClientTarget.ClientId,
					ClientName: updateClientName,
					PreventUserExistenceErrors: updateClientPreventUserExistenceErrors
				})
			);
			toast.success('App client updated');
			updateClientModal?.close();
			await tabLoader.refresh('clients');
		} catch (e) {
			const msg = describeError(e);
			updateClientError = msg;
			toast.error(msg);
		} finally {
			updatingClient = false;
		}
	}

	let clientDetailModal = $state<Modal | null>(null);
	let clientDetailLoading = $state(false);
	let clientDetailError = $state<string | null>(null);
	let viewedClient = $state<UserPoolClientType | null>(null);
	let revealClientSecret = $state(false);

	async function openClientDetail(c: UserPoolClientDescription): Promise<void> {
		if (!selectedPoolId || !c.ClientId) return;
		viewedClient = null;
		revealClientSecret = false;
		clientDetailError = null;
		clientDetailLoading = true;
		clientDetailModal?.open();
		try {
			const resp = await getCognitoIDPClient(selectedPoolRegion).send(
				new DescribeUserPoolClientCommand({ UserPoolId: selectedPoolId, ClientId: c.ClientId })
			);
			viewedClient = resp.UserPoolClient ?? null;
		} catch (e) {
			clientDetailError = describeError(e);
		} finally {
			clientDetailLoading = false;
		}
	}

	// ==================== Identity Providers: create / delete / update / detail ====================
	//
	// ProviderType is immutable after creation -- UpdateIdentityProviderRequest
	// has no ProviderType member at all (confirmed against the installed
	// SDK), only ProviderDetails/AttributeMapping/IdpIdentifiers. The create
	// and update forms therefore share the same type-specific ProviderDetails
	// fields, but only create lets the user pick the type.

	type IdpKind = 'Google' | 'Facebook' | 'LoginWithAmazon' | 'SignInWithApple' | 'OIDC' | 'SAML';

	let createIdpModal = $state<Modal | null>(null);
	let creatingIdp = $state(false);
	let createIdpError = $state<string | null>(null);
	let newIdpName = $state('');
	let newIdpType = $state<IdpKind>('OIDC');
	let newIdpClientId = $state('');
	let newIdpClientSecret = $state('');
	let newIdpAuthorizeScopes = $state('');
	let newIdpOidcIssuer = $state('');
	let newIdpMetadataUrl = $state('');

	function openCreateIdpModal(): void {
		createIdpError = selectedPoolId ? null : 'Select a user pool first.';
		newIdpName = '';
		newIdpType = 'OIDC';
		newIdpClientId = '';
		newIdpClientSecret = '';
		newIdpAuthorizeScopes = '';
		newIdpOidcIssuer = '';
		newIdpMetadataUrl = '';
		createIdpModal?.open();
	}

	function buildProviderDetails(
		kind: IdpKind,
		clientId: string,
		clientSecret: string,
		authorizeScopes: string,
		oidcIssuer: string,
		metadataUrl: string
	): Record<string, string> {
		if (kind === 'SAML') {
			return metadataUrl ? { MetadataURL: metadataUrl } : {};
		}
		const details: Record<string, string> = {};
		if (clientId) details.client_id = clientId;
		if (clientSecret) details.client_secret = clientSecret;
		if (authorizeScopes) details.authorize_scopes = authorizeScopes;
		if (kind === 'OIDC') {
			if (oidcIssuer) details.oidc_issuer = oidcIssuer;
			details.attributes_request_method = 'GET';
		}
		return details;
	}

	async function submitCreateIdp(): Promise<void> {
		if (!selectedPoolId) {
			createIdpError = 'Select a user pool first.';
			return;
		}
		if (!newIdpName) {
			createIdpError = 'Provider name is required.';
			return;
		}
		creatingIdp = true;
		createIdpError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new CreateIdentityProviderCommand({
					UserPoolId: selectedPoolId,
					ProviderName: newIdpName,
					ProviderType: newIdpType as IdentityProviderTypeType,
					ProviderDetails: buildProviderDetails(
						newIdpType,
						newIdpClientId,
						newIdpClientSecret,
						newIdpAuthorizeScopes,
						newIdpOidcIssuer,
						newIdpMetadataUrl
					)
				})
			);
			toast.success('Identity provider created');
			createIdpModal?.close();
			await tabLoader.refresh('identityProviders');
		} catch (e) {
			const msg = describeError(e);
			createIdpError = msg;
			toast.error(msg);
		} finally {
			creatingIdp = false;
		}
	}

	async function handleDeleteIdp(p: ProviderDescription): Promise<void> {
		if (!selectedPoolId || !p.ProviderName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete identity provider',
			message: `Delete identity provider ${p.ProviderName}?`
		});
		if (!confirmed) return;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new DeleteIdentityProviderCommand({ UserPoolId: selectedPoolId, ProviderName: p.ProviderName })
			);
			toast.success('Identity provider deleted');
			await tabLoader.refresh('identityProviders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let updateIdpModal = $state<Modal | null>(null);
	let updatingIdp = $state(false);
	let updateIdpLoading = $state(false);
	let updateIdpError = $state<string | null>(null);
	let updateIdpTarget = $state<ProviderDescription | null>(null);
	let updateIdpType = $state<IdpKind>('OIDC');
	let updateIdpClientId = $state('');
	let updateIdpClientSecret = $state('');
	let updateIdpAuthorizeScopes = $state('');
	let updateIdpOidcIssuer = $state('');
	let updateIdpMetadataUrl = $state('');

	async function openUpdateIdpModal(p: ProviderDescription): Promise<void> {
		if (!selectedPoolId || !p.ProviderName) return;
		updateIdpTarget = p;
		updateIdpType = (p.ProviderType as IdpKind | undefined) ?? 'OIDC';
		updateIdpError = null;
		updateIdpClientId = '';
		updateIdpClientSecret = '';
		updateIdpAuthorizeScopes = '';
		updateIdpOidcIssuer = '';
		updateIdpMetadataUrl = '';
		updateIdpLoading = true;
		updateIdpModal?.open();
		try {
			const resp = await getCognitoIDPClient(selectedPoolRegion).send(
				new DescribeIdentityProviderCommand({ UserPoolId: selectedPoolId, ProviderName: p.ProviderName })
			);
			const details = resp.IdentityProvider?.ProviderDetails ?? {};
			updateIdpClientId = details.client_id ?? '';
			updateIdpClientSecret = details.client_secret ?? '';
			updateIdpAuthorizeScopes = details.authorize_scopes ?? '';
			updateIdpOidcIssuer = details.oidc_issuer ?? '';
			updateIdpMetadataUrl = details.MetadataURL ?? '';
		} catch (e) {
			updateIdpError = describeError(e);
		} finally {
			updateIdpLoading = false;
		}
	}

	async function submitUpdateIdp(): Promise<void> {
		if (!selectedPoolId || !updateIdpTarget?.ProviderName) return;
		updatingIdp = true;
		updateIdpError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new UpdateIdentityProviderCommand({
					UserPoolId: selectedPoolId,
					ProviderName: updateIdpTarget.ProviderName,
					ProviderDetails: buildProviderDetails(
						updateIdpType,
						updateIdpClientId,
						updateIdpClientSecret,
						updateIdpAuthorizeScopes,
						updateIdpOidcIssuer,
						updateIdpMetadataUrl
					)
				})
			);
			toast.success('Identity provider updated');
			updateIdpModal?.close();
			await tabLoader.refresh('identityProviders');
		} catch (e) {
			const msg = describeError(e);
			updateIdpError = msg;
			toast.error(msg);
		} finally {
			updatingIdp = false;
		}
	}

	let idpDetailModal = $state<Modal | null>(null);
	let idpDetailLoading = $state(false);
	let idpDetailError = $state<string | null>(null);
	let viewedIdp = $state<IdentityProviderType | null>(null);

	async function openIdpDetail(p: ProviderDescription): Promise<void> {
		if (!selectedPoolId || !p.ProviderName) return;
		viewedIdp = null;
		idpDetailError = null;
		idpDetailLoading = true;
		idpDetailModal?.open();
		try {
			const resp = await getCognitoIDPClient(selectedPoolRegion).send(
				new DescribeIdentityProviderCommand({ UserPoolId: selectedPoolId, ProviderName: p.ProviderName })
			);
			viewedIdp = resp.IdentityProvider ?? null;
		} catch (e) {
			idpDetailError = describeError(e);
		} finally {
			idpDetailLoading = false;
		}
	}

	// ==================== Resource Servers: create / delete / update / detail ====================
	//
	// ListResourceServers already returns the complete ResourceServerType
	// shape for every server (Identifier/Name/Scopes), so unlike Pools/Clients
	// above there is no separate Describe call needed to populate the update
	// form or the detail view -- the row itself is enough.

	let createResourceServerModal = $state<Modal | null>(null);
	let creatingResourceServer = $state(false);
	let createResourceServerError = $state<string | null>(null);
	let newRsIdentifier = $state('');
	let newRsName = $state('');
	let newRsScopes = $state<{ name: string; description: string }[]>([]);

	function openCreateResourceServerModal(): void {
		createResourceServerError = selectedPoolId ? null : 'Select a user pool first.';
		newRsIdentifier = '';
		newRsName = '';
		newRsScopes = [];
		createResourceServerModal?.open();
	}

	function addNewRsScope(): void {
		newRsScopes = [...newRsScopes, { name: '', description: '' }];
	}

	function removeNewRsScope(index: number): void {
		newRsScopes = newRsScopes.filter((_, i) => i !== index);
	}

	function toScopeList(rows: { name: string; description: string }[]): ResourceServerScopeType[] | undefined {
		const scopes = rows
			.filter((r) => r.name.trim().length > 0)
			.map((r) => ({ ScopeName: r.name, ScopeDescription: r.description || r.name }));
		return scopes.length > 0 ? scopes : undefined;
	}

	async function submitCreateResourceServer(): Promise<void> {
		if (!selectedPoolId) {
			createResourceServerError = 'Select a user pool first.';
			return;
		}
		if (!newRsIdentifier || !newRsName) {
			createResourceServerError = 'Identifier and name are required.';
			return;
		}
		creatingResourceServer = true;
		createResourceServerError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new CreateResourceServerCommand({
					UserPoolId: selectedPoolId,
					Identifier: newRsIdentifier,
					Name: newRsName,
					Scopes: toScopeList(newRsScopes)
				})
			);
			toast.success('Resource server created');
			createResourceServerModal?.close();
			await tabLoader.refresh('resourceServers');
		} catch (e) {
			const msg = describeError(e);
			createResourceServerError = msg;
			toast.error(msg);
		} finally {
			creatingResourceServer = false;
		}
	}

	async function handleDeleteResourceServer(r: ResourceServerType): Promise<void> {
		if (!selectedPoolId || !r.Identifier) return;
		const confirmed = await confirmDestructive({
			title: 'Delete resource server',
			message: `Delete resource server ${r.Name ?? r.Identifier}?`
		});
		if (!confirmed) return;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new DeleteResourceServerCommand({ UserPoolId: selectedPoolId, Identifier: r.Identifier })
			);
			toast.success('Resource server deleted');
			await tabLoader.refresh('resourceServers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let updateResourceServerModal = $state<Modal | null>(null);
	let updatingResourceServer = $state(false);
	let updateResourceServerError = $state<string | null>(null);
	let updateRsTarget = $state<ResourceServerType | null>(null);
	let updateRsName = $state('');
	let updateRsScopes = $state<{ name: string; description: string }[]>([]);

	function openUpdateResourceServerModal(r: ResourceServerType): void {
		updateRsTarget = r;
		updateResourceServerError = null;
		updateRsName = r.Name ?? '';
		updateRsScopes = (r.Scopes ?? []).map((s) => ({
			name: s.ScopeName ?? '',
			description: s.ScopeDescription ?? ''
		}));
		updateResourceServerModal?.open();
	}

	function addUpdateRsScope(): void {
		updateRsScopes = [...updateRsScopes, { name: '', description: '' }];
	}

	function removeUpdateRsScope(index: number): void {
		updateRsScopes = updateRsScopes.filter((_, i) => i !== index);
	}

	async function submitUpdateResourceServer(): Promise<void> {
		if (!selectedPoolId || !updateRsTarget?.Identifier) return;
		if (!updateRsName) {
			updateResourceServerError = 'Name is required.';
			return;
		}
		updatingResourceServer = true;
		updateResourceServerError = null;
		try {
			await getCognitoIDPClient(selectedPoolRegion).send(
				new UpdateResourceServerCommand({
					UserPoolId: selectedPoolId,
					Identifier: updateRsTarget.Identifier,
					Name: updateRsName,
					Scopes: toScopeList(updateRsScopes)
				})
			);
			toast.success('Resource server updated');
			updateResourceServerModal?.close();
			await tabLoader.refresh('resourceServers');
		} catch (e) {
			const msg = describeError(e);
			updateResourceServerError = msg;
			toast.error(msg);
		} finally {
			updatingResourceServer = false;
		}
	}

	let resourceServerDetailModal = $state<Modal | null>(null);
	let viewedResourceServer = $state<ResourceServerType | null>(null);

	function openResourceServerDetail(r: ResourceServerType): void {
		viewedResourceServer = r;
		resourceServerDetailModal?.open();
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={UserCog}
		title="Cognito User Pools"
		description="User pools, users, groups, app clients, identity providers, and resource servers"
		onRefresh={handleRefresh}
		color="indigo"
	>
		{#snippet actions()}
			{#if activeTab === 'pools'}
				<button
					onclick={openCreatePoolModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create user pool
				</button>
			{:else if activeTab === 'users'}
				<button
					onclick={openCreateUserModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create user
				</button>
			{:else if activeTab === 'groups'}
				<button
					onclick={openCreateGroupModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create group
				</button>
			{:else if activeTab === 'clients'}
				<button
					onclick={openCreateClientModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create app client
				</button>
			{:else if activeTab === 'identityProviders'}
				<button
					onclick={openCreateIdpModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create identity provider
				</button>
			{:else if activeTab === 'resourceServers'}
				<button
					onclick={openCreateResourceServerModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create resource server
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="indigo" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if poolScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="pool-select" class="text-sm text-gray-500 dark:text-gray-400">User pool</label>
					<select
						id="pool-select"
						value={selectedPoolId}
						onchange={(e) => onPoolSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if pools.length === 0}
							<option value="">No user pools</option>
						{/if}
						{#each pools as p (p.region + '::' + p.Id)}
							<option value={p.Id}>{p.Name} ({p.Id}) — {p.region}</option>
						{/each}
					</select>
					{#if selectedPoolRegion}
						<RegionChip region={selectedPoolRegion} />
					{/if}
				</div>
			{/if}

			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'pools'}
				{#snippet poolRegionCell(p: Regioned<UserPoolDescriptionType>)}
					<RegionChip region={p.region} />
				{/snippet}
				{#snippet poolStatusCell(p: Regioned<UserPoolDescriptionType>)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(p.Status === 'Enabled')}">{p.Status ?? '—'}</span>
				{/snippet}
				{#snippet poolCreatedCell(p: Regioned<UserPoolDescriptionType>)}
					{formatDate(p.CreationDate)}
				{/snippet}
				{#snippet poolActionsCell(p: Regioned<UserPoolDescriptionType>)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openPoolDetail(p)} title="View" aria-label="View pool {p.Name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdatePoolModal(p)} title="Update" aria-label="Update pool {p.Name}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeletePool(p)} title="Delete" aria-label="Delete pool {p.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const poolColumns = defineColumns<Regioned<UserPoolDescriptionType>>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Id', label: 'Pool ID' },
					{ key: 'region', label: 'Region', render: poolRegionCell },
					{ key: 'Status', label: 'Status', render: poolStatusCell },
					{ key: 'CreationDate', label: 'Created', render: poolCreatedCell },
					{ key: 'actions', label: '', render: poolActionsCell }
				])}
				<DataTable
					rows={filteredPools}
					rowKey={(p) => p.region + '::' + (p.Id ?? '')}
					columns={poolColumns}
					loading={tabLoader.isLoading('pools')}
					emptyMessage="No user pools found"
				/>
				<LoadMore hasMore={!!poolsNextToken} loading={loadingMorePools} onLoadMore={loadMorePools} />
			{:else if activeTab === 'users'}
				{#snippet userStatusCell(u: UserType)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(u.UserStatus === 'CONFIRMED')}">{u.UserStatus ?? '—'}</span>
				{/snippet}
				{#snippet userEnabledCell(u: UserType)}
					<button
						onclick={() => handleToggleUserEnabled(u)}
						title={u.Enabled ? 'Disable' : 'Enable'}
						class="text-xs px-2 py-1 rounded-full {statusClass(!!u.Enabled)}"
					>
						{u.Enabled ? 'Enabled' : 'Disabled'}
					</button>
				{/snippet}
				{#snippet userCreatedCell(u: UserType)}
					{formatDate(u.UserCreateDate)}
				{/snippet}
				{#snippet userActionsCell(u: UserType)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openUserDetail(u)} title="View" aria-label="View user {u.Username}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateUserModal(u)} title="Edit attributes" aria-label="Edit attributes for {u.Username}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteUser(u)} title="Delete" aria-label="Delete user {u.Username}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const userColumns = defineColumns<UserType>([
					{ key: 'Username', label: 'Username' },
					{ key: 'UserStatus', label: 'Status', render: userStatusCell },
					{ key: 'Enabled', label: 'Enabled', render: userEnabledCell },
					{ key: 'UserCreateDate', label: 'Created', render: userCreatedCell },
					{ key: 'actions', label: '', render: userActionsCell }
				])}
				<DataTable
					rows={filteredUsers}
					rowKey={(u) => u.Username ?? ''}
					columns={userColumns}
					loading={tabLoader.isLoading('users')}
					emptyMessage={selectedPoolId ? 'No users found' : 'Select a user pool to see its users'}
				/>
				<LoadMore hasMore={!!usersPaginationToken} loading={loadingMoreUsers} onLoadMore={loadMoreUsers} />
			{:else if activeTab === 'groups'}
				{#snippet groupActionsCell(g: GroupType)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openGroupDetail(g)} title="View" aria-label="View group {g.GroupName}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateGroupModal(g)} title="Update" aria-label="Update group {g.GroupName}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteGroup(g)} title="Delete" aria-label="Delete group {g.GroupName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const groupColumns = defineColumns<GroupType>([
					{ key: 'GroupName', label: 'Group Name' },
					{ key: 'Description', label: 'Description' },
					{ key: 'Precedence', label: 'Precedence' },
					{ key: 'actions', label: '', render: groupActionsCell }
				])}
				<DataTable
					rows={filteredGroups}
					rowKey={(g) => g.GroupName ?? ''}
					columns={groupColumns}
					loading={tabLoader.isLoading('groups')}
					emptyMessage={selectedPoolId ? 'No groups found' : 'Select a user pool to see its groups'}
				/>
				<LoadMore hasMore={!!groupsNextToken} loading={loadingMoreGroups} onLoadMore={loadMoreGroups} />
			{:else if activeTab === 'clients'}
				{#snippet clientActionsCell(c: UserPoolClientDescription)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openClientDetail(c)} title="View" aria-label="View client {c.ClientName}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateClientModal(c)} title="Update" aria-label="Update client {c.ClientName}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteClient(c)} title="Delete" aria-label="Delete client {c.ClientName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const clientColumns = defineColumns<UserPoolClientDescription>([
					{ key: 'ClientName', label: 'Client Name' },
					{ key: 'ClientId', label: 'Client ID' },
					{ key: 'actions', label: '', render: clientActionsCell }
				])}
				<DataTable
					rows={filteredClients}
					rowKey={(c) => c.ClientId ?? ''}
					columns={clientColumns}
					loading={tabLoader.isLoading('clients')}
					emptyMessage={selectedPoolId ? 'No app clients found' : 'Select a user pool to see its app clients'}
				/>
				<LoadMore hasMore={!!clientsNextToken} loading={loadingMoreClients} onLoadMore={loadMoreClients} />
			{:else if activeTab === 'identityProviders'}
				{#snippet idpCreatedCell(p: ProviderDescription)}
					{formatDate(p.CreationDate)}
				{/snippet}
				{#snippet idpActionsCell(p: ProviderDescription)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openIdpDetail(p)} title="View" aria-label="View provider {p.ProviderName}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateIdpModal(p)} title="Update" aria-label="Update provider {p.ProviderName}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteIdp(p)} title="Delete" aria-label="Delete provider {p.ProviderName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const idpColumns = defineColumns<ProviderDescription>([
					{ key: 'ProviderName', label: 'Provider Name' },
					{ key: 'ProviderType', label: 'Type' },
					{ key: 'CreationDate', label: 'Created', render: idpCreatedCell },
					{ key: 'actions', label: '', render: idpActionsCell }
				])}
				<DataTable
					rows={filteredIdps}
					rowKey={(p) => p.ProviderName ?? ''}
					columns={idpColumns}
					loading={tabLoader.isLoading('identityProviders')}
					emptyMessage={selectedPoolId ? 'No identity providers found' : 'Select a user pool to see its identity providers'}
				/>
				<LoadMore hasMore={!!idpsNextToken} loading={loadingMoreIdps} onLoadMore={loadMoreIdps} />
			{:else if activeTab === 'resourceServers'}
				{#snippet rsScopesCell(r: ResourceServerType)}
					{(r.Scopes ?? []).map((s) => s.ScopeName).join(', ') || '—'}
				{/snippet}
				{#snippet rsActionsCell(r: ResourceServerType)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openResourceServerDetail(r)} title="View" aria-label="View resource server {r.Name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateResourceServerModal(r)} title="Update" aria-label="Update resource server {r.Name}" class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteResourceServer(r)} title="Delete" aria-label="Delete resource server {r.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const rsColumns = defineColumns<ResourceServerType>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Identifier', label: 'Identifier' },
					{ key: 'Scopes', label: 'Scopes', render: rsScopesCell },
					{ key: 'actions', label: '', render: rsActionsCell }
				])}
				<DataTable
					rows={filteredResourceServers}
					rowKey={(r) => r.Identifier ?? ''}
					columns={rsColumns}
					loading={tabLoader.isLoading('resourceServers')}
					emptyMessage={selectedPoolId ? 'No resource servers found' : 'Select a user pool to see its resource servers'}
				/>
				<LoadMore
					hasMore={!!resourceServersNextToken}
					loading={loadingMoreResourceServers}
					onLoadMore={loadMoreResourceServers}
				/>
			{/if}
		</div>
	</div>
</div>

<!-- ==================== User Pools modals ==================== -->

<Modal bind:this={createPoolModal} title="Create User Pool">
	{#snippet headerAction()}
		<WriteRegionHint />
	{/snippet}
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pool-name" class="text-sm text-slate-600 dark:text-slate-300">Pool name</label>
				<input id="pool-name" bind:value={newPoolName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pool-mfa" class="text-sm text-slate-600 dark:text-slate-300">MFA configuration</label>
				<select id="pool-mfa" bind:value={newPoolMfa} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="OFF">OFF</option>
					<option value="OPTIONAL">OPTIONAL</option>
					<option value="ON">ON</option>
				</select>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newPoolDeletionProtection} />
				Enable deletion protection
			</label>
			{#if createPoolError}
				<p class="text-sm text-red-600 dark:text-red-400">{createPoolError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createPoolModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreatePool} disabled={creatingPool} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingPool ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updatePoolModal} title="Update User Pool">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{updatePoolTarget?.Name}</span></p>
			{#if updatePoolLoading}
				<p class="text-sm text-slate-500 dark:text-slate-400">Loading current configuration…</p>
			{:else}
				<div>
					<label for="update-pool-mfa" class="text-sm text-slate-600 dark:text-slate-300">MFA configuration</label>
					<select id="update-pool-mfa" bind:value={updatePoolMfa} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="OFF">OFF</option>
						<option value="OPTIONAL">OPTIONAL</option>
						<option value="ON">ON</option>
					</select>
				</div>
				<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
					<input type="checkbox" bind:checked={updatePoolDeletionProtection} />
					Enable deletion protection
				</label>
			{/if}
			{#if updatePoolError}
				<p class="text-sm text-red-600 dark:text-red-400">{updatePoolError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updatePoolModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdatePool} disabled={updatingPool || updatePoolLoading} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{updatingPool ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={poolDetailModal} title="User Pool Details">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if poolDetailLoading}
				<p class="text-slate-500 dark:text-slate-400">Loading…</p>
			{:else if poolDetailError}
				<p class="text-red-600 dark:text-red-400">{poolDetailError}</p>
			{:else if viewedPool}
				<div><span class="text-slate-500 dark:text-slate-400">Name:</span> <span class="font-medium">{viewedPool.Name}</span></div>
				<div><span class="text-slate-500 dark:text-slate-400">Pool ID:</span> <span class="font-mono text-xs">{viewedPool.Id}</span></div>
				<div class="break-all"><span class="text-slate-500 dark:text-slate-400">ARN:</span> <span class="font-mono text-xs">{viewedPool.Arn}</span></div>
				<div><span class="text-slate-500 dark:text-slate-400">Status:</span> {viewedPool.Status ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">MFA:</span> {viewedPool.MfaConfiguration ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Deletion protection:</span> {viewedPool.DeletionProtection ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Estimated users:</span> {viewedPool.EstimatedNumberOfUsers ?? 0}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Created:</span> {formatDate(viewedPool.CreationDate)}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Last modified:</span> {formatDate(viewedPool.LastModifiedDate)}</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => poolDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Users modals ==================== -->

<Modal bind:this={createUserModal} title="Create User">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="user-username" class="text-sm text-slate-600 dark:text-slate-300">Username</label>
				<input id="user-username" bind:value={newUsername} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="user-email" class="text-sm text-slate-600 dark:text-slate-300">Email (optional)</label>
				<input id="user-email" type="email" bind:value={newUserEmail} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="user-temp-password" class="text-sm text-slate-600 dark:text-slate-300">Temporary password (optional)</label>
				<input id="user-temp-password" type="password" bind:value={newUserTempPassword} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newUserSuppressMessage} />
				Suppress the welcome message
			</label>
			{#if createUserError}
				<p class="text-sm text-red-600 dark:text-red-400">{createUserError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createUserModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateUser} disabled={creatingUser} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingUser ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateUserModal} title="Edit User Attributes">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Editing <span class="font-medium">{updateUserTarget?.Username}</span></p>
			<div class="space-y-2">
				{#each updateUserAttrRows as row, i (i)}
					<div class="flex items-center gap-2">
						<input aria-label="Attribute name" bind:value={row.name} placeholder="Name (e.g. email)" class="flex-1 px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input aria-label="Attribute value" bind:value={row.value} placeholder="Value" class="flex-1 px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<button type="button" onclick={() => removeUpdateUserAttrRow(i)} title="Remove attribute" class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button>
					</div>
				{/each}
			</div>
			<button type="button" onclick={addUpdateUserAttrRow} class="text-sm text-indigo-600 dark:text-indigo-400 hover:underline">+ Add attribute</button>
			{#if updateUserError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateUserError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updateUserModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdateUser} disabled={updatingUser} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{updatingUser ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={userDetailModal} title="User Details">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if userDetailLoading}
				<p class="text-slate-500 dark:text-slate-400">Loading…</p>
			{:else if userDetailError}
				<p class="text-red-600 dark:text-red-400">{userDetailError}</p>
			{:else if viewedUser}
				<div><span class="text-slate-500 dark:text-slate-400">Username:</span> <span class="font-medium">{viewedUser.Username}</span></div>
				<div><span class="text-slate-500 dark:text-slate-400">Status:</span> {viewedUser.UserStatus ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Enabled:</span> {viewedUser.Enabled ? 'Yes' : 'No'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Preferred MFA:</span> {viewedUser.PreferredMfaSetting ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Created:</span> {formatDate(viewedUser.UserCreateDate)}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Last modified:</span> {formatDate(viewedUser.UserLastModifiedDate)}</div>
				{#if viewedUser.UserAttributes && viewedUser.UserAttributes.length > 0}
					<table class="w-full text-sm mt-2">
						<thead><tr class="border-b border-slate-200 dark:border-slate-700"><th class="pb-1 text-left font-medium text-slate-600 dark:text-slate-400">Attribute</th><th class="pb-1 text-left font-medium text-slate-600 dark:text-slate-400">Value</th></tr></thead>
						<tbody>
							{#each viewedUser.UserAttributes as attr (attr.Name)}
								<tr class="border-b border-slate-100 dark:border-slate-800">
									<td class="py-1 text-xs font-mono">{attr.Name}</td>
									<td class="py-1 text-xs break-all">{attr.Value}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => userDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Groups modals ==================== -->

<Modal bind:this={createGroupModal} title="Create Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="group-name" class="text-sm text-slate-600 dark:text-slate-300">Group name</label>
				<input id="group-name" bind:value={newGroupName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="group-desc" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input id="group-desc" bind:value={newGroupDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="group-precedence" class="text-sm text-slate-600 dark:text-slate-300">Precedence (optional)</label>
				<input id="group-precedence" type="number" bind:value={newGroupPrecedence} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="group-role-arn" class="text-sm text-slate-600 dark:text-slate-300">IAM role ARN (optional)</label>
				<input id="group-role-arn" bind:value={newGroupRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{createGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createGroupModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateGroup} disabled={creatingGroup} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingGroup ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateGroupModal} title="Update Group">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{updateGroupTarget?.GroupName}</span></p>
			<div>
				<label for="update-group-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="update-group-desc" bind:value={updateGroupDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="update-group-precedence" class="text-sm text-slate-600 dark:text-slate-300">Precedence</label>
				<input id="update-group-precedence" type="number" bind:value={updateGroupPrecedence} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="update-group-role-arn" class="text-sm text-slate-600 dark:text-slate-300">IAM role ARN</label>
				<input id="update-group-role-arn" bind:value={updateGroupRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if updateGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updateGroupModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdateGroup} disabled={updatingGroup} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{updatingGroup ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={groupDetailModal} title="Group Details">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if groupDetailLoading}
				<p class="text-slate-500 dark:text-slate-400">Loading…</p>
			{:else if groupDetailError}
				<p class="text-red-600 dark:text-red-400">{groupDetailError}</p>
			{:else if viewedGroup}
				<div><span class="text-slate-500 dark:text-slate-400">Name:</span> <span class="font-medium">{viewedGroup.GroupName}</span></div>
				<div><span class="text-slate-500 dark:text-slate-400">Description:</span> {viewedGroup.Description ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Precedence:</span> {viewedGroup.Precedence ?? '—'}</div>
				<div class="break-all"><span class="text-slate-500 dark:text-slate-400">Role ARN:</span> {viewedGroup.RoleArn ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Created:</span> {formatDate(viewedGroup.CreationDate)}</div>
				<p class="font-medium mt-3">Members{viewedGroupMembersMore ? ' (first page)' : ''}</p>
				{#if viewedGroupMembers.length === 0}
					<p class="text-slate-500 dark:text-slate-400">No members.</p>
				{:else}
					<ul class="list-disc list-inside">
						{#each viewedGroupMembers as m (m.Username)}
							<li>{m.Username}</li>
						{/each}
					</ul>
				{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => groupDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== App Clients modals ==================== -->

<Modal bind:this={createClientModal} title="Create App Client">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="client-name" class="text-sm text-slate-600 dark:text-slate-300">Client name</label>
				<input id="client-name" bind:value={newClientName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newClientGenerateSecret} />
				Generate a client secret
			</label>
			{#if createClientError}
				<p class="text-sm text-red-600 dark:text-red-400">{createClientError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createClientModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateClient} disabled={creatingClient} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingClient ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateClientModal} title="Update App Client">
	{#snippet children()}
		<div class="space-y-3">
			{#if updateClientLoading}
				<p class="text-sm text-slate-500 dark:text-slate-400">Loading current configuration…</p>
			{:else}
				<div>
					<label for="update-client-name" class="text-sm text-slate-600 dark:text-slate-300">Client name</label>
					<input id="update-client-name" bind:value={updateClientName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="update-client-peue" class="text-sm text-slate-600 dark:text-slate-300">Prevent user existence errors</label>
					<select id="update-client-peue" bind:value={updateClientPreventUserExistenceErrors} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="LEGACY">LEGACY</option>
						<option value="ENABLED">ENABLED</option>
					</select>
				</div>
			{/if}
			{#if updateClientError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateClientError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updateClientModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdateClient} disabled={updatingClient || updateClientLoading} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{updatingClient ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={clientDetailModal} title="App Client Details">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if clientDetailLoading}
				<p class="text-slate-500 dark:text-slate-400">Loading…</p>
			{:else if clientDetailError}
				<p class="text-red-600 dark:text-red-400">{clientDetailError}</p>
			{:else if viewedClient}
				<div><span class="text-slate-500 dark:text-slate-400">Name:</span> <span class="font-medium">{viewedClient.ClientName}</span></div>
				<div><span class="text-slate-500 dark:text-slate-400">Client ID:</span> <span class="font-mono text-xs">{viewedClient.ClientId}</span></div>
				<div>
					<span class="text-slate-500 dark:text-slate-400">Secret:</span>
					{#if viewedClient.ClientSecret}
						{#if revealClientSecret}
							<span class="font-mono text-xs">{viewedClient.ClientSecret}</span>
							<button type="button" onclick={() => (revealClientSecret = false)} class="ml-1 text-indigo-500 hover:underline">hide</button>
						{:else}
							<span>••••••••</span>
							<button type="button" onclick={() => (revealClientSecret = true)} class="ml-1 text-indigo-500 hover:underline">reveal</button>
						{/if}
					{:else}
						<span class="text-slate-400 dark:text-slate-600">none</span>
					{/if}
				</div>
				<div><span class="text-slate-500 dark:text-slate-400">Prevent user existence errors:</span> {viewedClient.PreventUserExistenceErrors ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Created:</span> {formatDate(viewedClient.CreationDate)}</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => clientDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Identity Providers modals ==================== -->

<Modal bind:this={createIdpModal} title="Create Identity Provider">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="idp-name" class="text-sm text-slate-600 dark:text-slate-300">Provider name</label>
				<input id="idp-name" bind:value={newIdpName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="idp-type" class="text-sm text-slate-600 dark:text-slate-300">Provider type</label>
				<select id="idp-type" bind:value={newIdpType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="OIDC">OIDC</option>
					<option value="SAML">SAML</option>
					<option value="Google">Google</option>
					<option value="Facebook">Facebook</option>
					<option value="LoginWithAmazon">Login with Amazon</option>
					<option value="SignInWithApple">Sign in with Apple</option>
				</select>
			</div>
			{#if newIdpType === 'SAML'}
				<div>
					<label for="idp-metadata-url" class="text-sm text-slate-600 dark:text-slate-300">SAML metadata URL</label>
					<input id="idp-metadata-url" bind:value={newIdpMetadataUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{:else}
				<div>
					<label for="idp-client-id" class="text-sm text-slate-600 dark:text-slate-300">Client ID</label>
					<input id="idp-client-id" bind:value={newIdpClientId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="idp-client-secret" class="text-sm text-slate-600 dark:text-slate-300">Client secret</label>
					<input id="idp-client-secret" type="password" bind:value={newIdpClientSecret} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="idp-scopes" class="text-sm text-slate-600 dark:text-slate-300">Authorize scopes (space-separated)</label>
					<input id="idp-scopes" bind:value={newIdpAuthorizeScopes} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if newIdpType === 'OIDC'}
					<div>
						<label for="idp-oidc-issuer" class="text-sm text-slate-600 dark:text-slate-300">OIDC issuer URL</label>
						<input id="idp-oidc-issuer" bind:value={newIdpOidcIssuer} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
				{/if}
			{/if}
			{#if createIdpError}
				<p class="text-sm text-red-600 dark:text-red-400">{createIdpError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createIdpModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateIdp} disabled={creatingIdp} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingIdp ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateIdpModal} title="Update Identity Provider">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{updateIdpTarget?.ProviderName}</span> ({updateIdpType})</p>
			{#if updateIdpLoading}
				<p class="text-sm text-slate-500 dark:text-slate-400">Loading current configuration…</p>
			{:else if updateIdpType === 'SAML'}
				<div>
					<label for="update-idp-metadata-url" class="text-sm text-slate-600 dark:text-slate-300">SAML metadata URL</label>
					<input id="update-idp-metadata-url" bind:value={updateIdpMetadataUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{:else}
				<div>
					<label for="update-idp-client-id" class="text-sm text-slate-600 dark:text-slate-300">Client ID</label>
					<input id="update-idp-client-id" bind:value={updateIdpClientId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="update-idp-client-secret" class="text-sm text-slate-600 dark:text-slate-300">Client secret</label>
					<input id="update-idp-client-secret" type="password" bind:value={updateIdpClientSecret} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="update-idp-scopes" class="text-sm text-slate-600 dark:text-slate-300">Authorize scopes (space-separated)</label>
					<input id="update-idp-scopes" bind:value={updateIdpAuthorizeScopes} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				{#if updateIdpType === 'OIDC'}
					<div>
						<label for="update-idp-oidc-issuer" class="text-sm text-slate-600 dark:text-slate-300">OIDC issuer URL</label>
						<input id="update-idp-oidc-issuer" bind:value={updateIdpOidcIssuer} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
				{/if}
			{/if}
			{#if updateIdpError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateIdpError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updateIdpModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdateIdp} disabled={updatingIdp || updateIdpLoading} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{updatingIdp ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={idpDetailModal} title="Identity Provider Details">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if idpDetailLoading}
				<p class="text-slate-500 dark:text-slate-400">Loading…</p>
			{:else if idpDetailError}
				<p class="text-red-600 dark:text-red-400">{idpDetailError}</p>
			{:else if viewedIdp}
				<div><span class="text-slate-500 dark:text-slate-400">Name:</span> <span class="font-medium">{viewedIdp.ProviderName}</span></div>
				<div><span class="text-slate-500 dark:text-slate-400">Type:</span> {viewedIdp.ProviderType ?? '—'}</div>
				<div><span class="text-slate-500 dark:text-slate-400">Created:</span> {formatDate(viewedIdp.CreationDate)}</div>
				<p class="font-medium mt-3">Provider details</p>
				{#if viewedIdp.ProviderDetails && Object.keys(viewedIdp.ProviderDetails).length > 0}
					<table class="w-full text-sm">
						<tbody>
							{#each Object.entries(viewedIdp.ProviderDetails) as [k, v] (k)}
								<tr class="border-b border-slate-100 dark:border-slate-800">
									<td class="py-1 pr-2 text-xs font-mono">{k}</td>
									<td class="py-1 text-xs break-all">{v}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				{:else}
					<p class="text-slate-500 dark:text-slate-400">None.</p>
				{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => idpDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Resource Servers modals ==================== -->

<Modal bind:this={createResourceServerModal} title="Create Resource Server">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rs-identifier" class="text-sm text-slate-600 dark:text-slate-300">Identifier</label>
				<input id="rs-identifier" bind:value={newRsIdentifier} placeholder="my-api" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rs-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="rs-name" bind:value={newRsName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="space-y-2">
				<p class="text-sm text-slate-600 dark:text-slate-300">Scopes (optional)</p>
				{#each newRsScopes as row, i (i)}
					<div class="flex items-center gap-2">
						<input aria-label="Scope name" bind:value={row.name} placeholder="Scope name" class="flex-1 px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input aria-label="Scope description" bind:value={row.description} placeholder="Description" class="flex-1 px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<button type="button" onclick={() => removeNewRsScope(i)} title="Remove scope" class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button>
					</div>
				{/each}
				<button type="button" onclick={addNewRsScope} class="text-sm text-indigo-600 dark:text-indigo-400 hover:underline">+ Add scope</button>
			</div>
			{#if createResourceServerError}
				<p class="text-sm text-red-600 dark:text-red-400">{createResourceServerError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createResourceServerModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateResourceServer} disabled={creatingResourceServer} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingResourceServer ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateResourceServerModal} title="Update Resource Server">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{updateRsTarget?.Identifier}</span></p>
			<div>
				<label for="update-rs-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="update-rs-name" bind:value={updateRsName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="space-y-2">
				<p class="text-sm text-slate-600 dark:text-slate-300">Scopes</p>
				{#each updateRsScopes as row, i (i)}
					<div class="flex items-center gap-2">
						<input aria-label="Scope name" bind:value={row.name} placeholder="Scope name" class="flex-1 px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input aria-label="Scope description" bind:value={row.description} placeholder="Description" class="flex-1 px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<button type="button" onclick={() => removeUpdateRsScope(i)} title="Remove scope" class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button>
					</div>
				{/each}
				<button type="button" onclick={addUpdateRsScope} class="text-sm text-indigo-600 dark:text-indigo-400 hover:underline">+ Add scope</button>
			</div>
			{#if updateResourceServerError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateResourceServerError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updateResourceServerModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdateResourceServer} disabled={updatingResourceServer} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{updatingResourceServer ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={resourceServerDetailModal} title="Resource Server Details">
	{#snippet children()}
		<div class="space-y-2 text-sm">
			{#if viewedResourceServer}
				<div><span class="text-slate-500 dark:text-slate-400">Name:</span> <span class="font-medium">{viewedResourceServer.Name}</span></div>
				<div><span class="text-slate-500 dark:text-slate-400">Identifier:</span> <span class="font-mono text-xs">{viewedResourceServer.Identifier}</span></div>
				<p class="font-medium mt-3">Scopes</p>
				{#if viewedResourceServer.Scopes && viewedResourceServer.Scopes.length > 0}
					<ul class="list-disc list-inside">
						{#each viewedResourceServer.Scopes as s (s.ScopeName)}
							<li><span class="font-mono text-xs">{s.ScopeName}</span> — {s.ScopeDescription}</li>
						{/each}
					</ul>
				{:else}
					<p class="text-slate-500 dark:text-slate-400">None.</p>
				{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => resourceServerDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
