<script lang="ts">
import { onMount } from 'svelte';
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
import { getIdentityStoreClient } from '$lib/aws-client';
import {
CreateGroupCommand,
CreateGroupMembershipCommand,
CreateUserCommand,
DeleteGroupCommand,
DeleteGroupMembershipCommand,
DeleteUserCommand,
ListGroupMembershipsCommand,
ListGroupMembershipsForMemberCommand,
ListGroupsCommand,
ListUsersCommand,
UpdateGroupCommand,
UpdateUserCommand
} from '@aws-sdk/client-identitystore';
import { toast } from 'svelte-sonner';
import { Copy, RefreshCw, Beaker, ChevronUp, ChevronDown, X, Users, Layers } from 'lucide-svelte';

const identityStore = regionalClient(getIdentityStoreClient);
const defaultStoreID = 'd-0000000000';

type IdentityUser = {
UserId?: string;
UserName?: string;
DisplayName?: string;
Title?: string;
Emails?: Array<{ Value?: string; Type?: string; Primary?: boolean }>;
Addresses?: Array<{ Formatted?: string; Type?: string; Primary?: boolean }>;
PhoneNumbers?: Array<{ Value?: string; Type?: string; Primary?: boolean }>;
Name?: { GivenName?: string; FamilyName?: string };
ExternalIds?: Array<{ Issuer?: string; Id?: string }>;
};
type IdentityGroup = {
GroupId?: string;
DisplayName?: string;
Description?: string;
ExternalIds?: Array<{ Issuer?: string; Id?: string }>;
};
type IdentityMembership = { MembershipId?: string; GroupId?: string; MemberId?: { UserId?: string } };

let storeId = $state(defaultStoreID);
let activeTab = $state<'users' | 'groups' | 'docs' | 'metrics'>('users');
let users = $state<IdentityUser[]>([]);
let groups = $state<IdentityGroup[]>([]);
let loading = $state(false);
let loadError = $state<string | null>(null);
let lastLoaded = $state<Date | null>(null);
let userSearch = $state('');
let groupSearch = $state('');
let membershipsForUser = $state<Record<string, IdentityMembership>>({});
let groupMemberCounts = $state<Record<string, number>>({});

// Sort state
let userSortCol = $state<'UserName' | 'DisplayName' | 'Email'>('UserName');
let userSortAsc = $state(true);
let groupSortCol = $state<'DisplayName' | 'Description' | 'count'>('DisplayName');
let groupSortAsc = $state(true);

// Mutation-in-progress tracking per resource ID
let actionInProgress = $state<Record<string, boolean>>({});

let showCreateUserModal = $state(false);
let showCreateGroupModal = $state(false);
let showMembershipModal = $state(false);
let showEditUserModal = $state(false);
let showEditGroupModal = $state(false);
let showDeleteUserConfirm = $state(false);
let showDeleteGroupConfirm = $state(false);
let showViewMembersModal = $state(false);
let seedingDemo = $state(false);

let userName = $state('');
let userDisplayName = $state('');
let userGivenName = $state('');
let userFamilyName = $state('');
let userEmail = $state('');
let userPhone = $state('');
let groupDisplayName = $state('');
let groupDescription = $state('');
let selectedMembershipGroupID = $state('');
let membershipUser = $state<IdentityUser | null>(null);
let membershipLoadError = $state<string | null>(null);
let membershipLoading = $state(false);
let profileUser = $state<IdentityUser | null>(null);
let profileDisplayName = $state('');
let profileEmail = $state('');
let profilePhone = $state('');
let profileAddress = $state('');
let profileGivenName = $state('');
let profileFamilyName = $state('');
let profileTitle = $state('');
let editGroupTarget = $state<IdentityGroup | null>(null);
let editGroupDisplayName = $state('');
let editGroupDescription = $state('');
let deleteUserTarget = $state<IdentityUser | null>(null);
let deleteGroupTarget = $state<IdentityGroup | null>(null);
let viewMembersGroup = $state<IdentityGroup | null>(null);
let membersOfGroup = $state<IdentityUser[]>([]);
let membershipsOfGroup = $state<IdentityMembership[]>([]);

function effectiveStoreID(): string {
return storeId.trim() || defaultStoreID;
}

function shortID(id?: string): string {
if (!id) return '—';
return id.length > 16 ? id.slice(0, 8) + '…' + id.slice(-4) : id;
}

async function copyToClipboard(text: string, label: string) {
try {
await navigator.clipboard.writeText(text);
toast.success(`Copied ${label}`);
} catch {
toast.error('Failed to copy to clipboard');
}
}

// ---- Sorting helpers ----

let sortedUsers = $derived(() => {
const q = userSearch.toLowerCase();
const filtered = users.filter(
(u) =>
!q ||
(u.UserName ?? '').toLowerCase().includes(q) ||
(u.DisplayName ?? '').toLowerCase().includes(q) ||
(u.Emails?.[0]?.Value ?? '').toLowerCase().includes(q) ||
[(u.Name?.GivenName ?? ''), (u.Name?.FamilyName ?? '')].join(' ').toLowerCase().includes(q)
);
	return [...filtered].toSorted((a, b) => {
		if (userSortCol === 'Email') {
			const av = (a.Emails?.[0]?.Value ?? '').toLowerCase();
			const bv = (b.Emails?.[0]?.Value ?? '').toLowerCase();
			return userSortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
		}
		const av = (a[userSortCol] ?? '').toLowerCase();
		const bv = (b[userSortCol] ?? '').toLowerCase();
		return userSortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
	});
});

let sortedGroups = $derived(() => {
const filtered = groups.filter(
(g) =>
!groupSearch ||
(g.DisplayName ?? '').toLowerCase().includes(groupSearch.toLowerCase()) ||
(g.Description ?? '').toLowerCase().includes(groupSearch.toLowerCase())
);
	return [...filtered].toSorted((a, b) => {
		if (groupSortCol === 'count') {
			const ac = groupMemberCounts[a.GroupId ?? ''] ?? 0;
			const bc = groupMemberCounts[b.GroupId ?? ''] ?? 0;
			return groupSortAsc ? ac - bc : bc - ac;
		}
		const av = (a[groupSortCol] ?? '').toLowerCase();
		const bv = (b[groupSortCol] ?? '').toLowerCase();
		return groupSortAsc ? av.localeCompare(bv) : bv.localeCompare(av);
});
});

function toggleUserSort(col: typeof userSortCol) {
if (userSortCol === col) {
userSortAsc = !userSortAsc;
} else {
userSortCol = col;
userSortAsc = true;
}
}

function toggleGroupSort(col: typeof groupSortCol) {
if (groupSortCol === col) {
groupSortAsc = !groupSortAsc;
} else {
groupSortCol = col;
groupSortAsc = true;
}
}

// ---- Data loading ----

async function loadUsers() {
const out = await identityStore().send(
new ListUsersCommand({ IdentityStoreId: effectiveStoreID() })
);
users = (out.Users ?? []) as IdentityUser[];
}

async function loadGroups() {
const out = await identityStore().send(
new ListGroupsCommand({ IdentityStoreId: effectiveStoreID() })
);
groups = (out.Groups ?? []) as IdentityGroup[];
}

async function loadGroupMemberCounts(grps: IdentityGroup[]) {
const entries = await Promise.all(
grps.map(async (g) => {
if (!g.GroupId) return [g.GroupId ?? '', 0] as [string, number];
try {
const out = await identityStore().send(
new ListGroupMembershipsCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: g.GroupId
})
);
return [g.GroupId, (out.GroupMemberships ?? []).length] as [string, number];
} catch {
return [g.GroupId, 0] as [string, number];
}
})
);
groupMemberCounts = Object.fromEntries(entries);
}

async function loadMembershipsForMember(user: IdentityUser) {
if (!user.UserId) {
membershipsForUser = {};
return;
}
const out = await identityStore().send(
new ListGroupMembershipsForMemberCommand({
IdentityStoreId: effectiveStoreID(),
MemberId: { UserId: user.UserId }
})
);
membershipsForUser = Object.fromEntries(
((out.GroupMemberships ?? []) as IdentityMembership[])
.filter((m) => m.GroupId)
.map((m) => [m.GroupId ?? '', m])
);
}

async function refresh() {
loading = true;
loadError = null;
try {
await Promise.all([loadUsers(), loadGroups()]);
lastLoaded = new Date();
membershipsForUser = {};
membershipUser = null;
// Load member counts in background without blocking the UI.
void loadGroupMemberCounts(groups);
toast.success('Identity store loaded');
} catch (err: unknown) {
loadError = (err as Error).message;
toast.error(`Failed to load identity store: ${(err as Error).message}`);
} finally {
loading = false;
}
}

// ---- CRUD ----

async function createUser() {
try {
await identityStore().send(
new CreateUserCommand({
IdentityStoreId: effectiveStoreID(),
UserName: userName,
DisplayName:
userDisplayName || `${userGivenName} ${userFamilyName}`.trim() || undefined,
Name:
userGivenName || userFamilyName
? { GivenName: userGivenName, FamilyName: userFamilyName }
: undefined,
Emails: userEmail ? [{ Value: userEmail, Type: 'work', Primary: true }] : undefined,
PhoneNumbers: userPhone ? [{ Value: userPhone, Type: 'work', Primary: true }] : undefined
})
);
showCreateUserModal = false;
userName = '';
userDisplayName = '';
userGivenName = '';
userFamilyName = '';
userEmail = '';
userPhone = '';
await loadUsers();
toast.success('User created');
} catch (err: unknown) {
toast.error(`Failed to create user: ${(err as Error).message}`);
}
}

async function createGroup() {
try {
await identityStore().send(
new CreateGroupCommand({
IdentityStoreId: effectiveStoreID(),
DisplayName: groupDisplayName,
Description: groupDescription || undefined
})
);
showCreateGroupModal = false;
groupDisplayName = '';
groupDescription = '';
await loadGroups();
void loadGroupMemberCounts(groups);
toast.success('Group created');
} catch (err: unknown) {
toast.error(`Failed to create group: ${(err as Error).message}`);
}
}

async function openMembershipModal(user: IdentityUser) {
membershipUser = user;
selectedMembershipGroupID = '';
showMembershipModal = true;
membershipLoadError = null;
membershipLoading = true;
try {
await loadMembershipsForMember(user);
} catch (err: unknown) {
membershipLoadError = (err as Error).message;
toast.error(`Failed to load memberships: ${(err as Error).message}`);
} finally {
membershipLoading = false;
}
}

async function addMembership() {
if (!membershipUser?.UserId || !selectedMembershipGroupID) return;
membershipLoading = true;
try {
await identityStore().send(
new CreateGroupMembershipCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: selectedMembershipGroupID,
MemberId: { UserId: membershipUser.UserId }
})
);
selectedMembershipGroupID = '';
await loadMembershipsForMember(membershipUser);
void loadGroupMemberCounts(groups);
toast.success('Membership added');
} catch (err: unknown) {
toast.error(`Failed to add group membership: ${(err as Error).message}`);
} finally {
membershipLoading = false;
}
}

async function removeMembership(membershipID?: string) {
if (!membershipID || !membershipUser?.UserId) return;
membershipLoading = true;
try {
await identityStore().send(
new DeleteGroupMembershipCommand({
IdentityStoreId: effectiveStoreID(),
MembershipId: membershipID
})
);
await loadMembershipsForMember(membershipUser);
void loadGroupMemberCounts(groups);
toast.success('Membership removed');
} catch (err: unknown) {
toast.error(`Failed to remove group membership: ${(err as Error).message}`);
} finally {
membershipLoading = false;
}
}

function openProfileEditor(user: IdentityUser) {
profileUser = user;
profileDisplayName = user.DisplayName ?? '';
profileEmail = user.Emails?.[0]?.Value ?? '';
profilePhone = user.PhoneNumbers?.[0]?.Value ?? '';
profileAddress = user.Addresses?.[0]?.Formatted ?? '';
profileGivenName = user.Name?.GivenName ?? '';
profileFamilyName = user.Name?.FamilyName ?? '';
profileTitle = user.Title ?? '';
showEditUserModal = true;
}

async function updateUserProfile() {
if (!profileUser?.UserId) return;
const id = profileUser.UserId;
actionInProgress = { ...actionInProgress, [id]: true };
try {
await identityStore().send(
new UpdateUserCommand({
IdentityStoreId: effectiveStoreID(),
UserId: profileUser.UserId,
Operations: [
{ AttributePath: 'displayName', AttributeValue: profileDisplayName },
{
AttributePath: 'emails',
AttributeValue: profileEmail
? [{ Value: profileEmail, Type: 'work', Primary: true }]
: []
},
{
AttributePath: 'phoneNumbers',
AttributeValue: profilePhone
? [{ Value: profilePhone, Type: 'work', Primary: true }]
: []
},
{
AttributePath: 'addresses',
AttributeValue: profileAddress
? [{ Formatted: profileAddress, Type: 'work', Primary: true }]
: []
},
{ AttributePath: 'name.givenName', AttributeValue: profileGivenName },
{ AttributePath: 'name.familyName', AttributeValue: profileFamilyName },
{ AttributePath: 'title', AttributeValue: profileTitle }
]
})
);
showEditUserModal = false;
await loadUsers();
toast.success('User profile updated');
} catch (err: unknown) {
toast.error(`Failed to update user profile: ${(err as Error).message}`);
} finally {
const next = { ...actionInProgress };
delete next[id];
actionInProgress = next;
}
}

async function confirmDeleteUser() {
if (!deleteUserTarget?.UserId) return;
const id = deleteUserTarget.UserId;
actionInProgress = { ...actionInProgress, [id]: true };
try {
await identityStore().send(
new DeleteUserCommand({
IdentityStoreId: effectiveStoreID(),
UserId: deleteUserTarget.UserId
})
);
showDeleteUserConfirm = false;
deleteUserTarget = null;
await loadUsers();
void loadGroupMemberCounts(groups);
toast.success('User deleted');
} catch (err: unknown) {
toast.error(`Failed to delete user: ${(err as Error).message}`);
} finally {
const next = { ...actionInProgress };
delete next[id];
actionInProgress = next;
}
}

async function confirmDeleteGroup() {
if (!deleteGroupTarget?.GroupId) return;
const id = deleteGroupTarget.GroupId;
actionInProgress = { ...actionInProgress, [id]: true };
try {
await identityStore().send(
new DeleteGroupCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: deleteGroupTarget.GroupId
})
);
showDeleteGroupConfirm = false;
deleteGroupTarget = null;
await loadGroups();
void loadGroupMemberCounts(groups);
toast.success('Group deleted');
} catch (err: unknown) {
toast.error(`Failed to delete group: ${(err as Error).message}`);
} finally {
const next = { ...actionInProgress };
delete next[id];
actionInProgress = next;
}
}

function openEditGroup(group: IdentityGroup) {
editGroupTarget = group;
editGroupDisplayName = group.DisplayName ?? '';
editGroupDescription = group.Description ?? '';
showEditGroupModal = true;
}

async function updateGroup() {
if (!editGroupTarget?.GroupId) return;
const id = editGroupTarget.GroupId;
actionInProgress = { ...actionInProgress, [id]: true };
try {
await identityStore().send(
new UpdateGroupCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: editGroupTarget.GroupId,
Operations: [
{ AttributePath: 'displayName', AttributeValue: editGroupDisplayName },
{ AttributePath: 'description', AttributeValue: editGroupDescription }
]
})
);
showEditGroupModal = false;
await loadGroups();
void loadGroupMemberCounts(groups);
toast.success('Group updated');
} catch (err: unknown) {
toast.error(`Failed to update group: ${(err as Error).message}`);
} finally {
const next = { ...actionInProgress };
delete next[id];
actionInProgress = next;
}
}

async function openViewMembersModal(group: IdentityGroup) {
viewMembersGroup = group;
membersOfGroup = [];
membershipsOfGroup = [];
showViewMembersModal = true;
try {
const out = await identityStore().send(
new ListGroupMembershipsCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: group.GroupId!
})
);
membershipsOfGroup = (out.GroupMemberships ?? []) as IdentityMembership[];
const memberUserIDs = new Set(
membershipsOfGroup.map((m) => m.MemberId?.UserId).filter(Boolean) as string[]
);
membersOfGroup = users.filter((u) => u.UserId && memberUserIDs.has(u.UserId));
} catch (err: unknown) {
toast.error(`Failed to load group members: ${(err as Error).message}`);
}
}

async function removeMemberFromGroup(membershipID?: string) {
if (!membershipID || !viewMembersGroup) return;
const id = membershipID;
actionInProgress = { ...actionInProgress, [id]: true };
try {
await identityStore().send(
new DeleteGroupMembershipCommand({
IdentityStoreId: effectiveStoreID(),
MembershipId: membershipID
})
);
await openViewMembersModal(viewMembersGroup);
void loadGroupMemberCounts(groups);
toast.success('Member removed');
} catch (err: unknown) {
toast.error(`Failed to remove member: ${(err as Error).message}`);
} finally {
const next = { ...actionInProgress };
delete next[id];
actionInProgress = next;
}
}

// ---- Demo data ----

async function seedDemoData() {
seedingDemo = true;
try {
const storeID = effectiveStoreID();

// Create demo users
const demoUsers = [
{ userName: 'alice.johnson', givenName: 'Alice', familyName: 'Johnson', email: 'alice@example.com', title: 'Engineer' },
{ userName: 'bob.smith', givenName: 'Bob', familyName: 'Smith', email: 'bob@example.com', title: 'Designer' },
{ userName: 'carol.white', givenName: 'Carol', familyName: 'White', email: 'carol@example.com', title: 'Manager' },
{ userName: 'david.lee', givenName: 'David', familyName: 'Lee', email: 'david@example.com', title: 'DevOps' },
];

const createdUserIDs: string[] = [];
for (const u of demoUsers) {
try {
const r = await identityStore().send(new CreateUserCommand({
IdentityStoreId: storeID,
UserName: u.userName,
DisplayName: `${u.givenName} ${u.familyName}`,
Name: { GivenName: u.givenName, FamilyName: u.familyName },
Title: u.title,
Emails: [{ Value: u.email, Type: 'work', Primary: true }]
}));
		if (r.UserId) createdUserIDs.push(r.UserId);
		} catch (_err) {
			// user may already exist, continue
		}
}

// Create demo groups
const demoGroups = [
{ name: 'Engineering', description: 'Software engineering team' },
{ name: 'Design', description: 'Product design team' },
{ name: 'Platform', description: 'Platform & infrastructure team' },
];

const createdGroupIDs: string[] = [];
for (const g of demoGroups) {
try {
const r = await identityStore().send(new CreateGroupCommand({
IdentityStoreId: storeID,
DisplayName: g.name,
Description: g.description
}));
		if (r.GroupId) createdGroupIDs.push(r.GroupId);
		} catch (_err) {
			// group may already exist, continue
		}
}

// Add memberships: user 0+3 -> Engineering, user 1 -> Design, user 2+3 -> Platform
const plan: Array<[number, number]> = [[0,0],[3,0],[1,1],[2,2],[3,2]];
for (const [ui, gi] of plan) {
if (createdUserIDs[ui] && createdGroupIDs[gi]) {
try {
await identityStore().send(new CreateGroupMembershipCommand({
IdentityStoreId: storeID,
GroupId: createdGroupIDs[gi],
MemberId: { UserId: createdUserIDs[ui] }
			}));
			} catch (_err) {
				// membership may already exist, continue
			}
}
}

await refresh();
toast.success('Demo data loaded');
} catch (err: unknown) {
toast.error(`Failed to seed demo data: ${(err as Error).message}`);
} finally {
seedingDemo = false;
}
}

// ---- Modal management ----

function closeAllModals() {
showCreateUserModal = false;
showCreateGroupModal = false;
showMembershipModal = false;
showEditUserModal = false;
showEditGroupModal = false;
showDeleteUserConfirm = false;
showDeleteGroupConfirm = false;
showViewMembersModal = false;
}

function handleKeydown(event: KeyboardEvent) {
if (event.key === 'Escape') closeAllModals();
}

// Users, groups, and every user/group-scoped selection below are looked up
// by ID in the emulator via a region-composite key (services/identitystore:
// regionKey(region, id)), so they are region-scoped and must be cleared (not
// just reloaded) on a region switch, along with any open modal referencing
// them. The keydown listener is genuine setup, so it stays in onMount;
// only the load path moves to onRegionChange.
onRegionChange(() => {
users = [];
groups = [];
membershipsForUser = {};
groupMemberCounts = {};
membershipUser = null;
profileUser = null;
editGroupTarget = null;
deleteUserTarget = null;
deleteGroupTarget = null;
viewMembersGroup = null;
membersOfGroup = [];
membershipsOfGroup = [];
closeAllModals();
void refresh();
});

onMount(() => {
window.addEventListener('keydown', handleKeydown);
return () => window.removeEventListener('keydown', handleKeydown);
});
</script>

<div class="space-y-6 p-6">
<!-- Header -->
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Identity Store</h1>
<p class="mt-1 text-sm text-slate-600 dark:text-slate-300">
Manage users and groups in AWS IAM Identity Center
</p>
{#if lastLoaded}
<p class="mt-1 text-xs text-slate-400 dark:text-slate-500">
Last loaded: {lastLoaded.toLocaleTimeString()}
</p>
{/if}
</div>
<div class="flex flex-wrap gap-2">
<button
type="button"
onclick={() => void refresh()}
disabled={loading}
aria-label="Refresh identity store data"
class="flex items-center gap-1.5 rounded-lg border border-slate-300 px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-700"
>
<RefreshCw size={14} class={loading ? 'animate-spin' : ''} />
{loading ? 'Loading…' : 'Refresh'}
</button>
<button
type="button"
onclick={() => void seedDemoData()}
disabled={seedingDemo || loading}
aria-label="Seed demo data into the identity store"
class="flex items-center gap-1.5 rounded-lg border border-amber-300 bg-amber-50 px-3 py-2 text-sm font-medium text-amber-700 hover:bg-amber-100 disabled:opacity-50 dark:border-amber-700 dark:bg-amber-900/20 dark:text-amber-300"
>
<Beaker size={14} />
{seedingDemo ? 'Seeding…' : 'Demo Data'}
</button>
</div>
</div>
<div class="mt-4 flex flex-col gap-2 sm:flex-row sm:items-end">
<div class="w-full sm:max-w-xs">
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="identity-store-id-input"
>
IdentityStoreId
</label>
<input
id="identity-store-id-input"
name="identity_store_id"
bind:value={storeId}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder={defaultStoreID}
/>
</div>
<button
type="button"
onclick={() => void refresh()}
disabled={loading}
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
>
{loading ? 'Loading…' : 'Load Store'}
</button>
</div>
{#if loadError}
<div class="mt-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-800 dark:bg-red-900/20 dark:text-red-300">
<span class="font-medium">Error:</span> {loadError}
<button type="button" onclick={() => void refresh()} class="ml-2 underline">Retry</button>
</div>
{/if}
</div>

<!-- Tabs -->
<div class="flex gap-0 border-b border-slate-200 dark:border-slate-700">
{#each ([['users','Users',users.length],['groups','Groups',groups.length],['docs','Docs',null],['metrics','Metrics',null]] as const) as [tab, label, count]}
<button
type="button"
onclick={() => (activeTab = tab as typeof activeTab)}
class={`flex items-center gap-1.5 rounded-t-lg border px-4 py-2 text-sm font-medium transition-colors ${
activeTab === tab
? 'border-b-0 border-indigo-600 bg-white text-indigo-700 dark:border-indigo-400 dark:bg-slate-800 dark:text-indigo-300'
: 'border-transparent text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
}`}
>
{label}
{#if count !== null}
<span class="rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-600 dark:bg-slate-700 dark:text-slate-300"
aria-label="{count} {label.toLowerCase()}">{count}</span>
{/if}
</button>
{/each}
</div>

<!-- Users tab -->
{#if activeTab === 'users'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
<div class="flex w-full items-center gap-2 sm:max-w-sm">
<input
bind:value={userSearch}
aria-label="Search users by username, name, or email"
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Search users…"
/>
{#if userSearch}
<span class="text-xs text-slate-500 dark:text-slate-400 whitespace-nowrap">
{sortedUsers().length} of {users.length}
</span>
{/if}
</div>
<button
type="button"
onclick={() => (showCreateUserModal = true)}
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
aria-label="Create new user"
>
+ Create User
</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300">
<th class="py-2 pr-4">
<button type="button" onclick={() => toggleUserSort('UserName')} class="flex items-center gap-1 hover:text-slate-900 dark:hover:text-white" aria-label="Sort by username">
Username
{#if userSortCol === 'UserName'}{#if userSortAsc}<ChevronUp size={12}/>{:else}<ChevronDown size={12}/>{/if}{/if}
</button>
</th>
<th class="hidden py-2 pr-4 sm:table-cell">
<button type="button" onclick={() => toggleUserSort('DisplayName')} class="flex items-center gap-1 hover:text-slate-900 dark:hover:text-white" aria-label="Sort by display name">
Display Name
{#if userSortCol === 'DisplayName'}{#if userSortAsc}<ChevronUp size={12}/>{:else}<ChevronDown size={12}/>{/if}{/if}
</button>
</th>
<th class="hidden py-2 pr-4 md:table-cell">
<button type="button" onclick={() => toggleUserSort('Email')} class="flex items-center gap-1 hover:text-slate-900 dark:hover:text-white" aria-label="Sort by email">
Email
{#if userSortCol === 'Email'}{#if userSortAsc}<ChevronUp size={12}/>{:else}<ChevronDown size={12}/>{/if}{/if}
</button>
</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if loading}
{#each { length: 3 } as _}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td colspan="4" class="py-3">
<div class="h-4 w-full animate-pulse rounded bg-slate-200 dark:bg-slate-700"></div>
</td>
</tr>
{/each}
{:else if users.length === 0}
<tr>
<td colspan="4" class="py-8 text-center text-slate-500 dark:text-slate-400">
<Users class="mx-auto mb-2 opacity-40" size={32} />
<p>No users found. Click <strong>+ Create User</strong> to add one, or use <strong>Demo Data</strong> to load samples.</p>
</td>
</tr>
{:else if sortedUsers().length === 0}
<tr>
<td colspan="4" class="py-4 text-slate-500 dark:text-slate-400">No users match the search.</td>
</tr>
{:else}
{#each sortedUsers() as user (user.UserId)}
<tr class="border-b border-slate-100 hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-700/30">
<td class="py-3 pr-4">
<div class="font-mono text-xs text-slate-700 dark:text-slate-200">{user.UserName}</div>
<div class="mt-0.5 flex items-center gap-1">
<span class="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-400 dark:bg-slate-700 dark:text-slate-500"
title={user.UserId}>{shortID(user.UserId)}</span>
<button
type="button"
onclick={() => void copyToClipboard(user.UserId ?? '', 'User ID')}
aria-label="Copy user ID"
class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300"
>
<Copy size={10} />
</button>
</div>
{#if user.Title}
<div class="text-xs text-slate-400 dark:text-slate-500">{user.Title}</div>
{/if}
</td>
<td class="hidden py-3 pr-4 sm:table-cell">
<div>{user.DisplayName ?? '—'}</div>
{#if user.Name?.GivenName || user.Name?.FamilyName}
<div class="text-xs text-slate-400">{[user.Name?.GivenName, user.Name?.FamilyName].filter(Boolean).join(' ')}</div>
{/if}
</td>
<td class="hidden py-3 pr-4 text-slate-600 dark:text-slate-400 md:table-cell">
<div>{user.Emails?.[0]?.Value ?? '—'}</div>
{#if user.PhoneNumbers?.[0]?.Value}
<div class="text-xs text-slate-400 dark:text-slate-500">{user.PhoneNumbers[0].Value}</div>
{/if}
</td>
<td class="py-3">
<div class="flex flex-wrap gap-1">
<button
type="button"
disabled={actionInProgress[user.UserId ?? '']}
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => openProfileEditor(user)}
aria-label="Edit user {user.UserName}"
>Edit</button>
<button
type="button"
disabled={actionInProgress[user.UserId ?? '']}
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => void openMembershipModal(user)}
aria-label="Manage memberships for {user.UserName}"
>Memberships</button>
<button
type="button"
disabled={actionInProgress[user.UserId ?? '']}
class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 disabled:opacity-40 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20"
onclick={() => { deleteUserTarget = user; showDeleteUserConfirm = true; }}
aria-label="Delete user {user.UserName}"
>Delete</button>
</div>
</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
{/if}

<!-- Groups tab -->
{#if activeTab === 'groups'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
<div class="flex w-full items-center gap-2 sm:max-w-sm">
<input
bind:value={groupSearch}
aria-label="Search groups by name or description"
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Search groups…"
/>
{#if groupSearch}
<span class="text-xs text-slate-500 dark:text-slate-400 whitespace-nowrap">
{sortedGroups().length} of {groups.length}
</span>
{/if}
</div>
<button
type="button"
onclick={() => (showCreateGroupModal = true)}
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
aria-label="Create new group"
>
+ Create Group
</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300">
<th class="py-2 pr-4">
<button type="button" onclick={() => toggleGroupSort('DisplayName')} class="flex items-center gap-1 hover:text-slate-900 dark:hover:text-white" aria-label="Sort by group name">
Group
{#if groupSortCol === 'DisplayName'}{#if groupSortAsc}<ChevronUp size={12}/>{:else}<ChevronDown size={12}/>{/if}{/if}
</button>
</th>
<th class="hidden py-2 pr-4 sm:table-cell">
<button type="button" onclick={() => toggleGroupSort('Description')} class="flex items-center gap-1 hover:text-slate-900 dark:hover:text-white" aria-label="Sort by description">
Description
{#if groupSortCol === 'Description'}{#if groupSortAsc}<ChevronUp size={12}/>{:else}<ChevronDown size={12}/>{/if}{/if}
</button>
</th>
<th class="py-2 pr-4">
<button type="button" onclick={() => toggleGroupSort('count')} class="flex items-center gap-1 hover:text-slate-900 dark:hover:text-white" aria-label="Sort by member count">
Members
{#if groupSortCol === 'count'}{#if groupSortAsc}<ChevronUp size={12}/>{:else}<ChevronDown size={12}/>{/if}{/if}
</button>
</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if loading}
{#each { length: 3 } as _}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td colspan="4" class="py-3">
<div class="h-4 w-full animate-pulse rounded bg-slate-200 dark:bg-slate-700"></div>
</td>
</tr>
{/each}
{:else if groups.length === 0}
<tr>
<td colspan="4" class="py-8 text-center text-slate-500 dark:text-slate-400">
<Layers class="mx-auto mb-2 opacity-40" size={32} />
<p>No groups found. Click <strong>+ Create Group</strong> to add one, or use <strong>Demo Data</strong>.</p>
</td>
</tr>
{:else if sortedGroups().length === 0}
<tr>
<td colspan="4" class="py-4 text-slate-500 dark:text-slate-400">No groups match the search.</td>
</tr>
{:else}
{#each sortedGroups() as group (group.GroupId)}
<tr class="border-b border-slate-100 hover:bg-slate-50 dark:border-slate-800 dark:hover:bg-slate-700/30">
<td class="py-3 pr-4">
<div class="font-medium">{group.DisplayName}</div>
<div class="mt-0.5 flex items-center gap-1">
<span class="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-400 dark:bg-slate-700 dark:text-slate-500"
title={group.GroupId}>{shortID(group.GroupId)}</span>
<button
type="button"
onclick={() => void copyToClipboard(group.GroupId ?? '', 'Group ID')}
aria-label="Copy group ID"
class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-300"
>
<Copy size={10} />
</button>
</div>
</td>
<td class="hidden py-3 pr-4 text-slate-500 dark:text-slate-400 sm:table-cell">
{group.Description ?? '—'}
</td>
<td class="py-3 pr-4">
{#if groupMemberCounts[group.GroupId ?? ''] !== undefined}
<span class="rounded-full bg-slate-100 px-2 py-0.5 text-xs font-medium text-slate-600 dark:bg-slate-700 dark:text-slate-300"
aria-label="{groupMemberCounts[group.GroupId ?? '']} members">
{groupMemberCounts[group.GroupId ?? '']}
</span>
{:else}
<span class="text-xs text-slate-300 dark:text-slate-600">—</span>
{/if}
</td>
<td class="py-3">
<div class="flex flex-wrap gap-1">
<button
type="button"
disabled={actionInProgress[group.GroupId ?? '']}
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => openEditGroup(group)}
aria-label="Edit group {group.DisplayName}"
>Edit</button>
<button
type="button"
disabled={actionInProgress[group.GroupId ?? '']}
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 disabled:opacity-40 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => void openViewMembersModal(group)}
aria-label="View members of {group.DisplayName}"
>Members</button>
<button
type="button"
disabled={actionInProgress[group.GroupId ?? '']}
class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 disabled:opacity-40 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20"
onclick={() => { deleteGroupTarget = group; showDeleteGroupConfirm = true; }}
aria-label="Delete group {group.DisplayName}"
>Delete</button>
</div>
</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
{/if}

<!-- Docs tab -->
{#if activeTab === 'docs'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<h2 class="mb-4 text-xl font-semibold text-slate-900 dark:text-white">Supported Operations</h2>
<p class="mb-6 text-sm text-slate-600 dark:text-slate-300">
All 19 AWS Identity Store API operations are implemented. The service uses the
<code class="rounded bg-slate-100 px-1 dark:bg-slate-700">X-Amz-Target: AWSIdentityStore.&lt;Operation&gt;</code> protocol.
</p>
<div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
{#each [
{ op: 'CreateUser', desc: 'Create a new user in the identity store.' },
{ op: 'DescribeUser', desc: 'Retrieve a user by UserId.' },
{ op: 'UpdateUser', desc: 'Apply attribute patch operations to a user.' },
{ op: 'DeleteUser', desc: 'Delete a user and cascade-remove their memberships.' },
{ op: 'ListUsers', desc: 'List users with optional AttributePath filters (username, displayName, emails.value, phonenumbers.value, name.givenname, name.familyname, title, nickname, usertype) and MaxResults/NextToken pagination.' },
{ op: 'GetUserId', desc: 'Resolve a UserId from a UserName, email, or ExternalId alternate identifier.' },
{ op: 'CreateGroup', desc: 'Create a new group in the identity store.' },
{ op: 'DescribeGroup', desc: 'Retrieve a group by GroupId.' },
{ op: 'UpdateGroup', desc: 'Apply attribute patch operations to a group.' },
{ op: 'DeleteGroup', desc: 'Delete a group and cascade-remove its memberships.' },
{ op: 'ListGroups', desc: 'List groups with optional AttributePath filters (displayName, description) and MaxResults/NextToken pagination.' },
{ op: 'GetGroupId', desc: 'Resolve a GroupId from a DisplayName alternate identifier.' },
{ op: 'CreateGroupMembership', desc: 'Add a user to a group.' },
{ op: 'DescribeGroupMembership', desc: 'Retrieve a membership record by MembershipId.' },
{ op: 'DeleteGroupMembership', desc: 'Remove a user from a group.' },
{ op: 'ListGroupMemberships', desc: 'List all memberships for a group with pagination.' },
{ op: 'ListGroupMembershipsForMember', desc: 'List all group memberships for a user with pagination.' },
{ op: 'GetGroupMembershipId', desc: 'Resolve a MembershipId from GroupId + MemberId.' },
{ op: 'IsMemberInGroups', desc: 'Check membership existence for up to 100 groups at once (O(1) via index).' },
] as op}
<div class="rounded-lg border border-slate-200 p-4 dark:border-slate-700">
<div class="mb-1 font-mono text-sm font-semibold text-indigo-700 dark:text-indigo-400">{op.op}</div>
<div class="text-xs text-slate-600 dark:text-slate-400">{op.desc}</div>
</div>
{/each}
</div>
</div>
{/if}

<!-- Metrics tab -->
{#if activeTab === 'metrics'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<h2 class="mb-6 text-xl font-semibold text-slate-900 dark:text-white">Store Metrics</h2>
<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
<div class="rounded-xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
<div class="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Total Users</div>
<div class="mt-2 text-4xl font-bold text-indigo-600 dark:text-indigo-400">{users.length}</div>
</div>
<div class="rounded-xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
<div class="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Total Groups</div>
<div class="mt-2 text-4xl font-bold text-indigo-600 dark:text-indigo-400">{groups.length}</div>
</div>
<div class="rounded-xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
<div class="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Total Memberships</div>
<div class="mt-2 text-4xl font-bold text-indigo-600 dark:text-indigo-400">
{Object.values(groupMemberCounts).reduce((a, b) => a + b, 0)}
</div>
</div>
<div class="rounded-xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
<div class="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Identity Store ID</div>
<div class="mt-2 flex items-center gap-2">
<span class="font-mono text-sm font-semibold text-slate-700 dark:text-slate-200">{effectiveStoreID()}</span>
<button
type="button"
onclick={() => void copyToClipboard(effectiveStoreID(), 'Store ID')}
aria-label="Copy identity store ID"
class="text-slate-400 hover:text-slate-600"
>
<Copy size={14} />
</button>
</div>
</div>
<div class="rounded-xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
<div class="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Empty Groups</div>
<div class="mt-2 text-4xl font-bold text-amber-500 dark:text-amber-400">
{groups.filter((g) => (groupMemberCounts[g.GroupId ?? ''] ?? 0) === 0).length}
</div>
</div>
<div class="rounded-xl border border-slate-200 bg-slate-50 p-4 dark:border-slate-700 dark:bg-slate-900">
<div class="text-xs font-medium uppercase tracking-wide text-slate-500 dark:text-slate-400">Avg Members / Group</div>
<div class="mt-2 text-4xl font-bold text-indigo-600 dark:text-indigo-400">
{groups.length > 0 ? (Object.values(groupMemberCounts).reduce((a, b) => a + b, 0) / groups.length).toFixed(1) : '—'}
</div>
</div>
</div>
{#if groups.length > 0}
<h3 class="mb-3 mt-6 text-base font-semibold text-slate-800 dark:text-slate-200">Members per Group</h3>
<div class="space-y-2">
{#each groups as group (group.GroupId)}
{@const count = groupMemberCounts[group.GroupId ?? ''] ?? 0}
{@const maxCount = Math.max(...Object.values(groupMemberCounts), 1)}
<div class="flex items-center gap-3">
<div class="w-36 truncate text-sm text-slate-700 dark:text-slate-200">{group.DisplayName}</div>
<div class="flex-1 rounded-full bg-slate-200 dark:bg-slate-700" style="height:8px">
<div
class="rounded-full bg-indigo-500 transition-all"
style="height:8px; width:{Math.round((count / maxCount) * 100)}%"
></div>
</div>
<span class="w-6 text-right text-sm font-medium text-slate-600 dark:text-slate-300">{count}</span>
</div>
{/each}
</div>
{/if}
</div>
{/if}
</div>

<!-- Create User Modal -->
{#if showCreateUserModal}
<div
id="create-user-modal"
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showCreateUserModal = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showCreateUserModal = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="create-user-title"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 id="create-user-title" class="mb-4 text-xl font-semibold text-slate-900 dark:text-white">Create User</h2>
<form onsubmit={(e) => { e.preventDefault(); void createUser(); }} class="space-y-4">
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cu-username">
Username <span class="text-red-500">*</span>
</label>
<input id="cu-username" name="user_name" bind:value={userName} required
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="john.doe" />
</div>
<div class="flex gap-3">
<div class="flex-1">
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cu-given">First Name</label>
<input id="cu-given" name="given_name" bind:value={userGivenName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="John" />
</div>
<div class="flex-1">
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cu-family">Last Name</label>
<input id="cu-family" name="family_name" bind:value={userFamilyName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Doe" />
</div>
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cu-display">Display Name</label>
<input id="cu-display" name="display_name" bind:value={userDisplayName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="John Doe" />
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cu-email">Email</label>
<input id="cu-email" name="user_email" type="email" bind:value={userEmail}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="john.doe@example.com" />
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cu-phone">Phone</label>
<input id="cu-phone" name="user_phone" type="tel" bind:value={userPhone}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="+1-555-0100" />
</div>
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showCreateUserModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400">Cancel</button>
<button type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Create Group Modal -->
{#if showCreateGroupModal}
<div
id="create-group-modal"
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showCreateGroupModal = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showCreateGroupModal = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="create-group-title"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 id="create-group-title" class="mb-4 text-xl font-semibold text-slate-900 dark:text-white">Create Group</h2>
<form onsubmit={(e) => { e.preventDefault(); void createGroup(); }} class="space-y-4">
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cg-name">
Group Name <span class="text-red-500">*</span>
</label>
<input id="cg-name" name="display_name" bind:value={groupDisplayName} required
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Engineering" />
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="cg-desc">Description</label>
<input id="cg-desc" name="group_description" bind:value={groupDescription}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Optional description" />
</div>
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showCreateGroupModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400">Cancel</button>
<button type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Membership Modal -->
{#if showMembershipModal}
<div
id="membership-modal"
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showMembershipModal = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showMembershipModal = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="membership-modal-title"
>
<div class="w-full max-w-3xl rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<div class="mb-4 flex items-start justify-between">
<div>
<h2 id="membership-modal-title" class="text-xl font-semibold text-slate-900 dark:text-white">
Memberships — {membershipUser?.UserName}
</h2>
<p class="text-sm text-slate-600 dark:text-slate-300">Add or remove this user from groups</p>
</div>
<button type="button" onclick={() => (showMembershipModal = false)} aria-label="Close membership modal" title="Close (Esc)"
class="rounded border border-slate-300 px-3 py-1 text-sm dark:border-slate-600">
Close
</button>
</div>
{#if membershipLoadError}
<div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-700 dark:border-red-800 dark:bg-red-900/20 dark:text-red-300">
Failed to load memberships: {membershipLoadError}
<button type="button" onclick={() => membershipUser && void loadMembershipsForMember(membershipUser)} class="ml-2 underline">Retry</button>
</div>
{/if}
<div class="mb-4 flex gap-2">
<div class="flex-1">
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ms-group-select">Add to Group</label>
<select id="ms-group-select" bind:value={selectedMembershipGroupID} name="membership_group"
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white">
<option value="">Select group…</option>
{#each groups as group}
{#if group.GroupId && !membershipsForUser[group.GroupId]}
<option value={group.GroupId}>{group.DisplayName}</option>
{/if}
{/each}
</select>
</div>
<button type="button" disabled={!selectedMembershipGroupID || membershipLoading}
class="mt-5 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-40"
onclick={() => void addMembership()}
aria-label="Add user to selected group">
Add
</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300">
<th class="py-2 pr-4">Group</th>
<th class="py-2 pr-4">Status</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if membershipLoading}
<tr><td colspan="3" class="py-4 text-center text-slate-500 dark:text-slate-400">Loading…</td></tr>
{:else if groups.length === 0}
<tr><td colspan="3" class="py-3 text-slate-500 dark:text-slate-400">No groups found</td></tr>
{:else}
{#each groups as group}
{@const membership = group.GroupId ? membershipsForUser[group.GroupId] : undefined}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 pr-4">
<div>{group.DisplayName}</div>
{#if group.Description}
<div class="text-xs text-slate-400 dark:text-slate-500">{group.Description}</div>
{/if}
</td>
<td class="py-3 pr-4">
{#if membership}
<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700 dark:bg-green-900/30 dark:text-green-300">Member</span>
{:else}
<span class="text-xs text-slate-400 dark:text-slate-500">—</span>
{/if}
</td>
<td class="py-3">
{#if membership?.MembershipId}
<button type="button"
class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20"
onclick={() => void removeMembership(membership.MembershipId)}
aria-label="Remove from {group.DisplayName}">Remove</button>
{/if}
</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
</div>
{/if}

<!-- Edit User Profile Modal -->
{#if showEditUserModal}
<div
id="edit-user-modal"
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showEditUserModal = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showEditUserModal = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="edit-user-title"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<form onsubmit={(e) => { e.preventDefault(); void updateUserProfile(); }} class="space-y-4">
<h2 id="edit-user-title" class="text-xl font-semibold text-slate-900 dark:text-white">
Edit Profile — {profileUser?.UserName}
</h2>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ep-display">Display Name</label>
<input id="ep-display" name="profile_display_name" bind:value={profileDisplayName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Display name" />
</div>
<div class="flex gap-3">
<div class="flex-1">
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ep-given">First Name</label>
<input id="ep-given" name="profile_given_name" bind:value={profileGivenName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="John" />
</div>
<div class="flex-1">
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ep-family">Last Name</label>
<input id="ep-family" name="profile_family_name" bind:value={profileFamilyName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Doe" />
</div>
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ep-title">Title</label>
<input id="ep-title" name="profile_title" bind:value={profileTitle}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Engineer" />
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ep-email">Primary Email</label>
<input id="ep-email" name="profile_email" type="email" bind:value={profileEmail}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="user@example.com" />
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ep-phone">Primary Phone</label>
<input id="ep-phone" name="profile_phone" bind:value={profilePhone}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="+1-555-0100" />
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="ep-address">Primary Address</label>
<input id="ep-address" name="profile_address" bind:value={profileAddress}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="123 Main St, City, State" />
</div>
{#if profileUser?.ExternalIds && profileUser.ExternalIds.length > 0}
<div>
<div class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">External IDs</div>
<div class="space-y-1">
{#each profileUser.ExternalIds as extId}
<div class="rounded bg-slate-100 px-2 py-1 text-xs font-mono dark:bg-slate-700">
{extId.Issuer}: {extId.Id}
</div>
{/each}
</div>
</div>
{/if}
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showEditUserModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400">Cancel</button>
<button type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Save</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Edit Group Modal -->
{#if showEditGroupModal}
<div
id="edit-group-modal"
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showEditGroupModal = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showEditGroupModal = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="edit-group-title"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 id="edit-group-title" class="mb-4 text-xl font-semibold text-slate-900 dark:text-white">Edit Group</h2>
<form onsubmit={(e) => { e.preventDefault(); void updateGroup(); }} class="space-y-4">
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="eg-name">
Group Name <span class="text-red-500">*</span>
</label>
<input id="eg-name" name="edit_group_name" bind:value={editGroupDisplayName} required
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
</div>
<div>
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="eg-desc">Description</label>
<input id="eg-desc" name="edit_group_description" bind:value={editGroupDescription}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
</div>
<div class="flex justify-end gap-3">
<button type="button" onclick={() => (showEditGroupModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400">Cancel</button>
<button type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Save</button>
</div>
</form>
</div>
</div>
{/if}

<!-- Delete User Confirm -->
{#if showDeleteUserConfirm}
<div
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showDeleteUserConfirm = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showDeleteUserConfirm = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="delete-user-title"
>
<div class="w-full max-w-sm rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 id="delete-user-title" class="text-lg font-semibold text-slate-900 dark:text-white">Delete User</h2>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
Delete <strong>{deleteUserTarget?.UserName}</strong>? This will also remove all group memberships.
</p>
<div class="mt-4 flex justify-end gap-3">
<button type="button" onclick={() => (showDeleteUserConfirm = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400">Cancel</button>
<button type="button" onclick={() => void confirmDeleteUser()}
class="rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700">Delete</button>
</div>
</div>
</div>
{/if}

<!-- Delete Group Confirm -->
{#if showDeleteGroupConfirm}
<div
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showDeleteGroupConfirm = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showDeleteGroupConfirm = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="delete-group-title"
>
<div class="w-full max-w-sm rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 id="delete-group-title" class="text-lg font-semibold text-slate-900 dark:text-white">Delete Group</h2>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
Delete <strong>{deleteGroupTarget?.DisplayName}</strong>? This will also remove all group memberships.
</p>
<div class="mt-4 flex justify-end gap-3">
<button type="button" onclick={() => (showDeleteGroupConfirm = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400">Cancel</button>
<button type="button" onclick={() => void confirmDeleteGroup()}
class="rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700">Delete</button>
</div>
</div>
</div>
{/if}

<!-- View Group Members Modal -->
{#if showViewMembersModal}
<div
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => { if (e.target === e.currentTarget) showViewMembersModal = false; }}
onkeydown={(e) => { if (e.key === 'Escape') showViewMembersModal = false; }}
tabindex="-1"
role="dialog"
aria-modal="true"
aria-labelledby="view-members-title"
>
<div class="w-full max-w-2xl rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<div class="mb-4 flex items-start justify-between">
<div>
<h2 id="view-members-title" class="text-xl font-semibold text-slate-900 dark:text-white">
Members of {viewMembersGroup?.DisplayName}
</h2>
<div class="flex items-center gap-2">
<p class="text-sm text-slate-500 dark:text-slate-400">
{membersOfGroup.length} member{membersOfGroup.length === 1 ? '' : 's'}
</p>
<button type="button" onclick={() => viewMembersGroup && void openViewMembersModal(viewMembersGroup)}
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
aria-label="Refresh member list">
<RefreshCw size={12} />
</button>
</div>
</div>
<button type="button" onclick={() => (showViewMembersModal = false)} title="Close (Esc)"
class="rounded border border-slate-300 px-3 py-1 text-sm dark:border-slate-600">
Close
</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300">
<th class="py-2 pr-4">Username</th>
<th class="py-2 pr-4">Display Name</th>
<th class="py-2 pr-4">Email</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if membersOfGroup.length === 0}
<tr><td colspan="4" class="py-3 text-slate-500 dark:text-slate-400">No members in this group</td></tr>
{:else}
{#each membersOfGroup as member (member.UserId)}
{@const mem = membershipsOfGroup.find((m) => m.MemberId?.UserId === member.UserId)}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 pr-4 font-mono text-xs">{member.UserName}</td>
<td class="py-3 pr-4">{member.DisplayName ?? '—'}</td>
<td class="py-3 pr-4 text-slate-500 dark:text-slate-400">{member.Emails?.[0]?.Value ?? '—'}</td>
<td class="py-3">
{#if mem?.MembershipId}
<button
type="button"
disabled={actionInProgress[mem.MembershipId]}
class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 disabled:opacity-40 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20"
onclick={() => void removeMemberFromGroup(mem?.MembershipId)}
aria-label="Remove {member.UserName} from group"
>Remove</button>
{/if}
</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
</div>
{/if}

