<script lang="ts">
import { onMount } from 'svelte';
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

const identityStore = getIdentityStoreClient();
const defaultStoreID = 'd-0000000000';

type IdentityUser = {
UserId?: string;
UserName?: string;
DisplayName?: string;
Emails?: Array<{ Value?: string; Type?: string; Primary?: boolean }>;
Addresses?: Array<{ Formatted?: string; Type?: string; Primary?: boolean }>;
PhoneNumbers?: Array<{ Value?: string; Type?: string; Primary?: boolean }>;
Name?: { GivenName?: string; FamilyName?: string };
};
type IdentityGroup = {
GroupId?: string;
DisplayName?: string;
Description?: string;
};
type IdentityMembership = { MembershipId?: string; GroupId?: string };

let storeId = $state(defaultStoreID);
let activeTab = $state<'users' | 'groups'>('users');
let users = $state<IdentityUser[]>([]);
let groups = $state<IdentityGroup[]>([]);
let loading = $state(false);
let userSearch = $state('');
let groupSearch = $state('');
let membershipsForUser = $state<Record<string, IdentityMembership>>({});

let showCreateUserModal = $state(false);
let showCreateGroupModal = $state(false);
let showMembershipModal = $state(false);
let showEditUserModal = $state(false);
let showEditGroupModal = $state(false);
let showDeleteUserConfirm = $state(false);
let showDeleteGroupConfirm = $state(false);
let showViewMembersModal = $state(false);

let userName = $state('');
let userDisplayName = $state('');
let userGivenName = $state('');
let userFamilyName = $state('');
let userEmail = $state('');
let groupDisplayName = $state('');
let groupDescription = $state('');
let selectedMembershipGroupID = $state('');
let membershipUser = $state<IdentityUser | null>(null);
let profileUser = $state<IdentityUser | null>(null);
let profileDisplayName = $state('');
let profileEmail = $state('');
let profilePhone = $state('');
let profileAddress = $state('');
let editGroupTarget = $state<IdentityGroup | null>(null);
let editGroupDisplayName = $state('');
let editGroupDescription = $state('');
let deleteUserTarget = $state<IdentityUser | null>(null);
let deleteGroupTarget = $state<IdentityGroup | null>(null);
let viewMembersGroup = $state<IdentityGroup | null>(null);
let membersOfGroup = $state<IdentityUser[]>([]);

function effectiveStoreID(): string {
return storeId.trim() || defaultStoreID;
}

let filteredUsers = $derived(
users.filter(
(u) =>
!userSearch ||
(u.UserName ?? '').toLowerCase().includes(userSearch.toLowerCase()) ||
(u.DisplayName ?? '').toLowerCase().includes(userSearch.toLowerCase()) ||
(u.Emails?.[0]?.Value ?? '').toLowerCase().includes(userSearch.toLowerCase())
)
);

let filteredGroups = $derived(
groups.filter(
(g) =>
!groupSearch ||
(g.DisplayName ?? '').toLowerCase().includes(groupSearch.toLowerCase()) ||
(g.Description ?? '').toLowerCase().includes(groupSearch.toLowerCase())
)
);

async function loadUsers() {
const out = await identityStore.send(
new ListUsersCommand({ IdentityStoreId: effectiveStoreID() })
);
users = (out.Users ?? []) as IdentityUser[];
}

async function loadGroups() {
const out = await identityStore.send(
new ListGroupsCommand({ IdentityStoreId: effectiveStoreID() })
);
groups = (out.Groups ?? []) as IdentityGroup[];
}

async function loadMembershipsForMember(user: IdentityUser) {
if (!user.UserId) {
membershipsForUser = {};

return;
}

const out = await identityStore.send(
new ListGroupMembershipsForMemberCommand({
IdentityStoreId: effectiveStoreID(),
MemberId: { UserId: user.UserId }
})
);

membershipsForUser = Object.fromEntries(
((out.GroupMemberships ?? []) as IdentityMembership[])
.filter((membership) => membership.GroupId)
.map((membership) => [membership.GroupId ?? '', membership])
);
}

async function refresh() {
loading = true;
try {
await Promise.all([loadUsers(), loadGroups()]);
membershipsForUser = {};
membershipUser = null;
toast.success('Identity store loaded');
} catch (err: unknown) {
toast.error(`Failed to load identity store: ${(err as Error).message}`);
} finally {
loading = false;
}
}

async function createUser() {
try {
await identityStore.send(
new CreateUserCommand({
IdentityStoreId: effectiveStoreID(),
UserName: userName,
DisplayName:
userDisplayName || `${userGivenName} ${userFamilyName}`.trim() || undefined,
Name:
userGivenName || userFamilyName
? { GivenName: userGivenName, FamilyName: userFamilyName }
: undefined,
Emails: userEmail ? [{ Value: userEmail, Type: 'work', Primary: true }] : undefined
})
);
showCreateUserModal = false;
userName = '';
userDisplayName = '';
userGivenName = '';
userFamilyName = '';
userEmail = '';
await loadUsers();
toast.success('User created');
} catch (err: unknown) {
toast.error(`Failed to create user: ${(err as Error).message}`);
}
}

async function createGroup() {
try {
await identityStore.send(
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
toast.success('Group created');
} catch (err: unknown) {
toast.error(`Failed to create group: ${(err as Error).message}`);
}
}

async function openMembershipModal(user: IdentityUser) {
membershipUser = user;
selectedMembershipGroupID = '';
showMembershipModal = true;

try {
await loadMembershipsForMember(user);
} catch (err: unknown) {
toast.error(`Failed to load memberships: ${(err as Error).message}`);
}
}

async function addMembership() {
if (!membershipUser?.UserId || !selectedMembershipGroupID) {
return;
}

try {
await identityStore.send(
new CreateGroupMembershipCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: selectedMembershipGroupID,
MemberId: { UserId: membershipUser.UserId }
})
);
selectedMembershipGroupID = '';
await loadMembershipsForMember(membershipUser);
toast.success('Membership added');
} catch (err: unknown) {
toast.error(`Failed to add group membership: ${(err as Error).message}`);
}
}

async function removeMembership(membershipID?: string) {
if (!membershipID || !membershipUser?.UserId) {
return;
}

try {
await identityStore.send(
new DeleteGroupMembershipCommand({
IdentityStoreId: effectiveStoreID(),
MembershipId: membershipID
})
);
await loadMembershipsForMember(membershipUser);
toast.success('Membership removed');
} catch (err: unknown) {
toast.error(`Failed to remove group membership: ${(err as Error).message}`);
}
}

function openProfileEditor(user: IdentityUser) {
profileUser = user;
profileDisplayName = user.DisplayName ?? '';
profileEmail = user.Emails?.[0]?.Value ?? '';
profilePhone = user.PhoneNumbers?.[0]?.Value ?? '';
profileAddress = user.Addresses?.[0]?.Formatted ?? '';
showEditUserModal = true;
}

async function updateUserProfile() {
if (!profileUser?.UserId) {
return;
}

try {
await identityStore.send(
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
}
]
})
);
showEditUserModal = false;
await loadUsers();
toast.success('User profile updated');
} catch (err: unknown) {
toast.error(`Failed to update user profile: ${(err as Error).message}`);
}
}

async function confirmDeleteUser() {
if (!deleteUserTarget?.UserId) {
return;
}

try {
await identityStore.send(
new DeleteUserCommand({
IdentityStoreId: effectiveStoreID(),
UserId: deleteUserTarget.UserId
})
);
showDeleteUserConfirm = false;
deleteUserTarget = null;
await loadUsers();
toast.success('User deleted');
} catch (err: unknown) {
toast.error(`Failed to delete user: ${(err as Error).message}`);
}
}

async function confirmDeleteGroup() {
if (!deleteGroupTarget?.GroupId) {
return;
}

try {
await identityStore.send(
new DeleteGroupCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: deleteGroupTarget.GroupId
})
);
showDeleteGroupConfirm = false;
deleteGroupTarget = null;
await loadGroups();
toast.success('Group deleted');
} catch (err: unknown) {
toast.error(`Failed to delete group: ${(err as Error).message}`);
}
}

function openEditGroup(group: IdentityGroup) {
editGroupTarget = group;
editGroupDisplayName = group.DisplayName ?? '';
editGroupDescription = group.Description ?? '';
showEditGroupModal = true;
}

async function updateGroup() {
if (!editGroupTarget?.GroupId) {
return;
}

try {
await identityStore.send(
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
toast.success('Group updated');
} catch (err: unknown) {
toast.error(`Failed to update group: ${(err as Error).message}`);
}
}

async function openViewMembersModal(group: IdentityGroup) {
viewMembersGroup = group;
membersOfGroup = [];
showViewMembersModal = true;

try {
// Single API call: list all memberships for this group, then join with the users list.
const out = await identityStore.send(
new ListGroupMembershipsCommand({
IdentityStoreId: effectiveStoreID(),
GroupId: group.GroupId!
})
);

const memberUserIDs = new Set(
(out.GroupMemberships ?? [])
.map((m) => m.MemberId?.UserId)
.filter(Boolean) as string[]
);

membersOfGroup = users.filter((u) => u.UserId && memberUserIDs.has(u.UserId));
} catch (err: unknown) {
toast.error(`Failed to load group members: ${(err as Error).message}`);
}
}

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
if (event.key === 'Escape') {
closeAllModals();
}
}

onMount(() => {
void refresh();
window.addEventListener('keydown', handleKeydown);

return () => {
window.removeEventListener('keydown', handleKeydown);
};
});
</script>

<div class="space-y-6 p-6">
<!-- Header -->
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Identity Store</h1>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
Manage users and groups in AWS IAM Identity Center
</p>
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
onclick={() => {
void refresh();
}}
disabled={loading}
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
>
{loading ? 'Loading…' : 'Load Store'}
</button>
</div>
</div>

<!-- Tabs -->
<div class="flex gap-2 border-b border-slate-200 dark:border-slate-700">
<button
type="button"
onclick={() => (activeTab = 'users')}
class={`rounded-t-lg border px-4 py-2 text-sm font-medium transition-colors ${
activeTab === 'users'
? 'border-b-0 border-indigo-600 bg-white text-indigo-700 dark:border-indigo-400 dark:bg-slate-800 dark:text-indigo-300'
: 'border-transparent text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
}`}
>
Users <span
class="ml-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-600 dark:bg-slate-700 dark:text-slate-300"
>{users.length}</span
>
</button>
<button
type="button"
onclick={() => (activeTab = 'groups')}
class={`rounded-t-lg border px-4 py-2 text-sm font-medium transition-colors ${
activeTab === 'groups'
? 'border-b-0 border-indigo-600 bg-white text-indigo-700 dark:border-indigo-400 dark:bg-slate-800 dark:text-indigo-300'
: 'border-transparent text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'
}`}
>
Groups <span
class="ml-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs text-slate-600 dark:bg-slate-700 dark:text-slate-300"
>{groups.length}</span
>
</button>
</div>

{#if activeTab === 'users'}
<div
class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800"
>
<div
class="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"
>
<input
bind:value={userSearch}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm sm:max-w-xs dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Search users…"
/>
<button
type="button"
onclick={() => (showCreateUserModal = true)}
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
>
+ Create User
</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr
class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300"
>
<th class="py-2 pr-4">Username</th>
<th class="py-2 pr-4">Display Name</th>
<th class="py-2 pr-4">Email</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if loading}
<tr
><td
class="py-4 text-center text-slate-400 dark:text-slate-500"
colspan="4">Loading…</td
></tr
>
{:else if filteredUsers.length === 0}
<tr
><td class="py-3 text-slate-500 dark:text-slate-400" colspan="4"
>{users.length === 0
? 'No users found'
: 'No users match the search'}</td
></tr
>
{:else}
{#each filteredUsers as user}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 pr-4 font-mono text-xs text-slate-700 dark:text-slate-200"
>{user.UserName}</td
>
<td class="py-3 pr-4">{user.DisplayName ?? '—'}</td>
<td class="py-3 pr-4 text-slate-600 dark:text-slate-400"
>{user.Emails?.[0]?.Value ?? '—'}</td
>
<td class="py-3">
<div class="flex flex-wrap gap-1">
<button
type="button"
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => openProfileEditor(user)}>Edit</button
>
<button
type="button"
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => void openMembershipModal(user)}
>Memberships</button
>
<button
type="button"
class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20"
onclick={() => {
deleteUserTarget = user;
showDeleteUserConfirm = true;
}}>Delete</button
>
</div>
</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
{:else}
<div
class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800"
>
<div
class="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between"
>
<input
bind:value={groupSearch}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm sm:max-w-xs dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Search groups…"
/>
<button
type="button"
onclick={() => (showCreateGroupModal = true)}
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
>
+ Create Group
</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr
class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300"
>
<th class="py-2 pr-4">Group</th>
<th class="py-2 pr-4">Description</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if loading}
<tr
><td
class="py-4 text-center text-slate-400 dark:text-slate-500"
colspan="3">Loading…</td
></tr
>
{:else if filteredGroups.length === 0}
<tr
><td class="py-3 text-slate-500 dark:text-slate-400" colspan="3"
>{groups.length === 0
? 'No groups found'
: 'No groups match the search'}</td
></tr
>
{:else}
{#each filteredGroups as group}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 pr-4 font-medium">{group.DisplayName}</td>
<td class="py-3 pr-4 text-slate-500 dark:text-slate-400"
>{group.Description ?? '—'}</td
>
<td class="py-3">
<div class="flex flex-wrap gap-1">
<button
type="button"
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => openEditGroup(group)}>Edit</button
>
<button
type="button"
class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700"
onclick={() => void openViewMembersModal(group)}>Members</button
>
<button
type="button"
class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20"
onclick={() => {
deleteGroupTarget = group;
showDeleteGroupConfirm = true;
}}>Delete</button
>
</div>
</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
{/if}
</div>

<!-- Create User Modal -->
{#if showCreateUserModal}
<div
id="create-user-modal"
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => {
if (e.target === e.currentTarget) showCreateUserModal = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-xl font-semibold text-slate-900 dark:text-white">Create User</h2>
<form
onsubmit={(event) => {
event.preventDefault();
void createUser();
}}
class="space-y-4"
>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="cu-username"
>Username <span class="text-red-500">*</span></label
>
<input
id="cu-username"
name="user_name"
bind:value={userName}
required
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="john.doe"
/>
</div>
<div class="flex gap-3">
<div class="flex-1">
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="cu-given">First Name</label
>
<input
id="cu-given"
name="given_name"
bind:value={userGivenName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="John"
/>
</div>
<div class="flex-1">
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="cu-family">Last Name</label
>
<input
id="cu-family"
name="family_name"
bind:value={userFamilyName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Doe"
/>
</div>
</div>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="cu-display">Display Name</label
>
<input
id="cu-display"
name="display_name"
bind:value={userDisplayName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="John Doe"
/>
</div>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="cu-email">Email</label
>
<input
id="cu-email"
name="user_email"
type="email"
bind:value={userEmail}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="john.doe@example.com"
/>
</div>
<div class="flex justify-end gap-3">
<button
type="button"
onclick={() => (showCreateUserModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400"
>Cancel</button
>
<button
type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
>Create</button
>
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
onclick={(e) => {
if (e.target === e.currentTarget) showCreateGroupModal = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-xl font-semibold text-slate-900 dark:text-white">Create Group</h2>
<form
onsubmit={(event) => {
event.preventDefault();
void createGroup();
}}
class="space-y-4"
>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="cg-name"
>Group Name <span class="text-red-500">*</span></label
>
<input
id="cg-name"
name="display_name"
bind:value={groupDisplayName}
required
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Engineering"
/>
</div>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="cg-desc">Description</label
>
<input
id="cg-desc"
name="group_description"
bind:value={groupDescription}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Optional description"
/>
</div>
<div class="flex justify-end gap-3">
<button
type="button"
onclick={() => (showCreateGroupModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400"
>Cancel</button
>
<button
type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
>Create</button
>
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
onclick={(e) => {
if (e.target === e.currentTarget) showMembershipModal = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-3xl rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<div class="mb-4 flex items-start justify-between">
<div>
<h2 class="text-xl font-semibold text-slate-900 dark:text-white">
Memberships — {membershipUser?.UserName}
</h2>
<p class="text-sm text-slate-600 dark:text-slate-300">
Add or remove this user from groups
</p>
</div>
<button
type="button"
class="rounded border border-slate-300 px-3 py-1 text-sm dark:border-slate-600"
onclick={() => (showMembershipModal = false)}>Close</button
>
</div>
<div class="mb-4 flex gap-2">
<div class="flex-1">
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="ms-group-select">Add to Group</label
>
<select
id="ms-group-select"
bind:value={selectedMembershipGroupID}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
name="membership_group"
>
<option value="">Select group…</option>
{#each groups as group}
{#if group.GroupId && !membershipsForUser[group.GroupId]}
<option value={group.GroupId}>{group.DisplayName}</option>
{/if}
{/each}
</select>
</div>
<button
type="button"
disabled={!selectedMembershipGroupID}
class="mt-5 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-40"
onclick={() => void addMembership()}
>
Add
</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr
class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300"
>
<th class="py-2 pr-4">Group</th>
<th class="py-2 pr-4">Status</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if groups.length === 0}
<tr
><td class="py-3 text-slate-500 dark:text-slate-400" colspan="3"
>No groups found</td
></tr
>
{:else}
{#each groups as group}
{@const membership = group.GroupId
? membershipsForUser[group.GroupId]
: undefined}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 pr-4">{group.DisplayName}</td>
<td class="py-3 pr-4">
{#if membership}
<span
class="rounded-full bg-green-100 px-2 py-0.5 text-xs font-medium text-green-700 dark:bg-green-900/30 dark:text-green-300"
>Member</span
>
{:else}
<span class="text-xs text-slate-400 dark:text-slate-500">—</span>
{/if}
</td>
<td class="py-3">
{#if membership?.MembershipId}
<button
type="button"
class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20"
onclick={() => void removeMembership(membership.MembershipId)}
>Remove</button
>
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
onclick={(e) => {
if (e.target === e.currentTarget) showEditUserModal = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<form
onsubmit={(event) => {
event.preventDefault();
void updateUserProfile();
}}
class="space-y-4"
>
<h2 class="text-xl font-semibold text-slate-900 dark:text-white">
Edit Profile — {profileUser?.UserName}
</h2>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="ep-display">Display Name</label
>
<input
id="ep-display"
name="profile_display_name"
bind:value={profileDisplayName}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="Display name"
/>
</div>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="ep-email">Primary Email</label
>
<input
id="ep-email"
name="profile_email"
type="email"
bind:value={profileEmail}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="user@example.com"
/>
</div>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="ep-phone">Primary Phone</label
>
<input
id="ep-phone"
name="profile_phone"
bind:value={profilePhone}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="+1-555-0100"
/>
</div>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="ep-address">Primary Address</label
>
<input
id="ep-address"
name="profile_address"
bind:value={profileAddress}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
placeholder="123 Main St, City, State"
/>
</div>
<div class="flex justify-end gap-3">
<button
type="button"
onclick={() => (showEditUserModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400"
>Cancel</button
>
<button
type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
>Save</button
>
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
onclick={(e) => {
if (e.target === e.currentTarget) showEditGroupModal = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="mb-4 text-xl font-semibold text-slate-900 dark:text-white">Edit Group</h2>
<form
onsubmit={(event) => {
event.preventDefault();
void updateGroup();
}}
class="space-y-4"
>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="eg-name"
>Group Name <span class="text-red-500">*</span></label
>
<input
id="eg-name"
name="edit_group_name"
bind:value={editGroupDisplayName}
required
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
/>
</div>
<div>
<label
class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300"
for="eg-desc">Description</label
>
<input
id="eg-desc"
name="edit_group_description"
bind:value={editGroupDescription}
class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
/>
</div>
<div class="flex justify-end gap-3">
<button
type="button"
onclick={() => (showEditGroupModal = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400"
>Cancel</button
>
<button
type="submit"
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
>Save</button
>
</div>
</form>
</div>
</div>
{/if}

<!-- Delete User Confirm -->
{#if showDeleteUserConfirm}
<div
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => {
if (e.target === e.currentTarget) showDeleteUserConfirm = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-sm rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Delete User</h2>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
Delete <strong>{deleteUserTarget?.UserName}</strong>? This will also remove all group
memberships.
</p>
<div class="mt-4 flex justify-end gap-3">
<button
type="button"
onclick={() => (showDeleteUserConfirm = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400"
>Cancel</button
>
<button
type="button"
onclick={() => void confirmDeleteUser()}
class="rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700"
>Delete</button
>
</div>
</div>
</div>
{/if}

<!-- Delete Group Confirm -->
{#if showDeleteGroupConfirm}
<div
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => {
if (e.target === e.currentTarget) showDeleteGroupConfirm = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-sm rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Delete Group</h2>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
Delete <strong>{deleteGroupTarget?.DisplayName}</strong>? This will also remove all
group memberships.
</p>
<div class="mt-4 flex justify-end gap-3">
<button
type="button"
onclick={() => (showDeleteGroupConfirm = false)}
class="px-4 py-2 text-sm text-slate-600 hover:text-slate-900 dark:text-slate-400"
>Cancel</button
>
<button
type="button"
onclick={() => void confirmDeleteGroup()}
class="rounded-lg bg-red-600 px-4 py-2 text-sm font-medium text-white hover:bg-red-700"
>Delete</button
>
</div>
</div>
</div>
{/if}

<!-- View Group Members Modal -->
{#if showViewMembersModal}
<div
class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
onclick={(e) => {
if (e.target === e.currentTarget) showViewMembersModal = false;
}}
role="dialog"
aria-modal="true"
>
<div class="w-full max-w-2xl rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<div class="mb-4 flex items-start justify-between">
<div>
<h2 class="text-xl font-semibold text-slate-900 dark:text-white">
Members of {viewMembersGroup?.DisplayName}
</h2>
<p class="text-sm text-slate-500 dark:text-slate-400">
{membersOfGroup.length} member{membersOfGroup.length === 1 ? '' : 's'}
</p>
</div>
<button
type="button"
class="rounded border border-slate-300 px-3 py-1 text-sm dark:border-slate-600"
onclick={() => (showViewMembersModal = false)}>Close</button
>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr
class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300"
>
<th class="py-2 pr-4">Username</th>
<th class="py-2">Display Name</th>
</tr>
</thead>
<tbody>
{#if membersOfGroup.length === 0}
<tr
><td class="py-3 text-slate-500 dark:text-slate-400" colspan="2"
>No members in this group</td
></tr
>
{:else}
{#each membersOfGroup as member}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3 pr-4 font-mono text-xs">{member.UserName}</td>
<td class="py-3">{member.DisplayName ?? '—'}</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
</div>
{/if}
