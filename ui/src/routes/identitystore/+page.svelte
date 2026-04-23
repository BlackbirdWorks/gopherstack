<script lang="ts">
import { onMount } from 'svelte';
import { getIdentityStoreClient } from '$lib/aws-client';
import {
CreateGroupCommand,
CreateGroupMembershipCommand,
CreateUserCommand,
DeleteGroupMembershipCommand,
ListGroupMembershipsForMemberCommand,
ListGroupsCommand,
ListUsersCommand,
UpdateUserCommand
} from '@aws-sdk/client-identitystore';
import { toast } from 'svelte-sonner';

const identityStore = getIdentityStoreClient();
const defaultStoreID = 'd-0000000000';

type IdentityUser = {
UserId?: string;
UserName?: string;
DisplayName?: string;
Emails?: Array<{ Value?: string }>;
Addresses?: Array<{ Formatted?: string }>;
PhoneNumbers?: Array<{ Value?: string }>;
};
type IdentityGroup = { GroupId?: string; DisplayName?: string; Description?: string };
type IdentityMembership = { MembershipId?: string; GroupId?: string };

let storeId = $state(defaultStoreID);
let activeTab = $state<'users' | 'groups'>('users');
let users = $state<IdentityUser[]>([]);
let groups = $state<IdentityGroup[]>([]);
let membershipsForUser = $state<Record<string, IdentityMembership>>({});
let showCreateUserModal = $state(false);
let showCreateGroupModal = $state(false);
let showMembershipModal = $state(false);
let showEditUserModal = $state(false);
let userName = $state('');
let userDisplayName = $state('');
let groupDisplayName = $state('');
let selectedMembershipGroupID = $state('');
let membershipUser = $state<IdentityUser | null>(null);
let profileUser = $state<IdentityUser | null>(null);
let profileDisplayName = $state('');
let profileEmail = $state('');
let profilePhone = $state('');
let profileAddress = $state('');

function effectiveStoreID(): string {
return storeId.trim() || defaultStoreID;
}

async function loadUsers() {
const out = await identityStore.send(new ListUsersCommand({ IdentityStoreId: effectiveStoreID() }));
users = (out.Users ?? []) as IdentityUser[];
}

async function loadGroups() {
const out = await identityStore.send(new ListGroupsCommand({ IdentityStoreId: effectiveStoreID() }));
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
try {
await Promise.all([loadUsers(), loadGroups()]);
membershipsForUser = {};
membershipUser = null;
} catch (err: unknown) {
toast.error(`Failed to load identity store: ${(err as Error).message}`);
}
}

async function createUser() {
try {
await identityStore.send(
new CreateUserCommand({
IdentityStoreId: effectiveStoreID(),
UserName: userName,
DisplayName: userDisplayName
})
);
showCreateUserModal = false;
userName = '';
userDisplayName = '';
await loadUsers();
} catch (err: unknown) {
toast.error(`Failed to create user: ${(err as Error).message}`);
}
}

async function createGroup() {
try {
await identityStore.send(
new CreateGroupCommand({
IdentityStoreId: effectiveStoreID(),
DisplayName: groupDisplayName
})
);
showCreateGroupModal = false;
groupDisplayName = '';
await loadGroups();
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
await loadMembershipsForMember(membershipUser);
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
} catch (err: unknown) {
toast.error(`Failed to update user profile: ${(err as Error).message}`);
}
}

onMount(() => {
void refresh();
});
</script>

<div class="space-y-6 p-6">
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Identity Store</h1>
<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Users and groups</p>
<div class="mt-4 flex flex-col gap-2 sm:flex-row sm:items-end">
<div class="w-full sm:max-w-xs">
<label class="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300" for="identity-store-id-input">IdentityStoreId</label>
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
class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
>
Load Store
</button>
</div>
</div>

<div class="flex gap-2">
<button type="button" onclick={() => (activeTab = 'users')} class="rounded-lg border px-4 py-2 text-sm">Users</button>
<button type="button" onclick={() => (activeTab = 'groups')} class="rounded-lg border px-4 py-2 text-sm">Groups</button>
</div>

{#if activeTab === 'users'}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex justify-end">
<button type="button" onclick={() => (showCreateUserModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create User</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300">
<th class="py-2">User</th>
<th class="py-2">Display Name</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if users.length === 0}
<tr><td class="py-3 text-slate-500 dark:text-slate-400" colspan="3">No users found</td></tr>
{:else}
{#each users as user}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3">{user.UserName}</td>
<td class="py-3">{user.DisplayName}</td>
<td class="py-3">
<div class="flex gap-2">
<button type="button" class="rounded border px-2 py-1 text-xs" onclick={() => openProfileEditor(user)}>Edit Profile</button>
<button type="button" class="rounded border px-2 py-1 text-xs" onclick={() => void openMembershipModal(user)}>Manage Memberships</button>
</div>
</td>
</tr>
{/each}
{/if}
</tbody>
</table>
</div>
{:else}
<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
<div class="mb-4 flex justify-end">
<button type="button" onclick={() => (showCreateGroupModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create Group</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300">
<th class="py-2">Group</th>
<th class="py-2">Description</th>
</tr>
</thead>
<tbody>
{#if groups.length === 0}
<tr><td class="py-3 text-slate-500 dark:text-slate-400" colspan="2">No groups found</td></tr>
{:else}
{#each groups as group}
<tr class="border-b border-slate-100 dark:border-slate-800"><td class="py-3">{group.DisplayName}</td><td class="py-3">{group.Description}</td></tr>
{/each}
{/if}
</tbody>
</table>
</div>
{/if}
</div>

{#if showCreateUserModal}
<div id="create-user-modal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<form
onsubmit={(event) => {
event.preventDefault();
void createUser();
}}
class="space-y-4"
>
<input name="user_name" bind:value={userName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Username" />
<input name="display_name" bind:value={userDisplayName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Display name" />
<div class="flex justify-end gap-3"><button type="button" onclick={() => (showCreateUserModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
</form>
</div>
</div>
{/if}

{#if showCreateGroupModal}
<div id="create-group-modal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<form
onsubmit={(event) => {
event.preventDefault();
void createGroup();
}}
class="space-y-4"
>
<input name="display_name" bind:value={groupDisplayName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Group name" />
<div class="flex justify-end gap-3"><button type="button" onclick={() => (showCreateGroupModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
</form>
</div>
</div>
{/if}

{#if showMembershipModal}
<div id="membership-modal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-3xl rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<div class="mb-4 flex items-start justify-between">
<div>
<h2 class="text-xl font-semibold text-slate-900 dark:text-white">Memberships for {membershipUser?.UserName}</h2>
<p class="text-sm text-slate-600 dark:text-slate-300">Use ListGroupMembershipsForMember to review and manage membership links.</p>
</div>
<button type="button" class="rounded border px-3 py-1 text-sm" onclick={() => (showMembershipModal = false)}>Close</button>
</div>
<div class="mb-4 flex gap-2">
<select bind:value={selectedMembershipGroupID} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" name="membership_group">
<option value="">Select group</option>
{#each groups as group}
{#if group.GroupId}
<option value={group.GroupId}>{group.DisplayName}</option>
{/if}
{/each}
</select>
<button type="button" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white" onclick={() => void addMembership()}>Add</button>
</div>
<table class="w-full text-left text-sm">
<thead>
<tr class="border-b border-slate-200 text-slate-500 dark:border-slate-700 dark:text-slate-300">
<th class="py-2">Group</th>
<th class="py-2">Status</th>
<th class="py-2">Actions</th>
</tr>
</thead>
<tbody>
{#if groups.length === 0}
<tr><td class="py-3 text-slate-500 dark:text-slate-400" colspan="3">No groups found</td></tr>
{:else}
{#each groups as group}
{@const membership = group.GroupId ? membershipsForUser[group.GroupId] : undefined}
<tr class="border-b border-slate-100 dark:border-slate-800">
<td class="py-3">{group.DisplayName}</td>
<td class="py-3">{membership ? 'Member' : 'Not a member'}</td>
<td class="py-3">
{#if membership?.MembershipId}
<button type="button" class="rounded border px-2 py-1 text-xs" onclick={() => void removeMembership(membership.MembershipId)}>Remove</button>
{:else}
<span class="text-xs text-slate-500 dark:text-slate-400">Add from selector above</span>
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

{#if showEditUserModal}
<div id="edit-user-modal" class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
<form
onsubmit={(event) => {
event.preventDefault();
void updateUserProfile();
}}
class="space-y-4"
>
<h2 class="text-xl font-semibold text-slate-900 dark:text-white">Edit User Profile</h2>
<input name="profile_display_name" bind:value={profileDisplayName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Display name" />
<input name="profile_email" bind:value={profileEmail} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Primary email" />
<input name="profile_phone" bind:value={profilePhone} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Primary phone" />
<input name="profile_address" bind:value={profileAddress} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Primary address" />
<div class="flex justify-end gap-3"><button type="button" onclick={() => (showEditUserModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Save</button></div>
</form>
</div>
</div>
{/if}
