<script lang="ts">
import { onMount } from 'svelte';
import { confirmDestructive } from '$lib/confirm-dialog';
import { getIAMClient } from '$lib/aws-client';
import {
ListUsersCommand, ListRolesCommand, ListGroupsCommand, ListPoliciesCommand,
CreateUserCommand, DeleteUserCommand,
CreateRoleCommand, DeleteRoleCommand,
CreateGroupCommand, DeleteGroupCommand,
CreatePolicyCommand, DeletePolicyCommand,
ListAttachedUserPoliciesCommand, ListAttachedRolePoliciesCommand,
ListAttachedGroupPoliciesCommand,
CreateAccessKeyCommand, DeleteAccessKeyCommand, ListAccessKeysCommand, UpdateAccessKeyCommand,
ListAccountAliasesCommand, GetAccountSummaryCommand,
ListAccessKeysCommand as ListAccessKeysCmd,
ListUserPoliciesCommand, GetUserPolicyCommand, PutUserPolicyCommand, DeleteUserPolicyCommand,
ListGroupsForUserCommand, AddUserToGroupCommand, RemoveUserFromGroupCommand,
GetLoginProfileCommand, CreateLoginProfileCommand, UpdateLoginProfileCommand, DeleteLoginProfileCommand,
ListMFADevicesCommand, DeactivateMFADeviceCommand,
type IAMClient,
type User, type Role, type Group, type Policy as ManagedPolicy, type AccessKeyMetadata, type MFADevice
} from '@aws-sdk/client-iam';
import { toast } from 'svelte-sonner';
import {
Users, Shield, RefreshCw, Search, UserCircle, ChevronRight, FileText,
Copy, Plus, Trash2, Key, BarChart3, BookOpen, X, Eye, EyeOff
} from 'lucide-svelte';

let iamClient: IAMClient | undefined;
function iam(): IAMClient {
	return (iamClient ??= getIAMClient());
}

// ─── Types ───
type MainTab = 'users' | 'roles' | 'groups' | 'policies' | 'metrics' | 'docs';
type PolicyScope = 'Local' | 'AWS' | 'All';

interface AccountSummary {
Users?: number; Groups?: number; Roles?: number; Policies?: number;
InstanceProfiles?: number; AccessKeysPerUser?: number; ActiveAccessKeys?: number;
AttachedPolicies?: number; MFADevices?: number; SAMLProviders?: number;
OIDCProviders?: number; AccountAliases?: number;
}

// ─── State ───
let tab = $state<MainTab>('users');
let users = $state<User[]>([]);
let roles = $state<Role[]>([]);
let groups = $state<Group[]>([]);
let policies = $state<ManagedPolicy[]>([]);
let loading = $state(true);
let search = $state('');
let selectedUser = $state<User | null>(null);
let selectedRole = $state<Role | null>(null);
let selectedGroup = $state<Group | null>(null);
let selectedPolicy = $state<ManagedPolicy | null>(null);
let policyScope = $state<PolicyScope>('Local');
let accountAlias = $state('');
let summary = $state<AccountSummary>({});

// Detail state
let userAttachedPolicies = $state<{PolicyName?: string; PolicyArn?: string}[]>([]);
let roleAttachedPolicies = $state<{PolicyName?: string; PolicyArn?: string}[]>([]);
let groupAttachedPolicies = $state<{PolicyName?: string; PolicyArn?: string}[]>([]);
let userAccessKeys = $state<AccessKeyMetadata[]>([]);
let detailLoading = $state(false);
// Inline policies + group membership for user detail
let userInlinePolicies = $state<string[]>([]);
let userGroupMemberships = $state<string[]>([]);
let inlinePolicyName = $state('');
let inlinePolicyDoc = $state('');
let editingInlinePolicy = $state<string | null>(null);
let savingInlinePolicy = $state(false);
let addToGroupName = $state('');
// Login profile + MFA
let userHasLoginProfile = $state(false);
let userMfaDevices = $state<MFADevice[]>([]);
let showLoginProfileModal = $state(false);
let loginProfilePassword = $state('');
let loginProfileResetRequired = $state(true);
let savingLoginProfile = $state(false);

// Create modals
let showCreateUser = $state(false);
let newUserName = $state('');
let newUserPath = $state('/');
let creating = $state(false);

let showCreateRole = $state(false);
let newRoleName = $state('');
let newRolePath = $state('/');
let newRoleTrustPolicy = $state('{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Principal": {"Service": "ec2.amazonaws.com"},\n      "Action": "sts:AssumeRole"\n    }\n  ]\n}');

let showCreateGroup = $state(false);
let newGroupName = $state('');
let newGroupPath = $state('/');

let showCreatePolicy = $state(false);
let newPolicyName = $state('');
let newPolicyPath = $state('/');
let newPolicyDoc = $state('{\n  "Version": "2012-10-17",\n  "Statement": [\n    {\n      "Effect": "Allow",\n      "Action": "*",\n      "Resource": "*"\n    }\n  ]\n}');

// Access key create
let showCreateKey = $state(false);
let createdKey = $state<{AccessKeyId?: string; SecretAccessKey?: string} | null>(null);
let showSecret = $state(false);

onMount(async () => {
await Promise.all([loadUsers(), loadAccountAlias(), loadSummary()]);
});

async function loadUsers() {
try {
loading = true;
const data = await iam().send(new ListUsersCommand({}));
users = data.Users || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load users');
} finally {
loading = false;
}
}

async function loadRoles() {
try {
loading = true;
const data = await iam().send(new ListRolesCommand({}));
roles = data.Roles || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load roles');
} finally {
loading = false;
}
}

async function loadGroups() {
try {
loading = true;
const data = await iam().send(new ListGroupsCommand({}));
groups = data.Groups || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load groups');
} finally {
loading = false;
}
}

async function loadPolicies() {
try {
loading = true;
const data = await iam().send(new ListPoliciesCommand({ Scope: policyScope }));
policies = data.Policies || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load policies');
} finally {
loading = false;
}
}

async function loadAccountAlias() {
try {
const data = await iam().send(new ListAccountAliasesCommand({}));
accountAlias = (data.AccountAliases || [])[0] || '';
} catch {
		// non-critical
		}
}

async function loadSummary() {
try {
const data = await iam().send(new GetAccountSummaryCommand({}));
summary = (data.SummaryMap as AccountSummary) || {};
} catch {
		// non-critical
		}
}

async function loadUserDetail(user: User) {
if (!user.UserName) return;
detailLoading = true;
inlinePolicyName = '';
inlinePolicyDoc = '';
editingInlinePolicy = null;
addToGroupName = '';
userHasLoginProfile = false;
userMfaDevices = [];
try {
const [pol, keys, inline, grps, mfa] = await Promise.all([
iam().send(new ListAttachedUserPoliciesCommand({ UserName: user.UserName })),
iam().send(new ListAccessKeysCommand({ UserName: user.UserName })),
iam().send(new ListUserPoliciesCommand({ UserName: user.UserName })),
iam().send(new ListGroupsForUserCommand({ UserName: user.UserName })),
iam().send(new ListMFADevicesCommand({ UserName: user.UserName })),
]);
userAttachedPolicies = pol.AttachedPolicies || [];
userAccessKeys = keys.AccessKeyMetadata || [];
userInlinePolicies = inline.PolicyNames || [];
userGroupMemberships = (grps.Groups || []).map((g) => g.GroupName ?? '').filter(Boolean);
userMfaDevices = mfa.MFADevices || [];
// GetLoginProfile throws NoSuchEntity when the user has no console password.
try {
await iam().send(new GetLoginProfileCommand({ UserName: user.UserName }));
userHasLoginProfile = true;
} catch {
userHasLoginProfile = false;
}
} catch {
		// non-critical
		} finally {
detailLoading = false;
}
}

async function editInlinePolicy(name: string) {
if (!selectedUser?.UserName) return;
try {
const res = await iam().send(new GetUserPolicyCommand({ UserName: selectedUser.UserName, PolicyName: name }));
const doc = res.PolicyDocument ? decodeURIComponent(res.PolicyDocument) : '';
inlinePolicyName = name;
editingInlinePolicy = name;
try {
inlinePolicyDoc = JSON.stringify(JSON.parse(doc), null, 2);
} catch {
inlinePolicyDoc = doc;
}
} catch (e) {
toast.error(`Failed to load inline policy: ${e instanceof Error ? e.message : String(e)}`);
}
}

async function saveInlinePolicy() {
if (!selectedUser?.UserName || !inlinePolicyName.trim()) {
toast.error('Policy name is required');
return;
}
let doc = inlinePolicyDoc;
try {
doc = JSON.stringify(JSON.parse(inlinePolicyDoc));
} catch {
toast.error('Policy document is not valid JSON');
return;
}
savingInlinePolicy = true;
try {
await iam().send(new PutUserPolicyCommand({ UserName: selectedUser.UserName, PolicyName: inlinePolicyName.trim(), PolicyDocument: doc }));
toast.success(`Inline policy "${inlinePolicyName.trim()}" saved`);
inlinePolicyName = '';
inlinePolicyDoc = '';
editingInlinePolicy = null;
await loadUserDetail(selectedUser);
} catch (e) {
toast.error(`Failed to save inline policy: ${e instanceof Error ? e.message : String(e)}`);
} finally {
savingInlinePolicy = false;
}
}

async function deleteInlinePolicy(name: string) {
if (!selectedUser?.UserName) return;
try {
await iam().send(new DeleteUserPolicyCommand({ UserName: selectedUser.UserName, PolicyName: name }));
toast.success('Inline policy deleted');
await loadUserDetail(selectedUser);
} catch (e) {
toast.error(`Failed to delete inline policy: ${e instanceof Error ? e.message : String(e)}`);
}
}

// ─── Login profile (console password) ───
function openLoginProfileModal() {
loginProfilePassword = '';
loginProfileResetRequired = true;
showLoginProfileModal = true;
}

async function saveLoginProfile() {
if (!selectedUser?.UserName || !loginProfilePassword.trim()) return;
savingLoginProfile = true;
try {
if (userHasLoginProfile) {
await iam().send(new UpdateLoginProfileCommand({
UserName: selectedUser.UserName,
Password: loginProfilePassword,
PasswordResetRequired: loginProfileResetRequired
}));
toast.success('Console password updated');
} else {
await iam().send(new CreateLoginProfileCommand({
UserName: selectedUser.UserName,
Password: loginProfilePassword,
PasswordResetRequired: loginProfileResetRequired
}));
toast.success('Console password created');
}
showLoginProfileModal = false;
await loadUserDetail(selectedUser);
} catch (e) {
toast.error(`Failed to set console password: ${e instanceof Error ? e.message : String(e)}`);
} finally {
savingLoginProfile = false;
}
}

async function deleteLoginProfile() {
if (!selectedUser?.UserName) return;
if (!(await confirmDestructive({ title: 'Remove Console Access', message: 'Remove console access (delete login profile) for this user?' }))) return;
try {
await iam().send(new DeleteLoginProfileCommand({ UserName: selectedUser.UserName }));
toast.success('Login profile deleted');
await loadUserDetail(selectedUser);
} catch (e) {
toast.error(`Failed to delete login profile: ${e instanceof Error ? e.message : String(e)}`);
}
}

async function deactivateMfa(serialNumber: string) {
if (!selectedUser?.UserName) return;
if (!(await confirmDestructive({ title: 'Deactivate MFA', message: `Deactivate MFA device ${serialNumber}?` }))) return;
try {
await iam().send(new DeactivateMFADeviceCommand({ UserName: selectedUser.UserName, SerialNumber: serialNumber }));
toast.success('MFA device deactivated');
await loadUserDetail(selectedUser);
} catch (e) {
toast.error(`Failed to deactivate MFA: ${e instanceof Error ? e.message : String(e)}`);
}
}

async function addUserToGroup() {
if (!selectedUser?.UserName || !addToGroupName.trim()) {
toast.error('Group name is required');
return;
}
try {
await iam().send(new AddUserToGroupCommand({ UserName: selectedUser.UserName, GroupName: addToGroupName.trim() }));
toast.success(`Added to group "${addToGroupName.trim()}"`);
addToGroupName = '';
await loadUserDetail(selectedUser);
} catch (e) {
toast.error(`Failed to add to group: ${e instanceof Error ? e.message : String(e)}`);
}
}

async function removeUserFromGroup(groupName: string) {
if (!selectedUser?.UserName) return;
try {
await iam().send(new RemoveUserFromGroupCommand({ UserName: selectedUser.UserName, GroupName: groupName }));
toast.success(`Removed from group "${groupName}"`);
await loadUserDetail(selectedUser);
} catch (e) {
toast.error(`Failed to remove from group: ${e instanceof Error ? e.message : String(e)}`);
}
}

async function loadRoleDetail(role: Role) {
if (!role.RoleName) return;
detailLoading = true;
try {
const pol = await iam().send(new ListAttachedRolePoliciesCommand({ RoleName: role.RoleName }));
roleAttachedPolicies = pol.AttachedPolicies || [];
} catch {
		// non-critical
		} finally {
detailLoading = false;
}
}

async function loadGroupDetail(group: Group) {
if (!group.GroupName) return;
detailLoading = true;
try {
const pol = await iam().send(new ListAttachedGroupPoliciesCommand({ GroupName: group.GroupName }));
groupAttachedPolicies = pol.AttachedPolicies || [];
} catch {
		// non-critical
		} finally {
detailLoading = false;
}
}

async function selectTab(t: MainTab) {
tab = t;
search = '';
selectedUser = null; selectedRole = null; selectedGroup = null; selectedPolicy = null;
userAttachedPolicies = []; roleAttachedPolicies = []; groupAttachedPolicies = []; userAccessKeys = [];
if (t === 'users' && users.length === 0) await loadUsers();
else if (t === 'roles' && roles.length === 0) await loadRoles();
else if (t === 'groups' && groups.length === 0) await loadGroups();
else if (t === 'policies') await loadPolicies();
else if (t === 'metrics') await loadSummary();
}

async function refresh() {
selectedUser = null; selectedRole = null; selectedGroup = null; selectedPolicy = null;
userAttachedPolicies = []; roleAttachedPolicies = []; groupAttachedPolicies = []; userAccessKeys = [];
if (tab === 'users') { users = []; await loadUsers(); }
else if (tab === 'roles') { roles = []; await loadRoles(); }
else if (tab === 'groups') { groups = []; await loadGroups(); }
else if (tab === 'policies') { policies = []; await loadPolicies(); }
else if (tab === 'metrics') await loadSummary();
await loadAccountAlias();
}

// ─── Create/Delete actions ───

async function createUser() {
if (!newUserName.trim()) { toast.error('User name is required'); return; }
creating = true;
try {
await iam().send(new CreateUserCommand({ UserName: newUserName.trim(), Path: newUserPath || '/' }));
toast.success(`User "${newUserName}" created`);
showCreateUser = false; newUserName = ''; newUserPath = '/';
users = []; await loadUsers();
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to create user');
} finally {
creating = false;
}
}

async function deleteUser(user: User) {
if (!user.UserName) return;
try {
await iam().send(new DeleteUserCommand({ UserName: user.UserName }));
toast.success(`User "${user.UserName}" deleted`);
if (selectedUser?.UserName === user.UserName) { selectedUser = null; userAttachedPolicies = []; userAccessKeys = []; }
users = users.filter(u => u.UserName !== user.UserName);
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete user');
}
}

async function createRole() {
if (!newRoleName.trim()) { toast.error('Role name is required'); return; }
creating = true;
try {
await iam().send(new CreateRoleCommand({ RoleName: newRoleName.trim(), Path: newRolePath || '/', AssumeRolePolicyDocument: newRoleTrustPolicy }));
toast.success(`Role "${newRoleName}" created`);
showCreateRole = false; newRoleName = ''; newRolePath = '/';
roles = []; await loadRoles();
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to create role');
} finally {
creating = false;
}
}

async function deleteRole(role: Role) {
if (!role.RoleName) return;
try {
await iam().send(new DeleteRoleCommand({ RoleName: role.RoleName }));
toast.success(`Role "${role.RoleName}" deleted`);
if (selectedRole?.RoleName === role.RoleName) { selectedRole = null; roleAttachedPolicies = []; }
roles = roles.filter(r => r.RoleName !== role.RoleName);
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete role');
}
}

async function createGroup() {
if (!newGroupName.trim()) { toast.error('Group name is required'); return; }
creating = true;
try {
await iam().send(new CreateGroupCommand({ GroupName: newGroupName.trim(), Path: newGroupPath || '/' }));
toast.success(`Group "${newGroupName}" created`);
showCreateGroup = false; newGroupName = ''; newGroupPath = '/';
groups = []; await loadGroups();
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to create group');
} finally {
creating = false;
}
}

async function deleteGroup(group: Group) {
if (!group.GroupName) return;
try {
await iam().send(new DeleteGroupCommand({ GroupName: group.GroupName }));
toast.success(`Group "${group.GroupName}" deleted`);
if (selectedGroup?.GroupName === group.GroupName) { selectedGroup = null; groupAttachedPolicies = []; }
groups = groups.filter(g => g.GroupName !== group.GroupName);
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete group');
}
}

async function createPolicy() {
if (!newPolicyName.trim()) { toast.error('Policy name is required'); return; }
creating = true;
try {
await iam().send(new CreatePolicyCommand({ PolicyName: newPolicyName.trim(), Path: newPolicyPath || '/', PolicyDocument: newPolicyDoc }));
toast.success(`Policy "${newPolicyName}" created`);
showCreatePolicy = false; newPolicyName = ''; newPolicyPath = '/';
policies = []; await loadPolicies();
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to create policy');
} finally {
creating = false;
}
}

async function deletePolicy(policy: ManagedPolicy) {
if (!policy.Arn) return;
try {
await iam().send(new DeletePolicyCommand({ PolicyArn: policy.Arn }));
toast.success(`Policy "${policy.PolicyName}" deleted`);
if (selectedPolicy?.Arn === policy.Arn) selectedPolicy = null;
policies = policies.filter(p => p.Arn !== policy.Arn);
await loadSummary();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete policy');
}
}

// ─── Access Keys ───

async function createAccessKey(user: User) {
if (!user.UserName) return;
try {
const resp = await iam().send(new CreateAccessKeyCommand({ UserName: user.UserName }));
createdKey = resp.AccessKey ?? null;
showCreateKey = true;
showSecret = true;
// refresh keys list
const keys = await iam().send(new ListAccessKeysCmd({ UserName: user.UserName }));
userAccessKeys = keys.AccessKeyMetadata || [];
toast.success('Access key created');
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to create access key');
}
}

async function deleteAccessKey(user: User, keyId: string) {
if (!user.UserName) return;
try {
await iam().send(new DeleteAccessKeyCommand({ UserName: user.UserName, AccessKeyId: keyId }));
userAccessKeys = userAccessKeys.filter(k => k.AccessKeyId !== keyId);
toast.success('Access key deleted');
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete access key');
}
}

async function toggleAccessKey(user: User, key: AccessKeyMetadata) {
if (!user.UserName || !key.AccessKeyId) return;
const newStatus = key.Status === 'Active' ? 'Inactive' : 'Active';
try {
await iam().send(new UpdateAccessKeyCommand({ UserName: user.UserName, AccessKeyId: key.AccessKeyId, Status: newStatus }));
userAccessKeys = userAccessKeys.map(k => k.AccessKeyId === key.AccessKeyId ? { ...k, Status: newStatus } : k);
toast.success(`Access key ${newStatus.toLowerCase()}`);
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to update access key');
}
}

async function copyToClipboard(text: string, label = 'Copied') {
await navigator.clipboard.writeText(text);
toast.success(label);
}

// ─── Derived ───
let filteredUsers = $derived(users.filter(u => !search || u.UserName?.toLowerCase().includes(search.toLowerCase())));
let filteredRoles = $derived(roles.filter(r => !search || r.RoleName?.toLowerCase().includes(search.toLowerCase())));
let filteredGroups = $derived(groups.filter(g => !search || g.GroupName?.toLowerCase().includes(search.toLowerCase())));
let filteredPolicies = $derived(policies.filter(p => !search || p.PolicyName?.toLowerCase().includes(search.toLowerCase())));

function fmt(d?: Date): string { return d ? new Date(d).toLocaleDateString() : '—'; }

// ─── Docs ───
const iam_docs = [
{ op: 'CreateUser / DeleteUser / GetUser / ListUsers / UpdateUser', desc: 'Manage IAM users. ListUsers supports PathPrefix filtering and Marker/MaxItems pagination.' },
{ op: 'CreateRole / DeleteRole / GetRole / ListRoles / UpdateRole', desc: 'Manage IAM roles. ListRoles supports PathPrefix filtering. CreateRole accepts AssumeRolePolicyDocument.' },
{ op: 'CreateGroup / DeleteGroup / GetGroup / ListGroups / UpdateGroup', desc: 'Manage IAM groups. AddUserToGroup / RemoveUserFromGroup manage membership.' },
{ op: 'CreatePolicy / DeletePolicy / GetPolicy / ListPolicies', desc: 'Manage IAM policies. ListPolicies supports Scope (Local/AWS/All) and PathPrefix filtering.' },
{ op: 'CreatePolicyVersion / SetDefaultPolicyVersion / DeletePolicyVersion / ListPolicyVersions / GetPolicyVersion', desc: 'Manage policy versions. Each policy can have up to 5 versions.' },
{ op: 'AttachUserPolicy / DetachUserPolicy / ListAttachedUserPolicies', desc: 'Attach or detach managed policies to/from users.' },
{ op: 'AttachRolePolicy / DetachRolePolicy / ListAttachedRolePolicies', desc: 'Attach or detach managed policies to/from roles.' },
{ op: 'AttachGroupPolicy / DetachGroupPolicy / ListAttachedGroupPolicies', desc: 'Attach or detach managed policies to/from groups.' },
{ op: 'PutUserPolicy / GetUserPolicy / DeleteUserPolicy / ListUserPolicies', desc: 'Manage inline policies for users.' },
{ op: 'PutRolePolicy / GetRolePolicy / DeleteRolePolicy / ListRolePolicies', desc: 'Manage inline policies for roles.' },
{ op: 'PutGroupPolicy / GetGroupPolicy / DeleteGroupPolicy / ListGroupPolicies', desc: 'Manage inline policies for groups.' },
{ op: 'CreateAccessKey / DeleteAccessKey / ListAccessKeys / UpdateAccessKey / GetAccessKeyLastUsed', desc: 'Manage programmatic access keys. UpdateAccessKey changes status (Active/Inactive).' },
{ op: 'CreateLoginProfile / UpdateLoginProfile / DeleteLoginProfile / GetLoginProfile', desc: 'Manage console login passwords for IAM users.' },
{ op: 'CreateInstanceProfile / DeleteInstanceProfile / GetInstanceProfile / ListInstanceProfiles', desc: 'Manage EC2 instance profiles. AddRoleToInstanceProfile / RemoveRoleFromInstanceProfile manage role association.' },
{ op: 'CreateSAMLProvider / UpdateSAMLProvider / DeleteSAMLProvider / GetSAMLProvider / ListSAMLProviders', desc: 'Manage SAML 2.0 identity providers for federated access.' },
{ op: 'CreateOpenIDConnectProvider / DeleteOpenIDConnectProvider / GetOpenIDConnectProvider / ListOpenIDConnectProviders', desc: 'Manage OIDC identity providers. Supports thumbprint and client ID management.' },
{ op: 'PutUserPermissionsBoundary / DeleteUserPermissionsBoundary / GetUserPermissionsBoundary', desc: 'Manage permissions boundaries that cap the maximum permissions for an IAM user.' },
{ op: 'PutRolePermissionsBoundary / DeleteRolePermissionsBoundary / GetRolePermissionsBoundary', desc: 'Manage permissions boundaries that cap the maximum permissions for an IAM role.' },
{ op: 'CreateVirtualMFADevice / DeleteVirtualMFADevice / ListVirtualMFADevices / GetMFADevice', desc: 'Manage virtual MFA devices for multi-factor authentication.' },
{ op: 'TagUser / UntagUser / ListUserTags / TagRole / UntagRole / ListRoleTags', desc: 'Manage tags on IAM users and roles.' },
{ op: 'TagGroup / UntagGroup / ListGroupTags / TagPolicy / UntagPolicy / ListPolicyTags', desc: 'Manage tags on IAM groups and policies.' },
{ op: 'GetAccountSummary', desc: 'Returns a summary of entity counts and quotas for the current IAM account.' },
{ op: 'GetAccountPasswordPolicy / UpdateAccountPasswordPolicy / DeleteAccountPasswordPolicy', desc: 'Manage the IAM account password policy for IAM users.' },
{ op: 'ListAccountAliases / CreateAccountAlias / DeleteAccountAlias', desc: 'Manage AWS account aliases used instead of account ID in sign-in URLs.' },
{ op: 'GenerateCredentialReport / GetCredentialReport', desc: 'Generate and download a CSV credential report including password/key status for all users.' },
{ op: 'GetAccountAuthorizationDetails', desc: 'Returns all user, role, group and policy details for the entire account in a single call.' },
{ op: 'SimulatePrincipalPolicy / SimulateCustomPolicy', desc: 'Simulate whether policies would allow or deny actions. Useful for policy testing.' },
{ op: 'UpdateAssumeRolePolicy', desc: 'Update the trust relationship (assume-role policy) for an IAM role.' },
{ op: 'CreateServiceLinkedRole / GetServiceLinkedRoleDeletionStatus', desc: 'Manage service-linked roles for AWS services.' },
{ op: 'CreateServiceSpecificCredential / UpdateServiceSpecificCredential / DeleteServiceSpecificCredential / ListServiceSpecificCredentials', desc: 'Manage credentials for specific AWS services like CodeCommit.' },
{ op: 'GetContextKeysForCustomPolicy / GetContextKeysForPrincipalPolicy', desc: 'Retrieve context keys referenced in an IAM policy document.' },
{ op: 'ListEntitiesForPolicy', desc: 'List all entities (users, roles, groups) that a managed policy is attached to.' },
{ op: 'ChangePassword', desc: 'Change the password for the current IAM user.' },
];
</script>

<!-- ─── Page ─── -->
<div class="space-y-6">
<!-- Header -->
<div class="flex items-center justify-between flex-wrap gap-3">
<div class="flex items-center gap-3">
<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
<Shield class="w-6 h-6 text-orange-600 dark:text-orange-400" />
</div>
<div>
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">IAM</h1>
<p class="text-slate-500 dark:text-slate-400 text-sm">Identity and Access Management{accountAlias ? ` · ${accountAlias}` : ''}</p>
</div>
</div>
<button onclick={refresh} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-2">
<RefreshCw class="w-4 h-4" />
<span class="hidden sm:inline text-sm">Refresh</span>
</button>
</div>

<!-- Stats cards -->
<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
{#each [
{ icon: UserCircle, color: 'text-blue-500', count: users.length, label: 'Users' },
{ icon: Shield, color: 'text-purple-500', count: roles.length, label: 'Roles' },
{ icon: Users, color: 'text-green-500', count: groups.length, label: 'Groups' },
{ icon: FileText, color: 'text-amber-500', count: policies.length, label: 'Policies' },
] as card}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
<div class="flex items-center gap-3">
<card.icon class="w-5 h-5 {card.color}" />
<div>
<p class="text-2xl font-bold text-slate-900 dark:text-white">{card.count}</p>
<p class="text-xs text-slate-500 dark:text-slate-400">{card.label}</p>
</div>
</div>
</div>
{/each}
</div>

<!-- Tabs -->
<div class="flex flex-col sm:flex-row gap-3">
<div class="flex gap-1 p-1 bg-slate-100 dark:bg-slate-800 rounded-lg flex-wrap">
{#each ([
['users','Users'], ['roles','Roles'], ['groups','Groups'],
['policies','Policies'], ['metrics','Metrics'], ['docs','Docs']
] as [MainTab, string][]) as [t, label]}
<button id="{t}-tab" onclick={() => selectTab(t)}
class="px-3 py-2 rounded-md text-sm font-medium transition-colors {tab === t
? 'bg-white dark:bg-slate-700 text-slate-900 dark:text-white shadow'
: 'text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}">
{label}
{#if t === 'users'}<span class="ml-1 text-xs opacity-60">({users.length})</span>
{:else if t === 'roles'}<span class="ml-1 text-xs opacity-60">({roles.length})</span>
{:else if t === 'groups'}<span class="ml-1 text-xs opacity-60">({groups.length})</span>
{:else if t === 'policies'}<span class="ml-1 text-xs opacity-60">({policies.length})</span>
{/if}
</button>
{/each}
</div>

{#if tab !== 'metrics' && tab !== 'docs'}
<div class="relative flex-1">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search {tab}..." bind:value={search}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
</div>
{#if tab === 'policies'}
<select bind:value={policyScope} onchange={() => loadPolicies()}
class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
<option value="Local">Customer Managed</option>
<option value="AWS">AWS Managed</option>
<option value="All">All Policies</option>
</select>
{/if}
{#if tab === 'users'}
<button onclick={() => { showCreateUser = true; }} class="px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm flex items-center gap-2">
<Plus class="w-4 h-4" /><span class="hidden sm:inline">Create User</span>
</button>
{:else if tab === 'roles'}
<button onclick={() => { showCreateRole = true; }} class="px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm flex items-center gap-2">
<Plus class="w-4 h-4" /><span class="hidden sm:inline">Create Role</span>
</button>
{:else if tab === 'groups'}
<button onclick={() => { showCreateGroup = true; }} class="px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm flex items-center gap-2">
<Plus class="w-4 h-4" /><span class="hidden sm:inline">Create Group</span>
</button>
{:else if tab === 'policies'}
<button onclick={() => { showCreatePolicy = true; }} class="px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm flex items-center gap-2">
<Plus class="w-4 h-4" /><span class="hidden sm:inline">Create Policy</span>
</button>
{/if}
{/if}
</div>

<!-- ─── Metrics Tab ─── -->
{#if tab === 'metrics'}
<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-4 gap-4">
{#each [
{ label: 'Users', value: summary['Users'] ?? users.length },
{ label: 'Roles', value: summary['Roles'] ?? roles.length },
{ label: 'Groups', value: summary['Groups'] ?? groups.length },
{ label: 'Policies', value: summary['Policies'] ?? policies.length },
{ label: 'Instance Profiles', value: summary['InstanceProfiles'] ?? 0 },
{ label: 'Access Keys (total)', value: summary['AccessKeysPerUser'] ?? 0 },
{ label: 'Access Keys (active)', value: summary['ActiveAccessKeys'] ?? 0 },
{ label: 'Attached Policies', value: summary['AttachedPolicies'] ?? 0 },
{ label: 'MFA Devices', value: summary['MFADevices'] ?? 0 },
{ label: 'SAML Providers', value: summary['SAMLProviders'] ?? 0 },
{ label: 'OIDC Providers', value: summary['OIDCProviders'] ?? 0 },
{ label: 'Account Aliases', value: summary['AccountAliases'] ?? (accountAlias ? 1 : 0) },
] as m}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
<div class="flex items-center gap-2 mb-1">
<BarChart3 class="w-4 h-4 text-indigo-500" />
<p class="text-xs text-slate-500 dark:text-slate-400">{m.label}</p>
</div>
<p class="text-2xl font-bold text-slate-900 dark:text-white">{m.value}</p>
</div>
{/each}
</div>

<!-- ─── Docs Tab ─── -->
{:else if tab === 'docs'}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center gap-2">
<BookOpen class="w-4 h-4 text-indigo-500" />
<span class="font-semibold text-slate-800 dark:text-white text-sm">Supported IAM Operations</span>
</div>
<div class="divide-y divide-slate-100 dark:divide-slate-700">
{#each iam_docs as doc}
<div class="px-4 py-3">
<p class="font-mono text-xs text-indigo-700 dark:text-indigo-300 mb-1">{doc.op}</p>
<p class="text-sm text-slate-600 dark:text-slate-300">{doc.desc}</p>
</div>
{/each}
</div>
</div>

<!-- ─── Policies Tab ─── -->
{:else if tab === 'policies'}
{#if loading}
<div class="text-center py-12 text-slate-500">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
<p>Loading policies...</p>
</div>
{:else if filteredPolicies.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
<FileText class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p id="empty-state-text" class="text-slate-500 dark:text-slate-400 font-medium">No policies found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 text-xs text-slate-500 dark:text-slate-400">
{filteredPolicies.length} of {policies.length} policies
</div>
<div class="divide-y divide-slate-100 dark:divide-slate-700 max-h-[500px] overflow-y-auto">
{#each filteredPolicies as policy}
<div class="px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-700/50 flex items-center justify-between gap-2">
<div class="min-w-0">
<p class="font-medium text-slate-900 dark:text-white text-sm truncate">{policy.PolicyName}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 truncate font-mono">{policy.Arn}</p>
{#if policy.Description}
<p class="text-xs text-slate-400 mt-0.5 truncate">{policy.Description}</p>
{/if}
</div>
<div class="flex items-center gap-2 shrink-0">
<span class="text-xs px-2 py-0.5 bg-slate-100 dark:bg-slate-700 rounded text-slate-600 dark:text-slate-300">{policy.AttachmentCount ?? 0} attached</span>
<button onclick={() => copyToClipboard(policy.Arn ?? '', 'ARN copied')} class="p-1 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">
<Copy class="w-3.5 h-3.5" />
</button>
{#if policyScope === 'Local'}
<button onclick={() => deletePolicy(policy)} class="p-1 text-red-400 hover:text-red-600">
<Trash2 class="w-3.5 h-3.5" />
</button>
{/if}
</div>
</div>
{/each}
</div>
</div>
{/if}

<!-- ─── Users / Roles / Groups Tabs ─── -->
{:else}
{@const items = tab === 'users' ? filteredUsers : tab === 'roles' ? filteredRoles : filteredGroups}
{@const totalCount = tab === 'users' ? users.length : tab === 'roles' ? roles.length : groups.length}
{@const itemName = (x: User | Role | Group) =>
'UserName' in x ? (x.UserName ?? '') :
'RoleName' in x ? (x.RoleName ?? '') :
'GroupName' in x ? (x.GroupName ?? '') : ''}
{@const itemArn = (x: User | Role | Group) => x.Arn ?? ''}
{@const isSelected = (x: User | Role | Group) =>
tab === 'users' ? selectedUser === x :
tab === 'roles' ? selectedRole === x :
selectedGroup === x}
<div class="flex gap-4">
<!-- List -->
<div class="flex-1 min-w-0">
{#if loading}
<div class="text-center py-12 text-slate-500">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
<p>Loading {tab}...</p>
</div>
{:else if items.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
<Users class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
<p id="empty-state-text" class="text-slate-500 dark:text-slate-400 font-medium">No {tab} found</p>
{#if search}<p class="text-xs text-slate-400 mt-1">Try clearing the search filter</p>{/if}
</div>
{:else}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 text-xs text-slate-500 dark:text-slate-400">
{items.length} of {totalCount} {tab}
</div>
<div class="divide-y divide-slate-200 dark:divide-slate-700 max-h-[500px] overflow-y-auto">
{#each items as item}
<div class="px-4 py-3 hover:bg-slate-50 dark:hover:bg-slate-700/50 flex items-center justify-between gap-2 cursor-pointer {isSelected(item) ? 'bg-indigo-50 dark:bg-indigo-900/20' : ''}"
role="button"
tabindex="0"
onclick={() => {
if (tab === 'users') {
const u = item as User;
if (selectedUser === u) { selectedUser = null; } else { selectedUser = u; loadUserDetail(u); }
selectedRole = null; selectedGroup = null;
} else if (tab === 'roles') {
const r = item as Role;
if (selectedRole === r) { selectedRole = null; } else { selectedRole = r; loadRoleDetail(r); }
selectedUser = null; selectedGroup = null;
} else {
const g = item as Group;
if (selectedGroup === g) { selectedGroup = null; } else { selectedGroup = g; loadGroupDetail(g); }
selectedUser = null; selectedRole = null;
}
}}
onkeydown={(e) => { if (e.key === 'Enter' || e.key === ' ') e.currentTarget.click(); }}>
<div class="min-w-0 flex-1">
<p class="font-medium text-slate-900 dark:text-white text-sm truncate">{itemName(item)}</p>
<p class="text-xs text-slate-500 dark:text-slate-400 truncate font-mono">{itemArn(item)}</p>
</div>
<div class="flex items-center gap-1 shrink-0">
<button onclick={(e) => { e.stopPropagation(); copyToClipboard(itemArn(item), 'ARN copied'); }} class="p-1 text-slate-400 hover:text-slate-600 dark:hover:text-slate-200">
<Copy class="w-3.5 h-3.5" />
</button>
<button onclick={(e) => {
e.stopPropagation();
if (tab === 'users') deleteUser(item as User);
else if (tab === 'roles') deleteRole(item as Role);
else deleteGroup(item as Group);
}} class="p-1 text-red-400 hover:text-red-600">
<Trash2 class="w-3.5 h-3.5" />
</button>
<ChevronRight class="w-4 h-4 text-slate-400 {isSelected(item) ? 'rotate-90' : ''} transition-transform" />
</div>
</div>
{/each}
</div>
</div>
{/if}
</div>

<!-- Detail panel -->
{#if selectedUser && tab === 'users'}
<div class="w-80 shrink-0">
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5 space-y-4 sticky top-4">
<div class="flex items-start justify-between">
<div>
<h3 class="font-bold text-slate-900 dark:text-white">{selectedUser.UserName}</h3>
<span class="inline-block mt-1 px-2 py-0.5 bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 text-xs rounded">User</span>
</div>
<button onclick={() => { selectedUser = null; }} class="text-slate-400 hover:text-slate-600 text-lg">&times;</button>
</div>
<div class="space-y-2 text-sm">
<div>
<p class="text-xs font-medium text-slate-500 dark:text-slate-400">ARN</p>
<div class="flex items-start gap-1 mt-0.5">
<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all flex-1">{selectedUser.Arn}</p>
<button onclick={() => copyToClipboard(selectedUser?.Arn ?? '', 'ARN copied')} class="shrink-0 p-0.5 text-slate-400 hover:text-slate-600"><Copy class="w-3.5 h-3.5" /></button>
</div>
</div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">ID</p><p class="text-xs font-mono text-slate-700 dark:text-slate-300 mt-0.5">{selectedUser.UserId}</p></div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">Path</p><p class="text-xs text-slate-700 dark:text-slate-300 mt-0.5">{selectedUser.Path ?? '/'}</p></div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">Created</p><p class="text-xs text-slate-700 dark:text-slate-300 mt-0.5">{fmt(selectedUser.CreateDate)}</p></div>
</div>

<!-- Attached Policies -->
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-2">Attached Policies</p>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading...</p>
{:else if userAttachedPolicies.length === 0}
<p class="text-xs text-slate-400">None</p>
{:else}
<ul class="space-y-1">
{#each userAttachedPolicies as p}
<li class="text-xs text-indigo-700 dark:text-indigo-300 font-mono truncate">{p.PolicyName}</li>
{/each}
</ul>
{/if}
</div>

<!-- Console Access & MFA -->
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-2">Console Access &amp; MFA</p>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading...</p>
{:else}
<div class="flex flex-wrap items-center gap-2 mb-2">
<span class="rounded px-2 py-0.5 text-xs {userHasLoginProfile ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-slate-100 text-slate-500 dark:bg-slate-700'}">
{userHasLoginProfile ? 'Console password set' : 'No console password'}
</span>
<button onclick={openLoginProfileModal} class="rounded border border-slate-300 px-2 py-1 text-xs hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700">
{userHasLoginProfile ? 'Reset password' : 'Create password'}
</button>
{#if userHasLoginProfile}
<button onclick={deleteLoginProfile} class="rounded border border-red-300 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-700 dark:text-red-400">
Remove access
</button>
{/if}
</div>
<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">MFA Devices</p>
{#if userMfaDevices.length === 0}
<p class="text-xs text-slate-400">No MFA devices</p>
{:else}
<ul class="space-y-1">
{#each userMfaDevices as dev}
<li class="flex items-center justify-between rounded bg-slate-50 px-2 py-1 text-xs dark:bg-slate-700/50">
<span class="font-mono truncate">{dev.SerialNumber}</span>
<button onclick={() => deactivateMfa(dev.SerialNumber ?? '')} class="text-red-500 hover:text-red-700">Deactivate</button>
</li>
{/each}
</ul>
{/if}
{/if}
</div>

<!-- Group Membership -->
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-2">Group Membership</p>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading...</p>
{:else}
{#if userGroupMemberships.length === 0}
<p class="text-xs text-slate-400">Not a member of any group</p>
{:else}
<ul class="space-y-1 mb-2">
{#each userGroupMemberships as g}
<li class="flex items-center justify-between gap-1">
<span class="text-xs font-mono text-slate-700 dark:text-slate-300 truncate">{g}</span>
<button onclick={() => removeUserFromGroup(g)} class="p-0.5 text-red-400 hover:text-red-600" title="Remove from group"><Trash2 class="w-3 h-3" /></button>
</li>
{/each}
</ul>
{/if}
<div class="flex items-center gap-1">
<input type="text" bind:value={addToGroupName} placeholder="group name" class="flex-1 min-w-0 px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white" />
<button onclick={addUserToGroup} class="px-2 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700">Add</button>
</div>
{/if}
</div>

<!-- Inline Policies -->
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-2">Inline Policies</p>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading...</p>
{:else}
{#if userInlinePolicies.length === 0}
<p class="text-xs text-slate-400 mb-2">None</p>
{:else}
<ul class="space-y-1 mb-2">
{#each userInlinePolicies as p}
<li class="flex items-center justify-between gap-1">
<span class="text-xs font-mono text-indigo-700 dark:text-indigo-300 truncate">{p}</span>
<div class="flex items-center gap-1">
<button onclick={() => editInlinePolicy(p)} class="p-0.5 text-slate-400 hover:text-slate-600" title="Edit"><FileText class="w-3 h-3" /></button>
<button onclick={() => deleteInlinePolicy(p)} class="p-0.5 text-red-400 hover:text-red-600" title="Delete"><Trash2 class="w-3 h-3" /></button>
</div>
</li>
{/each}
</ul>
{/if}
<div class="space-y-1.5">
<input type="text" bind:value={inlinePolicyName} disabled={editingInlinePolicy !== null} placeholder="policy name" class="w-full px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white disabled:opacity-60" />
<textarea bind:value={inlinePolicyDoc} rows="5" placeholder={'{\n  "Version": "2012-10-17",\n  "Statement": []\n}'} class="w-full px-2 py-1 text-xs font-mono border border-slate-200 dark:border-slate-600 rounded bg-white dark:bg-slate-700 text-slate-900 dark:text-white"></textarea>
<div class="flex gap-1">
<button disabled={savingInlinePolicy} onclick={saveInlinePolicy} class="flex-1 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-50">{savingInlinePolicy ? 'Saving...' : (editingInlinePolicy ? 'Update Policy' : 'Add Policy')}</button>
{#if editingInlinePolicy}
<button onclick={() => { editingInlinePolicy = null; inlinePolicyName = ''; inlinePolicyDoc = ''; }} class="px-2 py-1 text-xs border border-slate-200 dark:border-slate-600 rounded text-slate-600 dark:text-slate-400">Cancel</button>
{/if}
</div>
</div>
{/if}
</div>

<!-- Access Keys -->
<div>
<div class="flex items-center justify-between mb-2">
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide">Access Keys</p>
<button onclick={() => createAccessKey(selectedUser!)} class="text-xs text-indigo-600 hover:text-indigo-700 flex items-center gap-1">
<Key class="w-3 h-3" /> New
</button>
</div>
{#if userAccessKeys.length === 0}
<p class="text-xs text-slate-400">None</p>
{:else}
<ul class="space-y-2">
{#each userAccessKeys as key}
<li class="flex items-center justify-between gap-1">
<span class="font-mono text-xs text-slate-700 dark:text-slate-300 truncate">{key.AccessKeyId?.slice(0, 12)}…</span>
<div class="flex items-center gap-1">
<span class="text-xs px-1.5 py-0.5 rounded {key.Status === 'Active' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-500'}">{key.Status}</span>
<button onclick={() => toggleAccessKey(selectedUser!, key)} class="p-0.5 text-slate-400 hover:text-slate-600" title="Toggle status">
{#if key.Status === 'Active'}<EyeOff class="w-3 h-3" />{:else}<Eye class="w-3 h-3" />{/if}
</button>
<button onclick={() => deleteAccessKey(selectedUser!, key.AccessKeyId!)} class="p-0.5 text-red-400 hover:text-red-600">
<Trash2 class="w-3 h-3" />
</button>
</div>
</li>
{/each}
</ul>
{/if}
</div>

<button onclick={() => deleteUser(selectedUser!)} class="w-full py-2 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 rounded-lg text-sm hover:bg-red-100 dark:hover:bg-red-900/30 flex items-center justify-center gap-2">
<Trash2 class="w-4 h-4" /> Delete User
</button>
</div>
</div>

{:else if selectedRole && tab === 'roles'}
<div class="w-80 shrink-0">
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5 space-y-4 sticky top-4">
<div class="flex items-start justify-between">
<div>
<h3 class="font-bold text-slate-900 dark:text-white">{selectedRole.RoleName}</h3>
<span class="inline-block mt-1 px-2 py-0.5 bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300 text-xs rounded">Role</span>
</div>
<button onclick={() => { selectedRole = null; }} class="text-slate-400 hover:text-slate-600 text-lg">&times;</button>
</div>
<div class="space-y-2 text-sm">
<div>
<p class="text-xs font-medium text-slate-500 dark:text-slate-400">ARN</p>
<div class="flex items-start gap-1 mt-0.5">
<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all flex-1">{selectedRole.Arn}</p>
<button onclick={() => copyToClipboard(selectedRole?.Arn ?? '', 'ARN copied')} class="shrink-0 p-0.5 text-slate-400 hover:text-slate-600"><Copy class="w-3.5 h-3.5" /></button>
</div>
</div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">ID</p><p class="text-xs font-mono text-slate-700 dark:text-slate-300 mt-0.5">{selectedRole.RoleId}</p></div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">Path</p><p class="text-xs text-slate-700 dark:text-slate-300 mt-0.5">{selectedRole.Path ?? '/'}</p></div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">Created</p><p class="text-xs text-slate-700 dark:text-slate-300 mt-0.5">{fmt(selectedRole.CreateDate)}</p></div>
{#if selectedRole.Description}
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">Description</p><p class="text-xs text-slate-700 dark:text-slate-300 mt-0.5">{selectedRole.Description}</p></div>
{/if}
</div>

<!-- Trust Policy -->
{#if selectedRole.AssumeRolePolicyDocument}
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Trust Policy</p>
<pre class="text-xs bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded p-2 overflow-x-auto text-slate-700 dark:text-slate-300 max-h-40 overflow-y-auto">{JSON.stringify(JSON.parse(decodeURIComponent(selectedRole.AssumeRolePolicyDocument)), null, 2)}</pre>
</div>
{/if}

<!-- Attached Policies -->
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-2">Attached Policies</p>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading...</p>
{:else if roleAttachedPolicies.length === 0}
<p class="text-xs text-slate-400">None</p>
{:else}
<ul class="space-y-1">
{#each roleAttachedPolicies as p}
<li class="text-xs text-indigo-700 dark:text-indigo-300 font-mono truncate">{p.PolicyName}</li>
{/each}
</ul>
{/if}
</div>

<button onclick={() => deleteRole(selectedRole!)} class="w-full py-2 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 rounded-lg text-sm hover:bg-red-100 dark:hover:bg-red-900/30 flex items-center justify-center gap-2">
<Trash2 class="w-4 h-4" /> Delete Role
</button>
</div>
</div>

{:else if selectedGroup && tab === 'groups'}
<div class="w-80 shrink-0">
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5 space-y-4 sticky top-4">
<div class="flex items-start justify-between">
<div>
<h3 class="font-bold text-slate-900 dark:text-white">{selectedGroup.GroupName}</h3>
<span class="inline-block mt-1 px-2 py-0.5 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 text-xs rounded">Group</span>
</div>
<button onclick={() => { selectedGroup = null; }} class="text-slate-400 hover:text-slate-600 text-lg">&times;</button>
</div>
<div class="space-y-2 text-sm">
<div>
<p class="text-xs font-medium text-slate-500 dark:text-slate-400">ARN</p>
<div class="flex items-start gap-1 mt-0.5">
<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all flex-1">{selectedGroup.Arn}</p>
<button onclick={() => copyToClipboard(selectedGroup?.Arn ?? '', 'ARN copied')} class="shrink-0 p-0.5 text-slate-400 hover:text-slate-600"><Copy class="w-3.5 h-3.5" /></button>
</div>
</div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">ID</p><p class="text-xs font-mono text-slate-700 dark:text-slate-300 mt-0.5">{selectedGroup.GroupId}</p></div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">Path</p><p class="text-xs text-slate-700 dark:text-slate-300 mt-0.5">{selectedGroup.Path ?? '/'}</p></div>
<div><p class="text-xs font-medium text-slate-500 dark:text-slate-400">Created</p><p class="text-xs text-slate-700 dark:text-slate-300 mt-0.5">{fmt(selectedGroup.CreateDate)}</p></div>
</div>

<!-- Attached Policies -->
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-2">Attached Policies</p>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading...</p>
{:else if groupAttachedPolicies.length === 0}
<p class="text-xs text-slate-400">None</p>
{:else}
<ul class="space-y-1">
{#each groupAttachedPolicies as p}
<li class="text-xs text-indigo-700 dark:text-indigo-300 font-mono truncate">{p.PolicyName}</li>
{/each}
</ul>
{/if}
</div>

<button onclick={() => deleteGroup(selectedGroup!)} class="w-full py-2 bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 rounded-lg text-sm hover:bg-red-100 dark:hover:bg-red-900/30 flex items-center justify-center gap-2">
<Trash2 class="w-4 h-4" /> Delete Group
</button>
</div>
</div>
{/if}
</div>
{/if}
</div>

<!-- ─── Create User Modal ─── -->
{#if showCreateUser}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onclick={(e) => { if (e.target === e.currentTarget) showCreateUser = false; }}>
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-md p-6 space-y-4">
<div class="flex items-center justify-between">
<h2 class="text-lg font-bold text-slate-900 dark:text-white">Create IAM User</h2>
<button onclick={() => showCreateUser = false}><X class="w-5 h-5 text-slate-400" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">User Name *</label>
<input type="text" bind:value={newUserName} placeholder="e.g. alice" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Path</label>
<input type="text" bind:value={newUserPath} placeholder="/" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => showCreateUser = false} class="flex-1 py-2 border border-slate-300 dark:border-slate-600 rounded-lg text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700">Cancel</button>
<button onclick={createUser} disabled={creating} class="flex-1 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">{creating ? 'Creating…' : 'Create User'}</button>
</div>
</div>
</div>
{/if}

<!-- ─── Create Role Modal ─── -->
{#if showCreateRole}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onclick={(e) => { if (e.target === e.currentTarget) showCreateRole = false; }}>
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h2 class="text-lg font-bold text-slate-900 dark:text-white">Create IAM Role</h2>
<button onclick={() => showCreateRole = false}><X class="w-5 h-5 text-slate-400" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Role Name *</label>
<input type="text" bind:value={newRoleName} placeholder="e.g. EC2AdminRole" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Path</label>
<input type="text" bind:value={newRolePath} placeholder="/" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Trust Policy (JSON)</label>
<textarea bind:value={newRoleTrustPolicy} rows={6} class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-xs font-mono"></textarea>
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => showCreateRole = false} class="flex-1 py-2 border border-slate-300 dark:border-slate-600 rounded-lg text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700">Cancel</button>
<button onclick={createRole} disabled={creating} class="flex-1 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">{creating ? 'Creating…' : 'Create Role'}</button>
</div>
</div>
</div>
{/if}

<!-- ─── Create Group Modal ─── -->
{#if showCreateGroup}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onclick={(e) => { if (e.target === e.currentTarget) showCreateGroup = false; }}>
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-md p-6 space-y-4">
<div class="flex items-center justify-between">
<h2 class="text-lg font-bold text-slate-900 dark:text-white">Create IAM Group</h2>
<button onclick={() => showCreateGroup = false}><X class="w-5 h-5 text-slate-400" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Group Name *</label>
<input type="text" bind:value={newGroupName} placeholder="e.g. Developers" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Path</label>
<input type="text" bind:value={newGroupPath} placeholder="/" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => showCreateGroup = false} class="flex-1 py-2 border border-slate-300 dark:border-slate-600 rounded-lg text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700">Cancel</button>
<button onclick={createGroup} disabled={creating} class="flex-1 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">{creating ? 'Creating…' : 'Create Group'}</button>
</div>
</div>
</div>
{/if}

<!-- ─── Create Policy Modal ─── -->
{#if showCreatePolicy}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4" onclick={(e) => { if (e.target === e.currentTarget) showCreatePolicy = false; }}>
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-lg p-6 space-y-4">
<div class="flex items-center justify-between">
<h2 class="text-lg font-bold text-slate-900 dark:text-white">Create IAM Policy</h2>
<button onclick={() => showCreatePolicy = false}><X class="w-5 h-5 text-slate-400" /></button>
</div>
<div class="space-y-3">
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Policy Name *</label>
<input type="text" bind:value={newPolicyName} placeholder="e.g. S3ReadOnlyAccess" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Path</label>
<input type="text" bind:value={newPolicyPath} placeholder="/" class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
</div>
<div>
<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Policy Document (JSON)</label>
<textarea bind:value={newPolicyDoc} rows={8} class="w-full px-3 py-2 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white text-xs font-mono"></textarea>
</div>
</div>
<div class="flex gap-3 pt-2">
<button onclick={() => showCreatePolicy = false} class="flex-1 py-2 border border-slate-300 dark:border-slate-600 rounded-lg text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-50 dark:hover:bg-slate-700">Cancel</button>
<button onclick={createPolicy} disabled={creating} class="flex-1 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-50 text-white rounded-lg text-sm font-medium">{creating ? 'Creating…' : 'Create Policy'}</button>
</div>
</div>
</div>
{/if}

<!-- ─── Created Access Key Modal ─── -->
{#if showCreateKey && createdKey}
<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-2xl w-full max-w-md p-6 space-y-4">
<div class="flex items-center justify-between">
<h2 class="text-lg font-bold text-slate-900 dark:text-white">Access Key Created</h2>
<button onclick={() => { showCreateKey = false; createdKey = null; }}><X class="w-5 h-5 text-slate-400" /></button>
</div>
<div class="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded-lg p-3 text-xs text-amber-800 dark:text-amber-200">
This is the only time you can view the secret access key. Store it securely.
</div>
<div class="space-y-3">
<div>
<label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Access Key ID</label>
<div class="flex items-center gap-2">
<code class="flex-1 text-xs font-mono bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-slate-800 dark:text-slate-200">{createdKey.AccessKeyId}</code>
<button onclick={() => copyToClipboard(createdKey?.AccessKeyId ?? '', 'Copied!')}><Copy class="w-4 h-4 text-slate-400 hover:text-slate-600" /></button>
</div>
</div>
<div>
<label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Secret Access Key</label>
<div class="flex items-center gap-2">
<code class="flex-1 text-xs font-mono bg-slate-100 dark:bg-slate-700 px-2 py-1 rounded text-slate-800 dark:text-slate-200 break-all">{showSecret ? createdKey.SecretAccessKey : '••••••••••••••••••••••••'}</code>
<button onclick={() => showSecret = !showSecret}>{#if showSecret}<EyeOff class="w-4 h-4 text-slate-400" />{:else}<Eye class="w-4 h-4 text-slate-400" />{/if}</button>
<button onclick={() => copyToClipboard(createdKey?.SecretAccessKey ?? '', 'Copied!')}><Copy class="w-4 h-4 text-slate-400 hover:text-slate-600" /></button>
</div>
</div>
</div>
<button onclick={() => { showCreateKey = false; createdKey = null; }} class="w-full py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm font-medium">Done</button>
</div>
</div>
{/if}

{#if showLoginProfileModal && selectedUser}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
<div class="w-full max-w-md rounded-lg bg-white p-6 shadow-xl dark:bg-slate-800 space-y-4">
<h2 class="text-lg font-semibold">{userHasLoginProfile ? 'Reset' : 'Create'} Console Password</h2>
<p class="text-sm text-slate-500">User: {selectedUser.UserName}</p>
<div>
<label for="lp-pass" class="block text-sm font-medium mb-1">Password</label>
<input id="lp-pass" type="text" bind:value={loginProfilePassword} placeholder="Temp password" class="w-full rounded-md border border-slate-300 bg-white px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900" />
</div>
<label class="flex items-center gap-2 text-sm">
<input type="checkbox" bind:checked={loginProfileResetRequired} />
Require password reset at next sign-in
</label>
<div class="flex justify-end gap-2">
<button onclick={() => (showLoginProfileModal = false)} class="rounded border border-slate-300 px-4 py-2 text-sm hover:bg-slate-50 dark:border-slate-600 dark:hover:bg-slate-700">Cancel</button>
<button onclick={saveLoginProfile} disabled={savingLoginProfile || !loginProfilePassword.trim()} class="rounded bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700 disabled:opacity-50">
{savingLoginProfile ? 'Saving…' : 'Save'}
</button>
</div>
</div>
</div>
{/if}
