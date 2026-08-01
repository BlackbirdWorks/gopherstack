<script lang="ts">
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
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
ListRolePoliciesCommand, GetRolePolicyCommand, PutRolePolicyCommand, DeleteRolePolicyCommand,
ListGroupsForUserCommand,
GetLoginProfileCommand, CreateLoginProfileCommand, UpdateLoginProfileCommand, DeleteLoginProfileCommand,
ListVirtualMFADevicesCommand, DeactivateMFADeviceCommand,
SimulateCustomPolicyCommand,
GenerateCredentialReportCommand, GetCredentialReportCommand,
type User, type Role, type Group, type Policy as ManagedPolicy, type AccessKeyMetadata,
type VirtualMFADevice
} from '@aws-sdk/client-iam';
import { toast } from 'svelte-sonner';
import {
Users, Shield, RefreshCw, Search, UserCircle, ChevronRight, FileText,
Copy, Plus, Trash2, Key, BarChart3, BookOpen, X, Eye, EyeOff,
FlaskConical, ClipboardList, CheckCircle2, XCircle
} from 'lucide-svelte';

const iam = regionalClient(getIAMClient);

// ─── Types ───
type MainTab = 'users' | 'roles' | 'groups' | 'policies' | 'simulator' | 'report' | 'metrics' | 'docs';

interface SimResult {
EvalActionName?: string;
EvalResourceName?: string;
EvalDecision?: string;
}

interface CredReportRow {
user: string;
arn: string;
passwordEnabled: string;
mfaActive: string;
key1Active: string;
key2Active: string;
}
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

// Inline policy state
let userInlinePolicies = $state<string[]>([]);
let inlinePolicyName = $state('');
let inlinePolicyDoc = $state('{\n  "Version": "2012-10-17",\n  "Statement": [{\n    "Effect": "Allow",\n    "Action": "*",\n    "Resource": "*"\n  }]\n}');
let showInlinePolicyEditor = $state(false);
let savingInlinePolicy = $state(false);

// Role inline policy state
let roleInlinePolicies = $state<string[]>([]);
let roleInlinePolicyName = $state('');
let roleInlinePolicyDoc = $state('{\n  "Version": "2012-10-17",\n  "Statement": [{\n    "Effect": "Allow",\n    "Action": "*",\n    "Resource": "*"\n  }]\n}');
let showRoleInlinePolicyEditor = $state(false);
let savingRoleInlinePolicy = $state(false);

// Group membership
let userGroups = $state<{ GroupName?: string; GroupId?: string }[]>([]);

// Policy simulator state
let simPolicyDoc = $state('{\n  "Version": "2012-10-17",\n  "Statement": [{\n    "Effect": "Allow",\n    "Action": "s3:GetObject",\n    "Resource": "arn:aws:s3:::example-bucket/*"\n  }]\n}');
let simActions = $state('s3:GetObject');
let simResources = $state('arn:aws:s3:::example-bucket/*');
let simResults = $state<SimResult[]>([]);
let simRunning = $state(false);
let simError = $state('');

// Credential report state
let credReportRows = $state<CredReportRow[]>([]);
let credReportGenerated = $state('');
let credReportLoading = $state(false);
let credReportError = $state('');

// Login profile
let loginProfileExists = $state<boolean | null>(null);
let loginProfilePassword = $state('');
let showLoginProfileSection = $state(false);

// MFA devices
let userMFADevices = $state<VirtualMFADevice[]>([]);
let showMFASection = $state(false);

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

// IAM users/roles/groups/policies are account-scoped, not region-scoped
// (confirmed against services/iam: StorageBackend has no region parameter
// anywhere), so a region switch does not need to clear this state — it only
// needs to rebuild the client so a stale signing region isn't reused, and
// re-run the same initial load.
onRegionChange(() => {
void Promise.all([loadUsers(), loadAccountAlias(), loadSummary()]);
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
userInlinePolicies = [];
userGroups = [];
loginProfileExists = null;
userMFADevices = [];
try {
const [pol, keys, inlinePol, groupsRes, mfaRes] = await Promise.all([
iam().send(new ListAttachedUserPoliciesCommand({ UserName: user.UserName })),
iam().send(new ListAccessKeysCommand({ UserName: user.UserName })),
iam().send(new ListUserPoliciesCommand({ UserName: user.UserName })),
iam().send(new ListGroupsForUserCommand({ UserName: user.UserName })),
iam().send(new ListVirtualMFADevicesCommand({ AssignmentStatus: 'Assigned' })),
]);
userAttachedPolicies = pol.AttachedPolicies || [];
userAccessKeys = keys.AccessKeyMetadata || [];
userInlinePolicies = inlinePol.PolicyNames || [];
userGroups = (groupsRes.Groups || []).map(g => ({ GroupName: g.GroupName, GroupId: g.GroupId }));
userMFADevices = (mfaRes.VirtualMFADevices || []).filter(d => d.User?.UserName === user.UserName);
} catch {
		// non-critical
		} finally {
detailLoading = false;
}
// Check login profile
try {
await iam().send(new GetLoginProfileCommand({ UserName: user.UserName! }));
loginProfileExists = true;
} catch {
loginProfileExists = false;
}
}

async function loadInlinePolicyDoc(policyName: string) {
if (!selectedUser?.UserName) return;
try {
const res = await iam().send(new GetUserPolicyCommand({ UserName: selectedUser.UserName, PolicyName: policyName }));
inlinePolicyName = policyName;
inlinePolicyDoc = decodeURIComponent(res.PolicyDocument ?? '{}');
showInlinePolicyEditor = true;
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load policy');
}
}

async function saveInlinePolicy() {
if (!selectedUser?.UserName || !inlinePolicyName.trim()) return;
savingInlinePolicy = true;
try {
await iam().send(new PutUserPolicyCommand({ UserName: selectedUser.UserName, PolicyName: inlinePolicyName.trim(), PolicyDocument: inlinePolicyDoc }));
toast.success('Inline policy saved');
showInlinePolicyEditor = false;
const res = await iam().send(new ListUserPoliciesCommand({ UserName: selectedUser.UserName }));
userInlinePolicies = res.PolicyNames || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to save policy');
} finally {
savingInlinePolicy = false;
}
}

async function deleteInlinePolicy(policyName: string) {
if (!selectedUser?.UserName) return;
try {
await iam().send(new DeleteUserPolicyCommand({ UserName: selectedUser.UserName, PolicyName: policyName }));
toast.success('Inline policy deleted');
const res = await iam().send(new ListUserPoliciesCommand({ UserName: selectedUser.UserName }));
userInlinePolicies = res.PolicyNames || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete policy');
}
}

async function createLoginProfile() {
if (!selectedUser?.UserName || !loginProfilePassword) return;
try {
await iam().send(new CreateLoginProfileCommand({ UserName: selectedUser.UserName, Password: loginProfilePassword, PasswordResetRequired: false }));
toast.success('Login profile created');
loginProfileExists = true;
loginProfilePassword = '';
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to create login profile');
}
}

async function updateLoginProfile() {
if (!selectedUser?.UserName || !loginProfilePassword) return;
try {
await iam().send(new UpdateLoginProfileCommand({ UserName: selectedUser.UserName, Password: loginProfilePassword }));
toast.success('Password updated');
loginProfilePassword = '';
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to update password');
}
}

async function deleteLoginProfile() {
if (!selectedUser?.UserName) return;
try {
await iam().send(new DeleteLoginProfileCommand({ UserName: selectedUser.UserName }));
toast.success('Login profile deleted');
loginProfileExists = false;
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete login profile');
}
}

async function deactivateMFA(serialNumber: string) {
if (!selectedUser?.UserName) return;
try {
await iam().send(new DeactivateMFADeviceCommand({ UserName: selectedUser.UserName, SerialNumber: serialNumber }));
toast.success('MFA device deactivated');
userMFADevices = userMFADevices.filter(d => d.SerialNumber !== serialNumber);
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to deactivate MFA');
}
}

async function loadRoleDetail(role: Role) {
if (!role.RoleName) return;
detailLoading = true;
roleInlinePolicies = [];
showRoleInlinePolicyEditor = false;
try {
const [pol, inline] = await Promise.all([
iam().send(new ListAttachedRolePoliciesCommand({ RoleName: role.RoleName })),
iam().send(new ListRolePoliciesCommand({ RoleName: role.RoleName })),
]);
roleAttachedPolicies = pol.AttachedPolicies || [];
roleInlinePolicies = inline.PolicyNames || [];
} catch {
		// non-critical
		} finally {
detailLoading = false;
}
}

async function loadRoleInlinePolicyDoc(policyName: string) {
if (!selectedRole?.RoleName) return;
try {
const res = await iam().send(new GetRolePolicyCommand({ RoleName: selectedRole.RoleName, PolicyName: policyName }));
roleInlinePolicyName = policyName;
roleInlinePolicyDoc = decodeURIComponent(res.PolicyDocument ?? '{}');
showRoleInlinePolicyEditor = true;
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to load policy');
}
}

async function saveRoleInlinePolicy() {
if (!selectedRole?.RoleName || !roleInlinePolicyName.trim()) return;
savingRoleInlinePolicy = true;
try {
await iam().send(new PutRolePolicyCommand({ RoleName: selectedRole.RoleName, PolicyName: roleInlinePolicyName.trim(), PolicyDocument: roleInlinePolicyDoc }));
toast.success('Role inline policy saved');
showRoleInlinePolicyEditor = false;
const res = await iam().send(new ListRolePoliciesCommand({ RoleName: selectedRole.RoleName }));
roleInlinePolicies = res.PolicyNames || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to save policy');
} finally {
savingRoleInlinePolicy = false;
}
}

async function deleteRoleInlinePolicy(policyName: string) {
if (!selectedRole?.RoleName) return;
try {
await iam().send(new DeleteRolePolicyCommand({ RoleName: selectedRole.RoleName, PolicyName: policyName }));
toast.success('Role inline policy deleted');
const res = await iam().send(new ListRolePoliciesCommand({ RoleName: selectedRole.RoleName }));
roleInlinePolicies = res.PolicyNames || [];
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete policy');
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
else if (t === 'report' && credReportRows.length === 0) await loadCredentialReport();
}

// ─── Policy simulator ───

async function runSimulation() {
const actions = simActions.split(/[\s,]+/).map(s => s.trim()).filter(Boolean);
if (actions.length === 0) { toast.error('At least one action is required'); return; }
const resources = simResources.split(/[\s,]+/).map(s => s.trim()).filter(Boolean);
simRunning = true;
simError = '';
simResults = [];
try {
const res = await iam().send(new SimulateCustomPolicyCommand({
PolicyInputList: [simPolicyDoc],
ActionNames: actions,
ResourceArns: resources.length > 0 ? resources : undefined,
}));
simResults = (res.EvaluationResults ?? []).map(r => ({
EvalActionName: r.EvalActionName,
EvalResourceName: r.EvalResourceName,
EvalDecision: r.EvalDecision,
}));
if (simResults.length === 0) toast.info('No evaluation results returned');
} catch (e) {
simError = e instanceof Error ? e.message : String(e);
toast.error('Simulation failed: ' + simError);
} finally {
simRunning = false;
}
}

// ─── Credential report ───

function decodeCredReport(content: unknown): string {
if (content === null || content === undefined) return '';
if (typeof content === 'string') { try { return atob(content); } catch { return content; } }
// Uint8Array / ArrayBuffer / array-like of bytes (avoid cross-realm instanceof).
try {
const bytes = content instanceof ArrayBuffer ? new Uint8Array(content) : new Uint8Array(content as ArrayLike<number>);
return new TextDecoder().decode(bytes);
} catch {
return String(content);
}
}

function parseCredReport(csv: string): CredReportRow[] {
const lines = csv.trim().split('\n');
if (lines.length < 2) return [];
const header = lines[0].split(',');
const idx = (name: string) => header.indexOf(name);
const rows: CredReportRow[] = [];
for (let i = 1; i < lines.length; i++) {
const cols = lines[i].split(',');
if (cols.length < 2) continue;
const at = (name: string) => { const j = idx(name); return j >= 0 && j < cols.length ? cols[j] : ''; };
rows.push({
user: at('user') || cols[0],
arn: at('arn') || cols[1],
passwordEnabled: at('password_enabled'),
mfaActive: at('mfa_active'),
key1Active: at('access_key_1_active'),
key2Active: at('access_key_2_active'),
});
}
return rows;
}

async function loadCredentialReport() {
credReportLoading = true;
credReportError = '';
try {
await iam().send(new GenerateCredentialReportCommand({}));
const res = await iam().send(new GetCredentialReportCommand({}));
const csv = decodeCredReport(res.Content);
credReportRows = parseCredReport(csv);
credReportGenerated = res.GeneratedTime ? new Date(res.GeneratedTime).toLocaleString() : new Date().toLocaleString();
} catch (e) {
credReportError = e instanceof Error ? e.message : String(e);
toast.error('Credential report failed: ' + credReportError);
} finally {
credReportLoading = false;
}
}

async function refresh() {
selectedUser = null; selectedRole = null; selectedGroup = null; selectedPolicy = null;
userAttachedPolicies = []; roleAttachedPolicies = []; groupAttachedPolicies = []; userAccessKeys = [];
if (tab === 'users') { users = []; await loadUsers(); }
else if (tab === 'roles') { roles = []; await loadRoles(); }
else if (tab === 'groups') { groups = []; await loadGroups(); }
else if (tab === 'policies') { policies = []; await loadPolicies(); }
else if (tab === 'metrics') await loadSummary();
else if (tab === 'report') { credReportRows = []; await loadCredentialReport(); }
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
['policies','Policies'], ['simulator','Simulator'], ['report','Credential Report'],
['metrics','Metrics'], ['docs','Docs']
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

{#if tab !== 'metrics' && tab !== 'docs' && tab !== 'simulator' && tab !== 'report'}
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

<!-- ─── Policy Simulator Tab ─── -->
{#if tab === 'simulator'}
<div class="grid grid-cols-1 lg:grid-cols-2 gap-4">
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center gap-2">
<FlaskConical class="w-4 h-4 text-indigo-500" />
<span class="font-semibold text-slate-800 dark:text-white text-sm">Simulate Custom Policy</span>
</div>
<div class="p-4 space-y-3">
<div>
<label for="sim-policy" class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Policy Document (JSON)</label>
<textarea id="sim-policy" bind:value={simPolicyDoc} rows="10"
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-slate-50 dark:bg-slate-900 text-slate-900 dark:text-white text-xs font-mono"></textarea>
</div>
<div>
<label for="sim-actions" class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Actions (comma or space separated)</label>
<input id="sim-actions" type="text" bind:value={simActions}
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white text-sm font-mono" />
</div>
<div>
<label for="sim-resources" class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Resource ARNs (optional)</label>
<input id="sim-resources" type="text" bind:value={simResources}
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white text-sm font-mono" />
</div>
<button id="run-simulation" onclick={runSimulation} disabled={simRunning}
class="w-full py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm disabled:opacity-50 flex items-center justify-center gap-2">
<FlaskConical class="w-4 h-4" />{simRunning ? 'Simulating…' : 'Run Simulation'}
</button>
</div>
</div>
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center gap-2">
<span class="font-semibold text-slate-800 dark:text-white text-sm">Evaluation Results</span>
</div>
<div class="p-4">
{#if simError}
<p class="text-sm text-red-500">{simError}</p>
{:else if simResults.length === 0}
<p class="text-sm text-slate-400" id="sim-empty">No results yet. Run a simulation.</p>
{:else}
<div class="divide-y divide-slate-100 dark:divide-slate-700">
{#each simResults as r}
<div class="py-2 flex items-center justify-between gap-2">
<div class="min-w-0">
<p class="font-mono text-xs text-slate-800 dark:text-slate-200 truncate">{r.EvalActionName}</p>
<p class="font-mono text-[11px] text-slate-400 truncate">{r.EvalResourceName}</p>
</div>
{#if r.EvalDecision === 'allowed'}
<span class="shrink-0 inline-flex items-center gap-1 text-xs font-semibold text-green-600 dark:text-green-400"><CheckCircle2 class="w-4 h-4" />allowed</span>
{:else}
<span class="shrink-0 inline-flex items-center gap-1 text-xs font-semibold text-red-500"><XCircle class="w-4 h-4" />{r.EvalDecision}</span>
{/if}
</div>
{/each}
</div>
{/if}
</div>
</div>
</div>

<!-- ─── Credential Report Tab ─── -->
{:else if tab === 'report'}
<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between gap-2">
<div class="flex items-center gap-2">
<ClipboardList class="w-4 h-4 text-indigo-500" />
<span class="font-semibold text-slate-800 dark:text-white text-sm">Credential Report</span>
{#if credReportGenerated}<span class="text-xs text-slate-400">generated {credReportGenerated}</span>{/if}
</div>
<button id="regen-report" onclick={loadCredentialReport} disabled={credReportLoading}
class="px-3 py-1.5 border border-slate-200 dark:border-slate-700 rounded-lg text-xs text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 disabled:opacity-50 flex items-center gap-1">
<RefreshCw class="w-3.5 h-3.5" />Regenerate
</button>
</div>
{#if credReportLoading}
<div class="p-12 text-center text-slate-500">
<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
<p>Generating report…</p>
</div>
{:else if credReportError}
<p class="p-4 text-sm text-red-500">{credReportError}</p>
{:else if credReportRows.length === 0}
<p class="p-12 text-center text-sm text-slate-400" id="report-empty">No credential data.</p>
{:else}
<div class="overflow-x-auto">
<table class="w-full text-sm">
<thead>
<tr class="text-left text-xs text-slate-500 dark:text-slate-400 border-b border-slate-200 dark:border-slate-700">
<th class="px-4 py-2 font-medium">User</th>
<th class="px-4 py-2 font-medium">Password</th>
<th class="px-4 py-2 font-medium">MFA</th>
<th class="px-4 py-2 font-medium">Access Key 1</th>
<th class="px-4 py-2 font-medium">Access Key 2</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each credReportRows as row}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-2">
<p class="font-medium text-slate-900 dark:text-white">{row.user}</p>
<p class="text-[11px] text-slate-400 font-mono truncate max-w-xs">{row.arn}</p>
</td>
<td class="px-4 py-2"><span class="text-xs {row.passwordEnabled === 'true' ? 'text-green-600 dark:text-green-400' : 'text-slate-400'}">{row.passwordEnabled || 'n/a'}</span></td>
<td class="px-4 py-2"><span class="text-xs {row.mfaActive === 'true' ? 'text-green-600 dark:text-green-400' : 'text-slate-400'}">{row.mfaActive || 'n/a'}</span></td>
<td class="px-4 py-2"><span class="text-xs {row.key1Active === 'true' ? 'text-green-600 dark:text-green-400' : 'text-slate-400'}">{row.key1Active || 'false'}</span></td>
<td class="px-4 py-2"><span class="text-xs {row.key2Active === 'true' ? 'text-green-600 dark:text-green-400' : 'text-slate-400'}">{row.key2Active || 'false'}</span></td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
</div>

<!-- ─── Metrics Tab ─── -->
{:else if tab === 'metrics'}
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

<!-- Inline Policies -->
<div>
<div class="flex items-center justify-between mb-2">
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide">Inline Policies</p>
<button onclick={() => { inlinePolicyName = ''; inlinePolicyDoc = '{\n  "Version": "2012-10-17",\n  "Statement": [{"Effect": "Allow","Action": "*","Resource": "*"}]\n}'; showInlinePolicyEditor = true; }} class="text-xs text-indigo-600 hover:text-indigo-700 flex items-center gap-1"><Plus class="w-3 h-3" /> New</button>
</div>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading…</p>
{:else if userInlinePolicies.length === 0}
<p class="text-xs text-slate-400">None</p>
{:else}
<ul class="space-y-1">
{#each userInlinePolicies as pname}
<li class="flex items-center justify-between text-xs">
<button onclick={() => loadInlinePolicyDoc(pname)} class="text-indigo-700 dark:text-indigo-300 font-mono hover:underline truncate">{pname}</button>
<button onclick={() => deleteInlinePolicy(pname)} class="shrink-0 text-red-400 hover:text-red-600 ml-1"><Trash2 class="w-3 h-3" /></button>
</li>
{/each}
</ul>
{/if}
{#if showInlinePolicyEditor}
<div class="mt-2 space-y-2">
<input type="text" bind:value={inlinePolicyName} placeholder="Policy name" class="w-full text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white" />
<textarea bind:value={inlinePolicyDoc} rows="6" class="w-full text-xs font-mono border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white"></textarea>
<div class="flex gap-2">
<button onclick={saveInlinePolicy} disabled={savingInlinePolicy} class="flex-1 text-xs py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg disabled:opacity-50">{savingInlinePolicy ? 'Saving…' : 'Save'}</button>
<button onclick={() => showInlinePolicyEditor = false} class="flex-1 text-xs py-1.5 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">Cancel</button>
</div>
</div>
{/if}
</div>

<!-- Group Membership -->
<div>
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-2">Group Membership</p>
{#if userGroups.length === 0}
<p class="text-xs text-slate-400">Not in any groups</p>
{:else}
<ul class="space-y-1">
{#each userGroups as g}
<li class="text-xs text-indigo-700 dark:text-indigo-300 font-mono truncate">{g.GroupName}</li>
{/each}
</ul>
{/if}
</div>

<!-- Login Profile -->
<div>
<div class="flex items-center justify-between mb-2">
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide">Console Login</p>
<button onclick={() => showLoginProfileSection = !showLoginProfileSection} class="text-xs text-indigo-600 hover:text-indigo-700">{showLoginProfileSection ? 'Hide' : 'Manage'}</button>
</div>
{#if loginProfileExists !== null}
<span class="text-xs px-1.5 py-0.5 rounded {loginProfileExists ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-slate-100 text-slate-500 dark:bg-slate-700'}">{loginProfileExists ? 'Enabled' : 'Disabled'}</span>
{/if}
{#if showLoginProfileSection}
<div class="mt-2 space-y-2">
<input type="password" bind:value={loginProfilePassword} placeholder="New password" class="w-full text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white" />
<div class="flex gap-1">
{#if loginProfileExists}
<button onclick={updateLoginProfile} disabled={!loginProfilePassword} class="flex-1 text-xs py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg disabled:opacity-50">Update</button>
<button onclick={deleteLoginProfile} class="text-xs py-1.5 px-2 bg-red-600 hover:bg-red-700 text-white rounded-lg">Delete</button>
{:else}
<button onclick={createLoginProfile} disabled={!loginProfilePassword} class="flex-1 text-xs py-1.5 bg-green-600 hover:bg-green-700 text-white rounded-lg disabled:opacity-50">Enable Login</button>
{/if}
</div>
</div>
{/if}
</div>

<!-- MFA Devices -->
<div>
<div class="flex items-center justify-between mb-2">
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide">MFA Devices</p>
<button onclick={() => showMFASection = !showMFASection} class="text-xs text-indigo-600 hover:text-indigo-700">{showMFASection ? 'Hide' : 'View'}</button>
</div>
{#if showMFASection}
{#if userMFADevices.length === 0}
<p class="text-xs text-slate-400">No MFA devices</p>
{:else}
<ul class="space-y-1">
{#each userMFADevices as mfa}
<li class="flex items-center justify-between text-xs">
<span class="font-mono text-slate-700 dark:text-slate-300 truncate">{mfa.SerialNumber?.split('/').pop()}</span>
<button onclick={() => mfa.SerialNumber && deactivateMFA(mfa.SerialNumber)} class="shrink-0 text-red-400 hover:text-red-600 ml-1">Deactivate</button>
</li>
{/each}
</ul>
{/if}
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

<!-- Role Inline Policies -->
<div>
<div class="flex items-center justify-between mb-2">
<p class="text-xs font-semibold text-slate-500 dark:text-slate-400 uppercase tracking-wide">Inline Policies</p>
<button onclick={() => { roleInlinePolicyName = ''; roleInlinePolicyDoc = '{\n  "Version": "2012-10-17",\n  "Statement": [{"Effect": "Allow","Action": "*","Resource": "*"}]\n}'; showRoleInlinePolicyEditor = true; }} class="text-xs text-indigo-600 hover:text-indigo-700 flex items-center gap-1"><Plus class="w-3 h-3" /> New</button>
</div>
{#if detailLoading}
<p class="text-xs text-slate-400 animate-pulse">Loading…</p>
{:else if roleInlinePolicies.length === 0}
<p class="text-xs text-slate-400">None</p>
{:else}
<ul class="space-y-1">
{#each roleInlinePolicies as pname}
<li class="flex items-center justify-between text-xs">
<button onclick={() => loadRoleInlinePolicyDoc(pname)} class="text-indigo-700 dark:text-indigo-300 font-mono hover:underline truncate">{pname}</button>
<button onclick={() => deleteRoleInlinePolicy(pname)} class="shrink-0 text-red-400 hover:text-red-600 ml-1"><Trash2 class="w-3 h-3" /></button>
</li>
{/each}
</ul>
{/if}
{#if showRoleInlinePolicyEditor}
<div class="mt-2 space-y-2">
<input type="text" bind:value={roleInlinePolicyName} placeholder="Policy name" class="w-full text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white" />
<textarea bind:value={roleInlinePolicyDoc} rows="6" class="w-full text-xs font-mono border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-white dark:bg-slate-700 dark:text-white"></textarea>
<div class="flex gap-2">
<button onclick={saveRoleInlinePolicy} disabled={savingRoleInlinePolicy} class="flex-1 text-xs py-1.5 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg disabled:opacity-50">{savingRoleInlinePolicy ? 'Saving…' : 'Save'}</button>
<button onclick={() => showRoleInlinePolicyEditor = false} class="flex-1 text-xs py-1.5 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">Cancel</button>
</div>
</div>
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
