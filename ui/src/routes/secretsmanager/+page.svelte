<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
import { multiRegionList } from '$lib/multi-region';
import RegionChip from '$lib/components/RegionChip.svelte';
import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
import { getSecretsManagerClient } from '$lib/aws-client';
import {
	ListSecretsCommand,
	CreateSecretCommand,
	DeleteSecretCommand,
	RestoreSecretCommand,
	DescribeSecretCommand,
	GetSecretValueCommand,
	UpdateSecretCommand,
	RotateSecretCommand,
	CancelRotateSecretCommand,
	TagResourceCommand,
	UntagResourceCommand,
	ListSecretVersionIdsCommand
} from '@aws-sdk/client-secrets-manager';
import { toast } from 'svelte-sonner';
import { LockKeyhole, Plus, RefreshCw, Search, Trash2, RotateCcw, Copy, Eye, EyeOff, ChevronRight, Tag, History, RotateCw, X, KeyRound, Undo2, SortAsc, SortDesc, Beaker } from 'lucide-svelte';

const sm = regionalClient(getSecretsManagerClient);

type SecretItem = {
	ARN?: string;
	Description?: string;
	KmsKeyId?: string;
	LastAccessedDate?: Date | string | number;
	LastChangedDate?: Date | string | number;
	LastRotatedDate?: Date | string | number;
	NextRotationDate?: Date | string | number;
	CreatedDate?: Date | string | number;
	Name?: string;
	RotationEnabled?: boolean;
	RotationLambdaARN?: string;
	RotationRules?: { AutomaticallyAfterDays?: number; ScheduleExpression?: string };
	ReplicationStatus?: Array<{ Region?: string; Status?: string; StatusMessage?: string }>;
	DeletedDate?: Date | string | number;
	region?: string;
};

// Row actions (view/delete/restore) must target the region the row was
// fanned out from, not the picker's current region -- under All mode the
// same secret name can legitimately exist in two regions.
function detailClient() {
	return getSecretsManagerClient(selectedSecret?.region);
}

type VersionEntry = {
	VersionId?: string;
	VersionStages?: string[];
	CreatedDate?: number;
};

let secrets = $state<SecretItem[]>([]);
let loading = $state(true);
let search = $state('');
let sortDesc = $state(false);
let showCreateModal = $state(false);
let newSecretName = $state('');
let newSecretValue = $state('');
let newSecretDescription = $state('');
let newSecretKmsKey = $state('');
let activeTab = $state<'secrets' | 'detail'>('secrets');
let detailSubTab = $state<'overview' | 'versions' | 'tags' | 'replication'>('overview');
let selectedSecret = $state<SecretItem | null>(null);
let secretValue = $state<string | null>(null);
let showValue = $state(false);
let loadingValue = $state(false);
let editingValue = $state(false);
let editValue = $state('');
let editValueMode = $state<'text' | 'kv'>('text');
let editKvRows = $state<Array<{ key: string; value: string }>>([]);
let editDescription = $state('');
let savingSecret = $state(false);
let versions = $state<VersionEntry[]>([]);
let loadingVersions = $state(false);
let tagKey = $state('');
let tagValue = $state('');
let secretTags = $state<Record<string, string>>({});
let seedingDemo = $state(false);

onRegionChange(() => { void loadSecrets(); });

async function loadSecrets() {
	try {
		loading = true;
		const result = await multiRegionList(
			(region) => getSecretsManagerClient(region).send(new ListSecretsCommand({})),
			(r) => r.SecretList ?? []
		);
		secrets = result.items.map(({ region, item }) => ({ ...item, region }) as SecretItem);
		if (result.errors.length > 0) toast.error(`Failed to load secrets from ${result.errors.length} region(s)`);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load secrets');
	} finally {
		loading = false;
	}
}

async function viewSecret(secret: SecretItem) {
	selectedSecret = secret;
	secretValue = null;
	showValue = false;
	editingValue = false;
	detailSubTab = 'overview';
	activeTab = 'detail';
	// Load full metadata via DescribeSecret
	try {
		const desc = await getSecretsManagerClient(secret.region).send(new DescribeSecretCommand({ SecretId: secret.Name! }));
		selectedSecret = { ...selectedSecret, ...desc } as SecretItem;
		// Load tags
		const tagMap: Record<string, string> = {};
		if (desc.Tags) {
			for (const t of desc.Tags as Array<{Key?: string; Value?: string}>) {
				if (t.Key) tagMap[t.Key] = t.Value ?? '';
			}
		}
		secretTags = tagMap;
	} catch {
		// best-effort
	}
}

async function loadSecretValue(name: string) {
	loadingValue = true;
	try {
		const data = await detailClient().send(new GetSecretValueCommand({ SecretId: name }));
		secretValue = data.SecretString ?? '<binary>';
		showValue = true;
	} catch (e) {
		toast.error('Access denied or secret not retrievable');
	} finally {
		loadingValue = false;
	}
}

async function loadVersions(name: string) {
	loadingVersions = true;
	try {
		const data = await detailClient().send(new ListSecretVersionIdsCommand({ SecretId: name, IncludeDeprecated: true }));
		versions = (data.Versions || []) as VersionEntry[];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load versions');
	} finally {
		loadingVersions = false;
	}
}

// Parse a flat JSON object of string/number/bool values into editable rows.
// Returns null if the value is not a flat key-value object (e.g. arrays/nested).
function parseFlatKv(text: string): Array<{ key: string; value: string }> | null {
	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch {
		return null;
	}
	if (typeof parsed !== 'object' || parsed === null || Array.isArray(parsed)) return null;
	const rows: Array<{ key: string; value: string }> = [];
	for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
		if (v !== null && typeof v === 'object') return null;
		rows.push({ key: k, value: String(v) });
	}
	return rows;
}

function startEditValue() {
	editValue = secretValue ?? '';
	const rows = parseFlatKv(editValue);
	if (rows) {
		editKvRows = rows;
		editValueMode = 'kv';
	} else {
		editKvRows = [];
		editValueMode = 'text';
	}
	editingValue = true;
}

function switchEditMode(mode: 'text' | 'kv') {
	if (mode === editValueMode) return;
	if (mode === 'kv') {
		const rows = parseFlatKv(editValue);
		if (!rows) {
			toast.error('Current value is not a flat JSON object; cannot switch to key-value editor');
			return;
		}
		editKvRows = rows;
	} else {
		editValue = kvRowsToJson();
	}
	editValueMode = mode;
}

function kvRowsToJson(): string {
	const obj: Record<string, string> = {};
	for (const row of editKvRows) {
		if (row.key.trim()) obj[row.key.trim()] = row.value;
	}
	return JSON.stringify(obj, null, 2);
}

function addKvRow() {
	editKvRows = [...editKvRows, { key: '', value: '' }];
}

function removeKvRow(i: number) {
	editKvRows = editKvRows.filter((_, idx) => idx !== i);
}

async function saveSecretValue() {
	if (!selectedSecret?.Name) return;
	const payload = editValueMode === 'kv' ? kvRowsToJson() : editValue;
	savingSecret = true;
	try {
		await detailClient().send(new UpdateSecretCommand({
			SecretId: selectedSecret.Name,
			SecretString: payload
		}));
		toast.success('Secret value updated');
		secretValue = payload;
		editingValue = false;
		await viewSecret(selectedSecret);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to update secret value');
	} finally {
		savingSecret = false;
	}
}

async function saveDescription() {
	if (!selectedSecret?.Name) return;
	savingSecret = true;
	try {
		await detailClient().send(new UpdateSecretCommand({
			SecretId: selectedSecret.Name,
			Description: editDescription
		}));
		toast.success('Description updated');
		selectedSecret = { ...selectedSecret, Description: editDescription };
		editDescription = '';
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to update description');
	} finally {
		savingSecret = false;
	}
}

async function rotateNow() {
	if (!selectedSecret?.Name) return;
	if (!await confirmDestructive({ title: 'Rotate Secret', message: `Immediately rotate "${selectedSecret.Name}"? A new version will be created.` })) return;
	try {
		await detailClient().send(new RotateSecretCommand({ SecretId: selectedSecret.Name }));
		toast.success('Rotation triggered');
		secretValue = null;
		await viewSecret(selectedSecret);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to rotate secret');
	}
}

async function cancelRotation() {
	if (!selectedSecret?.Name) return;
	try {
		await detailClient().send(new CancelRotateSecretCommand({ SecretId: selectedSecret.Name }));
		toast.success('Rotation cancelled');
		await viewSecret(selectedSecret);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to cancel rotation');
	}
}

async function addTag() {
	if (!selectedSecret?.Name || !tagKey.trim()) {
		toast.error('Tag key is required');
		return;
	}
	try {
		await detailClient().send(new TagResourceCommand({
			SecretId: selectedSecret.Name,
			Tags: [{ Key: tagKey.trim(), Value: tagValue.trim() }]
		}));
		secretTags = { ...secretTags, [tagKey.trim()]: tagValue.trim() };
		tagKey = '';
		tagValue = '';
		toast.success('Tag added');
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to add tag');
	}
}

async function removeTag(key: string) {
	if (!selectedSecret?.Name) return;
	try {
		await detailClient().send(new UntagResourceCommand({ SecretId: selectedSecret.Name, TagKeys: [key] }));
		const updated = { ...secretTags };
		delete updated[key];
		secretTags = updated;
		toast.success(`Tag "${key}" removed`);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to remove tag');
	}
}

async function deleteSecret(name: string, region?: string) {
	if (!await confirmDestructive({ title: 'Delete Secret', message: `Delete secret "${name}"? Applications using this secret will lose access immediately.` })) return;
	try {
		await getSecretsManagerClient(region).send(new DeleteSecretCommand({ SecretId: name }));
		toast.success(`Secret "${name}" deleted`);
		if (selectedSecret?.Name === name) {
			activeTab = 'secrets';
			selectedSecret = null;
		}
		await loadSecrets();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to delete secret');
	}
}

async function createSecret() {
	if (!newSecretName.trim()) {
		toast.error('Secret name is required');

		return;
	}

	try {
		await sm().send(new CreateSecretCommand({
			Name: newSecretName.trim(),
			Description: newSecretDescription.trim() || undefined,
			SecretString: newSecretValue || undefined,
			KmsKeyId: newSecretKmsKey.trim() || undefined
		}));
		toast.success(`Secret "${newSecretName.trim()}" created`);
		showCreateModal = false;
		newSecretName = '';
		newSecretValue = '';
		newSecretDescription = '';
		newSecretKmsKey = '';
		await loadSecrets();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to create secret');
	}
}

async function restoreSecret(name: string, region?: string) {
	if (!await confirmDestructive({ title: 'Restore Secret', message: `Restore secret "${name}" from pending deletion?` })) return;
	try {
		await getSecretsManagerClient(region).send(new RestoreSecretCommand({ SecretId: name }));
		toast.success(`Secret "${name}" restored`);
		await loadSecrets();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to restore secret');
	}
}

async function seedDemoData() {
	seedingDemo = true;
	const demos = [
		{
			name: 'prod/database/primary',
			description: 'Production primary database credentials',
			value: JSON.stringify({ username: 'admin', password: 'P@ssw0rd!', host: 'db.prod.example.com', port: 5432 })
		},
		{
			name: 'prod/api/stripe-key',
			description: 'Stripe API key for payment processing',
			value: JSON.stringify({ publishable_key: 'pk_test_example', secret_key: 'sk_test_example' })
		},
		{
			name: 'staging/database/primary',
			description: 'Staging database credentials',
			value: JSON.stringify({ username: 'staging_user', password: 'Stag!ngP@ss', host: 'db.staging.example.com', port: 5432 })
		},
		{
			name: 'prod/oauth/github',
			description: 'GitHub OAuth app credentials',
			value: JSON.stringify({ client_id: 'Iv1.demo123', client_secret: 'demo_secret_value' })
		},
		{
			name: 'shared/tls/wildcard-cert',
			description: 'Wildcard TLS certificate and private key',
			value: JSON.stringify({ certificate: '-----BEGIN CERTIFICATE-----\n...', private_key: '-----BEGIN PRIVATE KEY-----\n...' })
		}
	];
	let created = 0;
	for (const d of demos) {
		try {
			await sm().send(new CreateSecretCommand({ Name: d.name, Description: d.description, SecretString: d.value }));
			created++;
		} catch {
			// skip if already exists
		}
	}
	seedingDemo = false;
	toast.success(`Demo data seeded (${created} new secrets)`);
	await loadSecrets();
}

async function copyToClipboard(text: string) {
	await navigator.clipboard.writeText(text);
	toast.success('Copied to clipboard');
}

function formatDate(d: Date | string | number | undefined): string {
	if (!d) return 'N/A';
	return new Date(typeof d === 'number' ? d * 1000 : d).toLocaleString();
}

let filtered = $derived(secrets.filter(s =>
	!search || s.Name?.toLowerCase().includes(search.toLowerCase()) || s.Description?.toLowerCase().includes(search.toLowerCase())
));

let rotationCount = $derived(secrets.filter(s => s.RotationEnabled).length);
let kmsCount = $derived(secrets.filter(s => s.KmsKeyId && s.KmsKeyId !== 'alias/aws/secretsmanager').length);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
				<LockKeyhole class="w-6 h-6 text-red-600 dark:text-red-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Secrets Manager</h1>
				<p class="text-slate-600 dark:text-slate-300">Secure secrets storage and rotation</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button onclick={loadSecrets} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700" title="Refresh">
				<RefreshCw class="w-4 h-4" />
			</button>
			<button onclick={seedDemoData} disabled={seedingDemo} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-1 disabled:opacity-50" title="Seed demo data">
				<Beaker class="w-4 h-4" /> Demo
			</button>
			<WriteRegionHint />
			<button onclick={() => showCreateModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Create Secret
			</button>
		</div>
	</div>

	<!-- Stats -->
	{#if !loading}
		<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Total Secrets</p>
				<p class="text-3xl font-bold text-slate-900 dark:text-white">{secrets.length}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Auto Rotation</p>
				<p class="text-3xl font-bold text-green-600 dark:text-green-400">{rotationCount}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Custom KMS</p>
				<p class="text-3xl font-bold text-indigo-600 dark:text-indigo-400">{kmsCount}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-4">
				<p class="text-xs text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">No Rotation</p>
				<p class="text-3xl font-bold text-amber-600 dark:text-amber-400">{secrets.length - rotationCount}</p>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<nav class="flex gap-1 -mb-px">
			<button onclick={() => activeTab = 'secrets'} class="px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'secrets' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				All Secrets {#if secrets.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{secrets.length}</span>{/if}
			</button>
			{#if selectedSecret}
				<button onclick={() => activeTab = 'detail'} class="px-4 py-2.5 text-sm font-medium border-b-2 transition-colors max-w-xs truncate {activeTab === 'detail' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
					{selectedSecret.Name}
				</button>
			{/if}
		</nav>
	</div>

	<!-- Secrets List Tab -->
	{#if activeTab === 'secrets'}
		<div class="flex gap-2">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
				<input type="text" placeholder="Search secrets..." bind:value={search}
					class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
			</div>
			<button onclick={() => sortDesc = !sortDesc} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-1" title="Toggle sort order">
				{#if sortDesc}<SortDesc class="w-4 h-4" />{:else}<SortAsc class="w-4 h-4" />{/if}
			</button>
		</div>

		{#if loading}
			<div class="text-center py-12 text-slate-500"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div><p>Loading secrets...</p></div>
		{:else if filtered.length === 0}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
				<LockKeyhole class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
				<p class="text-slate-600 dark:text-slate-300">No secrets found</p>
				<button onclick={() => showCreateModal = true} class="mt-4 px-4 py-2 bg-indigo-600 text-white rounded-lg">Create Secret</button>
			</div>
		{:else}
			<p class="text-sm text-slate-500 dark:text-slate-400">{secrets.length} secret{secrets.length !== 1 ? 's' : ''} found</p>
			<div class="grid gap-3">
				{#each (sortDesc ? [...filtered].reverse() : filtered) as secret}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5 hover:border-indigo-300 dark:hover:border-indigo-700 transition-colors {secret.DeletedDate ? 'border-l-4 border-l-red-400' : ''}">
						<div class="flex items-start justify-between mb-3">
							<div class="flex-1 min-w-0">
								<div class="flex items-center gap-2">
									<h3 class="font-semibold text-slate-900 dark:text-white">{secret.Name}</h3>
									<RegionChip region={secret.region} />
									{#if secret.KmsKeyId && secret.KmsKeyId !== 'alias/aws/secretsmanager'}
										<span class="shrink-0 flex items-center gap-0.5 px-1.5 py-0.5 bg-indigo-50 dark:bg-indigo-900/20 text-indigo-600 dark:text-indigo-400 rounded text-xs" title="Custom KMS key: {secret.KmsKeyId}">
											<KeyRound class="w-3 h-3" /> KMS
										</span>
									{/if}
									{#if secret.DeletedDate}
										<span class="shrink-0 px-1.5 py-0.5 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 rounded text-xs">Pending Deletion</span>
									{/if}
								</div>
								{#if secret.Description}
									<p class="text-sm text-slate-500 dark:text-slate-400 mt-0.5">{secret.Description}</p>
								{/if}
							</div>
							{#if secret.RotationEnabled}
								<span class="ml-2 shrink-0 px-2 py-1 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 rounded text-xs font-medium flex items-center gap-1">
									<RotateCcw class="w-3 h-3" /> Rotation On
								</span>
							{/if}
						</div>
						<div class="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm mb-3">
							<div>
								<p class="text-xs text-slate-500 uppercase">Last Changed</p>
								<p class="text-slate-900 dark:text-white">{secret.LastChangedDate ? new Date(typeof secret.LastChangedDate === 'number' ? secret.LastChangedDate * 1000 : secret.LastChangedDate).toLocaleDateString() : 'N/A'}</p>
							</div>
							<div>
								<p class="text-xs text-slate-500 uppercase">Last Accessed</p>
								<p class="text-slate-900 dark:text-white">{secret.LastAccessedDate ? new Date(typeof secret.LastAccessedDate === 'number' ? secret.LastAccessedDate * 1000 : secret.LastAccessedDate).toLocaleDateString() : 'N/A'}</p>
							</div>
							<div>
								<p class="text-xs text-slate-500 uppercase">Last Rotated</p>
								<p class="text-slate-900 dark:text-white">{secret.LastRotatedDate ? new Date(typeof secret.LastRotatedDate === 'number' ? secret.LastRotatedDate * 1000 : secret.LastRotatedDate).toLocaleDateString() : 'N/A'}</p>
							</div>
							<div>
								<p class="text-xs text-slate-500 uppercase">Created</p>
								<p class="text-slate-900 dark:text-white">{secret.CreatedDate ? new Date(typeof secret.CreatedDate === 'number' ? secret.CreatedDate * 1000 : secret.CreatedDate).toLocaleDateString() : 'N/A'}</p>
							</div>
						</div>
						<div class="flex gap-2 flex-wrap">
							<button onclick={() => viewSecret(secret)} class="px-3 py-1.5 text-sm bg-indigo-50 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 rounded-lg hover:bg-indigo-100 dark:hover:bg-indigo-900/50 flex items-center gap-1">
								View <ChevronRight class="w-3 h-3" />
							</button>
							<button onclick={() => copyToClipboard(secret.ARN ?? '')} class="px-3 py-1.5 border border-slate-200 dark:border-slate-700 rounded text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-1">
								<Copy class="w-3 h-3" /> ARN
							</button>
							{#if secret.DeletedDate}
								<button onclick={() => restoreSecret(secret.Name ?? '', secret.region)} class="px-3 py-1.5 bg-green-600 text-white rounded text-sm hover:bg-green-700 flex items-center gap-1">
									<Undo2 class="w-3 h-3" /> Restore
								</button>
							{:else}
								<button onclick={() => deleteSecret(secret.Name ?? '', secret.region)} class="px-3 py-1.5 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1">
									<Trash2 class="w-3 h-3" /> Delete
								</button>
							{/if}
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- Detail Tab -->
	{#if activeTab === 'detail' && selectedSecret}
		<div class="space-y-4">
			<button onclick={() => { activeTab = 'secrets'; selectedSecret = null; }} class="text-sm text-indigo-600 dark:text-indigo-400 hover:underline flex items-center gap-1">
				← Back to all secrets
			</button>

			<!-- Detail sub-tabs -->
			<div class="border-b border-slate-200 dark:border-slate-700">
				<nav class="flex gap-1 -mb-px">
					{#each [['overview','Overview'],['versions','Versions'],['tags','Tags'],['replication','Replication']] as [tab, label]}
						<button onclick={() => {
							detailSubTab = tab as typeof detailSubTab;
							if (tab === 'versions' && selectedSecret) loadVersions(selectedSecret.Name!);
						}} class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {detailSubTab === tab ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
							{label}
						</button>
					{/each}
				</nav>
			</div>

			{#if detailSubTab === 'overview'}
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-6 space-y-4">
				<div class="flex items-start justify-between">
					<div>
						<div class="flex items-center gap-2">
							<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedSecret.Name}</h2>
							<RegionChip region={selectedSecret.region} />
						</div>
						<div class="flex items-center gap-2 mt-1">
							{#if selectedSecret.Description}
								<p class="text-slate-500 dark:text-slate-400 text-sm">{selectedSecret.Description}</p>
							{:else}
								<p class="text-slate-400 dark:text-slate-500 text-sm italic">No description</p>
							{/if}
						</div>
					</div>
					<div class="flex gap-2 items-center">
						{#if selectedSecret.RotationEnabled}
							<span class="px-2 py-1 bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300 rounded text-xs flex items-center gap-1">
								<RotateCcw class="w-3 h-3" /> Auto Rotation
							</span>
						{/if}
					</div>
				</div>

				<!-- Edit Description -->
				<div class="flex gap-2 items-center">
					<input type="text" bind:value={editDescription} placeholder="Update description..." class="flex-1 px-3 py-1.5 text-sm border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
					<button onclick={saveDescription} disabled={savingSecret || !editDescription.trim()} class="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-40">Save</button>
				</div>

				<div class="grid grid-cols-1 sm:grid-cols-2 gap-4 text-sm">
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">ARN</p>
						<div class="flex items-center gap-2">
							<p class="text-slate-700 dark:text-slate-300 font-mono text-xs truncate">{selectedSecret.ARN}</p>
							<button onclick={() => copyToClipboard(selectedSecret?.ARN ?? '')} class="shrink-0 p-1 hover:bg-slate-100 dark:hover:bg-slate-700 rounded">
								<Copy class="w-3.5 h-3.5 text-slate-400" />
							</button>
						</div>
					</div>
					{#if selectedSecret.KmsKeyId}
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">KMS Key</p>
						<div class="flex items-center gap-1">
							<KeyRound class="w-3 h-3 text-indigo-500" />
							<p class="text-slate-700 dark:text-slate-300 font-mono text-xs truncate">{selectedSecret.KmsKeyId}</p>
						</div>
					</div>
					{/if}
					{#if selectedSecret.CreatedDate}
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">Created</p>
						<p class="text-slate-900 dark:text-white">{formatDate(selectedSecret.CreatedDate as number)}</p>
					</div>
					{/if}
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">Last Changed</p>
						<p class="text-slate-900 dark:text-white">{formatDate(selectedSecret.LastChangedDate as number)}</p>
					</div>
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">Last Accessed</p>
						<p class="text-slate-900 dark:text-white">{formatDate(selectedSecret.LastAccessedDate as number)}</p>
					</div>
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">Last Rotated</p>
						<p class="text-slate-900 dark:text-white">{formatDate(selectedSecret.LastRotatedDate as number)}</p>
					</div>
					{#if selectedSecret.NextRotationDate}
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">Next Rotation</p>
						<p class="text-slate-900 dark:text-white">{formatDate(selectedSecret.NextRotationDate as number)}</p>
					</div>
					{/if}
					{#if selectedSecret.RotationLambdaARN}
					<div class="sm:col-span-2">
						<p class="text-xs text-slate-500 uppercase mb-1">Rotation Lambda ARN</p>
						<p class="text-slate-700 dark:text-slate-300 font-mono text-xs">{selectedSecret.RotationLambdaARN}</p>
					</div>
					{/if}
					{#if selectedSecret.RotationRules}
					<div>
						<p class="text-xs text-slate-500 uppercase mb-1">Rotation Schedule</p>
						<p class="text-slate-900 dark:text-white">
							{selectedSecret.RotationRules.ScheduleExpression || (selectedSecret.RotationRules.AutomaticallyAfterDays ? `Every ${selectedSecret.RotationRules.AutomaticallyAfterDays} days` : 'N/A')}
						</p>
					</div>
					{/if}
				</div>

				<!-- Secret Value Section -->
				<div class="border-t border-slate-200 dark:border-slate-700 pt-4">
					<div class="flex items-center justify-between mb-2">
						<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Secret Value</p>
						<div class="flex gap-2">
							{#if !secretValue}
								<button onclick={() => loadSecretValue(selectedSecret?.Name ?? '')} disabled={loadingValue} class="px-3 py-1.5 text-sm bg-amber-50 dark:bg-amber-900/20 text-amber-700 dark:text-amber-300 rounded-lg hover:bg-amber-100 dark:hover:bg-amber-900/40 flex items-center gap-1 disabled:opacity-50">
									{#if loadingValue}
										<div class="w-3 h-3 border border-amber-600 border-t-transparent rounded-full animate-spin"></div>
									{:else}
										<Eye class="w-3 h-3" />
									{/if}
									Reveal Value
								</button>
							{:else}
								<button onclick={() => showValue = !showValue} class="px-2 py-1 text-xs border border-slate-200 dark:border-slate-700 rounded hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-1">
									{#if showValue}<EyeOff class="w-3 h-3" /> Hide{:else}<Eye class="w-3 h-3" /> Show{/if}
								</button>
								<button onclick={() => copyToClipboard(secretValue!)} class="px-2 py-1 text-xs border border-slate-200 dark:border-slate-700 rounded hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-1">
									<Copy class="w-3 h-3" /> Copy
								</button>
								<button onclick={startEditValue} class="px-2 py-1 text-xs border border-indigo-300 dark:border-indigo-600 text-indigo-700 dark:text-indigo-300 rounded hover:bg-indigo-50 dark:hover:bg-indigo-900/30 flex items-center gap-1">
									Edit
								</button>
							{/if}
						</div>
					</div>
					{#if editingValue}
						<div class="flex items-center gap-1 mb-2">
							<button type="button" onclick={() => switchEditMode('kv')} class="px-2 py-0.5 text-xs rounded {editValueMode === 'kv' ? 'bg-indigo-600 text-white' : 'text-slate-500 dark:text-slate-400 border border-slate-300 dark:border-slate-600'}">Key/Value</button>
							<button type="button" onclick={() => switchEditMode('text')} class="px-2 py-0.5 text-xs rounded {editValueMode === 'text' ? 'bg-indigo-600 text-white' : 'text-slate-500 dark:text-slate-400 border border-slate-300 dark:border-slate-600'}">Plaintext</button>
						</div>
						{#if editValueMode === 'kv'}
							<div class="space-y-2">
								{#each editKvRows as row, i}
									<div class="flex items-center gap-2">
										<input type="text" bind:value={row.key} placeholder="Key" class="flex-1 px-2 py-1.5 border border-slate-200 dark:border-slate-700 rounded bg-white dark:bg-slate-900 text-slate-900 dark:text-white font-mono text-xs" />
										<input type="text" bind:value={row.value} placeholder="Value" class="flex-1 px-2 py-1.5 border border-slate-200 dark:border-slate-700 rounded bg-white dark:bg-slate-900 text-slate-900 dark:text-white font-mono text-xs" />
										<button onclick={() => removeKvRow(i)} class="p-1 text-slate-400 hover:text-red-500"><X class="w-4 h-4" /></button>
									</div>
								{/each}
								<button type="button" onclick={addKvRow} class="text-xs text-indigo-600 dark:text-indigo-400 hover:underline flex items-center gap-1"><Plus class="w-3 h-3" /> Add Key</button>
							</div>
						{:else}
							<textarea bind:value={editValue} rows={5} class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white font-mono text-xs"></textarea>
						{/if}
						<div class="flex gap-2 mt-2">
							<button onclick={saveSecretValue} disabled={savingSecret} class="px-3 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-40">Save</button>
							<button onclick={() => editingValue = false} class="px-3 py-1.5 text-sm border border-slate-200 dark:border-slate-700 rounded text-slate-700 dark:text-slate-300">Cancel</button>
						</div>
					{:else if secretValue && showValue}
						<pre class="bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg p-3 text-xs font-mono text-slate-700 dark:text-slate-300 overflow-x-auto whitespace-pre-wrap break-all">{secretValue}</pre>
					{:else if secretValue}
						<div class="bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg p-3 text-sm text-slate-400">
							••••••••••••••••••••••
						</div>
					{:else}
						<div class="bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-lg p-3 text-sm text-slate-400 italic">
							Click "Reveal Value" to retrieve the secret
						</div>
					{/if}
				</div>

				<div class="flex flex-wrap gap-2 pt-2 border-t border-slate-200 dark:border-slate-700">
					<button onclick={rotateNow} class="px-3 py-1.5 bg-amber-600 text-white rounded text-sm hover:bg-amber-700 flex items-center gap-1">
						<RotateCw class="w-3 h-3" /> Rotate Now
					</button>
					{#if selectedSecret.RotationEnabled}
					<button onclick={cancelRotation} class="px-3 py-1.5 border border-amber-300 dark:border-amber-700 text-amber-700 dark:text-amber-300 rounded text-sm hover:bg-amber-50 dark:hover:bg-amber-900/20 flex items-center gap-1">
						<X class="w-3 h-3" /> Cancel Rotation
					</button>
					{/if}
					<button onclick={() => deleteSecret(selectedSecret?.Name ?? '', selectedSecret?.region)} class="px-3 py-1.5 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1 ml-auto">
						<Trash2 class="w-3 h-3" /> Delete Secret
					</button>
				</div>
			</div>
			{/if}

			{#if detailSubTab === 'versions'}
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-6">
				<div class="flex items-center justify-between mb-4">
					<h3 class="text-sm font-semibold text-slate-900 dark:text-white flex items-center gap-2"><History class="w-4 h-4" /> Version History</h3>
					<button onclick={() => loadVersions(selectedSecret?.Name!)} disabled={loadingVersions} class="px-3 py-1.5 text-sm border border-slate-200 dark:border-slate-700 rounded hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-1 disabled:opacity-40">
						<RefreshCw class="w-3 h-3" /> Refresh
					</button>
				</div>
				{#if loadingVersions}
					<div class="text-center py-8 text-slate-500"><div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500 mb-2"></div><p>Loading versions...</p></div>
				{:else if versions.length === 0}
					<p class="text-slate-500 dark:text-slate-400 text-sm italic text-center py-8">No versions found.</p>
				{:else}
					<div class="space-y-2">
						{#each versions as v}
							<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-900 rounded-lg border border-slate-200 dark:border-slate-700">
								<div>
									<p class="text-xs font-mono text-slate-700 dark:text-slate-300">{v.VersionId}</p>
									<p class="text-xs text-slate-500 mt-0.5">{v.CreatedDate ? formatDate(v.CreatedDate) : 'N/A'}</p>
								</div>
								<div class="flex gap-1 flex-wrap justify-end">
									{#each (v.VersionStages || []) as stage}
										<span class="px-2 py-0.5 text-xs rounded-full {stage === 'AWSCURRENT' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : stage === 'AWSPREVIOUS' ? 'bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300' : 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300'}">{stage}</span>
									{/each}
								</div>
							</div>
						{/each}
					</div>
				{/if}
			</div>
			{/if}

			{#if detailSubTab === 'tags'}
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-6">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white flex items-center gap-2 mb-4"><Tag class="w-4 h-4" /> Tags</h3>
				<div class="flex gap-2 mb-4">
					<input type="text" bind:value={tagKey} placeholder="Key" class="flex-1 px-3 py-1.5 text-sm border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
					<input type="text" bind:value={tagValue} placeholder="Value" class="flex-1 px-3 py-1.5 text-sm border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
					<button onclick={addTag} class="px-3 py-1.5 bg-indigo-600 text-white text-sm rounded-lg hover:bg-indigo-700">Add Tag</button>
				</div>
				{#if Object.keys(secretTags).length === 0}
					<p class="text-slate-500 dark:text-slate-400 text-sm italic">No tags</p>
				{:else}
					<div class="space-y-2">
						{#each Object.entries(secretTags) as [k, v]}
							<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-900 rounded border border-slate-200 dark:border-slate-700">
								<div class="flex gap-2 text-sm">
									<span class="font-medium text-slate-700 dark:text-slate-300">{k}</span>
									<span class="text-slate-500">=</span>
									<span class="text-slate-600 dark:text-slate-400">{v}</span>
								</div>
								<button onclick={() => removeTag(k)} class="p-1 hover:text-red-600 text-slate-400 rounded">
									<X class="w-3.5 h-3.5" />
								</button>
							</div>
						{/each}
					</div>
				{/if}
			</div>
			{/if}

			{#if detailSubTab === 'replication'}
			<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl p-6">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-4">Replication Status</h3>
				{#if !selectedSecret.ReplicationStatus || selectedSecret.ReplicationStatus.length === 0}
					<p class="text-slate-500 dark:text-slate-400 text-sm italic">No replica regions configured.</p>
				{:else}
					<div class="space-y-2">
						{#each selectedSecret.ReplicationStatus as rep}
							<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-900 rounded-lg border border-slate-200 dark:border-slate-700">
								<div>
									<p class="text-sm font-medium text-slate-700 dark:text-slate-300">{rep.Region}</p>
									{#if rep.StatusMessage}
										<p class="text-xs text-slate-500 mt-0.5">{rep.StatusMessage}</p>
									{/if}
								</div>
								<span class="px-2 py-0.5 text-xs rounded-full {rep.Status === 'InSync' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : rep.Status === 'Failed' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300' : 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300'}">{rep.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			</div>
			{/if}
		</div>
	{/if}
</div>

{#if showCreateModal}
	<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl w-full max-w-md p-6">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Secret</h2>
			<div class="space-y-4">
				<div>
					<label for="secret-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Secret Name</label>
					<input id="secret-name" type="text" bind:value={newSecretName} placeholder="e.g. prod/db/password" required
						class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
				</div>
				<div>
					<label for="secret-description" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description</label>
					<input id="secret-description" type="text" bind:value={newSecretDescription} placeholder="Optional description"
						class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
				</div>
				<div>
					<label for="secret-value" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Secret Value</label>
					<textarea id="secret-value" bind:value={newSecretValue} rows={4} placeholder="&#123;&quot;username&quot;: &quot;admin&quot;, &quot;password&quot;: &quot;secret&quot;&#125;"
						class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white font-mono text-sm"></textarea>
				</div>
				<div>
					<label for="secret-kms-key" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">KMS Key ID (optional)</label>
					<input id="secret-kms-key" type="text" bind:value={newSecretKmsKey} placeholder="alias/my-key or key ARN"
						class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
				</div>
			</div>
			<div class="flex justify-end gap-3 mt-6">
				<button onclick={() => { showCreateModal = false; newSecretName = ''; newSecretValue = ''; newSecretDescription = ''; newSecretKmsKey = ''; }} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-700 dark:text-slate-300">Cancel</button>
				<button onclick={createSecret} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700">Create</button>
			</div>
		</div>
	</div>
{/if}
