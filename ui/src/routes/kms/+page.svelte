<script lang="ts">
import { onMount } from 'svelte';
import { getKMSClient } from '$lib/aws-client';
import { ListKeysCommand, DescribeKeyCommand, DisableKeyCommand, EnableKeyCommand, ListAliasesCommand, CreateKeyCommand, EncryptCommand, DecryptCommand, type AliasListEntry } from '@aws-sdk/client-kms';
import { toast } from 'svelte-sonner';
import { Key, RefreshCw, Search, Lock, Unlock, Copy, Tag, Plus, Send } from 'lucide-svelte';

const kms = getKMSClient();

type KMSKey = {
	CreationDate?: Date | string;
	DeletionDate?: Date | string;
	Description?: string;
	KeyArn?: string;
	KeyId?: string;
	KeyManager?: string;
	KeyState?: string;
	KeyUsage?: string;
	CustomerMasterKeySpec?: string;
	KeySpec?: string;
};

let keys = $state<KMSKey[]>([]);
let aliases = $state<AliasListEntry[]>([]);
let loading = $state(true);
let search = $state('');
let stateFilter = $state('all');
let usageFilter = $state('all');
let selectedKey = $state<KMSKey | null>(null);
let activeTab = $state<'keys' | 'aliases'>('keys');
let aliasSearch = $state('');
let showCreateModal = $state(false);
let creatingKey = $state(false);
let newKeyDescription = $state('');

let showCryptoModal = $state(false);
let cryptoKeyId = $state('');
let plaintext = $state('');
let ciphertext = $state('');
let decryptedText = $state('');
let encrypting = $state(false);
let decrypting = $state(false);

onMount(async () => { await loadKeys(); });

async function loadKeys() {
	try {
		loading = true;
		const listData = await kms.send(new ListKeysCommand({}));
		const rawKeys = listData.Keys || [];
		const details = await Promise.all(
			rawKeys.slice(0, 50).map(async (k) => {
				try {
					const d = await kms.send(new DescribeKeyCommand({ KeyId: k.KeyId }));
					return d.KeyMetadata;
				} catch {
					return { KeyId: k.KeyId, KeyArn: k.KeyArn, Description: '', KeyState: 'Unknown', KeyUsage: 'Unknown' };
				}
			})
		);
		keys = details.filter(Boolean) as KMSKey[];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load keys');
	} finally {
		loading = false;
	}
}

async function loadAliases() {
	try {
		loading = true;
		const data = await kms.send(new ListAliasesCommand({ Limit: 100 }));
		aliases = data.Aliases || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load aliases');
	} finally {
		loading = false;
	}
}

async function selectTab(t: typeof activeTab) {
	activeTab = t;
	if (t === 'keys' && keys.length === 0) await loadKeys();
	else if (t === 'aliases') await loadAliases();
}

async function refresh() {
	if (activeTab === 'keys') { keys = []; await loadKeys(); }
	else { aliases = []; await loadAliases(); }
}

async function copyId(id: string) {
	await navigator.clipboard.writeText(id);
	toast.success('Copied to clipboard');
}

async function disableKey(keyId: string) {
	try {
		await kms.send(new DisableKeyCommand({ KeyId: keyId }));
		toast.success(`Key ${keyId} disabled`);
		await loadKeys();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to disable key');
	}
}

async function enableKey(keyId: string) {
	try {
		await kms.send(new EnableKeyCommand({ KeyId: keyId }));
		toast.success(`Key ${keyId} enabled`);
		await loadKeys();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to enable key');
	}
}

async function createKey() {
	try {
		creatingKey = true;
		await kms.send(new CreateKeyCommand({ Description: newKeyDescription }));
		toast.success('Key created');
		showCreateModal = false;
		newKeyDescription = '';
		await loadKeys();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to create key');
	} finally {
		creatingKey = false;
	}
}

async function encrypt() {
	if (!plaintext) return;
	try {
		encrypting = true;
		const res = await kms.send(new EncryptCommand({ KeyId: cryptoKeyId, Plaintext: new TextEncoder().encode(plaintext) }));
		if (res.CiphertextBlob) {
			ciphertext = btoa(String.fromCodePoint(...res.CiphertextBlob));
		}
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Encryption failed');
	} finally {
		encrypting = false;
	}
}

async function decrypt() {
	if (!ciphertext) return;
	try {
		decrypting = true;
		const res = await kms.send(new DecryptCommand({ CiphertextBlob: Uint8Array.from(atob(ciphertext), c => c.codePointAt(0)!) }));
		if (res.Plaintext) {
			decryptedText = new TextDecoder().decode(res.Plaintext);
		}
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Decryption failed');
	} finally {
		decrypting = false;
	}
}

function openCrypto(keyId: string) {
	cryptoKeyId = keyId;
	plaintext = '';
	ciphertext = '';
	decryptedText = '';
	showCryptoModal = true;
}

function getStateColor(state: string) {
	if (state === 'Enabled') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
	if (state === 'Disabled') return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	if (state === 'PendingDeletion') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
	return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
}

function getKeySpec(key: KMSKey) {
	return key.CustomerMasterKeySpec || key.KeySpec || 'SYMMETRIC_DEFAULT';
}

function getKeyManager(key: KMSKey) {
	return key.KeyManager || 'CUSTOMER';
}

let enabledCount = $derived(keys.filter(k => k.KeyState === 'Enabled').length);
let disabledCount = $derived(keys.filter(k => k.KeyState === 'Disabled').length);
let pendingCount = $derived(keys.filter(k => k.KeyState === 'PendingDeletion').length);
let symmetricCount = $derived(keys.filter(k => (k.CustomerMasterKeySpec || k.KeySpec || '').includes('SYMMETRIC')).length);

let uniqueUsages = $derived([...new Set(keys.map(k => k.KeyUsage).filter((u): u is string => u !== null))]);

let filtered = $derived(keys.filter(k => {
	const desc = (k.Description || '').toLowerCase();
	const id = (k.KeyId || '').toLowerCase();
	const matchSearch = !search || desc.includes(search.toLowerCase()) || id.includes(search.toLowerCase());
	const matchState = stateFilter === 'all' || k.KeyState === stateFilter;
	const matchUsage = usageFilter === 'all' || k.KeyUsage === usageFilter;
	return matchSearch && matchState && matchUsage;
}));

let filteredAliases = $derived(aliases.filter(a =>
	!aliasSearch || (a.AliasName ?? '').toLowerCase().includes(aliasSearch.toLowerCase())
));

let awsAliasCount = $derived(aliases.filter(a => (a.AliasName ?? '').startsWith('alias/aws/')).length);
let customerAliasCount = $derived(aliases.filter(a => !(a.AliasName ?? '').startsWith('alias/aws/')).length);
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
				<Key class="w-6 h-6 text-amber-600 dark:text-amber-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">KMS Keys</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm">Key Management Service</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button onclick={refresh} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700" title="Refresh">
				<RefreshCw class="w-4 h-4" />
			</button>
			<button id="create-key-btn" onclick={() => showCreateModal = true} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 flex items-center gap-2">
				<Plus class="w-4 h-4" /> Create Key
			</button>
		</div>
	</div>

	<!-- Stats -->
	{#if !loading}
	<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
		{#each [
			{ label: 'Enabled', count: enabledCount, color: 'text-green-500' },
			{ label: 'Disabled', count: disabledCount, color: 'text-slate-400' },
			{ label: 'Pending Deletion', count: pendingCount, color: 'text-red-500' },
			{ label: 'Symmetric', count: symmetricCount, color: 'text-amber-500' }
		] as stat}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-2xl font-bold {stat.color}">{stat.count}</p>
				<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{stat.label}</p>
			</div>
		{/each}
	</div>
	{/if}

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<nav class="flex gap-1 -mb-px">
			<button onclick={() => selectTab('keys')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'keys' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Key class="w-4 h-4" /> Keys {#if keys.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{keys.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('aliases')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'aliases' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Tag class="w-4 h-4" /> Aliases {#if aliases.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{aliases.length}</span>{/if}
			</button>
		</nav>
	</div>

	<!-- Keys Tab -->
	{#if activeTab === 'keys'}
	<!-- Filters -->
	<div class="flex flex-wrap gap-3">
		<div class="relative flex-1 min-w-48">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" placeholder="Search keys by description or ID..." bind:value={search}
				class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
		</div>
		<select bind:value={stateFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
			<option value="all">All States</option>
			<option value="Enabled">Enabled</option>
			<option value="Disabled">Disabled</option>
			<option value="PendingDeletion">Pending Deletion</option>
		</select>
		<select bind:value={usageFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
			<option value="all">All Usages</option>
			{#each uniqueUsages as u}<option value={u}>{u.replace(/_/g, ' ')}</option>{/each}
		</select>
	</div>

	<!-- List + detail panel -->
	<div class="flex gap-4">
		<div class="flex-1 min-w-0">
			{#if loading}
				<div class="text-center py-12 text-slate-500">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-amber-500 mb-3"></div>
					<p>Loading keys...</p>
				</div>
			{:else if filtered.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Key class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
					<p class="text-slate-500 dark:text-slate-400 font-medium">No KMS keys found</p>
				</div>
			{:else}
				<div class="space-y-3">
					{#each filtered as key}
						<div
							class="bg-white dark:bg-slate-800 rounded-xl border cursor-pointer transition-all
								{selectedKey?.KeyId === key.KeyId
									? 'border-amber-400 dark:border-amber-500 ring-1 ring-amber-400/30'
									: 'border-slate-200 dark:border-slate-700 hover:border-slate-300 dark:hover:border-slate-600'}
								p-5"
							onclick={() => selectedKey = selectedKey?.KeyId === key.KeyId ? null : key}
							role="button"
							tabindex="0"
							onkeydown={(e) => e.key === 'Enter' && (selectedKey = selectedKey?.KeyId === key.KeyId ? null : key)}
						>
							<div class="flex items-start justify-between mb-3">
								<div class="min-w-0 flex-1">
									<h3 class="font-semibold text-slate-900 dark:text-white truncate">
										{key.Description || '(no description)'}
									</h3>
									<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mt-0.5 truncate">{key.KeyId}</p>
								</div>
								<span class={`ml-3 px-3 py-1 rounded-full text-xs font-medium shrink-0 ${getStateColor(key.KeyState ?? '')}`}>{key.KeyState}</span>
							</div>
							<div class="grid grid-cols-3 gap-3 text-sm mb-4">
								<div>
									<p class="text-xs text-slate-400 uppercase tracking-wide">Usage</p>
									<p class="font-medium text-slate-900 dark:text-white text-xs mt-0.5">{key.KeyUsage}</p>
								</div>
								<div>
									<p class="text-xs text-slate-400 uppercase tracking-wide">Spec</p>
									<p class="font-medium text-slate-900 dark:text-white text-xs mt-0.5">{getKeySpec(key)}</p>
								</div>
								<div>
									<p class="text-xs text-slate-400 uppercase tracking-wide">Manager</p>
									<p class="font-medium text-slate-900 dark:text-white text-xs mt-0.5">{getKeyManager(key)}</p>
								</div>
							</div>
							<div class="flex gap-2">
								{#if key.KeyState === 'Enabled'}
									<button
										onclick={(e) => { e.stopPropagation(); disableKey(key.KeyId ?? ''); }}
										class="px-3 py-1.5 bg-yellow-600 text-white rounded-lg text-xs hover:bg-yellow-700 flex items-center gap-1"
									>
										<Lock class="w-3 h-3" /> Disable
									</button>
								{:else if key.KeyState === 'Disabled'}
									<button
										onclick={(e) => { e.stopPropagation(); enableKey(key.KeyId ?? ''); }}
										class="px-3 py-1.5 bg-green-600 text-white rounded-lg text-xs hover:bg-green-700 flex items-center gap-1"
									>
										<Unlock class="w-3 h-3" /> Enable
									</button>
								{/if}
								<button
									id="crypto-btn-{key.KeyId}"
									onclick={(e) => { e.stopPropagation(); openCrypto(key.KeyId ?? ''); }}
									class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-xs hover:bg-indigo-700 flex items-center gap-1"
								>
									<Lock class="w-3 h-3" /> Crypto
								</button>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>

		<!-- Detail panel -->
		{#if selectedKey}
			<div class="w-80 shrink-0">
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5 space-y-4 sticky top-4">
					<div class="flex items-start justify-between">
						<h3 class="font-bold text-slate-900 dark:text-white">Key Details</h3>
						<button onclick={() => selectedKey = null} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 text-xl leading-none">&times;</button>
					</div>
					<div class="space-y-3 text-sm">
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Key ID</p>
							<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all">{selectedKey.KeyId}</p>
						</div>
						{#if selectedKey.KeyArn}
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">ARN</p>
							<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all">{selectedKey.KeyArn}</p>
						</div>
						{/if}
						<div class="grid grid-cols-2 gap-3">
							<div>
								<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">State</p>
								<span class={`px-2 py-0.5 rounded text-xs font-medium ${getStateColor(selectedKey.KeyState ?? '')}`}>{selectedKey.KeyState}</span>
							</div>
							<div>
								<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Manager</p>
								<p class="text-slate-700 dark:text-slate-300 text-xs">{getKeyManager(selectedKey)}</p>
							</div>
						</div>
						{#if selectedKey.CreationDate}
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Created</p>
							<p class="text-slate-700 dark:text-slate-300 text-xs">{new Date(selectedKey.CreationDate).toLocaleString()}</p>
						</div>
						{/if}
						{#if selectedKey.DeletionDate}
						<div>
							<p class="text-xs font-medium text-red-500 mb-1">Scheduled Deletion</p>
							<p class="text-red-600 dark:text-red-400 text-xs">{new Date(selectedKey.DeletionDate).toLocaleString()}</p>
						</div>
						{/if}
					</div>
				</div>
			</div>
		{/if}
	</div>

	<!-- End Keys Tab -->
	{/if}

	<!-- Aliases Tab -->
	{#if activeTab === 'aliases'}
		<div>
			<div class="flex items-center gap-3 mb-4">
				<div class="relative flex-1">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
					<input type="text" placeholder="Search aliases..." bind:value={aliasSearch}
						class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
				</div>
				<div class="flex gap-2 shrink-0 text-sm">
					<span class="px-2.5 py-1 rounded-full bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300">{customerAliasCount} customer</span>
					<span class="px-2.5 py-1 rounded-full bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300">{awsAliasCount} AWS</span>
				</div>
			</div>
			{#if loading}
				<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-amber-500"></div></div>
			{:else if filteredAliases.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Tag class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
					<p class="text-slate-500 dark:text-slate-400">No aliases found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-slate-50 dark:bg-slate-700">
							<tr>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Alias Name</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Target Key ID</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Type</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Copy</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each filteredAliases as alias}
								{@const isAws = (alias.AliasName || '').startsWith('alias/aws/')}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-mono text-xs text-slate-900 dark:text-white">{alias.AliasName}</td>
									<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{alias.TargetKeyId || '—'}</td>
									<td class="px-4 py-3">
										<span class="px-2 py-0.5 text-xs rounded-full {isAws ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400' : 'bg-green-100 dark:bg-green-900/30 text-green-600 dark:text-green-400'}">
											{isAws ? 'AWS Managed' : 'Customer'}
										</span>
									</td>
									<td class="px-4 py-3">
										{#if alias.TargetKeyId}
											<button onclick={() => copyId(alias.TargetKeyId ?? '')} class="p-1 text-slate-400 hover:text-slate-600" title="Copy Key ID">
												<Copy class="w-3.5 h-3.5" />
											</button>
										{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create KMS Key</h2>
			<form onsubmit={(e) => { e.preventDefault(); createKey(); }} class="space-y-4">
				<div>
					<label for="new-key-description" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description</label>
					<input id="new-key-description" name="description" type="text" bind:value={newKeyDescription} placeholder="e.g. Test Key" class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => showCreateModal = false} class="px-4 py-2 text-slate-500">Cancel</button>
					<button type="submit" disabled={creatingKey} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700">
						{creatingKey ? 'Creating...' : 'Create'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Crypto Modal -->
{#if showCryptoModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Encrypt / Decrypt</h2>
			<div class="space-y-6">
				<!-- Encrypt -->
				<div class="space-y-2">
					<label for="plaintext-input" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Plaintext</label>
					<div class="flex gap-2">
						<input id="plaintext-input" name="plaintext" type="text" bind:value={plaintext} class="flex-1 px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
						<button id="encrypt-submit" onclick={encrypt} disabled={encrypting} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
							<Lock class="w-4 h-4" /> Encrypt
						</button>
					</div>
					{#if ciphertext}
						<div class="p-3 bg-slate-100 dark:bg-slate-900 rounded-lg">
							<p class="text-xs text-slate-400 mb-1 leading-none uppercase tracking-wider font-semibold">Ciphertext (Base64)</p>
							<p id="encrypt-result" class="text-xs font-mono break-all text-slate-700 dark:text-slate-300">{ciphertext}</p>
						</div>
					{/if}
				</div>

				<!-- Decrypt -->
				<div class="space-y-2">
					<label for="ciphertext-input" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Ciphertext (Base64)</label>
					<div class="flex gap-2">
						<input id="ciphertext-input" name="ciphertext" type="text" bind:value={ciphertext} class="flex-1 px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
						<button id="decrypt-submit" onclick={decrypt} disabled={decrypting} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2">
							<Unlock class="w-4 h-4" /> Decrypt
						</button>
					</div>
					{#if decryptedText}
						<div class="p-3 bg-slate-100 dark:bg-slate-900 rounded-lg">
							<p class="text-xs text-slate-400 mb-1 leading-none uppercase tracking-wider font-semibold">Decrypted Text</p>
							<p id="decrypt-result" class="text-sm font-medium text-slate-900 dark:text-white">{decryptedText}</p>
						</div>
					{/if}
				</div>
			</div>
			<div class="flex justify-end mt-6">
				<button onclick={() => showCryptoModal = false} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 rounded-lg">Close</button>
			</div>
		</div>
	</div>
{/if}

