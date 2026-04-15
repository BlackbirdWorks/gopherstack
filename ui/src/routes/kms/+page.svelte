<script lang="ts">
import { onMount } from 'svelte';
import { getKMSClient } from '$lib/aws-client';
import { ListKeysCommand, DescribeKeyCommand, DisableKeyCommand, EnableKeyCommand } from '@aws-sdk/client-kms';
import { toast } from 'svelte-sonner';
import { Key, Plus, RefreshCw, Search, Lock, Unlock, Shield, Activity } from 'lucide-svelte';

const kms = getKMSClient();

let keys = $state<any[]>([]);
let loading = $state(true);
let search = $state('');
let stateFilter = $state('all');
let usageFilter = $state('all');
let selectedKey = $state<any | null>(null);

onMount(async () => { await loadKeys(); });

async function loadKeys() {
	try {
		loading = true;
		const listData = await kms.send(new ListKeysCommand({}));
		const rawKeys = listData.Keys || [];
		const details = await Promise.all(
			rawKeys.slice(0, 50).map(async (k: any) => {
				try {
					const d = await kms.send(new DescribeKeyCommand({ KeyId: k.KeyId }));
					return d.KeyMetadata;
				} catch {
					return { KeyId: k.KeyId, KeyArn: k.KeyArn, Description: '', KeyState: 'Unknown', KeyUsage: 'Unknown' };
				}
			})
		);
		keys = details.filter(Boolean);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load keys');
	} finally {
		loading = false;
	}
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

function getStateColor(state: string) {
	if (state === 'Enabled') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
	if (state === 'Disabled') return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	if (state === 'PendingDeletion') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
	return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
}

function getKeySpec(key: any) {
	return key.CustomerMasterKeySpec || key.KeySpec || 'SYMMETRIC_DEFAULT';
}

function getKeyManager(key: any) {
	return key.KeyManager || 'CUSTOMER';
}

let enabledCount = $derived(keys.filter(k => k.KeyState === 'Enabled').length);
let disabledCount = $derived(keys.filter(k => k.KeyState === 'Disabled').length);
let pendingCount = $derived(keys.filter(k => k.KeyState === 'PendingDeletion').length);
let symmetricCount = $derived(keys.filter(k => (k.CustomerMasterKeySpec || k.KeySpec || '').includes('SYMMETRIC')).length);

let uniqueUsages = $derived([...new Set(keys.map(k => k.KeyUsage).filter(Boolean))]);

let filtered = $derived(keys.filter(k => {
	const desc = (k.Description || '').toLowerCase();
	const id = (k.KeyId || '').toLowerCase();
	const matchSearch = !search || desc.includes(search.toLowerCase()) || id.includes(search.toLowerCase());
	const matchState = stateFilter === 'all' || k.KeyState === stateFilter;
	const matchUsage = usageFilter === 'all' || k.KeyUsage === usageFilter;
	return matchSearch && matchState && matchUsage;
}));
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
			<button onclick={loadKeys} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
				<RefreshCw class="w-4 h-4" />
			</button>
			<button onclick={() => toast.info('Create key via AWS console')} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Create Key
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
				<p class="text-2xl font-bold text-slate-900 dark:text-white {stat.color}">{stat.count}</p>
				<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{stat.label}</p>
			</div>
		{/each}
	</div>
	{/if}

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
								<span class={`ml-3 px-3 py-1 rounded-full text-xs font-medium shrink-0 ${getStateColor(key.KeyState)}`}>{key.KeyState}</span>
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
										onclick={(e) => { e.stopPropagation(); disableKey(key.KeyId); }}
										class="px-3 py-1.5 bg-yellow-600 text-white rounded-lg text-xs hover:bg-yellow-700 flex items-center gap-1"
									>
										<Lock class="w-3 h-3" /> Disable
									</button>
								{:else if key.KeyState === 'Disabled'}
									<button
										onclick={(e) => { e.stopPropagation(); enableKey(key.KeyId); }}
										class="px-3 py-1.5 bg-green-600 text-white rounded-lg text-xs hover:bg-green-700 flex items-center gap-1"
									>
										<Unlock class="w-3 h-3" /> Enable
									</button>
								{/if}
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
								<span class={`px-2 py-0.5 rounded text-xs font-medium ${getStateColor(selectedKey.KeyState)}`}>{selectedKey.KeyState}</span>
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
</div>

