<script lang="ts">
	import { onMount } from 'svelte';
	import { getVerifiedPermissionsClient } from '$lib/aws-client';
	import {
		ListPolicyStoresCommand,
		ListPoliciesCommand,
		ListIdentitySourcesCommand,
		type PolicyStoreItem,
		type PolicyItem,
		type IdentitySourceItem
	} from '@aws-sdk/client-verifiedpermissions';
	import { toast } from 'svelte-sonner';
	import { Shield, RefreshCw, Search, Lock, FileText, Users } from 'lucide-svelte';

	const vp = getVerifiedPermissionsClient();

	let loading = $state(false);
	let activeTab = $state<'stores' | 'policies' | 'identity'>('stores');
	let searchQuery = $state('');
	let stores = $state<PolicyStoreItem[]>([]);
	let policies = $state<PolicyItem[]>([]);
	let identitySources = $state<IdentitySourceItem[]>([]);
	let selectedStoreId = $state<string | null>(null);

	const filteredStores = $derived(stores.filter((s) => (s.policyStoreId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredPolicies = $derived(policies.filter((p) => (p.policyId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const resp = await vp.send(new ListPolicyStoresCommand({}));
			stores = resp.policyStores ?? [];
		} catch (e) {
			toast.error('Failed to load Verified Permissions data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadPolicies(storeId: string) {
		selectedStoreId = storeId;
		try {
			const [polResp, idResp] = await Promise.all([
				vp.send(new ListPoliciesCommand({ policyStoreId: storeId })),
				vp.send(new ListIdentitySourcesCommand({ policyStoreId: storeId }))
			]);
			policies = polResp.policies ?? [];
			identitySources = idResp.identitySources ?? [];
			activeTab = 'policies';
		} catch (e) {
			toast.error('Failed to load policies: ' + String(e));
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="w-7 h-7 text-green-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Verified Permissions</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fine-grained authorization and permissions management</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Lock class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{stores.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Policy Stores</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><FileText class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{policies.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Policies</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Users class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{identitySources.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Identity Sources</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['stores', 'Policy Stores'], ['policies', 'Policies'], ['identity', 'Identity Sources']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-green-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'stores'}
				{#if filteredStores.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No policy stores found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredStores as store}
							<button onclick={() => loadPolicies(store.policyStoreId ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<Lock class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{store.policyStoreId}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{store.arn}</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">Click to view policies →</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'policies'}
				{#if filteredPolicies.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No policies found. Select a policy store to view policies.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPolicies as policy}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<FileText class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{policy.policyId}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{policy.policyType}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'identity'}
				{#if identitySources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No identity sources found. Select a policy store to view identity sources.</div>
				{:else}
					<div class="space-y-2">
						{#each identitySources as src}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Users class="w-5 h-5 text-purple-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{src.identitySourceId}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{src.principalEntityType}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
