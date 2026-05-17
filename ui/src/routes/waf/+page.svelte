<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getWAFV2Client } from '$lib/aws-client';
	import {
		ListWebACLsCommand,
		GetWebACLCommand,
		CreateWebACLCommand,
		DeleteWebACLCommand,
		ListIPSetsCommand,
		ListRuleGroupsCommand,
		GetSampledRequestsCommand,
		type WebACLSummary,
		type WebACL,
		type IPSetSummary,
		type RuleGroupSummary,
		type SampledHTTPRequest
	} from '@aws-sdk/client-wafv2';
	import { toast } from 'svelte-sonner';
	import { Shield, Search, RefreshCw, Plus, Trash2, ChevronRight, Activity, Globe } from 'lucide-svelte';

	const waf = getWAFV2Client();

	let activeTab = $state<'webacls' | 'ipsets' | 'rulegroups'>('webacls');
	let searchQuery = $state('');
	let loading = $state(false);

	// Web ACLs
	let webAcls = $state<WebACLSummary[]>([]);
	let selectedWebACL = $state<WebACL | null>(null);
	let loadingDetail = $state(false);
	let sampledRequests = $state<SampledHTTPRequest[]>([]);
	let loadingSamples = $state(false);

	// IP Sets
	let ipSets = $state<IPSetSummary[]>([]);
	let loadingIPSets = $state(false);

	// Rule Groups
	let ruleGroups = $state<RuleGroupSummary[]>([]);
	let loadingRuleGroups = $state(false);

	// Scope selector
	let scope = $state<'REGIONAL' | 'CLOUDFRONT'>('REGIONAL');

	// Create WebACL
	let showCreateACL = $state(false);
	let creatingACL = $state(false);
	let newAclName = $state('');
	let newDefaultAction = $state<'ALLOW' | 'BLOCK'>('ALLOW');

	const filteredACLs = $derived(
		webAcls.filter((a) => !searchQuery || (a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadWebACLs() {
		loading = true;
		try {
			const resp = await waf.send(new ListWebACLsCommand({ Scope: scope, Limit: 50 }));
			webAcls = resp.WebACLs ?? [];
		} catch (e) {
			toast.error('Failed to load Web ACLs: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectWebACL(summary: WebACLSummary) {
		loadingDetail = true;
		sampledRequests = [];
		try {
			const resp = await waf.send(new GetWebACLCommand({
				Name: summary.Name ?? '',
				Scope: scope,
				Id: summary.Id ?? ''
			}));
			selectedWebACL = resp.WebACL ?? null;
		} catch (e) {
			toast.error('Failed to load Web ACL details: ' + String(e));
		} finally {
			loadingDetail = false;
		}
	}

	async function loadSampledRequests() {
		if (!selectedWebACL) return;
		loadingSamples = true;
		try {
			const resp = await waf.send(new GetSampledRequestsCommand({
				WebAclArn: selectedWebACL.ARN ?? '',
				RuleMetricName: selectedWebACL.Rules?.[0]?.Name ?? 'ALL',
				Scope: scope,
				TimeWindow: {
					StartTime: new Date(Date.now() - 3600000),
					EndTime: new Date()
				},
				MaxItems: 20
			}));
			sampledRequests = resp.SampledRequests ?? [];
		} catch (e) {
			toast.error('Failed to load sampled requests: ' + String(e));
		} finally {
			loadingSamples = false;
		}
	}

	async function createWebACL() {
		if (!newAclName.trim()) return;
		creatingACL = true;
		try {
			await waf.send(new CreateWebACLCommand({
				Name: newAclName.trim(),
				Scope: scope,
				DefaultAction: newDefaultAction === 'ALLOW' ? { Allow: {} } : { Block: {} },
				VisibilityConfig: {
					SampledRequestsEnabled: true,
					CloudWatchMetricsEnabled: true,
					MetricName: newAclName.trim().replaceAll(/[^a-zA-Z0-9]/g, '')
				},
				Rules: [],
				TokenDomains: []
			}));
			toast.success(`Web ACL "${newAclName}" created`);
			showCreateACL = false;
			newAclName = '';
			newDefaultAction = 'ALLOW';
			await loadWebACLs();
		} catch (e) {
			toast.error('Failed to create Web ACL: ' + String(e));
		} finally {
			creatingACL = false;
		}
	}

	async function deleteWebACL(summary: WebACLSummary) {
		if (!await confirmDestructive({ title: 'Delete Web ACL', message: `Delete Web ACL "${summary.Name}"? Any associated resources will lose their WAF protection.` })) return;
		try {
			await waf.send(new DeleteWebACLCommand({
				Name: summary.Name ?? '',
				Scope: scope,
				Id: summary.Id ?? '',
				LockToken: summary.LockToken ?? ''
			}));
			toast.success(`Web ACL "${summary.Name}" deleted`);
			if (selectedWebACL?.Id === summary.Id) selectedWebACL = null;
			await loadWebACLs();
		} catch (e) {
			toast.error('Failed to delete Web ACL: ' + String(e));
		}
	}

	async function loadIPSets() {
		loadingIPSets = true;
		try {
			const resp = await waf.send(new ListIPSetsCommand({ Scope: scope, Limit: 50 }));
			ipSets = resp.IPSets ?? [];
		} catch (e) {
			toast.error('Failed to load IP sets: ' + String(e));
		} finally {
			loadingIPSets = false;
		}
	}

	async function loadRuleGroups() {
		loadingRuleGroups = true;
		try {
			const resp = await waf.send(new ListRuleGroupsCommand({ Scope: scope, Limit: 50 }));
			ruleGroups = resp.RuleGroups ?? [];
		} catch (e) {
			toast.error('Failed to load rule groups: ' + String(e));
		} finally {
			loadingRuleGroups = false;
		}
	}

	async function handleScopeChange() {
		selectedWebACL = null;
		webAcls = [];
		ipSets = [];
		ruleGroups = [];
		sampledRequests = [];
		await loadWebACLs();
	}

	async function handleTabChange(tab: 'webacls' | 'ipsets' | 'rulegroups') {
		activeTab = tab;
		selectedWebACL = null;
		if (tab === 'ipsets' && ipSets.length === 0) await loadIPSets();
		if (tab === 'rulegroups' && ruleGroups.length === 0) await loadRuleGroups();
	}

	onMount(loadWebACLs);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="w-7 h-7 text-red-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS WAF v2</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Web application firewall</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<!-- Scope selector -->
			<div class="flex items-center gap-2">
				<label for="scope-select" class="text-sm text-gray-600 dark:text-gray-400">Scope:</label>
				<select id="scope-select" bind:value={scope} onchange={handleScopeChange} class="px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm">
					<option value="REGIONAL">Regional</option>
					<option value="CLOUDFRONT">CloudFront (Global)</option>
				</select>
			</div>
			<button onclick={loadWebACLs} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateACL = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Web ACL
			</button>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
		{#each [['webacls', 'Web ACLs'], ['ipsets', 'IP Sets'], ['rulegroups', 'Rule Groups']] as [tab, label]}
			<button
				onclick={() => handleTabChange(tab as 'webacls' | 'ipsets' | 'rulegroups')}
				class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-red-500 text-red-600 dark:text-red-400' : 'border-transparent text-gray-500 hover:text-gray-700'}`}
			>
				{label}
			</button>
		{/each}
	</div>

	{#if activeTab === 'webacls'}
		{#if selectedWebACL}
			<!-- ACL Detail -->
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2 text-sm">
					<button onclick={() => { selectedWebACL = null; sampledRequests = []; }} class="text-red-600 hover:underline">Web ACLs</button>
					<ChevronRight class="w-4 h-4 text-gray-400" />
					<span class="font-medium">{selectedWebACL.Name}</span>
					<span class={`px-2 py-0.5 rounded text-xs font-medium ${Object.keys(selectedWebACL.DefaultAction ?? {})[0] === 'Allow' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
						Default: {Object.keys(selectedWebACL.DefaultAction ?? {})[0]?.toUpperCase()}
					</span>
				</div>
				<button onclick={loadSampledRequests} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50">
					<Activity class="w-4 h-4" /> Sample Requests
				</button>
			</div>

			<!-- Stats -->
			<div class="grid grid-cols-3 gap-4">
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500">Rules</div>
					<div class="text-2xl font-bold text-gray-900 dark:text-white mt-1">{(selectedWebACL.Rules ?? []).length}</div>
				</div>
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500">Capacity</div>
					<div class="text-2xl font-bold text-gray-900 dark:text-white mt-1">{selectedWebACL.Capacity ?? 0}</div>
				</div>
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500">Managed Rules</div>
					<div class="text-2xl font-bold text-gray-900 dark:text-white mt-1">
						{(selectedWebACL.Rules ?? []).filter((r) => r.Statement?.ManagedRuleGroupStatement).length}
					</div>
				</div>
			</div>

			<!-- Rules -->
			{#if (selectedWebACL.Rules ?? []).length > 0}
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Rules</h3>
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Rule Name</th>
									<th class="px-4 py-3 text-left">Priority</th>
									<th class="px-4 py-3 text-left">Action</th>
									<th class="px-4 py-3 text-left">Type</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each (selectedWebACL.Rules ?? []).sort((a, b) => (a.Priority ?? 0) - (b.Priority ?? 0)) as rule}
									<tr>
										<td class="px-4 py-3 font-medium">{rule.Name}</td>
										<td class="px-4 py-3 text-gray-600">{rule.Priority}</td>
										<td class="px-4 py-3">
											<span class={`px-2 py-0.5 rounded text-xs font-medium ${Object.keys(rule.Action ?? rule.OverrideAction ?? {})[0] === 'Allow' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>
												{Object.keys(rule.Action ?? rule.OverrideAction ?? {})[0]?.toUpperCase() ?? 'OVERRIDE'}
											</span>
										</td>
										<td class="px-4 py-3 text-xs text-gray-500">
											{rule.Statement?.ManagedRuleGroupStatement ? 'Managed Rule Group' :
											rule.Statement?.IPSetReferenceStatement ? 'IP Set' :
											rule.Statement?.ByteMatchStatement ? 'Byte Match' : 'Custom'}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}

			<!-- Sampled Requests -->
			{#if loadingSamples}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div></div>
			{:else if sampledRequests.length > 0}
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Sampled Requests (Last Hour)</h3>
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Client IP</th>
									<th class="px-4 py-3 text-left">Method</th>
									<th class="px-4 py-3 text-left">URI</th>
									<th class="px-4 py-3 text-left">Action</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each sampledRequests as req}
									<tr>
										<td class="px-4 py-3 font-mono text-xs">{req.Request?.ClientIP}</td>
										<td class="px-4 py-3 font-mono text-xs">{req.Request?.Method}</td>
										<td class="px-4 py-3 font-mono text-xs truncate max-w-xs">{req.Request?.URI}</td>
										<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium ${req.Action === 'ALLOW' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}>{req.Action}</span></td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		{:else}
			<!-- ACL List -->
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} type="text" placeholder="Search Web ACLs..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
			</div>

			{#if loading}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div></div>
			{:else if filteredACLs.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<Shield class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No Web ACLs found</p>
					<p class="text-sm mt-1">Create a Web ACL to protect your applications</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
							<tr>
								<th class="px-4 py-3 text-left">Web ACL Name</th>
								<th class="px-4 py-3 text-left">Description</th>
								<th class="px-4 py-3 text-left">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredACLs as acl}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button onclick={() => selectWebACL(acl)} class="text-red-600 dark:text-red-400 hover:underline font-medium">{acl.Name}</button>
									</td>
									<td class="px-4 py-3 text-xs text-gray-500">{acl.Description ?? '-'}</td>
									<td class="px-4 py-3">
										<button onclick={() => deleteWebACL(acl)} class="text-red-500 hover:text-red-700 p-1">
											<Trash2 class="w-4 h-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}

	{#if activeTab === 'ipsets'}
		{#if loadingIPSets}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div></div>
		{:else if ipSets.length === 0}
			<div class="text-center py-16 text-gray-500"><Globe class="w-12 h-12 mx-auto mb-3 opacity-40" /><p>No IP sets found</p></div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Description</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each ipSets as ipset}
							<tr><td class="px-4 py-3 font-medium">{ipset.Name}</td><td class="px-4 py-3 text-xs text-gray-500">{ipset.Description ?? '-'}</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	{#if activeTab === 'rulegroups'}
		{#if loadingRuleGroups}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div></div>
		{:else if ruleGroups.length === 0}
			<div class="text-center py-16 text-gray-500"><Shield class="w-12 h-12 mx-auto mb-3 opacity-40" /><p>No rule groups found</p></div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Description</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each ruleGroups as rg}
							<tr><td class="px-4 py-3 font-medium">{rg.Name}</td><td class="px-4 py-3 text-xs text-gray-500">{rg.Description ?? '-'}</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Web ACL Modal -->
{#if showCreateACL}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Web ACL</h2>
			<div>
				<label for="acl-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Web ACL Name</label>
				<input id="acl-name" bind:value={newAclName} type="text" placeholder="my-web-acl" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="default-action" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Default Action</label>
				<select id="default-action" bind:value={newDefaultAction} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="ALLOW">Allow</option>
					<option value="BLOCK">Block</option>
				</select>
			</div>
			<p class="text-xs text-gray-500">Scope: {scope}. Rules can be added after creation.</p>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreateACL = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createWebACL} disabled={creatingACL || !newAclName.trim()} class="flex-1 px-4 py-2 rounded-lg bg-red-600 text-white text-sm font-medium hover:bg-red-700 disabled:opacity-50">
					{creatingACL ? 'Creating...' : 'Create Web ACL'}
				</button>
			</div>
		</div>
	</div>
{/if}
