<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getWAFV2Client } from '$lib/aws-client';
	import {
		ListWebACLsCommand,
		GetWebACLCommand,
		CreateWebACLCommand,
		DeleteWebACLCommand,
		ListIPSetsCommand,
		GetIPSetCommand,
		ListRegexPatternSetsCommand,
		ListRuleGroupsCommand,
		ListLoggingConfigurationsCommand,
		PutLoggingConfigurationCommand,
		DeleteLoggingConfigurationCommand,
		GetSampledRequestsCommand,
		type WebACLSummary,
		type WebACL,
		type IPSetSummary,
		type IPSet,
		type RegexPatternSetSummary,
		type RuleGroupSummary,
		type LoggingConfiguration,
		type SampledHTTPRequest
	} from '@aws-sdk/client-wafv2';
	import { toast } from 'svelte-sonner';
	import {
		Shield,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		ChevronRight,
		Activity,
		Globe,
		FileText,
		List,
		Settings
	} from 'lucide-svelte';

	const waf = regionalClient(getWAFV2Client);

	type Tab = 'webacls' | 'ipsets' | 'regexsets' | 'rulegroups' | 'logging';

	let activeTab = $state<Tab>('webacls');
	let searchQuery = $state('');
	let scope = $state<'REGIONAL' | 'CLOUDFRONT'>('REGIONAL');

	// Web ACLs
	let webAcls = $state<WebACLSummary[]>([]);
	let loadingWebACLs = $state(false);
	let selectedWebACL = $state<WebACL | null>(null);
	let loadingDetail = $state(false);
	let sampledRequests = $state<SampledHTTPRequest[]>([]);
	let loadingSamples = $state(false);

	// Create Web ACL modal
	let showCreateACL = $state(false);
	let creatingACL = $state(false);
	let newAclName = $state('');
	let newAclDescription = $state('');
	let newDefaultAction = $state<'ALLOW' | 'BLOCK'>('ALLOW');

	// IP Sets
	let ipSets = $state<IPSetSummary[]>([]);
	let loadingIPSets = $state(false);
	let selectedIPSet = $state<IPSet | null>(null);
	let loadingIPSetDetail = $state(false);

	// Regex Pattern Sets
	let regexSets = $state<RegexPatternSetSummary[]>([]);
	let loadingRegexSets = $state(false);

	// Rule Groups
	let ruleGroups = $state<RuleGroupSummary[]>([]);
	let loadingRuleGroups = $state(false);

	// Logging
	let loggingConfigs = $state<LoggingConfiguration[]>([]);
	let loadingLogging = $state(false);
	let newLogResourceArn = $state('');
	let newLogDestinationArn = $state('');
	let savingLog = $state(false);
	let showAddLogging = $state(false);

	const filteredACLs = $derived(
		webAcls.filter(
			(a) => !searchQuery || (a.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredIPSets = $derived(
		ipSets.filter(
			(s) => !searchQuery || (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredRegexSets = $derived(
		regexSets.filter(
			(s) => !searchQuery || (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredRuleGroups = $derived(
		ruleGroups.filter(
			(rg) => !searchQuery || (rg.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadWebACLs() {
		loadingWebACLs = true;
		try {
			const resp = await waf().send(new ListWebACLsCommand({ Scope: scope, Limit: 100 }));
			webAcls = resp.WebACLs ?? [];
		} catch (e) {
			toast.error('Failed to load Web ACLs: ' + String(e));
		} finally {
			loadingWebACLs = false;
		}
	}

	async function selectWebACL(summary: WebACLSummary) {
		loadingDetail = true;
		selectedWebACL = null;
		sampledRequests = [];
		try {
			const resp = await waf().send(
				new GetWebACLCommand({
					Name: summary.Name ?? '',
					Scope: scope,
					Id: summary.Id ?? ''
				})
			);
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
			const resp = await waf().send(
				new GetSampledRequestsCommand({
					WebAclArn: selectedWebACL.ARN ?? '',
					RuleMetricName: selectedWebACL.VisibilityConfig?.MetricName ?? 'ALL',
					Scope: scope,
					TimeWindow: {
						StartTime: new Date(Date.now() - 3_600_000),
						EndTime: new Date()
					},
					MaxItems: 20
				})
			);
			sampledRequests = resp.SampledRequests ?? [];
			if (sampledRequests.length === 0) toast.info('No sampled requests in the last hour');
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
			const metricName = newAclName.trim().replaceAll(/[^a-zA-Z0-9]/g, '');
			await waf().send(
				new CreateWebACLCommand({
					Name: newAclName.trim(),
					Scope: scope,
					Description: newAclDescription.trim() || undefined,
					DefaultAction: newDefaultAction === 'ALLOW' ? { Allow: {} } : { Block: {} },
					VisibilityConfig: {
						SampledRequestsEnabled: true,
						CloudWatchMetricsEnabled: true,
						MetricName: metricName || 'DefaultMetric'
					},
					Rules: []
				})
			);
			toast.success(`Web ACL "${newAclName}" created`);
			showCreateACL = false;
			newAclName = '';
			newAclDescription = '';
			newDefaultAction = 'ALLOW';
			await loadWebACLs();
		} catch (e) {
			toast.error('Failed to create Web ACL: ' + String(e));
		} finally {
			creatingACL = false;
		}
	}

	async function deleteWebACL(summary: WebACLSummary) {
		const confirmed = await confirmDestructive({
			title: 'Delete Web ACL',
			message: `Delete Web ACL "${summary.Name}"? Associated resources will lose WAF protection.`
		});
		if (!confirmed) return;
		try {
			await waf().send(
				new DeleteWebACLCommand({
					Name: summary.Name ?? '',
					Scope: scope,
					Id: summary.Id ?? '',
					LockToken: summary.LockToken ?? ''
				})
			);
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
			const resp = await waf().send(new ListIPSetsCommand({ Scope: scope, Limit: 100 }));
			ipSets = resp.IPSets ?? [];
		} catch (e) {
			toast.error('Failed to load IP sets: ' + String(e));
		} finally {
			loadingIPSets = false;
		}
	}

	async function selectIPSet(summary: IPSetSummary) {
		loadingIPSetDetail = true;
		selectedIPSet = null;
		try {
			const resp = await waf().send(
				new GetIPSetCommand({
					Name: summary.Name ?? '',
					Scope: scope,
					Id: summary.Id ?? ''
				})
			);
			selectedIPSet = resp.IPSet ?? null;
		} catch (e) {
			toast.error('Failed to load IP set details: ' + String(e));
		} finally {
			loadingIPSetDetail = false;
		}
	}

	async function loadRegexSets() {
		loadingRegexSets = true;
		try {
			const resp = await waf().send(new ListRegexPatternSetsCommand({ Scope: scope, Limit: 100 }));
			regexSets = resp.RegexPatternSets ?? [];
		} catch (e) {
			toast.error('Failed to load regex pattern sets: ' + String(e));
		} finally {
			loadingRegexSets = false;
		}
	}

	async function loadRuleGroups() {
		loadingRuleGroups = true;
		try {
			const resp = await waf().send(new ListRuleGroupsCommand({ Scope: scope, Limit: 100 }));
			ruleGroups = resp.RuleGroups ?? [];
		} catch (e) {
			toast.error('Failed to load rule groups: ' + String(e));
		} finally {
			loadingRuleGroups = false;
		}
	}

	async function loadLoggingConfigs() {
		loadingLogging = true;
		try {
			const resp = await waf().send(new ListLoggingConfigurationsCommand({ Scope: scope }));
			loggingConfigs = resp.LoggingConfigurations ?? [];
		} catch (e) {
			toast.error('Failed to load logging configurations: ' + String(e));
		} finally {
			loadingLogging = false;
		}
	}

	async function addLoggingConfig() {
		if (!newLogResourceArn.trim() || !newLogDestinationArn.trim()) return;
		savingLog = true;
		try {
			await waf().send(
				new PutLoggingConfigurationCommand({
					LoggingConfiguration: {
						ResourceArn: newLogResourceArn.trim(),
						LogDestinationConfigs: [newLogDestinationArn.trim()]
					}
				})
			);
			toast.success('Logging configuration saved');
			showAddLogging = false;
			newLogResourceArn = '';
			newLogDestinationArn = '';
			await loadLoggingConfigs();
		} catch (e) {
			toast.error('Failed to save logging configuration: ' + String(e));
		} finally {
			savingLog = false;
		}
	}

	async function deleteLoggingConfig(resourceArn: string) {
		const confirmed = await confirmDestructive({
			title: 'Delete Logging Configuration',
			message: 'Remove logging configuration for this resource?'
		});
		if (!confirmed) return;
		try {
			await waf().send(new DeleteLoggingConfigurationCommand({ ResourceArn: resourceArn }));
			toast.success('Logging configuration removed');
			await loadLoggingConfigs();
		} catch (e) {
			toast.error('Failed to remove logging configuration: ' + String(e));
		}
	}

	async function handleScopeChange() {
		selectedWebACL = null;
		selectedIPSet = null;
		webAcls = [];
		ipSets = [];
		regexSets = [];
		ruleGroups = [];
		loggingConfigs = [];
		sampledRequests = [];
		await loadForTab(activeTab);
	}

	async function loadForTab(tab: Tab) {
		if (tab === 'webacls') await loadWebACLs();
		else if (tab === 'ipsets') await loadIPSets();
		else if (tab === 'regexsets') await loadRegexSets();
		else if (tab === 'rulegroups') await loadRuleGroups();
		else if (tab === 'logging') await loadLoggingConfigs();
	}

	async function handleTabChange(tab: Tab) {
		activeTab = tab;
		selectedWebACL = null;
		selectedIPSet = null;
		searchQuery = '';
		await loadForTab(tab);
	}

	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	function ruleType(rule: { Statement?: any }): string {
		if (rule.Statement?.ManagedRuleGroupStatement) return 'Managed Rule Group';
		if (rule.Statement?.IPSetReferenceStatement) return 'IP Set';
		if (rule.Statement?.ByteMatchStatement) return 'Byte Match';
		if (rule.Statement?.RateBasedStatement) return 'Rate Based';
		if (rule.Statement?.GeoMatchStatement) return 'Geo Match';
		return 'Custom';
	}

	// A Web ACL/IP set is only unique within a scope+region; the scope select
	// already reloads via handleScopeChange, and the tab switcher already
	// reloads via handleTabChange, so read both with `untrack` to keep them
	// out of this effect's dependencies and avoid double-fetching when either
	// changes. Selected detail state and all lists belong to the old region,
	// so clear them the same way handleScopeChange does before reloading.
	onRegionChange(() => {
		selectedWebACL = null;
		selectedIPSet = null;
		webAcls = [];
		ipSets = [];
		regexSets = [];
		ruleGroups = [];
		loggingConfigs = [];
		sampledRequests = [];
		untrack(() => loadForTab(activeTab));
	});
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="w-7 h-7 text-red-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS WAF v2</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Web application firewall rules and protections</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<label for="wafv2-scope" class="text-sm text-gray-600 dark:text-gray-400">Scope:</label>
			<select
				id="wafv2-scope"
				bind:value={scope}
				onchange={handleScopeChange}
				class="px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm"
			>
				<option value="REGIONAL">Regional</option>
				<option value="CLOUDFRONT">CloudFront (Global)</option>
			</select>
			<button
				onclick={() => loadForTab(activeTab)}
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			{#if activeTab === 'webacls'}
				<button
					onclick={() => (showCreateACL = true)}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium"
				>
					<Plus class="w-4 h-4" /> Create Web ACL
				</button>
			{/if}
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
		{#each ([['webacls', 'Web ACLs', Shield], ['ipsets', 'IP Sets', Globe], ['regexsets', 'Regex Sets', Search], ['rulegroups', 'Rule Groups', List], ['logging', 'Logging', FileText]] as const) as [tab, label, Icon]}
			<button
				onclick={() => handleTabChange(tab)}
				class={`flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-red-500 text-red-600 dark:text-red-400' : 'border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`}
			>
				<Icon class="w-4 h-4" />
				{label}
			</button>
		{/each}
	</div>

	<!-- Search bar (shared for list views) -->
	{#if activeTab !== 'logging' && !selectedWebACL && !selectedIPSet}
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input
				bind:value={searchQuery}
				type="text"
				placeholder="Search..."
				class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm"
			/>
		</div>
	{/if}

	<!-- Web ACLs Tab -->
	{#if activeTab === 'webacls'}
		{#if selectedWebACL}
			<!-- ACL Detail View -->
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2 text-sm">
					<button
						onclick={() => { selectedWebACL = null; sampledRequests = []; }}
						class="text-red-600 hover:underline"
					>Web ACLs</button>
					<ChevronRight class="w-4 h-4 text-gray-400" />
					<span class="font-medium">{selectedWebACL.Name}</span>
					<span
						class={`px-2 py-0.5 rounded text-xs font-medium ${Object.keys(selectedWebACL.DefaultAction ?? {})[0] === 'Allow' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}
					>
						Default: {Object.keys(selectedWebACL.DefaultAction ?? {})[0]?.toUpperCase() ?? 'ALLOW'}
					</span>
				</div>
				<button
					onclick={loadSampledRequests}
					disabled={loadingSamples}
					class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800 disabled:opacity-50"
				>
					<Activity class="w-4 h-4" /> Sample Requests
				</button>
			</div>

			<!-- Stats Cards -->
			<div class="grid grid-cols-3 gap-4">
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 dark:text-gray-400">Rules</div>
					<div class="text-2xl font-bold text-gray-900 dark:text-white mt-1">{(selectedWebACL.Rules ?? []).length}</div>
				</div>
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 dark:text-gray-400">Capacity</div>
					<div class="text-2xl font-bold text-gray-900 dark:text-white mt-1">{selectedWebACL.Capacity ?? 0}</div>
				</div>
				<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
					<div class="text-xs text-gray-500 dark:text-gray-400">Managed Rules</div>
					<div class="text-2xl font-bold text-gray-900 dark:text-white mt-1">
						{(selectedWebACL.Rules ?? []).filter((r) => r.Statement?.ManagedRuleGroupStatement).length}
					</div>
				</div>
			</div>

			<!-- Rule Builder Table -->
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
									<th class="px-4 py-3 text-left">Match Condition</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each (selectedWebACL.Rules ?? []).sort((a, b) => (a.Priority ?? 0) - (b.Priority ?? 0)) as rule}
									<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
										<td class="px-4 py-3 font-medium">{rule.Name}</td>
										<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{rule.Priority}</td>
										<td class="px-4 py-3">
											<span
												class={`px-2 py-0.5 rounded text-xs font-medium ${Object.keys(rule.Action ?? rule.OverrideAction ?? {})[0] === 'Allow' ? 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300' : 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300'}`}
											>
												{Object.keys(rule.Action ?? rule.OverrideAction ?? {})[0]?.toUpperCase() ?? 'OVERRIDE'}
											</span>
										</td>
										<td class="px-4 py-3 text-xs text-gray-500 dark:text-gray-400">
											{ruleType(rule)}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{:else}
				<div class="text-center py-8 text-gray-400">
					<Settings class="w-8 h-8 mx-auto mb-2 opacity-40" />
					<p class="text-sm">No rules configured. Add rules to protect your application.</p>
				</div>
			{/if}

			<!-- Sampled Requests Viewer -->
			{#if loadingSamples}
				<div class="flex justify-center py-8">
					<div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div>
				</div>
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
									<th class="px-4 py-3 text-left">Rule</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each sampledRequests as req}
									<tr>
										<td class="px-4 py-3 font-mono text-xs">{req.Request?.ClientIP ?? '-'}</td>
										<td class="px-4 py-3 font-mono text-xs">{req.Request?.Method ?? '-'}</td>
										<td class="px-4 py-3 font-mono text-xs truncate max-w-xs">{req.Request?.URI ?? '-'}</td>
										<td class="px-4 py-3">
											<span
												class={`px-2 py-0.5 rounded text-xs font-medium ${req.Action === 'ALLOW' ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'}`}
											>{req.Action ?? '-'}</span>
										</td>
										<td class="px-4 py-3 text-xs text-gray-500">{req.RuleNameWithinRuleGroup ?? '-'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		{:else}
			<!-- ACL List -->
			{#if loadingWebACLs}
				<div class="flex justify-center py-12">
					<div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div>
				</div>
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
								<th class="px-4 py-3 text-left">ARN</th>
								<th class="px-4 py-3 text-left">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each filteredACLs as acl}
								<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
									<td class="px-4 py-3">
										<button
											onclick={() => selectWebACL(acl)}
											class="text-red-600 dark:text-red-400 hover:underline font-medium"
										>{acl.Name}</button>
									</td>
									<td class="px-4 py-3 text-xs text-gray-500">{acl.Description ?? '-'}</td>
									<td class="px-4 py-3 font-mono text-xs text-gray-400 truncate max-w-xs">{acl.ARN ?? '-'}</td>
									<td class="px-4 py-3">
										<button
											onclick={() => deleteWebACL(acl)}
											class="text-red-500 hover:text-red-700 p-1"
											title="Delete Web ACL"
										>
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

	<!-- IP Sets Tab -->
	{#if activeTab === 'ipsets'}
		{#if selectedIPSet}
			<div class="flex items-center gap-2 text-sm mb-4">
				<button onclick={() => (selectedIPSet = null)} class="text-red-600 hover:underline">IP Sets</button>
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<span class="font-medium">{selectedIPSet.Name}</span>
				<span class="px-2 py-0.5 rounded text-xs bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300">
					{selectedIPSet.IPAddressVersion}
				</span>
			</div>
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
					IP Addresses ({(selectedIPSet.Addresses ?? []).length})
				</h3>
				{#if (selectedIPSet.Addresses ?? []).length === 0}
					<p class="text-sm text-gray-400">No IP addresses configured</p>
				{:else}
					<div class="grid grid-cols-3 gap-2">
						{#each selectedIPSet.Addresses ?? [] as addr}
							<span class="font-mono text-xs bg-gray-50 dark:bg-gray-800 px-2 py-1 rounded">{addr}</span>
						{/each}
					</div>
				{/if}
			</div>
		{:else if loadingIPSets}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if filteredIPSets.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Globe class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No IP sets found</p>
				<p class="text-sm mt-1">IP sets let you allow or block specific IP addresses</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">IP Version</th>
							<th class="px-4 py-3 text-left">Description</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredIPSets as ipset}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
								<td class="px-4 py-3">
									<button
										onclick={() => selectIPSet(ipset)}
										class="text-red-600 dark:text-red-400 hover:underline font-medium"
									>{ipset.Name}</button>
								</td>
								<td class="px-4 py-3 text-xs text-gray-500">-</td>
								<td class="px-4 py-3 text-xs text-gray-500">{ipset.Description ?? '-'}</td>
								<td class="px-4 py-3 text-xs font-mono text-gray-400 truncate max-w-xs">{ipset.ARN ?? ''}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Regex Sets Tab -->
	{#if activeTab === 'regexsets'}
		{#if loadingRegexSets}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if filteredRegexSets.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Search class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No regex pattern sets found</p>
				<p class="text-sm mt-1">Regex pattern sets match request components against regular expressions</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Description</th>
							<th class="px-4 py-3 text-left">ARN</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredRegexSets as rs}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
								<td class="px-4 py-3 font-medium">{rs.Name}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{rs.Description ?? '-'}</td>
								<td class="px-4 py-3 font-mono text-xs text-gray-400 truncate max-w-xs">{rs.ARN ?? ''}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Rule Groups Tab -->
	{#if activeTab === 'rulegroups'}
		{#if loadingRuleGroups}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if filteredRuleGroups.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<List class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No rule groups found</p>
				<p class="text-sm mt-1">Rule groups are reusable collections of WAF rules</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Name</th>
							<th class="px-4 py-3 text-left">Description</th>
							<th class="px-4 py-3 text-left">ARN</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredRuleGroups as rg}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
								<td class="px-4 py-3 font-medium">{rg.Name}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{rg.Description ?? '-'}</td>
								<td class="px-4 py-3 font-mono text-xs text-gray-400 truncate max-w-xs">{rg.ARN ?? ''}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Logging Tab -->
	{#if activeTab === 'logging'}
		<div class="flex items-center justify-between mb-4">
			<h2 class="text-base font-semibold text-gray-700 dark:text-gray-300">Logging Configurations</h2>
			<button
				onclick={() => (showAddLogging = true)}
				class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium"
			>
				<Plus class="w-4 h-4" /> Add Logging
			</button>
		</div>

		{#if loadingLogging}
			<div class="flex justify-center py-12">
				<div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if loggingConfigs.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<FileText class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No logging configurations</p>
				<p class="text-sm mt-1">Enable logging to capture detailed information about traffic analysed by WAF</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Resource ARN</th>
							<th class="px-4 py-3 text-left">Log Destinations</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each loggingConfigs as lc}
							<tr>
								<td class="px-4 py-3 font-mono text-xs text-gray-700 dark:text-gray-300 truncate max-w-xs">{lc.ResourceArn}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{(lc.LogDestinationConfigs ?? []).join(', ')}</td>
								<td class="px-4 py-3">
									<button
										onclick={() => deleteLoggingConfig(lc.ResourceArn ?? '')}
										class="text-red-500 hover:text-red-700 p-1"
										title="Remove logging"
									>
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- Add Logging Modal -->
		{#if showAddLogging}
			<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
				<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl p-6 w-full max-w-lg mx-4">
					<h2 class="text-lg font-semibold mb-4 text-gray-900 dark:text-white">Add Logging Configuration</h2>
					<div class="space-y-4">
						<div>
							<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="log-resource-arn">
								Web ACL ARN
							</label>
							<input
								id="log-resource-arn"
								bind:value={newLogResourceArn}
								type="text"
								placeholder="arn:aws:wafv2:..."
								class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
							/>
						</div>
						<div>
							<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="log-dest-arn">
								Log Destination ARN (Kinesis, S3, or CloudWatch)
							</label>
							<input
								id="log-dest-arn"
								bind:value={newLogDestinationArn}
								type="text"
								placeholder="arn:aws:kinesis:..."
								class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
							/>
						</div>
					</div>
					<div class="flex justify-end gap-3 mt-6">
						<button
							onclick={() => (showAddLogging = false)}
							class="px-4 py-2 text-sm text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 rounded-lg hover:bg-gray-50"
						>Cancel</button>
						<button
							onclick={addLoggingConfig}
							disabled={savingLog || !newLogResourceArn.trim() || !newLogDestinationArn.trim()}
							class="px-4 py-2 text-sm bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50"
						>
							{savingLog ? 'Saving...' : 'Save'}
						</button>
					</div>
				</div>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Web ACL Modal -->
{#if showCreateACL}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl p-6 w-full max-w-lg mx-4">
			<h2 class="text-lg font-semibold mb-4 text-gray-900 dark:text-white">Create Web ACL</h2>
			<div class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="acl-name">
						Web ACL Name
					</label>
					<input
						id="acl-name"
						bind:value={newAclName}
						type="text"
						placeholder="my-web-acl"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="acl-desc">
						Description (optional)
					</label>
					<input
						id="acl-desc"
						bind:value={newAclDescription}
						type="text"
						placeholder="Protects my application"
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
					/>
				</div>
				<div>
					<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="default-action">
						Default Action
					</label>
					<select
						id="default-action"
						bind:value={newDefaultAction}
						class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"
					>
						<option value="ALLOW">Allow</option>
						<option value="BLOCK">Block</option>
					</select>
				</div>
			</div>
			<div class="flex justify-end gap-3 mt-6">
				<button
					onclick={() => (showCreateACL = false)}
					class="px-4 py-2 text-sm text-gray-600 dark:text-gray-300 border border-gray-200 dark:border-gray-700 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-800"
				>Cancel</button>
				<button
					onclick={createWebACL}
					disabled={creatingACL || !newAclName.trim()}
					class="px-4 py-2 text-sm bg-red-600 text-white rounded-lg hover:bg-red-700 disabled:opacity-50"
				>
					{creatingACL ? 'Creating...' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}
