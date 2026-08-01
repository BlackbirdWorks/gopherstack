<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getConfigClient } from '$lib/aws-client';
	import {
		DescribeConfigRulesCommand,
		DescribeComplianceByConfigRuleCommand,
		GetComplianceDetailsByConfigRuleCommand,
		DescribeConfigurationRecordersCommand,
		DescribeConfigurationRecorderStatusCommand,
		StartConfigurationRecorderCommand,
		StopConfigurationRecorderCommand,
		DeleteConfigRuleCommand,
		PutConfigRuleCommand,
		DescribeConformancePacksCommand,
		DescribeConformancePackStatusCommand,
		GetConformancePackComplianceSummaryCommand,
		DeleteConformancePackCommand,
		DescribeRemediationConfigurationsCommand,
		StartRemediationExecutionCommand,
		type ConfigRule,
		type ConformancePackDetail,
		type ConformancePackStatusDetail,
		type RemediationConfiguration
	} from '@aws-sdk/client-config-service';
	import { toast } from 'svelte-sonner';
	import {
		Shield,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		CheckCircle,
		XCircle,
		AlertTriangle,
		Play,
		Square,
		Package,
		Wrench
	} from 'lucide-svelte';

	const config = regionalClient(getConfigClient);

	let loading = $state(false);
	let activeTab = $state<'rules' | 'recorder' | 'conformance' | 'remediation'>('rules');
	let searchQuery = $state('');

	// Rules
	let rules = $state<ConfigRule[]>([]);
	let compliance = $state<Record<string, string>>({});
	let selectedRule = $state<ConfigRule | null>(null);
	let ruleDetails = $state<Array<{ ResourceId?: string; ComplianceType?: string }>>([]);
	let loadingDetails = $state(false);

	// Create rule modal
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newRuleName = $state('');
	let newRuleSource = $state('AWS_CONFIG_RULE');
	let newRuleSourceIdentifier = $state('RESTRICTED_INCOMING_SSH');

	// Recorder
	let recorders = $state<Array<{ name?: string; recordingGroup?: Record<string, unknown> }>>([]);
	let recorderStatuses = $state<Record<string, { recording?: boolean; lastStatus?: string }>>({});

	// Conformance packs
	let conformancePacks = $state<ConformancePackDetail[]>([]);
	let conformancePackStatuses = $state<Record<string, ConformancePackStatusDetail>>({});
	let conformanceScores = $state<Record<string, string>>({});

	// Remediation
	let remediations = $state<RemediationConfiguration[]>([]);
	let remediatingRule = $state<string | null>(null);

	const filteredRules = $derived(
		rules.filter(
			(r) =>
				(r.ConfigRuleName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(r.Source?.SourceIdentifier ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredConformancePacks = $derived(
		conformancePacks.filter((p) =>
			(p.ConformancePackName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredRemediations = $derived(
		remediations.filter((r) =>
			(r.ConfigRuleName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function complianceBadge(type: string) {
		if (type === 'COMPLIANT')
			return { color: 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900', label: 'Compliant' };
		if (type === 'NON_COMPLIANT')
			return { color: 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900', label: 'Non-Compliant' };
		if (type === 'INSUFFICIENT_DATA')
			return { color: 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900', label: 'Insufficient Data' };
		return { color: 'text-muted-foreground bg-muted', label: type || 'Unknown' };
	}

	function conformanceStatusBadge(status: string) {
		if (status === 'CREATE_COMPLETE' || status === 'UPDATE_COMPLETE')
			return { color: 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900', label: status };
		if (status === 'CREATE_FAILED' || status === 'UPDATE_FAILED' || status === 'DELETE_FAILED')
			return { color: 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900', label: status };
		if (status?.includes('IN_PROGRESS'))
			return { color: 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900', label: status };
		return { color: 'text-muted-foreground bg-muted', label: status || 'Unknown' };
	}

	async function loadRules() {
		loading = true;
		try {
			const res = await config().send(new DescribeConfigRulesCommand({}));
			rules = res.ConfigRules ?? [];
			const compRes = await config().send(new DescribeComplianceByConfigRuleCommand({}));
			const byRule: Record<string, string> = {};
			for (const item of compRes.ComplianceByConfigRules ?? []) {
				byRule[item.ConfigRuleName ?? ''] = item.Compliance?.ComplianceType ?? 'UNKNOWN';
			}
			compliance = byRule;
		} catch (e) {
			toast.error(`Failed to load rules: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadRecorders() {
		loading = true;
		try {
			const res = await config().send(new DescribeConfigurationRecordersCommand({}));
			recorders = (res.ConfigurationRecorders ?? []).map((r) => ({
				name: r.name,
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				recordingGroup: r.recordingGroup as Record<string, any>
			}));
			const statusRes = await config().send(new DescribeConfigurationRecorderStatusCommand({}));
			const statusMap: Record<string, { recording?: boolean; lastStatus?: string }> = {};
			for (const s of statusRes.ConfigurationRecordersStatus ?? []) {
				statusMap[s.name ?? ''] = {
					recording: s.recording,
					lastStatus: s.lastStatus
				};
			}
			recorderStatuses = statusMap;
		} catch (e) {
			toast.error(`Failed to load recorders: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadConformancePacks() {
		loading = true;
		try {
			const [packRes, statusRes] = await Promise.all([
				config().send(new DescribeConformancePacksCommand({})),
				config().send(new DescribeConformancePackStatusCommand({}))
			]);
			conformancePacks = packRes.ConformancePackDetails ?? [];

			const statusMap: Record<string, ConformancePackStatusDetail> = {};
			for (const s of statusRes.ConformancePackStatusDetails ?? []) {
				statusMap[s.ConformancePackName ?? ''] = s;
			}
			conformancePackStatuses = statusMap;

			const scores: Record<string, string> = {};
			if (conformancePacks.length > 0) {
				try {
					const scoreRes = await config().send(
						new GetConformancePackComplianceSummaryCommand({
							ConformancePackNames: conformancePacks.map((p) => p.ConformancePackName ?? '')
						})
					);
					for (const s of scoreRes.ConformancePackComplianceSummaryList ?? []) {
						scores[s.ConformancePackName ?? ''] =
							s.ConformancePackComplianceStatus ?? 'UNKNOWN';
					}
				} catch {
					// scores remain empty if compliance summary fails
				}
			}
			conformanceScores = scores;
		} catch (e) {
			toast.error(`Failed to load conformance packs: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadRemediation() {
		loading = true;
		try {
			const res = await config().send(new DescribeRemediationConfigurationsCommand({ ConfigRuleNames: [] }));
			remediations = res.RemediationConfigurations ?? [];
		} catch (e) {
			toast.error(`Failed to load remediation configurations: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewRuleDetails(rule: ConfigRule) {
		selectedRule = rule;
		loadingDetails = true;
		ruleDetails = [];
		try {
			const res = await config().send(
				new GetComplianceDetailsByConfigRuleCommand({
					ConfigRuleName: rule.ConfigRuleName,
					Limit: 25
				})
			);
			ruleDetails = (res.EvaluationResults ?? []).map((r) => ({
				ResourceId: r.EvaluationResultIdentifier?.EvaluationResultQualifier?.ResourceId,
				ComplianceType: r.ComplianceType
			}));
		} catch (e) {
			toast.error(`Failed to load rule details: ${e}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function deleteRule(rule: ConfigRule) {
		if (!await confirmDestructive({ title: 'Delete Config Rule', message: `Delete rule "${rule.ConfigRuleName}"? Compliance evaluation for this rule will stop.` })) return;
		try {
			await config().send(new DeleteConfigRuleCommand({ ConfigRuleName: rule.ConfigRuleName }));
			toast.success(`Rule "${rule.ConfigRuleName}" deleted`);
			await loadRules();
		} catch (e) {
			toast.error(`Failed to delete rule: ${e}`);
		}
	}

	async function createRule() {
		if (!newRuleName.trim()) return;
		creating = true;
		try {
			await config().send(
				new PutConfigRuleCommand({
					ConfigRule: {
						ConfigRuleName: newRuleName.trim(),
						Source: {
							Owner: newRuleSource === 'AWS_CONFIG_RULE' ? 'AWS' : 'CUSTOM_LAMBDA',
							SourceIdentifier: newRuleSourceIdentifier.trim()
						}
					}
				})
			);
			toast.success(`Rule "${newRuleName}" created`);
			showCreateModal = false;
			newRuleName = '';
			await loadRules();
		} catch (e) {
			toast.error(`Failed to create rule: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function toggleRecorder(name: string) {
		const isRecording = recorderStatuses[name]?.recording;
		try {
			if (isRecording) {
				await config().send(new StopConfigurationRecorderCommand({ ConfigurationRecorderName: name }));
				toast.success(`Stopped recorder "${name}"`);
			} else {
				await config().send(new StartConfigurationRecorderCommand({ ConfigurationRecorderName: name }));
				toast.success(`Started recorder "${name}"`);
			}
			await loadRecorders();
		} catch (e) {
			toast.error(`Failed to toggle recorder: ${e}`);
		}
	}

	async function deleteConformancePack(pack: ConformancePackDetail) {
		if (!await confirmDestructive({ title: 'Delete Conformance Pack', message: `Delete conformance pack "${pack.ConformancePackName}"?` })) return;
		try {
			await config().send(new DeleteConformancePackCommand({ ConformancePackName: pack.ConformancePackName }));
			toast.success(`Conformance pack "${pack.ConformancePackName}" deleted`);
			await loadConformancePacks();
		} catch (e) {
			toast.error(`Failed to delete conformance pack: ${e}`);
		}
	}

	async function startRemediation(remediation: RemediationConfiguration) {
		if (!remediation.ConfigRuleName) return;
		remediatingRule = remediation.ConfigRuleName;
		try {
			await config().send(
				new StartRemediationExecutionCommand({
					ConfigRuleName: remediation.ConfigRuleName,
					ResourceKeys: []
				})
			);
			toast.success(`Remediation started for rule "${remediation.ConfigRuleName}"`);
		} catch (e) {
			toast.error(`Failed to start remediation: ${e}`);
		} finally {
			remediatingRule = null;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'rules') await loadRules();
		else if (tab === 'recorder') await loadRecorders();
		else if (tab === 'conformance') await loadConformancePacks();
		else if (tab === 'remediation') await loadRemediation();
	}

	onRegionChange(loadRules);
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="h-8 w-8 text-purple-600" />
			<div>
				<h1 class="text-2xl font-bold">AWS Config</h1>
				<p class="text-sm text-muted-foreground">Monitor resource configurations and compliance</p>
			</div>
		</div>
		<button
			onclick={() => {
				if (activeTab === 'rules') loadRules();
				else if (activeTab === 'recorder') loadRecorders();
				else if (activeTab === 'conformance') loadConformancePacks();
				else loadRemediation();
			}}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [
			{ id: 'rules', label: 'Config Rules' },
			{ id: 'recorder', label: 'Configuration Recorder' },
			{ id: 'conformance', label: 'Conformance Packs' },
			{ id: 'remediation', label: 'Remediation' }
		] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Search bar (shared across tabs) -->
	{#if activeTab !== 'recorder'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			{#if activeTab === 'rules'}
				<button
					onclick={() => (showCreateModal = true)}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
				>
					<Plus class="h-4 w-4" />
					Add Rule
				</button>
			{/if}
		</div>
	{/if}

	<!-- Rules Tab -->
	{#if activeTab === 'rules'}
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredRules.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Shield class="h-12 w-12 mb-3 opacity-30" />
				<p>No Config rules found</p>
				<p class="text-sm">Add rules to evaluate resource compliance</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Rule Name</th>
							<th class="px-4 py-3 text-left font-medium">Source</th>
							<th class="px-4 py-3 text-left font-medium">Compliance</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredRules as rule}
							{@const badge = complianceBadge(compliance[rule.ConfigRuleName ?? ''] ?? '')}
							<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewRuleDetails(rule)}>
								<td class="px-4 py-3 font-medium">{rule.ConfigRuleName}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{rule.Source?.SourceIdentifier ?? rule.Source?.Owner ?? '—'}
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {badge.color}">
										{badge.label}
									</span>
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={(e) => { e.stopPropagation(); deleteRule(rule); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete rule"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- Rule Details Panel -->
		{#if selectedRule}
			<div class="rounded-lg border p-4 space-y-3">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold">{selectedRule.ConfigRuleName} — Resources</h3>
					<button onclick={() => (selectedRule = null)} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>
				{#if loadingDetails}
					<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
				{:else if ruleDetails.length === 0}
					<p class="text-sm text-muted-foreground">No evaluation results found.</p>
				{:else}
					<div class="divide-y rounded border overflow-hidden">
						{#each ruleDetails as detail}
							{@const badge = complianceBadge(detail.ComplianceType ?? '')}
							<div class="flex items-center justify-between px-4 py-2 text-sm">
								<span>{detail.ResourceId ?? '—'}</span>
								<span class="rounded-full px-2 py-0.5 text-xs {badge.color}">{badge.label}</span>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	{/if}

	<!-- Recorder Tab -->
	{#if activeTab === 'recorder'}
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if recorders.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<AlertTriangle class="h-12 w-12 mb-3 opacity-30" />
				<p>No configuration recorders found</p>
			</div>
		{:else}
			<div class="space-y-3">
				{#each recorders as recorder}
					{@const status = recorderStatuses[recorder.name ?? '']}
					<div class="rounded-lg border p-4 flex items-center justify-between">
						<div>
							<p class="font-medium">{recorder.name ?? 'default'}</p>
							<p class="text-sm text-muted-foreground">
								Last status: {status?.lastStatus ?? '—'}
							</p>
						</div>
						<div class="flex items-center gap-3">
							{#if status?.recording}
								<span class="flex items-center gap-1 text-sm text-green-600">
									<CheckCircle class="h-4 w-4" />
									Recording
								</span>
							{:else}
								<span class="flex items-center gap-1 text-sm text-muted-foreground">
									<XCircle class="h-4 w-4" />
									Stopped
								</span>
							{/if}
							<button
								onclick={() => toggleRecorder(recorder.name ?? '')}
								class="flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-sm hover:bg-accent"
							>
								{#if status?.recording}
									<Square class="h-3.5 w-3.5" />
									Stop
								{:else}
									<Play class="h-3.5 w-3.5" />
									Start
								{/if}
							</button>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- Conformance Packs Tab -->
	{#if activeTab === 'conformance'}
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredConformancePacks.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Package class="h-12 w-12 mb-3 opacity-30" />
				<p>No conformance packs found</p>
				<p class="text-sm">Deploy conformance packs to enforce compliance standards</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Pack Name</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-left font-medium">Compliance</th>
							<th class="px-4 py-3 text-left font-medium">Last Updated</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredConformancePacks as pack}
							{@const packStatus = conformancePackStatuses[pack.ConformancePackName ?? '']}
							{@const statusBadge = conformanceStatusBadge(packStatus?.ConformancePackState ?? '')}
							{@const compBadge = complianceBadge(conformanceScores[pack.ConformancePackName ?? ''] ?? '')}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{pack.ConformancePackName}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge.color}">
										{statusBadge.label || '—'}
									</span>
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {compBadge.color}">
										{compBadge.label}
									</span>
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{packStatus?.LastUpdateCompletedTime
										? new Date(packStatus.LastUpdateCompletedTime).toLocaleString()
										: '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => deleteConformancePack(pack)}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete conformance pack"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Remediation Tab -->
	{#if activeTab === 'remediation'}
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredRemediations.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Wrench class="h-12 w-12 mb-3 opacity-30" />
				<p>No remediation configurations found</p>
				<p class="text-sm">Configure auto-remediation on Config rules to fix non-compliant resources</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Config Rule</th>
							<th class="px-4 py-3 text-left font-medium">Target Type</th>
							<th class="px-4 py-3 text-left font-medium">Target ID</th>
							<th class="px-4 py-3 text-left font-medium">Auto Remediate</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredRemediations as remediation}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{remediation.ConfigRuleName ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground">{remediation.TargetType ?? '—'}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground font-mono">{remediation.TargetId ?? '—'}</td>
								<td class="px-4 py-3">
									{#if remediation.Automatic}
										<span class="flex items-center gap-1 text-green-600 text-xs">
											<CheckCircle class="h-3.5 w-3.5" />
											Auto
										</span>
									{:else}
										<span class="text-muted-foreground text-xs">Manual</span>
									{/if}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={() => startRemediation(remediation)}
										disabled={remediatingRule === remediation.ConfigRuleName}
										class="flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-50"
										title="Start remediation"
									>
										{#if remediatingRule === remediation.ConfigRuleName}
											<RefreshCw class="h-3.5 w-3.5 animate-spin" />
											Running...
										{:else}
											<Play class="h-3.5 w-3.5" />
											Remediate
										{/if}
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Rule Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add Config Rule</h2>
			<div class="space-y-3">
				<div>
					<label for="rule-name" class="block text-sm font-medium mb-1">Rule Name *</label>
					<input
						id="rule-name"
						type="text"
						bind:value={newRuleName}
						placeholder="my-config-rule"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="rule-source" class="block text-sm font-medium mb-1">Source Type</label>
					<select
						id="rule-source"
						bind:value={newRuleSource}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="AWS_CONFIG_RULE">AWS Managed Rule</option>
						<option value="CUSTOM_LAMBDA">Custom Lambda Rule</option>
					</select>
				</div>
				<div>
					<label for="rule-identifier" class="block text-sm font-medium mb-1">Source Identifier</label>
					<input
						id="rule-identifier"
						type="text"
						bind:value={newRuleSourceIdentifier}
						placeholder="RESTRICTED_INCOMING_SSH"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createRule}
					disabled={creating || !newRuleName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Add Rule'}
				</button>
			</div>
		</div>
	</div>
{/if}
