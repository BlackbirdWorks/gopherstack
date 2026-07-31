<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSecurityHubClient } from '$lib/aws-client';
	import {
		GetFindingsCommand,
		GetInsightsCommand,
		DescribeHubCommand,
		ListStandardsControlAssociationsCommand,
		BatchUpdateFindingsCommand,
		CreateInsightCommand,
		DeleteInsightCommand,
		type AwsSecurityFinding,
		type AwsSecurityFindingFilters,
		type Insight
	} from '@aws-sdk/client-securityhub';
	import { toast } from 'svelte-sonner';
	import {
		Shield,
		Search,
		RefreshCw,
		AlertTriangle,
		CheckCircle,
		XCircle,
		BarChart2,
		Filter,
		Plus,
		Trash2,
		X
	} from 'lucide-svelte';

	const hub = regionalClient(getSecurityHubClient);

	let loading = $state(false);
	let activeTab = $state<'findings' | 'insights'>('findings');
	let searchQuery = $state('');

	let findings = $state<AwsSecurityFinding[]>([]);
	let insights = $state<Insight[]>([]);
	let hubEnabled = $state<boolean | null>(null);
	let severityFilter = $state<'all' | 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW'>('all');
	let statusFilter = $state<'all' | 'NEW' | 'NOTIFIED' | 'RESOLVED' | 'SUPPRESSED'>('all');

	const filteredFindings = $derived(
		findings.filter((f) => {
			const text =
				(f.Title ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(f.Types?.[0] ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(f.ProductName ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			const sev =
				severityFilter === 'all' ||
				(f.Severity?.Label ?? 'INFORMATIONAL') === severityFilter;
			const status = statusFilter === 'all' || (f.Workflow?.Status ?? 'NEW') === statusFilter;
			return text && sev && status;
		})
	);

	function severityBadge(label?: string) {
		if (label === 'CRITICAL') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		if (label === 'HIGH') return 'text-orange-700 bg-orange-100 dark:text-orange-300 dark:bg-orange-900';
		if (label === 'MEDIUM') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (label === 'LOW') return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
		return 'text-muted-foreground bg-muted';
	}

	function complianceBadge(status?: string) {
		if (status === 'PASSED') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (status === 'FAILED') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		if (status === 'WARNING') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		return 'text-muted-foreground bg-muted';
	}

	async function checkHubStatus() {
		try {
			await hub().send(new DescribeHubCommand({}));
			hubEnabled = true;
		} catch (e) {
			hubEnabled = false;
		}
	}

	async function loadFindings() {
		loading = true;
		try {
			const res = await hub().send(
				new GetFindingsCommand({
					MaxResults: 50,
					SortCriteria: [{ Field: 'LastObservedAt', SortOrder: 'desc' }]
				})
			);
			findings = res.Findings ?? [];
		} catch (e) {
			toast.error(`Failed to load findings: ${e}`);
		} finally {
			loading = false;
		}
	}

	// Finding detail + workflow update
	let selectedFinding = $state<AwsSecurityFinding | null>(null);
	let updatingWorkflow = $state(false);

	async function setWorkflowStatus(
		finding: AwsSecurityFinding,
		status: 'NEW' | 'NOTIFIED' | 'RESOLVED' | 'SUPPRESSED'
	) {
		if (!finding.Id || !finding.ProductArn) return;
		updatingWorkflow = true;
		try {
			await hub().send(
				new BatchUpdateFindingsCommand({
					FindingIdentifiers: [{ Id: finding.Id, ProductArn: finding.ProductArn }],
					Workflow: { Status: status }
				})
			);
			toast.success(`Workflow set to ${status}`);
			selectedFinding = null;
			await loadFindings();
		} catch (e) {
			toast.error(`Failed to update finding: ${e}`);
		} finally {
			updatingWorkflow = false;
		}
	}

	async function loadInsights() {
		loading = true;
		try {
			const res = await hub().send(new GetInsightsCommand({ MaxResults: 50 }));
			insights = res.Insights ?? [];
		} catch (e) {
			toast.error(`Failed to load insights: ${e}`);
		} finally {
			loading = false;
		}
	}

	// ── Custom insight creation (CreateInsight / DeleteInsight) ──────────────
	let showInsightModal = $state(false);
	let savingInsight = $state(false);
	let deletingInsight = $state<string | null>(null);
	let newInsightName = $state('');
	let newInsightGroupBy = $state('ResourceType');
	let newInsightSeverity = $state<'all' | 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW'>('all');

	const groupByAttributes = [
		'ResourceType',
		'SeverityLabel',
		'ProductName',
		'WorkflowStatus',
		'ComplianceStatus',
		'RecordState',
		'ResourceId'
	];

	function openCreateInsight() {
		newInsightName = '';
		newInsightGroupBy = 'ResourceType';
		newInsightSeverity = 'all';
		showInsightModal = true;
	}

	async function createInsight() {
		if (!newInsightName.trim()) {
			toast.error('Insight name is required');
			return;
		}
		savingInsight = true;
		try {
			const filters: AwsSecurityFindingFilters =
				newInsightSeverity === 'all'
					? { RecordState: [{ Value: 'ACTIVE', Comparison: 'EQUALS' }] }
					: { SeverityLabel: [{ Value: newInsightSeverity, Comparison: 'EQUALS' }] };
			await hub().send(
				new CreateInsightCommand({
					Name: newInsightName.trim(),
					Filters: filters,
					GroupByAttribute: newInsightGroupBy
				})
			);
			toast.success(`Insight "${newInsightName.trim()}" created`);
			showInsightModal = false;
			await loadInsights();
		} catch (e) {
			toast.error(`Failed to create insight: ${e}`);
		} finally {
			savingInsight = false;
		}
	}

	async function deleteInsight(arn: string) {
		deletingInsight = arn;
		try {
			await hub().send(new DeleteInsightCommand({ InsightArn: arn }));
			toast.success('Insight deleted');
			await loadInsights();
		} catch (e) {
			toast.error(`Failed to delete insight: ${e}`);
		} finally {
			deletingInsight = null;
		}
	}

	// Compute severity counts for summary cards
	const criticalCount = $derived(findings.filter((f) => f.Severity?.Label === 'CRITICAL').length);
	const highCount = $derived(findings.filter((f) => f.Severity?.Label === 'HIGH').length);
	const mediumCount = $derived(findings.filter((f) => f.Severity?.Label === 'MEDIUM').length);
	const lowCount = $derived(findings.filter((f) => f.Severity?.Label === 'LOW').length);
	const resolvedCount = $derived(findings.filter((f) => f.Workflow?.Status === 'RESOLVED').length);
	const newCount = $derived(findings.filter((f) => f.Workflow?.Status === 'NEW').length);
	const suppressedCount = $derived(findings.filter((f) => f.Workflow?.Status === 'SUPPRESSED').length);

	// Group findings by product name
	const productGroups = $derived(() => {
		const groups: Record<string, number> = {};
		for (const f of findings) {
			const prod = f.ProductName ?? 'Unknown';
			groups[prod] = (groups[prod] ?? 0) + 1;
		}
		return Object.entries(groups).toSorted((a, b) => b[1] - a[1]).slice(0, 5);
	});

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'findings') await loadFindings();
		else await loadInsights();
	}

	onRegionChange(async () => {
		await checkHubStatus();
		if (hubEnabled) await loadFindings();
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="h-8 w-8 text-blue-600" />
			<div>
				<h1 class="text-2xl font-bold">Security Hub</h1>
				<p class="text-sm text-muted-foreground">Centralized security posture management</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	{#if hubEnabled === false}
		<div class="rounded-lg border border-yellow-200 bg-yellow-50 p-6 text-center dark:border-yellow-800 dark:bg-yellow-950">
			<AlertTriangle class="h-10 w-10 mx-auto mb-3 text-yellow-500" />
			<p class="font-medium">Security Hub is not enabled</p>
			<p class="text-sm text-muted-foreground mt-1">Enable Security Hub to centralize security findings</p>
		</div>
	{:else}
		<!-- Summary Cards - always visible -->
		<div class="grid gap-4 grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4 text-center">
				<div class="text-2xl font-bold text-red-600">{criticalCount}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400 mt-1 uppercase tracking-wide">Critical</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4 text-center">
				<div class="text-2xl font-bold text-orange-500">{highCount}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400 mt-1 uppercase tracking-wide">High</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4 text-center">
				<div class="text-2xl font-bold text-yellow-500">{mediumCount}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400 mt-1 uppercase tracking-wide">Medium</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4 text-center">
				<div class="text-2xl font-bold text-blue-500">{lowCount}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400 mt-1 uppercase tracking-wide">Low</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4 text-center">
				<div class="text-2xl font-bold text-green-600">{resolvedCount}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400 mt-1 uppercase tracking-wide">Resolved</div>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4 text-center">
				<div class="text-2xl font-bold text-slate-600 dark:text-slate-300">{newCount}</div>
				<div class="text-xs text-slate-500 dark:text-slate-400 mt-1 uppercase tracking-wide">New</div>
			</div>
		</div>

		<!-- Top products bar -->
		{#if productGroups().length > 0}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-3">Top Sources ({findings.length} total findings)</p>
				<div class="space-y-2">
					{#each productGroups() as [prod, count]}
						<div class="flex items-center gap-3">
							<div class="w-32 text-xs text-slate-600 dark:text-slate-400 truncate">{prod}</div>
							<div class="flex-1 bg-slate-100 dark:bg-slate-700 rounded-full h-2 overflow-hidden">
								<div class="h-full bg-blue-500 rounded-full" style="width: {Math.round(count / findings.length * 100)}%"></div>
							</div>
							<div class="text-xs text-slate-500 w-8 text-right">{count}</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
	{/if}

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'findings', label: 'Findings' }, { id: 'insights', label: 'Insights' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
				{#if tab.id === 'findings' && findings.length > 0}
					<span class="ml-1.5 rounded-full bg-muted px-1.5 py-0.5 text-xs">{findings.length}</span>
				{/if}
			</button>
		{/each}
	</div>

	<!-- Findings Tab -->
	{#if activeTab === 'findings'}
		<div class="flex flex-wrap gap-3">
			<div class="relative flex-1 min-w-[180px]">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search findings..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<select
				bind:value={severityFilter}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Severities</option>
				<option value="CRITICAL">Critical</option>
				<option value="HIGH">High</option>
				<option value="MEDIUM">Medium</option>
				<option value="LOW">Low</option>
			</select>
			<select
				bind:value={statusFilter}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Statuses</option>
				<option value="NEW">New</option>
				<option value="NOTIFIED">Notified</option>
				<option value="RESOLVED">Resolved</option>
				<option value="SUPPRESSED">Suppressed</option>
			</select>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredFindings.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<CheckCircle class="h-12 w-12 mb-3 opacity-30 text-green-500" />
				<p>No findings found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Severity</th>
							<th class="px-4 py-3 text-left font-medium">Title</th>
							<th class="px-4 py-3 text-left font-medium">Product</th>
							<th class="px-4 py-3 text-left font-medium">Compliance</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredFindings as finding}
							<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => { selectedFinding = finding; }}>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {severityBadge(finding.Severity?.Label)}">
										{finding.Severity?.Label ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 font-medium max-w-[250px] truncate">
									<button class="text-left hover:text-primary hover:underline">{finding.Title ?? '—'}</button>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{finding.ProductName ?? '—'}</td>
								<td class="px-4 py-3">
									{#if finding.Compliance?.Status}
										<span class="rounded-full px-2 py-0.5 text-xs font-medium {complianceBadge(finding.Compliance.Status)}">
											{finding.Compliance.Status}
										</span>
									{:else}
										<span class="text-muted-foreground">—</span>
									{/if}
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{finding.Workflow?.Status ?? 'NEW'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Insights Tab -->
	{#if activeTab === 'insights'}
		<div class="mb-3 flex justify-end">
			<button
				onclick={openCreateInsight}
				class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Insight
			</button>
		</div>
		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if insights.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<BarChart2 class="h-12 w-12 mb-3 opacity-30" />
				<p>No insights found</p>
			</div>
		{:else}
			<div class="space-y-3">
				{#each insights as insight}
					<div class="flex items-start justify-between gap-3 rounded-lg border p-4">
						<div class="min-w-0">
							<div class="font-medium">{insight.Name}</div>
							{#if insight.GroupByAttribute}
								<div class="mt-0.5 text-xs text-muted-foreground">Grouped by: <span class="font-medium">{insight.GroupByAttribute}</span></div>
							{/if}
							<div class="text-xs text-muted-foreground mt-1 font-mono truncate">{insight.InsightArn}</div>
						</div>
						<button
							onclick={() => insight.InsightArn && deleteInsight(insight.InsightArn)}
							disabled={deletingInsight === insight.InsightArn}
							class="flex-shrink-0 rounded p-1.5 text-red-500 hover:bg-red-50 disabled:opacity-50 dark:hover:bg-red-950"
							title="Delete insight"
							aria-label="Delete insight"
						>
							{#if deletingInsight === insight.InsightArn}
								<div class="h-4 w-4 animate-spin rounded-full border-b-2 border-red-500"></div>
							{:else}
								<Trash2 class="h-4 w-4" />
							{/if}
						</button>
					</div>
				{/each}
			</div>
		{/if}
	{/if}
</div>

<!-- Create Insight Modal -->
{#if showInsightModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showInsightModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showInsightModal = false)} role="dialog" aria-modal="true">
		<div class="w-full max-w-md rounded-lg border bg-background shadow-xl" role="document">
			<div class="flex items-center justify-between border-b px-5 py-3">
				<h2 class="text-sm font-semibold">Create Custom Insight</h2>
				<button onclick={() => { showInsightModal = false; }} class="text-muted-foreground hover:text-foreground" aria-label="Close"><X class="h-5 w-5" /></button>
			</div>
			<form class="space-y-4 p-5" onsubmit={(e) => { e.preventDefault(); createInsight(); }}>
				<div>
					<label for="insight-name" class="mb-1 block text-sm font-medium">Name</label>
					<input id="insight-name" type="text" bind:value={newInsightName} placeholder="High-severity findings by resource" required class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
				</div>
				<div>
					<label for="insight-groupby" class="mb-1 block text-sm font-medium">Group by attribute</label>
					<select id="insight-groupby" bind:value={newInsightGroupBy} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						{#each groupByAttributes as attr}
							<option value={attr}>{attr}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="insight-severity" class="mb-1 block text-sm font-medium">Severity filter</label>
					<select id="insight-severity" bind:value={newInsightSeverity} class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
						<option value="all">All active findings</option>
						<option value="CRITICAL">Critical only</option>
						<option value="HIGH">High only</option>
						<option value="MEDIUM">Medium only</option>
						<option value="LOW">Low only</option>
					</select>
				</div>
				<div class="flex justify-end gap-2 pt-1">
					<button type="button" onclick={() => { showInsightModal = false; }} class="rounded-md border px-4 py-2 text-sm hover:bg-accent">Cancel</button>
					<button type="submit" disabled={savingInsight} class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">{savingInsight ? 'Creating...' : 'Create Insight'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Finding Detail Drawer -->
{#if selectedFinding}
	<div class="fixed inset-0 z-50 flex justify-end bg-black/40">
		<div class="h-full w-full max-w-xl overflow-y-auto bg-background shadow-xl">
			<div class="sticky top-0 flex items-center justify-between border-b bg-background px-5 py-3">
				<div class="flex items-center gap-2">
					<Shield class="h-5 w-5 text-blue-500" />
					<h2 class="text-sm font-semibold">Finding Detail</h2>
					<span class="rounded-full px-2 py-0.5 text-xs font-medium {severityBadge(selectedFinding.Severity?.Label)}">{selectedFinding.Severity?.Label ?? "—"}</span>
				</div>
				<button onclick={() => { selectedFinding = null; }} class="text-muted-foreground hover:text-foreground"><X class="h-5 w-5" /></button>
			</div>
			<div class="space-y-4 p-5">
				<div>
					<h3 class="text-base font-semibold">{selectedFinding.Title ?? "—"}</h3>
					<p class="mt-1 text-sm text-muted-foreground">{selectedFinding.Description ?? ""}</p>
				</div>
				<div class="grid grid-cols-2 gap-3 text-sm">
					{#each [{ label: "Product", value: selectedFinding.ProductName ?? "—" }, { label: "Compliance", value: selectedFinding.Compliance?.Status ?? "—" }, { label: "Workflow", value: selectedFinding.Workflow?.Status ?? "NEW" }, { label: "Record State", value: selectedFinding.RecordState ?? "—" }, { label: "First Observed", value: selectedFinding.FirstObservedAt ?? "—" }, { label: "Last Observed", value: selectedFinding.LastObservedAt ?? "—" }] as row}
						<div class="rounded-md border p-3">
							<div class="text-xs text-muted-foreground">{row.label}</div>
							<div class="mt-0.5 truncate font-mono text-xs">{row.value}</div>
						</div>
					{/each}
				</div>
				{#if selectedFinding.Remediation?.Recommendation}
					<div class="rounded-md border border-blue-200 bg-blue-50 p-3 dark:border-blue-900/40 dark:bg-blue-900/20">
						<div class="mb-1 text-xs font-semibold text-blue-700 dark:text-blue-300">Remediation</div>
						<p class="text-sm text-blue-900 dark:text-blue-200">{selectedFinding.Remediation.Recommendation.Text ?? ""}</p>
						{#if selectedFinding.Remediation.Recommendation.Url}
							<a href={selectedFinding.Remediation.Recommendation.Url} target="_blank" rel="noopener noreferrer" class="mt-1 inline-block text-xs text-blue-600 underline">View guidance</a>
						{/if}
					</div>
				{/if}
				{#if (selectedFinding.Resources ?? []).length > 0}
					<div>
						<div class="mb-1 text-xs font-medium text-muted-foreground">Affected Resources</div>
						<div class="rounded-md border divide-y">
							{#each selectedFinding.Resources ?? [] as res}
								<div class="p-3 text-xs">
									<span class="rounded bg-muted px-1.5 py-0.5 font-medium">{res.Type ?? "—"}</span>
									<span class="ml-2 font-mono text-muted-foreground break-all">{res.Id ?? "—"}</span>
									{#if res.Region}<span class="ml-2 text-muted-foreground">({res.Region})</span>{/if}
								</div>
							{/each}
						</div>
					</div>
				{/if}
				<div class="border-t pt-4">
					<div class="mb-2 text-xs font-medium text-muted-foreground">Set Workflow Status</div>
					<div class="flex flex-wrap gap-2">
						{#each (['NEW', 'NOTIFIED', 'RESOLVED', 'SUPPRESSED'] as const) as st}
							<button onclick={() => setWorkflowStatus(selectedFinding!, st)} disabled={updatingWorkflow || selectedFinding.Workflow?.Status === st} class="rounded-md border px-3 py-1.5 text-xs hover:bg-accent disabled:opacity-40">{st}</button>
						{/each}
					</div>
				</div>
			</div>
		</div>
	</div>
{/if}
