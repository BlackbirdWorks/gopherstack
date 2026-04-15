<script lang="ts">
	import { onMount } from 'svelte';
	import { getInspectorClient } from '$lib/aws-client';
	import {
		ListFindingsCommand,
		ListCoverageCommand,
		GetFindingsReportStatusCommand,
		DescribeOrganizationConfigurationCommand,
		type Finding,
		type CoveredResource
	} from '@aws-sdk/client-inspector2';
	import { toast } from 'svelte-sonner';
	import {
		Search,
		RefreshCw,
		AlertTriangle,
		CheckCircle,
		ShieldCheck,
		Filter
	} from 'lucide-svelte';

	const inspector = getInspectorClient();

	let loading = $state(false);
	let activeTab = $state<'findings' | 'coverage'>('findings');
	let searchQuery = $state('');
	let severityFilter = $state<'all' | 'CRITICAL' | 'HIGH' | 'MEDIUM' | 'LOW'>('all');

	let findings = $state<Finding[]>([]);
	let coverage = $state<CoveredResource[]>([]);

	const filteredFindings = $derived(
		findings.filter((f) => {
			const text =
				(f.title ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(f.packageVulnerabilityDetails?.vulnerabilityId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(f.resources?.[0]?.id ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			const sev =
				severityFilter === 'all' || f.severity === severityFilter;
			return text && sev;
		})
	);

	const filteredCoverage = $derived(
		coverage.filter(
			(c) =>
				(c.resourceId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(c.resourceType ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function severityBadge(sev?: string) {
		if (sev === 'CRITICAL') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		if (sev === 'HIGH') return 'text-orange-700 bg-orange-100 dark:text-orange-300 dark:bg-orange-900';
		if (sev === 'MEDIUM') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (sev === 'LOW') return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
		return 'text-muted-foreground bg-muted';
	}

	function fixStateBadge(state?: string) {
		if (state === 'FIXED') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'ACTIVE') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	const criticalCount = $derived(findings.filter((f) => f.severity === 'CRITICAL').length);
	const highCount = $derived(findings.filter((f) => f.severity === 'HIGH').length);

	async function loadFindings() {
		loading = true;
		try {
			const res = await inspector.send(new ListFindingsCommand({ maxResults: 50 }));
			findings = res.findings ?? [];
		} catch (e) {
			toast.error(`Failed to load findings: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadCoverage() {
		loading = true;
		try {
			const res = await inspector.send(new ListCoverageCommand({ maxResults: 100 }));
			coverage = res.coveredResources ?? [];
		} catch (e) {
			toast.error(`Failed to load coverage: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'findings') await loadFindings();
		else await loadCoverage();
	}

	onMount(() => loadFindings());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ShieldCheck class="h-8 w-8 text-green-600" />
			<div>
				<h1 class="text-2xl font-bold">Amazon Inspector</h1>
				<p class="text-sm text-muted-foreground">Automated vulnerability management for workloads</p>
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

	<!-- Summary Cards -->
	{#if findings.length > 0}
		<div class="grid gap-4 sm:grid-cols-3">
			<div class="rounded-lg border p-4 text-center">
				<div class="text-2xl font-bold text-red-600">{criticalCount}</div>
				<div class="text-sm text-muted-foreground mt-1">Critical</div>
			</div>
			<div class="rounded-lg border p-4 text-center">
				<div class="text-2xl font-bold text-orange-500">{highCount}</div>
				<div class="text-sm text-muted-foreground mt-1">High</div>
			</div>
			<div class="rounded-lg border p-4 text-center">
				<div class="text-2xl font-bold">{findings.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Total Findings</div>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'findings', label: 'Findings' }, { id: 'coverage', label: 'Coverage' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Findings Tab -->
	{#if activeTab === 'findings'}
		<div class="flex flex-wrap gap-3">
			<div class="relative flex-1 min-w-[200px]">
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
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredFindings.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<CheckCircle class="h-12 w-12 mb-3 opacity-30 text-green-500" />
				<p>No findings found</p>
				<p class="text-sm">Your resources look clean!</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Severity</th>
							<th class="px-4 py-3 text-left font-medium">Title</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">Resource</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredFindings as finding}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {severityBadge(finding.severity)}">
										{finding.severity ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 font-medium max-w-[200px] truncate">{finding.title ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">{finding.type ?? '—'}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[150px]">
									{finding.resources?.[0]?.id ?? '—'}
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {fixStateBadge(finding.fixAvailable)}">
										{finding.fixAvailable ?? '—'}
									</span>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- Coverage Tab -->
	{#if activeTab === 'coverage'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search covered resources..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredCoverage.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<ShieldCheck class="h-12 w-12 mb-3 opacity-30" />
				<p>No covered resources</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Resource ID</th>
							<th class="px-4 py-3 text-left font-medium">Type</th>
							<th class="px-4 py-3 text-left font-medium">Account</th>
							<th class="px-4 py-3 text-left font-medium">Region</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredCoverage as resource}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-mono text-xs font-medium truncate max-w-[200px]">{resource.resourceId}</td>
								<td class="px-4 py-3 text-muted-foreground">{resource.resourceType}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">{resource.accountId}</td>
								<td class="px-4 py-3 text-muted-foreground">{resource.resourceMetadata?.ec2Metadata?.platform ?? '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
