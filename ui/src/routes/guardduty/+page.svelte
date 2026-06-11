<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getGuardDutyClient } from '$lib/aws-client';
	import {
		ListDetectorsCommand,
		GetDetectorCommand,
		CreateDetectorCommand,
		DeleteDetectorCommand,
		ListFindingsCommand,
		GetFindingsCommand,
		ArchiveFindingsCommand,
		UnarchiveFindingsCommand,
		UpdateDetectorCommand,
		type GetDetectorCommandOutput,
		type Finding
	} from '@aws-sdk/client-guardduty';
	import { toast } from 'svelte-sonner';
	import {
		ShieldAlert,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		AlertTriangle,
		CheckCircle,
		XCircle,
		Filter,
		Archive,
		ArchiveRestore,
		X
	} from 'lucide-svelte';

	const gd = getGuardDutyClient();

	let loading = $state(false);
	let activeTab = $state<'detectors' | 'findings'>('detectors');
	let searchQuery = $state('');

	// Detectors
	let detectorIds = $state<string[]>([]);
	let detectorDetails = $state<Record<string, GetDetectorCommandOutput>>({});
	let selectedDetectorId = $state('');
	let creating = $state(false);

	// Findings
	let findings = $state<Finding[]>([]);
	let loadingFindings = $state(false);
	let severityFilter = $state<'all' | 'HIGH' | 'MEDIUM' | 'LOW'>('all');
	let findingFilter = $state('');

	const filteredFindings = $derived(
		findings.filter((f) => {
			const sev =
				severityFilter === 'all' ||
				(severityFilter === 'HIGH' && (f.Severity ?? 0) >= 7) ||
				(severityFilter === 'MEDIUM' && (f.Severity ?? 0) >= 4 && (f.Severity ?? 0) < 7) ||
				(severityFilter === 'LOW' && (f.Severity ?? 0) < 4);
			const text =
				(f.Title ?? '').toLowerCase().includes(findingFilter.toLowerCase()) ||
				(f.Type ?? '').toLowerCase().includes(findingFilter.toLowerCase());
			return sev && text;
		})
	);

	function severityBadge(severity?: number) {
		if (!severity) return { color: 'text-muted-foreground bg-muted', label: 'Unknown' };
		if (severity >= 7) return { color: 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900', label: 'High' };
		if (severity >= 4) return { color: 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900', label: 'Medium' };
		return { color: 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900', label: 'Low' };
	}

	function detectorStatusBadge(status?: string) {
		if (status === 'ENABLED') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadDetectors() {
		loading = true;
		try {
			const res = await gd.send(new ListDetectorsCommand({}));
			detectorIds = res.DetectorIds ?? [];
			await Promise.all(
				detectorIds.map(async (id) => {
					try {
						const detail = await gd.send(new GetDetectorCommand({ DetectorId: id }));
						detectorDetails[id] = detail;
					} catch {
						// ignore
					}
				})
			);
		} catch (e) {
			toast.error(`Failed to load detectors: ${e}`);
		} finally {
			loading = false;
		}
	}

	// Finding detail + archive
	let selectedFinding = $state<Finding | null>(null);
	let archiving = $state(false);

	async function archiveFinding(finding: Finding, archive: boolean) {
		if (!finding.Id || !selectedDetectorId) return;
		archiving = true;
		try {
			if (archive) {
				await gd.send(
					new ArchiveFindingsCommand({ DetectorId: selectedDetectorId, FindingIds: [finding.Id] })
				);
				toast.success('Finding archived');
			} else {
				await gd.send(
					new UnarchiveFindingsCommand({ DetectorId: selectedDetectorId, FindingIds: [finding.Id] })
				);
				toast.success('Finding unarchived');
			}
			selectedFinding = null;
			await loadFindings(selectedDetectorId);
		} catch (e) {
			toast.error(`Failed to update finding: ${e}`);
		} finally {
			archiving = false;
		}
	}

	async function loadFindings(detectorId: string) {
		loadingFindings = true;
		findings = [];
		try {
			const listRes = await gd.send(
				new ListFindingsCommand({
					DetectorId: detectorId,
					MaxResults: 50
				})
			);
			const findingIds = listRes.FindingIds ?? [];
			if (findingIds.length > 0) {
				const getRes = await gd.send(
					new GetFindingsCommand({ DetectorId: detectorId, FindingIds: findingIds })
				);
				findings = getRes.Findings ?? [];
			}
		} catch (e) {
			toast.error(`Failed to load findings: ${e}`);
		} finally {
			loadingFindings = false;
		}
	}

	async function createDetector() {
		creating = true;
		try {
			const res = await gd.send(
				new CreateDetectorCommand({ Enable: true, FindingPublishingFrequency: 'SIX_HOURS' })
			);
			toast.success(`Detector created: ${res.DetectorId}`);
			await loadDetectors();
		} catch (e) {
			toast.error(`Failed to create detector: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteDetector(id: string) {
		if (!await confirmDestructive({ title: 'Delete Detector', message: `Delete GuardDuty detector ${id}? Threat detection will be disabled for this account.` })) return;
		try {
			await gd.send(new DeleteDetectorCommand({ DetectorId: id }));
			toast.success('Detector deleted');
			delete detectorDetails[id];
			detectorIds = detectorIds.filter((d) => d !== id);
			if (selectedDetectorId === id) {
				selectedDetectorId = '';
				findings = [];
			}
		} catch (e) {
			toast.error(`Failed to delete detector: ${e}`);
		}
	}

	async function toggleDetector(id: string) {
		const current = detectorDetails[id]?.Status;
		const enable = current !== 'ENABLED';
		try {
			await gd.send(new UpdateDetectorCommand({ DetectorId: id, Enable: enable }));
			toast.success(`Detector ${enable ? 'enabled' : 'disabled'}`);
			await loadDetectors();
		} catch (e) {
			toast.error(`Failed to update detector: ${e}`);
		}
	}

	async function selectDetectorForFindings(id: string) {
		selectedDetectorId = id;
		activeTab = 'findings';
		await loadFindings(id);
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		if (tab === 'detectors') await loadDetectors();
		else if (selectedDetectorId) await loadFindings(selectedDetectorId);
	}

	onMount(() => loadDetectors());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ShieldAlert class="h-8 w-8 text-red-600" />
			<div>
				<h1 class="text-2xl font-bold">GuardDuty</h1>
				<p class="text-sm text-muted-foreground">Intelligent threat detection for AWS accounts</p>
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

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'detectors', label: 'Detectors' }, { id: 'findings', label: 'Findings' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
				{#if tab.id === 'findings' && findings.length > 0}
					<span class="ml-1.5 rounded-full bg-red-100 px-1.5 py-0.5 text-xs text-red-700 dark:bg-red-900 dark:text-red-300">
						{findings.length}
					</span>
				{/if}
			</button>
		{/each}
	</div>

	<!-- Detectors Tab -->
	{#if activeTab === 'detectors'}
		<div class="flex items-center justify-between gap-4">
			<div class="text-sm text-muted-foreground">
				{detectorIds.length} detector(s) in this region
			</div>
			<button
				onclick={createDetector}
				disabled={creating}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
			>
				<Plus class="h-4 w-4" />
				{creating ? 'Creating...' : 'Enable GuardDuty'}
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if detectorIds.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<ShieldAlert class="h-12 w-12 mb-3 opacity-30" />
				<p>GuardDuty is not enabled</p>
				<p class="text-sm">Enable GuardDuty to start detecting threats</p>
			</div>
		{:else}
			<div class="space-y-3">
				{#each detectorIds as id}
					{@const detail = detectorDetails[id]}
					<div class="rounded-lg border p-4">
						<div class="flex items-center justify-between">
							<div class="space-y-1">
								<div class="flex items-center gap-2">
									<span class="font-mono text-sm">{id}</span>
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {detectorStatusBadge(detail?.Status)}">
										{detail?.Status ?? '—'}
									</span>
								</div>
								<p class="text-xs text-muted-foreground">
									Created: {detail?.CreatedAt ?? '—'} · Updated: {detail?.UpdatedAt ?? '—'}
								</p>
							</div>
							<div class="flex gap-2">
								<button
									onclick={() => selectDetectorForFindings(id)}
									class="flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs hover:bg-accent"
								>
									<Eye class="h-3.5 w-3.5" />
									Findings
								</button>
								<button
									onclick={() => toggleDetector(id)}
									class="flex items-center gap-1.5 rounded-md border px-3 py-1.5 text-xs hover:bg-accent"
								>
									{#if detail?.Status === 'ENABLED'}
										<XCircle class="h-3.5 w-3.5 text-yellow-500" />
										Disable
									{:else}
										<CheckCircle class="h-3.5 w-3.5 text-green-500" />
										Enable
									{/if}
								</button>
								<button
									onclick={() => deleteDetector(id)}
									class="rounded p-1.5 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</div>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- Findings Tab -->
	{#if activeTab === 'findings'}
		{#if !selectedDetectorId}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<AlertTriangle class="h-12 w-12 mb-3 opacity-30" />
				<p>Select a detector from the Detectors tab to view findings</p>
			</div>
		{:else}
			<div class="flex flex-wrap items-end gap-3 rounded-lg border p-4 bg-muted/20">
				<div class="flex-1 min-w-[200px]">
					<div class="relative">
						<Filter class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
						<input
							type="text"
							bind:value={findingFilter}
							placeholder="Filter findings..."
							class="w-full rounded-md border bg-background pl-9 pr-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
				</div>
				<select
					bind:value={severityFilter}
					class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				>
					<option value="all">All Severities</option>
					<option value="HIGH">High (7+)</option>
					<option value="MEDIUM">Medium (4-7)</option>
					<option value="LOW">Low (&lt;4)</option>
				</select>
				<button
					onclick={() => loadFindings(selectedDetectorId)}
					disabled={loadingFindings}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					<RefreshCw class="h-4 w-4 {loadingFindings ? 'animate-spin' : ''}" />
					{loadingFindings ? 'Loading...' : 'Refresh'}
				</button>
			</div>

			{#if loadingFindings}
				<div class="flex justify-center py-12">
					<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
				</div>
			{:else if filteredFindings.length === 0}
				<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
					<CheckCircle class="h-12 w-12 mb-3 opacity-30 text-green-500" />
					<p>No findings found</p>
					<p class="text-sm">All clear! No threats detected matching your filters.</p>
				</div>
			{:else}
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Severity</th>
								<th class="px-4 py-3 text-left font-medium">Type</th>
								<th class="px-4 py-3 text-left font-medium">Title</th>
								<th class="px-4 py-3 text-left font-medium">Region</th>
								<th class="px-4 py-3 text-left font-medium">Updated</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each filteredFindings as finding}
								{@const badge = severityBadge(finding.Severity)}
								<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => { selectedFinding = finding; }}>
									<td class="px-4 py-3">
										<span class="rounded-full px-2 py-0.5 text-xs font-medium {badge.color}">
											{badge.label} ({finding.Severity?.toFixed(1) ?? '?'})
										</span>
									</td>
									<td class="px-4 py-3 text-xs font-mono text-muted-foreground">{finding.Type ?? '—'}</td>
									<td class="px-4 py-3 font-medium">
										<button class="text-left hover:text-primary hover:underline">{finding.Title ?? '—'}</button>
									</td>
									<td class="px-4 py-3 text-muted-foreground">{finding.Region ?? '—'}</td>
									<td class="px-4 py-3 text-xs text-muted-foreground">
										{finding.UpdatedAt ?? '—'}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>

<!-- Finding Detail Drawer -->
{#if selectedFinding}
	{@const badge = severityBadge(selectedFinding.Severity)}
	<div class="fixed inset-0 z-50 flex justify-end bg-black/40">
		<div class="h-full w-full max-w-xl overflow-y-auto bg-background shadow-xl">
			<div class="sticky top-0 flex items-center justify-between border-b bg-background px-5 py-3">
				<div class="flex items-center gap-2">
					<ShieldAlert class="h-5 w-5 text-rose-500" />
					<h2 class="text-sm font-semibold">Finding Detail</h2>
					<span class="rounded-full px-2 py-0.5 text-xs font-medium {badge.color}">{badge.label} ({selectedFinding.Severity?.toFixed(1) ?? '?'})</span>
				</div>
				<button onclick={() => { selectedFinding = null; }} class="text-muted-foreground hover:text-foreground"><X class="h-5 w-5" /></button>
			</div>
			<div class="space-y-4 p-5">
				<div>
					<h3 class="text-base font-semibold">{selectedFinding.Title ?? '—'}</h3>
					<p class="mt-1 font-mono text-xs text-muted-foreground">{selectedFinding.Type ?? '—'}</p>
				</div>
				{#if selectedFinding.Description}
					<p class="text-sm text-muted-foreground">{selectedFinding.Description}</p>
				{/if}
				<div class="grid grid-cols-2 gap-3 text-sm">
					{#each [
						{ label: 'Region', value: selectedFinding.Region ?? '—' },
						{ label: 'Account', value: selectedFinding.AccountId ?? '—' },
						{ label: 'Resource Type', value: selectedFinding.Resource?.ResourceType ?? '—' },
						{ label: 'Count', value: String(selectedFinding.Service?.Count ?? '—') },
						{ label: 'First Seen', value: selectedFinding.Service?.EventFirstSeen ?? '—' },
						{ label: 'Last Seen', value: selectedFinding.Service?.EventLastSeen ?? '—' },
						{ label: 'Archived', value: selectedFinding.Service?.Archived ? 'Yes' : 'No' }
					] as row}
						<div class="rounded-md border p-3">
							<div class="text-xs text-muted-foreground">{row.label}</div>
							<div class="mt-0.5 truncate font-mono text-xs">{row.value}</div>
						</div>
					{/each}
				</div>
				<div>
					<div class="mb-1 text-xs font-medium text-muted-foreground">Raw Finding</div>
					<pre class="max-h-64 overflow-auto rounded-md border bg-muted/30 p-3 text-xs">{JSON.stringify(selectedFinding, null, 2)}</pre>
				</div>
				<div class="flex justify-end gap-2 border-t pt-4">
					{#if selectedFinding.Service?.Archived}
						<button onclick={() => archiveFinding(selectedFinding!, false)} disabled={archiving} class="flex items-center gap-2 rounded-md border px-4 py-2 text-sm hover:bg-accent disabled:opacity-50">
							<ArchiveRestore class="h-4 w-4" /> Unarchive
						</button>
					{:else}
						<button onclick={() => archiveFinding(selectedFinding!, true)} disabled={archiving} class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50">
							<Archive class="h-4 w-4" /> Archive
						</button>
					{/if}
				</div>
			</div>
		</div>
	</div>
{/if}
