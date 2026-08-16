<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { multiRegionList } from '$lib/multi-region';
	import RegionChip from '$lib/components/RegionChip.svelte';
	import WriteRegionHint from '$lib/components/WriteRegionHint.svelte';
	import { getBackupClient } from '$lib/aws-client';
	import {
		ListBackupPlansCommand,
		ListBackupVaultsCommand,
		ListBackupJobsCommand,
		ListRestoreJobsCommand,
		ListRecoveryPointsByBackupVaultCommand,
		DeleteRecoveryPointCommand,
		CreateBackupPlanCommand,
		DeleteBackupPlanCommand,
		GetBackupPlanCommand,
		type BackupPlansListMember,
		type BackupVaultListMember,
		type BackupJob,
		type RestoreJobsListMember,
		type RecoveryPointByBackupVault
	} from '@aws-sdk/client-backup';
	import { toast } from 'svelte-sonner';
	import {
		Archive,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		CheckCircle,
		XCircle,
		Clock,
		HardDrive,
		Play
	} from 'lucide-svelte';

	const backup = regionalClient(getBackupClient);

	// Every row carries the region its List call was made against.
	// Detail/action calls must build a client for THAT region -- in All mode
	// the same resource name can legitimately exist in two different regions.
	type Regioned<T> = T & { region: string };

	let loading = $state(false);
	let activeTab = $state<'plans' | 'vaults' | 'jobs' | 'recovery-points'>('plans');
	let searchQuery = $state('');

	// Backup Plans
	let plans = $state<Regioned<BackupPlansListMember>[]>([]);
	let selectedPlan = $state<{
		BackupPlanId?: string;
		BackupPlanName?: string;
		Rules?: Array<{ RuleName?: string; TargetBackupVaultName?: string; ScheduleExpression?: string }>;
	} | null>(null);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newPlanName = $state('');
	let newPlanVault = $state('Default');
	let newPlanSchedule = $state('cron(0 5 ? * * *)');

	// Vaults
	let vaults = $state<Regioned<BackupVaultListMember>[]>([]);

	// Recovery Points
	let recoveryPoints = $state<RecoveryPointByBackupVault[]>([]);
	let selectedVaultForRP = $state<Regioned<BackupVaultListMember> | null>(null);
	let rpLoading = $state(false);

	// Jobs
	let jobs = $state<Regioned<BackupJob>[]>([]);
	let restoreJobs = $state<RestoreJobsListMember[]>([]);
	let jobStatus = $state<'all' | 'RUNNING' | 'COMPLETED' | 'FAILED'>('all');

	const filteredPlans = $derived(
		plans.filter((p) => (p.BackupPlanName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredVaults = $derived(
		vaults.filter((v) => (v.BackupVaultName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredJobs = $derived(
		jobs.filter(
			(j) =>
				(jobStatus === 'all' || j.State === jobStatus) &&
				((j.ResourceArn ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
					(j.BackupVaultName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
		)
	);

	function jobStatusBadge(state?: string) {
		if (state === 'COMPLETED') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'RUNNING') return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
		if (state === 'FAILED') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadPlans() {
		loading = true;
		try {
			const result = await multiRegionList(
				(region) => getBackupClient(region).send(new ListBackupPlansCommand({ IncludeDeleted: false })),
				(r) => r.BackupPlansList ?? []
			);
			plans = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load backup plans from ${result.errors.length} region(s)`);
		} catch (e) {
			toast.error(`Failed to load backup plans: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadVaults() {
		loading = true;
		try {
			const result = await multiRegionList(
				(region) => getBackupClient(region).send(new ListBackupVaultsCommand({})),
				(r) => r.BackupVaultList ?? []
			);
			vaults = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load vaults from ${result.errors.length} region(s)`);
		} catch (e) {
			toast.error(`Failed to load vaults: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadJobs() {
		loading = true;
		try {
			const result = await multiRegionList(
				(region) => getBackupClient(region).send(new ListBackupJobsCommand({ MaxResults: 100 })),
				(r) => r.BackupJobs ?? []
			);
			jobs = result.items.map(({ region, item }) => ({ ...item, region }));
			if (result.errors.length > 0) toast.error(`Failed to load jobs from ${result.errors.length} region(s)`);
		} catch (e) {
			toast.error(`Failed to load jobs: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewPlan(plan: Regioned<BackupPlansListMember>) {
		if (!plan.BackupPlanId) return;
		try {
			const res = await getBackupClient(plan.region).send(new GetBackupPlanCommand({ BackupPlanId: plan.BackupPlanId }));
			selectedPlan = {
				BackupPlanId: res.BackupPlanId,
				BackupPlanName: res.BackupPlan?.BackupPlanName,
				Rules: (res.BackupPlan?.Rules ?? []).map((r) => ({
					RuleName: r.RuleName,
					TargetBackupVaultName: r.TargetBackupVaultName,
					ScheduleExpression: r.ScheduleExpression
				}))
			};
		} catch (e) {
			toast.error(`Failed to load plan: ${e}`);
		}
	}

	async function deletePlan(plan: Regioned<BackupPlansListMember>) {
		if (!plan.BackupPlanId || !await confirmDestructive({ title: 'Delete Backup Plan', message: `Delete backup plan "${plan.BackupPlanName}"? Scheduled backups will no longer run.` })) return;
		try {
			await getBackupClient(plan.region).send(new DeleteBackupPlanCommand({ BackupPlanId: plan.BackupPlanId }));
			toast.success(`Plan "${plan.BackupPlanName}" deleted`);
			await loadPlans();
		} catch (e) {
			toast.error(`Failed to delete plan: ${e}`);
		}
	}

	async function createPlan() {
		if (!newPlanName.trim()) return;
		creating = true;
		try {
			await backup().send(
				new CreateBackupPlanCommand({
					BackupPlan: {
						BackupPlanName: newPlanName.trim(),
						Rules: [
							{
								RuleName: 'DailyBackup',
								TargetBackupVaultName: newPlanVault.trim(),
								ScheduleExpression: newPlanSchedule.trim()
							}
						]
					}
				})
			);
			toast.success(`Backup plan "${newPlanName}" created`);
			showCreateModal = false;
			newPlanName = '';
			await loadPlans();
		} catch (e) {
			toast.error(`Failed to create plan: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function loadRecoveryPoints() {
		if (!selectedVaultForRP) return;
		rpLoading = true;
		try {
			const res = await getBackupClient(selectedVaultForRP.region).send(
				new ListRecoveryPointsByBackupVaultCommand({ BackupVaultName: selectedVaultForRP.BackupVaultName })
			);
			recoveryPoints = res.RecoveryPoints ?? [];
		} catch (e) {
			toast.error(`Failed to load recovery points: ${e}`);
		} finally {
			rpLoading = false;
		}
	}

	async function deleteRecoveryPoint(rp: RecoveryPointByBackupVault) {
		if (
			!rp.BackupVaultName ||
			!rp.RecoveryPointArn ||
			!selectedVaultForRP ||
			!(await confirmDestructive({
				title: 'Delete Recovery Point',
				message: `Delete recovery point ${rp.RecoveryPointArn?.slice(-12)}? This cannot be undone.`
			}))
		)
			return;
		try {
			await getBackupClient(selectedVaultForRP.region).send(
				new DeleteRecoveryPointCommand({
					BackupVaultName: rp.BackupVaultName,
					RecoveryPointArn: rp.RecoveryPointArn
				})
			);
			toast.success('Recovery point deleted');
			await loadRecoveryPoints();
		} catch (e) {
			toast.error(`Failed to delete recovery point: ${e}`);
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		if (tab === 'plans') await loadPlans();
		else if (tab === 'vaults') await loadVaults();
		else if (tab === 'recovery-points') await loadVaults();
		else await loadJobs();
	}

	// `onMount` used to unconditionally load the Plans tab because that's
	// always the tab active at first mount. On a region change, though, the
	// user may be viewing any tab, so refresh whichever tab is actually
	// active -- read via `untrack` so switching tabs (which already reloads
	// its own data through onTabChange) doesn't also re-trigger this effect.
	onRegionChange(() => {
		const tab = untrack(() => activeTab);
		selectedPlan = null;
		if (tab === 'plans') {
			loadPlans();
		} else if (tab === 'vaults') {
			loadVaults();
		} else if (tab === 'recovery-points') {
			selectedVaultForRP = null;
			recoveryPoints = [];
			loadVaults();
		} else {
			loadJobs();
		}
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Archive class="h-8 w-8 text-orange-600" />
			<div>
				<h1 class="text-2xl font-bold">AWS Backup</h1>
				<p class="text-sm text-muted-foreground">Centralized backup management for AWS services</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => (activeTab === 'plans' ? loadPlans() : activeTab === 'vaults' ? loadVaults() : loadJobs())}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
			>
				<RefreshCw class="h-4 w-4" />
				Refresh
			</button>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'plans', label: 'Backup Plans' }, { id: 'vaults', label: 'Backup Vaults' }, { id: 'jobs', label: 'Backup Jobs' }, { id: 'recovery-points', label: 'Recovery Points' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Plans Tab -->
	{#if activeTab === 'plans'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search plans..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Plan
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredPlans.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Archive class="h-12 w-12 mb-3 opacity-30" />
				<p>No backup plans found</p>
				<p class="text-sm">Create a plan to automate backups</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Plan Name</th>
							<th class="px-4 py-3 text-left font-medium">Region</th>
							<th class="px-4 py-3 text-left font-medium">Version</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredPlans as plan}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewPlan(plan)}
							>
								<td class="px-4 py-3 font-medium">{plan.BackupPlanName}</td>
								<td class="px-4 py-3"><RegionChip region={plan.region} /></td>
								<td class="px-4 py-3 text-xs text-muted-foreground">{plan.VersionId ?? '—'}</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{plan.CreationDate ? new Date(plan.CreationDate).toLocaleDateString() : '—'}
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={(e) => { e.stopPropagation(); deletePlan(plan); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete plan"
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

		<!-- Plan Details -->
		{#if selectedPlan}
			<div class="rounded-lg border p-4 space-y-3">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold">{selectedPlan.BackupPlanName} — Rules</h3>
					<button onclick={() => (selectedPlan = null)} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>
				<div class="divide-y rounded border overflow-hidden">
					{#each selectedPlan.Rules ?? [] as rule}
						<div class="px-4 py-3 text-sm">
							<div class="font-medium">{rule.RuleName}</div>
							<div class="text-muted-foreground">
								Vault: {rule.TargetBackupVaultName} · Schedule: {rule.ScheduleExpression ?? '—'}
							</div>
						</div>
					{/each}
				</div>
			</div>
		{/if}
	{/if}

	<!-- Vaults Tab -->
	{#if activeTab === 'vaults'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search vaults..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredVaults.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<HardDrive class="h-12 w-12 mb-3 opacity-30" />
				<p>No backup vaults found</p>
			</div>
		{:else}
			<div class="grid gap-3 sm:grid-cols-2 lg:grid-cols-3">
				{#each filteredVaults as vault}
					<div class="rounded-lg border p-4 space-y-2">
						<div class="flex items-center gap-2">
							<HardDrive class="h-5 w-5 text-orange-500" />
							<span class="font-medium truncate">{vault.BackupVaultName}</span>
							<RegionChip region={vault.region} />
						</div>
						<p class="text-xs text-muted-foreground">
							Recovery points: {vault.NumberOfRecoveryPoints ?? 0}
						</p>
						<p class="text-xs text-muted-foreground">
							Encryption: {vault.EncryptionKeyArn ? 'Customer-managed' : 'AWS-managed'}
						</p>
					</div>
				{/each}
			</div>
		{/if}
	{/if}

	<!-- Jobs Tab -->
	{#if activeTab === 'jobs'}
		<div class="flex items-center gap-3">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search jobs..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<select
				bind:value={jobStatus}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Statuses</option>
				<option value="RUNNING">Running</option>
				<option value="COMPLETED">Completed</option>
				<option value="FAILED">Failed</option>
			</select>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredJobs.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Play class="h-12 w-12 mb-3 opacity-30" />
				<p>No backup jobs found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Job ID</th>
							<th class="px-4 py-3 text-left font-medium">Region</th>
							<th class="px-4 py-3 text-left font-medium">Resource</th>
							<th class="px-4 py-3 text-left font-medium">Vault</th>
							<th class="px-4 py-3 text-left font-medium">State</th>
							<th class="px-4 py-3 text-left font-medium">Started</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredJobs as job}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-mono text-xs">{(job.BackupJobId ?? '').slice(0, 8)}…</td>
								<td class="px-4 py-3"><RegionChip region={job.region} /></td>
								<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[200px]">
									{job.ResourceArn ?? '—'}
								</td>
								<td class="px-4 py-3">{job.BackupVaultName ?? '—'}</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {jobStatusBadge(job.State)}">
										{job.State ?? '—'}
									</span>
								</td>
								<td class="px-4 py-3 text-xs text-muted-foreground">
									{job.CreationDate ? new Date(job.CreationDate).toLocaleString() : '—'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Recovery Points Tab -->
{#if activeTab === 'recovery-points'}
	<div class="flex items-center gap-3">
		<select
			bind:value={selectedVaultForRP}
			onchange={loadRecoveryPoints}
			class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
		>
			<option value={null}>Select a vault...</option>
			{#each vaults as vault}
				<option value={vault}>{vault.BackupVaultName} ({vault.region})</option>
			{/each}
		</select>
		{#if selectedVaultForRP}
			<button
				onclick={loadRecoveryPoints}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
			>
				<RefreshCw class="h-4 w-4" />
				Refresh
			</button>
		{/if}
	</div>

	{#if !selectedVaultForRP}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<HardDrive class="h-12 w-12 mb-3 opacity-30" />
			<p>Select a vault to browse recovery points</p>
		</div>
	{:else if rpLoading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if recoveryPoints.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Archive class="h-12 w-12 mb-3 opacity-30" />
			<p>No recovery points found in "{selectedVaultForRP?.BackupVaultName}"</p>
		</div>
	{:else}
		<div class="rounded-lg border overflow-hidden">
			<table class="w-full text-sm">
				<thead class="bg-muted/50">
					<tr>
						<th class="px-4 py-3 text-left font-medium">Recovery Point ARN</th>
						<th class="px-4 py-3 text-left font-medium">Resource</th>
						<th class="px-4 py-3 text-left font-medium">Type</th>
						<th class="px-4 py-3 text-left font-medium">Status</th>
						<th class="px-4 py-3 text-left font-medium">Created</th>
						<th class="px-4 py-3 text-right font-medium">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y">
					{#each recoveryPoints as rp}
						<tr class="hover:bg-muted/30">
							<td class="px-4 py-3 font-mono text-xs">
								{(rp.RecoveryPointArn ?? '').split(':').pop()?.slice(0, 16) ?? '—'}…
							</td>
							<td class="px-4 py-3 text-xs text-muted-foreground truncate max-w-[180px]">
								{rp.ResourceArn ?? '—'}
							</td>
							<td class="px-4 py-3 text-xs">{rp.ResourceType ?? '—'}</td>
							<td class="px-4 py-3">
								<span
									class="rounded-full px-2 py-0.5 text-xs font-medium {rp.Status === 'COMPLETED'
										? 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900'
										: rp.Status === 'PARTIAL'
											? 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900'
											: 'text-muted-foreground bg-muted'}"
								>
									{rp.Status ?? '—'}
								</span>
							</td>
							<td class="px-4 py-3 text-xs text-muted-foreground">
								{rp.CreationDate ? new Date(rp.CreationDate).toLocaleString() : '—'}
							</td>
							<td class="px-4 py-3 text-right">
								<button
									onclick={() => deleteRecoveryPoint(rp)}
									class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
									title="Delete recovery point"
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
		<p class="text-xs text-muted-foreground">{recoveryPoints.length} recovery point{recoveryPoints.length !== 1 ? 's' : ''} in "{selectedVaultForRP?.BackupVaultName}"</p>
	{/if}
{/if}

<!-- Create Plan Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-lg font-semibold">Create Backup Plan</h2>
				<WriteRegionHint />
			</div>
			<div class="space-y-3">
				<div>
					<label for="plan-name" class="block text-sm font-medium mb-1">Plan Name *</label>
					<input
						id="plan-name"
						type="text"
						bind:value={newPlanName}
						placeholder="my-backup-plan"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="plan-vault" class="block text-sm font-medium mb-1">Target Vault</label>
					<input
						id="plan-vault"
						type="text"
						bind:value={newPlanVault}
						placeholder="Default"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="plan-schedule" class="block text-sm font-medium mb-1">Schedule (cron)</label>
					<input
						id="plan-schedule"
						type="text"
						bind:value={newPlanSchedule}
						placeholder="cron(0 5 ? * * *)"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary font-mono"
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
					onclick={createPlan}
					disabled={creating || !newPlanName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Plan'}
				</button>
			</div>
		</div>
	</div>
{/if}
