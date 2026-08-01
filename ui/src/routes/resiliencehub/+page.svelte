<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getResiliencehubClient } from '$lib/aws-client';
	import {
		ListAppsCommand,
		CreateAppCommand,
		DescribeAppCommand,
		UpdateAppCommand,
		DeleteAppCommand,
		PublishAppVersionCommand,
		StartAppAssessmentCommand,
		ListResiliencyPoliciesCommand,
		CreateResiliencyPolicyCommand,
		DescribeResiliencyPolicyCommand,
		UpdateResiliencyPolicyCommand,
		DeleteResiliencyPolicyCommand,
		ListAppAssessmentsCommand,
		DeleteAppAssessmentCommand,
		type AppSummary,
		type App,
		type ResiliencyPolicy,
		type AppAssessmentSummary,
		type AppAssessmentScheduleType,
		type DataLocationConstraint,
		type ResiliencyPolicyTier,
		type DisruptionType,
		type FailurePolicy
	} from '@aws-sdk/client-resiliencehub';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { ShieldCheck, Plus, Trash2, Eye, Pencil, UploadCloud, PlayCircle } from 'lucide-svelte';

	// Resilience Hub has three top-level listable resource families that
	// don't require a parent id to list: Applications, Resiliency Policies,
	// and Assessments (ListAppAssessments' `appArn` filter is optional).
	//
	// Deliberately NOT built here (see project report, not invented as UI):
	//  - App components/resources, resource mappings, recommendations,
	//    drifts, SOP/alarm/test recommendations, metrics, input sources,
	//    recommendation templates: every one of these nests under a specific
	//    app *version* (draft or published), most requiring an import/
	//    resolve step first (ImportResourcesToDraftAppVersion,
	//    ResolveAppVersionResources) before they are even listable. That is
	//    a multi-step modeling workflow, not a create/delete-shaped resource
	//    list -- out of scope for this floor pass.
	//  - Assessment creation is included (Publish + Start Assessment,
	//    below), since StartAppAssessment's required `appVersion` argument
	//    is directly produced by PublishAppVersion. Assessment DELETE is
	//    included on the Assessments tab. Assessment UPDATE has no
	//    operation in the real API at all (an assessment is a point-in-time
	//    run, not an editable record) -- not offered here.
	const rh = regionalClient(getResiliencehubClient);

	type TabId = 'apps' | 'policies' | 'assessments';
	const tabs: TabDef[] = [
		{ id: 'apps', label: 'Applications' },
		{ id: 'policies', label: 'Resiliency Policies' },
		{ id: 'assessments', label: 'Assessments' }
	];
	let activeTab = $state<TabId>('apps');
	let searchQuery = $state('');

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function complianceClass(status: string | undefined): string {
		if (status === 'PolicyMet') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status === 'PolicyBreached') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	// --- Apps ---

	let apps = $state<AppSummary[]>([]);

	async function fetchApps(): Promise<void> {
		const resp = await rh().send(new ListAppsCommand({}));
		apps = resp.appSummaries ?? [];
	}

	// --- Resiliency policies ---

	let policies = $state<ResiliencyPolicy[]>([]);

	async function fetchPolicies(): Promise<void> {
		const resp = await rh().send(new ListResiliencyPoliciesCommand({}));
		policies = resp.resiliencyPolicies ?? [];
	}

	// --- Assessments ---

	let assessments = $state<AppAssessmentSummary[]>([]);

	async function fetchAssessments(): Promise<void> {
		const resp = await rh().send(new ListAppAssessmentsCommand({}));
		assessments = resp.assessmentSummaries ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		apps: () => fetchApps().catch(rethrowDescribed),
		policies: () => fetchPolicies().catch(rethrowDescribed),
		assessments: () => fetchAssessments().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Every selected-resource ARN (app/policy) is only unique within the
	// region it was fetched from -- clear detail selections on region
	// change, then reload only whichever tab is active.
	onRegionChange(() => {
		selectedApp = null;
		selectedPolicy = null;
		lastPublishedVersion = '';
		tabLoader.refresh(untrack(() => activeTab));
	});

	const filteredApps = $derived(
		apps.filter((a) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (a.name ?? '').toLowerCase().includes(q) || (a.appArn ?? '').toLowerCase().includes(q);
		})
	);
	const filteredPolicies = $derived(
		policies.filter((p) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (p.policyName ?? '').toLowerCase().includes(q) || (p.tier ?? '').toLowerCase().includes(q);
		})
	);
	const filteredAssessments = $derived(
		assessments.filter((a) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(a.assessmentName ?? '').toLowerCase().includes(q) ||
				(a.appArn ?? '').toLowerCase().includes(q) ||
				(a.assessmentStatus ?? '').toLowerCase().includes(q)
			);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- App CRUD ---

	let createAppModal = $state<Modal | null>(null);
	let creatingApp = $state(false);
	let createAppError = $state<string | null>(null);
	let newAppName = $state('');
	let newAppDescription = $state('');
	let newAppPolicyArn = $state('');
	let newAppSchedule = $state<AppAssessmentScheduleType>('Disabled');

	function openCreateAppModal(): void {
		createAppError = null;
		newAppName = '';
		newAppDescription = '';
		newAppPolicyArn = '';
		newAppSchedule = 'Disabled';
		createAppModal?.open();
	}

	async function submitCreateApp(): Promise<void> {
		if (!newAppName.trim()) {
			createAppError = 'Name is required.';
			return;
		}
		creatingApp = true;
		createAppError = null;
		try {
			await rh().send(
				new CreateAppCommand({
					name: newAppName.trim(),
					description: newAppDescription.trim() || undefined,
					policyArn: newAppPolicyArn.trim() || undefined,
					assessmentSchedule: newAppSchedule
				})
			);
			toast.success(`Application "${newAppName}" created`);
			createAppModal?.close();
			await tabLoader.refresh('apps');
		} catch (e) {
			const msg = describeError(e);
			createAppError = msg;
			toast.error(msg);
		} finally {
			creatingApp = false;
		}
	}

	async function handleDeleteApp(a: AppSummary): Promise<void> {
		if (!a.appArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete application',
			message: `Delete application "${a.name ?? a.appArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await rh().send(new DeleteAppCommand({ appArn: a.appArn }));
			toast.success('Application deleted');
			if (selectedApp?.appArn === a.appArn) {
				selectedApp = null;
				appDetailModal?.close();
			}
			await tabLoader.refresh('apps');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let appDetailModal = $state<Modal | null>(null);
	let selectedApp = $state<App | null>(null);
	let appDetailLoading = $state(false);
	let appDetailError = $state<string | null>(null);
	let lastPublishedVersion = $state('');
	let publishing = $state(false);
	let newAssessmentName = $state('');
	let startingAssessment = $state(false);
	let assessmentActionError = $state<string | null>(null);

	async function openAppDetail(a: AppSummary): Promise<void> {
		selectedApp = null;
		appDetailError = null;
		assessmentActionError = null;
		lastPublishedVersion = '';
		newAssessmentName = '';
		appDetailModal?.open();
		if (!a.appArn) return;
		appDetailLoading = true;
		try {
			const resp = await rh().send(new DescribeAppCommand({ appArn: a.appArn }));
			selectedApp = resp.app ?? null;
		} catch (e) {
			appDetailError = describeError(e);
		} finally {
			appDetailLoading = false;
		}
	}

	async function refreshAppDetail(): Promise<void> {
		if (!selectedApp?.appArn) return;
		try {
			const resp = await rh().send(new DescribeAppCommand({ appArn: selectedApp.appArn }));
			selectedApp = resp.app ?? selectedApp;
		} catch (e) {
			appDetailError = describeError(e);
		}
	}

	async function publishVersion(): Promise<void> {
		if (!selectedApp?.appArn) return;
		publishing = true;
		assessmentActionError = null;
		try {
			const resp = await rh().send(new PublishAppVersionCommand({ appArn: selectedApp.appArn }));
			lastPublishedVersion = resp.appVersion ?? '';
			toast.success(`Published version ${lastPublishedVersion}`);
		} catch (e) {
			const msg = describeError(e);
			assessmentActionError = msg;
			toast.error(msg);
		} finally {
			publishing = false;
		}
	}

	async function startAssessment(): Promise<void> {
		if (!selectedApp?.appArn || !lastPublishedVersion || !newAssessmentName.trim()) return;
		startingAssessment = true;
		assessmentActionError = null;
		try {
			await rh().send(
				new StartAppAssessmentCommand({
					appArn: selectedApp.appArn,
					appVersion: lastPublishedVersion,
					assessmentName: newAssessmentName.trim()
				})
			);
			toast.success('Assessment started');
			newAssessmentName = '';
			await tabLoader.refresh('assessments');
		} catch (e) {
			const msg = describeError(e);
			assessmentActionError = msg;
			toast.error(msg);
		} finally {
			startingAssessment = false;
		}
	}

	let editAppModal = $state<Modal | null>(null);
	let editingApp = $state(false);
	let editAppError = $state<string | null>(null);
	let editAppArn = $state('');
	let editAppDescription = $state('');
	let editAppPolicyArn = $state('');
	let editAppSchedule = $state<AppAssessmentScheduleType>('Disabled');

	function openEditAppModal(a: App): void {
		editAppError = null;
		editAppArn = a.appArn ?? '';
		editAppDescription = a.description ?? '';
		editAppPolicyArn = a.policyArn ?? '';
		editAppSchedule = a.assessmentSchedule ?? 'Disabled';
		editAppModal?.open();
	}

	async function submitEditApp(): Promise<void> {
		if (!editAppArn) return;
		editingApp = true;
		editAppError = null;
		try {
			await rh().send(
				new UpdateAppCommand({
					appArn: editAppArn,
					description: editAppDescription.trim() || undefined,
					policyArn: editAppPolicyArn.trim() || undefined,
					assessmentSchedule: editAppSchedule
				})
			);
			toast.success('Application updated');
			editAppModal?.close();
			await tabLoader.refresh('apps');
			await refreshAppDetail();
		} catch (e) {
			const msg = describeError(e);
			editAppError = msg;
			toast.error(msg);
		} finally {
			editingApp = false;
		}
	}

	// --- Resiliency policy CRUD ---

	const DISRUPTION_TYPES = ['AZ', 'Hardware', 'Region', 'Software'] as const;

	function defaultDisruptionPolicy(): Record<DisruptionType, FailurePolicy> {
		return {
			AZ: { rtoInSecs: 3600, rpoInSecs: 3600 },
			Hardware: { rtoInSecs: 3600, rpoInSecs: 3600 },
			Region: { rtoInSecs: 86400, rpoInSecs: 86400 },
			Software: { rtoInSecs: 3600, rpoInSecs: 3600 }
		};
	}

	let createPolicyModal = $state<Modal | null>(null);
	let creatingPolicy = $state(false);
	let createPolicyError = $state<string | null>(null);
	let newPolicyName = $state('');
	let newPolicyDescription = $state('');
	let newPolicyTier = $state<ResiliencyPolicyTier>('Important');
	let newPolicyDataLocation = $state<DataLocationConstraint>('AnyLocation');
	let newPolicyDisruption = $state(defaultDisruptionPolicy());

	function openCreatePolicyModal(): void {
		createPolicyError = null;
		newPolicyName = '';
		newPolicyDescription = '';
		newPolicyTier = 'Important';
		newPolicyDataLocation = 'AnyLocation';
		newPolicyDisruption = defaultDisruptionPolicy();
		createPolicyModal?.open();
	}

	async function submitCreatePolicy(): Promise<void> {
		if (!newPolicyName.trim()) {
			createPolicyError = 'Policy name is required.';
			return;
		}
		creatingPolicy = true;
		createPolicyError = null;
		try {
			await rh().send(
				new CreateResiliencyPolicyCommand({
					policyName: newPolicyName.trim(),
					policyDescription: newPolicyDescription.trim() || undefined,
					tier: newPolicyTier,
					dataLocationConstraint: newPolicyDataLocation,
					policy: newPolicyDisruption as Partial<Record<DisruptionType, FailurePolicy>>
				})
			);
			toast.success(`Policy "${newPolicyName}" created`);
			createPolicyModal?.close();
			await tabLoader.refresh('policies');
		} catch (e) {
			const msg = describeError(e);
			createPolicyError = msg;
			toast.error(msg);
		} finally {
			creatingPolicy = false;
		}
	}

	async function handleDeletePolicy(p: ResiliencyPolicy): Promise<void> {
		if (!p.policyArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete resiliency policy',
			message: `Delete policy "${p.policyName ?? p.policyArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await rh().send(new DeleteResiliencyPolicyCommand({ policyArn: p.policyArn }));
			toast.success('Policy deleted');
			if (selectedPolicy?.policyArn === p.policyArn) {
				selectedPolicy = null;
				policyDetailModal?.close();
			}
			await tabLoader.refresh('policies');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let policyDetailModal = $state<Modal | null>(null);
	let selectedPolicy = $state<ResiliencyPolicy | null>(null);
	let policyDetailLoading = $state(false);
	let policyDetailError = $state<string | null>(null);

	async function openPolicyDetail(p: ResiliencyPolicy): Promise<void> {
		selectedPolicy = null;
		policyDetailError = null;
		policyDetailModal?.open();
		if (!p.policyArn) return;
		policyDetailLoading = true;
		try {
			const resp = await rh().send(new DescribeResiliencyPolicyCommand({ policyArn: p.policyArn }));
			selectedPolicy = resp.policy ?? null;
		} catch (e) {
			policyDetailError = describeError(e);
		} finally {
			policyDetailLoading = false;
		}
	}

	let editPolicyModal = $state<Modal | null>(null);
	let editingPolicy = $state(false);
	let editPolicyError = $state<string | null>(null);
	let editPolicyArn = $state('');
	let editPolicyName = $state('');
	let editPolicyDescription = $state('');
	let editPolicyTier = $state<ResiliencyPolicyTier>('Important');

	function openEditPolicyModal(p: ResiliencyPolicy): void {
		editPolicyError = null;
		editPolicyArn = p.policyArn ?? '';
		editPolicyName = p.policyName ?? '';
		editPolicyDescription = p.policyDescription ?? '';
		editPolicyTier = p.tier ?? 'Important';
		editPolicyModal?.open();
	}

	async function submitEditPolicy(): Promise<void> {
		if (!editPolicyArn) return;
		editingPolicy = true;
		editPolicyError = null;
		try {
			await rh().send(
				new UpdateResiliencyPolicyCommand({
					policyArn: editPolicyArn,
					policyName: editPolicyName.trim() || undefined,
					policyDescription: editPolicyDescription.trim() || undefined,
					tier: editPolicyTier
				})
			);
			toast.success('Policy updated');
			editPolicyModal?.close();
			await tabLoader.refresh('policies');
			if (selectedPolicy?.policyArn === editPolicyArn) await openPolicyDetail(selectedPolicy);
		} catch (e) {
			const msg = describeError(e);
			editPolicyError = msg;
			toast.error(msg);
		} finally {
			editingPolicy = false;
		}
	}

	// --- Assessment delete (see header note: no update op exists) ---

	async function handleDeleteAssessment(a: AppAssessmentSummary): Promise<void> {
		if (!a.assessmentArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete assessment',
			message: `Delete assessment "${a.assessmentName ?? a.assessmentArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await rh().send(new DeleteAppAssessmentCommand({ assessmentArn: a.assessmentArn }));
			toast.success('Assessment deleted');
			await tabLoader.refresh('assessments');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={ShieldCheck}
		title="AWS Resilience Hub"
		description="Assess, monitor, and optimize application resilience"
		onRefresh={handleRefresh}
		color="emerald"
	>
		{#snippet actions()}
			{#if activeTab === 'apps'}
				<button onclick={openCreateAppModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm">
					<Plus class="w-4 h-4" /> Create application
				</button>
			{:else if activeTab === 'policies'}
				<button onclick={openCreatePolicyModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm">
					<Plus class="w-4 h-4" /> Create policy
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="emerald" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'apps'}
				{#snippet appComplianceCell(a: AppSummary)}
					<span class="text-xs px-2 py-1 rounded-full {complianceClass(a.complianceStatus)}">{a.complianceStatus ?? '—'}</span>
				{/snippet}
				{#snippet appActionsCell(a: AppSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openAppDetail(a)} title="View" aria-label="View application {a.name}" class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteApp(a)} title="Delete" aria-label="Delete application {a.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const appColumns = defineColumns<AppSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'complianceStatus', label: 'Compliance', render: appComplianceCell },
					{ key: 'resiliencyScore', label: 'Score' },
					{ key: 'assessmentSchedule', label: 'Schedule' },
					{ key: 'actions', label: '', render: appActionsCell }
				])}
				<DataTable
					rows={filteredApps}
					rowKey={(a) => a.appArn ?? ''}
					columns={appColumns}
					loading={tabLoader.isLoading('apps')}
					emptyMessage="No applications found"
				/>
			{:else if activeTab === 'policies'}
				{#snippet policyTierCell(p: ResiliencyPolicy)}
					<span class="text-xs px-2 py-1 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400">{p.tier ?? '—'}</span>
				{/snippet}
				{#snippet policyActionsCell(p: ResiliencyPolicy)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openPolicyDetail(p)} title="View" aria-label="View policy {p.policyName}" class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeletePolicy(p)} title="Delete" aria-label="Delete policy {p.policyName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const policyColumns = defineColumns<ResiliencyPolicy>([
					{ key: 'policyName', label: 'Name' },
					{ key: 'tier', label: 'Tier', render: policyTierCell },
					{ key: 'dataLocationConstraint', label: 'Data location' },
					{ key: 'estimatedCostTier', label: 'Cost tier' },
					{ key: 'actions', label: '', render: policyActionsCell }
				])}
				<DataTable
					rows={filteredPolicies}
					rowKey={(p) => p.policyArn ?? ''}
					columns={policyColumns}
					loading={tabLoader.isLoading('policies')}
					emptyMessage="No resiliency policies found"
				/>
			{:else if activeTab === 'assessments'}
				{#snippet assessmentComplianceCell(a: AppAssessmentSummary)}
					<span class="text-xs px-2 py-1 rounded-full {complianceClass(a.complianceStatus)}">{a.complianceStatus ?? '—'}</span>
				{/snippet}
				{#snippet assessmentStartedCell(a: AppAssessmentSummary)}
					{formatDate(a.startTime)}
				{/snippet}
				{#snippet assessmentActionsCell(a: AppAssessmentSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => handleDeleteAssessment(a)} title="Delete" aria-label="Delete assessment {a.assessmentName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const assessmentColumns = defineColumns<AppAssessmentSummary>([
					{ key: 'assessmentName', label: 'Name' },
					{ key: 'assessmentStatus', label: 'Status' },
					{ key: 'complianceStatus', label: 'Compliance', render: assessmentComplianceCell },
					{ key: 'startTime', label: 'Started', render: assessmentStartedCell },
					{ key: 'actions', label: '', render: assessmentActionsCell }
				])}
				<DataTable
					rows={filteredAssessments}
					rowKey={(a) => a.assessmentArn ?? ''}
					columns={assessmentColumns}
					loading={tabLoader.isLoading('assessments')}
					emptyMessage="No assessments found"
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createAppModal} title="Create Application">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rh-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="rh-new-name" bind:value={newAppName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rh-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="rh-new-desc" bind:value={newAppDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rh-new-policy" class="text-sm text-slate-600 dark:text-slate-300">Resiliency policy ARN</label>
				<select id="rh-new-policy" bind:value={newAppPolicyArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">(none)</option>
					{#each policies as p (p.policyArn)}
						<option value={p.policyArn}>{p.policyName}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="rh-new-schedule" class="text-sm text-slate-600 dark:text-slate-300">Assessment schedule</label>
				<select id="rh-new-schedule" bind:value={newAppSchedule} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="Disabled">Disabled</option>
					<option value="Daily">Daily</option>
				</select>
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Permission model and event subscriptions are not editable here -- see the project follow-up notes.</p>
			{#if createAppError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAppError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createAppModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateApp} disabled={creatingApp} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{creatingApp ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editAppModal} title="Edit Application">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rh-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="rh-edit-desc" bind:value={editAppDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rh-edit-policy" class="text-sm text-slate-600 dark:text-slate-300">Resiliency policy ARN</label>
				<select id="rh-edit-policy" bind:value={editAppPolicyArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">(none)</option>
					{#each policies as p (p.policyArn)}
						<option value={p.policyArn}>{p.policyName}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="rh-edit-schedule" class="text-sm text-slate-600 dark:text-slate-300">Assessment schedule</label>
				<select id="rh-edit-schedule" bind:value={editAppSchedule} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="Disabled">Disabled</option>
					<option value="Daily">Daily</option>
				</select>
			</div>
			{#if editAppError}
				<p class="text-sm text-red-600 dark:text-red-400">{editAppError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editAppModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditApp} disabled={editingApp} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{editingApp ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={appDetailModal} title="Application">
	{#snippet children()}
		{#if appDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if appDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{appDetailError}</p>
		{:else if selectedApp}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{selectedApp.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{selectedApp.appArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{selectedApp.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Compliance</dt><dd class="text-slate-900 dark:text-white">{selectedApp.complianceStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Resiliency score</dt><dd class="text-slate-900 dark:text-white">{selectedApp.resiliencyScore ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(selectedApp.creationTime)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Publish &amp; assess</dt>
					<dd class="text-slate-900 dark:text-white space-y-2">
						<div class="flex items-center gap-2">
							<button type="button" onclick={publishVersion} disabled={publishing} class="flex items-center gap-1 text-xs px-2 py-1 rounded-lg bg-emerald-600/10 text-emerald-600 hover:bg-emerald-600/20 disabled:opacity-50">
								<UploadCloud class="w-3.5 h-3.5" /> {publishing ? 'Publishing…' : 'Publish version'}
							</button>
							{#if lastPublishedVersion}
								<span class="text-xs text-slate-500 dark:text-slate-400">version {lastPublishedVersion}</span>
							{/if}
						</div>
						{#if lastPublishedVersion}
							<div class="flex items-center gap-2">
								<input bind:value={newAssessmentName} placeholder="Assessment name" aria-label="New assessment name" class="w-1/2 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
								<button type="button" onclick={startAssessment} disabled={startingAssessment || !newAssessmentName.trim()} class="flex items-center gap-1 text-xs px-2 py-1 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50">
									<PlayCircle class="w-3.5 h-3.5" /> {startingAssessment ? 'Starting…' : 'Start assessment'}
								</button>
							</div>
						{/if}
						{#if assessmentActionError}
							<p class="text-sm text-red-600 dark:text-red-400">{assessmentActionError}</p>
						{/if}
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => appDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if selectedApp}
			<button type="button" onclick={() => selectedApp && openEditAppModal(selectedApp)} class="flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={createPolicyModal} title="Create Resiliency Policy">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rp-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="rp-new-name" bind:value={newPolicyName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rp-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="rp-new-desc" bind:value={newPolicyDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rp-new-tier" class="text-sm text-slate-600 dark:text-slate-300">Tier</label>
				<select id="rp-new-tier" bind:value={newPolicyTier} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="MissionCritical">MissionCritical</option>
					<option value="Critical">Critical</option>
					<option value="Important">Important</option>
					<option value="CoreServices">CoreServices</option>
					<option value="NonCritical">NonCritical</option>
				</select>
			</div>
			<div>
				<label for="rp-new-loc" class="text-sm text-slate-600 dark:text-slate-300">Data location constraint</label>
				<select id="rp-new-loc" bind:value={newPolicyDataLocation} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="AnyLocation">AnyLocation</option>
					<option value="SameContinent">SameContinent</option>
					<option value="SameCountry">SameCountry</option>
				</select>
			</div>
			<div>
				<span class="text-sm text-slate-600 dark:text-slate-300">Disruption policy (RTO / RPO, seconds)</span>
				<div class="mt-1 grid grid-cols-1 gap-2">
					{#each DISRUPTION_TYPES as dt (dt)}
						<div class="flex items-center gap-2">
							<span class="w-20 text-xs text-slate-500 dark:text-slate-400">{dt}</span>
							<input type="number" min="0" bind:value={newPolicyDisruption[dt].rtoInSecs} aria-label="{dt} RTO seconds" class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							<input type="number" min="0" bind:value={newPolicyDisruption[dt].rpoInSecs} aria-label="{dt} RPO seconds" class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						</div>
					{/each}
				</div>
			</div>
			{#if createPolicyError}
				<p class="text-sm text-red-600 dark:text-red-400">{createPolicyError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createPolicyModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreatePolicy} disabled={creatingPolicy} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{creatingPolicy ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editPolicyModal} title="Edit Resiliency Policy">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rp-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="rp-edit-name" bind:value={editPolicyName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rp-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="rp-edit-desc" bind:value={editPolicyDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rp-edit-tier" class="text-sm text-slate-600 dark:text-slate-300">Tier</label>
				<select id="rp-edit-tier" bind:value={editPolicyTier} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="MissionCritical">MissionCritical</option>
					<option value="Critical">Critical</option>
					<option value="Important">Important</option>
					<option value="CoreServices">CoreServices</option>
					<option value="NonCritical">NonCritical</option>
				</select>
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Data location constraint and per-disruption-type RTO/RPO are not editable here -- see the project follow-up notes.</p>
			{#if editPolicyError}
				<p class="text-sm text-red-600 dark:text-red-400">{editPolicyError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editPolicyModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditPolicy} disabled={editingPolicy} class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50">{editingPolicy ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={policyDetailModal} title="Resiliency Policy">
	{#snippet children()}
		{#if policyDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if policyDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{policyDetailError}</p>
		{:else if selectedPolicy}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{selectedPolicy.policyName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{selectedPolicy.policyArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Tier</dt><dd class="text-slate-900 dark:text-white">{selectedPolicy.tier ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Data location</dt><dd class="text-slate-900 dark:text-white">{selectedPolicy.dataLocationConstraint ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Cost tier</dt><dd class="text-slate-900 dark:text-white">{selectedPolicy.estimatedCostTier ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(selectedPolicy.creationTime)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Disruption policy</dt>
					<dd class="text-slate-900 dark:text-white">
						<pre class="mt-1 max-h-48 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(selectedPolicy.policy ?? {}, null, 2)}</pre>
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => policyDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if selectedPolicy}
			<button type="button" onclick={() => selectedPolicy && openEditPolicyModal(selectedPolicy)} class="flex items-center gap-2 rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>
