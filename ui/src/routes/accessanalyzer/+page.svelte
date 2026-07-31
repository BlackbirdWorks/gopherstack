<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAccessAnalyzerClient } from '$lib/aws-client';
	import {
		ListAnalyzersCommand,
		CreateAnalyzerCommand,
		DeleteAnalyzerCommand,
		GetAnalyzerCommand,
		ListArchiveRulesCommand,
		CreateArchiveRuleCommand,
		DeleteArchiveRuleCommand,
		ListFindingsV2Command,
		GetFindingV2Command,
		UpdateFindingsCommand,
		ListAnalyzedResourcesCommand,
		GetAnalyzedResourceCommand,
		ListAccessPreviewsCommand,
		CreateAccessPreviewCommand,
		GetAccessPreviewCommand,
		ListAccessPreviewFindingsCommand,
		ListPolicyGenerationsCommand,
		StartPolicyGenerationCommand,
		GetGeneratedPolicyCommand,
		CancelPolicyGenerationCommand,
		type AnalyzerSummary,
		type ArchiveRuleSummary,
		type FindingSummaryV2,
		type AnalyzedResourceSummary,
		type AccessPreviewSummary,
		type AccessPreviewFinding,
		type PolicyGeneration,
		type JobDetails,
		type AnalyzedResource,
		type AccessPreview,
		type Criterion,
		type Configuration
	} from '@aws-sdk/client-accessanalyzer';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import type { Column } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { ShieldCheck, Plus, Trash2, Eye, Archive, ArchiveRestore, Ban } from 'lucide-svelte';

	const client = regionalClient(getAccessAnalyzerClient);

	type TabId =
		| 'analyzers'
		| 'archiveRules'
		| 'findings'
		| 'analyzedResources'
		| 'accessPreviews'
		| 'policyGenerations';

	const tabs: TabDef[] = [
		{ id: 'analyzers', label: 'Analyzers' },
		{ id: 'archiveRules', label: 'Archive Rules' },
		{ id: 'findings', label: 'Findings' },
		{ id: 'analyzedResources', label: 'Analyzed Resources' },
		{ id: 'accessPreviews', label: 'Access Previews' },
		{ id: 'policyGenerations', label: 'Policy Generations' }
	];

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
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

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	let activeTab = $state<TabId>('analyzers');
	let searchQuery = $state('');

	// Archive Rules / Findings / Analyzed Resources / Access Previews are all
	// scoped to one analyzer, the same shared-selector pattern detective uses
	// for its behavior-graph-scoped tabs.
	let selectedAnalyzerArn = $state('');
	const selectedAnalyzer = $derived(analyzersFind(selectedAnalyzerArn));
	function analyzersFind(arn: string): AnalyzerSummary | undefined {
		return analyzers.find((a) => a.arn === arn);
	}
	const selectedAnalyzerName = $derived(selectedAnalyzer?.name ?? '');

	let analyzers = $state<AnalyzerSummary[]>([]);
	let analyzersNextToken = $state<string | undefined>();
	let loadingMoreAnalyzers = $state(false);

	let archiveRules = $state<ArchiveRuleSummary[]>([]);
	let archiveRulesNextToken = $state<string | undefined>();
	let loadingMoreArchiveRules = $state(false);

	let findings = $state<FindingSummaryV2[]>([]);
	let findingsNextToken = $state<string | undefined>();
	let loadingMoreFindings = $state(false);

	let analyzedResources = $state<AnalyzedResourceSummary[]>([]);
	let analyzedResourcesNextToken = $state<string | undefined>();
	let loadingMoreAnalyzedResources = $state(false);

	let accessPreviews = $state<AccessPreviewSummary[]>([]);
	let accessPreviewsNextToken = $state<string | undefined>();
	let loadingMoreAccessPreviews = $state(false);

	let policyGenerations = $state<PolicyGeneration[]>([]);
	let policyGenerationsNextToken = $state<string | undefined>();
	let loadingMorePolicyGenerations = $state(false);

	async function fetchAnalyzers(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAnalyzersCommand({ nextToken: reset ? undefined : analyzersNextToken })
		);
		analyzers = reset ? (resp.analyzers ?? []) : [...analyzers, ...(resp.analyzers ?? [])];
		analyzersNextToken = resp.nextToken;
		if (!selectedAnalyzerArn && analyzers.length > 0) {
			selectedAnalyzerArn = analyzers[0].arn ?? '';
		}
	}

	async function fetchArchiveRules(reset: boolean): Promise<void> {
		if (!selectedAnalyzerName) {
			archiveRules = [];
			archiveRulesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListArchiveRulesCommand({
				analyzerName: selectedAnalyzerName,
				nextToken: reset ? undefined : archiveRulesNextToken
			})
		);
		archiveRules = reset ? (resp.archiveRules ?? []) : [...archiveRules, ...(resp.archiveRules ?? [])];
		archiveRulesNextToken = resp.nextToken;
	}

	async function fetchFindings(reset: boolean): Promise<void> {
		if (!selectedAnalyzerArn) {
			findings = [];
			findingsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListFindingsV2Command({
				analyzerArn: selectedAnalyzerArn,
				nextToken: reset ? undefined : findingsNextToken
			})
		);
		findings = reset ? (resp.findings ?? []) : [...findings, ...(resp.findings ?? [])];
		findingsNextToken = resp.nextToken;
	}

	async function fetchAnalyzedResources(reset: boolean): Promise<void> {
		if (!selectedAnalyzerArn) {
			analyzedResources = [];
			analyzedResourcesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListAnalyzedResourcesCommand({
				analyzerArn: selectedAnalyzerArn,
				nextToken: reset ? undefined : analyzedResourcesNextToken
			})
		);
		analyzedResources = reset
			? (resp.analyzedResources ?? [])
			: [...analyzedResources, ...(resp.analyzedResources ?? [])];
		analyzedResourcesNextToken = resp.nextToken;
	}

	async function fetchAccessPreviews(reset: boolean): Promise<void> {
		if (!selectedAnalyzerArn) {
			accessPreviews = [];
			accessPreviewsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListAccessPreviewsCommand({
				analyzerArn: selectedAnalyzerArn,
				nextToken: reset ? undefined : accessPreviewsNextToken
			})
		);
		accessPreviews = reset
			? (resp.accessPreviews ?? [])
			: [...accessPreviews, ...(resp.accessPreviews ?? [])];
		accessPreviewsNextToken = resp.nextToken;
	}

	async function fetchPolicyGenerations(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListPolicyGenerationsCommand({
				nextToken: reset ? undefined : policyGenerationsNextToken
			})
		);
		policyGenerations = reset
			? (resp.policyGenerations ?? [])
			: [...policyGenerations, ...(resp.policyGenerations ?? [])];
		policyGenerationsNextToken = resp.nextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		analyzers: () => fetchAnalyzers(true).catch(rethrowDescribed),
		archiveRules: () => fetchArchiveRules(true).catch(rethrowDescribed),
		findings: () => fetchFindings(true).catch(rethrowDescribed),
		analyzedResources: () => fetchAnalyzedResources(true).catch(rethrowDescribed),
		accessPreviews: () => fetchAccessPreviews(true).catch(rethrowDescribed),
		policyGenerations: () => fetchPolicyGenerations(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	const analyzerScopedTabs: TabId[] = ['archiveRules', 'findings', 'analyzedResources', 'accessPreviews'];

	function onAnalyzerSelect(arn: string): void {
		selectedAnalyzerArn = arn;
		if (analyzerScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	// Analyzers is the parent resource for the four analyzer-scoped tabs: on a
	// region change the previously selected analyzer ARN belongs to the old
	// region and must not be reused, so reload analyzers first (which
	// re-selects an analyzer for the new region) before reloading whichever
	// tab is active.
	onRegionChange(() => {
		selectedAnalyzerArn = '';
		analyzers = [];
		analyzersNextToken = undefined;
		void tabLoader.refresh('analyzers').then(() => {
			if (activeTab !== 'analyzers') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const filteredAnalyzers = $derived(
		analyzers.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.name ?? '').toLowerCase().includes(q) ||
				(a.arn ?? '').toLowerCase().includes(q) ||
				(a.type ?? '').toLowerCase().includes(q) ||
				(a.status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredArchiveRules = $derived(
		archiveRules.filter((r) => (r.ruleName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredFindings = $derived(
		findings.filter((f) => {
			const q = searchQuery.toLowerCase();
			return (
				(f.id ?? '').toLowerCase().includes(q) ||
				(f.resource ?? '').toLowerCase().includes(q) ||
				(f.resourceType ?? '').toLowerCase().includes(q) ||
				(f.status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredAnalyzedResources = $derived(
		analyzedResources.filter((r) => {
			const q = searchQuery.toLowerCase();
			return (
				(r.resourceArn ?? '').toLowerCase().includes(q) ||
				(r.resourceType ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredAccessPreviews = $derived(
		accessPreviews.filter((ap) => {
			const q = searchQuery.toLowerCase();
			return (ap.id ?? '').toLowerCase().includes(q) || (ap.status ?? '').toLowerCase().includes(q);
		})
	);
	const filteredPolicyGenerations = $derived(
		policyGenerations.filter((pg) => {
			const q = searchQuery.toLowerCase();
			return (
				(pg.jobId ?? '').toLowerCase().includes(q) ||
				(pg.principalArn ?? '').toLowerCase().includes(q) ||
				(pg.status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreAnalyzers(): Promise<void> {
		loadingMoreAnalyzers = true;
		try {
			await fetchAnalyzers(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAnalyzers = false;
		}
	}

	async function loadMoreArchiveRules(): Promise<void> {
		loadingMoreArchiveRules = true;
		try {
			await fetchArchiveRules(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreArchiveRules = false;
		}
	}

	async function loadMoreFindings(): Promise<void> {
		loadingMoreFindings = true;
		try {
			await fetchFindings(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreFindings = false;
		}
	}

	async function loadMoreAnalyzedResources(): Promise<void> {
		loadingMoreAnalyzedResources = true;
		try {
			await fetchAnalyzedResources(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAnalyzedResources = false;
		}
	}

	async function loadMoreAccessPreviews(): Promise<void> {
		loadingMoreAccessPreviews = true;
		try {
			await fetchAccessPreviews(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAccessPreviews = false;
		}
	}

	async function loadMorePolicyGenerations(): Promise<void> {
		loadingMorePolicyGenerations = true;
		try {
			await fetchPolicyGenerations(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMorePolicyGenerations = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// --- Analyzers: create / delete / detail ---

	let createAnalyzerModal = $state<Modal | null>(null);
	let creatingAnalyzer = $state(false);
	let createAnalyzerError = $state<string | null>(null);
	let newAnalyzerName = $state('');
	let newAnalyzerType = $state<
		'ACCOUNT' | 'ORGANIZATION' | 'ACCOUNT_UNUSED_ACCESS' | 'ORGANIZATION_UNUSED_ACCESS' | 'ACCOUNT_INTERNAL_ACCESS' | 'ORGANIZATION_INTERNAL_ACCESS'
	>('ACCOUNT');

	function openCreateAnalyzerModal(): void {
		createAnalyzerError = null;
		newAnalyzerName = '';
		newAnalyzerType = 'ACCOUNT';
		createAnalyzerModal?.open();
	}

	async function submitCreateAnalyzer(): Promise<void> {
		if (!newAnalyzerName) {
			createAnalyzerError = 'Analyzer name is required.';
			return;
		}
		creatingAnalyzer = true;
		createAnalyzerError = null;
		try {
			await client().send(
				new CreateAnalyzerCommand({ analyzerName: newAnalyzerName, type: newAnalyzerType })
			);
			toast.success('Analyzer created');
			createAnalyzerModal?.close();
			await tabLoader.refresh('analyzers');
		} catch (e) {
			const msg = describeError(e);
			createAnalyzerError = msg;
			toast.error(msg);
		} finally {
			creatingAnalyzer = false;
		}
	}

	async function handleDeleteAnalyzer(a: AnalyzerSummary): Promise<void> {
		if (!a.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete analyzer',
			message: `Delete analyzer ${a.name}? This also deletes its archive rules, findings, analyzed resources, and access previews.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAnalyzerCommand({ analyzerName: a.name }));
			toast.success('Analyzer deleted');
			if (selectedAnalyzerArn === a.arn) {
				selectedAnalyzerArn = '';
			}
			await tabLoader.refresh('analyzers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let analyzerDetailModal = $state<Modal | null>(null);
	let viewedAnalyzer = $state<AnalyzerSummary | null>(null);
	let analyzerDetailLoading = $state(false);
	let analyzerDetailError = $state<string | null>(null);

	async function openAnalyzerDetail(a: AnalyzerSummary): Promise<void> {
		viewedAnalyzer = a;
		analyzerDetailError = null;
		analyzerDetailModal?.open();
		if (!a.name) return;
		analyzerDetailLoading = true;
		try {
			const resp = await client().send(new GetAnalyzerCommand({ analyzerName: a.name }));
			viewedAnalyzer = resp.analyzer ?? a;
		} catch (e) {
			analyzerDetailError = describeError(e);
		} finally {
			analyzerDetailLoading = false;
		}
	}

	// --- Archive Rules: create / delete / detail ---

	let createArchiveRuleModal = $state<Modal | null>(null);
	let creatingArchiveRule = $state(false);
	let createArchiveRuleError = $state<string | null>(null);
	let newRuleName = $state('');
	let newRuleFilter = $state('{}');

	function openCreateArchiveRuleModal(): void {
		createArchiveRuleError = selectedAnalyzerName ? null : 'Select an analyzer first.';
		newRuleName = '';
		newRuleFilter = '{}';
		createArchiveRuleModal?.open();
	}

	async function submitCreateArchiveRule(): Promise<void> {
		if (!selectedAnalyzerName) {
			createArchiveRuleError = 'Select an analyzer first.';
			return;
		}
		if (!newRuleName) {
			createArchiveRuleError = 'Rule name is required.';
			return;
		}
		let filter: Record<string, Criterion>;
		try {
			filter = JSON.parse(newRuleFilter);
		} catch {
			createArchiveRuleError = 'Filter must be valid JSON.';
			return;
		}
		creatingArchiveRule = true;
		createArchiveRuleError = null;
		try {
			await client().send(
				new CreateArchiveRuleCommand({
					analyzerName: selectedAnalyzerName,
					ruleName: newRuleName,
					filter
				})
			);
			toast.success('Archive rule created');
			createArchiveRuleModal?.close();
			await tabLoader.refresh('archiveRules');
		} catch (e) {
			const msg = describeError(e);
			createArchiveRuleError = msg;
			toast.error(msg);
		} finally {
			creatingArchiveRule = false;
		}
	}

	async function handleDeleteArchiveRule(r: ArchiveRuleSummary): Promise<void> {
		if (!r.ruleName || !selectedAnalyzerName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete archive rule',
			message: `Delete archive rule ${r.ruleName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteArchiveRuleCommand({ analyzerName: selectedAnalyzerName, ruleName: r.ruleName })
			);
			toast.success('Archive rule deleted');
			await tabLoader.refresh('archiveRules');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let archiveRuleDetailModal = $state<Modal | null>(null);
	let viewedArchiveRule = $state<ArchiveRuleSummary | null>(null);

	function openArchiveRuleDetail(r: ArchiveRuleSummary): void {
		viewedArchiveRule = r;
		archiveRuleDetailModal?.open();
	}

	// --- Findings: archive/restore (the only mutation the real API offers;
	// findings themselves are generated by the analyzer, not user-created or
	// user-deletable) / detail ---

	async function handleToggleFindingStatus(f: FindingSummaryV2): Promise<void> {
		if (!f.id || !selectedAnalyzerArn) return;
		const nextStatus = f.status === 'ARCHIVED' ? 'ACTIVE' : 'ARCHIVED';
		try {
			await client().send(
				new UpdateFindingsCommand({
					analyzerArn: selectedAnalyzerArn,
					ids: [f.id],
					status: nextStatus
				})
			);
			toast.success(nextStatus === 'ARCHIVED' ? 'Finding archived' : 'Finding restored');
			await tabLoader.refresh('findings');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let findingDetailModal = $state<Modal | null>(null);
	let viewedFinding = $state<FindingSummaryV2 | null>(null);
	let findingDetailLoading = $state(false);
	let findingDetailError = $state<string | null>(null);

	async function openFindingDetail(f: FindingSummaryV2): Promise<void> {
		viewedFinding = f;
		findingDetailError = null;
		findingDetailModal?.open();
		if (!f.id || !selectedAnalyzerArn) return;
		findingDetailLoading = true;
		try {
			const resp = await client().send(
				new GetFindingV2Command({ analyzerArn: selectedAnalyzerArn, id: f.id })
			);
			viewedFinding = { ...f, ...resp };
		} catch (e) {
			findingDetailError = describeError(e);
		} finally {
			findingDetailLoading = false;
		}
	}

	// --- Analyzed Resources: detail (read-only; discovered by the analyzer,
	// not user-created or user-deletable) ---

	let analyzedResourceDetailModal = $state<Modal | null>(null);
	let viewedAnalyzedResource = $state<AnalyzedResource | AnalyzedResourceSummary | null>(null);
	let analyzedResourceDetailLoading = $state(false);
	let analyzedResourceDetailError = $state<string | null>(null);

	async function openAnalyzedResourceDetail(r: AnalyzedResourceSummary): Promise<void> {
		viewedAnalyzedResource = r;
		analyzedResourceDetailError = null;
		analyzedResourceDetailModal?.open();
		if (!r.resourceArn || !selectedAnalyzerArn) return;
		analyzedResourceDetailLoading = true;
		try {
			const resp = await client().send(
				new GetAnalyzedResourceCommand({
					analyzerArn: selectedAnalyzerArn,
					resourceArn: r.resourceArn
				})
			);
			viewedAnalyzedResource = resp.resource ?? r;
		} catch (e) {
			analyzedResourceDetailError = describeError(e);
		} finally {
			analyzedResourceDetailLoading = false;
		}
	}

	// --- Access Previews: create / detail (no delete -- the real API has no
	// DeleteAccessPreview operation) ---

	let createAccessPreviewModal = $state<Modal | null>(null);
	let creatingAccessPreview = $state(false);
	let createAccessPreviewError = $state<string | null>(null);
	let newPreviewResourceArn = $state('');
	let newPreviewConfiguration = $state('{\n  "s3Bucket": {\n    "bucketPolicy": "{}"\n  }\n}');

	function openCreateAccessPreviewModal(): void {
		createAccessPreviewError = selectedAnalyzerArn ? null : 'Select an analyzer first.';
		newPreviewResourceArn = '';
		createAccessPreviewModal?.open();
	}

	async function submitCreateAccessPreview(): Promise<void> {
		if (!selectedAnalyzerArn) {
			createAccessPreviewError = 'Select an analyzer first.';
			return;
		}
		if (!newPreviewResourceArn) {
			createAccessPreviewError = 'Resource ARN is required.';
			return;
		}
		let configuration: Configuration;
		try {
			configuration = JSON.parse(newPreviewConfiguration);
		} catch {
			createAccessPreviewError = 'Configuration must be valid JSON.';
			return;
		}
		creatingAccessPreview = true;
		createAccessPreviewError = null;
		try {
			await client().send(
				new CreateAccessPreviewCommand({
					analyzerArn: selectedAnalyzerArn,
					configurations: { [newPreviewResourceArn]: configuration }
				})
			);
			toast.success('Access preview created');
			createAccessPreviewModal?.close();
			await tabLoader.refresh('accessPreviews');
		} catch (e) {
			const msg = describeError(e);
			createAccessPreviewError = msg;
			toast.error(msg);
		} finally {
			creatingAccessPreview = false;
		}
	}

	let accessPreviewDetailModal = $state<Modal | null>(null);
	let viewedAccessPreview = $state<AccessPreview | AccessPreviewSummary | null>(null);
	let viewedAccessPreviewFindings = $state<AccessPreviewFinding[]>([]);
	let accessPreviewDetailLoading = $state(false);
	let accessPreviewDetailError = $state<string | null>(null);

	async function openAccessPreviewDetail(ap: AccessPreviewSummary): Promise<void> {
		viewedAccessPreview = ap;
		viewedAccessPreviewFindings = [];
		accessPreviewDetailError = null;
		accessPreviewDetailModal?.open();
		if (!ap.id || !selectedAnalyzerArn) return;
		accessPreviewDetailLoading = true;
		try {
			const [detailResp, findingsResp] = await Promise.all([
				client().send(
					new GetAccessPreviewCommand({ accessPreviewId: ap.id, analyzerArn: selectedAnalyzerArn })
				),
				client().send(
					new ListAccessPreviewFindingsCommand({
						accessPreviewId: ap.id,
						analyzerArn: selectedAnalyzerArn
					})
				)
			]);
			viewedAccessPreview = detailResp.accessPreview ?? ap;
			viewedAccessPreviewFindings = findingsResp.findings ?? [];
		} catch (e) {
			accessPreviewDetailError = describeError(e);
		} finally {
			accessPreviewDetailLoading = false;
		}
	}

	// --- Policy Generations: start (create) / cancel (no delete -- the
	// closest the real API offers is cancelling an in-progress job) / detail ---

	let startPolicyGenerationModal = $state<Modal | null>(null);
	let startingPolicyGeneration = $state(false);
	let startPolicyGenerationError = $state<string | null>(null);
	let newPrincipalArn = $state('');

	function openStartPolicyGenerationModal(): void {
		startPolicyGenerationError = null;
		newPrincipalArn = '';
		startPolicyGenerationModal?.open();
	}

	async function submitStartPolicyGeneration(): Promise<void> {
		if (!newPrincipalArn) {
			startPolicyGenerationError = 'Principal ARN is required.';
			return;
		}
		startingPolicyGeneration = true;
		startPolicyGenerationError = null;
		try {
			await client().send(
				new StartPolicyGenerationCommand({
					policyGenerationDetails: { principalArn: newPrincipalArn }
				})
			);
			toast.success('Policy generation started');
			startPolicyGenerationModal?.close();
			await tabLoader.refresh('policyGenerations');
		} catch (e) {
			const msg = describeError(e);
			startPolicyGenerationError = msg;
			toast.error(msg);
		} finally {
			startingPolicyGeneration = false;
		}
	}

	async function handleCancelPolicyGeneration(pg: PolicyGeneration): Promise<void> {
		if (!pg.jobId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel policy generation',
			message: `Cancel policy generation job ${pg.jobId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new CancelPolicyGenerationCommand({ jobId: pg.jobId }));
			toast.success('Policy generation canceled');
			await tabLoader.refresh('policyGenerations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let policyGenerationDetailModal = $state<Modal | null>(null);
	let viewedPolicyGeneration = $state<PolicyGeneration | null>(null);
	let viewedJobDetails = $state<JobDetails | null>(null);
	let policyGenerationDetailLoading = $state(false);
	let policyGenerationDetailError = $state<string | null>(null);

	async function openPolicyGenerationDetail(pg: PolicyGeneration): Promise<void> {
		viewedPolicyGeneration = pg;
		viewedJobDetails = null;
		policyGenerationDetailError = null;
		policyGenerationDetailModal?.open();
		if (!pg.jobId) return;
		policyGenerationDetailLoading = true;
		try {
			const resp = await client().send(new GetGeneratedPolicyCommand({ jobId: pg.jobId }));
			viewedJobDetails = resp.jobDetails ?? null;
		} catch (e) {
			policyGenerationDetailError = describeError(e);
		} finally {
			policyGenerationDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={ShieldCheck}
		title="IAM Access Analyzer"
		description="Identify resources shared with external entities"
		onRefresh={handleRefresh}
		color="emerald"
	>
		{#snippet actions()}
			{#if activeTab === 'analyzers'}
				<button
					onclick={openCreateAnalyzerModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create analyzer
				</button>
			{:else if activeTab === 'archiveRules'}
				<button
					onclick={openCreateArchiveRuleModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create archive rule
				</button>
			{:else if activeTab === 'accessPreviews'}
				<button
					onclick={openCreateAccessPreviewModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create access preview
				</button>
			{:else if activeTab === 'policyGenerations'}
				<button
					onclick={openStartPolicyGenerationModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start policy generation
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="emerald" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if analyzerScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="analyzer-select" class="text-sm text-gray-500 dark:text-gray-400">Analyzer</label>
					<select
						id="analyzer-select"
						value={selectedAnalyzerArn}
						onchange={(e) => onAnalyzerSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if analyzers.length === 0}
							<option value="">No analyzers</option>
						{/if}
						{#each analyzers as a (a.arn)}
							<option value={a.arn}>{a.name} ({a.type})</option>
						{/each}
					</select>
				</div>
			{/if}

			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'analyzers'}
				{#snippet analyzerStatusCell(a: AnalyzerSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.status === 'ACTIVE')}"
						>{a.status ?? '—'}</span
					>
				{/snippet}
				{#snippet analyzerCreatedCell(a: AnalyzerSummary)}
					{formatDate(a.createdAt)}
				{/snippet}
				{#snippet analyzerActionsCell(a: AnalyzerSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openAnalyzerDetail(a)}
							title="View"
							aria-label="View analyzer {a.name}"
							class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteAnalyzer(a)}
							title="Delete"
							aria-label="Delete analyzer {a.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const analyzerColumns = [
					{ key: 'name', label: 'Name' },
					{ key: 'type', label: 'Type' },
					{ key: 'status', label: 'Status', render: analyzerStatusCell },
					{ key: 'createdAt', label: 'Created', render: analyzerCreatedCell },
					{ key: 'actions', label: '', render: analyzerActionsCell }
				] as Column<AnalyzerSummary>[]}
				<DataTable
					rows={filteredAnalyzers}
					rowKey={(a) => a.arn ?? ''}
					columns={analyzerColumns}
					loading={tabLoader.isLoading('analyzers')}
					emptyMessage="No analyzers found"
				/>
				<LoadMore
					hasMore={!!analyzersNextToken}
					loading={loadingMoreAnalyzers}
					onLoadMore={loadMoreAnalyzers}
				/>
			{:else if activeTab === 'archiveRules'}
				{#snippet ruleFilterCell(r: ArchiveRuleSummary)}
					<span class="font-mono text-xs">{JSON.stringify(r.filter ?? {})}</span>
				{/snippet}
				{#snippet ruleUpdatedCell(r: ArchiveRuleSummary)}
					{formatDate(r.updatedAt)}
				{/snippet}
				{#snippet ruleActionsCell(r: ArchiveRuleSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openArchiveRuleDetail(r)}
							title="View"
							aria-label="View archive rule {r.ruleName}"
							class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteArchiveRule(r)}
							title="Delete"
							aria-label="Delete archive rule {r.ruleName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const archiveRuleColumns = [
					{ key: 'ruleName', label: 'Rule Name' },
					{ key: 'filter', label: 'Filter', render: ruleFilterCell },
					{ key: 'updatedAt', label: 'Updated', render: ruleUpdatedCell },
					{ key: 'actions', label: '', render: ruleActionsCell }
				] as Column<ArchiveRuleSummary>[]}
				<DataTable
					rows={filteredArchiveRules}
					rowKey={(r) => r.ruleName ?? ''}
					columns={archiveRuleColumns}
					loading={tabLoader.isLoading('archiveRules')}
					emptyMessage={selectedAnalyzerName
						? 'No archive rules found'
						: 'Select an analyzer to see its archive rules'}
				/>
				<LoadMore
					hasMore={!!archiveRulesNextToken}
					loading={loadingMoreArchiveRules}
					onLoadMore={loadMoreArchiveRules}
				/>
			{:else if activeTab === 'findings'}
				{#snippet findingStatusCell(f: FindingSummaryV2)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(f.status === 'ACTIVE')}"
						>{f.status ?? '—'}</span
					>
				{/snippet}
				{#snippet findingCreatedCell(f: FindingSummaryV2)}
					{formatDate(f.createdAt)}
				{/snippet}
				{#snippet findingActionsCell(f: FindingSummaryV2)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openFindingDetail(f)}
							title="View"
							aria-label="View finding {f.id}"
							class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleToggleFindingStatus(f)}
							title={f.status === 'ARCHIVED' ? 'Restore' : 'Archive'}
							aria-label={f.status === 'ARCHIVED' ? `Restore finding ${f.id}` : `Archive finding ${f.id}`}
							class="text-gray-400 hover:text-emerald-500"
						>
							{#if f.status === 'ARCHIVED'}
								<ArchiveRestore class="w-4 h-4" />
							{:else}
								<Archive class="w-4 h-4" />
							{/if}
						</button>
					</div>
				{/snippet}
				{@const findingColumns = [
					{ key: 'id', label: 'ID' },
					{ key: 'resource', label: 'Resource' },
					{ key: 'resourceType', label: 'Resource Type' },
					{ key: 'status', label: 'Status', render: findingStatusCell },
					{ key: 'createdAt', label: 'Created', render: findingCreatedCell },
					{ key: 'actions', label: '', render: findingActionsCell }
				] as Column<FindingSummaryV2>[]}
				<DataTable
					rows={filteredFindings}
					rowKey={(f) => f.id ?? ''}
					columns={findingColumns}
					loading={tabLoader.isLoading('findings')}
					emptyMessage={selectedAnalyzerArn
						? 'No findings found'
						: 'Select an analyzer to see its findings'}
				/>
				<LoadMore
					hasMore={!!findingsNextToken}
					loading={loadingMoreFindings}
					onLoadMore={loadMoreFindings}
				/>
			{:else if activeTab === 'analyzedResources'}
				{#snippet resourcePublicCell(r: AnalyzedResourceSummary & { isPublic?: boolean })}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(!r.isPublic)}"
						>{r.isPublic ? 'Public' : 'Private'}</span
					>
				{/snippet}
				{#snippet resourceActionsCell(r: AnalyzedResourceSummary)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => openAnalyzedResourceDetail(r)}
							title="View"
							aria-label="View resource {r.resourceArn}"
							class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const analyzedResourceColumns = [
					{ key: 'resourceArn', label: 'Resource ARN' },
					{ key: 'resourceType', label: 'Resource Type' },
					{ key: 'resourceOwnerAccount', label: 'Owner Account' },
					{ key: 'actions', label: '', render: resourceActionsCell }
				] as Column<AnalyzedResourceSummary>[]}
				<DataTable
					rows={filteredAnalyzedResources}
					rowKey={(r) => r.resourceArn ?? ''}
					columns={analyzedResourceColumns}
					loading={tabLoader.isLoading('analyzedResources')}
					emptyMessage={selectedAnalyzerArn
						? 'No analyzed resources found'
						: 'Select an analyzer to see its analyzed resources'}
				/>
				<LoadMore
					hasMore={!!analyzedResourcesNextToken}
					loading={loadingMoreAnalyzedResources}
					onLoadMore={loadMoreAnalyzedResources}
				/>
			{:else if activeTab === 'accessPreviews'}
				{#snippet previewStatusCell(ap: AccessPreviewSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(ap.status === 'COMPLETED')}"
						>{ap.status ?? '—'}</span
					>
				{/snippet}
				{#snippet previewCreatedCell(ap: AccessPreviewSummary)}
					{formatDate(ap.createdAt)}
				{/snippet}
				{#snippet previewActionsCell(ap: AccessPreviewSummary)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => openAccessPreviewDetail(ap)}
							title="View"
							aria-label="View access preview {ap.id}"
							class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const accessPreviewColumns = [
					{ key: 'id', label: 'ID' },
					{ key: 'status', label: 'Status', render: previewStatusCell },
					{ key: 'createdAt', label: 'Created', render: previewCreatedCell },
					{ key: 'actions', label: '', render: previewActionsCell }
				] as Column<AccessPreviewSummary>[]}
				<DataTable
					rows={filteredAccessPreviews}
					rowKey={(ap) => ap.id ?? ''}
					columns={accessPreviewColumns}
					loading={tabLoader.isLoading('accessPreviews')}
					emptyMessage={selectedAnalyzerArn
						? 'No access previews found'
						: 'Select an analyzer to see its access previews'}
				/>
				<LoadMore
					hasMore={!!accessPreviewsNextToken}
					loading={loadingMoreAccessPreviews}
					onLoadMore={loadMoreAccessPreviews}
				/>
			{:else if activeTab === 'policyGenerations'}
				{#snippet jobStatusCell(pg: PolicyGeneration)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(pg.status === 'SUCCEEDED')}"
						>{pg.status ?? '—'}</span
					>
				{/snippet}
				{#snippet jobStartedCell(pg: PolicyGeneration)}
					{formatDate(pg.startedOn)}
				{/snippet}
				{#snippet jobActionsCell(pg: PolicyGeneration)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openPolicyGenerationDetail(pg)}
							title="View"
							aria-label="View policy generation {pg.jobId}"
							class="text-gray-400 hover:text-emerald-500"><Eye class="w-4 h-4" /></button
						>
						{#if pg.status === 'IN_PROGRESS'}
							<button
								onclick={() => handleCancelPolicyGeneration(pg)}
								title="Cancel"
								aria-label="Cancel policy generation {pg.jobId}"
								class="text-gray-400 hover:text-red-500"><Ban class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const policyGenerationColumns = [
					{ key: 'jobId', label: 'Job ID' },
					{ key: 'principalArn', label: 'Principal' },
					{ key: 'status', label: 'Status', render: jobStatusCell },
					{ key: 'startedOn', label: 'Started', render: jobStartedCell },
					{ key: 'actions', label: '', render: jobActionsCell }
				] as Column<PolicyGeneration>[]}
				<DataTable
					rows={filteredPolicyGenerations}
					rowKey={(pg) => pg.jobId ?? ''}
					columns={policyGenerationColumns}
					loading={tabLoader.isLoading('policyGenerations')}
					emptyMessage="No policy generations found"
				/>
				<LoadMore
					hasMore={!!policyGenerationsNextToken}
					loading={loadingMorePolicyGenerations}
					onLoadMore={loadMorePolicyGenerations}
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createAnalyzerModal} title="Create Analyzer">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="analyzer-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="analyzer-name"
					bind:value={newAnalyzerName}
					placeholder="my-analyzer"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="analyzer-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="analyzer-type"
					bind:value={newAnalyzerType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ACCOUNT">Account</option>
					<option value="ORGANIZATION">Organization</option>
					<option value="ACCOUNT_UNUSED_ACCESS">Account (Unused Access)</option>
					<option value="ORGANIZATION_UNUSED_ACCESS">Organization (Unused Access)</option>
					<option value="ACCOUNT_INTERNAL_ACCESS">Account (Internal Access)</option>
					<option value="ORGANIZATION_INTERNAL_ACCESS">Organization (Internal Access)</option>
				</select>
			</div>
			{#if createAnalyzerError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAnalyzerError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAnalyzerModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateAnalyzer}
			disabled={creatingAnalyzer}
			class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50"
			>{creatingAnalyzer ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={analyzerDetailModal} title="Analyzer">
	{#snippet children()}
		{#if analyzerDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAnalyzer}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalyzer.name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedAnalyzer.arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalyzer.type ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalyzer.status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedAnalyzer.createdAt)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last resource analyzed</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalyzer.lastResourceAnalyzed ?? '—'}</dd>
				</div>
				{#if viewedAnalyzer.configuration}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Configuration</dt>
						<dd class="font-mono text-xs break-all text-slate-900 dark:text-white"
							>{JSON.stringify(viewedAnalyzer.configuration)}</dd
						>
					</div>
				{/if}
			</dl>
			{#if analyzerDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{analyzerDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => analyzerDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createArchiveRuleModal} title="Create Archive Rule">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				For analyzer <span class="font-medium">{selectedAnalyzerName || '(none selected)'}</span>. Findings
				matching the filter are archived immediately, and any future matching findings are archived
				automatically.
			</p>
			<div>
				<label for="rule-name" class="text-sm text-slate-600 dark:text-slate-300">Rule name</label>
				<input
					id="rule-name"
					bind:value={newRuleName}
					placeholder="archive-test-bucket"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="rule-filter" class="text-sm text-slate-600 dark:text-slate-300"
					>Filter (JSON, e.g. {'{"resourceType":{"eq":["AWS::S3::Bucket"]}}'})</label
				>
				<textarea
					id="rule-filter"
					bind:value={newRuleFilter}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createArchiveRuleError}
				<p class="text-sm text-red-600 dark:text-red-400">{createArchiveRuleError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createArchiveRuleModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateArchiveRule}
			disabled={creatingArchiveRule}
			class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50"
			>{creatingArchiveRule ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={archiveRuleDetailModal} title="Archive Rule">
	{#snippet children()}
		{#if viewedArchiveRule}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Rule name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedArchiveRule.ruleName ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Filter</dt>
					<dd class="font-mono text-xs break-all text-slate-900 dark:text-white"
						>{JSON.stringify(viewedArchiveRule.filter ?? {}, null, 2)}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedArchiveRule.createdAt)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedArchiveRule.updatedAt)}</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => archiveRuleDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={findingDetailModal} title="Finding">
	{#snippet children()}
		{#if findingDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedFinding}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ID</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedFinding.id ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Resource</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedFinding.resource ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Resource type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.resourceType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFinding.status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFinding.createdAt)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Analyzed</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFinding.analyzedAt)}</dd>
				</div>
			</dl>
			{#if findingDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{findingDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => findingDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={analyzedResourceDetailModal} title="Analyzed Resource">
	{#snippet children()}
		{#if analyzedResourceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAnalyzedResource}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Resource ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white"
						>{viewedAnalyzedResource.resourceArn ?? '—'}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Resource type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalyzedResource.resourceType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Owner account</dt>
					<dd class="text-slate-900 dark:text-white"
						>{viewedAnalyzedResource.resourceOwnerAccount ?? '—'}</dd
					>
				</div>
				{#if 'isPublic' in viewedAnalyzedResource}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Public</dt>
						<dd class="text-slate-900 dark:text-white">{viewedAnalyzedResource.isPublic ? 'Yes' : 'No'}</dd>
					</div>
				{/if}
				{#if 'createdAt' in viewedAnalyzedResource}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Analyzed</dt>
						<dd class="text-slate-900 dark:text-white">{formatDate(viewedAnalyzedResource.analyzedAt)}</dd>
					</div>
				{/if}
			</dl>
			{#if analyzedResourceDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{analyzedResourceDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => analyzedResourceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createAccessPreviewModal} title="Create Access Preview">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				For analyzer <span class="font-medium">{selectedAnalyzer?.name || '(none selected)'}</span>. Previews
				the findings that a proposed resource policy change would generate, without applying it.
			</p>
			<div>
				<label for="preview-resource-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Resource ARN</label
				>
				<input
					id="preview-resource-arn"
					bind:value={newPreviewResourceArn}
					placeholder="arn:aws:s3:::example-bucket"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="preview-configuration" class="text-sm text-slate-600 dark:text-slate-300"
					>Proposed configuration (JSON)</label
				>
				<textarea
					id="preview-configuration"
					bind:value={newPreviewConfiguration}
					rows="6"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createAccessPreviewError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAccessPreviewError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAccessPreviewModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateAccessPreview}
			disabled={creatingAccessPreview}
			class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50"
			>{creatingAccessPreview ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={accessPreviewDetailModal} title="Access Preview">
	{#snippet children()}
		{#if accessPreviewDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAccessPreview}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAccessPreview.id ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAccessPreview.status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedAccessPreview.createdAt)}</dd>
				</div>
			</dl>
			<div class="mt-4">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">Findings</h3>
				{#if accessPreviewDetailError}
					<p class="text-sm text-red-600 dark:text-red-400">{accessPreviewDetailError}</p>
				{:else if viewedAccessPreviewFindings.length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-400">No findings found.</p>
				{:else}
					<ul class="mt-2 space-y-1 text-sm">
						{#each viewedAccessPreviewFindings as finding (finding.id)}
							<li class="text-slate-700 dark:text-slate-300">
								<span class="font-medium">{finding.changeType ?? 'UNKNOWN'}</span>: {finding.resource ??
									'—'} ({finding.resourceType ?? '—'})
							</li>
						{/each}
					</ul>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => accessPreviewDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={startPolicyGenerationModal} title="Start Policy Generation">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pg-principal-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Principal ARN (IAM user or role)</label
				>
				<input
					id="pg-principal-arn"
					bind:value={newPrincipalArn}
					placeholder="arn:aws:iam::123456789012:role/example"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if startPolicyGenerationError}
				<p class="text-sm text-red-600 dark:text-red-400">{startPolicyGenerationError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => startPolicyGenerationModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitStartPolicyGeneration}
			disabled={startingPolicyGeneration}
			class="rounded-lg bg-emerald-600 px-4 py-2 text-sm font-semibold text-white hover:bg-emerald-700 disabled:opacity-50"
			>{startingPolicyGeneration ? 'Starting…' : 'Start'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={policyGenerationDetailModal} title="Policy Generation">
	{#snippet children()}
		{#if policyGenerationDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedPolicyGeneration}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Job ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedPolicyGeneration.jobId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Principal</dt>
					<dd class="break-all text-slate-900 dark:text-white"
						>{viewedPolicyGeneration.principalArn ?? '—'}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white"
						>{viewedJobDetails?.status ?? viewedPolicyGeneration.status ?? '—'}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Started</dt>
					<dd class="text-slate-900 dark:text-white"
						>{formatDate(viewedJobDetails?.startedOn ?? viewedPolicyGeneration.startedOn)}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Completed</dt>
					<dd class="text-slate-900 dark:text-white"
						>{formatDate(viewedJobDetails?.completedOn)}</dd
					>
				</div>
			</dl>
			{#if policyGenerationDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{policyGenerationDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => policyGenerationDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
