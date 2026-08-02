<script lang="ts">
	// Amazon Personalize is a train-then-deploy ML service: DatasetGroup ->
	// Dataset (needs a pre-existing Schema, managed by CreateSchema/etc.) ->
	// Solution -> SolutionVersion (a trained model) -> Campaign (a deployed,
	// queryable endpoint) or Recommender (a domain-specific deployed
	// endpoint). EventTracker is a lightweight sibling resource (real-time
	// event ingestion for a dataset group) kept here since it was already
	// surfaced by the previous read-only page and is cheap to support fully.
	//
	// Out of scope for this page (real, implemented backend families, just
	// not surfaced here): Schema (CreateSchema/DescribeSchema/DeleteSchema/
	// ListSchemas -- schemaArn is taken as a free-text ARN below instead),
	// Filter, MetricAttribution, the five async job families
	// (DatasetImportJob/DatasetExportJob/BatchInferenceJob/BatchSegmentJob/
	// DataDeletionJob), and the read-only built-in catalogs (Recipe/
	// Algorithm/FeatureTransformation) -- recipeArn is likewise a free-text
	// ARN. This mirrors forecast/+page.svelte's documented scope line.
	//
	// GetRecommendations/GetPersonalizedRanking live on the sibling
	// `personalizeruntime` client, not this control-plane client (see
	// services/personalize/PARITY.md) -- surfaced here as a "Get
	// Recommendations" tester tab using that second client.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getPersonalizeClient, getPersonalizeRuntimeClient } from '$lib/aws-client';
	import {
		ListDatasetGroupsCommand,
		ListDatasetsCommand,
		ListSolutionsCommand,
		ListSolutionVersionsCommand,
		ListCampaignsCommand,
		ListRecommendersCommand,
		ListEventTrackersCommand,
		DescribeDatasetGroupCommand,
		DescribeDatasetCommand,
		DescribeSolutionCommand,
		DescribeSolutionVersionCommand,
		DescribeCampaignCommand,
		DescribeRecommenderCommand,
		DescribeEventTrackerCommand,
		CreateDatasetGroupCommand,
		CreateDatasetCommand,
		CreateSolutionCommand,
		CreateSolutionVersionCommand,
		CreateCampaignCommand,
		CreateRecommenderCommand,
		CreateEventTrackerCommand,
		UpdateDatasetCommand,
		UpdateSolutionCommand,
		UpdateCampaignCommand,
		UpdateRecommenderCommand,
		DeleteDatasetGroupCommand,
		DeleteDatasetCommand,
		DeleteSolutionCommand,
		DeleteCampaignCommand,
		DeleteRecommenderCommand,
		DeleteEventTrackerCommand,
		StopSolutionVersionCreationCommand,
		StartRecommenderCommand,
		StopRecommenderCommand,
		type DatasetGroupSummary,
		type DatasetSummary,
		type SolutionSummary,
		type SolutionVersionSummary,
		type CampaignSummary,
		type RecommenderSummary,
		type EventTrackerSummary,
		type DescribeDatasetGroupCommandOutput,
		type DescribeDatasetCommandOutput,
		type DescribeSolutionCommandOutput,
		type DescribeSolutionVersionCommandOutput,
		type DescribeCampaignCommandOutput,
		type DescribeRecommenderCommandOutput,
		type DescribeEventTrackerCommandOutput
	} from '@aws-sdk/client-personalize';
	import { GetRecommendationsCommand } from '@aws-sdk/client-personalize-runtime';
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
	import { Sparkles, Plus, Trash2, Eye, Pencil, Play, Square, RefreshCw } from 'lucide-svelte';

	const client = regionalClient(getPersonalizeClient);
	const runtimeClient = regionalClient(getPersonalizeRuntimeClient);

	type TabId = 'datasetgroups' | 'datasets' | 'solutions' | 'solutionversions' | 'campaigns' | 'recommenders' | 'trackers' | 'recommend';

	const tabs: TabDef[] = [
		{ id: 'datasetgroups', label: 'Dataset Groups' },
		{ id: 'datasets', label: 'Datasets' },
		{ id: 'solutions', label: 'Solutions' },
		{ id: 'solutionversions', label: 'Solution Versions' },
		{ id: 'campaigns', label: 'Campaigns' },
		{ id: 'recommenders', label: 'Recommenders' },
		{ id: 'trackers', label: 'Event Trackers' },
		{ id: 'recommend', label: 'Get Recommendations' }
	];

	const DOMAINS = ['', 'ECOMMERCE', 'VIDEO_ON_DEMAND'];
	const DATASET_TYPES = ['INTERACTIONS', 'ITEMS', 'USERS', 'ACTIONS', 'ACTION_INTERACTIONS'];
	const TRAINING_MODES = ['', 'FULL', 'UPDATE'];

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

	function statusClass(status: string | undefined): string {
		if (status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status?.includes('FAILED')) return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		if (status?.includes('PENDING') || status?.includes('IN_PROGRESS') || status?.includes('STOPPING')) {
			return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('datasetgroups');
	let searchQuery = $state('');

	let datasetGroups = $state<DatasetGroupSummary[]>([]);
	let datasets = $state<DatasetSummary[]>([]);
	let solutions = $state<SolutionSummary[]>([]);
	let solutionVersions = $state<SolutionVersionSummary[]>([]);
	let campaigns = $state<CampaignSummary[]>([]);
	let recommenders = $state<RecommenderSummary[]>([]);
	let trackers = $state<EventTrackerSummary[]>([]);

	async function fetchDatasetGroups(): Promise<void> {
		const resp = await client().send(new ListDatasetGroupsCommand({}));
		datasetGroups = resp.datasetGroups ?? [];
	}
	async function fetchDatasets(): Promise<void> {
		const resp = await client().send(new ListDatasetsCommand({}));
		datasets = resp.datasets ?? [];
	}
	async function fetchSolutions(): Promise<void> {
		const resp = await client().send(new ListSolutionsCommand({}));
		solutions = resp.solutions ?? [];
	}
	async function fetchSolutionVersions(): Promise<void> {
		const resp = await client().send(new ListSolutionVersionsCommand({}));
		solutionVersions = resp.solutionVersions ?? [];
	}
	async function fetchCampaigns(): Promise<void> {
		const resp = await client().send(new ListCampaignsCommand({}));
		campaigns = resp.campaigns ?? [];
	}
	async function fetchRecommenders(): Promise<void> {
		const resp = await client().send(new ListRecommendersCommand({}));
		recommenders = resp.recommenders ?? [];
	}
	async function fetchTrackers(): Promise<void> {
		const resp = await client().send(new ListEventTrackersCommand({}));
		trackers = resp.eventTrackers ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		datasetgroups: () => fetchDatasetGroups().catch(rethrowDescribed),
		datasets: () => fetchDatasets().catch(rethrowDescribed),
		solutions: () => fetchSolutions().catch(rethrowDescribed),
		solutionversions: () => fetchSolutionVersions().catch(rethrowDescribed),
		campaigns: () => fetchCampaigns().catch(rethrowDescribed),
		recommenders: () => fetchRecommenders().catch(rethrowDescribed),
		trackers: () => fetchTrackers().catch(rethrowDescribed),
		recommend: () => Promise.resolve()
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		detailModal?.close();
		detailKind = null;
		tabLoader.refresh(untrack(() => activeTab));
	});

	function matches(q: string, ...fields: (string | undefined)[]): boolean {
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}

	const filteredDatasetGroups = $derived(datasetGroups.filter((g) => matches(searchQuery.toLowerCase(), g.name, g.datasetGroupArn)));
	const filteredDatasets = $derived(datasets.filter((d) => matches(searchQuery.toLowerCase(), d.name, d.datasetArn, d.datasetType)));
	const filteredSolutions = $derived(solutions.filter((s) => matches(searchQuery.toLowerCase(), s.name, s.solutionArn)));
	const filteredSolutionVersions = $derived(solutionVersions.filter((v) => matches(searchQuery.toLowerCase(), v.solutionVersionArn, v.status)));
	const filteredCampaigns = $derived(campaigns.filter((c) => matches(searchQuery.toLowerCase(), c.name, c.campaignArn)));
	const filteredRecommenders = $derived(recommenders.filter((r) => matches(searchQuery.toLowerCase(), r.name, r.recommenderArn)));
	const filteredTrackers = $derived(trackers.filter((t) => matches(searchQuery.toLowerCase(), t.name, t.eventTrackerArn)));

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Dataset Group: create / delete / detail ---

	let createGroupModal = $state<Modal | null>(null);
	let creatingGroup = $state(false);
	let createGroupError = $state<string | null>(null);
	let newGroupName = $state('');
	let newGroupDomain = $state('');

	function openCreateGroupModal(): void {
		createGroupError = null;
		newGroupName = '';
		newGroupDomain = '';
		createGroupModal?.open();
	}

	async function submitCreateGroup(): Promise<void> {
		if (!newGroupName) {
			createGroupError = 'Name is required.';
			return;
		}
		creatingGroup = true;
		createGroupError = null;
		try {
			await client().send(
				new CreateDatasetGroupCommand({ name: newGroupName, domain: newGroupDomain ? (newGroupDomain as never) : undefined })
			);
			toast.success('Dataset group created');
			createGroupModal?.close();
			await tabLoader.refresh('datasetgroups');
		} catch (e) {
			const msg = describeError(e);
			createGroupError = msg;
			toast.error(msg);
		} finally {
			creatingGroup = false;
		}
	}

	async function deleteDatasetGroup(g: DatasetGroupSummary): Promise<void> {
		if (!g.datasetGroupArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete dataset group',
			message: `Delete dataset group "${g.name ?? g.datasetGroupArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDatasetGroupCommand({ datasetGroupArn: g.datasetGroupArn }));
			toast.success('Dataset group deleted');
			await tabLoader.refresh('datasetgroups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Dataset: create / update / delete / detail ---

	let createDatasetModal = $state<Modal | null>(null);
	let creatingDataset = $state(false);
	let createDatasetError = $state<string | null>(null);
	let newDatasetName = $state('');
	let newDatasetGroupArn = $state('');
	let newDatasetType = $state('INTERACTIONS');
	let newDatasetSchemaArn = $state('');

	function openCreateDatasetModal(): void {
		createDatasetError = null;
		newDatasetName = '';
		newDatasetGroupArn = '';
		newDatasetType = 'INTERACTIONS';
		newDatasetSchemaArn = '';
		createDatasetModal?.open();
	}

	async function submitCreateDataset(): Promise<void> {
		if (!newDatasetName || !newDatasetGroupArn || !newDatasetSchemaArn) {
			createDatasetError = 'Name, dataset group, and schema ARN are required.';
			return;
		}
		creatingDataset = true;
		createDatasetError = null;
		try {
			await client().send(
				new CreateDatasetCommand({
					name: newDatasetName,
					datasetGroupArn: newDatasetGroupArn,
					datasetType: newDatasetType,
					schemaArn: newDatasetSchemaArn
				})
			);
			toast.success('Dataset created');
			createDatasetModal?.close();
			await tabLoader.refresh('datasets');
		} catch (e) {
			const msg = describeError(e);
			createDatasetError = msg;
			toast.error(msg);
		} finally {
			creatingDataset = false;
		}
	}

	let editDatasetModal = $state<Modal | null>(null);
	let editingDataset = $state(false);
	let editDatasetError = $state<string | null>(null);
	let editDatasetArn = $state('');
	let editDatasetSchemaArn = $state('');

	function openEditDataset(): void {
		editDatasetError = null;
		editDatasetArn = viewedDatasetArn;
		editDatasetSchemaArn = viewedDataset?.dataset?.schemaArn ?? '';
		editDatasetModal?.open();
	}

	async function submitEditDataset(): Promise<void> {
		if (!editDatasetSchemaArn) {
			editDatasetError = 'Schema ARN is required.';
			return;
		}
		editingDataset = true;
		editDatasetError = null;
		try {
			await client().send(new UpdateDatasetCommand({ datasetArn: editDatasetArn, schemaArn: editDatasetSchemaArn }));
			toast.success('Dataset updated');
			editDatasetModal?.close();
			await tabLoader.refresh('datasets');
			await refreshDatasetDetail();
		} catch (e) {
			const msg = describeError(e);
			editDatasetError = msg;
			toast.error(msg);
		} finally {
			editingDataset = false;
		}
	}

	async function deleteDataset(d: DatasetSummary): Promise<void> {
		if (!d.datasetArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete dataset',
			message: `Delete dataset "${d.name ?? d.datasetArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDatasetCommand({ datasetArn: d.datasetArn }));
			toast.success('Dataset deleted');
			await tabLoader.refresh('datasets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Solution: create / update / delete / detail ---

	let createSolutionModal = $state<Modal | null>(null);
	let creatingSolution = $state(false);
	let createSolutionError = $state<string | null>(null);
	let newSolutionName = $state('');
	let newSolutionDatasetGroupArn = $state('');
	let newSolutionPerformAutoML = $state(false);
	let newSolutionRecipeArn = $state('');

	function openCreateSolutionModal(): void {
		createSolutionError = null;
		newSolutionName = '';
		newSolutionDatasetGroupArn = '';
		newSolutionPerformAutoML = false;
		newSolutionRecipeArn = '';
		createSolutionModal?.open();
	}

	async function submitCreateSolution(): Promise<void> {
		if (!newSolutionName || !newSolutionDatasetGroupArn) {
			createSolutionError = 'Name and dataset group are required.';
			return;
		}
		if (!newSolutionPerformAutoML && !newSolutionRecipeArn) {
			createSolutionError = 'A recipe ARN is required unless AutoML is enabled.';
			return;
		}
		creatingSolution = true;
		createSolutionError = null;
		try {
			await client().send(
				new CreateSolutionCommand({
					name: newSolutionName,
					datasetGroupArn: newSolutionDatasetGroupArn,
					performAutoML: newSolutionPerformAutoML,
					recipeArn: newSolutionPerformAutoML ? undefined : newSolutionRecipeArn
				})
			);
			toast.success('Solution created');
			createSolutionModal?.close();
			await tabLoader.refresh('solutions');
		} catch (e) {
			const msg = describeError(e);
			createSolutionError = msg;
			toast.error(msg);
		} finally {
			creatingSolution = false;
		}
	}

	let editSolutionModal = $state<Modal | null>(null);
	let editingSolution = $state(false);
	let editSolutionError = $state<string | null>(null);
	let editSolutionArn = $state('');
	let editSolutionAutoTraining = $state(true);
	let editSolutionIncrementalUpdate = $state(false);

	function openEditSolution(): void {
		editSolutionError = null;
		editSolutionArn = viewedSolutionArn;
		editSolutionAutoTraining = viewedSolution?.solution?.performAutoTraining ?? true;
		editSolutionIncrementalUpdate = viewedSolution?.solution?.performIncrementalUpdate ?? false;
		editSolutionModal?.open();
	}

	async function submitEditSolution(): Promise<void> {
		editingSolution = true;
		editSolutionError = null;
		try {
			await client().send(
				new UpdateSolutionCommand({
					solutionArn: editSolutionArn,
					performAutoTraining: editSolutionAutoTraining,
					performIncrementalUpdate: editSolutionIncrementalUpdate
				})
			);
			toast.success('Solution updated');
			editSolutionModal?.close();
			await tabLoader.refresh('solutions');
			await refreshSolutionDetail();
		} catch (e) {
			const msg = describeError(e);
			editSolutionError = msg;
			toast.error(msg);
		} finally {
			editingSolution = false;
		}
	}

	async function deleteSolution(s: SolutionSummary): Promise<void> {
		if (!s.solutionArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete solution',
			message: `Delete solution "${s.name ?? s.solutionArn}"? This also deletes all of its solution versions.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSolutionCommand({ solutionArn: s.solutionArn }));
			toast.success('Solution deleted');
			await tabLoader.refresh('solutions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Solution Version: create (train) / stop / detail (no delete op in the real API) ---

	let createVersionModal = $state<Modal | null>(null);
	let creatingVersion = $state(false);
	let createVersionError = $state<string | null>(null);
	let newVersionSolutionArn = $state('');
	let newVersionTrainingMode = $state('');

	function openCreateVersionModal(): void {
		createVersionError = null;
		newVersionSolutionArn = '';
		newVersionTrainingMode = '';
		createVersionModal?.open();
	}

	async function submitCreateVersion(): Promise<void> {
		if (!newVersionSolutionArn) {
			createVersionError = 'A solution is required.';
			return;
		}
		creatingVersion = true;
		createVersionError = null;
		try {
			await client().send(
				new CreateSolutionVersionCommand({
					solutionArn: newVersionSolutionArn,
					trainingMode: newVersionTrainingMode ? (newVersionTrainingMode as never) : undefined
				})
			);
			toast.success('Solution version training started');
			createVersionModal?.close();
			await tabLoader.refresh('solutionversions');
		} catch (e) {
			const msg = describeError(e);
			createVersionError = msg;
			toast.error(msg);
		} finally {
			creatingVersion = false;
		}
	}

	async function stopVersion(v: SolutionVersionSummary): Promise<void> {
		if (!v.solutionVersionArn) return;
		const confirmed = await confirmDestructive({
			title: 'Stop solution version training',
			message: `Stop training solution version "${v.solutionVersionArn}"?`,
			confirmLabel: 'Stop'
		});
		if (!confirmed) return;
		try {
			await client().send(new StopSolutionVersionCreationCommand({ solutionVersionArn: v.solutionVersionArn }));
			toast.success('Stop requested');
			await tabLoader.refresh('solutionversions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Campaign: create / update / delete / detail ---

	let createCampaignModal = $state<Modal | null>(null);
	let creatingCampaign = $state(false);
	let createCampaignError = $state<string | null>(null);
	let newCampaignName = $state('');
	let newCampaignSolutionVersionArn = $state('');
	let newCampaignMinTPS = $state<number | undefined>();

	function openCreateCampaignModal(): void {
		createCampaignError = null;
		newCampaignName = '';
		newCampaignSolutionVersionArn = '';
		newCampaignMinTPS = undefined;
		createCampaignModal?.open();
	}

	async function submitCreateCampaign(): Promise<void> {
		if (!newCampaignName || !newCampaignSolutionVersionArn) {
			createCampaignError = 'Name and solution version are required.';
			return;
		}
		creatingCampaign = true;
		createCampaignError = null;
		try {
			await client().send(
				new CreateCampaignCommand({
					name: newCampaignName,
					solutionVersionArn: newCampaignSolutionVersionArn,
					minProvisionedTPS: newCampaignMinTPS
				})
			);
			toast.success('Campaign created');
			createCampaignModal?.close();
			await tabLoader.refresh('campaigns');
		} catch (e) {
			const msg = describeError(e);
			createCampaignError = msg;
			toast.error(msg);
		} finally {
			creatingCampaign = false;
		}
	}

	let editCampaignModal = $state<Modal | null>(null);
	let editingCampaign = $state(false);
	let editCampaignError = $state<string | null>(null);
	let editCampaignArn = $state('');
	let editCampaignSolutionVersionArn = $state('');
	let editCampaignMinTPS = $state<number | undefined>();

	function openEditCampaign(): void {
		editCampaignError = null;
		editCampaignArn = viewedCampaignArn;
		editCampaignSolutionVersionArn = '';
		editCampaignMinTPS = viewedCampaign?.campaign?.minProvisionedTPS;
		editCampaignModal?.open();
	}

	async function submitEditCampaign(): Promise<void> {
		editingCampaign = true;
		editCampaignError = null;
		try {
			await client().send(
				new UpdateCampaignCommand({
					campaignArn: editCampaignArn,
					solutionVersionArn: editCampaignSolutionVersionArn || undefined,
					minProvisionedTPS: editCampaignMinTPS
				})
			);
			toast.success('Campaign updated');
			editCampaignModal?.close();
			await tabLoader.refresh('campaigns');
			await refreshCampaignDetail();
		} catch (e) {
			const msg = describeError(e);
			editCampaignError = msg;
			toast.error(msg);
		} finally {
			editingCampaign = false;
		}
	}

	async function deleteCampaign(c: CampaignSummary): Promise<void> {
		if (!c.campaignArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete campaign',
			message: `Delete campaign "${c.name ?? c.campaignArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCampaignCommand({ campaignArn: c.campaignArn }));
			toast.success('Campaign deleted');
			await tabLoader.refresh('campaigns');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Recommender: create / update / delete / start / stop / detail ---

	let createRecommenderModal = $state<Modal | null>(null);
	let creatingRecommender = $state(false);
	let createRecommenderError = $state<string | null>(null);
	let newRecommenderName = $state('');
	let newRecommenderDatasetGroupArn = $state('');
	let newRecommenderRecipeArn = $state('');
	let newRecommenderMinRPS = $state<number | undefined>();

	function openCreateRecommenderModal(): void {
		createRecommenderError = null;
		newRecommenderName = '';
		newRecommenderDatasetGroupArn = '';
		newRecommenderRecipeArn = '';
		newRecommenderMinRPS = undefined;
		createRecommenderModal?.open();
	}

	async function submitCreateRecommender(): Promise<void> {
		if (!newRecommenderName || !newRecommenderDatasetGroupArn || !newRecommenderRecipeArn) {
			createRecommenderError = 'Name, dataset group, and recipe ARN are required.';
			return;
		}
		creatingRecommender = true;
		createRecommenderError = null;
		try {
			await client().send(
				new CreateRecommenderCommand({
					name: newRecommenderName,
					datasetGroupArn: newRecommenderDatasetGroupArn,
					recipeArn: newRecommenderRecipeArn,
					recommenderConfig: newRecommenderMinRPS ? { minRecommendationRequestsPerSecond: newRecommenderMinRPS } : undefined
				})
			);
			toast.success('Recommender created');
			createRecommenderModal?.close();
			await tabLoader.refresh('recommenders');
		} catch (e) {
			const msg = describeError(e);
			createRecommenderError = msg;
			toast.error(msg);
		} finally {
			creatingRecommender = false;
		}
	}

	let editRecommenderModal = $state<Modal | null>(null);
	let editingRecommender = $state(false);
	let editRecommenderError = $state<string | null>(null);
	let editRecommenderArn = $state('');
	let editRecommenderMinRPS = $state<number | undefined>();

	function openEditRecommender(): void {
		editRecommenderError = null;
		editRecommenderArn = viewedRecommenderArn;
		editRecommenderMinRPS = viewedRecommender?.recommender?.recommenderConfig?.minRecommendationRequestsPerSecond;
		editRecommenderModal?.open();
	}

	async function submitEditRecommender(): Promise<void> {
		if (!editRecommenderMinRPS) {
			editRecommenderError = 'recommenderConfig is required -- set a minimum recommendation requests/sec.';
			return;
		}
		editingRecommender = true;
		editRecommenderError = null;
		try {
			await client().send(
				new UpdateRecommenderCommand({
					recommenderArn: editRecommenderArn,
					recommenderConfig: { minRecommendationRequestsPerSecond: editRecommenderMinRPS }
				})
			);
			toast.success('Recommender updated');
			editRecommenderModal?.close();
			await tabLoader.refresh('recommenders');
			await refreshRecommenderDetail();
		} catch (e) {
			const msg = describeError(e);
			editRecommenderError = msg;
			toast.error(msg);
		} finally {
			editingRecommender = false;
		}
	}

	async function deleteRecommender(r: RecommenderSummary): Promise<void> {
		if (!r.recommenderArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete recommender',
			message: `Delete recommender "${r.name ?? r.recommenderArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteRecommenderCommand({ recommenderArn: r.recommenderArn }));
			toast.success('Recommender deleted');
			await tabLoader.refresh('recommenders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function startRecommender(r: RecommenderSummary): Promise<void> {
		if (!r.recommenderArn) return;
		try {
			await client().send(new StartRecommenderCommand({ recommenderArn: r.recommenderArn }));
			toast.success('Recommender starting');
			await tabLoader.refresh('recommenders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function stopRecommender(r: RecommenderSummary): Promise<void> {
		if (!r.recommenderArn) return;
		try {
			await client().send(new StopRecommenderCommand({ recommenderArn: r.recommenderArn }));
			toast.success('Recommender stopping');
			await tabLoader.refresh('recommenders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Event Tracker: create / delete / detail ---

	let createTrackerModal = $state<Modal | null>(null);
	let creatingTracker = $state(false);
	let createTrackerError = $state<string | null>(null);
	let newTrackerName = $state('');
	let newTrackerDatasetGroupArn = $state('');

	function openCreateTrackerModal(): void {
		createTrackerError = null;
		newTrackerName = '';
		newTrackerDatasetGroupArn = '';
		createTrackerModal?.open();
	}

	async function submitCreateTracker(): Promise<void> {
		if (!newTrackerName || !newTrackerDatasetGroupArn) {
			createTrackerError = 'Name and dataset group are required.';
			return;
		}
		creatingTracker = true;
		createTrackerError = null;
		try {
			await client().send(new CreateEventTrackerCommand({ name: newTrackerName, datasetGroupArn: newTrackerDatasetGroupArn }));
			toast.success('Event tracker created');
			createTrackerModal?.close();
			await tabLoader.refresh('trackers');
		} catch (e) {
			const msg = describeError(e);
			createTrackerError = msg;
			toast.error(msg);
		} finally {
			creatingTracker = false;
		}
	}

	async function deleteTracker(t: EventTrackerSummary): Promise<void> {
		if (!t.eventTrackerArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete event tracker',
			message: `Delete event tracker "${t.name ?? t.eventTrackerArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteEventTrackerCommand({ eventTrackerArn: t.eventTrackerArn }));
			toast.success('Event tracker deleted');
			await tabLoader.refresh('trackers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Get Recommendations tester (personalizeruntime client) ---

	let recommendCampaignArn = $state('');
	let recommendUserId = $state('');
	let recommendNumResults = $state<number | undefined>();
	let recommending = $state(false);
	let recommendError = $state<string | null>(null);
	let recommendResults = $state<{ itemId?: string; score?: number }[]>([]);
	let recommendRan = $state(false);

	async function runGetRecommendations(): Promise<void> {
		if (!recommendCampaignArn.trim()) {
			recommendError = 'A campaign or recommender ARN is required.';
			return;
		}
		recommending = true;
		recommendError = null;
		recommendRan = false;
		try {
			const resp = await runtimeClient().send(
				new GetRecommendationsCommand({
					campaignArn: recommendCampaignArn.trim(),
					userId: recommendUserId.trim() || undefined,
					numResults: recommendNumResults
				})
			);
			recommendResults = (resp.itemList ?? []).map((i) => ({ itemId: i.itemId, score: i.score }));
			recommendRan = true;
		} catch (e) {
			recommendError = describeError(e);
			toast.error(recommendError);
		} finally {
			recommending = false;
		}
	}

	// --- Detail (shared modal per family) ---

	let detailModal = $state<Modal | null>(null);
	let detailKind = $state<'group' | 'dataset' | 'solution' | 'version' | 'campaign' | 'recommender' | 'tracker' | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let viewedGroup = $state<DescribeDatasetGroupCommandOutput | null>(null);
	let viewedDataset = $state<DescribeDatasetCommandOutput | null>(null);
	let viewedSolution = $state<DescribeSolutionCommandOutput | null>(null);
	let viewedVersion = $state<DescribeSolutionVersionCommandOutput | null>(null);
	let viewedCampaign = $state<DescribeCampaignCommandOutput | null>(null);
	let viewedRecommender = $state<DescribeRecommenderCommandOutput | null>(null);
	let viewedTracker = $state<DescribeEventTrackerCommandOutput | null>(null);
	let viewedGroupArn = $state('');
	let viewedDatasetArn = $state('');
	let viewedSolutionArn = $state('');
	let viewedVersionArn = $state('');
	let viewedCampaignArn = $state('');
	let viewedRecommenderArn = $state('');
	let viewedTrackerArn = $state('');

	async function openGroupDetail(g: DatasetGroupSummary): Promise<void> {
		detailKind = 'group';
		viewedGroup = null;
		detailError = null;
		detailModal?.open();
		if (!g.datasetGroupArn) return;
		viewedGroupArn = g.datasetGroupArn;
		await refreshGroupDetail();
	}
	async function refreshGroupDetail(): Promise<void> {
		if (!viewedGroupArn) return;
		detailLoading = true;
		try {
			viewedGroup = await client().send(new DescribeDatasetGroupCommand({ datasetGroupArn: viewedGroupArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openDatasetDetail(d: DatasetSummary): Promise<void> {
		detailKind = 'dataset';
		viewedDataset = null;
		detailError = null;
		detailModal?.open();
		if (!d.datasetArn) return;
		viewedDatasetArn = d.datasetArn;
		await refreshDatasetDetail();
	}
	async function refreshDatasetDetail(): Promise<void> {
		if (!viewedDatasetArn) return;
		detailLoading = true;
		try {
			viewedDataset = await client().send(new DescribeDatasetCommand({ datasetArn: viewedDatasetArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openSolutionDetail(s: SolutionSummary): Promise<void> {
		detailKind = 'solution';
		viewedSolution = null;
		detailError = null;
		detailModal?.open();
		if (!s.solutionArn) return;
		viewedSolutionArn = s.solutionArn;
		await refreshSolutionDetail();
	}
	async function refreshSolutionDetail(): Promise<void> {
		if (!viewedSolutionArn) return;
		detailLoading = true;
		try {
			viewedSolution = await client().send(new DescribeSolutionCommand({ solutionArn: viewedSolutionArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openVersionDetail(v: SolutionVersionSummary): Promise<void> {
		detailKind = 'version';
		viewedVersion = null;
		detailError = null;
		detailModal?.open();
		if (!v.solutionVersionArn) return;
		viewedVersionArn = v.solutionVersionArn;
		await refreshVersionDetail();
	}
	async function refreshVersionDetail(): Promise<void> {
		if (!viewedVersionArn) return;
		detailLoading = true;
		try {
			viewedVersion = await client().send(new DescribeSolutionVersionCommand({ solutionVersionArn: viewedVersionArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openCampaignDetail(c: CampaignSummary): Promise<void> {
		detailKind = 'campaign';
		viewedCampaign = null;
		detailError = null;
		detailModal?.open();
		if (!c.campaignArn) return;
		viewedCampaignArn = c.campaignArn;
		await refreshCampaignDetail();
	}
	async function refreshCampaignDetail(): Promise<void> {
		if (!viewedCampaignArn) return;
		detailLoading = true;
		try {
			viewedCampaign = await client().send(new DescribeCampaignCommand({ campaignArn: viewedCampaignArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openRecommenderDetail(r: RecommenderSummary): Promise<void> {
		detailKind = 'recommender';
		viewedRecommender = null;
		detailError = null;
		detailModal?.open();
		if (!r.recommenderArn) return;
		viewedRecommenderArn = r.recommenderArn;
		await refreshRecommenderDetail();
	}
	async function refreshRecommenderDetail(): Promise<void> {
		if (!viewedRecommenderArn) return;
		detailLoading = true;
		try {
			viewedRecommender = await client().send(new DescribeRecommenderCommand({ recommenderArn: viewedRecommenderArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openTrackerDetail(t: EventTrackerSummary): Promise<void> {
		detailKind = 'tracker';
		viewedTracker = null;
		detailError = null;
		detailModal?.open();
		if (!t.eventTrackerArn) return;
		viewedTrackerArn = t.eventTrackerArn;
		detailLoading = true;
		try {
			viewedTracker = await client().send(new DescribeEventTrackerCommand({ eventTrackerArn: viewedTrackerArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	function refreshDetail(): void {
		if (detailKind === 'group') void refreshGroupDetail();
		else if (detailKind === 'dataset') void refreshDatasetDetail();
		else if (detailKind === 'solution') void refreshSolutionDetail();
		else if (detailKind === 'version') void refreshVersionDetail();
		else if (detailKind === 'campaign') void refreshCampaignDetail();
		else if (detailKind === 'recommender') void refreshRecommenderDetail();
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader icon={Sparkles} title="Amazon Personalize" description="Real-time personalization and recommendations" onRefresh={handleRefresh} color="indigo">
		{#snippet actions()}
			{#if activeTab === 'datasetgroups'}
				<button onclick={openCreateGroupModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create dataset group
				</button>
			{:else if activeTab === 'datasets'}
				<button onclick={openCreateDatasetModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create dataset
				</button>
			{:else if activeTab === 'solutions'}
				<button onclick={openCreateSolutionModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create solution
				</button>
			{:else if activeTab === 'solutionversions'}
				<button onclick={openCreateVersionModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Train version
				</button>
			{:else if activeTab === 'campaigns'}
				<button onclick={openCreateCampaignModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create campaign
				</button>
			{:else if activeTab === 'recommenders'}
				<button onclick={openCreateRecommenderModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create recommender
				</button>
			{:else if activeTab === 'trackers'}
				<button onclick={openCreateTrackerModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create event tracker
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="indigo" />
			{#if activeTab !== 'recommend'}
				<SearchInput bind:value={searchQuery} />
			{/if}
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'datasetgroups'}
				{#snippet groupStatusCell(g: DatasetGroupSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(g.status)}">{g.status ?? '—'}</span>
				{/snippet}
				{#snippet groupActionsCell(g: DatasetGroupSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openGroupDetail(g)} title="View" aria-label="View dataset group {g.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteDatasetGroup(g)} title="Delete" aria-label="Delete dataset group {g.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const groupColumns = defineColumns<DatasetGroupSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'domain', label: 'Domain' },
					{ key: 'status', label: 'Status', render: groupStatusCell },
					{ key: 'actions', label: '', render: groupActionsCell }
				])}
				<DataTable rows={filteredDatasetGroups} rowKey={(g) => g.datasetGroupArn ?? ''} columns={groupColumns} loading={tabLoader.isLoading('datasetgroups')} emptyMessage="No dataset groups found" />
			{:else if activeTab === 'datasets'}
				{#snippet datasetStatusCell(d: DatasetSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(d.status)}">{d.status ?? '—'}</span>
				{/snippet}
				{#snippet datasetActionsCell(d: DatasetSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDatasetDetail(d)} title="View" aria-label="View dataset {d.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteDataset(d)} title="Delete" aria-label="Delete dataset {d.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const datasetColumns = defineColumns<DatasetSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'datasetType', label: 'Type' },
					{ key: 'status', label: 'Status', render: datasetStatusCell },
					{ key: 'actions', label: '', render: datasetActionsCell }
				])}
				<DataTable rows={filteredDatasets} rowKey={(d) => d.datasetArn ?? ''} columns={datasetColumns} loading={tabLoader.isLoading('datasets')} emptyMessage="No datasets found" />
			{:else if activeTab === 'solutions'}
				{#snippet solutionStatusCell(s: SolutionSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.status)}">{s.status ?? '—'}</span>
				{/snippet}
				{#snippet solutionActionsCell(s: SolutionSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSolutionDetail(s)} title="View" aria-label="View solution {s.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteSolution(s)} title="Delete" aria-label="Delete solution {s.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const solutionColumns = defineColumns<SolutionSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'status', label: 'Status', render: solutionStatusCell },
					{ key: 'actions', label: '', render: solutionActionsCell }
				])}
				<DataTable rows={filteredSolutions} rowKey={(s) => s.solutionArn ?? ''} columns={solutionColumns} loading={tabLoader.isLoading('solutions')} emptyMessage="No solutions found" />
			{:else if activeTab === 'solutionversions'}
				{#snippet versionStatusCell(v: SolutionVersionSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(v.status)}">{v.status ?? '—'}</span>
				{/snippet}
				{#snippet versionActionsCell(v: SolutionVersionSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openVersionDetail(v)} title="View" aria-label="View solution version {v.solutionVersionArn}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						{#if v.status === 'CREATE PENDING' || v.status === 'CREATE IN_PROGRESS'}
							<button onclick={() => stopVersion(v)} title="Stop" aria-label="Stop solution version {v.solutionVersionArn}" class="text-gray-400 hover:text-red-500"><Square class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{@const versionColumns = defineColumns<SolutionVersionSummary>([
					{ key: 'solutionVersionArn', label: 'ARN' },
					{ key: 'trainingMode', label: 'Training Mode' },
					{ key: 'status', label: 'Status', render: versionStatusCell },
					{ key: 'actions', label: '', render: versionActionsCell }
				])}
				<DataTable rows={filteredSolutionVersions} rowKey={(v) => v.solutionVersionArn ?? ''} columns={versionColumns} loading={tabLoader.isLoading('solutionversions')} emptyMessage="No solution versions found" />
			{:else if activeTab === 'campaigns'}
				{#snippet campaignStatusCell(c: CampaignSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(c.status)}">{c.status ?? '—'}</span>
				{/snippet}
				{#snippet campaignActionsCell(c: CampaignSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openCampaignDetail(c)} title="View" aria-label="View campaign {c.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteCampaign(c)} title="Delete" aria-label="Delete campaign {c.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const campaignColumns = defineColumns<CampaignSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'status', label: 'Status', render: campaignStatusCell },
					{ key: 'actions', label: '', render: campaignActionsCell }
				])}
				<DataTable rows={filteredCampaigns} rowKey={(c) => c.campaignArn ?? ''} columns={campaignColumns} loading={tabLoader.isLoading('campaigns')} emptyMessage="No campaigns found" />
			{:else if activeTab === 'recommenders'}
				{#snippet recommenderStatusCell(r: RecommenderSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(r.status)}">{r.status ?? '—'}</span>
				{/snippet}
				{#snippet recommenderActionsCell(r: RecommenderSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openRecommenderDetail(r)} title="View" aria-label="View recommender {r.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						{#if r.status === 'ACTIVE' || r.status === 'INACTIVE'}
							{#if r.status === 'INACTIVE'}
								<button onclick={() => startRecommender(r)} title="Start" aria-label="Start recommender {r.name}" class="text-gray-400 hover:text-green-500"><Play class="w-4 h-4" /></button>
							{:else}
								<button onclick={() => stopRecommender(r)} title="Stop" aria-label="Stop recommender {r.name}" class="text-gray-400 hover:text-amber-500"><Square class="w-4 h-4" /></button>
							{/if}
						{/if}
						<button onclick={() => deleteRecommender(r)} title="Delete" aria-label="Delete recommender {r.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const recommenderColumns = defineColumns<RecommenderSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'status', label: 'Status', render: recommenderStatusCell },
					{ key: 'actions', label: '', render: recommenderActionsCell }
				])}
				<DataTable rows={filteredRecommenders} rowKey={(r) => r.recommenderArn ?? ''} columns={recommenderColumns} loading={tabLoader.isLoading('recommenders')} emptyMessage="No recommenders found" />
			{:else if activeTab === 'trackers'}
				{#snippet trackerActionsCell(t: EventTrackerSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTrackerDetail(t)} title="View" aria-label="View event tracker {t.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteTracker(t)} title="Delete" aria-label="Delete event tracker {t.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const trackerColumns = defineColumns<EventTrackerSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'status', label: 'Status' },
					{ key: 'actions', label: '', render: trackerActionsCell }
				])}
				<DataTable rows={filteredTrackers} rowKey={(t) => t.eventTrackerArn ?? ''} columns={trackerColumns} loading={tabLoader.isLoading('trackers')} emptyMessage="No event trackers found" />
			{:else if activeTab === 'recommend'}
				<div class="space-y-4 max-w-lg">
					<p class="text-xs text-slate-500 dark:text-slate-400">
						Calls <code>GetRecommendations</code> on the Personalize Runtime client (a sibling of the control-plane
						client used everywhere else on this page) against a deployed campaign.
					</p>
					<div>
						<label for="pz-rec-campaign" class="text-sm text-slate-600 dark:text-slate-300">Campaign ARN</label>
						<input id="pz-rec-campaign" bind:value={recommendCampaignArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
					<div class="grid grid-cols-2 gap-2">
						<div>
							<label for="pz-rec-user" class="text-sm text-slate-600 dark:text-slate-300">User ID (optional)</label>
							<input id="pz-rec-user" bind:value={recommendUserId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						</div>
						<div>
							<label for="pz-rec-num" class="text-sm text-slate-600 dark:text-slate-300">Num results (optional)</label>
							<input id="pz-rec-num" type="number" bind:value={recommendNumResults} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						</div>
					</div>
					<button onclick={runGetRecommendations} disabled={recommending} class="flex items-center gap-2 px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm font-medium hover:bg-indigo-700 disabled:opacity-50">
						{recommending ? 'Fetching…' : 'Get Recommendations'}
					</button>
					{#if recommendError}<p class="text-sm text-red-600 dark:text-red-400">{recommendError}</p>{/if}
					{#if recommendRan}
						{#if recommendResults.length === 0}
							<div class="text-center py-8 text-gray-500 dark:text-gray-400">No recommendations returned</div>
						{:else}
							<div class="space-y-2">
								{#each recommendResults as item, i (i)}
									<div class="flex items-center justify-between p-2 rounded-lg bg-gray-50 dark:bg-slate-700/50 text-sm">
										<span class="text-gray-900 dark:text-white">{item.itemId}</span>
										<span class="text-gray-500 dark:text-gray-400">{item.score !== undefined ? item.score.toFixed(4) : '—'}</span>
									</div>
								{/each}
							</div>
						{/if}
					{/if}
				</div>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createGroupModal} title="Create Dataset Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-group-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="pz-group-name" bind:value={newGroupName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pz-group-domain" class="text-sm text-slate-600 dark:text-slate-300">Domain (optional -- empty makes a Custom group)</label>
				<select id="pz-group-domain" bind:value={newGroupDomain} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each DOMAINS as d (d)}<option value={d}>{d || 'Custom (none)'}</option>{/each}
				</select>
			</div>
			{#if createGroupError}<p class="text-sm text-red-600 dark:text-red-400">{createGroupError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createGroupModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateGroup} disabled={creatingGroup} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingGroup ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createDatasetModal} title="Create Dataset">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-dataset-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="pz-dataset-name" bind:value={newDatasetName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pz-dataset-group" class="text-sm text-slate-600 dark:text-slate-300">Dataset group</label>
				<select id="pz-dataset-group" bind:value={newDatasetGroupArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a dataset group…</option>
					{#each datasetGroups as g (g.datasetGroupArn)}<option value={g.datasetGroupArn}>{g.name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="pz-dataset-type" class="text-sm text-slate-600 dark:text-slate-300">Dataset type</label>
				<select id="pz-dataset-type" bind:value={newDatasetType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each DATASET_TYPES as t (t)}<option value={t}>{t}</option>{/each}
				</select>
			</div>
			<div>
				<label for="pz-dataset-schema" class="text-sm text-slate-600 dark:text-slate-300">Schema ARN (from an existing CreateSchema call)</label>
				<input id="pz-dataset-schema" bind:value={newDatasetSchemaArn} placeholder="arn:aws:personalize:::schema/my-schema" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createDatasetError}<p class="text-sm text-red-600 dark:text-red-400">{createDatasetError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createDatasetModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateDataset} disabled={creatingDataset} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingDataset ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editDatasetModal} title="Update Dataset">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-edit-dataset-schema" class="text-sm text-slate-600 dark:text-slate-300">New schema ARN</label>
				<input id="pz-edit-dataset-schema" bind:value={editDatasetSchemaArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editDatasetError}<p class="text-sm text-red-600 dark:text-red-400">{editDatasetError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editDatasetModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditDataset} disabled={editingDataset} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{editingDataset ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createSolutionModal} title="Create Solution">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-solution-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="pz-solution-name" bind:value={newSolutionName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pz-solution-group" class="text-sm text-slate-600 dark:text-slate-300">Dataset group</label>
				<select id="pz-solution-group" bind:value={newSolutionDatasetGroupArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a dataset group…</option>
					{#each datasetGroups as g (g.datasetGroupArn)}<option value={g.datasetGroupArn}>{g.name}</option>{/each}
				</select>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newSolutionPerformAutoML} /> Perform AutoML (picks the best recipe automatically)
			</label>
			{#if !newSolutionPerformAutoML}
				<div>
					<label for="pz-solution-recipe" class="text-sm text-slate-600 dark:text-slate-300">Recipe ARN</label>
					<input id="pz-solution-recipe" bind:value={newSolutionRecipeArn} placeholder="arn:aws:personalize:::recipe/aws-user-personalization" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if createSolutionError}<p class="text-sm text-red-600 dark:text-red-400">{createSolutionError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createSolutionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateSolution} disabled={creatingSolution} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingSolution ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editSolutionModal} title="Update Solution">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editSolutionAutoTraining} /> Perform auto-training
			</label>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editSolutionIncrementalUpdate} /> Perform incremental update
			</label>
			{#if editSolutionError}<p class="text-sm text-red-600 dark:text-red-400">{editSolutionError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editSolutionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditSolution} disabled={editingSolution} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{editingSolution ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createVersionModal} title="Train Solution Version">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-version-solution" class="text-sm text-slate-600 dark:text-slate-300">Solution</label>
				<select id="pz-version-solution" bind:value={newVersionSolutionArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a solution…</option>
					{#each solutions as s (s.solutionArn)}<option value={s.solutionArn}>{s.name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="pz-version-mode" class="text-sm text-slate-600 dark:text-slate-300">Training mode (optional)</label>
				<select id="pz-version-mode" bind:value={newVersionTrainingMode} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each TRAINING_MODES as m (m)}<option value={m}>{m || 'Default'}</option>{/each}
				</select>
			</div>
			{#if createVersionError}<p class="text-sm text-red-600 dark:text-red-400">{createVersionError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createVersionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateVersion} disabled={creatingVersion} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingVersion ? 'Starting…' : 'Train'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createCampaignModal} title="Create Campaign">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-campaign-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="pz-campaign-name" bind:value={newCampaignName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pz-campaign-version" class="text-sm text-slate-600 dark:text-slate-300">Solution version</label>
				<select id="pz-campaign-version" bind:value={newCampaignSolutionVersionArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a solution version…</option>
					{#each solutionVersions as v (v.solutionVersionArn)}<option value={v.solutionVersionArn}>{v.solutionVersionArn}</option>{/each}
				</select>
			</div>
			<div>
				<label for="pz-campaign-tps" class="text-sm text-slate-600 dark:text-slate-300">Min provisioned TPS (optional)</label>
				<input id="pz-campaign-tps" type="number" bind:value={newCampaignMinTPS} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createCampaignError}<p class="text-sm text-red-600 dark:text-red-400">{createCampaignError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createCampaignModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateCampaign} disabled={creatingCampaign} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingCampaign ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editCampaignModal} title="Update Campaign">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-edit-campaign-version" class="text-sm text-slate-600 dark:text-slate-300">Redeploy to solution version (optional)</label>
				<select id="pz-edit-campaign-version" bind:value={editCampaignSolutionVersionArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Unchanged</option>
					{#each solutionVersions as v (v.solutionVersionArn)}<option value={v.solutionVersionArn}>{v.solutionVersionArn}</option>{/each}
				</select>
			</div>
			<div>
				<label for="pz-edit-campaign-tps" class="text-sm text-slate-600 dark:text-slate-300">Min provisioned TPS (optional)</label>
				<input id="pz-edit-campaign-tps" type="number" bind:value={editCampaignMinTPS} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editCampaignError}<p class="text-sm text-red-600 dark:text-red-400">{editCampaignError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editCampaignModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditCampaign} disabled={editingCampaign} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{editingCampaign ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createRecommenderModal} title="Create Recommender">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-recommender-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="pz-recommender-name" bind:value={newRecommenderName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pz-recommender-group" class="text-sm text-slate-600 dark:text-slate-300">Dataset group</label>
				<select id="pz-recommender-group" bind:value={newRecommenderDatasetGroupArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a dataset group…</option>
					{#each datasetGroups as g (g.datasetGroupArn)}<option value={g.datasetGroupArn}>{g.name}</option>{/each}
				</select>
			</div>
			<div>
				<label for="pz-recommender-recipe" class="text-sm text-slate-600 dark:text-slate-300">Recipe ARN</label>
				<input id="pz-recommender-recipe" bind:value={newRecommenderRecipeArn} placeholder="arn:aws:personalize:::recipe/aws-ecomm-popular-items-by-views" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pz-recommender-rps" class="text-sm text-slate-600 dark:text-slate-300">Min recommendation requests/sec (optional)</label>
				<input id="pz-recommender-rps" type="number" bind:value={newRecommenderMinRPS} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createRecommenderError}<p class="text-sm text-red-600 dark:text-red-400">{createRecommenderError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createRecommenderModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateRecommender} disabled={creatingRecommender} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingRecommender ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editRecommenderModal} title="Update Recommender">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-edit-recommender-rps" class="text-sm text-slate-600 dark:text-slate-300">Min recommendation requests/sec</label>
				<input id="pz-edit-recommender-rps" type="number" bind:value={editRecommenderMinRPS} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">recommenderConfig is a required member of UpdateRecommenderInput on the real API.</p>
			{#if editRecommenderError}<p class="text-sm text-red-600 dark:text-red-400">{editRecommenderError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editRecommenderModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditRecommender} disabled={editingRecommender} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{editingRecommender ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createTrackerModal} title="Create Event Tracker">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pz-tracker-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="pz-tracker-name" bind:value={newTrackerName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pz-tracker-group" class="text-sm text-slate-600 dark:text-slate-300">Dataset group</label>
				<select id="pz-tracker-group" bind:value={newTrackerDatasetGroupArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a dataset group…</option>
					{#each datasetGroups as g (g.datasetGroupArn)}<option value={g.datasetGroupArn}>{g.name}</option>{/each}
				</select>
			</div>
			{#if createTrackerError}<p class="text-sm text-red-600 dark:text-red-400">{createTrackerError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createTrackerModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateTracker} disabled={creatingTracker} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingTracker ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal
	bind:this={detailModal}
	title={detailKind === 'group' ? 'Dataset Group' : detailKind === 'dataset' ? 'Dataset' : detailKind === 'solution' ? 'Solution' : detailKind === 'version' ? 'Solution Version' : detailKind === 'campaign' ? 'Campaign' : detailKind === 'recommender' ? 'Recommender' : 'Event Tracker'}
>
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if detailKind === 'group' && viewedGroup?.datasetGroup}
			{@const g = viewedGroup.datasetGroup}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{g.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{g.datasetGroupArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Domain</dt><dd class="text-slate-900 dark:text-white">{g.domain ?? '— (Custom)'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(g.status)}">{g.status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Role ARN</dt><dd class="break-all text-slate-900 dark:text-white">{g.roleArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Failure reason</dt><dd class="text-slate-900 dark:text-white">{g.failureReason ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(g.creationDateTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Updated</dt><dd class="text-slate-900 dark:text-white">{formatDate(g.lastUpdatedDateTime)}</dd></div>
			</dl>
		{:else if detailKind === 'dataset' && viewedDataset?.dataset}
			{@const d = viewedDataset.dataset}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{d.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{d.datasetArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{d.datasetType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Schema ARN</dt><dd class="break-all text-slate-900 dark:text-white">{d.schemaArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(d.status)}">{d.status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Tracking ID</dt><dd class="text-slate-900 dark:text-white">{d.trackingId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(d.creationDateTime)}</dd></div>
			</dl>
		{:else if detailKind === 'solution' && viewedSolution?.solution}
			{@const s = viewedSolution.solution}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{s.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{s.solutionArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Recipe ARN</dt><dd class="break-all text-slate-900 dark:text-white">{s.recipeArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(s.status)}">{s.status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Perform AutoML</dt><dd class="text-slate-900 dark:text-white">{s.performAutoML ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Perform auto-training</dt><dd class="text-slate-900 dark:text-white">{s.performAutoTraining ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Perform incremental update</dt><dd class="text-slate-900 dark:text-white">{s.performIncrementalUpdate ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(s.creationDateTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Updated</dt><dd class="text-slate-900 dark:text-white">{formatDate(s.lastUpdatedDateTime)}</dd></div>
			</dl>
		{:else if detailKind === 'version' && viewedVersion?.solutionVersion}
			{@const v = viewedVersion.solutionVersion}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{v.solutionVersionArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Solution ARN</dt><dd class="break-all text-slate-900 dark:text-white">{v.solutionArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(v.status)}">{v.status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Training mode</dt><dd class="text-slate-900 dark:text-white">{v.trainingMode ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Training hours</dt><dd class="text-slate-900 dark:text-white">{v.trainingHours ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Failure reason</dt><dd class="text-slate-900 dark:text-white">{v.failureReason ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(v.creationDateTime)}</dd></div>
			</dl>
		{:else if detailKind === 'campaign' && viewedCampaign?.campaign}
			{@const c = viewedCampaign.campaign}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{c.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{c.campaignArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Solution version ARN</dt><dd class="break-all text-slate-900 dark:text-white">{c.solutionVersionArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Min provisioned TPS</dt><dd class="text-slate-900 dark:text-white">{c.minProvisionedTPS ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(c.status)}">{c.status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(c.creationDateTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Updated</dt><dd class="text-slate-900 dark:text-white">{formatDate(c.lastUpdatedDateTime)}</dd></div>
			</dl>
		{:else if detailKind === 'recommender' && viewedRecommender?.recommender}
			{@const r = viewedRecommender.recommender}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{r.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{r.recommenderArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Recipe ARN</dt><dd class="break-all text-slate-900 dark:text-white">{r.recipeArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Min recommendation req/sec</dt><dd class="text-slate-900 dark:text-white">{r.recommenderConfig?.minRecommendationRequestsPerSecond ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(r.status)}">{r.status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(r.creationDateTime)}</dd></div>
			</dl>
		{:else if detailKind === 'tracker' && viewedTracker?.eventTracker}
			{@const t = viewedTracker.eventTracker}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{t.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{t.eventTrackerArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Tracking ID</dt><dd class="text-slate-900 dark:text-white">{t.trackingId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{t.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(t.creationDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if detailKind === 'group' || detailKind === 'dataset' || detailKind === 'solution' || detailKind === 'version' || detailKind === 'campaign' || detailKind === 'recommender'}
			<button type="button" onclick={refreshDetail} class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><RefreshCw class="w-4 h-4" /> Refresh</button>
		{/if}
		{#if detailKind === 'dataset' && viewedDataset?.dataset}
			<button type="button" onclick={openEditDataset} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700">Edit</button>
		{:else if detailKind === 'solution' && viewedSolution?.solution}
			<button type="button" onclick={openEditSolution} class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700"><Pencil class="w-4 h-4" /> Edit</button>
		{:else if detailKind === 'campaign' && viewedCampaign?.campaign}
			<button type="button" onclick={openEditCampaign} class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700"><Pencil class="w-4 h-4" /> Edit</button>
		{:else if detailKind === 'recommender' && viewedRecommender?.recommender}
			<button type="button" onclick={openEditRecommender} class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>
