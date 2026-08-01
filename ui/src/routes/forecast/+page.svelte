<script lang="ts">
	// Amazon Forecast resources are created, then move through an async
	// CREATE_PENDING -> CREATE_IN_PROGRESS -> ACTIVE/CREATE_FAILED lifecycle
	// (Predictor/Forecast/Monitor all carry a `Status` field) rather than
	// existing instantly like most AWS resources -- there is no "wait for
	// completion" call, only Describe/List polling, so the detail modal's
	// Refresh button is how a caller would observe that transition.
	//
	// Five listable families are wired here: DatasetGroup, Dataset, Predictor,
	// Forecast, Monitor. The real API additionally has DatasetImportJob,
	// PredictorBacktestExportJob, ForecastExportJob, Explainability(Export),
	// WhatIfAnalysis/Forecast(Export) -- all genuinely implemented backend
	// families (see services/forecast/PARITY.md) but out of scope for this
	// page; DeleteResourceTree/StopResource/ResumeResource are real
	// cascade/lifecycle ops also not surfaced here. None of that is a backend
	// limitation, just a UI scope line.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getForecastClient } from '$lib/aws-client';
	import {
		ListDatasetGroupsCommand,
		ListDatasetsCommand,
		ListForecastsCommand,
		ListMonitorsCommand,
		ListPredictorsCommand,
		DescribeDatasetGroupCommand,
		DescribeDatasetCommand,
		DescribeForecastCommand,
		DescribeMonitorCommand,
		DescribePredictorCommand,
		CreateDatasetGroupCommand,
		CreateDatasetCommand,
		CreateForecastCommand,
		CreateMonitorCommand,
		CreateAutoPredictorCommand,
		UpdateDatasetGroupCommand,
		DeleteDatasetGroupCommand,
		DeleteDatasetCommand,
		DeleteForecastCommand,
		DeleteMonitorCommand,
		DeletePredictorCommand,
		type DatasetGroupSummary,
		type DatasetSummary,
		type ForecastSummary,
		type MonitorSummary,
		type PredictorSummary,
		type SchemaAttribute,
		type DescribeDatasetGroupCommandOutput,
		type DescribeDatasetCommandOutput,
		type DescribePredictorCommandOutput,
		type DescribeForecastCommandOutput,
		type DescribeMonitorCommandOutput
	} from '@aws-sdk/client-forecast';
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
	import { TrendingUp, Plus, Trash2, Eye, X, RefreshCw } from 'lucide-svelte';

	const client = regionalClient(getForecastClient);

	type TabId = 'datasetgroups' | 'datasets' | 'predictors' | 'forecasts' | 'monitors';

	const tabs: TabDef[] = [
		{ id: 'datasetgroups', label: 'Dataset Groups' },
		{ id: 'datasets', label: 'Datasets' },
		{ id: 'predictors', label: 'Predictors' },
		{ id: 'forecasts', label: 'Forecasts' },
		{ id: 'monitors', label: 'Monitors' }
	];

	const DOMAINS = ['CUSTOM', 'EC2_CAPACITY', 'INVENTORY_PLANNING', 'METRICS', 'RETAIL', 'WEB_TRAFFIC', 'WORK_FORCE'];
	const DATASET_TYPES = ['TARGET_TIME_SERIES', 'RELATED_TIME_SERIES', 'ITEM_METADATA'];
	const ATTRIBUTE_TYPES = ['string', 'integer', 'float', 'geolocation', 'timestamp'];

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
		if (status?.includes('PENDING') || status?.includes('IN_PROGRESS')) {
			return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('datasetgroups');
	let searchQuery = $state('');

	let datasetGroups = $state<DatasetGroupSummary[]>([]);
	let datasets = $state<DatasetSummary[]>([]);
	let predictors = $state<PredictorSummary[]>([]);
	let forecasts = $state<ForecastSummary[]>([]);
	let monitors = $state<MonitorSummary[]>([]);

	async function fetchDatasetGroups(): Promise<void> {
		const resp = await client().send(new ListDatasetGroupsCommand({}));
		datasetGroups = resp.DatasetGroups ?? [];
	}
	async function fetchDatasets(): Promise<void> {
		const resp = await client().send(new ListDatasetsCommand({}));
		datasets = resp.Datasets ?? [];
	}
	async function fetchPredictors(): Promise<void> {
		const resp = await client().send(new ListPredictorsCommand({}));
		predictors = resp.Predictors ?? [];
	}
	async function fetchForecasts(): Promise<void> {
		const resp = await client().send(new ListForecastsCommand({}));
		forecasts = resp.Forecasts ?? [];
	}
	async function fetchMonitors(): Promise<void> {
		const resp = await client().send(new ListMonitorsCommand({}));
		monitors = resp.Monitors ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		datasetgroups: () => fetchDatasetGroups().catch(rethrowDescribed),
		datasets: () => fetchDatasets().catch(rethrowDescribed),
		predictors: () => fetchPredictors().catch(rethrowDescribed),
		forecasts: () => fetchForecasts().catch(rethrowDescribed),
		monitors: () => fetchMonitors().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// ARNs are only unique within a region -- clear any selected-resource
	// drill-down state on region change.
	onRegionChange(() => {
		detailModal?.close();
		detailKind = null;
		tabLoader.refresh(untrack(() => activeTab));
	});

	function matches(q: string, ...fields: (string | undefined)[]): boolean {
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}

	const filteredDatasetGroups = $derived(
		datasetGroups.filter((g) => matches(searchQuery.toLowerCase(), g.DatasetGroupName, g.DatasetGroupArn))
	);
	const filteredDatasets = $derived(
		datasets.filter((d) => matches(searchQuery.toLowerCase(), d.DatasetName, d.DatasetArn, d.DatasetType, d.Domain))
	);
	const filteredPredictors = $derived(
		predictors.filter((p) => matches(searchQuery.toLowerCase(), p.PredictorName, p.PredictorArn, p.Status))
	);
	const filteredForecasts = $derived(
		forecasts.filter((f) => matches(searchQuery.toLowerCase(), f.ForecastName, f.ForecastArn, f.Status))
	);
	const filteredMonitors = $derived(
		monitors.filter((m) => matches(searchQuery.toLowerCase(), m.MonitorName, m.MonitorArn, m.Status))
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Dataset Group create ---

	let createGroupModal = $state<Modal | null>(null);
	let creatingGroup = $state(false);
	let createGroupError = $state<string | null>(null);
	let newGroupName = $state('');
	let newGroupDomain = $state('RETAIL');

	function openCreateGroupModal(): void {
		createGroupError = null;
		newGroupName = '';
		newGroupDomain = 'RETAIL';
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
				new CreateDatasetGroupCommand({ DatasetGroupName: newGroupName, Domain: newGroupDomain as never })
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
		if (!g.DatasetGroupArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete dataset group',
			message: `Delete dataset group "${g.DatasetGroupName ?? g.DatasetGroupArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDatasetGroupCommand({ DatasetGroupArn: g.DatasetGroupArn }));
			toast.success('Dataset group deleted');
			await tabLoader.refresh('datasetgroups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Dataset create ---

	let createDatasetModal = $state<Modal | null>(null);
	let creatingDataset = $state(false);
	let createDatasetError = $state<string | null>(null);
	let newDatasetName = $state('');
	let newDatasetDomain = $state('RETAIL');
	let newDatasetType = $state('TARGET_TIME_SERIES');
	let newDatasetFrequency = $state('');
	let newDatasetAttributes = $state<{ name: string; type: string }[]>([{ name: '', type: 'string' }]);

	function openCreateDatasetModal(): void {
		createDatasetError = null;
		newDatasetName = '';
		newDatasetDomain = 'RETAIL';
		newDatasetType = 'TARGET_TIME_SERIES';
		newDatasetFrequency = '';
		newDatasetAttributes = [{ name: '', type: 'string' }];
		createDatasetModal?.open();
	}

	function addAttributeRow(): void {
		newDatasetAttributes = [...newDatasetAttributes, { name: '', type: 'string' }];
	}

	function removeAttributeRow(index: number): void {
		newDatasetAttributes = newDatasetAttributes.filter((_, i) => i !== index);
	}

	async function submitCreateDataset(): Promise<void> {
		const attrs = newDatasetAttributes.filter((a) => a.name.trim());
		if (!newDatasetName || attrs.length === 0) {
			createDatasetError = 'Name and at least one schema attribute are required.';
			return;
		}
		creatingDataset = true;
		createDatasetError = null;
		try {
			const attributes: SchemaAttribute[] = attrs.map((a) => ({
				AttributeName: a.name.trim(),
				AttributeType: a.type as never
			}));
			await client().send(
				new CreateDatasetCommand({
					DatasetName: newDatasetName,
					Domain: newDatasetDomain as never,
					DatasetType: newDatasetType as never,
					DataFrequency: newDatasetFrequency || undefined,
					Schema: { Attributes: attributes }
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

	async function deleteDataset(d: DatasetSummary): Promise<void> {
		if (!d.DatasetArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete dataset',
			message: `Delete dataset "${d.DatasetName ?? d.DatasetArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDatasetCommand({ DatasetArn: d.DatasetArn }));
			toast.success('Dataset deleted');
			await tabLoader.refresh('datasets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Predictor create (via CreateAutoPredictor, the current recommended API) ---

	let createPredictorModal = $state<Modal | null>(null);
	let creatingPredictor = $state(false);
	let createPredictorError = $state<string | null>(null);
	let newPredictorName = $state('');
	let newPredictorHorizon = $state<number | undefined>();
	let newPredictorDatasetGroupArn = $state('');

	function openCreatePredictorModal(): void {
		createPredictorError = null;
		newPredictorName = '';
		newPredictorHorizon = undefined;
		newPredictorDatasetGroupArn = '';
		createPredictorModal?.open();
	}

	async function submitCreatePredictor(): Promise<void> {
		if (!newPredictorName) {
			createPredictorError = 'Name is required.';
			return;
		}
		creatingPredictor = true;
		createPredictorError = null;
		try {
			await client().send(
				new CreateAutoPredictorCommand({
					PredictorName: newPredictorName,
					ForecastHorizon: newPredictorHorizon,
					DataConfig: newPredictorDatasetGroupArn ? { DatasetGroupArn: newPredictorDatasetGroupArn } : undefined
				})
			);
			toast.success('Predictor creation started');
			createPredictorModal?.close();
			await tabLoader.refresh('predictors');
		} catch (e) {
			const msg = describeError(e);
			createPredictorError = msg;
			toast.error(msg);
		} finally {
			creatingPredictor = false;
		}
	}

	async function deletePredictor(p: PredictorSummary): Promise<void> {
		if (!p.PredictorArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete predictor',
			message: `Delete predictor "${p.PredictorName ?? p.PredictorArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeletePredictorCommand({ PredictorArn: p.PredictorArn }));
			toast.success('Predictor deleted');
			await tabLoader.refresh('predictors');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Forecast create ---

	let createForecastModal = $state<Modal | null>(null);
	let creatingForecast = $state(false);
	let createForecastError = $state<string | null>(null);
	let newForecastName = $state('');
	let newForecastPredictorArn = $state('');

	function openCreateForecastModal(): void {
		createForecastError = null;
		newForecastName = '';
		newForecastPredictorArn = '';
		createForecastModal?.open();
	}

	async function submitCreateForecast(): Promise<void> {
		if (!newForecastName || !newForecastPredictorArn) {
			createForecastError = 'Name and predictor are required.';
			return;
		}
		creatingForecast = true;
		createForecastError = null;
		try {
			await client().send(
				new CreateForecastCommand({ ForecastName: newForecastName, PredictorArn: newForecastPredictorArn })
			);
			toast.success('Forecast generation started');
			createForecastModal?.close();
			await tabLoader.refresh('forecasts');
		} catch (e) {
			const msg = describeError(e);
			createForecastError = msg;
			toast.error(msg);
		} finally {
			creatingForecast = false;
		}
	}

	async function deleteForecast(f: ForecastSummary): Promise<void> {
		if (!f.ForecastArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete forecast',
			message: `Delete forecast "${f.ForecastName ?? f.ForecastArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteForecastCommand({ ForecastArn: f.ForecastArn }));
			toast.success('Forecast deleted');
			await tabLoader.refresh('forecasts');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Monitor create ---

	let createMonitorModal = $state<Modal | null>(null);
	let creatingMonitor = $state(false);
	let createMonitorError = $state<string | null>(null);
	let newMonitorName = $state('');
	let newMonitorResourceArn = $state('');

	function openCreateMonitorModal(): void {
		createMonitorError = null;
		newMonitorName = '';
		newMonitorResourceArn = '';
		createMonitorModal?.open();
	}

	async function submitCreateMonitor(): Promise<void> {
		if (!newMonitorName || !newMonitorResourceArn) {
			createMonitorError = 'Name and predictor are required.';
			return;
		}
		creatingMonitor = true;
		createMonitorError = null;
		try {
			await client().send(
				new CreateMonitorCommand({ MonitorName: newMonitorName, ResourceArn: newMonitorResourceArn })
			);
			toast.success('Monitor created');
			createMonitorModal?.close();
			await tabLoader.refresh('monitors');
		} catch (e) {
			const msg = describeError(e);
			createMonitorError = msg;
			toast.error(msg);
		} finally {
			creatingMonitor = false;
		}
	}

	async function deleteMonitor(m: MonitorSummary): Promise<void> {
		if (!m.MonitorArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete monitor',
			message: `Delete monitor "${m.MonitorName ?? m.MonitorArn}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteMonitorCommand({ MonitorArn: m.MonitorArn }));
			toast.success('Monitor deleted');
			await tabLoader.refresh('monitors');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Detail (shared modal per family) ---

	let detailModal = $state<Modal | null>(null);
	let detailKind = $state<'group' | 'dataset' | 'predictor' | 'forecast' | 'monitor' | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let viewedGroup = $state<DescribeDatasetGroupCommandOutput | null>(null);
	let viewedDataset = $state<DescribeDatasetCommandOutput | null>(null);
	let viewedPredictor = $state<DescribePredictorCommandOutput | null>(null);
	let viewedForecast = $state<DescribeForecastCommandOutput | null>(null);
	let viewedMonitor = $state<DescribeMonitorCommandOutput | null>(null);
	let viewedGroupArn = $state('');
	let viewedDatasetArn = $state('');
	let viewedPredictorArn = $state('');
	let viewedForecastArn = $state('');
	let viewedMonitorArn = $state('');

	async function openGroupDetail(g: DatasetGroupSummary): Promise<void> {
		detailKind = 'group';
		viewedGroup = null;
		detailError = null;
		detailModal?.open();
		if (!g.DatasetGroupArn) return;
		viewedGroupArn = g.DatasetGroupArn;
		await refreshGroupDetail();
	}
	async function refreshGroupDetail(): Promise<void> {
		if (!viewedGroupArn) return;
		detailLoading = true;
		try {
			viewedGroup = await client().send(new DescribeDatasetGroupCommand({ DatasetGroupArn: viewedGroupArn }));
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
		if (!d.DatasetArn) return;
		viewedDatasetArn = d.DatasetArn;
		detailLoading = true;
		try {
			viewedDataset = await client().send(new DescribeDatasetCommand({ DatasetArn: viewedDatasetArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openPredictorDetail(p: PredictorSummary): Promise<void> {
		detailKind = 'predictor';
		viewedPredictor = null;
		detailError = null;
		detailModal?.open();
		if (!p.PredictorArn) return;
		viewedPredictorArn = p.PredictorArn;
		await refreshPredictorDetail();
	}
	async function refreshPredictorDetail(): Promise<void> {
		if (!viewedPredictorArn) return;
		detailLoading = true;
		try {
			viewedPredictor = await client().send(new DescribePredictorCommand({ PredictorArn: viewedPredictorArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openForecastDetail(f: ForecastSummary): Promise<void> {
		detailKind = 'forecast';
		viewedForecast = null;
		detailError = null;
		detailModal?.open();
		if (!f.ForecastArn) return;
		viewedForecastArn = f.ForecastArn;
		await refreshForecastDetail();
	}
	async function refreshForecastDetail(): Promise<void> {
		if (!viewedForecastArn) return;
		detailLoading = true;
		try {
			viewedForecast = await client().send(new DescribeForecastCommand({ ForecastArn: viewedForecastArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	async function openMonitorDetail(m: MonitorSummary): Promise<void> {
		detailKind = 'monitor';
		viewedMonitor = null;
		detailError = null;
		detailModal?.open();
		if (!m.MonitorArn) return;
		viewedMonitorArn = m.MonitorArn;
		await refreshMonitorDetail();
	}
	async function refreshMonitorDetail(): Promise<void> {
		if (!viewedMonitorArn) return;
		detailLoading = true;
		try {
			viewedMonitor = await client().send(new DescribeMonitorCommand({ MonitorArn: viewedMonitorArn }));
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
	}

	function refreshDetail(): void {
		if (detailKind === 'group') void refreshGroupDetail();
		else if (detailKind === 'predictor') void refreshPredictorDetail();
		else if (detailKind === 'forecast') void refreshForecastDetail();
		else if (detailKind === 'monitor') void refreshMonitorDetail();
	}

	// --- Edit (Dataset Group only -- UpdateDatasetGroup replaces DatasetArns wholesale) ---

	let editGroupModal = $state<Modal | null>(null);
	let editingGroup = $state(false);
	let editGroupError = $state<string | null>(null);
	let editGroupArn = $state('');
	let editGroupDatasetArns = $state('');

	function openEditGroup(): void {
		editGroupError = null;
		editGroupArn = viewedGroupArn;
		editGroupDatasetArns = (viewedGroup?.DatasetArns ?? []).join(', ');
		editGroupModal?.open();
	}

	async function submitEditGroup(): Promise<void> {
		const datasetArns = editGroupDatasetArns
			.split(',')
			.map((s) => s.trim())
			.filter(Boolean);
		editingGroup = true;
		editGroupError = null;
		try {
			await client().send(new UpdateDatasetGroupCommand({ DatasetGroupArn: editGroupArn, DatasetArns: datasetArns }));
			toast.success('Dataset group updated');
			editGroupModal?.close();
			await tabLoader.refresh('datasetgroups');
			await refreshGroupDetail();
		} catch (e) {
			const msg = describeError(e);
			editGroupError = msg;
			toast.error(msg);
		} finally {
			editingGroup = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={TrendingUp}
		title="Amazon Forecast"
		description="Time-series forecasting service"
		onRefresh={handleRefresh}
		color="violet"
	>
		{#snippet actions()}
			{#if activeTab === 'datasetgroups'}
				<button onclick={openCreateGroupModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Create dataset group
				</button>
			{:else if activeTab === 'datasets'}
				<button onclick={openCreateDatasetModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Create dataset
				</button>
			{:else if activeTab === 'predictors'}
				<button onclick={openCreatePredictorModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Train predictor
				</button>
			{:else if activeTab === 'forecasts'}
				<button onclick={openCreateForecastModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Generate forecast
				</button>
			{:else if activeTab === 'monitors'}
				<button onclick={openCreateMonitorModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Create monitor
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="violet" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'datasetgroups'}
				{#snippet groupActionsCell(g: DatasetGroupSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openGroupDetail(g)} title="View" aria-label="View dataset group {g.DatasetGroupName}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteDatasetGroup(g)} title="Delete" aria-label="Delete dataset group {g.DatasetGroupName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const groupColumns = defineColumns<DatasetGroupSummary>([
					{ key: 'DatasetGroupName', label: 'Name' },
					{ key: 'DatasetGroupArn', label: 'ARN' },
					{ key: 'actions', label: '', render: groupActionsCell }
				])}
				<DataTable rows={filteredDatasetGroups} rowKey={(g) => g.DatasetGroupArn ?? ''} columns={groupColumns} loading={tabLoader.isLoading('datasetgroups')} emptyMessage="No dataset groups found" />
			{:else if activeTab === 'datasets'}
				{#snippet datasetActionsCell(d: DatasetSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDatasetDetail(d)} title="View" aria-label="View dataset {d.DatasetName}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteDataset(d)} title="Delete" aria-label="Delete dataset {d.DatasetName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const datasetColumns = defineColumns<DatasetSummary>([
					{ key: 'DatasetName', label: 'Name' },
					{ key: 'DatasetType', label: 'Type' },
					{ key: 'Domain', label: 'Domain' },
					{ key: 'actions', label: '', render: datasetActionsCell }
				])}
				<DataTable rows={filteredDatasets} rowKey={(d) => d.DatasetArn ?? ''} columns={datasetColumns} loading={tabLoader.isLoading('datasets')} emptyMessage="No datasets found" />
			{:else if activeTab === 'predictors'}
				{#snippet predictorStatusCell(p: PredictorSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(p.Status)}">{p.Status ?? '—'}</span>
				{/snippet}
				{#snippet predictorActionsCell(p: PredictorSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openPredictorDetail(p)} title="View" aria-label="View predictor {p.PredictorName}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deletePredictor(p)} title="Delete" aria-label="Delete predictor {p.PredictorName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const predictorColumns = defineColumns<PredictorSummary>([
					{ key: 'PredictorName', label: 'Name' },
					{ key: 'Status', label: 'Status', render: predictorStatusCell },
					{ key: 'actions', label: '', render: predictorActionsCell }
				])}
				<DataTable rows={filteredPredictors} rowKey={(p) => p.PredictorArn ?? ''} columns={predictorColumns} loading={tabLoader.isLoading('predictors')} emptyMessage="No predictors found" />
			{:else if activeTab === 'forecasts'}
				{#snippet forecastStatusCell(f: ForecastSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(f.Status)}">{f.Status ?? '—'}</span>
				{/snippet}
				{#snippet forecastActionsCell(f: ForecastSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openForecastDetail(f)} title="View" aria-label="View forecast {f.ForecastName}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteForecast(f)} title="Delete" aria-label="Delete forecast {f.ForecastName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const forecastColumns = defineColumns<ForecastSummary>([
					{ key: 'ForecastName', label: 'Name' },
					{ key: 'Status', label: 'Status', render: forecastStatusCell },
					{ key: 'actions', label: '', render: forecastActionsCell }
				])}
				<DataTable rows={filteredForecasts} rowKey={(f) => f.ForecastArn ?? ''} columns={forecastColumns} loading={tabLoader.isLoading('forecasts')} emptyMessage="No forecasts found" />
			{:else if activeTab === 'monitors'}
				{#snippet monitorStatusCell(m: MonitorSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(m.Status)}">{m.Status ?? '—'}</span>
				{/snippet}
				{#snippet monitorActionsCell(m: MonitorSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openMonitorDetail(m)} title="View" aria-label="View monitor {m.MonitorName}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteMonitor(m)} title="Delete" aria-label="Delete monitor {m.MonitorName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const monitorColumns = defineColumns<MonitorSummary>([
					{ key: 'MonitorName', label: 'Name' },
					{ key: 'Status', label: 'Status', render: monitorStatusCell },
					{ key: 'actions', label: '', render: monitorActionsCell }
				])}
				<DataTable rows={filteredMonitors} rowKey={(m) => m.MonitorArn ?? ''} columns={monitorColumns} loading={tabLoader.isLoading('monitors')} emptyMessage="No monitors found" />
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createGroupModal} title="Create Dataset Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fc-group-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="fc-group-name" bind:value={newGroupName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="fc-group-domain" class="text-sm text-slate-600 dark:text-slate-300">Domain</label>
				<select id="fc-group-domain" bind:value={newGroupDomain} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each DOMAINS as d (d)}<option value={d}>{d}</option>{/each}
				</select>
			</div>
			{#if createGroupError}<p class="text-sm text-red-600 dark:text-red-400">{createGroupError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createGroupModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateGroup} disabled={creatingGroup} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingGroup ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createDatasetModal} title="Create Dataset">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fc-dataset-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="fc-dataset-name" bind:value={newDatasetName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="fc-dataset-domain" class="text-sm text-slate-600 dark:text-slate-300">Domain</label>
					<select id="fc-dataset-domain" bind:value={newDatasetDomain} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						{#each DOMAINS as d (d)}<option value={d}>{d}</option>{/each}
					</select>
				</div>
				<div>
					<label for="fc-dataset-type" class="text-sm text-slate-600 dark:text-slate-300">Dataset type</label>
					<select id="fc-dataset-type" bind:value={newDatasetType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						{#each DATASET_TYPES as t (t)}<option value={t}>{t}</option>{/each}
					</select>
				</div>
			</div>
			<div>
				<label for="fc-dataset-freq" class="text-sm text-slate-600 dark:text-slate-300">Data frequency (optional, e.g. 1D, 1H)</label>
				<input id="fc-dataset-freq" bind:value={newDatasetFrequency} placeholder="1D" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<div class="flex items-center justify-between">
					<span class="text-sm text-slate-600 dark:text-slate-300">Schema attributes</span>
					<button type="button" onclick={addAttributeRow} class="text-xs text-violet-600 dark:text-violet-400 hover:underline">Add attribute</button>
				</div>
				{#each newDatasetAttributes as attr, index (index)}
					<div class="mt-2 flex items-center gap-2">
						<input bind:value={attr.name} placeholder="AttributeName" aria-label="Attribute name" class="w-1/2 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<select bind:value={attr.type} aria-label="Attribute type" class="w-1/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							{#each ATTRIBUTE_TYPES as t (t)}<option value={t}>{t}</option>{/each}
						</select>
						<button type="button" onclick={() => removeAttributeRow(index)} aria-label="Remove attribute row" class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button>
					</div>
				{/each}
			</div>
			{#if createDatasetError}<p class="text-sm text-red-600 dark:text-red-400">{createDatasetError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createDatasetModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateDataset} disabled={creatingDataset} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingDataset ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createPredictorModal} title="Train Predictor">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fc-pred-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="fc-pred-name" bind:value={newPredictorName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="fc-pred-group" class="text-sm text-slate-600 dark:text-slate-300">Dataset group (optional)</label>
				<select id="fc-pred-group" bind:value={newPredictorDatasetGroupArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">None</option>
					{#each datasetGroups as g (g.DatasetGroupArn)}
						<option value={g.DatasetGroupArn}>{g.DatasetGroupName}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="fc-pred-horizon" class="text-sm text-slate-600 dark:text-slate-300">Forecast horizon (optional)</label>
				<input id="fc-pred-horizon" type="number" bind:value={newPredictorHorizon} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">
				Trains via CreateAutoPredictor -- the modern recommended API. Training is asynchronous; watch the
				predictor's Status in its detail view.
			</p>
			{#if createPredictorError}<p class="text-sm text-red-600 dark:text-red-400">{createPredictorError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createPredictorModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreatePredictor} disabled={creatingPredictor} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingPredictor ? 'Starting…' : 'Train'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createForecastModal} title="Generate Forecast">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fc-forecast-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="fc-forecast-name" bind:value={newForecastName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="fc-forecast-predictor" class="text-sm text-slate-600 dark:text-slate-300">Predictor</label>
				<select id="fc-forecast-predictor" bind:value={newForecastPredictorArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a predictor…</option>
					{#each predictors as p (p.PredictorArn)}
						<option value={p.PredictorArn}>{p.PredictorName}</option>
					{/each}
				</select>
			</div>
			{#if createForecastError}<p class="text-sm text-red-600 dark:text-red-400">{createForecastError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createForecastModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateForecast} disabled={creatingForecast} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingForecast ? 'Starting…' : 'Generate'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createMonitorModal} title="Create Monitor">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fc-monitor-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="fc-monitor-name" bind:value={newMonitorName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="fc-monitor-resource" class="text-sm text-slate-600 dark:text-slate-300">Predictor</label>
				<select id="fc-monitor-resource" bind:value={newMonitorResourceArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">Select a predictor…</option>
					{#each predictors as p (p.PredictorArn)}
						<option value={p.PredictorArn}>{p.PredictorName}</option>
					{/each}
				</select>
			</div>
			{#if createMonitorError}<p class="text-sm text-red-600 dark:text-red-400">{createMonitorError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createMonitorModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateMonitor} disabled={creatingMonitor} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingMonitor ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal
	bind:this={detailModal}
	title={detailKind === 'group' ? 'Dataset Group' : detailKind === 'dataset' ? 'Dataset' : detailKind === 'predictor' ? 'Predictor' : detailKind === 'forecast' ? 'Forecast' : 'Monitor'}
>
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if detailKind === 'group' && viewedGroup}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.DatasetGroupName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedGroup.DatasetGroupArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Domain</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.Domain ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedGroup.Status)}">{viewedGroup.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Dataset ARNs</dt><dd class="text-slate-900 dark:text-white">{(viewedGroup.DatasetArns ?? []).join(', ') || 'None'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedGroup.CreationTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Modified</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedGroup.LastModificationTime)}</dd></div>
			</dl>
		{:else if detailKind === 'dataset' && viewedDataset}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.DatasetName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedDataset.DatasetArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Domain</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.Domain ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.DatasetType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Data frequency</dt><dd class="text-slate-900 dark:text-white">{viewedDataset.DataFrequency ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedDataset.Status)}">{viewedDataset.Status ?? '—'}</span></dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Schema</dt>
					<dd class="text-slate-900 dark:text-white">
						{#if (viewedDataset.Schema?.Attributes ?? []).length === 0}
							—
						{:else}
							<ul class="space-y-0.5">
								{#each viewedDataset.Schema?.Attributes ?? [] as attr, i (i)}
									<li><span class="font-mono text-xs">{attr.AttributeName}</span> : {attr.AttributeType}</li>
								{/each}
							</ul>
						{/if}
					</dd>
				</div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedDataset.CreationTime)}</dd></div>
			</dl>
		{:else if detailKind === 'predictor' && viewedPredictor}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedPredictor.PredictorName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedPredictor.PredictorArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedPredictor.Status)}">{viewedPredictor.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Message</dt><dd class="text-slate-900 dark:text-white">{viewedPredictor.Message ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Forecast horizon</dt><dd class="text-slate-900 dark:text-white">{viewedPredictor.ForecastHorizon ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Auto predictor</dt><dd class="text-slate-900 dark:text-white">{viewedPredictor.IsAutoPredictor ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Est. time remaining (min)</dt><dd class="text-slate-900 dark:text-white">{viewedPredictor.EstimatedTimeRemainingInMinutes ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedPredictor.CreationTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Modified</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedPredictor.LastModificationTime)}</dd></div>
			</dl>
		{:else if detailKind === 'forecast' && viewedForecast}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedForecast.ForecastName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedForecast.ForecastArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Predictor ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedForecast.PredictorArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedForecast.Status)}">{viewedForecast.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Message</dt><dd class="text-slate-900 dark:text-white">{viewedForecast.Message ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Est. time remaining (min)</dt><dd class="text-slate-900 dark:text-white">{viewedForecast.EstimatedTimeRemainingInMinutes ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedForecast.CreationTime)}</dd></div>
			</dl>
		{:else if detailKind === 'monitor' && viewedMonitor}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedMonitor.MonitorName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedMonitor.MonitorArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Resource ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedMonitor.ResourceArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedMonitor.Status)}">{viewedMonitor.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Last evaluation state</dt><dd class="text-slate-900 dark:text-white">{viewedMonitor.LastEvaluationState ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedMonitor.CreationTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if detailKind === 'group' || detailKind === 'predictor' || detailKind === 'forecast' || detailKind === 'monitor'}
			<button type="button" onclick={refreshDetail} class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><RefreshCw class="w-4 h-4" /> Refresh status</button>
		{/if}
		{#if detailKind === 'group' && viewedGroup}
			<button type="button" onclick={openEditGroup} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700">Edit datasets</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={editGroupModal} title="Edit Dataset Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="fc-edit-group-datasets" class="text-sm text-slate-600 dark:text-slate-300">Dataset ARNs (comma-separated, replaces the full set)</label>
				<input id="fc-edit-group-datasets" bind:value={editGroupDatasetArns} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editGroupError}<p class="text-sm text-red-600 dark:text-red-400">{editGroupError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editGroupModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditGroup} disabled={editingGroup} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{editingGroup ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
