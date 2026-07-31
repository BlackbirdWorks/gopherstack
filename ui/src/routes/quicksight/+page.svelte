<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getQuickSightClient } from '$lib/aws-client';
	import {
		ListDashboardsCommand,
		CreateDashboardCommand,
		UpdateDashboardCommand,
		DeleteDashboardCommand,
		DescribeDashboardCommand,
		ListAnalysesCommand,
		CreateAnalysisCommand,
		UpdateAnalysisCommand,
		DeleteAnalysisCommand,
		DescribeAnalysisCommand,
		ListDataSetsCommand,
		CreateDataSetCommand,
		UpdateDataSetCommand,
		DeleteDataSetCommand,
		DescribeDataSetCommand,
		ListDataSourcesCommand,
		CreateDataSourceCommand,
		UpdateDataSourceCommand,
		DeleteDataSourceCommand,
		DescribeDataSourceCommand,
		ListFoldersCommand,
		CreateFolderCommand,
		UpdateFolderCommand,
		DeleteFolderCommand,
		DescribeFolderCommand,
		ListVPCConnectionsCommand,
		CreateVPCConnectionCommand,
		UpdateVPCConnectionCommand,
		DeleteVPCConnectionCommand,
		DescribeVPCConnectionCommand,
		type DashboardSummary,
		type Dashboard,
		type DashboardVersionDefinition,
		type AnalysisSummary,
		type Analysis,
		type AnalysisDefinition,
		type DataSetSummary,
		type DataSet,
		type DataSetImportMode,
		type DataSource,
		type DataSourceType,
		type FolderSummary,
		type Folder,
		type FolderType,
		type SharingModel,
		type VPCConnectionSummary,
		type VPCConnection
	} from '@aws-sdk/client-quicksight';
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
	import { BarChart3, Plus, Trash2, Eye, Pencil } from 'lucide-svelte';

	const client = regionalClient(getQuickSightClient);

	type TabId = 'dashboards' | 'analyses' | 'datasets' | 'datasources' | 'folders' | 'vpcConnections';

	const tabs: TabDef[] = [
		{ id: 'dashboards', label: 'Dashboards' },
		{ id: 'analyses', label: 'Analyses' },
		{ id: 'datasets', label: 'Data Sets' },
		{ id: 'datasources', label: 'Data Sources' },
		{ id: 'folders', label: 'Folders' },
		{ id: 'vpcConnections', label: 'VPC Connections' }
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

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	let activeTab = $state<TabId>('dashboards');
	let searchQuery = $state('');

	// Every QuickSight call requires an AwsAccountId. Rather than hardcode a
	// placeholder, this is resolved once from the server's real effective
	// account id (GET /dashboard/api/system/settings, the same endpoint the
	// Settings page reads -- see settings/+page.svelte) so list/create/update
	// calls address the account this gopherstack instance actually runs as.
	// '000000000000' is kept only as the fallback for an older server build
	// that doesn't expose the endpoint yet, or when it's unreachable.
	let awsAccountId = $state('000000000000');
	let accountIdLoaded = false;

	async function ensureAccountId(): Promise<void> {
		if (accountIdLoaded) return;
		accountIdLoaded = true;
		try {
			const res = await fetch('/dashboard/api/system/settings');
			if (!res.ok) return;
			const data = (await res.json()) as { accountID?: unknown };
			if (typeof data.accountID === 'string' && data.accountID) {
				awsAccountId = data.accountID;
			}
		} catch {
			// Endpoint missing (older server build) or unreachable -- keep the
			// '000000000000' placeholder default.
		}
	}

	let dashboards = $state<DashboardSummary[]>([]);
	let dashboardsNextToken = $state<string | undefined>();
	let loadingMoreDashboards = $state(false);

	let analyses = $state<AnalysisSummary[]>([]);
	let analysesNextToken = $state<string | undefined>();
	let loadingMoreAnalyses = $state(false);

	let dataSets = $state<DataSetSummary[]>([]);
	let dataSetsNextToken = $state<string | undefined>();
	let loadingMoreDataSets = $state(false);

	let dataSources = $state<DataSource[]>([]);
	let dataSourcesNextToken = $state<string | undefined>();
	let loadingMoreDataSources = $state(false);

	let folders = $state<FolderSummary[]>([]);
	let foldersNextToken = $state<string | undefined>();
	let loadingMoreFolders = $state(false);

	let vpcConnections = $state<VPCConnectionSummary[]>([]);
	let vpcConnectionsNextToken = $state<string | undefined>();
	let loadingMoreVpcConnections = $state(false);

	async function fetchDashboards(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListDashboardsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : dashboardsNextToken
			})
		);
		dashboards = reset
			? (resp.DashboardSummaryList ?? [])
			: [...dashboards, ...(resp.DashboardSummaryList ?? [])];
		dashboardsNextToken = resp.NextToken;
	}

	async function fetchAnalyses(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAnalysesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : analysesNextToken
			})
		);
		analyses = reset
			? (resp.AnalysisSummaryList ?? [])
			: [...analyses, ...(resp.AnalysisSummaryList ?? [])];
		analysesNextToken = resp.NextToken;
	}

	async function fetchDataSets(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListDataSetsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : dataSetsNextToken
			})
		);
		dataSets = reset ? (resp.DataSetSummaries ?? []) : [...dataSets, ...(resp.DataSetSummaries ?? [])];
		dataSetsNextToken = resp.NextToken;
	}

	async function fetchDataSources(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListDataSourcesCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : dataSourcesNextToken
			})
		);
		dataSources = reset
			? (resp.DataSources ?? [])
			: [...dataSources, ...(resp.DataSources ?? [])];
		dataSourcesNextToken = resp.NextToken;
	}

	async function fetchFolders(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListFoldersCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : foldersNextToken
			})
		);
		folders = reset ? (resp.FolderSummaryList ?? []) : [...folders, ...(resp.FolderSummaryList ?? [])];
		foldersNextToken = resp.NextToken;
	}

	async function fetchVpcConnections(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListVPCConnectionsCommand({
				AwsAccountId: awsAccountId,
				NextToken: reset ? undefined : vpcConnectionsNextToken
			})
		);
		vpcConnections = reset
			? (resp.VPCConnectionSummaries ?? [])
			: [...vpcConnections, ...(resp.VPCConnectionSummaries ?? [])];
		vpcConnectionsNextToken = resp.NextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		dashboards: () => fetchDashboards(true).catch(rethrowDescribed),
		analyses: () => fetchAnalyses(true).catch(rethrowDescribed),
		datasets: () => fetchDataSets(true).catch(rethrowDescribed),
		datasources: () => fetchDataSources(true).catch(rethrowDescribed),
		folders: () => fetchFolders(true).catch(rethrowDescribed),
		vpcConnections: () => fetchVpcConnections(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Every one of the six tabs holds data scoped by AwsAccountId + region.
	// `onRegionChange`'s effect always fires once immediately on mount, in
	// addition to on every later region switch (see region-effect.svelte.ts).
	// This is used for two different jobs depending on which of those it is:
	//
	//  - On mount: resolve the real account id from
	//    /dashboard/api/system/settings (see ensureAccountId) before the
	//    first list call, then load only the active tab -- the normal lazy,
	//    load-on-demand behavior every other tab-loader page has.
	//  - On a REAL region change: refresh every tab, not just the active
	//    one. Unlike accessanalyzer/detective's parent-scoped child tabs,
	//    these six tabs are independent of each other, so there is no
	//    cascading "reload the parent then the active child" to lean on --
	//    but tab-loader's `loaded` flag is still per-tab, and would
	//    otherwise let switching to an already-visited tab after a region
	//    change silently keep serving the previous region's cached rows.
	//    This only runs on an actual region switch (a deliberate, rare user
	//    action), not on mount, so it doesn't turn every page load into six
	//    requests.
	let regionChangeCount = 0;
	onRegionChange(() => {
		const isInitialMount = regionChangeCount === 0;
		regionChangeCount++;
		void ensureAccountId().then(() => {
			if (isInitialMount) {
				void tabLoader.refresh(activeTab);
				return;
			}
			for (const t of tabs) {
				void tabLoader.refresh(t.id as TabId);
			}
		});
	});

	const filteredDashboards = $derived(
		dashboards.filter((d) => {
			const q = searchQuery.toLowerCase();
			return (
				(d.Name ?? '').toLowerCase().includes(q) || (d.DashboardId ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredAnalyses = $derived(
		analyses.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.Name ?? '').toLowerCase().includes(q) ||
				(a.AnalysisId ?? '').toLowerCase().includes(q) ||
				(a.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredDataSets = $derived(
		dataSets.filter((ds) => {
			const q = searchQuery.toLowerCase();
			return (
				(ds.Name ?? '').toLowerCase().includes(q) ||
				(ds.DataSetId ?? '').toLowerCase().includes(q) ||
				(ds.ImportMode ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredDataSources = $derived(
		dataSources.filter((ds) => {
			const q = searchQuery.toLowerCase();
			return (
				(ds.Name ?? '').toLowerCase().includes(q) ||
				(ds.DataSourceId ?? '').toLowerCase().includes(q) ||
				(ds.Type ?? '').toLowerCase().includes(q) ||
				(ds.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredFolders = $derived(
		folders.filter((f) => {
			const q = searchQuery.toLowerCase();
			return (
				(f.Name ?? '').toLowerCase().includes(q) ||
				(f.FolderId ?? '').toLowerCase().includes(q) ||
				(f.FolderType ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredVpcConnections = $derived(
		vpcConnections.filter((v) => {
			const q = searchQuery.toLowerCase();
			return (
				(v.Name ?? '').toLowerCase().includes(q) ||
				(v.VPCConnectionId ?? '').toLowerCase().includes(q) ||
				(v.VPCId ?? '').toLowerCase().includes(q) ||
				(v.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreDashboards(): Promise<void> {
		loadingMoreDashboards = true;
		try {
			await fetchDashboards(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDashboards = false;
		}
	}

	async function loadMoreAnalyses(): Promise<void> {
		loadingMoreAnalyses = true;
		try {
			await fetchAnalyses(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAnalyses = false;
		}
	}

	async function loadMoreDataSets(): Promise<void> {
		loadingMoreDataSets = true;
		try {
			await fetchDataSets(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDataSets = false;
		}
	}

	async function loadMoreDataSources(): Promise<void> {
		loadingMoreDataSources = true;
		try {
			await fetchDataSources(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDataSources = false;
		}
	}

	async function loadMoreFolders(): Promise<void> {
		loadingMoreFolders = true;
		try {
			await fetchFolders(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreFolders = false;
		}
	}

	async function loadMoreVpcConnections(): Promise<void> {
		loadingMoreVpcConnections = true;
		try {
			await fetchVpcConnections(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreVpcConnections = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// ==================== Dashboards: create / update / delete / detail ====================

	let createDashboardModal = $state<Modal | null>(null);
	let creatingDashboard = $state(false);
	let createDashboardError = $state<string | null>(null);
	let newDashboardId = $state('');
	let newDashboardName = $state('');
	let newDashboardDefinition = $state('');

	function openCreateDashboardModal(): void {
		createDashboardError = null;
		newDashboardId = '';
		newDashboardName = '';
		newDashboardDefinition = '';
		createDashboardModal?.open();
	}

	async function submitCreateDashboard(): Promise<void> {
		if (!newDashboardId || !newDashboardName) {
			createDashboardError = 'Dashboard ID and name are required.';
			return;
		}
		let definition: DashboardVersionDefinition | undefined;
		if (newDashboardDefinition.trim()) {
			try {
				definition = JSON.parse(newDashboardDefinition) as DashboardVersionDefinition;
			} catch {
				createDashboardError = 'Definition must be valid JSON.';
				return;
			}
		}
		creatingDashboard = true;
		createDashboardError = null;
		try {
			await client().send(
				new CreateDashboardCommand({
					AwsAccountId: awsAccountId,
					DashboardId: newDashboardId,
					Name: newDashboardName,
					Definition: definition
				})
			);
			toast.success('Dashboard created');
			createDashboardModal?.close();
			await tabLoader.refresh('dashboards');
		} catch (e) {
			const msg = describeError(e);
			createDashboardError = msg;
			toast.error(msg);
		} finally {
			creatingDashboard = false;
		}
	}

	let editDashboardModal = $state<Modal | null>(null);
	let editingDashboard = $state<DashboardSummary | null>(null);
	let editingDashboardName = $state('');
	let editingDashboardDefinition = $state('');
	let savingDashboard = $state(false);
	let editDashboardError = $state<string | null>(null);

	function openEditDashboardModal(d: DashboardSummary): void {
		editingDashboard = d;
		editingDashboardName = d.Name ?? '';
		editingDashboardDefinition = '';
		editDashboardError = null;
		editDashboardModal?.open();
	}

	async function submitEditDashboard(): Promise<void> {
		if (!editingDashboard?.DashboardId) return;
		if (!editingDashboardName) {
			editDashboardError = 'Name is required.';
			return;
		}
		let definition: DashboardVersionDefinition | undefined;
		if (editingDashboardDefinition.trim()) {
			try {
				definition = JSON.parse(editingDashboardDefinition) as DashboardVersionDefinition;
			} catch {
				editDashboardError = 'Definition must be valid JSON.';
				return;
			}
		}
		savingDashboard = true;
		editDashboardError = null;
		try {
			await client().send(
				new UpdateDashboardCommand({
					AwsAccountId: awsAccountId,
					DashboardId: editingDashboard.DashboardId,
					Name: editingDashboardName,
					Definition: definition
				})
			);
			toast.success('Dashboard updated');
			editDashboardModal?.close();
			await tabLoader.refresh('dashboards');
		} catch (e) {
			const msg = describeError(e);
			editDashboardError = msg;
			toast.error(msg);
		} finally {
			savingDashboard = false;
		}
	}

	async function handleDeleteDashboard(d: DashboardSummary): Promise<void> {
		if (!d.DashboardId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete dashboard',
			message: `Delete dashboard ${d.Name ?? d.DashboardId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDashboardCommand({ AwsAccountId: awsAccountId, DashboardId: d.DashboardId })
			);
			toast.success('Dashboard deleted');
			await tabLoader.refresh('dashboards');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let dashboardDetailModal = $state<Modal | null>(null);
	let viewedDashboard = $state<DashboardSummary | Dashboard | null>(null);
	let dashboardDetailLoading = $state(false);
	let dashboardDetailError = $state<string | null>(null);

	async function openDashboardDetail(d: DashboardSummary): Promise<void> {
		viewedDashboard = d;
		dashboardDetailError = null;
		dashboardDetailModal?.open();
		if (!d.DashboardId) return;
		dashboardDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeDashboardCommand({ AwsAccountId: awsAccountId, DashboardId: d.DashboardId })
			);
			viewedDashboard = resp.Dashboard ?? d;
		} catch (e) {
			dashboardDetailError = describeError(e);
		} finally {
			dashboardDetailLoading = false;
		}
	}

	// ==================== Analyses: create / update / delete / detail ====================

	let createAnalysisModal = $state<Modal | null>(null);
	let creatingAnalysis = $state(false);
	let createAnalysisError = $state<string | null>(null);
	let newAnalysisId = $state('');
	let newAnalysisName = $state('');
	let newAnalysisDefinition = $state('');

	function openCreateAnalysisModal(): void {
		createAnalysisError = null;
		newAnalysisId = '';
		newAnalysisName = '';
		newAnalysisDefinition = '';
		createAnalysisModal?.open();
	}

	async function submitCreateAnalysis(): Promise<void> {
		if (!newAnalysisId || !newAnalysisName) {
			createAnalysisError = 'Analysis ID and name are required.';
			return;
		}
		let definition: AnalysisDefinition | undefined;
		if (newAnalysisDefinition.trim()) {
			try {
				definition = JSON.parse(newAnalysisDefinition) as AnalysisDefinition;
			} catch {
				createAnalysisError = 'Definition must be valid JSON.';
				return;
			}
		}
		creatingAnalysis = true;
		createAnalysisError = null;
		try {
			await client().send(
				new CreateAnalysisCommand({
					AwsAccountId: awsAccountId,
					AnalysisId: newAnalysisId,
					Name: newAnalysisName,
					Definition: definition
				})
			);
			toast.success('Analysis created');
			createAnalysisModal?.close();
			await tabLoader.refresh('analyses');
		} catch (e) {
			const msg = describeError(e);
			createAnalysisError = msg;
			toast.error(msg);
		} finally {
			creatingAnalysis = false;
		}
	}

	let editAnalysisModal = $state<Modal | null>(null);
	let editingAnalysis = $state<AnalysisSummary | null>(null);
	let editingAnalysisName = $state('');
	let editingAnalysisDefinition = $state('');
	let savingAnalysis = $state(false);
	let editAnalysisError = $state<string | null>(null);

	function openEditAnalysisModal(a: AnalysisSummary): void {
		editingAnalysis = a;
		editingAnalysisName = a.Name ?? '';
		editingAnalysisDefinition = '';
		editAnalysisError = null;
		editAnalysisModal?.open();
	}

	async function submitEditAnalysis(): Promise<void> {
		if (!editingAnalysis?.AnalysisId) return;
		if (!editingAnalysisName) {
			editAnalysisError = 'Name is required.';
			return;
		}
		let definition: AnalysisDefinition | undefined;
		if (editingAnalysisDefinition.trim()) {
			try {
				definition = JSON.parse(editingAnalysisDefinition) as AnalysisDefinition;
			} catch {
				editAnalysisError = 'Definition must be valid JSON.';
				return;
			}
		}
		savingAnalysis = true;
		editAnalysisError = null;
		try {
			await client().send(
				new UpdateAnalysisCommand({
					AwsAccountId: awsAccountId,
					AnalysisId: editingAnalysis.AnalysisId,
					Name: editingAnalysisName,
					Definition: definition
				})
			);
			toast.success('Analysis updated');
			editAnalysisModal?.close();
			await tabLoader.refresh('analyses');
		} catch (e) {
			const msg = describeError(e);
			editAnalysisError = msg;
			toast.error(msg);
		} finally {
			savingAnalysis = false;
		}
	}

	async function handleDeleteAnalysis(a: AnalysisSummary): Promise<void> {
		if (!a.AnalysisId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete analysis',
			message: `Delete analysis ${a.Name ?? a.AnalysisId}? This deletes it immediately, without the 30-day recovery window real AWS offers by default.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteAnalysisCommand({
					AwsAccountId: awsAccountId,
					AnalysisId: a.AnalysisId,
					ForceDeleteWithoutRecovery: true
				})
			);
			toast.success('Analysis deleted');
			await tabLoader.refresh('analyses');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let analysisDetailModal = $state<Modal | null>(null);
	let viewedAnalysis = $state<AnalysisSummary | Analysis | null>(null);
	let analysisDetailLoading = $state(false);
	let analysisDetailError = $state<string | null>(null);

	async function openAnalysisDetail(a: AnalysisSummary): Promise<void> {
		viewedAnalysis = a;
		analysisDetailError = null;
		analysisDetailModal?.open();
		if (!a.AnalysisId) return;
		analysisDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeAnalysisCommand({ AwsAccountId: awsAccountId, AnalysisId: a.AnalysisId })
			);
			viewedAnalysis = resp.Analysis ?? a;
		} catch (e) {
			analysisDetailError = describeError(e);
		} finally {
			analysisDetailLoading = false;
		}
	}

	// ==================== Data Sets: create / update / delete / detail ====================
	//
	// CreateDataSetCommand/UpdateDataSetCommand's PhysicalTableMap is required
	// by the SDK's request type (real AWS uses it to declare the dataset's
	// underlying physical tables), but this backend never reads or returns it
	// (dataSetToMap only echoes Arn/CreatedTime/DataSetId/ImportMode/
	// LastUpdatedTime/Name -- confirmed in services/quicksight/handler_dataset.go).
	// Rather than build a physical-table-declaration editor for a value this
	// backend silently discards, an empty map is sent to satisfy the type; see
	// PARITY.md's precedent of documenting fields a mock backend can't
	// meaningfully model.

	let createDataSetModal = $state<Modal | null>(null);
	let creatingDataSet = $state(false);
	let createDataSetError = $state<string | null>(null);
	let newDataSetId = $state('');
	let newDataSetName = $state('');
	let newDataSetImportMode = $state<DataSetImportMode>('SPICE');

	function openCreateDataSetModal(): void {
		createDataSetError = null;
		newDataSetId = '';
		newDataSetName = '';
		newDataSetImportMode = 'SPICE';
		createDataSetModal?.open();
	}

	async function submitCreateDataSet(): Promise<void> {
		if (!newDataSetId || !newDataSetName) {
			createDataSetError = 'Data set ID and name are required.';
			return;
		}
		creatingDataSet = true;
		createDataSetError = null;
		try {
			await client().send(
				new CreateDataSetCommand({
					AwsAccountId: awsAccountId,
					DataSetId: newDataSetId,
					Name: newDataSetName,
					ImportMode: newDataSetImportMode,
					PhysicalTableMap: {}
				})
			);
			toast.success('Data set created');
			createDataSetModal?.close();
			await tabLoader.refresh('datasets');
		} catch (e) {
			const msg = describeError(e);
			createDataSetError = msg;
			toast.error(msg);
		} finally {
			creatingDataSet = false;
		}
	}

	let editDataSetModal = $state<Modal | null>(null);
	let editingDataSet = $state<DataSetSummary | null>(null);
	let editingDataSetName = $state('');
	let editingDataSetImportMode = $state<DataSetImportMode>('SPICE');
	let savingDataSet = $state(false);
	let editDataSetError = $state<string | null>(null);

	function openEditDataSetModal(ds: DataSetSummary): void {
		editingDataSet = ds;
		editingDataSetName = ds.Name ?? '';
		editingDataSetImportMode = ds.ImportMode ?? 'SPICE';
		editDataSetError = null;
		editDataSetModal?.open();
	}

	async function submitEditDataSet(): Promise<void> {
		if (!editingDataSet?.DataSetId) return;
		if (!editingDataSetName) {
			editDataSetError = 'Name is required.';
			return;
		}
		savingDataSet = true;
		editDataSetError = null;
		try {
			await client().send(
				new UpdateDataSetCommand({
					AwsAccountId: awsAccountId,
					DataSetId: editingDataSet.DataSetId,
					Name: editingDataSetName,
					ImportMode: editingDataSetImportMode,
					PhysicalTableMap: {}
				})
			);
			toast.success('Data set updated');
			editDataSetModal?.close();
			await tabLoader.refresh('datasets');
		} catch (e) {
			const msg = describeError(e);
			editDataSetError = msg;
			toast.error(msg);
		} finally {
			savingDataSet = false;
		}
	}

	async function handleDeleteDataSet(ds: DataSetSummary): Promise<void> {
		if (!ds.DataSetId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete data set',
			message: `Delete data set ${ds.Name ?? ds.DataSetId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDataSetCommand({ AwsAccountId: awsAccountId, DataSetId: ds.DataSetId })
			);
			toast.success('Data set deleted');
			await tabLoader.refresh('datasets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let dataSetDetailModal = $state<Modal | null>(null);
	let viewedDataSet = $state<DataSetSummary | DataSet | null>(null);
	let dataSetDetailLoading = $state(false);
	let dataSetDetailError = $state<string | null>(null);

	async function openDataSetDetail(ds: DataSetSummary): Promise<void> {
		viewedDataSet = ds;
		dataSetDetailError = null;
		dataSetDetailModal?.open();
		if (!ds.DataSetId) return;
		dataSetDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeDataSetCommand({ AwsAccountId: awsAccountId, DataSetId: ds.DataSetId })
			);
			viewedDataSet = resp.DataSet ?? ds;
		} catch (e) {
			dataSetDetailError = describeError(e);
		} finally {
			dataSetDetailLoading = false;
		}
	}

	// ==================== Data Sources: create / update / delete / detail ====================

	const dataSourceTypes: DataSourceType[] = [
		'MYSQL',
		'POSTGRESQL',
		'REDSHIFT',
		'S3',
		'ATHENA',
		'SNOWFLAKE',
		'ORACLE',
		'SQLSERVER',
		'AURORA',
		'AURORA_POSTGRESQL',
		'MARIADB',
		'BIGQUERY',
		'SPARK',
		'TERADATA'
	];

	let createDataSourceModal = $state<Modal | null>(null);
	let creatingDataSource = $state(false);
	let createDataSourceError = $state<string | null>(null);
	let newDataSourceId = $state('');
	let newDataSourceName = $state('');
	let newDataSourceType = $state<DataSourceType>('MYSQL');

	function openCreateDataSourceModal(): void {
		createDataSourceError = null;
		newDataSourceId = '';
		newDataSourceName = '';
		newDataSourceType = 'MYSQL';
		createDataSourceModal?.open();
	}

	async function submitCreateDataSource(): Promise<void> {
		if (!newDataSourceId || !newDataSourceName) {
			createDataSourceError = 'Data source ID and name are required.';
			return;
		}
		creatingDataSource = true;
		createDataSourceError = null;
		try {
			await client().send(
				new CreateDataSourceCommand({
					AwsAccountId: awsAccountId,
					DataSourceId: newDataSourceId,
					Name: newDataSourceName,
					Type: newDataSourceType
				})
			);
			toast.success('Data source created');
			createDataSourceModal?.close();
			await tabLoader.refresh('datasources');
		} catch (e) {
			const msg = describeError(e);
			createDataSourceError = msg;
			toast.error(msg);
		} finally {
			creatingDataSource = false;
		}
	}

	let editDataSourceModal = $state<Modal | null>(null);
	let editingDataSource = $state<DataSource | null>(null);
	let editingDataSourceName = $state('');
	let savingDataSource = $state(false);
	let editDataSourceError = $state<string | null>(null);

	function openEditDataSourceModal(ds: DataSource): void {
		editingDataSource = ds;
		editingDataSourceName = ds.Name ?? '';
		editDataSourceError = null;
		editDataSourceModal?.open();
	}

	async function submitEditDataSource(): Promise<void> {
		if (!editingDataSource?.DataSourceId) return;
		if (!editingDataSourceName) {
			editDataSourceError = 'Name is required.';
			return;
		}
		savingDataSource = true;
		editDataSourceError = null;
		try {
			await client().send(
				new UpdateDataSourceCommand({
					AwsAccountId: awsAccountId,
					DataSourceId: editingDataSource.DataSourceId,
					Name: editingDataSourceName
				})
			);
			toast.success('Data source updated');
			editDataSourceModal?.close();
			await tabLoader.refresh('datasources');
		} catch (e) {
			const msg = describeError(e);
			editDataSourceError = msg;
			toast.error(msg);
		} finally {
			savingDataSource = false;
		}
	}

	async function handleDeleteDataSource(ds: DataSource): Promise<void> {
		if (!ds.DataSourceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete data source',
			message: `Delete data source ${ds.Name ?? ds.DataSourceId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDataSourceCommand({ AwsAccountId: awsAccountId, DataSourceId: ds.DataSourceId })
			);
			toast.success('Data source deleted');
			await tabLoader.refresh('datasources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let dataSourceDetailModal = $state<Modal | null>(null);
	let viewedDataSource = $state<DataSource | null>(null);
	let dataSourceDetailLoading = $state(false);
	let dataSourceDetailError = $state<string | null>(null);

	async function openDataSourceDetail(ds: DataSource): Promise<void> {
		viewedDataSource = ds;
		dataSourceDetailError = null;
		dataSourceDetailModal?.open();
		if (!ds.DataSourceId) return;
		dataSourceDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeDataSourceCommand({ AwsAccountId: awsAccountId, DataSourceId: ds.DataSourceId })
			);
			viewedDataSource = resp.DataSource ?? ds;
		} catch (e) {
			dataSourceDetailError = describeError(e);
		} finally {
			dataSourceDetailLoading = false;
		}
	}

	// ==================== Folders: create / update / delete / detail ====================

	let createFolderModal = $state<Modal | null>(null);
	let creatingFolder = $state(false);
	let createFolderError = $state<string | null>(null);
	let newFolderId = $state('');
	let newFolderName = $state('');
	let newFolderType = $state<FolderType>('SHARED');
	let newFolderParentArn = $state('');
	let newFolderSharingModel = $state<SharingModel | ''>('');

	function openCreateFolderModal(): void {
		createFolderError = null;
		newFolderId = '';
		newFolderName = '';
		newFolderType = 'SHARED';
		newFolderParentArn = '';
		newFolderSharingModel = '';
		createFolderModal?.open();
	}

	async function submitCreateFolder(): Promise<void> {
		if (!newFolderId || !newFolderName) {
			createFolderError = 'Folder ID and name are required.';
			return;
		}
		creatingFolder = true;
		createFolderError = null;
		try {
			await client().send(
				new CreateFolderCommand({
					AwsAccountId: awsAccountId,
					FolderId: newFolderId,
					Name: newFolderName,
					FolderType: newFolderType,
					ParentFolderArn: newFolderParentArn || undefined,
					SharingModel: newFolderSharingModel || undefined
				})
			);
			toast.success('Folder created');
			createFolderModal?.close();
			await tabLoader.refresh('folders');
		} catch (e) {
			const msg = describeError(e);
			createFolderError = msg;
			toast.error(msg);
		} finally {
			creatingFolder = false;
		}
	}

	let editFolderModal = $state<Modal | null>(null);
	let editingFolder = $state<FolderSummary | null>(null);
	let editingFolderName = $state('');
	let savingFolder = $state(false);
	let editFolderError = $state<string | null>(null);

	function openEditFolderModal(f: FolderSummary): void {
		editingFolder = f;
		editingFolderName = f.Name ?? '';
		editFolderError = null;
		editFolderModal?.open();
	}

	async function submitEditFolder(): Promise<void> {
		if (!editingFolder?.FolderId) return;
		if (!editingFolderName) {
			editFolderError = 'Name is required.';
			return;
		}
		savingFolder = true;
		editFolderError = null;
		try {
			await client().send(
				new UpdateFolderCommand({
					AwsAccountId: awsAccountId,
					FolderId: editingFolder.FolderId,
					Name: editingFolderName
				})
			);
			toast.success('Folder updated');
			editFolderModal?.close();
			await tabLoader.refresh('folders');
		} catch (e) {
			const msg = describeError(e);
			editFolderError = msg;
			toast.error(msg);
		} finally {
			savingFolder = false;
		}
	}

	async function handleDeleteFolder(f: FolderSummary): Promise<void> {
		if (!f.FolderId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete folder',
			message: `Delete folder ${f.Name ?? f.FolderId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteFolderCommand({ AwsAccountId: awsAccountId, FolderId: f.FolderId }));
			toast.success('Folder deleted');
			await tabLoader.refresh('folders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let folderDetailModal = $state<Modal | null>(null);
	let viewedFolder = $state<FolderSummary | Folder | null>(null);
	let folderDetailLoading = $state(false);
	let folderDetailError = $state<string | null>(null);

	async function openFolderDetail(f: FolderSummary): Promise<void> {
		viewedFolder = f;
		folderDetailError = null;
		folderDetailModal?.open();
		if (!f.FolderId) return;
		folderDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeFolderCommand({ AwsAccountId: awsAccountId, FolderId: f.FolderId })
			);
			viewedFolder = resp.Folder ?? f;
		} catch (e) {
			folderDetailError = describeError(e);
		} finally {
			folderDetailLoading = false;
		}
	}

	// ==================== VPC Connections: create / update / delete / detail ====================

	let createVpcConnectionModal = $state<Modal | null>(null);
	let creatingVpcConnection = $state(false);
	let createVpcConnectionError = $state<string | null>(null);
	let newVpcConnectionId = $state('');
	let newVpcConnectionName = $state('');
	let newVpcConnectionSubnetIds = $state('');
	let newVpcConnectionSecurityGroupIds = $state('');
	let newVpcConnectionDnsResolvers = $state('');
	let newVpcConnectionRoleArn = $state('');

	function openCreateVpcConnectionModal(): void {
		createVpcConnectionError = null;
		newVpcConnectionId = '';
		newVpcConnectionName = '';
		newVpcConnectionSubnetIds = '';
		newVpcConnectionSecurityGroupIds = '';
		newVpcConnectionDnsResolvers = '';
		newVpcConnectionRoleArn = '';
		createVpcConnectionModal?.open();
	}

	async function submitCreateVpcConnection(): Promise<void> {
		if (
			!newVpcConnectionId ||
			!newVpcConnectionName ||
			!newVpcConnectionSubnetIds ||
			!newVpcConnectionSecurityGroupIds ||
			!newVpcConnectionRoleArn
		) {
			createVpcConnectionError =
				'VPC connection ID, name, subnet IDs, security group IDs, and role ARN are required.';
			return;
		}
		creatingVpcConnection = true;
		createVpcConnectionError = null;
		try {
			await client().send(
				new CreateVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: newVpcConnectionId,
					Name: newVpcConnectionName,
					SubnetIds: parseCommaList(newVpcConnectionSubnetIds),
					SecurityGroupIds: parseCommaList(newVpcConnectionSecurityGroupIds),
					DnsResolvers: newVpcConnectionDnsResolvers
						? parseCommaList(newVpcConnectionDnsResolvers)
						: undefined,
					RoleArn: newVpcConnectionRoleArn
				})
			);
			toast.success('VPC connection created');
			createVpcConnectionModal?.close();
			await tabLoader.refresh('vpcConnections');
		} catch (e) {
			const msg = describeError(e);
			createVpcConnectionError = msg;
			toast.error(msg);
		} finally {
			creatingVpcConnection = false;
		}
	}

	let editVpcConnectionModal = $state<Modal | null>(null);
	let editingVpcConnection = $state<VPCConnectionSummary | null>(null);
	let editingVpcConnectionName = $state('');
	let editingVpcConnectionSubnetIds = $state('');
	let editingVpcConnectionSecurityGroupIds = $state('');
	let editingVpcConnectionDnsResolvers = $state('');
	let editingVpcConnectionRoleArn = $state('');
	let savingVpcConnection = $state(false);
	let editVpcConnectionError = $state<string | null>(null);

	function openEditVpcConnectionModal(v: VPCConnectionSummary): void {
		editingVpcConnection = v;
		editingVpcConnectionName = v.Name ?? '';
		// Real AWS's VPCConnectionSummary/VPCConnection response shapes carry no
		// SubnetIds field at all (confirmed against both aws-sdk-go-v2's and
		// this installed @aws-sdk/client-quicksight's types: SubnetIds is
		// accepted by Create/UpdateVPCConnectionRequest but never echoed back by
		// Describe/List -- only indirectly inferable, once AWS actually
		// provisions ENIs, via NetworkInterfaces[].SubnetId). So unlike every
		// other editable field here, there is no value to prefill this from;
		// UpdateVPCConnectionCommand still requires it, so the field is left
		// blank for the caller to (re)supply.
		editingVpcConnectionSubnetIds = '';
		editingVpcConnectionSecurityGroupIds = (v.SecurityGroupIds ?? []).join(', ');
		editingVpcConnectionDnsResolvers = (v.DnsResolvers ?? []).join(', ');
		editingVpcConnectionRoleArn = v.RoleArn ?? '';
		editVpcConnectionError = null;
		editVpcConnectionModal?.open();
	}

	async function submitEditVpcConnection(): Promise<void> {
		if (!editingVpcConnection?.VPCConnectionId) return;
		if (
			!editingVpcConnectionName ||
			!editingVpcConnectionSubnetIds ||
			!editingVpcConnectionSecurityGroupIds ||
			!editingVpcConnectionRoleArn
		) {
			editVpcConnectionError = 'Name, subnet IDs, security group IDs, and role ARN are required.';
			return;
		}
		savingVpcConnection = true;
		editVpcConnectionError = null;
		try {
			await client().send(
				new UpdateVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: editingVpcConnection.VPCConnectionId,
					Name: editingVpcConnectionName,
					SubnetIds: parseCommaList(editingVpcConnectionSubnetIds),
					SecurityGroupIds: parseCommaList(editingVpcConnectionSecurityGroupIds),
					DnsResolvers: editingVpcConnectionDnsResolvers
						? parseCommaList(editingVpcConnectionDnsResolvers)
						: undefined,
					RoleArn: editingVpcConnectionRoleArn
				})
			);
			toast.success('VPC connection updated');
			editVpcConnectionModal?.close();
			await tabLoader.refresh('vpcConnections');
		} catch (e) {
			const msg = describeError(e);
			editVpcConnectionError = msg;
			toast.error(msg);
		} finally {
			savingVpcConnection = false;
		}
	}

	async function handleDeleteVpcConnection(v: VPCConnectionSummary): Promise<void> {
		if (!v.VPCConnectionId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete VPC connection',
			message: `Delete VPC connection ${v.Name ?? v.VPCConnectionId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: v.VPCConnectionId
				})
			);
			toast.success('VPC connection deleted');
			await tabLoader.refresh('vpcConnections');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let vpcConnectionDetailModal = $state<Modal | null>(null);
	let viewedVpcConnection = $state<VPCConnectionSummary | VPCConnection | null>(null);
	let vpcConnectionDetailLoading = $state(false);
	let vpcConnectionDetailError = $state<string | null>(null);

	async function openVpcConnectionDetail(v: VPCConnectionSummary): Promise<void> {
		viewedVpcConnection = v;
		vpcConnectionDetailError = null;
		vpcConnectionDetailModal?.open();
		if (!v.VPCConnectionId) return;
		vpcConnectionDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVPCConnectionCommand({
					AwsAccountId: awsAccountId,
					VPCConnectionId: v.VPCConnectionId
				})
			);
			viewedVpcConnection = resp.VPCConnection ?? v;
		} catch (e) {
			vpcConnectionDetailError = describeError(e);
		} finally {
			vpcConnectionDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={BarChart3}
		title="Amazon QuickSight"
		description="Business intelligence dashboards"
		onRefresh={handleRefresh}
	>
		{#snippet actions()}
			{#if activeTab === 'dashboards'}
				<button
					onclick={openCreateDashboardModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create dashboard
				</button>
			{:else if activeTab === 'analyses'}
				<button
					onclick={openCreateAnalysisModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create analysis
				</button>
			{:else if activeTab === 'datasets'}
				<button
					onclick={openCreateDataSetModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create data set
				</button>
			{:else if activeTab === 'datasources'}
				<button
					onclick={openCreateDataSourceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create data source
				</button>
			{:else if activeTab === 'folders'}
				<button
					onclick={openCreateFolderModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create folder
				</button>
			{:else if activeTab === 'vpcConnections'}
				<button
					onclick={openCreateVpcConnectionModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create VPC connection
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'dashboards'}
				{#snippet dashboardVersionCell(d: DashboardSummary)}
					{d.PublishedVersionNumber ?? '—'}
				{/snippet}
				{#snippet dashboardUpdatedCell(d: DashboardSummary)}
					{formatDate(d.LastUpdatedTime)}
				{/snippet}
				{#snippet dashboardActionsCell(d: DashboardSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openDashboardDetail(d)}
							title="View"
							aria-label="View dashboard {d.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditDashboardModal(d)}
							title="Edit"
							aria-label="Edit dashboard {d.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteDashboard(d)}
							title="Delete"
							aria-label="Delete dashboard {d.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const dashboardColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'DashboardId', label: 'ID' },
					{ key: 'PublishedVersionNumber', label: 'Version', render: dashboardVersionCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: dashboardUpdatedCell },
					{ key: 'actions', label: '', render: dashboardActionsCell }
				] as Column<DashboardSummary>[]}
				<DataTable
					rows={filteredDashboards}
					rowKey={(d) => d.DashboardId ?? ''}
					columns={dashboardColumns}
					loading={tabLoader.isLoading('dashboards')}
					emptyMessage="No dashboards found"
				/>
				<LoadMore
					hasMore={!!dashboardsNextToken}
					loading={loadingMoreDashboards}
					onLoadMore={loadMoreDashboards}
				/>
			{:else if activeTab === 'analyses'}
				{#snippet analysisStatusCell(a: AnalysisSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.Status === 'CREATION_SUCCESSFUL')}"
						>{a.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet analysisUpdatedCell(a: AnalysisSummary)}
					{formatDate(a.LastUpdatedTime)}
				{/snippet}
				{#snippet analysisActionsCell(a: AnalysisSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openAnalysisDetail(a)}
							title="View"
							aria-label="View analysis {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditAnalysisModal(a)}
							title="Edit"
							aria-label="Edit analysis {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteAnalysis(a)}
							title="Delete"
							aria-label="Delete analysis {a.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const analysisColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'AnalysisId', label: 'ID' },
					{ key: 'Status', label: 'Status', render: analysisStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: analysisUpdatedCell },
					{ key: 'actions', label: '', render: analysisActionsCell }
				] as Column<AnalysisSummary>[]}
				<DataTable
					rows={filteredAnalyses}
					rowKey={(a) => a.AnalysisId ?? ''}
					columns={analysisColumns}
					loading={tabLoader.isLoading('analyses')}
					emptyMessage="No analyses found"
				/>
				<LoadMore
					hasMore={!!analysesNextToken}
					loading={loadingMoreAnalyses}
					onLoadMore={loadMoreAnalyses}
				/>
			{:else if activeTab === 'datasets'}
				{#snippet dataSetUpdatedCell(ds: DataSetSummary)}
					{formatDate(ds.LastUpdatedTime)}
				{/snippet}
				{#snippet dataSetActionsCell(ds: DataSetSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openDataSetDetail(ds)}
							title="View"
							aria-label="View data set {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditDataSetModal(ds)}
							title="Edit"
							aria-label="Edit data set {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteDataSet(ds)}
							title="Delete"
							aria-label="Delete data set {ds.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const dataSetColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'DataSetId', label: 'ID' },
					{ key: 'ImportMode', label: 'Import Mode' },
					{ key: 'LastUpdatedTime', label: 'Updated', render: dataSetUpdatedCell },
					{ key: 'actions', label: '', render: dataSetActionsCell }
				] as Column<DataSetSummary>[]}
				<DataTable
					rows={filteredDataSets}
					rowKey={(ds) => ds.DataSetId ?? ''}
					columns={dataSetColumns}
					loading={tabLoader.isLoading('datasets')}
					emptyMessage="No data sets found"
				/>
				<LoadMore
					hasMore={!!dataSetsNextToken}
					loading={loadingMoreDataSets}
					onLoadMore={loadMoreDataSets}
				/>
			{:else if activeTab === 'datasources'}
				{#snippet dataSourceStatusCell(ds: DataSource)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(ds.Status === 'CREATION_SUCCESSFUL')}"
						>{ds.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet dataSourceUpdatedCell(ds: DataSource)}
					{formatDate(ds.LastUpdatedTime)}
				{/snippet}
				{#snippet dataSourceActionsCell(ds: DataSource)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openDataSourceDetail(ds)}
							title="View"
							aria-label="View data source {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditDataSourceModal(ds)}
							title="Edit"
							aria-label="Edit data source {ds.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteDataSource(ds)}
							title="Delete"
							aria-label="Delete data source {ds.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const dataSourceColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'DataSourceId', label: 'ID' },
					{ key: 'Type', label: 'Type' },
					{ key: 'Status', label: 'Status', render: dataSourceStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: dataSourceUpdatedCell },
					{ key: 'actions', label: '', render: dataSourceActionsCell }
				] as Column<DataSource>[]}
				<DataTable
					rows={filteredDataSources}
					rowKey={(ds) => ds.DataSourceId ?? ''}
					columns={dataSourceColumns}
					loading={tabLoader.isLoading('datasources')}
					emptyMessage="No data sources found"
				/>
				<LoadMore
					hasMore={!!dataSourcesNextToken}
					loading={loadingMoreDataSources}
					onLoadMore={loadMoreDataSources}
				/>
			{:else if activeTab === 'folders'}
				{#snippet folderUpdatedCell(f: FolderSummary)}
					{formatDate(f.LastUpdatedTime)}
				{/snippet}
				{#snippet folderActionsCell(f: FolderSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openFolderDetail(f)}
							title="View"
							aria-label="View folder {f.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditFolderModal(f)}
							title="Edit"
							aria-label="Edit folder {f.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteFolder(f)}
							title="Delete"
							aria-label="Delete folder {f.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const folderColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'FolderId', label: 'ID' },
					{ key: 'FolderType', label: 'Type' },
					{ key: 'LastUpdatedTime', label: 'Updated', render: folderUpdatedCell },
					{ key: 'actions', label: '', render: folderActionsCell }
				] as Column<FolderSummary>[]}
				<DataTable
					rows={filteredFolders}
					rowKey={(f) => f.FolderId ?? ''}
					columns={folderColumns}
					loading={tabLoader.isLoading('folders')}
					emptyMessage="No folders found"
				/>
				<LoadMore
					hasMore={!!foldersNextToken}
					loading={loadingMoreFolders}
					onLoadMore={loadMoreFolders}
				/>
			{:else if activeTab === 'vpcConnections'}
				{#snippet vpcStatusCell(v: VPCConnectionSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(v.Status === 'CREATION_SUCCESSFUL')}"
						>{v.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet vpcUpdatedCell(v: VPCConnectionSummary)}
					{formatDate(v.LastUpdatedTime)}
				{/snippet}
				{#snippet vpcActionsCell(v: VPCConnectionSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openVpcConnectionDetail(v)}
							title="View"
							aria-label="View VPC connection {v.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditVpcConnectionModal(v)}
							title="Edit"
							aria-label="Edit VPC connection {v.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteVpcConnection(v)}
							title="Delete"
							aria-label="Delete VPC connection {v.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const vpcColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'VPCConnectionId', label: 'ID' },
					{ key: 'Status', label: 'Status', render: vpcStatusCell },
					{ key: 'LastUpdatedTime', label: 'Updated', render: vpcUpdatedCell },
					{ key: 'actions', label: '', render: vpcActionsCell }
				] as Column<VPCConnectionSummary>[]}
				<DataTable
					rows={filteredVpcConnections}
					rowKey={(v) => v.VPCConnectionId ?? ''}
					columns={vpcColumns}
					loading={tabLoader.isLoading('vpcConnections')}
					emptyMessage="No VPC connections found"
				/>
				<LoadMore
					hasMore={!!vpcConnectionsNextToken}
					loading={loadingMoreVpcConnections}
					onLoadMore={loadMoreVpcConnections}
				/>
			{/if}
		</div>
	</div>
</div>

<!-- ==================== Dashboards modals ==================== -->

<Modal bind:this={createDashboardModal} title="Create Dashboard">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dashboard-id" class="text-sm text-slate-600 dark:text-slate-300">Dashboard ID</label>
				<input
					id="dashboard-id"
					bind:value={newDashboardId}
					placeholder="my-dashboard"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dashboard-name" class="text-sm text-slate-600 dark:text-slate-300">Dashboard name</label>
				<input
					id="dashboard-name"
					bind:value={newDashboardName}
					placeholder="Sales Overview"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dashboard-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional)</label
				>
				<textarea
					id="dashboard-definition"
					bind:value={newDashboardDefinition}
					rows="4"
					placeholder="{'{'}}"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createDashboardError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDashboardError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createDashboardModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateDashboard}
			disabled={creatingDashboard}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingDashboard ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editDashboardModal} title="Edit Dashboard">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingDashboard?.DashboardId}</span>
			</p>
			<div>
				<label for="edit-dashboard-name" class="text-sm text-slate-600 dark:text-slate-300">New dashboard name</label>
				<input
					id="edit-dashboard-name"
					bind:value={editingDashboardName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-dashboard-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional -- leave blank to keep the existing definition)</label
				>
				<textarea
					id="edit-dashboard-definition"
					bind:value={editingDashboardDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editDashboardError}
				<p class="text-sm text-red-600 dark:text-red-400">{editDashboardError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editDashboardModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditDashboard}
			disabled={savingDashboard}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingDashboard ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={dashboardDetailModal} title="Dashboard">
	{#snippet children()}
		{#if dashboardDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedDashboard}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDashboard.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Dashboard ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDashboard.DashboardId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedDashboard.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDashboard.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDashboard.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if dashboardDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{dashboardDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => dashboardDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Analyses modals ==================== -->

<Modal bind:this={createAnalysisModal} title="Create Analysis">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="analysis-id" class="text-sm text-slate-600 dark:text-slate-300">Analysis ID</label>
				<input
					id="analysis-id"
					bind:value={newAnalysisId}
					placeholder="my-analysis"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="analysis-name" class="text-sm text-slate-600 dark:text-slate-300">Analysis name</label>
				<input
					id="analysis-name"
					bind:value={newAnalysisName}
					placeholder="Quarterly Trends"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="analysis-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional)</label
				>
				<textarea
					id="analysis-definition"
					bind:value={newAnalysisDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createAnalysisError}
				<p class="text-sm text-red-600 dark:text-red-400">{createAnalysisError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createAnalysisModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateAnalysis}
			disabled={creatingAnalysis}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingAnalysis ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editAnalysisModal} title="Edit Analysis">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingAnalysis?.AnalysisId}</span>
			</p>
			<div>
				<label for="edit-analysis-name" class="text-sm text-slate-600 dark:text-slate-300">New analysis name</label>
				<input
					id="edit-analysis-name"
					bind:value={editingAnalysisName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-analysis-definition" class="text-sm text-slate-600 dark:text-slate-300"
					>Definition (JSON, optional -- leave blank to keep the existing definition)</label
				>
				<textarea
					id="edit-analysis-definition"
					bind:value={editingAnalysisDefinition}
					rows="4"
					class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editAnalysisError}
				<p class="text-sm text-red-600 dark:text-red-400">{editAnalysisError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editAnalysisModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditAnalysis}
			disabled={savingAnalysis}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingAnalysis ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={analysisDetailModal} title="Analysis">
	{#snippet children()}
		{#if analysisDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAnalysis}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalysis.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Analysis ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalysis.AnalysisId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedAnalysis.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAnalysis.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedAnalysis.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedAnalysis.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if analysisDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{analysisDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => analysisDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Data Sets modals ==================== -->

<Modal bind:this={createDataSetModal} title="Create Data Set">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dataset-id" class="text-sm text-slate-600 dark:text-slate-300">Data set ID</label>
				<input
					id="dataset-id"
					bind:value={newDataSetId}
					placeholder="my-dataset"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dataset-name" class="text-sm text-slate-600 dark:text-slate-300">Data set name</label>
				<input
					id="dataset-name"
					bind:value={newDataSetName}
					placeholder="Orders"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="dataset-import-mode" class="text-sm text-slate-600 dark:text-slate-300"
					>Import mode</label
				>
				<select
					id="dataset-import-mode"
					bind:value={newDataSetImportMode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SPICE">SPICE</option>
					<option value="DIRECT_QUERY">Direct Query</option>
				</select>
			</div>
			{#if createDataSetError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDataSetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createDataSetModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateDataSet}
			disabled={creatingDataSet}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingDataSet ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editDataSetModal} title="Edit Data Set">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingDataSet?.DataSetId}</span>
			</p>
			<div>
				<label for="edit-dataset-name" class="text-sm text-slate-600 dark:text-slate-300">New data set name</label>
				<input
					id="edit-dataset-name"
					bind:value={editingDataSetName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-dataset-import-mode" class="text-sm text-slate-600 dark:text-slate-300"
					>Import mode</label
				>
				<select
					id="edit-dataset-import-mode"
					bind:value={editingDataSetImportMode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SPICE">SPICE</option>
					<option value="DIRECT_QUERY">Direct Query</option>
				</select>
			</div>
			{#if editDataSetError}
				<p class="text-sm text-red-600 dark:text-red-400">{editDataSetError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editDataSetModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditDataSet}
			disabled={savingDataSet}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingDataSet ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={dataSetDetailModal} title="Data Set">
	{#snippet children()}
		{#if dataSetDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedDataSet}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSet.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Data set ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSet.DataSetId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedDataSet.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Import mode</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSet.ImportMode ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSet.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSet.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if dataSetDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{dataSetDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => dataSetDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Data Sources modals ==================== -->

<Modal bind:this={createDataSourceModal} title="Create Data Source">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="datasource-id" class="text-sm text-slate-600 dark:text-slate-300">Data source ID</label>
				<input
					id="datasource-id"
					bind:value={newDataSourceId}
					placeholder="my-datasource"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="datasource-name" class="text-sm text-slate-600 dark:text-slate-300">Data source name</label>
				<input
					id="datasource-name"
					bind:value={newDataSourceName}
					placeholder="Production MySQL"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="datasource-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="datasource-type"
					bind:value={newDataSourceType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					{#each dataSourceTypes as t (t)}
						<option value={t}>{t}</option>
					{/each}
				</select>
			</div>
			{#if createDataSourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDataSourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createDataSourceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateDataSource}
			disabled={creatingDataSource}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingDataSource ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editDataSourceModal} title="Edit Data Source">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingDataSource?.DataSourceId}</span> ({editingDataSource?.Type})
			</p>
			<div>
				<label for="edit-datasource-name" class="text-sm text-slate-600 dark:text-slate-300">New data source name</label>
				<input
					id="edit-datasource-name"
					bind:value={editingDataSourceName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editDataSourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{editDataSourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editDataSourceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditDataSource}
			disabled={savingDataSource}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingDataSource ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={dataSourceDetailModal} title="Data Source">
	{#snippet children()}
		{#if dataSourceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedDataSource}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Data source ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.DataSourceId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedDataSource.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.Type ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedDataSource.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSource.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedDataSource.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if dataSourceDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{dataSourceDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => dataSourceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Folders modals ==================== -->

<Modal bind:this={createFolderModal} title="Create Folder">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="folder-id" class="text-sm text-slate-600 dark:text-slate-300">Folder ID</label>
				<input
					id="folder-id"
					bind:value={newFolderId}
					placeholder="my-folder"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="folder-name" class="text-sm text-slate-600 dark:text-slate-300">Folder name</label>
				<input
					id="folder-name"
					bind:value={newFolderName}
					placeholder="Marketing"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="folder-type" class="text-sm text-slate-600 dark:text-slate-300">Folder type</label>
				<select
					id="folder-type"
					bind:value={newFolderType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="SHARED">Shared</option>
					<option value="RESTRICTED">Restricted</option>
				</select>
			</div>
			<div>
				<label for="folder-parent-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>Parent folder ARN (optional -- omit for a root-level folder)</label
				>
				<input
					id="folder-parent-arn"
					bind:value={newFolderParentArn}
					placeholder="arn:aws:quicksight:...:folder/parent-id"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="folder-sharing-model" class="text-sm text-slate-600 dark:text-slate-300"
					>Sharing model (optional -- defaults to Account)</label
				>
				<select
					id="folder-sharing-model"
					bind:value={newFolderSharingModel}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">(default)</option>
					<option value="ACCOUNT">Account</option>
					<option value="NAMESPACE">Namespace</option>
				</select>
			</div>
			{#if createFolderError}
				<p class="text-sm text-red-600 dark:text-red-400">{createFolderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createFolderModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateFolder}
			disabled={creatingFolder}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingFolder ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editFolderModal} title="Edit Folder">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingFolder?.FolderId}</span>
			</p>
			<div>
				<label for="edit-folder-name" class="text-sm text-slate-600 dark:text-slate-300">New folder name</label>
				<input
					id="edit-folder-name"
					bind:value={editingFolderName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editFolderError}
				<p class="text-sm text-red-600 dark:text-red-400">{editFolderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editFolderModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditFolder}
			disabled={savingFolder}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingFolder ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={folderDetailModal} title="Folder">
	{#snippet children()}
		{#if folderDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedFolder}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Folder ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.FolderId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedFolder.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Type</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.FolderType ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Sharing model</dt>
					<dd class="text-slate-900 dark:text-white">{viewedFolder.SharingModel ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFolder.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedFolder.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if folderDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{folderDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => folderDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== VPC Connections modals ==================== -->

<Modal bind:this={createVpcConnectionModal} title="Create VPC Connection">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="vpc-connection-id" class="text-sm text-slate-600 dark:text-slate-300"
					>VPC connection ID</label
				>
				<input
					id="vpc-connection-id"
					bind:value={newVpcConnectionId}
					placeholder="my-vpc-connection"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-name" class="text-sm text-slate-600 dark:text-slate-300">VPC connection name</label>
				<input
					id="vpc-connection-name"
					bind:value={newVpcConnectionName}
					placeholder="Private VPC"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-subnets" class="text-sm text-slate-600 dark:text-slate-300"
					>Subnet IDs (comma-separated)</label
				>
				<input
					id="vpc-connection-subnets"
					bind:value={newVpcConnectionSubnetIds}
					placeholder="subnet-0123, subnet-0456"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-sgs" class="text-sm text-slate-600 dark:text-slate-300"
					>Security group IDs (comma-separated)</label
				>
				<input
					id="vpc-connection-sgs"
					bind:value={newVpcConnectionSecurityGroupIds}
					placeholder="sg-0123"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-dns" class="text-sm text-slate-600 dark:text-slate-300"
					>DNS resolver IPs (comma-separated, optional)</label
				>
				<input
					id="vpc-connection-dns"
					bind:value={newVpcConnectionDnsResolvers}
					placeholder="10.0.0.2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="vpc-connection-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>IAM role ARN</label
				>
				<input
					id="vpc-connection-role-arn"
					bind:value={newVpcConnectionRoleArn}
					placeholder="arn:aws:iam::123456789012:role/quicksight-vpc"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createVpcConnectionError}
				<p class="text-sm text-red-600 dark:text-red-400">{createVpcConnectionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createVpcConnectionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateVpcConnection}
			disabled={creatingVpcConnection}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingVpcConnection ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editVpcConnectionModal} title="Edit VPC Connection">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingVpcConnection?.VPCConnectionId}</span>
			</p>
			<div>
				<label for="edit-vpc-connection-name" class="text-sm text-slate-600 dark:text-slate-300">New VPC connection name</label>
				<input
					id="edit-vpc-connection-name"
					bind:value={editingVpcConnectionName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-subnets" class="text-sm text-slate-600 dark:text-slate-300"
					>Subnet IDs (comma-separated -- not returned by Describe/List, re-enter the desired set)</label
				>
				<input
					id="edit-vpc-connection-subnets"
					bind:value={editingVpcConnectionSubnetIds}
					placeholder="subnet-0123, subnet-0456"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-sgs" class="text-sm text-slate-600 dark:text-slate-300"
					>Security group IDs (comma-separated)</label
				>
				<input
					id="edit-vpc-connection-sgs"
					bind:value={editingVpcConnectionSecurityGroupIds}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-dns" class="text-sm text-slate-600 dark:text-slate-300"
					>DNS resolver IPs (comma-separated, optional)</label
				>
				<input
					id="edit-vpc-connection-dns"
					bind:value={editingVpcConnectionDnsResolvers}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vpc-connection-role-arn" class="text-sm text-slate-600 dark:text-slate-300"
					>IAM role ARN</label
				>
				<input
					id="edit-vpc-connection-role-arn"
					bind:value={editingVpcConnectionRoleArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editVpcConnectionError}
				<p class="text-sm text-red-600 dark:text-red-400">{editVpcConnectionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editVpcConnectionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditVpcConnection}
			disabled={savingVpcConnection}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{savingVpcConnection ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={vpcConnectionDetailModal} title="VPC Connection">
	{#snippet children()}
		{#if vpcConnectionDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedVpcConnection}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">VPC connection ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.VPCConnectionId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedVpcConnection.Arn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">VPC ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.VPCId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Security group IDs</dt>
					<dd class="text-slate-900 dark:text-white"
						>{(viewedVpcConnection.SecurityGroupIds ?? []).join(', ') || '—'}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">DNS resolvers</dt>
					<dd class="text-slate-900 dark:text-white"
						>{(viewedVpcConnection.DnsResolvers ?? []).join(', ') || '—'}</dd
					>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Role ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedVpcConnection.RoleArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Availability</dt>
					<dd class="text-slate-900 dark:text-white">{viewedVpcConnection.AvailabilityStatus ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedVpcConnection.CreatedTime)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Last updated</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedVpcConnection.LastUpdatedTime)}</dd>
				</div>
			</dl>
			{#if vpcConnectionDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{vpcConnectionDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => vpcConnectionDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
