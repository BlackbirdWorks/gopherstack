<script lang="ts">
	// Amazon Timestream is split across two AWS SDK clients / gopherstack
	// backends: TimestreamWrite (services/timestreamwrite) owns the control
	// plane (Databases, Tables, BatchLoadTasks) plus the WriteRecords data
	// plane, and TimestreamQuery (services/timestreamquery) owns SQL query
	// execution and scheduled queries. This page — labelled "Timestream
	// Write" in the nav (see $lib/nav.ts) — is the control-plane + write-path
	// page and is built out to the CRUD floor for Databases, Tables and
	// Batch Load Tasks (every family TimestreamWrite lists). The sibling
	// "Timestream Query" page (/dashboard/timestreamquery) already has full
	// CRUD for scheduled queries, ad-hoc SQL execution, and account settings
	// — that surface is intentionally NOT duplicated here; the Scheduled
	// Queries tab below is a read-only cross-reference so an operator
	// looking at Timestream Write still sees what's scheduled against it,
	// with a link to the page that can actually manage them.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getTimestreamQueryClient, getTimestreamWriteClient } from '$lib/aws-client';
	import {
		ListDatabasesCommand,
		DescribeDatabaseCommand,
		CreateDatabaseCommand,
		UpdateDatabaseCommand,
		DeleteDatabaseCommand,
		ListTablesCommand,
		DescribeTableCommand,
		CreateTableCommand,
		UpdateTableCommand,
		DeleteTableCommand,
		WriteRecordsCommand,
		ListBatchLoadTasksCommand,
		DescribeBatchLoadTaskCommand,
		CreateBatchLoadTaskCommand,
		ResumeBatchLoadTaskCommand,
		type Database,
		type Table,
		type BatchLoadTask,
		type BatchLoadTaskDescription
	} from '@aws-sdk/client-timestream-write';
	import { ListScheduledQueriesCommand, type ScheduledQuery } from '@aws-sdk/client-timestream-query';
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
	import {
		Clock,
		Database as DatabaseIcon,
		Table2,
		Upload,
		Plus,
		Trash2,
		Pencil,
		Eye,
		PlayCircle,
		Send
	} from 'lucide-svelte';

	const tsWrite = regionalClient(getTimestreamWriteClient);
	const tsQuery = regionalClient(getTimestreamQueryClient);

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

	type TabId = 'databases' | 'tables' | 'batchLoadTasks' | 'scheduledQueries';

	const tabs: TabDef[] = [
		{ id: 'databases', label: 'Databases' },
		{ id: 'tables', label: 'Tables' },
		{ id: 'batchLoadTasks', label: 'Batch Load Tasks' },
		{ id: 'scheduledQueries', label: 'Scheduled Queries' }
	];

	let activeTab = $state<TabId>('databases');
	let searchQuery = $state('');

	// ==================== Databases ====================

	let databases = $state<Database[]>([]);

	async function fetchDatabases(): Promise<void> {
		const resp = await tsWrite().send(new ListDatabasesCommand({}));
		databases = resp.Databases ?? [];
	}

	const filteredDatabases = $derived(
		databases.filter((d) => (d.DatabaseName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ==================== Tables (scoped to selectedDatabase) ====================

	let selectedDatabase = $state<string | null>(null);
	let tables = $state<Table[]>([]);

	async function fetchTables(): Promise<void> {
		// Read untracked: switchTab()/selectDatabaseForTables() already write
		// selectedDatabase and force a reload themselves, so letting this
		// tab-loader fetcher also depend on it would double-fetch on every
		// region change (same hazard workmail's orgIdFilter has to avoid).
		const dbName = untrack(() => selectedDatabase);
		if (!dbName) {
			tables = [];
			return;
		}
		const resp = await tsWrite().send(new ListTablesCommand({ DatabaseName: dbName }));
		tables = resp.Tables ?? [];
	}

	function selectDatabaseForTables(dbName: string): void {
		selectedDatabase = dbName;
		activeTab = 'tables';
		searchQuery = '';
		tabLoader.refresh('tables');
	}

	const filteredTables = $derived(
		tables.filter((t) => (t.TableName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ==================== Batch Load Tasks (global) ====================

	let batchLoadTasks = $state<BatchLoadTask[]>([]);

	async function fetchBatchLoadTasks(): Promise<void> {
		const resp = await tsWrite().send(new ListBatchLoadTasksCommand({}));
		batchLoadTasks = resp.BatchLoadTasks ?? [];
	}

	const filteredBatchLoadTasks = $derived(
		batchLoadTasks.filter((t) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(t.TaskId ?? '').toLowerCase().includes(q) ||
				(t.DatabaseName ?? '').toLowerCase().includes(q) ||
				(t.TableName ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ==================== Scheduled Queries (read-only cross-reference) ====================

	let scheduledQueries = $state<ScheduledQuery[]>([]);

	async function fetchScheduledQueries(): Promise<void> {
		const resp = await tsQuery().send(new ListScheduledQueriesCommand({}));
		scheduledQueries = resp.ScheduledQueries ?? [];
	}

	const filteredScheduledQueries = $derived(
		scheduledQueries.filter((q) => (q.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ==================== Tab loader ====================

	const tabLoader = createTabLoader<TabId>({
		databases: () => fetchDatabases().catch(rethrowDescribed),
		tables: () => fetchTables().catch(rethrowDescribed),
		batchLoadTasks: () => fetchBatchLoadTasks().catch(rethrowDescribed),
		scheduledQueries: () => fetchScheduledQueries().catch(rethrowDescribed)
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
		selectedDatabase = null;
		tables = [];
		tabLoader.refresh('databases');
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// ==================== Database detail (Describe) ====================

	let dbDetailModal = $state<Modal | null>(null);
	let dbDetail = $state<Database | null>(null);
	let dbDetailLoading = $state(false);

	async function openDatabaseDetail(d: Database): Promise<void> {
		if (!d.DatabaseName) return;
		dbDetail = null;
		dbDetailLoading = true;
		dbDetailModal?.open();
		try {
			const resp = await tsWrite().send(new DescribeDatabaseCommand({ DatabaseName: d.DatabaseName }));
			dbDetail = resp.Database ?? null;
		} catch (e) {
			toast.error(describeError(e));
			dbDetailModal?.close();
		} finally {
			dbDetailLoading = false;
		}
	}

	// ==================== Create / Edit / Delete Database ====================

	let dbCreateModal = $state<Modal | null>(null);
	let dbCreating = $state(false);
	let dbCreateError = $state<string | null>(null);
	let newDbName = $state('');
	let newDbKmsKeyId = $state('');

	function openDbCreateModal(): void {
		dbCreateError = null;
		newDbName = '';
		newDbKmsKeyId = '';
		dbCreateModal?.open();
	}

	async function submitCreateDatabase(): Promise<void> {
		if (!newDbName) {
			dbCreateError = 'Database name is required.';
			return;
		}
		dbCreating = true;
		dbCreateError = null;
		try {
			await tsWrite().send(
				new CreateDatabaseCommand({ DatabaseName: newDbName, KmsKeyId: newDbKmsKeyId || undefined })
			);
			toast.success(`Database "${newDbName}" created`);
			dbCreateModal?.close();
			await tabLoader.refresh('databases');
		} catch (e) {
			const msg = describeError(e);
			dbCreateError = msg;
			toast.error(msg);
		} finally {
			dbCreating = false;
		}
	}

	let dbEditModal = $state<Modal | null>(null);
	let dbEditing = $state(false);
	let dbEditError = $state<string | null>(null);
	let editDbName = $state('');
	let editDbKmsKeyId = $state('');

	function openDbEditModal(d: Database): void {
		dbEditError = null;
		editDbName = d.DatabaseName ?? '';
		editDbKmsKeyId = d.KmsKeyId ?? '';
		dbEditModal?.open();
	}

	async function submitEditDatabase(): Promise<void> {
		if (!editDbName) return;
		dbEditing = true;
		dbEditError = null;
		try {
			// UpdateDatabaseRequest.KmsKeyId is a required field on the real
			// wire shape (empty string clears the key, per
			// services/timestreamwrite/PARITY.md's UpdateDatabase note).
			await tsWrite().send(new UpdateDatabaseCommand({ DatabaseName: editDbName, KmsKeyId: editDbKmsKeyId }));
			toast.success('Database updated');
			dbEditModal?.close();
			await tabLoader.refresh('databases');
		} catch (e) {
			const msg = describeError(e);
			dbEditError = msg;
			toast.error(msg);
		} finally {
			dbEditing = false;
		}
	}

	async function deleteDatabase(d: Database): Promise<void> {
		if (!d.DatabaseName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete database',
			message: `Delete database "${d.DatabaseName}"? All tables must already be deleted, and this cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await tsWrite().send(new DeleteDatabaseCommand({ DatabaseName: d.DatabaseName }));
			toast.success(`Database "${d.DatabaseName}" deleted`);
			if (selectedDatabase === d.DatabaseName) selectedDatabase = null;
			await tabLoader.refresh('databases');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Table detail (Describe) ====================

	let tableDetailModal = $state<Modal | null>(null);
	let tableDetail = $state<Table | null>(null);
	let tableDetailLoading = $state(false);

	async function openTableDetail(t: Table): Promise<void> {
		if (!t.DatabaseName || !t.TableName) return;
		tableDetail = null;
		tableDetailLoading = true;
		tableDetailModal?.open();
		try {
			const resp = await tsWrite().send(
				new DescribeTableCommand({ DatabaseName: t.DatabaseName, TableName: t.TableName })
			);
			tableDetail = resp.Table ?? null;
		} catch (e) {
			toast.error(describeError(e));
			tableDetailModal?.close();
		} finally {
			tableDetailLoading = false;
		}
	}

	// ==================== Create / Edit / Delete Table ====================

	let tableCreateModal = $state<Modal | null>(null);
	let tableCreating = $state(false);
	let tableCreateError = $state<string | null>(null);
	let newTableName = $state('');
	let newTableMemoryHours = $state(24);
	let newTableMagneticDays = $state(7);

	function openTableCreateModal(): void {
		if (!selectedDatabase) {
			toast.error('Select a database first');
			return;
		}
		tableCreateError = null;
		newTableName = '';
		newTableMemoryHours = 24;
		newTableMagneticDays = 7;
		tableCreateModal?.open();
	}

	async function submitCreateTable(): Promise<void> {
		if (!selectedDatabase || !newTableName) {
			tableCreateError = 'Table name is required.';
			return;
		}
		tableCreating = true;
		tableCreateError = null;
		try {
			await tsWrite().send(
				new CreateTableCommand({
					DatabaseName: selectedDatabase,
					TableName: newTableName,
					RetentionProperties: {
						MemoryStoreRetentionPeriodInHours: newTableMemoryHours,
						MagneticStoreRetentionPeriodInDays: newTableMagneticDays
					}
				})
			);
			toast.success(`Table "${newTableName}" created`);
			tableCreateModal?.close();
			await tabLoader.refresh('tables');
			await tabLoader.refresh('databases');
		} catch (e) {
			const msg = describeError(e);
			tableCreateError = msg;
			toast.error(msg);
		} finally {
			tableCreating = false;
		}
	}

	let tableEditModal = $state<Modal | null>(null);
	let tableEditing = $state(false);
	let tableEditError = $state<string | null>(null);
	let editTableDbName = $state('');
	let editTableName = $state('');
	let editTableMemoryHours = $state(24);
	let editTableMagneticDays = $state(7);

	function openTableEditModal(t: Table): void {
		tableEditError = null;
		editTableDbName = t.DatabaseName ?? '';
		editTableName = t.TableName ?? '';
		editTableMemoryHours = t.RetentionProperties?.MemoryStoreRetentionPeriodInHours ?? 24;
		editTableMagneticDays = t.RetentionProperties?.MagneticStoreRetentionPeriodInDays ?? 7;
		tableEditModal?.open();
	}

	async function submitEditTable(): Promise<void> {
		if (!editTableDbName || !editTableName) return;
		tableEditing = true;
		tableEditError = null;
		try {
			await tsWrite().send(
				new UpdateTableCommand({
					DatabaseName: editTableDbName,
					TableName: editTableName,
					RetentionProperties: {
						MemoryStoreRetentionPeriodInHours: editTableMemoryHours,
						MagneticStoreRetentionPeriodInDays: editTableMagneticDays
					}
				})
			);
			toast.success('Table updated');
			tableEditModal?.close();
			await tabLoader.refresh('tables');
		} catch (e) {
			const msg = describeError(e);
			tableEditError = msg;
			toast.error(msg);
		} finally {
			tableEditing = false;
		}
	}

	async function deleteTable(t: Table): Promise<void> {
		if (!t.DatabaseName || !t.TableName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete table',
			message: `Delete table "${t.TableName}"? All stored records will be permanently lost.`
		});
		if (!confirmed) return;
		try {
			await tsWrite().send(new DeleteTableCommand({ DatabaseName: t.DatabaseName, TableName: t.TableName }));
			toast.success(`Table "${t.TableName}" deleted`);
			await tabLoader.refresh('tables');
			await tabLoader.refresh('databases');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Write Records (data plane) ====================

	let writeRecordsModal = $state<Modal | null>(null);
	let writeRecordsBusy = $state(false);
	let writeRecordsError = $state<string | null>(null);
	let wrDbName = $state('');
	let wrTableName = $state('');
	let wrMeasureName = $state('');
	let wrMeasureValue = $state('');
	let wrMeasureValueType = $state<'DOUBLE' | 'BIGINT' | 'VARCHAR' | 'BOOLEAN' | 'TIMESTAMP'>('DOUBLE');
	let wrTime = $state('');
	let wrDimensionName = $state('');
	let wrDimensionValue = $state('');

	function openWriteRecordsModal(t: Table): void {
		writeRecordsError = null;
		wrDbName = t.DatabaseName ?? '';
		wrTableName = t.TableName ?? '';
		wrMeasureName = '';
		wrMeasureValue = '';
		wrMeasureValueType = 'DOUBLE';
		wrTime = String(Date.now());
		wrDimensionName = '';
		wrDimensionValue = '';
		writeRecordsModal?.open();
	}

	async function submitWriteRecords(): Promise<void> {
		if (!wrDbName || !wrTableName || !wrMeasureName || !wrMeasureValue || !wrTime) {
			writeRecordsError = 'Measure name, measure value, and time are required.';
			return;
		}
		writeRecordsBusy = true;
		writeRecordsError = null;
		try {
			const resp = await tsWrite().send(
				new WriteRecordsCommand({
					DatabaseName: wrDbName,
					TableName: wrTableName,
					Records: [
						{
							MeasureName: wrMeasureName,
							MeasureValue: wrMeasureValue,
							MeasureValueType: wrMeasureValueType,
							Time: wrTime,
							TimeUnit: 'MILLISECONDS',
							Dimensions: wrDimensionName
								? [{ Name: wrDimensionName, Value: wrDimensionValue }]
								: undefined
						}
					]
				})
			);
			toast.success(`Wrote ${resp.RecordsIngested?.Total ?? 1} record(s)`);
			writeRecordsModal?.close();
		} catch (e) {
			const msg = describeError(e);
			writeRecordsError = msg;
			toast.error(msg);
		} finally {
			writeRecordsBusy = false;
		}
	}

	// ==================== Batch Load Task detail (Describe) ====================

	let taskDetailModal = $state<Modal | null>(null);
	let taskDetail = $state<BatchLoadTaskDescription | null>(null);
	let taskDetailLoading = $state(false);

	async function openTaskDetail(t: BatchLoadTask): Promise<void> {
		if (!t.TaskId) return;
		taskDetail = null;
		taskDetailLoading = true;
		taskDetailModal?.open();
		try {
			const resp = await tsWrite().send(new DescribeBatchLoadTaskCommand({ TaskId: t.TaskId }));
			taskDetail = resp.BatchLoadTaskDescription ?? null;
		} catch (e) {
			toast.error(describeError(e));
			taskDetailModal?.close();
		} finally {
			taskDetailLoading = false;
		}
	}

	// ==================== Create Batch Load Task ====================

	let taskCreateModal = $state<Modal | null>(null);
	let taskCreating = $state(false);
	let taskCreateError = $state<string | null>(null);
	let newTaskDbName = $state('');
	let newTaskTableName = $state('');
	let newTaskBucket = $state('');
	let newTaskPrefix = $state('');
	let newTaskReportBucket = $state('');

	function openTaskCreateModal(): void {
		taskCreateError = null;
		newTaskDbName = selectedDatabase ?? databases[0]?.DatabaseName ?? '';
		newTaskTableName = '';
		newTaskBucket = '';
		newTaskPrefix = '';
		newTaskReportBucket = '';
		taskCreateModal?.open();
	}

	async function submitCreateTask(): Promise<void> {
		if (!newTaskDbName || !newTaskTableName || !newTaskBucket || !newTaskReportBucket) {
			taskCreateError = 'Target database/table and both S3 buckets are required.';
			return;
		}
		taskCreating = true;
		taskCreateError = null;
		try {
			await tsWrite().send(
				new CreateBatchLoadTaskCommand({
					TargetDatabaseName: newTaskDbName,
					TargetTableName: newTaskTableName,
					DataSourceConfiguration: {
						DataFormat: 'CSV',
						DataSourceS3Configuration: { BucketName: newTaskBucket, ObjectKeyPrefix: newTaskPrefix || undefined }
					},
					ReportConfiguration: {
						ReportS3Configuration: { BucketName: newTaskReportBucket }
					}
				})
			);
			toast.success('Batch load task created');
			taskCreateModal?.close();
			await tabLoader.refresh('batchLoadTasks');
		} catch (e) {
			const msg = describeError(e);
			taskCreateError = msg;
			toast.error(msg);
		} finally {
			taskCreating = false;
		}
	}

	async function resumeTask(t: BatchLoadTask): Promise<void> {
		if (!t.TaskId) return;
		try {
			await tsWrite().send(new ResumeBatchLoadTaskCommand({ TaskId: t.TaskId }));
			toast.success('Batch load task resumed');
			await tabLoader.refresh('batchLoadTasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	function isResumable(status: string | undefined): boolean {
		return status === 'FAILED' || status === 'PENDING_RESUME';
	}

	function taskStatusClass(status: string | undefined): string {
		if (status === 'SUCCEEDED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		if (status === 'IN_PROGRESS') return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}
</script>

{#snippet dbActionsCell(d: Database)}
	<div class="flex items-center gap-2 justify-end">
		<button onclick={() => selectDatabaseForTables(d.DatabaseName ?? '')} title="View tables" aria-label="View tables in {d.DatabaseName}" class="text-gray-400 hover:text-cyan-500">
			<Table2 class="w-4 h-4" />
		</button>
		<button onclick={() => openDatabaseDetail(d)} title="View" aria-label="View database {d.DatabaseName}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => openDbEditModal(d)} title="Edit" aria-label="Edit database {d.DatabaseName}" class="text-gray-400 hover:text-cyan-500"><Pencil class="w-4 h-4" /></button>
		<button onclick={() => deleteDatabase(d)} title="Delete" aria-label="Delete database {d.DatabaseName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}
{#snippet dbUpdatedCell(d: Database)}
	<span class="text-xs text-gray-500 dark:text-gray-400">{formatDate(d.LastUpdatedTime)}</span>
{/snippet}
{#snippet dbKmsCell(d: Database)}
	<span class="text-xs font-mono text-gray-500 dark:text-gray-400">{d.KmsKeyId ?? '—'}</span>
{/snippet}

{#snippet tableStatusCell(t: Table)}
	<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">{t.TableStatus}</span>
{/snippet}
{#snippet tableRetentionCell(t: Table)}
	<span class="text-xs text-gray-500 dark:text-gray-400">
		{t.RetentionProperties?.MemoryStoreRetentionPeriodInHours ?? '—'}h mem / {t.RetentionProperties?.MagneticStoreRetentionPeriodInDays ?? '—'}d magnetic
	</span>
{/snippet}
{#snippet tableActionsCell(t: Table)}
	<div class="flex items-center gap-2 justify-end">
		<button onclick={() => openWriteRecordsModal(t)} title="Write records" aria-label="Write records to {t.TableName}" class="text-gray-400 hover:text-cyan-500"><Send class="w-4 h-4" /></button>
		<button onclick={() => openTableDetail(t)} title="View" aria-label="View table {t.TableName}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
		<button onclick={() => openTableEditModal(t)} title="Edit" aria-label="Edit table {t.TableName}" class="text-gray-400 hover:text-cyan-500"><Pencil class="w-4 h-4" /></button>
		<button onclick={() => deleteTable(t)} title="Delete" aria-label="Delete table {t.TableName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
	</div>
{/snippet}

{#snippet taskStatusCell(t: BatchLoadTask)}
	<span class="text-xs px-2 py-1 rounded-full {taskStatusClass(t.TaskStatus)}">{t.TaskStatus}</span>
{/snippet}
{#snippet taskCreatedCell(t: BatchLoadTask)}
	<span class="text-xs text-gray-500 dark:text-gray-400">{formatDate(t.CreationTime)}</span>
{/snippet}
{#snippet taskActionsCell(t: BatchLoadTask)}
	<div class="flex items-center gap-2 justify-end">
		{#if isResumable(t.TaskStatus)}
			<button onclick={() => resumeTask(t)} title="Resume" aria-label="Resume task {t.TaskId}" class="text-gray-400 hover:text-cyan-500"><PlayCircle class="w-4 h-4" /></button>
		{/if}
		<button onclick={() => openTaskDetail(t)} title="View" aria-label="View task {t.TaskId}" class="text-gray-400 hover:text-cyan-500"><Eye class="w-4 h-4" /></button>
	</div>
{/snippet}

{#snippet sqStateCell(q: ScheduledQuery)}
	<span class="text-xs px-2 py-1 rounded-full {q.State === 'ENABLED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{q.State}</span>
{/snippet}

<div class="p-6 space-y-6">
	<PageHeader
		icon={Clock}
		title="Amazon Timestream Write"
		description="Databases, tables and batch loads for the Timestream control and write planes"
		onRefresh={handleRefresh}
		color="cyan"
	>
		{#snippet actions()}
			{#if activeTab === 'databases'}
				<button onclick={openDbCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Plus class="w-4 h-4" /> Create database
				</button>
			{:else if activeTab === 'tables'}
				<button onclick={openTableCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Plus class="w-4 h-4" /> Create table
				</button>
			{:else if activeTab === 'batchLoadTasks'}
				<button onclick={openTaskCreateModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700 text-sm">
					<Upload class="w-4 h-4" /> Create batch load task
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="cyan" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'databases'}
				{@const dbColumns = defineColumns<Database>([
					{ key: 'DatabaseName', label: 'Name' },
					{ key: 'TableCount', label: 'Tables' },
					{ key: 'KmsKeyId', label: 'KMS Key', render: dbKmsCell },
					{ key: 'LastUpdatedTime', label: 'Last Updated', render: dbUpdatedCell },
					{ key: 'actions', label: '', render: dbActionsCell }
				])}
				<DataTable
					rows={filteredDatabases}
					rowKey={(d) => d.DatabaseName ?? ''}
					columns={dbColumns}
					loading={tabLoader.isLoading('databases')}
					emptyMessage="No databases found"
				/>
			{:else if activeTab === 'tables'}
				<div class="flex items-center gap-2 text-sm">
					<DatabaseIcon class="w-4 h-4 text-gray-400" />
					<label for="ts-table-db-select" class="text-gray-500 dark:text-gray-400">Database</label>
					<select
						id="ts-table-db-select"
						value={selectedDatabase ?? ''}
						onchange={(e) => selectDatabaseForTables((e.target as HTMLSelectElement).value)}
						class="px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white text-sm"
					>
						<option value="" disabled>Select a database…</option>
						{#each databases as d (d.DatabaseName)}
							<option value={d.DatabaseName}>{d.DatabaseName}</option>
						{/each}
					</select>
				</div>
				{#if !selectedDatabase}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a database to view its tables</div>
				{:else}
					{@const tableColumns = defineColumns<Table>([
						{ key: 'TableName', label: 'Name' },
						{ key: 'TableStatus', label: 'Status', render: tableStatusCell },
						{ key: 'RetentionProperties', label: 'Retention', render: tableRetentionCell },
						{ key: 'actions', label: '', render: tableActionsCell }
					])}
					<DataTable
						rows={filteredTables}
						rowKey={(t) => t.TableName ?? ''}
						columns={tableColumns}
						loading={tabLoader.isLoading('tables')}
						emptyMessage="No tables found in this database"
					/>
				{/if}
			{:else if activeTab === 'batchLoadTasks'}
				{@const taskColumns = defineColumns<BatchLoadTask>([
					{ key: 'TaskId', label: 'Task ID' },
					{ key: 'DatabaseName', label: 'Database' },
					{ key: 'TableName', label: 'Table' },
					{ key: 'TaskStatus', label: 'Status', render: taskStatusCell },
					{ key: 'CreationTime', label: 'Created', render: taskCreatedCell },
					{ key: 'actions', label: '', render: taskActionsCell }
				])}
				<DataTable
					rows={filteredBatchLoadTasks}
					rowKey={(t) => t.TaskId ?? ''}
					columns={taskColumns}
					loading={tabLoader.isLoading('batchLoadTasks')}
					emptyMessage="No batch load tasks found"
				/>
			{:else if activeTab === 'scheduledQueries'}
				<p class="text-xs text-gray-500 dark:text-gray-400">
					Scheduled queries are managed on the
					<a href="/dashboard/timestreamquery" class="text-cyan-600 dark:text-cyan-400 hover:underline">Timestream Query</a>
					page. This list is read-only.
				</p>
				{@const sqColumns = defineColumns<ScheduledQuery>([
					{ key: 'Name', label: 'Name' },
					{ key: 'State', label: 'State', render: sqStateCell },
					{ key: 'Arn', label: 'ARN' }
				])}
				<DataTable
					rows={filteredScheduledQueries}
					rowKey={(q) => q.Arn ?? ''}
					columns={sqColumns}
					loading={tabLoader.isLoading('scheduledQueries')}
					emptyMessage="No scheduled queries found"
				/>
			{/if}
		</div>
	</div>
</div>

<!-- Create Database -->
<Modal bind:this={dbCreateModal} title="Create Database">
	{#snippet children()}
		<div class="space-y-3">
			{#if dbCreateError}<p class="text-sm text-red-600 dark:text-red-400">{dbCreateError}</p>{/if}
			<div>
				<label for="new-db-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Database Name</label>
				<input id="new-db-name" bind:value={newDbName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-db-kms" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">KMS Key ID <span class="text-gray-400">(optional)</span></label>
				<input id="new-db-kms" bind:value={newDbKmsKeyId} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => dbCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateDatabase} disabled={dbCreating} class="px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
			{dbCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Edit Database -->
<Modal bind:this={dbEditModal} title="Edit Database">
	{#snippet children()}
		<div class="space-y-3">
			{#if dbEditError}<p class="text-sm text-red-600 dark:text-red-400">{dbEditError}</p>{/if}
			<p class="text-sm text-gray-500 dark:text-gray-400">{editDbName}</p>
			<div>
				<label for="edit-db-kms" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">KMS Key ID <span class="text-gray-400">(empty clears the key)</span></label>
				<input id="edit-db-kms" bind:value={editDbKmsKeyId} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => dbEditModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitEditDatabase} disabled={dbEditing} class="px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
			{dbEditing ? 'Saving…' : 'Save'}
		</button>
	{/snippet}
</Modal>

<!-- Database Detail -->
<Modal bind:this={dbDetailModal} title="Database Detail">
	{#snippet children()}
		{#if dbDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if dbDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Name</span><span class="font-mono">{dbDetail.DatabaseName}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">ARN</span><span class="font-mono text-xs break-all">{dbDetail.Arn}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Table Count</span><span>{dbDetail.TableCount}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">KMS Key</span><span class="font-mono text-xs">{dbDetail.KmsKeyId ?? '—'}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Created</span><span>{formatDate(dbDetail.CreationTime)}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Last Updated</span><span>{formatDate(dbDetail.LastUpdatedTime)}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => dbDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Create Table -->
<Modal bind:this={tableCreateModal} title="Create Table">
	{#snippet children()}
		<div class="space-y-3">
			{#if tableCreateError}<p class="text-sm text-red-600 dark:text-red-400">{tableCreateError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400">Database: {selectedDatabase}</p>
			<div>
				<label for="new-table-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Table Name</label>
				<input id="new-table-name" bind:value={newTableName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-table-mem" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Memory Retention (hours)</label>
					<input id="new-table-mem" type="number" min="1" max="8766" bind:value={newTableMemoryHours} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
				<div>
					<label for="new-table-mag" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Magnetic Retention (days)</label>
					<input id="new-table-mag" type="number" min="1" max="73000" bind:value={newTableMagneticDays} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => tableCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateTable} disabled={tableCreating} class="px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
			{tableCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Edit Table -->
<Modal bind:this={tableEditModal} title="Edit Table">
	{#snippet children()}
		<div class="space-y-3">
			{#if tableEditError}<p class="text-sm text-red-600 dark:text-red-400">{tableEditError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400">{editTableDbName}.{editTableName}</p>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="edit-table-mem" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Memory Retention (hours)</label>
					<input id="edit-table-mem" type="number" min="1" max="8766" bind:value={editTableMemoryHours} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
				<div>
					<label for="edit-table-mag" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Magnetic Retention (days)</label>
					<input id="edit-table-mag" type="number" min="1" max="73000" bind:value={editTableMagneticDays} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => tableEditModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitEditTable} disabled={tableEditing} class="px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
			{tableEditing ? 'Saving…' : 'Save'}
		</button>
	{/snippet}
</Modal>

<!-- Table Detail -->
<Modal bind:this={tableDetailModal} title="Table Detail">
	{#snippet children()}
		{#if tableDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if tableDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Name</span><span class="font-mono">{tableDetail.TableName}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">ARN</span><span class="font-mono text-xs break-all">{tableDetail.Arn}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Status</span><span>{tableDetail.TableStatus}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Memory Retention</span><span>{tableDetail.RetentionProperties?.MemoryStoreRetentionPeriodInHours ?? '—'}h</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Magnetic Retention</span><span>{tableDetail.RetentionProperties?.MagneticStoreRetentionPeriodInDays ?? '—'}d</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Created</span><span>{formatDate(tableDetail.CreationTime)}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Last Updated</span><span>{formatDate(tableDetail.LastUpdatedTime)}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => tableDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Write Records -->
<Modal bind:this={writeRecordsModal} title="Write Records">
	{#snippet children()}
		<div class="space-y-3">
			{#if writeRecordsError}<p class="text-sm text-red-600 dark:text-red-400">{writeRecordsError}</p>{/if}
			<p class="text-xs text-gray-500 dark:text-gray-400">{wrDbName}.{wrTableName}</p>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="wr-measure-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Measure Name</label>
					<input id="wr-measure-name" bind:value={wrMeasureName} placeholder="cpu_utilization" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
				<div>
					<label for="wr-measure-value" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Measure Value</label>
					<input id="wr-measure-value" bind:value={wrMeasureValue} placeholder="12.5" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
				<div>
					<label for="wr-measure-type" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Value Type</label>
					<select id="wr-measure-type" bind:value={wrMeasureValueType} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
						<option value="DOUBLE">DOUBLE</option>
						<option value="BIGINT">BIGINT</option>
						<option value="VARCHAR">VARCHAR</option>
						<option value="BOOLEAN">BOOLEAN</option>
						<option value="TIMESTAMP">TIMESTAMP</option>
					</select>
				</div>
				<div>
					<label for="wr-time" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Time (epoch ms)</label>
					<input id="wr-time" bind:value={wrTime} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono" />
				</div>
				<div>
					<label for="wr-dim-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Dimension Name <span class="text-gray-400">(optional)</span></label>
					<input id="wr-dim-name" bind:value={wrDimensionName} placeholder="host" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
				<div>
					<label for="wr-dim-value" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Dimension Value</label>
					<input id="wr-dim-value" bind:value={wrDimensionValue} placeholder="host-1" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => writeRecordsModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitWriteRecords} disabled={writeRecordsBusy} class="px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
			{writeRecordsBusy ? 'Writing…' : 'Write'}
		</button>
	{/snippet}
</Modal>

<!-- Create Batch Load Task -->
<Modal bind:this={taskCreateModal} title="Create Batch Load Task">
	{#snippet children()}
		<div class="space-y-3">
			{#if taskCreateError}<p class="text-sm text-red-600 dark:text-red-400">{taskCreateError}</p>{/if}
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-task-db" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Target Database</label>
					<select id="new-task-db" bind:value={newTaskDbName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
						<option value="" disabled>Select…</option>
						{#each databases as d (d.DatabaseName)}
							<option value={d.DatabaseName}>{d.DatabaseName}</option>
						{/each}
					</select>
				</div>
				<div>
					<label for="new-task-table" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Target Table</label>
					<input id="new-task-table" bind:value={newTaskTableName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
				</div>
			</div>
			<div>
				<label for="new-task-bucket" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Source S3 Bucket</label>
				<input id="new-task-bucket" bind:value={newTaskBucket} placeholder="my-csv-bucket" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-task-prefix" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Source Object Key Prefix <span class="text-gray-400">(optional)</span></label>
				<input id="new-task-prefix" bind:value={newTaskPrefix} placeholder="imports/" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
			<div>
				<label for="new-task-report-bucket" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Error Report S3 Bucket</label>
				<input id="new-task-report-bucket" bind:value={newTaskReportBucket} placeholder="my-error-bucket" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => taskCreateModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateTask} disabled={taskCreating} class="px-4 py-2 rounded-lg bg-cyan-600 text-white text-sm font-medium hover:bg-cyan-700 disabled:opacity-50">
			{taskCreating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<!-- Batch Load Task Detail -->
<Modal bind:this={taskDetailModal} title="Batch Load Task Detail">
	{#snippet children()}
		{#if taskDetailLoading}
			<p class="text-sm text-gray-500 dark:text-gray-400">Loading…</p>
		{:else if taskDetail}
			<div class="space-y-2 text-sm">
				<div class="flex justify-between gap-2"><span class="text-gray-500">Task ID</span><span class="font-mono text-xs">{taskDetail.TaskId}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Status</span><span>{taskDetail.TaskStatus}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Target</span><span class="font-mono text-xs">{taskDetail.TargetDatabaseName}.{taskDetail.TargetTableName}</span></div>
				{#if taskDetail.ErrorMessage}
					<div class="flex justify-between gap-2"><span class="text-gray-500">Error</span><span class="text-red-600 dark:text-red-400 text-xs break-all">{taskDetail.ErrorMessage}</span></div>
				{/if}
				{#if taskDetail.ProgressReport}
					<div class="pt-2 border-t border-gray-100 dark:border-gray-800">
						<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-1">Progress</p>
						<div class="flex justify-between gap-2"><span class="text-gray-500">Records Processed</span><span>{taskDetail.ProgressReport.RecordsProcessed ?? 0}</span></div>
						<div class="flex justify-between gap-2"><span class="text-gray-500">Records Ingested</span><span>{taskDetail.ProgressReport.RecordsIngested ?? 0}</span></div>
						<div class="flex justify-between gap-2"><span class="text-gray-500">Parse Failures</span><span>{taskDetail.ProgressReport.ParseFailures ?? 0}</span></div>
						<div class="flex justify-between gap-2"><span class="text-gray-500">Record Ingestion Failures</span><span>{taskDetail.ProgressReport.RecordIngestionFailures ?? 0}</span></div>
					</div>
				{/if}
				<div class="flex justify-between gap-2"><span class="text-gray-500">Created</span><span>{formatDate(taskDetail.CreationTime)}</span></div>
				<div class="flex justify-between gap-2"><span class="text-gray-500">Last Updated</span><span>{formatDate(taskDetail.LastUpdatedTime)}</span></div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => taskDetailModal?.close()} class="px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>
