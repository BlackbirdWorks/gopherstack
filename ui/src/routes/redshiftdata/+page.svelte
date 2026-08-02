<script lang="ts">
	// Redshift Data API is a data-plane service, not a resource-management
	// one: there is no Create/Update/Delete for a "statement" the way there is
	// for, say, an RDS instance. A statement is *created* by running it
	// (ExecuteStatement/BatchExecuteStatement) and *terminated* by cancelling
	// it (CancelStatement) -- there is no update, and "delete" doesn't apply
	// to something that already ran. Catalogue introspection
	// (ListDatabases/ListSchemas/ListTables/DescribeTable) is read-only. So
	// this page is organized around the four real workflows instead of a
	// forced CRUD shape:
	//   - Query Console: submit SQL (ExecuteStatement/BatchExecuteStatement),
	//     watch it reach a terminal state (DescribeStatement), read results
	//     (GetStatementResult/GetStatementResultV2), cancel while in flight
	//     (CancelStatement).
	//   - Statement History: ListStatements.
	//   - Sessions: ListSessions -- a real, distinct operation the previous
	//     version of this page never called at all.
	//   - Catalog: ListDatabases -> ListSchemas -> ListTables -> DescribeTable.
	//
	// Backend reality check (services/redshiftdata): ExecuteStatement and
	// BatchExecuteStatement complete synchronously to Status=FINISHED within
	// the same call that creates the statement -- there is no real SQL engine
	// behind this, so results (GetStatementResult/GetStatementResultV2) and
	// catalogue contents (ListDatabases/ListSchemas/ListTables/DescribeTable)
	// are deterministic mock data, not a reflection of whatever SQL was
	// submitted. Because completion is synchronous, CancelStatement is real
	// code but is always called against an already-terminal statement in
	// practice, so it deterministically 400s with ValidationException
	// (ErrTerminalState) -- this page still wires it up for real (matching
	// real AWS "you can only cancel a running query"), it just won't ever
	// succeed against this backend. Nothing here pretends the mock executes
	// arbitrary SQL against real data.
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getRedshiftDataClient } from '$lib/aws-client';
	import {
		ExecuteStatementCommand,
		BatchExecuteStatementCommand,
		DescribeStatementCommand,
		DescribeTableCommand,
		GetStatementResultCommand,
		GetStatementResultV2Command,
		ListStatementsCommand,
		ListSessionsCommand,
		ListDatabasesCommand,
		ListSchemasCommand,
		ListTablesCommand,
		CancelStatementCommand,
		type StatementData,
		type SessionData,
		type TableMember,
		type ColumnMetadata,
		type DescribeStatementCommandOutput
	} from '@aws-sdk/client-redshift-data';
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
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { Database, Play, XCircle, Clock, Table2, Copy, Download, Layers, Users, Eye } from 'lucide-svelte';

	const client = regionalClient(getRedshiftDataClient);

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

	// Replaces the JSON.stringify(x) antipattern: search only over the named
	// fields that are actually meaningful for each resource.
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	// ==================== Connection settings (persisted) ====================
	//
	// Database/WorkgroupName/ClusterIdentifier/DbUser/SecretArn are shared
	// identity/target fields across ExecuteStatement, BatchExecuteStatement,
	// ListDatabases, ListSchemas, ListTables, and DescribeTable. Database is
	// a documented "This member is required" field on the four catalogue
	// ops (confirmed against the installed SDK's ListDatabasesRequest /
	// ListSchemasRequest / ListTablesRequest / DescribeTableRequest, and
	// enforced server-side per services/redshiftdata/PARITY.md) -- the
	// Catalog tab below will not call AWS at all until it is filled in,
	// rather than firing a guaranteed ValidationException on first tab load.

	const LS_PREFIX = 'gopherstack_redshiftdata_';
	function lsGet(key: string, fallback: string): string {
		try {
			return localStorage.getItem(LS_PREFIX + key) ?? fallback;
		} catch {
			return fallback;
		}
	}
	function lsSet(key: string, value: string): void {
		try {
			localStorage.setItem(LS_PREFIX + key, value);
		} catch {
			// storage unavailable
		}
	}

	let database = $state(lsGet('database', ''));
	let workgroupName = $state(lsGet('workgroupName', ''));
	let clusterIdentifier = $state(lsGet('clusterIdentifier', ''));
	let dbUser = $state(lsGet('dbUser', ''));
	let secretArn = $state(lsGet('secretArn', ''));

	$effect(() => { lsSet('database', database); });
	$effect(() => { lsSet('workgroupName', workgroupName); });
	$effect(() => { lsSet('clusterIdentifier', clusterIdentifier); });
	$effect(() => { lsSet('dbUser', dbUser); });
	$effect(() => { lsSet('secretArn', secretArn); });

	function connectionParams() {
		return {
			WorkgroupName: workgroupName.trim() || undefined,
			ClusterIdentifier: clusterIdentifier.trim() || undefined,
			DbUser: dbUser.trim() || undefined,
			SecretArn: secretArn.trim() || undefined
		};
	}

	type TabId = 'query' | 'history' | 'sessions' | 'catalog';

	const tabs: TabDef[] = [
		{ id: 'query', label: 'Query Console' },
		{ id: 'history', label: 'Statement History' },
		{ id: 'sessions', label: 'Sessions' },
		{ id: 'catalog', label: 'Catalog' }
	];

	let activeTab = $state<TabId>('query');
	let searchQuery = $state('');

	// ==================== Query Console ====================
	//
	// ExecuteStatementOutput/BatchExecuteStatementOutput do not include
	// Status (it's an optional field this backend never populates on the
	// initial response -- confirmed in services/redshiftdata/handler_statements.go,
	// handleExecuteStatement/handleBatchExecuteStatement only ever set
	// Id/ClusterIdentifier/WorkgroupName/Database/DbUser/SecretArn/CreatedAt/
	// SessionId), so a real client always needs at least one DescribeStatement
	// call to learn the outcome -- this polling loop models that honestly
	// even though the backend has, by the time ExecuteStatement returns,
	// already finished the statement internally.

	let sqlText = $state('SELECT 1;');
	let batchMode = $state(false);
	let batchSqls = $state('SELECT 1;\nSELECT 2;');
	let statementName = $state(lsGet('statementName', ''));
	let withEvent = $state(lsGet('withEvent', 'false') === 'true');
	let sessionIdInput = $state('');

	$effect(() => { lsSet('statementName', statementName); });
	$effect(() => { lsSet('withEvent', String(withEvent)); });

	let executing = $state(false);
	let currentStatementId = $state<string | null>(null);
	let statementDetail = $state<DescribeStatementCommandOutput | null>(null);
	let cancelling = $state(false);

	let resultColumns = $state<ColumnMetadata[]>([]);
	let resultRows = $state<string[][]>([]);
	let resultNextToken = $state<string | undefined>();
	let loadingMoreResults = $state(false);
	let resultTotalRows = $state<number | undefined>();

	let resultV2Records = $state<string[]>([]);
	let resultV2NextToken = $state<string | undefined>();
	let showResultV2 = $state(false);
	let loadingResultV2 = $state(false);

	// nanoseconds, per DescribeStatementResponse.Duration's real shape
	// (services/redshiftdata/PARITY.md's 2026-07-13 pass: this field was
	// milliseconds in gopherstack until fixed to match AWS docs). Rendering
	// it as milliseconds without converting -- which the previous version of
	// this page did (`formatDuration(resp.Duration)` fed nanoseconds
	// straight into a ms formatter) -- would show a query that ran in
	// microseconds as if it took the age of the universe; convert first.
	function formatDurationNanos(ns: number | undefined): string {
		if (ns === undefined) return '—';
		const ms = ns / 1_000_000;
		if (ms < 1000) return `${ms.toFixed(1)}ms`;
		return `${(ms / 1000).toFixed(2)}s`;
	}

	function statusBadgeClass(status: string | undefined): string {
		switch (status) {
			case 'FINISHED':
			case 'AVAILABLE':
				return 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300';
			case 'FAILED':
			case 'ABORTED':
				return 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300';
			case 'STARTED':
			case 'SUBMITTED':
			case 'PICKED':
			case 'BUSY':
				return 'bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300';
			case 'CLOSED':
				return 'bg-gray-200 text-gray-700 dark:bg-gray-700 dark:text-gray-300';
			default:
				return 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300';
		}
	}

	function truncateSql(sql: string | undefined, max = 120): string {
		if (!sql) return '—';
		return sql.length > max ? sql.slice(0, max) + '…' : sql;
	}

	async function copyToClipboard(text: string, label = 'Copied'): Promise<void> {
		try {
			await navigator.clipboard.writeText(text);
			toast.success(label);
		} catch {
			toast.error('Clipboard access denied');
		}
	}

	function resetQueryState(): void {
		statementDetail = null;
		currentStatementId = null;
		resultColumns = [];
		resultRows = [];
		resultNextToken = undefined;
		resultTotalRows = undefined;
		resultV2Records = [];
		resultV2NextToken = undefined;
		showResultV2 = false;
	}

	async function executeStatement(): Promise<void> {
		if (!database.trim()) {
			toast.error('Database is required');
			return;
		}
		const activeSql = batchMode ? batchSqls.trim() : sqlText.trim();
		if (!activeSql) return;

		executing = true;
		resetQueryState();

		try {
			const shared = {
				Database: database.trim(),
				...connectionParams(),
				StatementName: statementName.trim() || undefined,
				WithEvent: withEvent,
				SessionId: sessionIdInput.trim() || undefined
			};
			const resp = batchMode
				? await client().send(
						new BatchExecuteStatementCommand({
							Sqls: batchSqls.split(';').map((s) => s.trim()).filter(Boolean),
							...shared
						})
					)
				: await client().send(new ExecuteStatementCommand({ Sql: activeSql, ...shared }));
			currentStatementId = resp.Id ?? null;
			toast.success('Statement submitted');
			if (currentStatementId) {
				await pollStatement(currentStatementId);
			}
		} catch (e) {
			toast.error(describeError(e));
			executing = false;
		}
	}

	// Exponential back-off polling (100ms -> 200ms -> 400ms -> ... capped at
	// 4s). The backend finishes synchronously, so in practice the very first
	// DescribeStatement call already observes a terminal status -- this loop
	// is what a client talking to a real, asynchronous Redshift cluster
	// would need to do, and is kept so behavior doesn't silently depend on
	// the mock's synchronicity.
	async function pollStatement(id: string): Promise<void> {
		let attempts = 0;
		let delay = 100;

		const poll = async (): Promise<void> => {
			attempts++;
			try {
				const resp = await client().send(new DescribeStatementCommand({ Id: id }));
				statementDetail = resp;

				if (resp.Status === 'FINISHED' || resp.Status === 'FAILED' || resp.Status === 'ABORTED') {
					executing = false;
					if (resp.Status === 'FINISHED') {
						toast.success(`Statement finished${resp.Duration === undefined ? '' : ` in ${formatDurationNanos(resp.Duration)}`}`);
						if (resp.HasResultSet) {
							await loadResults(id, true);
						}
					} else {
						toast.error(`Statement ${resp.Status.toLowerCase()}${resp.Error ? ': ' + resp.Error : ''}`);
					}
					if (tabLoader.isLoaded('history')) await tabLoader.refresh('history');
				} else if (attempts > 60) {
					executing = false;
					toast.warning('Statement is taking long. Check Statement History for status.');
				} else {
					delay = Math.min(delay * 2, 4000);
					setTimeout(poll, delay);
				}
			} catch (e) {
				executing = false;
				toast.error(describeError(e));
			}
		};
		await poll();
	}

	async function loadResults(id: string, reset: boolean): Promise<void> {
		const resp = await client().send(
			new GetStatementResultCommand({ Id: id, NextToken: reset ? undefined : resultNextToken })
		);
		resultColumns = resp.ColumnMetadata ?? [];
		resultTotalRows = resp.TotalNumRows;
		const rows = (resp.Records ?? []).map((row) =>
			row.map((field) => {
				if (field.isNull) return 'NULL';
				if (field.stringValue !== undefined) return field.stringValue;
				if (field.longValue !== undefined) return String(field.longValue);
				if (field.doubleValue !== undefined) return String(field.doubleValue);
				if (field.booleanValue !== undefined) return String(field.booleanValue);
				if (field.blobValue !== undefined) return '<blob>';
				return '';
			})
		);
		resultRows = reset ? rows : [...resultRows, ...rows];
		resultNextToken = resp.NextToken;
	}

	async function loadMoreResults(): Promise<void> {
		if (!currentStatementId || !resultNextToken) return;
		loadingMoreResults = true;
		try {
			await loadResults(currentStatementId, false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreResults = false;
		}
	}

	async function toggleResultV2(): Promise<void> {
		if (!currentStatementId) return;
		showResultV2 = !showResultV2;
		if (!showResultV2) return;
		loadingResultV2 = true;
		try {
			const resp = await client().send(new GetStatementResultV2Command({ Id: currentStatementId }));
			resultV2Records = (resp.Records ?? []).map((r) => ('CSVRecords' in r ? (r.CSVRecords as string) : JSON.stringify(r)));
			resultV2NextToken = resp.NextToken;
		} catch (e) {
			toast.error(describeError(e));
			showResultV2 = false;
		} finally {
			loadingResultV2 = false;
		}
	}

	async function loadMoreResultV2(): Promise<void> {
		if (!currentStatementId || !resultV2NextToken) return;
		loadingResultV2 = true;
		try {
			const resp = await client().send(
				new GetStatementResultV2Command({ Id: currentStatementId, NextToken: resultV2NextToken })
			);
			resultV2Records = [
				...resultV2Records,
				...(resp.Records ?? []).map((r) => ('CSVRecords' in r ? (r.CSVRecords as string) : JSON.stringify(r)))
			];
			resultV2NextToken = resp.NextToken;
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingResultV2 = false;
		}
	}

	// Real AWS: "To be canceled, a query must be running." This backend
	// finishes every statement synchronously inside ExecuteStatement/
	// BatchExecuteStatement, so by the time this can be clicked the
	// statement is already terminal server-side and CancelStatement
	// deterministically returns ValidationException (see
	// services/redshiftdata/PARITY.md's CancelStatement gap) -- that failure
	// is real and is surfaced through the same error banner as any other
	// call, not swallowed or faked into a success.
	async function cancelStatement(): Promise<void> {
		if (!currentStatementId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel statement',
			message: `Cancel statement ${currentStatementId}?`,
			confirmLabel: 'Cancel statement',
			dangerous: false
		});
		if (!confirmed) return;
		cancelling = true;
		try {
			await client().send(new CancelStatementCommand({ Id: currentStatementId }));
			toast.success('Cancel requested');
			statementDetail = await client().send(new DescribeStatementCommand({ Id: currentStatementId }));
			executing = statementDetail.Status === 'SUBMITTED' || statementDetail.Status === 'PICKED' || statementDetail.Status === 'STARTED';
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			cancelling = false;
		}
	}

	function exportResultsCsv(): void {
		if (resultColumns.length === 0) return;
		const header = resultColumns.map((c) => c.name ?? c.label ?? '').join(',');
		const lines = [header];
		for (const row of resultRows) {
			lines.push(row.map((c) => `"${String(c).replaceAll('"', '""')}"`).join(','));
		}
		const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `redshiftdata-results-${currentStatementId ?? 'export'}.csv`;
		a.click();
		URL.revokeObjectURL(url);
	}

	function handleKeydown(e: KeyboardEvent): void {
		if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
			e.preventDefault();
			if (!executing && (batchMode ? batchSqls.trim() : sqlText.trim()) && database.trim()) {
				executeStatement();
			}
		} else if (e.key === 'Escape' && executing && currentStatementId) {
			e.preventDefault();
			cancelStatement();
		}
	}

	// ==================== Statement History (ListStatements) ====================
	//
	// Default (Status omitted) returns only FINISHED statements per
	// services/redshiftdata/PARITY.md's documented default-filter behavior
	// -- the status select below defaults to empty/"(default: FINISHED)" to
	// match, rather than silently hiding that AWS-documented default from
	// the user.

	let historyStatements = $state<StatementData[]>([]);
	let historyNextToken = $state<string | undefined>();
	let loadingMoreHistory = $state(false);
	let historyStatusFilter = $state('');

	async function fetchHistory(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListStatementsCommand({
				...connectionParams(),
				Database: database.trim() || undefined,
				Status: (historyStatusFilter || undefined) as StatementData['Status'],
				NextToken: reset ? undefined : historyNextToken
			})
		);
		historyStatements = reset ? (resp.Statements ?? []) : [...historyStatements, ...(resp.Statements ?? [])];
		historyNextToken = resp.NextToken;
	}

	async function loadMoreHistory(): Promise<void> {
		loadingMoreHistory = true;
		try {
			await fetchHistory(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreHistory = false;
		}
	}

	function handleHistoryStatusChange(): void {
		tabLoader.refresh('history');
	}

	function reuseStatement(stmt: StatementData): void {
		if (stmt.IsBatchStatement) {
			batchMode = true;
			batchSqls = (stmt.QueryStrings ?? []).join(';\n');
		} else {
			batchMode = false;
			sqlText = stmt.QueryString ?? '';
		}
		if (stmt.SessionId) sessionIdInput = stmt.SessionId;
		activeTab = 'query';
	}

	let historyDetailModal = $state<Modal | null>(null);
	let viewedStatement = $state<DescribeStatementCommandOutput | null>(null);

	async function openStatementDetail(stmt: StatementData): Promise<void> {
		if (!stmt.Id) return;
		viewedStatement = null;
		historyDetailModal?.open();
		try {
			viewedStatement = await client().send(new DescribeStatementCommand({ Id: stmt.Id }));
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ==================== Sessions (ListSessions) ====================
	//
	// New tab: the previous version of this page never called ListSessions
	// at all, despite it being one of the 12 operations
	// GetSupportedOperations() returns and a real, current SDK command
	// (confirmed present in both the installed @aws-sdk/client-redshift-data
	// command list and services/redshiftdata/handler.go's dispatch table).
	// Per services/redshiftdata/PARITY.md: Status here is always AVAILABLE
	// in practice (BUSY is unreachable because statements complete
	// synchronously, CLOSED is unreachable because session TTL isn't
	// tracked) -- this page doesn't invent a BUSY/CLOSED demo state to make
	// the filter look more useful than it is.

	let sessions = $state<SessionData[]>([]);
	let sessionsNextToken = $state<string | undefined>();
	let loadingMoreSessions = $state(false);
	let sessionsStatusFilter = $state('');

	async function fetchSessions(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListSessionsCommand({
				Status: (sessionsStatusFilter || undefined) as SessionData['Status'],
				NextToken: reset ? undefined : sessionsNextToken
			})
		);
		sessions = reset ? (resp.Sessions ?? []) : [...sessions, ...(resp.Sessions ?? [])];
		sessionsNextToken = resp.NextToken;
	}

	async function loadMoreSessions(): Promise<void> {
		loadingMoreSessions = true;
		try {
			await fetchSessions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSessions = false;
		}
	}

	function handleSessionsStatusChange(): void {
		tabLoader.refresh('sessions');
	}

	function useSession(session: SessionData): void {
		if (session.SessionId) sessionIdInput = session.SessionId;
		if (session.Database) database = session.Database;
		activeTab = 'query';
		toast.success(`Session ${session.SessionId} set in Query Console`);
	}

	// ==================== Catalog (ListDatabases / ListSchemas / ListTables / DescribeTable) ====================

	let catalogDatabases = $state<string[]>([]);
	let catalogDatabasesNextToken = $state<string | undefined>();
	let loadingMoreCatalogDatabases = $state(false);

	let catalogSchemas = $state<string[]>([]);
	let catalogSchemasNextToken = $state<string | undefined>();
	let loadingMoreCatalogSchemas = $state(false);
	let selectedSchema = $state<string | null>(null);

	let catalogTables = $state<TableMember[]>([]);
	let catalogTablesNextToken = $state<string | undefined>();
	let loadingMoreCatalogTables = $state(false);

	async function fetchCatalog(): Promise<void> {
		catalogDatabases = [];
		catalogSchemas = [];
		catalogTables = [];
		selectedSchema = null;
		if (!database.trim()) return;

		const dbResp = await client().send(new ListDatabasesCommand({ Database: database.trim(), ...connectionParams() }));
		catalogDatabases = dbResp.Databases ?? [];
		catalogDatabasesNextToken = dbResp.NextToken;

		const schemaResp = await client().send(
			new ListSchemasCommand({ Database: database.trim(), ...connectionParams(), SchemaPattern: '%' })
		);
		catalogSchemas = schemaResp.Schemas ?? [];
		catalogSchemasNextToken = schemaResp.NextToken;

		await fetchCatalogTables(true);
	}

	async function fetchCatalogTables(reset: boolean): Promise<void> {
		if (!database.trim()) return;
		const resp = await client().send(
			new ListTablesCommand({
				Database: database.trim(),
				...connectionParams(),
				SchemaPattern: selectedSchema ?? '%',
				TablePattern: '%',
				NextToken: reset ? undefined : catalogTablesNextToken
			})
		);
		catalogTables = reset ? (resp.Tables ?? []) : [...catalogTables, ...(resp.Tables ?? [])];
		catalogTablesNextToken = resp.NextToken;
	}

	async function loadMoreCatalogDatabases(): Promise<void> {
		if (!catalogDatabasesNextToken) return;
		loadingMoreCatalogDatabases = true;
		try {
			const resp = await client().send(
				new ListDatabasesCommand({ Database: database.trim(), ...connectionParams(), NextToken: catalogDatabasesNextToken })
			);
			catalogDatabases = [...catalogDatabases, ...(resp.Databases ?? [])];
			catalogDatabasesNextToken = resp.NextToken;
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCatalogDatabases = false;
		}
	}

	async function loadMoreCatalogSchemas(): Promise<void> {
		if (!catalogSchemasNextToken) return;
		loadingMoreCatalogSchemas = true;
		try {
			const resp = await client().send(
				new ListSchemasCommand({
					Database: database.trim(),
					...connectionParams(),
					SchemaPattern: '%',
					NextToken: catalogSchemasNextToken
				})
			);
			catalogSchemas = [...catalogSchemas, ...(resp.Schemas ?? [])];
			catalogSchemasNextToken = resp.NextToken;
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCatalogSchemas = false;
		}
	}

	async function loadMoreCatalogTables(): Promise<void> {
		loadingMoreCatalogTables = true;
		try {
			await fetchCatalogTables(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCatalogTables = false;
		}
	}

	function selectSchema(schema: string): void {
		selectedSchema = selectedSchema === schema ? null : schema;
		fetchCatalogTables(true).catch((e) => toast.error(describeError(e)));
	}

	function queryTable(t: TableMember): void {
		sqlText = `SELECT * FROM "${t.schema}"."${t.name}" LIMIT 10;`;
		batchMode = false;
		activeTab = 'query';
	}

	let tableColumnsModal = $state<Modal | null>(null);
	let viewedTable = $state<TableMember | null>(null);
	let viewedTableColumns = $state<ColumnMetadata[]>([]);
	let viewedTableColumnsLoading = $state(false);

	async function openTableColumns(t: TableMember): Promise<void> {
		viewedTable = t;
		viewedTableColumns = [];
		viewedTableColumnsLoading = true;
		tableColumnsModal?.open();
		try {
			const resp = await client().send(
				new DescribeTableCommand({
					Database: database.trim(),
					...connectionParams(),
					Schema: t.schema,
					Table: t.name
				})
			);
			viewedTableColumns = resp.ColumnList ?? [];
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			viewedTableColumnsLoading = false;
		}
	}

	// ==================== Tab loader ====================

	const tabLoader = createTabLoader<TabId>({
		// The Query Console has nothing to bootstrap-fetch: results only
		// appear once a statement is executed from within the tab itself.
		query: () => Promise.resolve(),
		history: () => fetchHistory(true).catch(rethrowDescribed),
		sessions: () => fetchSessions(true).catch(rethrowDescribed),
		catalog: () => fetchCatalog().catch(rethrowDescribed)
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
		tabLoader.refresh(activeTab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredHistory = $derived(
		historyStatements.filter((s) => matches(searchQuery, s.Id, s.QueryString, s.StatementName, s.Status, s.SessionId))
	);
	const filteredSessions = $derived(
		sessions.filter((s) =>
			matches(searchQuery, s.SessionId, s.Database, s.DbUser, s.ClusterIdentifier, s.WorkgroupName, s.Status)
		)
	);
	const filteredTables = $derived(catalogTables.filter((t) => matches(searchQuery, t.name, t.schema, t.type)));
</script>

<svelte:window on:keydown={handleKeydown} />

<div class="p-6 space-y-6">
	<PageHeader
		icon={Database}
		title="Redshift Data"
		description="Run SQL statements against Amazon Redshift, browse statement history and sessions, and inspect the catalogue."
		onRefresh={handleRefresh}
		color="blue"
	/>

	<!-- Connection settings -->
	<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-5 gap-3 text-sm">
		<div>
			<label for="rd-database" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Database *</label>
			<input id="rd-database" bind:value={database} type="text" placeholder="dev" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
		<div>
			<label for="rd-workgroup" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Workgroup Name</label>
			<input id="rd-workgroup" bind:value={workgroupName} type="text" placeholder="default (serverless)" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
		<div>
			<label for="rd-cluster" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Cluster Identifier</label>
			<input id="rd-cluster" bind:value={clusterIdentifier} type="text" placeholder="my-cluster (provisioned)" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
		<div>
			<label for="rd-dbuser" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">DB User</label>
			<input id="rd-dbuser" bind:value={dbUser} type="text" placeholder="admin" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
		<div>
			<label for="rd-secret" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Secret ARN</label>
			<input id="rd-secret" bind:value={secretArn} type="text" placeholder="arn:aws:secretsmanager:…" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
	</div>

	<div class="flex flex-col sm:flex-row gap-3 justify-between">
		<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
		{#if activeTab !== 'query'}
			<SearchInput bind:value={searchQuery} />
		{/if}
	</div>

	{#if activeTabError}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load data</p>
			<p>{activeTabError}</p>
		</div>
	{/if}

	<!-- ==================== QUERY CONSOLE ==================== -->
	{#if activeTab === 'query'}
		<div class="space-y-4">
			<div class="grid grid-cols-1 sm:grid-cols-3 gap-3 text-sm">
				<div>
					<label for="rd-stmtname" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Statement Name</label>
					<input id="rd-stmtname" bind:value={statementName} type="text" placeholder="optional label" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
				</div>
				<div>
					<label for="rd-sessionid" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Session ID <span class="text-gray-400">(optional)</span></label>
					<input id="rd-sessionid" bind:value={sessionIdInput} type="text" placeholder="run within an existing session" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono" />
				</div>
				<div class="flex items-end pb-2">
					<label class="flex items-center gap-2 text-xs text-gray-600 dark:text-gray-400">
						<input bind:checked={withEvent} type="checkbox" class="w-4 h-4 rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500" />
						WithEvent — emit an EventBridge event on completion
					</label>
				</div>
			</div>

			<div class="flex items-center justify-between">
				<div class="flex items-center gap-3">
					<label for={batchMode ? 'rd-sql-batch' : 'rd-sql'} class="block text-sm font-semibold text-gray-700 dark:text-gray-300">
						{batchMode ? 'Batch SQL (semicolon-separated)' : 'SQL Statement'}
						<span class="ml-2 text-xs font-normal text-gray-400">(Ctrl+Enter to run · Escape to cancel)</span>
					</label>
					<button
						onclick={() => (batchMode = !batchMode)}
						aria-label="Toggle batch mode"
						class={`flex items-center gap-1 px-2 py-1 rounded-lg text-xs border transition-colors ${batchMode ? 'bg-purple-100 text-purple-700 border-purple-300 dark:bg-purple-900/40 dark:text-purple-300 dark:border-purple-700' : 'border-gray-200 dark:border-gray-700 text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
					>
						<Layers class="w-3.5 h-3.5" />
						{batchMode ? 'Batch ON' : 'Batch OFF'}
					</button>
				</div>
				{#if executing}
					<button onclick={cancelStatement} disabled={cancelling} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium disabled:opacity-50">
						<XCircle class="w-4 h-4" /> {cancelling ? 'Cancelling…' : 'Cancel'}
					</button>
				{:else}
					<button
						onclick={executeStatement}
						disabled={!database.trim() || !(batchMode ? batchSqls.trim() : sqlText.trim())}
						class="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm font-medium disabled:opacity-50"
					>
						<Play class="w-4 h-4" /> Run Query
					</button>
				{/if}
			</div>

			{#if batchMode}
				<textarea id="rd-sql-batch" bind:value={batchSqls} rows={8} aria-label="Batch SQL" class="w-full px-4 py-3 rounded-xl border border-purple-300 dark:border-purple-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-y" placeholder="SELECT 1;&#10;SELECT 2;"></textarea>
			{:else}
				<textarea id="rd-sql" bind:value={sqlText} rows={8} aria-label="SQL Query" class="w-full px-4 py-3 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-y" placeholder="-- Example: SELECT * FROM public.my_table LIMIT 10;"></textarea>
			{/if}

			{#if statementDetail}
				<div class={`flex items-start gap-3 p-4 rounded-xl border ${statementDetail.Status === 'FINISHED' ? 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800' : statementDetail.Status === 'FAILED' || statementDetail.Status === 'ABORTED' ? 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800' : 'bg-blue-50 dark:bg-blue-900/20 border-blue-200 dark:border-blue-800'}`}>
					{#if executing}
						<div class="animate-spin w-4 h-4 mt-0.5 border-2 border-blue-600 border-t-transparent rounded-full shrink-0"></div>
					{/if}
					<div class="flex-1 min-w-0">
						<div class="flex flex-wrap items-center gap-2">
							<span class={`text-sm font-medium px-2 py-0.5 rounded ${statusBadgeClass(statementDetail.Status)}`}>{statementDetail.Status}</span>
							{#if statementDetail.Duration !== undefined}
								<span class="text-xs text-gray-500">{formatDurationNanos(statementDetail.Duration)}</span>
							{/if}
							{#if currentStatementId}
								<button onclick={() => copyToClipboard(currentStatementId!, 'Statement ID copied')} class="flex items-center gap-1 text-xs font-mono text-gray-500 hover:text-gray-700 dark:hover:text-gray-300">
									<Copy class="w-3 h-3" />{currentStatementId}
								</button>
							{/if}
						</div>
						{#if statementDetail.Error}
							<p class="mt-1 text-sm text-red-600 dark:text-red-400 break-words">{statementDetail.Error}</p>
						{/if}
					</div>
				</div>
			{/if}

			{#if resultColumns.length > 0 || resultRows.length > 0}
				<div>
					<div class="flex items-center justify-between mb-2">
						<span class="text-sm font-semibold text-gray-700 dark:text-gray-300">
							Results
							<span class="ml-1 text-xs font-normal text-gray-400">
								({resultRows.length} row{resultRows.length === 1 ? '' : 's'}{resultTotalRows !== undefined ? ` of ${resultTotalRows}` : ''}, {resultColumns.length} col{resultColumns.length === 1 ? '' : 's'})
							</span>
						</span>
						<div class="flex items-center gap-2">
							<button
								onclick={toggleResultV2}
								disabled={loadingResultV2}
								title="Fetch results in CSV format via GetStatementResultV2"
								class={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-xs transition-colors ${showResultV2 ? 'bg-purple-100 dark:bg-purple-900/40 border-purple-300 dark:border-purple-700 text-purple-700 dark:text-purple-300' : 'border-gray-200 dark:border-gray-700 text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
							>
								<Layers class="w-3.5 h-3.5" /> V2 CSV
							</button>
							<button onclick={exportResultsCsv} class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 text-xs text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800">
								<Download class="w-3.5 h-3.5" /> Export CSV
							</button>
						</div>
					</div>
					{#if showResultV2}
						<div class="overflow-auto max-h-96 rounded-xl border border-purple-200 dark:border-purple-700 bg-purple-50 dark:bg-purple-900/20 text-xs text-gray-700 dark:text-gray-300 p-4 font-mono space-y-1">
							{#each resultV2Records as rec, i (i)}
								<div>{rec}</div>
							{:else}
								<p class="text-gray-400 italic">No CSV records.</p>
							{/each}
						</div>
						<LoadMore hasMore={!!resultV2NextToken} loading={loadingResultV2} onLoadMore={loadMoreResultV2} />
					{:else}
						<div class="overflow-auto max-h-96 rounded-xl border border-gray-200 dark:border-gray-700">
							<table class="w-full text-sm">
								<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase sticky top-0">
									<tr>
										{#each resultColumns as col, i (i)}
											<th class="px-4 py-3 text-left whitespace-nowrap" title={col.typeName ?? ''}>
												{col.name ?? col.label ?? ''}
												{#if col.typeName}<span class="ml-1 text-gray-400 normal-case font-normal">({col.typeName})</span>{/if}
											</th>
										{/each}
									</tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
									{#each resultRows as row, i (i)}
										<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
											{#each row as cell, j (j)}
												<td class={`px-4 py-2 whitespace-nowrap font-mono text-xs ${cell === 'NULL' ? 'text-gray-400 dark:text-gray-500 italic' : ''}`}>{cell}</td>
											{/each}
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
						<LoadMore hasMore={!!resultNextToken} loading={loadingMoreResults} onLoadMore={loadMoreResults} />
					{/if}
				</div>
			{:else if statementDetail?.Status === 'FINISHED'}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400 text-sm">
					<Table2 class="w-10 h-10 mx-auto mb-2 opacity-40" />
					<p>Statement finished with no result set.</p>
					<p class="text-xs mt-1 text-gray-400">DML statements (INSERT, UPDATE, DELETE) do not return rows.</p>
				</div>
			{:else if !statementDetail}
				<div class="text-center py-10 text-gray-400 text-sm border border-dashed border-gray-200 dark:border-gray-700 rounded-xl">
					<Play class="w-8 h-8 mx-auto mb-2 opacity-30" />
					<p>Run a query to see results here</p>
					<p class="text-xs mt-1">Tip: use <code class="bg-gray-100 dark:bg-gray-800 px-1 rounded">SELECT 1</code> to test connectivity</p>
				</div>
			{/if}
		</div>

	<!-- ==================== STATEMENT HISTORY ==================== -->
	{:else if activeTab === 'history'}
		<div class="flex items-center justify-between gap-3">
			<span class="text-sm text-gray-600 dark:text-gray-400">{filteredHistory.length} statement{filteredHistory.length === 1 ? '' : 's'}</span>
			<select bind:value={historyStatusFilter} onchange={handleHistoryStatusChange} aria-label="Filter by status" class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-gray-700 dark:text-gray-300">
				<option value="">All statuses (default: FINISHED)</option>
				<option value="ALL">ALL</option>
				<option value="FINISHED">FINISHED</option>
				<option value="FAILED">FAILED</option>
				<option value="ABORTED">ABORTED</option>
				<option value="STARTED">STARTED</option>
				<option value="SUBMITTED">SUBMITTED</option>
				<option value="PICKED">PICKED</option>
			</select>
		</div>

		{#snippet histQueryCell(s: StatementData)}
			<code class="text-xs font-mono" title={s.QueryString ?? (s.QueryStrings ?? []).join('; ')}>{s.IsBatchStatement ? `(batch: ${s.QueryStrings?.length ?? 0} statements)` : truncateSql(s.QueryString)}</code>
		{/snippet}
		{#snippet histStatusCell(s: StatementData)}
			<span class={`text-xs px-2 py-1 rounded-full ${statusBadgeClass(s.Status)}`}>{s.Status ?? '—'}</span>
		{/snippet}
		{#snippet histCreatedCell(s: StatementData)}
			{formatDate(s.CreatedAt)}
		{/snippet}
		{#snippet histActionsCell(s: StatementData)}
			<div class="flex items-center gap-2 justify-end">
				<button onclick={() => openStatementDetail(s)} title="View" aria-label="View statement {s.Id}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
				<button onclick={() => reuseStatement(s)} title="Reuse in Query Console" aria-label="Reuse statement {s.Id}" class="text-xs text-blue-600 dark:text-blue-400 hover:underline">Reuse</button>
			</div>
		{/snippet}
		{@const historyColumns = defineColumns<StatementData>([
			{ key: 'Id', label: 'ID' },
			{ key: 'QueryString', label: 'Query', render: histQueryCell },
			{ key: 'Status', label: 'Status', render: histStatusCell },
			{ key: 'CreatedAt', label: 'Created', render: histCreatedCell },
			{ key: 'actions', label: '', render: histActionsCell }
		])}
		<DataTable
			rows={filteredHistory}
			rowKey={(s) => s.Id ?? ''}
			columns={historyColumns}
			loading={tabLoader.isLoading('history')}
			emptyMessage="No statements found"
		/>
		<LoadMore hasMore={!!historyNextToken} loading={loadingMoreHistory} onLoadMore={loadMoreHistory} />

	<!-- ==================== SESSIONS ==================== -->
	{:else if activeTab === 'sessions'}
		<div class="flex items-center justify-between gap-3">
			<span class="text-sm text-gray-600 dark:text-gray-400">{filteredSessions.length} session{filteredSessions.length === 1 ? '' : 's'}</span>
			<select bind:value={sessionsStatusFilter} onchange={handleSessionsStatusChange} aria-label="Filter sessions by status" class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-gray-700 dark:text-gray-300">
				<option value="">All statuses (default: AVAILABLE, BUSY)</option>
				<option value="AVAILABLE">AVAILABLE</option>
				<option value="BUSY">BUSY</option>
				<option value="CLOSED">CLOSED</option>
			</select>
		</div>

		{#snippet sessStatusCell(s: SessionData)}
			<span class={`text-xs px-2 py-1 rounded-full ${statusBadgeClass(s.Status)}`}>{s.Status}</span>
		{/snippet}
		{#snippet sessTargetCell(s: SessionData)}
			<span class="font-mono text-xs">{s.ClusterIdentifier ?? s.WorkgroupName ?? '—'}</span>
		{/snippet}
		{#snippet sessCreatedCell(s: SessionData)}
			{formatDate(s.CreatedAt)}
		{/snippet}
		{#snippet sessUpdatedCell(s: SessionData)}
			{formatDate(s.UpdatedAt)}
		{/snippet}
		{#snippet sessActionsCell(s: SessionData)}
			<div class="flex items-center justify-end">
				<button onclick={() => useSession(s)} class="text-xs text-blue-600 dark:text-blue-400 hover:underline">Use in Query Console</button>
			</div>
		{/snippet}
		{@const sessionColumns = defineColumns<SessionData>([
			{ key: 'SessionId', label: 'Session ID' },
			{ key: 'Status', label: 'Status', render: sessStatusCell },
			{ key: 'Database', label: 'Database' },
			{ key: 'DbUser', label: 'DB User' },
			{ key: 'target', label: 'Cluster / Workgroup', render: sessTargetCell },
			{ key: 'CreatedAt', label: 'Created', render: sessCreatedCell },
			{ key: 'UpdatedAt', label: 'Updated', render: sessUpdatedCell },
			{ key: 'actions', label: '', render: sessActionsCell }
		])}
		<DataTable
			rows={filteredSessions}
			rowKey={(s) => s.SessionId ?? ''}
			columns={sessionColumns}
			loading={tabLoader.isLoading('sessions')}
			emptyMessage="No sessions found. Sessions only appear here once a statement is executed with an explicit Session ID."
		/>
		<LoadMore hasMore={!!sessionsNextToken} loading={loadingMoreSessions} onLoadMore={loadMoreSessions} />

	<!-- ==================== CATALOG ==================== -->
	{:else if activeTab === 'catalog'}
		{#if !database.trim()}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Database class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">Enter a Database name above, then Refresh</p>
				<p class="text-sm mt-1">Database is a required field on ListDatabases/ListSchemas/ListTables/DescribeTable.</p>
			</div>
		{:else}
			{#snippet tblActionsCell(t: TableMember)}
				<div class="flex items-center gap-3 justify-end">
					<button onclick={() => queryTable(t)} class="text-xs text-blue-600 dark:text-blue-400 hover:underline">Query</button>
					<button onclick={() => openTableColumns(t)} aria-label="Show columns for {t.name}" class="text-xs text-gray-500 hover:text-gray-700 dark:hover:text-gray-300">Columns</button>
				</div>
			{/snippet}
			{@const tableColumnsDef = defineColumns<TableMember>([
				{ key: 'schema', label: 'Schema' },
				{ key: 'name', label: 'Table' },
				{ key: 'type', label: 'Type' },
				{ key: 'actions', label: '', render: tblActionsCell }
			])}
			<div class="space-y-4">
				{#if catalogDatabases.length > 0}
					<div>
						<h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Databases</h3>
						<div class="flex flex-wrap gap-2 items-center">
							{#each catalogDatabases as db (db)}
								<span class="px-3 py-1 rounded-full bg-blue-100 text-blue-800 dark:bg-blue-900/40 dark:text-blue-300 text-xs font-medium">{db}</span>
							{/each}
						</div>
						<LoadMore hasMore={!!catalogDatabasesNextToken} loading={loadingMoreCatalogDatabases} onLoadMore={loadMoreCatalogDatabases} />
					</div>
				{/if}

				{#if catalogSchemas.length > 0}
					<div>
						<h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">Schemas</h3>
						<div class="flex flex-wrap gap-2">
							{#each catalogSchemas as schema (schema)}
								<button
									onclick={() => selectSchema(schema)}
									class={`px-3 py-1 rounded-full text-xs font-medium transition-colors ${selectedSchema === schema ? 'bg-blue-500 text-white' : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-gray-600'}`}
								>
									{schema}
								</button>
							{/each}
						</div>
						<LoadMore hasMore={!!catalogSchemasNextToken} loading={loadingMoreCatalogSchemas} onLoadMore={loadMoreCatalogSchemas} />
					</div>
				{/if}

				<div>
					<h3 class="text-xs font-semibold text-gray-500 uppercase tracking-wider mb-2">
						Tables ({filteredTables.length}){#if selectedSchema} in schema <strong>{selectedSchema}</strong>{/if}
					</h3>
					<DataTable
						rows={filteredTables}
						rowKey={(t) => `${t.schema}.${t.name}`}
						columns={tableColumnsDef}
						loading={tabLoader.isLoading('catalog')}
						emptyMessage="No tables found"
					/>
					<LoadMore hasMore={!!catalogTablesNextToken} loading={loadingMoreCatalogTables} onLoadMore={loadMoreCatalogTables} />
				</div>
			</div>
		{/if}
	{/if}
</div>

<!-- Statement detail modal -->
<Modal bind:this={historyDetailModal} title="Statement Details">
	{#snippet children()}
		{#if viewedStatement}
			<div class="space-y-3 text-sm max-h-[65vh] overflow-y-auto pr-1">
				{#each [
					['ID', viewedStatement.Id],
					['Status', viewedStatement.Status],
					['Database', viewedStatement.Database],
					['Cluster / Workgroup', viewedStatement.ClusterIdentifier ?? viewedStatement.WorkgroupName],
					['Session ID', viewedStatement.SessionId ?? '—'],
					['Duration', formatDurationNanos(viewedStatement.Duration)],
					['Result Rows', viewedStatement.ResultRows ?? '—'],
					['Has Result Set', String(viewedStatement.HasResultSet ?? false)],
					['Created', formatDate(viewedStatement.CreatedAt)],
					['Updated', formatDate(viewedStatement.UpdatedAt)]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
				{#if viewedStatement.Error}
					<p class="text-xs text-red-600 dark:text-red-400 font-mono break-all">{viewedStatement.Error}</p>
				{/if}
				<div>
					<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Query</h4>
					<code class="text-xs font-mono break-all block bg-gray-50 dark:bg-gray-800 rounded-lg p-2">{viewedStatement.QueryString ?? '—'}</code>
				</div>
				{#if viewedStatement.QueryParameters?.length}
					<div>
						<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Parameters</h4>
						{#each viewedStatement.QueryParameters as p, i (i)}
							<div class="text-xs font-mono text-gray-700 dark:text-gray-300">{p.name} = {p.value}</div>
						{/each}
					</div>
				{/if}
				{#if viewedStatement.SubStatements?.length}
					<div>
						<h4 class="text-xs font-semibold uppercase tracking-wide text-gray-500 mb-2">Sub-statements ({viewedStatement.SubStatements.length})</h4>
						{#each viewedStatement.SubStatements as sub, i (i)}
							<div class="text-xs font-mono text-gray-700 dark:text-gray-300 mb-1">{sub.Id}: {sub.Status} — {truncateSql(sub.QueryString, 80)}</div>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => historyDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Table columns modal -->
<Modal bind:this={tableColumnsModal} title={viewedTable ? `Columns — ${viewedTable.schema}.${viewedTable.name}` : 'Columns'}>
	{#snippet children()}
		{#if viewedTableColumnsLoading}
			<div class="flex justify-center py-8">
				<div class="animate-spin w-6 h-6 border-4 border-blue-600 border-t-transparent rounded-full"></div>
			</div>
		{:else if viewedTableColumns.length === 0}
			<p class="text-sm text-gray-400 italic">No columns.</p>
		{:else}
			<div class="space-y-1 max-h-[60vh] overflow-y-auto pr-1">
				{#each viewedTableColumns as col, i (i)}
					<div class="flex items-center justify-between text-xs font-mono border-b border-gray-100 dark:border-gray-800 py-1.5">
						<span class="text-gray-800 dark:text-gray-200">{col.name}</span>
						<span class="text-gray-400 flex items-center gap-2">
							{col.typeName}{col.length ? `(${col.length})` : ''}
							{#if col.nullable === 0}<span class="text-orange-400">NOT NULL</span>{/if}
						</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => tableColumnsModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>
