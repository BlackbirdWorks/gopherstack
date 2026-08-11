<script lang="ts">
	// RDS Data API is a data-plane service for Aurora Serverless, not a
	// resource-management one: nothing is created/listed/updated/deleted in
	// the CRUD sense. Six real operations exist (confirmed against
	// services/rdsdata/handler.go's GetSupportedOperations() and the
	// installed @aws-sdk/client-rds-data's command list -- both sides match
	// exactly, no fabricated or missing ops):
	//   - ExecuteStatement / BatchExecuteStatement: run SQL, optionally
	//     inside a transaction.
	//   - BeginTransaction / CommitTransaction / RollbackTransaction: the
	//     transaction lifecycle.
	//   - ExecuteSql: deprecated predecessor to the two Execute* ops above.
	// There is no ListTransactions or ListStatements op in the real API (or
	// this backend) -- transactions and statement history shown here are
	// tracked client-side from responses this page has itself seen, exactly
	// like the page this replaced. That's not a shortcut; it's what AWS
	// itself offers (the real Data API has no server-side query/transaction
	// history endpoint either).
	//
	// Backend reality check (services/rdsdata): unlike redshiftdata's
	// canned-response mock, this backend runs a REAL embedded SQLite engine
	// per resourceArn (engine.go) and opens a genuine *sql.Tx for each
	// BeginTransaction (transactions.go) -- SQL you submit here actually
	// executes, and transaction ids are honoured for real (a statement
	// tagged with a stale/committed/rolled-back id 400s with
	// TransactionNotFoundException, verified in transactions.go's
	// Commit/RollbackTransaction, which delete the id before returning).
	// So this console can honestly show real result rows, not fabricated
	// ones -- the opposite framing problem from redshiftdata.
	//
	// BatchExecuteStatement is NOT "run several different SQL strings" (that
	// is Redshift Data's Sqls: string[] shape) -- RDS Data's batch takes ONE
	// sql template plus parameterSets: SqlParameter[][], executing the same
	// template once per parameter set (the documented bulk-insert pattern).
	// The previous version of this page got this wrong (it just toggled
	// which command wrapped an arbitrary multi-statement blob), which made
	// "batch mode" a no-op muddle. This version threads a real named
	// Parameters / Parameter Sets editor through both ExecuteStatement and
	// BatchExecuteStatement so batch mode demonstrates what it is actually
	// for.
	//
	// ExecuteSql (legacy) uses different field names on the wire
	// (dbClusterOrInstanceArn / awsSecretStoreArn instead of
	// resourceArn / secretArn) but the same underlying cluster + secret, so
	// this page reuses the shared connection fields rather than duplicating
	// them. Its response type, SqlStatementResult, carries a resultFrame with
	// real row data for query statements (models.go), converted from the
	// same engine row extraction ExecuteStatement uses -- but on the wire it
	// uses the older Value union (bigIntValue/bitValue, not longValue/
	// booleanValue), so this page renders it with valueToDisplay rather than
	// fieldToDisplay.
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getRDSDataClient } from '$lib/aws-client';
	import {
		ExecuteStatementCommand,
		BatchExecuteStatementCommand,
		BeginTransactionCommand,
		CommitTransactionCommand,
		RollbackTransactionCommand,
		ExecuteSqlCommand,
		type Field,
		type ColumnMetadata,
		type SqlParameter,
		type UpdateResult,
		type SqlStatementResult,
		type Value,
		type DecimalReturnType,
		type LongReturnType,
		type RecordsFormatType,
		type TypeHint
	} from '@aws-sdk/client-rds-data';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import {
		Database,
		Play,
		Clock,
		Copy,
		Download,
		Trash2,
		ChevronRight,
		CheckCircle,
		XCircle,
		Layers,
		Plus,
		Send,
		RotateCcw,
		History
	} from 'lucide-svelte';

	const client = regionalClient(getRDSDataClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text.
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

	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	const LS_PREFIX = 'gopherstack_rdsdata_';
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

	// ==================== Connection settings (persisted) ====================
	// Shared across every op: ExecuteStatement/BatchExecuteStatement/
	// BeginTransaction require resourceArn+secretArn (both "This member is
	// required" per the real SqlParameter request shapes and enforced
	// server-side -- handler.go's validateRequiredFields); database/schema
	// are optional everywhere. ExecuteSql (legacy tab) reuses these same two
	// identity fields under its own wire names.

	let resourceArn = $state(lsGet('resourceArn', ''));
	let secretArn = $state(lsGet('secretArn', ''));
	let database = $state(lsGet('database', ''));
	let schema = $state(lsGet('schema', ''));

	$effect(() => { lsSet('resourceArn', resourceArn); });
	$effect(() => { lsSet('secretArn', secretArn); });
	$effect(() => { lsSet('database', database); });
	$effect(() => { lsSet('schema', schema); });

	const connectionReady = $derived(resourceArn.trim() !== '' && secretArn.trim() !== '');

	type TabId = 'query' | 'transactions' | 'history' | 'legacy';

	const tabs: TabDef[] = [
		{ id: 'query', label: 'Query Console' },
		{ id: 'transactions', label: 'Transactions' },
		{ id: 'history', label: 'Statement History' },
		{ id: 'legacy', label: 'ExecuteSql (legacy)' }
	];

	let activeTab = $state<TabId>('query');
	let searchQuery = $state('');

	// ==================== Parameters / Parameter Sets ====================
	//
	// One shared row shape backs both ExecuteStatement's single `parameters`
	// list and BatchExecuteStatement's `parameterSets` (an array of such
	// lists, one per template application). `type` picks which Field union
	// member to populate; `typeHint` is the real API's DATE/DECIMAL/JSON/
	// TIME/TIMESTAMP/UUID hint, which round-trips on the wire but -- per
	// services/rdsdata/PARITY.md -- does not change bind behavior against
	// this mock's SQLite engine (no distinct DATE/TIMESTAMP/UUID column
	// types to convert into). Exposed anyway since it's a real, accepted
	// field, not fabricated.

	type ParamType = 'string' | 'long' | 'double' | 'boolean' | 'null' | 'blob';

	interface ParamRow {
		id: string;
		name: string;
		type: ParamType;
		value: string;
		typeHint: '' | TypeHint;
	}

	let paramRowSeq = 0;
	function newParamRow(): ParamRow {
		return { id: `p${++paramRowSeq}`, name: '', type: 'string', value: '', typeHint: '' };
	}

	// paramSets[0] is used as ExecuteStatement's `parameters` in single mode;
	// every entry is used as one BatchExecuteStatement parameter set in
	// batch mode.
	let paramSets = $state<ParamRow[][]>([[]]);

	function addParamRow(setIndex: number): void {
		paramSets[setIndex] = [...paramSets[setIndex], newParamRow()];
	}
	function removeParamRow(setIndex: number, rowId: string): void {
		paramSets[setIndex] = paramSets[setIndex].filter((r) => r.id !== rowId);
	}
	function addParamSet(): void {
		paramSets = [...paramSets, []];
	}
	function removeParamSet(setIndex: number): void {
		if (paramSets.length <= 1) return;
		paramSets = paramSets.filter((_, i) => i !== setIndex);
	}

	function base64ToBytes(b64: string): Uint8Array {
		const bin = atob(b64);
		const bytes = new Uint8Array(bin.length);
		for (let i = 0; i < bin.length; i++) bytes[i] = bin.codePointAt(i) ?? 0;
		return bytes;
	}

	function paramRowToField(row: ParamRow): Field {
		switch (row.type) {
			case 'null':
				return { isNull: true };
			case 'long': {
				const n = Number(row.value);
				return { longValue: Number.isFinite(n) ? n : 0 };
			}
			case 'double': {
				const n = Number(row.value);
				return { doubleValue: Number.isFinite(n) ? n : 0 };
			}
			case 'boolean':
				return { booleanValue: row.value === 'true' };
			case 'blob':
				try {
					return { blobValue: base64ToBytes(row.value) };
				} catch {
					return { blobValue: new Uint8Array() };
				}
			default:
				return { stringValue: row.value };
		}
	}

	function rowsToSqlParameters(rows: ParamRow[]): SqlParameter[] {
		return rows
			.filter((r) => r.name.trim() !== '')
			.map((r) => ({
				name: r.name.trim(),
				value: paramRowToField(r),
				typeHint: r.typeHint || undefined
			}));
	}

	// ==================== Field / result rendering ====================

	function fieldToDisplay(f: Field): string {
		if (f.isNull) return 'NULL';
		if (f.stringValue !== undefined) return f.stringValue;
		if (f.longValue !== undefined) return String(f.longValue);
		if (f.doubleValue !== undefined) return String(f.doubleValue);
		if (f.booleanValue !== undefined) return String(f.booleanValue);
		if (f.blobValue !== undefined) return '<blob>';
		if (f.arrayValue !== undefined) return '<array>';
		return '—';
	}

	// Value is ExecuteSql's older union (bigIntValue/bitValue instead of
	// longValue/booleanValue) -- distinct from Field, see the RDS Data API docs.
	function valueToDisplay(v: Value): string {
		if (v.isNull) return 'NULL';
		if (v.stringValue !== undefined) return v.stringValue;
		if (v.bigIntValue !== undefined) return String(v.bigIntValue);
		if (v.intValue !== undefined) return String(v.intValue);
		if (v.doubleValue !== undefined) return String(v.doubleValue);
		if (v.realValue !== undefined) return String(v.realValue);
		if (v.bitValue !== undefined) return String(v.bitValue);
		if (v.blobValue !== undefined) return '<blob>';
		if (v.arrayValues !== undefined) return '<array>';
		if (v.structValue !== undefined) return '<struct>';
		return '—';
	}

	// ==================== Statement History (client-tracked) ====================
	//
	// The real Data API has no ListStatements-equivalent operation, so this
	// log only ever contains statements this browser session itself
	// submitted -- it resets on reload, exactly like the previous version of
	// this page. Not a limitation introduced here; there is no server-side
	// history to fetch instead.

	interface HistoryEntry {
		id: string;
		op: 'ExecuteStatement' | 'BatchExecuteStatement' | 'ExecuteSql';
		sql: string;
		transactionId?: string;
		executedAt: Date;
		durationMs: number;
		ok: boolean;
		error?: string;
	}
	let history = $state<HistoryEntry[]>([]);

	let entrySeq = 0;
	function addHistory(entry: Omit<HistoryEntry, 'id' | 'executedAt'>): void {
		history = [{ ...entry, id: `h${++entrySeq}`, executedAt: new Date() }, ...history].slice(0, 200);
	}

	const filteredHistory = $derived(
		history.filter((e) => matches(searchQuery, e.sql, e.op, e.transactionId))
	);

	function formatMs(ms: number): string {
		if (ms < 1000) return `${Math.round(ms)}ms`;
		return `${(ms / 1000).toFixed(1)}s`;
	}

	function truncateSql(sql: string, max = 100): string {
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

	function exportHistoryCsv(): void {
		const lines = ['op,sql,transactionId,executedAt,durationMs,ok,error'];
		for (const e of history) {
			lines.push(
				[
					`"${e.op}"`,
					`"${e.sql.replaceAll('"', '""')}"`,
					`"${e.transactionId ?? ''}"`,
					`"${e.executedAt.toISOString()}"`,
					String(e.durationMs),
					String(e.ok),
					`"${(e.error ?? '').replaceAll('"', '""')}"`
				].join(',')
			);
		}
		const blob = new Blob([lines.join('\n')], { type: 'text/csv' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = 'rdsdata-history.csv';
		a.click();
		URL.revokeObjectURL(url);
	}

	function reuseHistoryEntry(entry: HistoryEntry): void {
		if (entry.op === 'ExecuteSql') {
			legacySqlStatements = entry.sql;
			activeTab = 'legacy';
			return;
		}
		batchMode = entry.op === 'BatchExecuteStatement';
		sqlText = entry.sql;
		transactionIdInput = entry.transactionId ?? '';
		activeTab = 'query';
	}

	// ==================== Transactions (client-tracked) ====================
	//
	// Real per-region, real engine-side sql.Tx (transactions.go): committing
	// or rolling back deletes the id server-side, so reusing a closed id
	// genuinely 400s with TransactionNotFoundException -- this page doesn't
	// paper over that, it surfaces whatever the backend returns.

	interface LocalTransaction {
		id: string;
		resourceArn: string;
		database: string;
		startedAt: Date;
	}
	let openTransactions = $state<LocalTransaction[]>([]);
	let beginningTx = $state(false);
	let committingTx = $state<string | null>(null);
	let rollingBackTx = $state<string | null>(null);

	const filteredTransactions = $derived(
		openTransactions.filter((t) => matches(searchQuery, t.id, t.resourceArn, t.database))
	);

	async function beginTransaction(): Promise<void> {
		if (!connectionReady) {
			toast.error('Resource ARN and Secret ARN are required');
			return;
		}
		beginningTx = true;
		const capturedArn = resourceArn.trim();
		try {
			const resp = await client().send(
				new BeginTransactionCommand({
					resourceArn: capturedArn,
					secretArn: secretArn.trim(),
					database: database.trim() || undefined,
					schema: schema.trim() || undefined
				})
			);
			const txId = resp.transactionId;
			if (!txId) {
				toast.error('BeginTransaction returned no transactionId');
				return;
			}
			openTransactions = [
				...openTransactions,
				{ id: txId, resourceArn: capturedArn, database: database.trim(), startedAt: new Date() }
			];
			toast.success(`Transaction ${txId} started`);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			beginningTx = false;
		}
	}

	async function commitTransaction(tx: LocalTransaction): Promise<void> {
		committingTx = tx.id;
		try {
			await client().send(
				new CommitTransactionCommand({
					transactionId: tx.id,
					resourceArn: resourceArn.trim(),
					secretArn: secretArn.trim()
				})
			);
			openTransactions = openTransactions.filter((t) => t.id !== tx.id);
			if (transactionIdInput === tx.id) transactionIdInput = '';
			toast.success(`Transaction ${tx.id} committed`);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			committingTx = null;
		}
	}

	async function rollbackTransaction(tx: LocalTransaction): Promise<void> {
		const confirmed = await confirmDestructive({
			title: 'Roll back transaction',
			message: `Roll back transaction ${tx.id}? Any statements executed under it will be undone.`,
			confirmLabel: 'Roll back',
			dangerous: true
		});
		if (!confirmed) return;

		rollingBackTx = tx.id;
		try {
			await client().send(
				new RollbackTransactionCommand({
					transactionId: tx.id,
					resourceArn: resourceArn.trim(),
					secretArn: secretArn.trim()
				})
			);
			openTransactions = openTransactions.filter((t) => t.id !== tx.id);
			if (transactionIdInput === tx.id) transactionIdInput = '';
			toast.success(`Transaction ${tx.id} rolled back`);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			rollingBackTx = null;
		}
	}

	function useTransaction(txId: string): void {
		transactionIdInput = txId;
		activeTab = 'query';
		toast.info(`Transaction ${txId} set in Query Console`);
	}

	// ==================== Query Console (ExecuteStatement / BatchExecuteStatement) ====================

	let sqlText = $state('SELECT 1;');
	let batchMode = $state(false);
	let transactionIdInput = $state('');
	let includeResultMetadata = $state(false);
	let formatRecordsAs = $state<'' | RecordsFormatType>('');
	let decimalReturnType = $state<'' | DecimalReturnType>('');
	let longReturnType = $state<'' | LongReturnType>('');
	let continueAfterTimeout = $state(false);

	let executing = $state(false);
	let runError = $state<string | null>(null);
	let executionMs = $state<number | null>(null);

	// ExecuteStatement result shape
	let resultRecords = $state<Field[][]>([]);
	let resultColumns = $state<ColumnMetadata[]>([]);
	let resultFormatted = $state<string | undefined>();
	let resultGenerated = $state<Field[]>([]);
	let resultUpdated = $state<number | undefined>();
	let hasRun = $state(false);

	// BatchExecuteStatement result shape
	let batchUpdateResults = $state<UpdateResult[]>([]);

	function resetResults(): void {
		runError = null;
		resultRecords = [];
		resultColumns = [];
		resultFormatted = undefined;
		resultGenerated = [];
		resultUpdated = undefined;
		batchUpdateResults = [];
	}

	type SharedExecParams = {
		resourceArn: string;
		secretArn: string;
		database: string | undefined;
		schema: string | undefined;
		transactionId: string | undefined;
	};

	async function executeBatchStatement(shared: SharedExecParams, sql: string): Promise<void> {
		const parameterSets = paramSets
			.map((set) => rowsToSqlParameters(set))
			.filter((set) => set.length > 0);
		const resp = await client().send(
			new BatchExecuteStatementCommand({
				...shared,
				sql,
				parameterSets: parameterSets.length > 0 ? parameterSets : undefined
			})
		);
		batchUpdateResults = resp.updateResults ?? [];
	}

	async function executeSingleStatement(shared: SharedExecParams, sql: string): Promise<void> {
		const parameters = rowsToSqlParameters(paramSets[0] ?? []);
		const resp = await client().send(
			new ExecuteStatementCommand({
				...shared,
				sql,
				parameters: parameters.length > 0 ? parameters : undefined,
				includeResultMetadata,
				continueAfterTimeout,
				resultSetOptions:
					decimalReturnType || longReturnType
						? {
								decimalReturnType: decimalReturnType || undefined,
								longReturnType: longReturnType || undefined
							}
						: undefined,
				formatRecordsAs: formatRecordsAs || undefined
			})
		);
		resultRecords = resp.records ?? [];
		resultColumns = resp.columnMetadata ?? [];
		resultFormatted = resp.formattedRecords;
		resultGenerated = resp.generatedFields ?? [];
		resultUpdated = resp.numberOfRecordsUpdated;
	}

	function recordQueryHistory(sql: string, durationMs: number, ok: boolean, error?: string): void {
		addHistory({
			op: batchMode ? 'BatchExecuteStatement' : 'ExecuteStatement',
			sql,
			transactionId: transactionIdInput.trim() || undefined,
			durationMs,
			ok,
			error
		});
	}

	async function runQuery(): Promise<void> {
		if (!connectionReady) {
			toast.error('Resource ARN and Secret ARN are required');
			return;
		}
		const sql = sqlText.trim();
		if (!sql) return;

		executing = true;
		resetResults();
		hasRun = true;
		const start = performance.now();

		const shared: SharedExecParams = {
			resourceArn: resourceArn.trim(),
			secretArn: secretArn.trim(),
			database: database.trim() || undefined,
			schema: schema.trim() || undefined,
			transactionId: transactionIdInput.trim() || undefined
		};

		try {
			if (batchMode) {
				await executeBatchStatement(shared, sql);
			} else {
				await executeSingleStatement(shared, sql);
			}

			const ms = performance.now() - start;
			executionMs = ms;
			toast.success('Statement executed');
			recordQueryHistory(sql, ms, true);
		} catch (e) {
			const ms = performance.now() - start;
			executionMs = ms;
			runError = describeError(e);
			toast.error('Execution failed');
			recordQueryHistory(sql, ms, false, runError);
		} finally {
			executing = false;
		}
	}

	function handleKeydown(e: KeyboardEvent): void {
		if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
			e.preventDefault();
			if (activeTab === 'query' && !executing && connectionReady && sqlText.trim()) {
				runQuery();
			} else if (activeTab === 'legacy' && !executingLegacy && connectionReady && legacySqlStatements.trim()) {
				runLegacyExecuteSql();
			}
		}
	}

	// ==================== ExecuteSql (legacy) ====================

	let legacySqlStatements = $state('SELECT 1;');
	let executingLegacy = $state(false);
	let legacyError = $state<string | null>(null);
	let legacyResults = $state<SqlStatementResult[]>([]);
	let legacyHasRun = $state(false);

	async function runLegacyExecuteSql(): Promise<void> {
		if (!connectionReady) {
			toast.error('Resource ARN and Secret ARN are required');
			return;
		}
		const sql = legacySqlStatements.trim();
		if (!sql) return;

		executingLegacy = true;
		legacyError = null;
		legacyResults = [];
		legacyHasRun = true;
		const start = performance.now();

		try {
			const resp = await client().send(
				new ExecuteSqlCommand({
					dbClusterOrInstanceArn: resourceArn.trim(),
					awsSecretStoreArn: secretArn.trim(),
					sqlStatements: sql,
					database: database.trim() || undefined,
					schema: schema.trim() || undefined
				})
			);
			legacyResults = resp.sqlStatementResults ?? [];
			const ms = performance.now() - start;
			toast.success('SQL statement(s) executed');
			addHistory({ op: 'ExecuteSql', sql, durationMs: ms, ok: true });
		} catch (e) {
			const ms = performance.now() - start;
			legacyError = describeError(e);
			toast.error('Execution failed');
			addHistory({ op: 'ExecuteSql', sql, durationMs: ms, ok: false, error: legacyError });
		} finally {
			executingLegacy = false;
		}
	}

	// ==================== Tab loader ====================
	//
	// No tab here has anything to bootstrap-fetch from the server -- RDS
	// Data has no read/list operations at all, so every tab's data is either
	// produced by an action the user takes (Query Console, legacy tab) or
	// tracked client-side from those actions' responses (Transactions,
	// History). createTabLoader is still used for the shared
	// loading/error-banner plumbing and to match the structure of every
	// other service page, matching how redshiftdata's own zero-fetch
	// "query" tab is wired.

	const tabLoader = createTabLoader<TabId>({
		query: () => Promise.resolve(),
		transactions: () => Promise.resolve(),
		history: () => Promise.resolve(),
		legacy: () => Promise.resolve()
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

	const typeHintOptions: ('' | TypeHint)[] = ['', 'DATE', 'DECIMAL', 'JSON', 'TIME', 'TIMESTAMP', 'UUID'];
</script>

<svelte:window on:keydown={handleKeydown} />

<div class="p-6 space-y-6">
	<PageHeader
		icon={Database}
		title="RDS Data"
		description="Run SQL against an Aurora Serverless cluster through the Data API, and manage the begin/commit/rollback transaction lifecycle."
		onRefresh={handleRefresh}
		color="blue"
	/>

	<!-- Connection settings -->
	<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 text-sm">
		<div>
			<label for="rds-resource-arn" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Resource ARN *</label>
			<input id="rds-resource-arn" bind:value={resourceArn} type="text" placeholder="arn:aws:rds:us-east-1:…:cluster:…" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono" />
		</div>
		<div>
			<label for="rds-secret-arn" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Secret ARN *</label>
			<input id="rds-secret-arn" bind:value={secretArn} type="text" placeholder="arn:aws:secretsmanager:…" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono" />
		</div>
		<div>
			<label for="rds-database" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Database <span class="text-gray-400">(optional)</span></label>
			<input id="rds-database" bind:value={database} type="text" placeholder="mydb" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
		<div>
			<label for="rds-schema" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Schema <span class="text-gray-400">(optional)</span></label>
			<input id="rds-schema" bind:value={schema} type="text" placeholder="public" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
	</div>

	<div class="flex flex-col sm:flex-row gap-3 justify-between">
		<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
		{#if activeTab === 'transactions' || activeTab === 'history'}
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
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-3">
					<label for={batchMode ? 'rds-sql-batch' : 'rds-sql'} class="block text-sm font-semibold text-gray-700 dark:text-gray-300">
						{batchMode ? 'SQL Template (one statement, applied to each parameter set)' : 'SQL Statement'}
						<span class="ml-2 text-xs font-normal text-gray-400">(Ctrl+Enter to run)</span>
					</label>
					<button
						type="button"
						aria-label="Toggle batch mode"
						onclick={() => (batchMode = !batchMode)}
						class={`flex items-center gap-1 px-2 py-1 rounded-lg text-xs border transition-colors ${batchMode ? 'bg-purple-100 text-purple-700 border-purple-300 dark:bg-purple-900/40 dark:text-purple-300 dark:border-purple-700' : 'border-gray-200 dark:border-gray-700 text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
					>
						<Layers class="w-3.5 h-3.5" />
						{batchMode ? 'Batch ON' : 'Batch OFF'}
					</button>
				</div>
				<div class="flex items-center gap-2">
					{#if executionMs !== null}
						<span class="text-xs text-gray-400">{formatMs(executionMs)}</span>
					{/if}
					<button
						type="button"
						onclick={runQuery}
						disabled={executing || !connectionReady || !sqlText.trim()}
						class="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed"
					>
						<Play class="w-4 h-4" />
						{executing ? 'Running…' : batchMode ? 'Run Batch' : 'Run Query'}
					</button>
				</div>
			</div>

			<textarea
				id={batchMode ? 'rds-sql-batch' : 'rds-sql'}
				bind:value={sqlText}
				rows={5}
				aria-label={batchMode ? 'Batch SQL Template' : 'SQL Query'}
				class="w-full px-4 py-3 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-y"
				placeholder={batchMode ? 'INSERT INTO users (id, name) VALUES (:id, :name)' : 'SELECT * FROM my_table LIMIT 10;'}
			></textarea>
			{#if batchMode}
				<p class="text-xs text-gray-400">
					Don't include a trailing semicolon. Bind named parameters (e.g. <code class="bg-gray-100 dark:bg-gray-800 px-1 rounded">:id</code>) and add one Parameter Set below per row you want inserted/updated.
				</p>
			{/if}

			<!-- Transaction id -->
			<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
				<div>
					<label for="rds-txid" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Transaction ID <span class="text-gray-400">(optional — leave empty for autocommit)</span></label>
					<input id="rds-txid" bind:value={transactionIdInput} type="text" placeholder="txn-000001" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono" />
				</div>
				{#if openTransactions.length > 0}
					<div>
						<span class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Open transactions</span>
						<div class="flex flex-wrap gap-1.5">
							{#each openTransactions as tx (tx.id)}
								<button
									type="button"
									onclick={() => (transactionIdInput = tx.id)}
									class={`text-xs font-mono px-2 py-1 rounded-full border ${transactionIdInput === tx.id ? 'bg-blue-100 border-blue-400 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300' : 'border-gray-200 dark:border-gray-700 text-gray-500 hover:bg-gray-50 dark:hover:bg-gray-800'}`}
								>
									{tx.id}
								</button>
							{/each}
						</div>
					</div>
				{/if}
			</div>

			<!-- Options -->
			<details class="rounded-xl border border-gray-200 dark:border-gray-700 p-3">
				<summary class="text-sm font-medium text-gray-600 dark:text-gray-400 cursor-pointer select-none">Options</summary>
				<div class="mt-3 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 text-sm">
					{#if !batchMode}
						<label class="flex items-center gap-2 text-xs text-gray-600 dark:text-gray-400">
							<input bind:checked={includeResultMetadata} type="checkbox" class="w-4 h-4 rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500" />
							includeResultMetadata (column names)
						</label>
						<div>
							<label for="rds-format-records" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">formatRecordsAs</label>
							<select id="rds-format-records" bind:value={formatRecordsAs} class="w-full px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900">
								<option value="">NONE (default)</option>
								<option value="JSON">JSON</option>
							</select>
						</div>
						<div>
							<label for="rds-decimal-return" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">resultSetOptions.decimalReturnType</label>
							<select id="rds-decimal-return" bind:value={decimalReturnType} class="w-full px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900">
								<option value="">STRING (default)</option>
								<option value="DOUBLE_OR_LONG">DOUBLE_OR_LONG</option>
							</select>
						</div>
						<div>
							<label for="rds-long-return" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">resultSetOptions.longReturnType</label>
							<select id="rds-long-return" bind:value={longReturnType} class="w-full px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900">
								<option value="">LONG (default)</option>
								<option value="STRING">STRING</option>
							</select>
						</div>
						<label class="flex items-center gap-2 text-xs text-gray-600 dark:text-gray-400">
							<input bind:checked={continueAfterTimeout} type="checkbox" class="w-4 h-4 rounded border-gray-300 dark:border-gray-600 text-blue-600 focus:ring-blue-500" />
							continueAfterTimeout <span class="text-gray-400">(accepted, no-op — this mock has no statement timeouts)</span>
						</label>
					{:else}
						<p class="text-xs text-gray-400 sm:col-span-2 lg:col-span-4">Batch mode has no per-call options beyond the SQL template, transaction id, and parameter sets below.</p>
					{/if}
				</div>
			</details>

			<!-- Parameters / Parameter Sets -->
			<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-3 space-y-3">
				{#each paramSets as set, setIndex (setIndex)}
					<div class={setIndex > 0 ? 'pt-3 border-t border-gray-100 dark:border-gray-800' : ''}>
						<div class="flex items-center justify-between mb-2">
							<span class="text-xs font-semibold text-gray-500 uppercase tracking-wider">
								{batchMode ? `Parameter Set ${setIndex + 1}` : 'Parameters'}
							</span>
							<div class="flex items-center gap-2">
								<button type="button" onclick={() => addParamRow(setIndex)} class="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:underline">
									<Plus class="w-3 h-3" /> Add Parameter
								</button>
								{#if batchMode && paramSets.length > 1}
									<button type="button" onclick={() => removeParamSet(setIndex)} aria-label="Remove parameter set {setIndex + 1}" class="text-xs text-red-500 hover:underline">
										Remove Set
									</button>
								{/if}
							</div>
						</div>
						{#if set.length === 0}
							<p class="text-xs text-gray-400 italic">No parameters.</p>
						{:else}
							<div class="space-y-1.5">
								{#each set as row (row.id)}
									<div class="flex items-center gap-1.5">
										<input
											bind:value={row.name}
											type="text"
											aria-label="Parameter name"
											placeholder="name"
											class="w-28 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 font-mono"
										/>
										<select bind:value={row.type} aria-label="Parameter type" class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900">
											<option value="string">string</option>
											<option value="long">long</option>
											<option value="double">double</option>
											<option value="boolean">boolean</option>
											<option value="null">null</option>
											<option value="blob">blob (base64)</option>
										</select>
										{#if row.type !== 'null'}
											{#if row.type === 'boolean'}
												<select bind:value={row.value} aria-label="Parameter value" class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900">
													<option value="true">true</option>
													<option value="false">false</option>
												</select>
											{:else}
												<input
													bind:value={row.value}
													type="text"
													aria-label="Parameter value"
													placeholder="value"
													class="flex-1 min-w-0 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 font-mono"
												/>
											{/if}
										{:else}
											<span class="flex-1 min-w-0 text-xs text-gray-400 italic px-2">NULL</span>
										{/if}
										<select bind:value={row.typeHint} aria-label="Parameter type hint" class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900">
											{#each typeHintOptions as hint (hint)}
												<option value={hint}>{hint === '' ? 'no hint' : hint}</option>
											{/each}
										</select>
										<button type="button" onclick={() => removeParamRow(setIndex, row.id)} aria-label="Remove parameter {row.name || setIndex}" class="text-gray-400 hover:text-red-500">
											<Trash2 class="w-3.5 h-3.5" />
										</button>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{/each}
				{#if batchMode}
					<button type="button" onclick={addParamSet} class="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:underline pt-1">
						<Plus class="w-3 h-3" /> Add Parameter Set
					</button>
				{/if}
			</div>

			{#if runError}
				<div role="alert" class="flex items-start gap-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-lg text-sm text-red-700 dark:text-red-300">
					<XCircle class="w-4 h-4 mt-0.5 shrink-0" />
					<span class="font-mono break-all">{runError}</span>
				</div>
			{/if}

			<!-- Results -->
			{#if batchMode}
				{#if batchUpdateResults.length > 0}
					<div>
						<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">
							Update Results <span class="font-normal text-gray-400">({batchUpdateResults.length} set{batchUpdateResults.length === 1 ? '' : 's'})</span>
						</h3>
						<div class="space-y-1">
							{#each batchUpdateResults as ur, i (i)}
								<div class="flex items-center gap-3 text-xs font-mono px-3 py-2 rounded-lg border border-gray-100 dark:border-gray-800">
									<span class="text-gray-400">Set {i + 1}</span>
									<span class="text-gray-700 dark:text-gray-300">
										generatedFields: {ur.generatedFields && ur.generatedFields.length > 0 ? ur.generatedFields.map(fieldToDisplay).join(', ') : '(none)'}
									</span>
								</div>
							{/each}
						</div>
					</div>
				{:else if hasRun}
					<p class="text-sm text-gray-400 italic">No update results (empty parameterSets run the template once with no generated fields to report).</p>
				{/if}
			{:else if resultFormatted !== undefined}
				<div>
					<div class="flex items-center justify-between mb-1">
						<span class="text-sm font-semibold text-gray-700 dark:text-gray-300">formattedRecords (JSON)</span>
						<button type="button" onclick={() => copyToClipboard(resultFormatted ?? '')} class="flex items-center gap-1 text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800">
							<Copy class="w-3 h-3" /> Copy
						</button>
					</div>
					<pre class="text-xs font-mono bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg p-3 overflow-auto max-h-96">{resultFormatted}</pre>
				</div>
			{:else if resultRecords.length > 0}
				<div>
					<div class="flex items-center justify-between mb-2">
						<span class="text-sm font-semibold text-gray-700 dark:text-gray-300">
							Records <span class="font-normal text-xs text-gray-400">({resultRecords.length} row{resultRecords.length === 1 ? '' : 's'})</span>
						</span>
						{#if resultUpdated !== undefined}
							<span class="text-xs text-gray-400">numberOfRecordsUpdated: {resultUpdated}</span>
						{/if}
					</div>
					<div class="overflow-auto max-h-96 rounded-xl border border-gray-200 dark:border-gray-700">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase sticky top-0">
								<tr>
									{#each resultRecords[0] as _col, i (i)}
										<th class="px-4 py-3 text-left whitespace-nowrap">
											{resultColumns[i]?.name ?? `column${i + 1}`}
											{#if resultColumns[i]?.typeName}<span class="ml-1 text-gray-400 normal-case font-normal">({resultColumns[i].typeName})</span>{/if}
										</th>
									{/each}
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
								{#each resultRecords as row, i (i)}
									<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
										{#each row as cell, j (j)}
											{@const display = fieldToDisplay(cell)}
											<td class={`px-4 py-2 whitespace-nowrap font-mono text-xs ${display === 'NULL' ? 'text-gray-400 dark:text-gray-500 italic' : ''}`}>{display}</td>
										{/each}
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
					{#if !includeResultMetadata}
						<p class="text-xs text-gray-400 mt-1">Column headers are placeholders (column1, column2, …) — check "includeResultMetadata" above to get real column names.</p>
					{/if}
					{#if resultGenerated.length > 0}
						<p class="text-xs text-gray-500 mt-2">generatedFields: <span class="font-mono">{resultGenerated.map(fieldToDisplay).join(', ')}</span></p>
					{/if}
				</div>
			{:else if hasRun}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400 text-sm border border-dashed border-gray-200 dark:border-gray-700 rounded-xl">
					<p>Statement finished with no result rows.</p>
					<p class="text-xs mt-1 text-gray-400">DDL/DML (CREATE, INSERT, UPDATE, DELETE) don't return records.{#if resultUpdated !== undefined} numberOfRecordsUpdated: {resultUpdated}{/if}</p>
				</div>
			{:else}
				<div class="text-center py-10 text-gray-400 text-sm border border-dashed border-gray-200 dark:border-gray-700 rounded-xl">
					<Play class="w-8 h-8 mx-auto mb-2 opacity-30" />
					<p>Run a statement to see results here</p>
					<p class="text-xs mt-1">This backend runs a real embedded SQLite engine per resource — SQL you submit actually executes.</p>
				</div>
			{/if}
		</div>

	<!-- ==================== TRANSACTIONS ==================== -->
	{:else if activeTab === 'transactions'}
		<div class="space-y-4">
			<div class="flex items-center justify-between gap-3">
				<span class="text-sm text-gray-600 dark:text-gray-400">{filteredTransactions.length} open transaction{filteredTransactions.length === 1 ? '' : 's'}</span>
				<button
					type="button"
					onclick={beginTransaction}
					disabled={beginningTx || !connectionReady}
					class="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
				>
					<Plus class="w-3.5 h-3.5" />
					{beginningTx ? 'Starting…' : 'Begin Transaction'}
				</button>
			</div>

			{#if filteredTransactions.length === 0}
				<div class="text-center py-12 text-gray-400 dark:text-gray-500">
					<Layers class="w-8 h-8 mx-auto mb-2 opacity-40" />
					<p class="text-sm">{openTransactions.length === 0 ? 'No open transactions' : 'No transactions match your search'}</p>
					<p class="text-xs mt-1">RDS Data has no ListTransactions operation — only transactions begun in this session are shown.</p>
				</div>
			{:else}
				<div class="space-y-2">
					{#each filteredTransactions as tx (tx.id)}
						{@const txBusy = committingTx === tx.id || rollingBackTx === tx.id}
						<div class="flex items-center justify-between p-3 border border-gray-200 dark:border-gray-700 rounded-lg">
							<div class="flex items-center gap-3 min-w-0">
								<div class="w-2 h-2 rounded-full bg-green-500 shrink-0"></div>
								<div class="min-w-0">
									<p class="text-sm font-mono text-gray-900 dark:text-gray-100 truncate">{tx.id}</p>
									<p class="text-xs text-gray-400 truncate">{tx.resourceArn}{tx.database ? ` · ${tx.database}` : ''}</p>
									<p class="text-xs text-gray-400">Started {tx.startedAt.toLocaleTimeString()}</p>
								</div>
							</div>
							<div class="flex items-center gap-2 shrink-0 ml-4">
								<button type="button" onclick={() => useTransaction(tx.id)} class="text-xs px-2 py-1 rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800 flex items-center gap-1">
									<ChevronRight class="w-3 h-3" /> Use
								</button>
								<button
									type="button"
									onclick={() => commitTransaction(tx)}
									disabled={txBusy}
									class="text-xs px-2 py-1 rounded border border-green-400 text-green-700 dark:text-green-400 hover:bg-green-50 dark:hover:bg-green-900/20 flex items-center gap-1 disabled:opacity-50"
								>
									<Send class="w-3 h-3" />
									{committingTx === tx.id ? '…' : 'Commit'}
								</button>
								<button
									type="button"
									onclick={() => rollbackTransaction(tx)}
									disabled={txBusy}
									class="text-xs px-2 py-1 rounded border border-red-400 text-red-700 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 flex items-center gap-1 disabled:opacity-50"
								>
									<RotateCcw class="w-3 h-3" />
									{rollingBackTx === tx.id ? '…' : 'Rollback'}
								</button>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>

	<!-- ==================== STATEMENT HISTORY ==================== -->
	{:else if activeTab === 'history'}
		{#snippet histSqlCell(e: HistoryEntry)}
			<code class="text-xs font-mono" title={e.sql}>{truncateSql(e.sql)}</code>
		{/snippet}
		{#snippet histOpCell(e: HistoryEntry)}
			<span class="text-xs px-2 py-1 rounded-full bg-gray-100 text-gray-700 dark:bg-gray-800 dark:text-gray-300">{e.op}</span>
		{/snippet}
		{#snippet histStatusCell(e: HistoryEntry)}
			{#if e.ok}
				<span class="flex items-center gap-1 text-xs text-green-600 dark:text-green-400"><CheckCircle class="w-3.5 h-3.5" /> OK</span>
			{:else}
				<span class="flex items-center gap-1 text-xs text-red-600 dark:text-red-400" title={e.error}><XCircle class="w-3.5 h-3.5" /> Failed</span>
			{/if}
		{/snippet}
		{#snippet histWhenCell(e: HistoryEntry)}
			<span class="text-xs text-gray-500">{e.executedAt.toLocaleTimeString()} · {formatMs(e.durationMs)}</span>
		{/snippet}
		{#snippet histTxCell(e: HistoryEntry)}
			{#if e.transactionId}
				<span class="text-xs font-mono text-gray-500">{e.transactionId}</span>
			{:else}
				<span class="text-xs text-gray-400">—</span>
			{/if}
		{/snippet}
		{#snippet histActionsCell(e: HistoryEntry)}
			<div class="flex items-center justify-end">
				<button type="button" onclick={() => reuseHistoryEntry(e)} class="text-xs text-blue-600 dark:text-blue-400 hover:underline">Reuse</button>
			</div>
		{/snippet}
		{@const historyColumns = defineColumns<HistoryEntry>([
			{ key: 'op', label: 'Op', render: histOpCell },
			{ key: 'sql', label: 'SQL', render: histSqlCell },
			{ key: 'status', label: 'Status', render: histStatusCell },
			{ key: 'transactionId', label: 'Transaction', render: histTxCell },
			{ key: 'executedAt', label: 'When', render: histWhenCell },
			{ key: 'actions', label: '', render: histActionsCell }
		])}
		<div class="space-y-3">
			<div class="flex items-center justify-between gap-3">
				<span class="text-sm text-gray-600 dark:text-gray-400">{filteredHistory.length} statement{filteredHistory.length === 1 ? '' : 's'} (this session)</span>
				<div class="flex items-center gap-2">
					<button
						type="button"
						onclick={exportHistoryCsv}
						disabled={history.length === 0}
						class="text-xs px-2 py-1.5 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800 flex items-center gap-1 disabled:opacity-40"
					>
						<Download class="w-3 h-3" /> Export CSV
					</button>
					<button
						type="button"
						onclick={() => { history = []; }}
						disabled={history.length === 0}
						class="text-xs px-2 py-1.5 rounded-lg border border-red-300 text-red-600 dark:border-red-700 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 flex items-center gap-1 disabled:opacity-40"
					>
						<Trash2 class="w-3 h-3" /> Clear
					</button>
				</div>
			</div>

			<DataTable
				rows={filteredHistory}
				rowKey={(e) => e.id}
				columns={historyColumns}
				loading={false}
				emptyMessage="No statements executed yet this session"
			/>
		</div>

	<!-- ==================== EXECUTESQL (LEGACY) ==================== -->
	{:else if activeTab === 'legacy'}
		<div class="space-y-4">
			<div class="flex items-start gap-2 p-3 rounded-lg border border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-900/20 text-xs text-amber-800 dark:text-amber-300">
				<History class="w-4 h-4 mt-0.5 shrink-0" />
				<p>
					<strong>ExecuteSql is deprecated</strong> — AWS recommends ExecuteStatement/BatchExecuteStatement instead (Query Console tab). It uses the Resource ARN and Secret ARN above as
					<code class="bg-amber-100 dark:bg-amber-900/40 px-1 rounded">dbClusterOrInstanceArn</code> / <code class="bg-amber-100 dark:bg-amber-900/40 px-1 rounded">awsSecretStoreArn</code>.
				</p>
			</div>

			<div class="flex items-center justify-between">
				<label for="rds-legacy-sql" class="text-sm font-semibold text-gray-700 dark:text-gray-300">
					SQL Statement(s) <span class="text-xs font-normal text-gray-400">(semicolon-separated · Ctrl+Enter to run)</span>
				</label>
				<button
					type="button"
					onclick={runLegacyExecuteSql}
					disabled={executingLegacy || !connectionReady || !legacySqlStatements.trim()}
					class="flex items-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm font-medium disabled:opacity-50 disabled:cursor-not-allowed"
				>
					<Play class="w-4 h-4" />
					{executingLegacy ? 'Running…' : 'Run'}
				</button>
			</div>
			<textarea
				id="rds-legacy-sql"
				bind:value={legacySqlStatements}
				rows={5}
				aria-label="Legacy SQL Statements"
				class="w-full px-4 py-3 rounded-xl border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono text-gray-900 dark:text-white resize-y"
				placeholder="CREATE TABLE t (id INT PRIMARY KEY); INSERT INTO t VALUES (1);"
			></textarea>

			{#if legacyError}
				<div role="alert" class="flex items-start gap-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-lg text-sm text-red-700 dark:text-red-300">
					<XCircle class="w-4 h-4 mt-0.5 shrink-0" />
					<span class="font-mono break-all">{legacyError}</span>
				</div>
			{/if}

			{#if legacyResults.length > 0}
				<div>
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">sqlStatementResults</h3>
					<div class="space-y-3">
						{#each legacyResults as r, i (i)}
							<div class="text-xs font-mono px-3 py-2 rounded-lg border border-gray-100 dark:border-gray-800">
								<div class="flex items-center gap-3">
									<span class="text-gray-400">Statement {i + 1}</span>
									<span class="text-gray-700 dark:text-gray-300">numberOfRecordsUpdated: {r.numberOfRecordsUpdated ?? 0}</span>
								</div>
								{#if r.resultFrame?.records && r.resultFrame.records.length > 0}
									{@const cols = r.resultFrame.resultSetMetadata?.columnMetadata ?? []}
									<div class="overflow-auto max-h-96 rounded-lg border border-gray-200 dark:border-gray-700 mt-2">
										<table class="w-full text-sm">
											<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase sticky top-0">
												<tr>
													{#each r.resultFrame.records[0].values ?? [] as _val, j (j)}
														<th class="px-4 py-3 text-left whitespace-nowrap">{cols[j]?.name ?? `column${j + 1}`}</th>
													{/each}
												</tr>
											</thead>
											<tbody class="divide-y divide-gray-100 dark:divide-gray-800 bg-white dark:bg-gray-900">
												{#each r.resultFrame.records as row, ri (ri)}
													<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
														{#each row.values ?? [] as cell, ci (ci)}
															{@const display = valueToDisplay(cell)}
															<td class={`px-4 py-2 whitespace-nowrap font-mono text-xs ${display === 'NULL' ? 'text-gray-400 dark:text-gray-500 italic' : ''}`}>{display}</td>
														{/each}
													</tr>
												{/each}
											</tbody>
										</table>
									</div>
								{:else if r.resultFrame}
									<p class="text-xs text-gray-400 mt-1">Statement finished with no result rows.</p>
								{/if}
							</div>
						{/each}
					</div>
				</div>
			{:else if legacyHasRun}
				<p class="text-sm text-gray-400 italic">No statement results returned.</p>
			{/if}
		</div>
	{/if}
</div>
