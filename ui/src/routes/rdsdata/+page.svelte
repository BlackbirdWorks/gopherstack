<script lang="ts">
	import { getRDSDataClient } from '$lib/aws-client';
	import {
		ExecuteStatementCommand,
		BatchExecuteStatementCommand,
		BeginTransactionCommand,
		CommitTransactionCommand,
		RollbackTransactionCommand
	} from '@aws-sdk/client-rds-data';
	import { toast } from 'svelte-sonner';
	import {
		Database,
		Play,
		RefreshCw,
		Clock,
		Code2,
		Copy,
		Download,
		Trash2,
		BookOpen,
		ChevronRight,
		CheckCircle,
		XCircle,
		Layers,
		Plus,
		Send,
		RotateCcw
	} from 'lucide-svelte';

	const client = getRDSDataClient();
	const LS_PREFIX = 'gopherstack_rdsdata_';

	function lsGet(key: string, fallback: string): string {
		try {
			return localStorage.getItem(LS_PREFIX + key) ?? fallback;
		} catch {
			return fallback;
		}
	}
	function lsSet(key: string, value: string) {
		try {
			localStorage.setItem(LS_PREFIX + key, value);
		} catch {
			// storage unavailable
		}
	}

	type ActiveTab = 'runner' | 'transactions' | 'history' | 'docs';
	let activeTab = $state<ActiveTab>('runner');

	// ── Connection settings ────────────────────────────────────────────────────
	let resourceArn = $state(lsGet('resourceArn', ''));
	let secretArn = $state(lsGet('secretArn', ''));
	let database = $state(lsGet('database', ''));

	$effect(() => { lsSet('resourceArn', resourceArn); });
	$effect(() => { lsSet('secretArn', secretArn); });
	$effect(() => { lsSet('database', database); });

	// ── SQL Runner ─────────────────────────────────────────────────────────────
	let sqlText = $state('SELECT 1;');
	let transactionIdInput = $state('');
	let batchMode = $state(false);
	let batchSqls = $state('SELECT 1;\nSELECT 2;');
	let executing = $state(false);
	let runError = $state<string | null>(null);
	let runResult = $state<string | null>(null);
	let executionMs = $state<number | null>(null);
	let showRawJson = $state(false);

	// ── Transaction Browser ─────────────────────────────────────────────────────
	interface LocalTransaction {
		id: string;
		resourceArn: string;
		secretArn: string;
		startedAt: Date;
		status: 'active';
	}
	let transactions = $state<LocalTransaction[]>([]);
	let committingTx = $state<string | null>(null);
	let rollingBackTx = $state<string | null>(null);
	let beginningTx = $state(false);

	// ── Statement History ──────────────────────────────────────────────────────
	interface HistoryEntry {
		id: string;
		sql: string;
		resourceArn: string;
		transactionId?: string;
		executedAt: Date;
		durationMs: number;
		ok: boolean;
		error?: string;
	}
	let history = $state<HistoryEntry[]>([]);
	let historyFilter = $state('');

	const filteredHistory = $derived(
		historyFilter
			? history.filter(
					(e) =>
						e.sql.toLowerCase().includes(historyFilter.toLowerCase()) ||
						e.resourceArn.toLowerCase().includes(historyFilter.toLowerCase())
				)
			: history
	);

	let entryId = 0;
	function nextId() {
		return `e${++entryId}`;
	}

	function addHistory(entry: Omit<HistoryEntry, 'id' | 'executedAt'>) {
		history = [{ ...entry, id: nextId(), executedAt: new Date() }, ...history].slice(0, 200);
	}

	function formatMs(ms: number | null): string {
		if (ms === null) return '';
		if (ms < 1000) return `${ms}ms`;
		return `${(ms / 1000).toFixed(1)}s`;
	}

	function truncateSql(sql: string, max = 100): string {
		return sql.length > max ? sql.slice(0, max) + '…' : sql;
	}

	async function copyToClipboard(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied');
		} catch {
			toast.error('Clipboard access denied');
		}
	}

	function exportHistoryCsv() {
		const lines = ['sql,resourceArn,transactionId,executedAt,durationMs,ok,error'];
		for (const e of history) {
			lines.push(
				[
					`"${e.sql.replaceAll('"', '""')}"`,
					`"${e.resourceArn}"`,
					`"${e.transactionId ?? ''}"`,
					`"${e.executedAt.toISOString()}"`,
					String(e.durationMs),
					String(e.ok),
					`"${e.error ?? ''}"`
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

	// ── SQL Runner actions ──────────────────────────────────────────────────────
	async function executeStatement() {
		if (!resourceArn.trim()) {
			toast.error('Resource ARN is required');
			return;
		}
		const sql = batchMode ? batchSqls.trim() : sqlText.trim();
		if (!sql) return;

		executing = true;
		runError = null;
		runResult = null;
		executionMs = null;
		showRawJson = false;

		const start = Date.now();
		try {
			let raw: unknown;
			if (batchMode) {
				raw = await client.send(
					new BatchExecuteStatementCommand({
						resourceArn: resourceArn.trim(),
						secretArn: secretArn.trim() || undefined,
						sql: sql,
						database: database.trim() || undefined,
						transactionId: transactionIdInput.trim() || undefined
					})
				);
			} else {
				raw = await client.send(
					new ExecuteStatementCommand({
						resourceArn: resourceArn.trim(),
						secretArn: secretArn.trim() || undefined,
						sql: sql,
						database: database.trim() || undefined,
						transactionId: transactionIdInput.trim() || undefined
					})
				);
			}
			const ms = Date.now() - start;
			executionMs = ms;
			runResult = JSON.stringify(raw, null, 2);
			toast.success('Statement executed');
			addHistory({
				sql,
				resourceArn: resourceArn.trim(),
				transactionId: transactionIdInput.trim() || undefined,
				durationMs: ms,
				ok: true
			});
		} catch (e) {
			const ms = Date.now() - start;
			executionMs = ms;
			runError = String(e);
			toast.error('Execution failed');
			addHistory({
				sql,
				resourceArn: resourceArn.trim(),
				transactionId: transactionIdInput.trim() || undefined,
				durationMs: ms,
				ok: false,
				error: String(e)
			});
		} finally {
			executing = false;
		}
	}

	function handleKeydown(e: KeyboardEvent) {
		if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
			e.preventDefault();
			if (!executing && resourceArn.trim()) executeStatement();
		}
	}

	// ── Transaction actions ─────────────────────────────────────────────────────
	async function beginTransaction() {
		if (!resourceArn.trim()) {
			toast.error('Resource ARN is required');
			return;
		}
		beginningTx = true;
		const capturedArn = resourceArn.trim();
		const capturedSecret = secretArn.trim();
		try {
			const resp = await client.send(
				new BeginTransactionCommand({
					resourceArn: capturedArn,
					secretArn: capturedSecret || undefined,
					database: database.trim() || undefined
				})
			);
			const txId = resp.transactionId ?? `txn-${Date.now()}`;
			transactions = [
				...transactions,
				{ id: txId, resourceArn: capturedArn, secretArn: capturedSecret, startedAt: new Date(), status: 'active' }
			];
			toast.success(`Transaction ${txId} started`);
		} catch (e) {
			toast.error('Failed to begin transaction: ' + String(e));
		} finally {
			beginningTx = false;
		}
	}

	async function commitTransaction(tx: LocalTransaction) {
		committingTx = tx.id;
		try {
			await client.send(
				new CommitTransactionCommand({
					transactionId: tx.id,
					resourceArn: tx.resourceArn,
					secretArn: tx.secretArn
				})
			);
			transactions = transactions.filter((t) => t.id !== tx.id);
			if (transactionIdInput === tx.id) transactionIdInput = '';
			toast.success(`Transaction ${tx.id} committed`);
		} catch (e) {
			toast.error('Commit failed: ' + String(e));
		} finally {
			committingTx = null;
		}
	}

	async function rollbackTransaction(tx: LocalTransaction) {
		rollingBackTx = tx.id;
		try {
			await client.send(
				new RollbackTransactionCommand({
					transactionId: tx.id,
					resourceArn: tx.resourceArn,
					secretArn: tx.secretArn
				})
			);
			transactions = transactions.filter((t) => t.id !== tx.id);
			if (transactionIdInput === tx.id) transactionIdInput = '';
			toast.success(`Transaction ${tx.id} rolled back`);
		} catch (e) {
			toast.error('Rollback failed: ' + String(e));
		} finally {
			rollingBackTx = null;
		}
	}

	function useTransaction(txId: string) {
		transactionIdInput = txId;
		activeTab = 'runner';
		toast.info(`Transaction ${txId} set in SQL Runner`);
	}

	// ── Seed demo ──────────────────────────────────────────────────────────────
	function seedDemo() {
		const demoArn = 'arn:aws:rds:us-east-1:123456789012:cluster:my-aurora-cluster';
		const now = new Date();
		const entries: HistoryEntry[] = [
			{
				id: nextId(),
				sql: 'CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255));',
				resourceArn: demoArn,
				executedAt: new Date(now.getTime() - 300000),
				durationMs: 42,
				ok: true
			},
			{
				id: nextId(),
				sql: "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');",
				resourceArn: demoArn,
				executedAt: new Date(now.getTime() - 240000),
				durationMs: 18,
				ok: true
			},
			{
				id: nextId(),
				sql: 'SELECT * FROM users;',
				resourceArn: demoArn,
				executedAt: new Date(now.getTime() - 180000),
				durationMs: 11,
				ok: true
			},
			{
				id: nextId(),
				sql: "SELECT * FROM nonexistent;",
				resourceArn: demoArn,
				executedAt: new Date(now.getTime() - 60000),
				durationMs: 7,
				ok: false,
				error: 'Table not found: nonexistent'
			}
		];
		history = [...entries, ...history].slice(0, 200);
		toast.success('Demo history seeded');
	}
</script>

<svelte:window on:keydown={handleKeydown} />

<div class="min-h-screen bg-gray-50 dark:bg-gray-950 p-6">
	<!-- Header -->
	<div class="mb-6">
		<div class="flex items-center gap-3 mb-1">
			<Database class="w-7 h-7 text-blue-600" />
			<h1 class="text-2xl font-bold text-gray-900 dark:text-gray-100">RDS Data</h1>
		</div>
		<p class="text-sm text-gray-500 dark:text-gray-400">
			Aurora Serverless Data API — SQL runner, transaction browser, statement history
		</p>
	</div>

	<!-- Connection Settings -->
	<div class="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-4 mb-6">
		<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Connection</h2>
		<div class="grid grid-cols-1 md:grid-cols-3 gap-3">
			<div>
				<label for="resource-arn" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Resource ARN</label>
				<input
					id="resource-arn"
					type="text"
					bind:value={resourceArn}
					placeholder="arn:aws:rds:us-east-1:…:cluster:…"
					class="w-full text-sm border border-gray-300 dark:border-gray-600 rounded px-3 py-1.5 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
				/>
			</div>
			<div>
				<label for="secret-arn" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Secret ARN <span class="text-gray-400">(optional)</span></label>
				<input
					id="secret-arn"
					type="text"
					bind:value={secretArn}
					placeholder="arn:aws:secretsmanager:…"
					class="w-full text-sm border border-gray-300 dark:border-gray-600 rounded px-3 py-1.5 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
				/>
			</div>
			<div>
				<label for="database-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Database <span class="text-gray-400">(optional)</span></label>
				<input
					id="database-name"
					type="text"
					bind:value={database}
					placeholder="mydb"
					class="w-full text-sm border border-gray-300 dark:border-gray-600 rounded px-3 py-1.5 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
				/>
			</div>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-1 mb-4 border-b border-gray-200 dark:border-gray-700">
		{#each ([
			{ id: 'runner' as ActiveTab, label: 'SQL Runner', icon: Play, badge: null as string | null },
			{ id: 'transactions' as ActiveTab, label: 'Transaction Browser', icon: Layers, badge: transactions.length > 0 ? String(transactions.length) : null as string | null },
			{ id: 'history' as ActiveTab, label: 'Statement History', icon: Clock, badge: history.length > 0 ? String(history.length) : null as string | null },
			{ id: 'docs' as ActiveTab, label: 'Docs', icon: BookOpen, badge: null as string | null }
		]) as tab}
			{@const Icon = tab.icon}
			<button
				type="button"
				onclick={() => (activeTab = tab.id)}
				class="flex items-center gap-1.5 px-4 py-2 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === tab.id ? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400' : 'border-transparent text-gray-600 dark:text-gray-400 hover:text-gray-900 dark:hover:text-gray-200'}"
			>
				<Icon class="w-4 h-4" />
				{tab.label}
				{#if tab.badge}
					<span class="bg-blue-100 text-blue-700 dark:bg-blue-900/40 dark:text-blue-300 text-xs rounded-full px-1.5 py-0.5 leading-none">{tab.badge}</span>
				{/if}
			</button>
		{/each}
	</div>

	<!-- Tab: SQL Runner -->
	{#if activeTab === 'runner'}
		<div class="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
			<div class="flex items-center justify-between mb-3">
				<div class="flex items-center gap-2">
					<button
						type="button"
						aria-label="Toggle batch mode"
						onclick={() => (batchMode = !batchMode)}
						class="text-xs px-2 py-1 rounded border {batchMode ? 'border-blue-500 bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300 dark:border-blue-500' : 'border-gray-300 text-gray-600 dark:border-gray-600 dark:text-gray-400'}"
					>
						Batch {batchMode ? 'ON' : 'OFF'}
					</button>
					<span class="text-xs text-gray-400 hidden sm:inline">Ctrl+Enter to run</span>
				</div>
				<div class="flex items-center gap-2">
					{#if executionMs !== null}
						<span class="text-xs text-gray-400">{formatMs(executionMs)}</span>
					{/if}
					<button
						type="button"
						onclick={executeStatement}
						disabled={executing || !resourceArn.trim()}
						class="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
					>
						<Play class="w-3.5 h-3.5" />
						{executing ? 'Running…' : 'Run Query'}
					</button>
				</div>
			</div>

			<!-- Transaction ID -->
			<div class="mb-3">
				<label for="tx-id" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Transaction ID <span class="text-gray-400">(optional — leave empty for auto-commit)</span></label>
				<input
					id="tx-id"
					type="text"
					bind:value={transactionIdInput}
					placeholder="txn-000001"
					class="w-full text-sm border border-gray-300 dark:border-gray-600 rounded px-3 py-1.5 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
				/>
			</div>

			{#if batchMode}
				<textarea
					aria-label="Batch SQL"
					bind:value={batchSqls}
					rows={5}
					class="w-full font-mono text-sm border border-gray-300 dark:border-gray-600 rounded px-3 py-2 bg-gray-50 dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y mb-3"
					placeholder="SELECT 1;&#10;SELECT 2;"
				></textarea>
			{:else}
				<textarea
					aria-label="SQL Query"
					bind:value={sqlText}
					rows={5}
					class="w-full font-mono text-sm border border-gray-300 dark:border-gray-600 rounded px-3 py-2 bg-gray-50 dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y mb-3"
					placeholder="SELECT * FROM my_table LIMIT 10;"
				></textarea>
			{/if}

			{#if runError}
				<div class="flex items-start gap-2 p-3 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded text-sm text-red-700 dark:text-red-300">
					<XCircle class="w-4 h-4 mt-0.5 shrink-0" />
					<span class="font-mono break-all">{runError}</span>
				</div>
			{/if}

			{#if runResult}
				<div class="mt-3">
					<div class="flex items-center justify-between mb-1">
						<div class="flex items-center gap-2">
							<CheckCircle class="w-4 h-4 text-green-600" />
							<span class="text-sm font-medium text-gray-700 dark:text-gray-300">Result</span>
						</div>
						<div class="flex items-center gap-2">
							<button
								type="button"
								aria-label="Toggle raw JSON"
								onclick={() => (showRawJson = !showRawJson)}
								class="text-xs px-2 py-1 rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800"
							>
								{showRawJson ? 'Hide JSON' : 'Raw JSON'}
							</button>
							<button
								type="button"
								onclick={() => copyToClipboard(runResult!)}
								class="text-xs px-2 py-1 rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800 flex items-center gap-1"
							>
								<Copy class="w-3 h-3" />
								Copy
							</button>
						</div>
					</div>
					{#if showRawJson}
						<pre class="text-xs font-mono bg-gray-50 dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded p-3 overflow-auto max-h-64">{runResult}</pre>
					{/if}
				</div>
			{/if}
		</div>
	{/if}

	<!-- Tab: Transaction Browser -->
	{#if activeTab === 'transactions'}
		<div class="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Active Transactions</h2>
				<button
					type="button"
					onclick={beginTransaction}
					disabled={beginningTx || !resourceArn.trim()}
					class="flex items-center gap-1.5 px-3 py-1.5 text-sm font-medium rounded bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed"
				>
					<Plus class="w-3.5 h-3.5" />
					{beginningTx ? 'Starting…' : 'Begin Transaction'}
				</button>
			</div>

			{#if transactions.length === 0}
				<div class="text-center py-12 text-gray-400 dark:text-gray-500">
					<Layers class="w-8 h-8 mx-auto mb-2 opacity-40" />
					<p class="text-sm">No active transactions</p>
					<p class="text-xs mt-1">Click "Begin Transaction" to start one</p>
				</div>
			{:else}
				<div class="space-y-2">
					{#each transactions as tx (tx.id)}
						{@const txBusy = committingTx === tx.id || rollingBackTx === tx.id}
						<div class="flex items-center justify-between p-3 border border-gray-200 dark:border-gray-700 rounded-lg">
							<div class="flex items-center gap-3 min-w-0">
								<div class="w-2 h-2 rounded-full bg-green-500 shrink-0"></div>
								<div class="min-w-0">
									<p class="text-sm font-mono text-gray-900 dark:text-gray-100 truncate">{tx.id}</p>
									<p class="text-xs text-gray-400 truncate">{tx.resourceArn}</p>
									<p class="text-xs text-gray-400">Started {tx.startedAt.toLocaleTimeString()}</p>
								</div>
							</div>
							<div class="flex items-center gap-2 shrink-0 ml-4">
								<button
									type="button"
									onclick={() => useTransaction(tx.id)}
									class="text-xs px-2 py-1 rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800 flex items-center gap-1"
								>
									<Code2 class="w-3 h-3" />
									Use
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
	{/if}

	<!-- Tab: Statement History -->
	{#if activeTab === 'history'}
		<div class="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-4">
			<div class="flex items-center justify-between mb-4">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Statement History</h2>
				<div class="flex items-center gap-2">
					<button
						type="button"
						aria-label="Seed demo history"
						onclick={seedDemo}
						class="text-xs px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800 flex items-center gap-1"
					>
						<RefreshCw class="w-3 h-3" />
						Seed Demo
					</button>
					<button
						type="button"
						onclick={exportHistoryCsv}
						disabled={history.length === 0}
						class="text-xs px-2 py-1.5 rounded border border-gray-300 dark:border-gray-600 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800 flex items-center gap-1 disabled:opacity-40"
					>
						<Download class="w-3 h-3" />
						Export CSV
					</button>
					<button
						type="button"
						onclick={() => { history = []; }}
						disabled={history.length === 0}
						class="text-xs px-2 py-1.5 rounded border border-red-300 text-red-600 dark:border-red-700 dark:text-red-400 hover:bg-red-50 dark:hover:bg-red-900/20 flex items-center gap-1 disabled:opacity-40"
					>
						<Trash2 class="w-3 h-3" />
						Clear
					</button>
				</div>
			</div>

			<div class="mb-3">
				<input
					type="text"
					aria-label="Filter statement history"
					bind:value={historyFilter}
					placeholder="Filter by SQL or resource ARN…"
					class="w-full text-sm border border-gray-300 dark:border-gray-600 rounded px-3 py-1.5 bg-white dark:bg-gray-800 text-gray-900 dark:text-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500"
				/>
			</div>

			{#if filteredHistory.length === 0}
				<div class="text-center py-12 text-gray-400 dark:text-gray-500">
					<Clock class="w-8 h-8 mx-auto mb-2 opacity-40" />
					<p class="text-sm">{history.length === 0 ? 'No statements executed yet' : 'No results match your filter'}</p>
				</div>
			{:else}
				<div class="space-y-1">
					{#each filteredHistory as entry (entry.id)}
						<div
							class="flex items-start gap-3 p-3 rounded-lg border {entry.ok ? 'border-green-100 dark:border-green-900/30 bg-green-50/30 dark:bg-green-900/10' : 'border-red-100 dark:border-red-900/30 bg-red-50/30 dark:bg-red-900/10'}"
						>
							{#if entry.ok}
								<CheckCircle class="w-4 h-4 text-green-500 mt-0.5 shrink-0" />
							{:else}
								<XCircle class="w-4 h-4 text-red-500 mt-0.5 shrink-0" />
							{/if}
							<div class="min-w-0 flex-1">
								<p class="text-xs font-mono text-gray-800 dark:text-gray-200 break-all">{truncateSql(entry.sql)}</p>
								{#if entry.error}
									<p class="text-xs text-red-600 dark:text-red-400 mt-0.5">{entry.error}</p>
								{/if}
								<div class="flex items-center gap-3 mt-1 text-xs text-gray-400">
									<span>{entry.executedAt.toLocaleTimeString()}</span>
									<span>{formatMs(entry.durationMs)}</span>
									{#if entry.transactionId}
										<span class="font-mono">{entry.transactionId}</span>
									{/if}
								</div>
							</div>
							<button
								type="button"
								onclick={() => {
									sqlText = entry.sql;
									resourceArn = entry.resourceArn;
									transactionIdInput = entry.transactionId ?? '';
									activeTab = 'runner';
								}}
								class="shrink-0 text-xs px-2 py-1 rounded border border-gray-300 dark:border-gray-600 text-gray-500 dark:text-gray-400 hover:bg-gray-100 dark:hover:bg-gray-800 flex items-center gap-1"
							>
								<ChevronRight class="w-3 h-3" />
								Reuse
							</button>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}

	<!-- Tab: Docs -->
	{#if activeTab === 'docs'}
		<div class="bg-white dark:bg-gray-900 border border-gray-200 dark:border-gray-700 rounded-lg p-6">
			<h2 class="text-base font-semibold text-gray-800 dark:text-gray-200 mb-4">RDS Data API Reference</h2>
			<div class="space-y-4 text-sm text-gray-700 dark:text-gray-300">
				<div>
					<h3 class="font-semibold text-gray-900 dark:text-gray-100 mb-1 flex items-center gap-2"><Code2 class="w-4 h-4 text-blue-500" />ExecuteStatement</h3>
					<p class="text-gray-600 dark:text-gray-400">Runs a single SQL statement against an Aurora Serverless v1/v2 cluster. Provide <code class="bg-gray-100 dark:bg-gray-800 px-1 rounded">resourceArn</code>, optional <code class="bg-gray-100 dark:bg-gray-800 px-1 rounded">secretArn</code>, and <code class="bg-gray-100 dark:bg-gray-800 px-1 rounded">sql</code>. Use <code class="bg-gray-100 dark:bg-gray-800 px-1 rounded">transactionId</code> to run inside an open transaction.</p>
				</div>
				<div>
					<h3 class="font-semibold text-gray-900 dark:text-gray-100 mb-1 flex items-center gap-2"><Layers class="w-4 h-4 text-blue-500" />BatchExecuteStatement</h3>
					<p class="text-gray-600 dark:text-gray-400">Runs a SQL statement against a set of parameter arrays. Toggle "Batch ON" in SQL Runner to use this mode.</p>
				</div>
				<div>
					<h3 class="font-semibold text-gray-900 dark:text-gray-100 mb-1 flex items-center gap-2"><RefreshCw class="w-4 h-4 text-blue-500" />Transaction lifecycle</h3>
					<p class="text-gray-600 dark:text-gray-400"><strong>BeginTransaction</strong> → execute statements with the returned <code class="bg-gray-100 dark:bg-gray-800 px-1 rounded">transactionId</code> → <strong>CommitTransaction</strong> or <strong>RollbackTransaction</strong>. Active transactions are tracked in the Transaction Browser tab.</p>
				</div>
				<div>
					<h3 class="font-semibold text-gray-900 dark:text-gray-100 mb-1 flex items-center gap-2"><BookOpen class="w-4 h-4 text-blue-500" />Limitations</h3>
					<ul class="list-disc list-inside space-y-1 text-gray-600 dark:text-gray-400">
						<li>Statement history is session-local; it resets on page reload.</li>
						<li>The gopherstack simulator records executed statements but always returns empty result sets.</li>
						<li>ExecuteSQL (deprecated) is not exposed in this UI; use ExecuteStatement instead.</li>
					</ul>
				</div>
			</div>
		</div>
	{/if}
</div>
