<script lang="ts">
	import { onDestroy } from 'svelte';
	import { page } from '$app/state';
	import { getDynamoDBClient } from '$lib/aws-client';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import {
		DescribeTableCommand,
		DescribeTimeToLiveCommand,
		ExecuteStatementCommand,
		ScanCommand,
		QueryCommand,
		ListBackupsCommand,
		CreateBackupCommand,
		DeleteTableCommand,
		type StreamViewType,
		type TableDescription,
		type AttributeValue,
		type BackupSummary,
		UpdateTableCommand,
		UpdateTimeToLiveCommand
	} from '@aws-sdk/client-dynamodb';
	import { toast } from 'svelte-sonner';
	import { itemToJson, avToJson } from '$lib/dynamodb';

	const dynamodb = regionalClient(getDynamoDBClient);
	const tableName = $derived(page.params.tableName ?? '');

	// ── Core state ─────────────────────────────────────────────────────────────
	let loading = $state(false);
	let loadError = $state('');
	let ttlEnabled = $state(false);
	let ttlAttributeName = $state('');
	let streamsEnabled = $state(false);
	let streamViewType = $state<StreamViewType>('NEW_AND_OLD_IMAGES');
	let activeTab = $state<'overview' | 'items' | 'query' | 'backups' | 'streams' | 'partiql'>('overview');
	let tableDesc = $state<TableDescription | null>(null);

	// ── Items tab ──────────────────────────────────────────────────────────────
	let items = $state<Record<string, AttributeValue>[]>([]);
	let itemsLoading = $state(false);
	let itemsLastKey = $state<Record<string, AttributeValue> | undefined>();
	let itemsHasMore = $state(false);
	let itemsFilter = $state('');
	const itemColumns = $derived(getItemColumns(items));
	const filteredItems = $derived(
		itemsFilter.trim()
			? items.filter(row => Object.values(row).some(av => cellValue(av).toLowerCase().includes(itemsFilter.toLowerCase())))
			: items
	);

	function getItemColumns(rows: Record<string, AttributeValue>[]): string[] {
		const cols = new Set<string>();
		for (const row of rows) {
			for (const k of Object.keys(row)) cols.add(k);
		}
		return [...cols];
	}

	async function loadItems(reset = false): Promise<void> {
		if (!tableName) return;
		itemsLoading = true;
		try {
			const result = await dynamodb().send(new ScanCommand({
				TableName: tableName,
				Limit: 50,
				ExclusiveStartKey: reset ? undefined : itemsLastKey
			}));
			const newItems = result.Items ?? [];
			if (reset) {
				items = newItems;
			} else {
				items = [...items, ...newItems];
			}
			itemsLastKey = result.LastEvaluatedKey;
			itemsHasMore = !!result.LastEvaluatedKey;
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'Failed to load items');
		} finally {
			itemsLoading = false;
		}
	}

	function exportItemsJSON(): void {
		const data = items.map((item) => itemToJson(item));
		const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `${tableName}-items.json`;
		a.click();
		URL.revokeObjectURL(url);
	}

	function exportItemsCSV(): void {
		const data = items.map((item) => itemToJson(item));
		const cols = [...new Set(data.flatMap(r => Object.keys(r as Record<string, unknown>)))];
		const header = cols.map(c => JSON.stringify(c)).join(',');
		const rows = data.map(r => cols.map(c => {
			const v = (r as Record<string, unknown>)[c];
			return JSON.stringify(v ?? '');
		}).join(','));
		const csv = [header, ...rows].join('\n');
		const blob = new Blob([csv], { type: 'text/csv' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `${tableName}-items.csv`;
		a.click();
		URL.revokeObjectURL(url);
	}

	// ── Query tab ──────────────────────────────────────────────────────────────
	let queryPK = $state('');
	let queryPKValue = $state('');
	let querySKAttr = $state('');
	let querySKOp = $state('=');
	let querySKValue = $state('');
	let querySKValue2 = $state('');
	let queryIndexName = $state('');
	let queryResults = $state<Record<string, AttributeValue>[]>([]);
	let queryLoading = $state(false);
	const queryColumns = $derived(getItemColumns(queryResults));
	const availableIndexes = $derived([
		...(tableDesc?.GlobalSecondaryIndexes?.map(g => g.IndexName ?? '') ?? []),
		...(tableDesc?.LocalSecondaryIndexes?.map(l => l.IndexName ?? '') ?? [])
	].filter(Boolean));

	async function runQuery(): Promise<void> {
		if (!tableName || !queryPKValue.trim()) {
			toast.error('Partition key value is required');
			return;
		}
		queryLoading = true;
		try {
			const pkAttr = queryPK || (tableDesc?.KeySchema?.[0]?.AttributeName ?? 'pk');
			let keyCondition = `#pk = :pkVal`;
			const exprNames: Record<string, string> = { '#pk': pkAttr };
			const exprValues: Record<string, AttributeValue> = { ':pkVal': { S: queryPKValue } };
			if (querySKAttr && querySKValue) {
				exprNames['#sk'] = querySKAttr;
				if (querySKOp === 'begins_with') {
					keyCondition += ` AND begins_with(#sk, :skVal)`;
				} else if (querySKOp === 'between' && querySKValue2) {
					keyCondition += ` AND #sk BETWEEN :skVal AND :skVal2`;
					exprValues[':skVal2'] = { S: querySKValue2 };
				} else {
					keyCondition += ` AND #sk ${querySKOp} :skVal`;
				}
				exprValues[':skVal'] = { S: querySKValue };
			}
			const result = await dynamodb().send(new QueryCommand({
				TableName: tableName,
				IndexName: queryIndexName || undefined,
				KeyConditionExpression: keyCondition,
				ExpressionAttributeNames: exprNames,
				ExpressionAttributeValues: exprValues
			}));
			queryResults = result.Items ?? [];
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'Query failed');
		} finally {
			queryLoading = false;
		}
	}

	// ── Backups tab ────────────────────────────────────────────────────────────
	let backups = $state<BackupSummary[]>([]);
	let backupsLoading = $state(false);
	let newBackupName = $state('');

	async function loadBackups(): Promise<void> {
		if (!tableName) return;
		backupsLoading = true;
		try {
			const result = await dynamodb().send(new ListBackupsCommand({ TableName: tableName }));
			backups = result.BackupSummaries ?? [];
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'Failed to load backups');
		} finally {
			backupsLoading = false;
		}
	}

	async function createBackup(): Promise<void> {
		const name = newBackupName.trim() || `${tableName}-${Date.now()}`;
		try {
			await dynamodb().send(new CreateBackupCommand({ TableName: tableName, BackupName: name }));
			toast.success(`Backup "${name}" created`);
			newBackupName = '';
			await loadBackups();
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'Failed to create backup');
		}
	}

	// ── Streams tab ────────────────────────────────────────────────────────────
	let streamEventsHtml = $state('');
	let streamPollInterval: ReturnType<typeof setInterval> | null = null;
	let streamEventFilter = $state('ALL');

	type ShardInfo = { shardId: string; startSeq: string; endSeq: string };
	type EventTypeCounts = { insert: number; modify: number; remove: number };
	type StreamInfo = {
		available: boolean;
		eventCount: number;
		latestEventUnix: number;
		oldestSeq: string;
		latestSeq: string;
		lagSeconds: number;
		shards: ShardInfo[];
		eventTypes: EventTypeCounts;
	};
	let streamInfo = $state<StreamInfo | null>(null);

	async function loadStreamEvents(): Promise<void> {
		if (!tableName) return;
		try {
			const resp = await fetch(`/dashboard/dynamodb/table/${tableName}/stream-events`);
			streamEventsHtml = await resp.text();
		} catch {
			streamEventsHtml = 'Failed to load stream events.';
		}
	}

	async function loadStreamInfo(): Promise<void> {
		if (!tableName) return;
		try {
			const resp = await fetch(`/dashboard/dynamodb/table/${tableName}/stream-info`);
			if (resp.ok) streamInfo = await resp.json();
		} catch {
			// ignore
		}
	}

	function startStreamPolling(): void {
		streamInfo = null;
		streamEventFilter = 'ALL';
		void loadStreamEvents();
		void loadStreamInfo();
		streamPollInterval = setInterval(() => {
			void loadStreamEvents();
			void loadStreamInfo();
		}, 3000);
	}

	function stopStreamPolling(): void {
		if (streamPollInterval !== null) {
			clearInterval(streamPollInterval);
			streamPollInterval = null;
		}
	}

	// ── PartiQL tab ────────────────────────────────────────────────────────────
	let statement = $state('');
	let partiqlOutput = $state('');
	let partiqlLoading = $state(false);
	let partiqlDurationMs = $state(0);

	async function executePartiQL(): Promise<void> {
		if (!tableName || !statement.trim()) return;
		partiqlLoading = true;
		partiqlDurationMs = 0;
		const start = Date.now();
		try {
			const result = await dynamodb().send(new ExecuteStatementCommand({ Statement: statement }));
			partiqlDurationMs = Date.now() - start;
			partiqlOutput = JSON.stringify(result.Items ?? [], null, 2);
		} catch (err) {
			partiqlDurationMs = Date.now() - start;
			partiqlOutput = err instanceof Error ? err.message : 'query failed';
		} finally {
			partiqlLoading = false;
		}
	}

	// ── TTL & Streams config ───────────────────────────────────────────────────
	async function updateTTL(): Promise<void> {
		if (!tableName) return;
		try {
			await dynamodb().send(new UpdateTimeToLiveCommand({
				TableName: tableName,
				TimeToLiveSpecification: { AttributeName: ttlAttributeName, Enabled: ttlEnabled }
			}));
			if (ttlEnabled) {
				toast.success('TTL enabled successfully');
			} else {
				toast.success('TTL disabled successfully');
			}
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'failed to update ttl');
		}
		await loadState();
	}

	async function updateStreams(): Promise<void> {
		if (!tableName) return;
		try {
			await dynamodb().send(new UpdateTableCommand({
				TableName: tableName,
				StreamSpecification: streamsEnabled
					? { StreamEnabled: true, StreamViewType: streamViewType }
					: { StreamEnabled: false }
			}));
			if (streamsEnabled) {
				toast.success('Streams enabled successfully');
			} else {
				toast.success('Streams disabled successfully');
			}
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'failed to update streams');
		}
		await loadState();
	}

	// ── Delete table ───────────────────────────────────────────────────────────
	let showDeleteConfirm = $state(false);
	let deleteConfirmInput = $state('');
	let deleting = $state(false);

	async function deleteTable(): Promise<void> {
		if (deleteConfirmInput !== tableName) {
			toast.error('Table name does not match');
			return;
		}
		deleting = true;
		try {
			await dynamodb().send(new DeleteTableCommand({ TableName: tableName }));
			toast.success(`Table "${tableName}" deleted`);
			window.location.href = '/dashboard/dynamodb';
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'Failed to delete table');
			deleting = false;
		}
	}

	// ── ARN copy ───────────────────────────────────────────────────────────────
	let arnCopied = $state(false);

	async function copyArn(): Promise<void> {
		const arn = tableDesc?.TableArn ?? '';
		if (!arn) return;
		await navigator.clipboard.writeText(arn);
		arnCopied = true;
		setTimeout(() => { arnCopied = false; }, 2000);
	}

	// ── Helpers ────────────────────────────────────────────────────────────────
	function formatBytes(bytes: number): string {
		if (bytes < 1024) return `${bytes} B`;
		if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
		return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '—';
		return new Date(d).toLocaleString();
	}

	function cellValue(av: AttributeValue): string {
		const v = avToJson(av);
		if (v === null) return 'null';
		if (typeof v === 'object') return JSON.stringify(v);
		return String(v);
	}

	// ── Load core state ────────────────────────────────────────────────────────
	async function loadState(): Promise<void> {
		if (!tableName) return;
		loading = true;
		loadError = '';
		try {
			const [ttl, desc] = await Promise.all([
				dynamodb().send(new DescribeTimeToLiveCommand({ TableName: tableName })),
				dynamodb().send(new DescribeTableCommand({ TableName: tableName }))
			]);
			tableDesc = desc.Table ?? null;
			const ttlStatus = ttl.TimeToLiveDescription?.TimeToLiveStatus;
			ttlEnabled = ttlStatus === 'ENABLED';
			ttlAttributeName = ttl.TimeToLiveDescription?.AttributeName ?? '';
			const streamSpec = desc.Table?.StreamSpecification;
			streamsEnabled = streamSpec?.StreamEnabled ?? false;
			if (streamSpec?.StreamViewType) streamViewType = streamSpec.StreamViewType;
			statement = `SELECT * FROM "${tableName}"`;
		} catch (err) {
			loadError = err instanceof Error ? err.message : 'Failed to load table details';
		} finally {
			loading = false;
		}
	}

	// ── Tab switch ─────────────────────────────────────────────────────────────
	function switchTab(tab: typeof activeTab): void {
		activeTab = tab;
		if (tab === 'items' && items.length === 0) void loadItems(true);
		if (tab === 'backups' && backups.length === 0) void loadBackups();
		if (tab === 'streams') startStreamPolling();
		else stopStreamPolling();
	}

	// ── Lifecycle ──────────────────────────────────────────────────────────────
	onRegionChange(loadState);
	onDestroy(() => { stopStreamPolling(); });

	$effect(() => {
		const filter = streamEventFilter;
		const container = document.querySelector('.stream-events-filter');
		if (!container) return;
		const rows = container.querySelectorAll('tbody tr[data-event]');
		rows.forEach((row) => {
			const el = row as HTMLElement;
			el.style.display = (filter === 'ALL' || el.dataset.event === filter) ? '' : 'none';
		});
	});
</script>

<div class="space-y-4">
	<!-- Breadcrumb + Header -->
	<div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4">
		<div class="flex items-center gap-3">
			<a href="/dashboard/dynamodb" class="inline-flex items-center gap-1.5 text-sm font-medium text-slate-600 hover:text-blue-600 dark:text-slate-400 dark:hover:text-white transition-colors">
				<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" /></svg>
				DynamoDB Tables
			</a>
			<span class="text-slate-400">/</span>
			<h1 class="text-2xl font-bold text-slate-900 dark:text-white">{tableName}</h1>
		</div>
		<button
			type="button"
			class="text-sm text-red-600 border border-red-300 rounded-lg px-3 py-1.5 hover:bg-red-50 dark:border-red-700 dark:text-red-400 dark:hover:bg-red-900/20 transition-colors"
			onclick={() => { showDeleteConfirm = true; }}>
			Delete Table
		</button>
	</div>

	<!-- Stat Cards (loading skeleton) -->
	{#if loading}
		<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
			{#each Array(8) as _}
				<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 animate-pulse">
					<div class="h-3 bg-slate-200 dark:bg-slate-700 rounded w-2/3 mb-2"></div>
					<div class="h-6 bg-slate-200 dark:bg-slate-700 rounded w-1/2"></div>
				</div>
			{/each}
		</div>
	{:else}
		<!-- Row 1: Primary metrics -->
		<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
			<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Item Count</div>
				<div class="text-2xl font-bold text-blue-600 dark:text-blue-400 mt-1">{tableDesc?.ItemCount ?? 0}</div>
			</div>
			<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Table Size</div>
				<div class="text-lg font-bold text-slate-800 dark:text-slate-100 mt-1">{formatBytes(tableDesc?.TableSizeBytes ?? 0)}</div>
			</div>
			<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Billing Mode</div>
				<div class="mt-1"><span class="inline-block rounded bg-blue-100 text-blue-800 px-2 py-1 text-xs font-semibold dark:bg-blue-900 dark:text-blue-300">{tableDesc?.BillingModeSummary?.BillingMode ?? 'PAY_PER_REQUEST'}</span></div>
			</div>
			<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Status</div>
				<div class="mt-1"><span class="inline-block rounded bg-green-100 text-green-800 px-2 py-1 text-xs font-semibold dark:bg-green-900 dark:text-green-300">{tableDesc?.TableStatus ?? '—'}</span></div>
			</div>
		</div>
		<!-- Row 2: TTL / Streams / GSI / LSI -->
		<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
			<div id="ttl-status-card" class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">TTL</div>
				<div class="mt-1"><span class="inline-block rounded bg-slate-100 px-2 py-1 text-xs font-semibold dark:bg-slate-700 dark:text-slate-300">{ttlEnabled ? 'ENABLED' : 'DISABLED'}</span></div>
				{#if ttlEnabled && ttlAttributeName}
					<div class="text-xs text-slate-500 dark:text-slate-400 mt-1 truncate">{ttlAttributeName}</div>
				{/if}
			</div>
			<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Streams</div>
				<div class="mt-1"><span class="inline-block rounded bg-slate-100 px-2 py-1 text-xs font-semibold dark:bg-slate-700 dark:text-slate-300">{streamsEnabled ? 'ENABLED' : 'DISABLED'}</span></div>
			</div>
			<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">GSIs</div>
				<div class="text-2xl font-bold text-purple-600 dark:text-purple-400 mt-1">{tableDesc?.GlobalSecondaryIndexes?.length ?? 0}</div>
			</div>
			<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
				<div class="text-xs font-medium text-slate-500 dark:text-slate-400">LSIs</div>
				<div class="text-2xl font-bold text-indigo-600 dark:text-indigo-400 mt-1">{tableDesc?.LocalSecondaryIndexes?.length ?? 0}</div>
			</div>
		</div>

		<!-- ARN row -->
		<div class="flex items-center gap-2 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 px-4 py-3 shadow-sm">
			<span class="text-xs font-medium text-slate-500 dark:text-slate-400 shrink-0">ARN</span>
			<span class="text-xs text-slate-700 dark:text-slate-300 truncate flex-1 font-mono">{tableDesc?.TableArn ?? '—'}</span>
			{#if tableDesc?.TableArn}
				<button type="button" onclick={copyArn} class="text-xs text-blue-600 dark:text-blue-400 hover:underline shrink-0">
					{arnCopied ? 'Copied!' : 'Copy'}
				</button>
			{/if}
		</div>
	{/if}

	<!-- Load error -->
	{#if loadError}
		<div class="rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800 dark:bg-red-900/20 dark:text-red-400 flex items-center justify-between">
			<span>{loadError}</span>
			<button type="button" class="text-xs underline ml-4" onclick={() => void loadState()}>Retry</button>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="flex gap-1 border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
		{#each [['overview','Overview'],['items','Items'],['query','Query'],['backups','Backups'],['streams','Stream Events'],['partiql','PartiQL']] as [id, label]}
			<button id="{id}-tab" type="button"
				class="px-4 py-2 text-sm font-medium border-b-2 -mb-px whitespace-nowrap {activeTab === id ? 'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
				onclick={() => switchTab(id as typeof activeTab)}>
				{label}
				{#if id === 'items' && items.length > 0}
					<span class="ml-1 text-xs bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300 rounded-full px-1.5">{items.length}</span>
				{/if}
				{#if id === 'backups' && backups.length > 0}
					<span class="ml-1 text-xs bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300 rounded-full px-1.5">{backups.length}</span>
				{/if}
			</button>
		{/each}
	</div>

	<!-- ── Overview tab ────────────────────────────────────────────────────── -->
	{#if activeTab === 'overview'}
		<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
			<!-- TTL config -->
			<section class="space-y-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">Time To Live (TTL)</h3>
				<div class="space-y-2">
					<label class="block text-xs font-medium text-slate-700 dark:text-slate-300" for="ttl-attr">TTL Attribute Name</label>
					<input id="ttl-attr" name="attributeName" class="w-full rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white focus:ring-2 focus:ring-blue-500 focus:outline-none" bind:value={ttlAttributeName} placeholder="e.g. expiry" />
					<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer" for="ttl-enabled">
						<input id="ttl-enabled" type="checkbox" bind:checked={ttlEnabled} class="rounded" />
						Enable TTL
					</label>
					<button type="button" class="rounded-lg bg-blue-600 text-white px-4 py-1.5 text-sm hover:bg-blue-700 transition-colors" onclick={updateTTL}>Update TTL</button>
				</div>
			</section>

			<!-- Streams config -->
			<section class="space-y-3 rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">DynamoDB Streams</h3>
				<div class="space-y-2">
					<label class="block text-xs font-medium text-slate-700 dark:text-slate-300" for="viewType">Stream View Type</label>
					<select id="viewType" name="viewType" class="w-full rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white focus:ring-2 focus:ring-blue-500 focus:outline-none" bind:value={streamViewType}>
						<option value="NEW_IMAGE">NEW_IMAGE</option>
						<option value="OLD_IMAGE">OLD_IMAGE</option>
						<option value="NEW_AND_OLD_IMAGES">NEW_AND_OLD_IMAGES</option>
						<option value="KEYS_ONLY">KEYS_ONLY</option>
					</select>
					<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer" for="streams-enabled">
						<input id="streams-enabled" type="checkbox" bind:checked={streamsEnabled} class="rounded" />
						Enable Streams
					</label>
					{#if tableDesc?.LatestStreamArn}
						<div class="text-xs text-slate-500 dark:text-slate-400 font-mono truncate" title={tableDesc.LatestStreamArn}>Stream ARN: {tableDesc.LatestStreamArn}</div>
					{/if}
					<button type="button" class="rounded-lg bg-blue-600 text-white px-4 py-1.5 text-sm hover:bg-blue-700 transition-colors" onclick={updateStreams}>Update Streams</button>
				</div>
			</section>
		</div>

		<!-- Key Schema -->
		{#if tableDesc?.KeySchema && tableDesc.KeySchema.length > 0}
			<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-3">Key Schema</h3>
				<table class="w-full text-sm">
					<thead><tr class="border-b border-slate-200 dark:border-slate-700">
						<th class="text-left text-xs text-slate-500 pb-2">Attribute</th>
						<th class="text-left text-xs text-slate-500 pb-2">Key Type</th>
						<th class="text-left text-xs text-slate-500 pb-2">Data Type</th>
					</tr></thead>
					<tbody>
						{#each tableDesc.KeySchema as ks}
							{@const attrDef = tableDesc.AttributeDefinitions?.find(a => a.AttributeName === ks.AttributeName)}
							<tr class="border-b border-slate-100 dark:border-slate-700 last:border-0">
								<td class="py-1.5 font-mono text-slate-800 dark:text-slate-200">{ks.AttributeName}</td>
								<td class="py-1.5"><span class="text-xs rounded bg-orange-100 text-orange-700 dark:bg-orange-900 dark:text-orange-300 px-1.5 py-0.5">{ks.KeyType}</span></td>
								<td class="py-1.5 text-slate-500 dark:text-slate-400">{attrDef?.AttributeType ?? '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</section>
		{/if}

		<!-- GSIs -->
		{#if (tableDesc?.GlobalSecondaryIndexes?.length ?? 0) > 0}
			<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-3">Global Secondary Indexes ({tableDesc?.GlobalSecondaryIndexes?.length})</h3>
				<table class="w-full text-xs">
					<thead><tr class="border-b border-slate-200 dark:border-slate-700">
						<th class="text-left text-slate-500 pb-2">Index Name</th>
						<th class="text-left text-slate-500 pb-2">Status</th>
						<th class="text-left text-slate-500 pb-2">Items</th>
						<th class="text-left text-slate-500 pb-2">Projection</th>
					</tr></thead>
					<tbody>
						{#each tableDesc?.GlobalSecondaryIndexes ?? [] as gsi}
							<tr class="border-b border-slate-100 dark:border-slate-700 last:border-0">
								<td class="py-1.5 font-mono text-slate-800 dark:text-slate-200">{gsi.IndexName}</td>
								<td class="py-1.5"><span class="text-xs rounded bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300 px-1.5 py-0.5">{gsi.IndexStatus ?? 'ACTIVE'}</span></td>
								<td class="py-1.5 text-slate-600 dark:text-slate-400">{gsi.ItemCount ?? 0}</td>
								<td class="py-1.5 text-slate-600 dark:text-slate-400">{gsi.Projection?.ProjectionType ?? '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</section>
		{/if}

		<!-- LSIs -->
		{#if (tableDesc?.LocalSecondaryIndexes?.length ?? 0) > 0}
			<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-3">Local Secondary Indexes ({tableDesc?.LocalSecondaryIndexes?.length})</h3>
				<p class="text-xs text-amber-600 dark:text-amber-400 mb-2">LSIs are immutable — they cannot be added or removed after table creation.</p>
				<table class="w-full text-xs">
					<thead><tr class="border-b border-slate-200 dark:border-slate-700">
						<th class="text-left text-slate-500 pb-2">Index Name</th>
						<th class="text-left text-slate-500 pb-2">Items</th>
						<th class="text-left text-slate-500 pb-2">Projection</th>
					</tr></thead>
					<tbody>
						{#each tableDesc?.LocalSecondaryIndexes ?? [] as lsi}
							<tr class="border-b border-slate-100 dark:border-slate-700 last:border-0">
								<td class="py-1.5 font-mono text-slate-800 dark:text-slate-200">{lsi.IndexName}</td>
								<td class="py-1.5 text-slate-600 dark:text-slate-400">{lsi.ItemCount ?? 0}</td>
								<td class="py-1.5 text-slate-600 dark:text-slate-400">{lsi.Projection?.ProjectionType ?? '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</section>
		{/if}

		<!-- Table metadata -->
		<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
			<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-3">Table Details</h3>
			<dl class="grid grid-cols-2 gap-x-4 gap-y-2 text-xs">
				<dt class="text-slate-500 dark:text-slate-400">Table ID</dt>
				<dd class="font-mono text-slate-800 dark:text-slate-200 truncate">{tableDesc?.TableId ?? '—'}</dd>
				<dt class="text-slate-500 dark:text-slate-400">Created</dt>
				<dd class="text-slate-800 dark:text-slate-200">{formatDate(tableDesc?.CreationDateTime)}</dd>
				<dt class="text-slate-500 dark:text-slate-400">Encryption</dt>
				<dd class="text-slate-800 dark:text-slate-200">{tableDesc?.SSEDescription?.Status ?? 'AWS_OWNED_KMS'}</dd>
			</dl>
		</section>
	{/if}

	<!-- ── Items tab ───────────────────────────────────────────────────────── -->
	{#if activeTab === 'items'}
		<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 shadow-sm overflow-hidden">
			<div class="flex flex-wrap items-center justify-between gap-2 px-4 py-3 border-b border-slate-200 dark:border-slate-700">
				<span class="text-sm font-medium text-slate-800 dark:text-slate-200">
					{filteredItems.length}{itemsFilter ? ` / ${items.length}` : ''} item{items.length !== 1 ? 's' : ''} loaded
				</span>
				<div class="flex gap-2 flex-wrap">
					<input
						type="text"
						bind:value={itemsFilter}
						placeholder="Filter…"
						class="text-xs rounded-lg border border-slate-300 px-3 py-1.5 dark:bg-slate-700 dark:border-slate-600 dark:text-white"
					/>
					<button type="button" class="text-xs rounded-lg border border-slate-300 px-3 py-1.5 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700" onclick={() => void loadItems(true)} disabled={itemsLoading}>
						{itemsLoading ? 'Loading…' : 'Refresh'}
					</button>
					{#if items.length > 0}
						<button type="button" class="text-xs rounded-lg border border-green-500 text-green-700 px-3 py-1.5 hover:bg-green-50 dark:text-green-400 dark:border-green-600 dark:hover:bg-green-900/20" onclick={exportItemsJSON}>
							Export JSON
						</button>
						<button type="button" class="text-xs rounded-lg border border-teal-500 text-teal-700 px-3 py-1.5 hover:bg-teal-50 dark:text-teal-400 dark:border-teal-600 dark:hover:bg-teal-900/20" onclick={exportItemsCSV}>
							Export CSV
						</button>
					{/if}
				</div>
			</div>
			{#if itemsLoading && items.length === 0}
				<div class="p-8 text-center text-sm text-slate-500 dark:text-slate-400">Loading items…</div>
			{:else if items.length === 0}
				<div class="p-8 text-center text-sm text-slate-500 dark:text-slate-400">No items found in this table.</div>
			{:else if filteredItems.length === 0}
				<div class="p-8 text-center text-sm text-slate-500 dark:text-slate-400">No items match the filter.</div>
			{:else}
				<div class="overflow-x-auto">
					<table class="w-full text-xs">
						<thead class="bg-slate-50 dark:bg-slate-900">
							<tr>
								{#each itemColumns as col}
									<th class="text-left px-3 py-2 font-medium text-slate-600 dark:text-slate-400 whitespace-nowrap">{col}</th>
								{/each}
							</tr>
						</thead>
						<tbody>
							{#each filteredItems as item, i}
								<tr class="border-t border-slate-100 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-750 {i % 2 === 0 ? '' : 'bg-slate-50/50 dark:bg-slate-800/50'}">
									{#each itemColumns as col}
										<td class="px-3 py-2 max-w-xs truncate font-mono text-slate-800 dark:text-slate-200" title={item[col] ? cellValue(item[col]) : ''}>
											{#if item[col]}
												{cellValue(item[col])}
											{:else}
												<span class="text-slate-400">—</span>
											{/if}
										</td>
									{/each}
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
				{#if itemsHasMore}
					<div class="px-4 py-3 border-t border-slate-200 dark:border-slate-700">
						<button type="button" class="text-sm text-blue-600 dark:text-blue-400 hover:underline" onclick={() => void loadItems(false)} disabled={itemsLoading}>
							{itemsLoading ? 'Loading…' : 'Load more'}
						</button>
					</div>
				{/if}
			{/if}
		</section>
	{/if}

	<!-- ── Query tab ───────────────────────────────────────────────────────── -->
	{#if activeTab === 'query'}
		<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm space-y-3">
			<h3 class="text-sm font-semibold text-slate-900 dark:text-white">Query by Key</h3>
			{#if availableIndexes.length > 0}
				<div>
					<label for="query-index" class="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1">Index (optional)</label>
					<select id="query-index" class="w-full rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={queryIndexName}>
						<option value="">Table (primary key)</option>
						{#each availableIndexes as idx}
							<option value={idx}>{idx}</option>
						{/each}
					</select>
				</div>
			{/if}
			<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
				<div>
					<label for="query-pk-attr" class="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1">Partition Key Attribute</label>
					<input id="query-pk-attr" class="w-full rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={queryPK} placeholder="{tableDesc?.KeySchema?.[0]?.AttributeName ?? 'pk'}" />
				</div>
				<div>
					<label for="query-pk-value" class="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1">Partition Key Value</label>
					<input id="query-pk-value" class="w-full rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={queryPKValue} placeholder="value" />
				</div>
				<div>
					<label for="query-sk-attr" class="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1">Sort Key Attribute (optional)</label>
					<input id="query-sk-attr" class="w-full rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={querySKAttr} placeholder="sk" />
				</div>
				<div>
					<label for="query-sk-op" class="block text-xs font-medium text-slate-700 dark:text-slate-300 mb-1">Sort Key Condition</label>
					<div class="flex gap-2">
						<select id="query-sk-op" class="rounded-lg border border-slate-300 px-2 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={querySKOp}>
							<option value="=">=</option>
							<option value="<">&lt;</option>
							<option value="<=">&lt;=</option>
							<option value=">">&gt;</option>
							<option value=">=">&gt;=</option>
							<option value="begins_with">begins_with</option>
							<option value="between">between</option>
						</select>
						<input class="flex-1 rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={querySKValue} placeholder="value" />
						{#if querySKOp === 'between'}
							<input class="flex-1 rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={querySKValue2} placeholder="end value" />
						{/if}
					</div>
				</div>
			</div>
			<button type="button" class="rounded-lg bg-blue-600 text-white px-4 py-1.5 text-sm hover:bg-blue-700 transition-colors" onclick={runQuery} disabled={queryLoading}>
				{queryLoading ? 'Querying…' : 'Run Query'}
			</button>

			{#if queryResults.length > 0}
				<div class="overflow-x-auto border border-slate-200 dark:border-slate-700 rounded-lg">
					<table class="w-full text-xs">
						<thead class="bg-slate-50 dark:bg-slate-900">
							<tr>
								{#each queryColumns as col}
									<th class="text-left px-3 py-2 font-medium text-slate-600 dark:text-slate-400">{col}</th>
								{/each}
							</tr>
						</thead>
						<tbody>
							{#each queryResults as item}
								<tr class="border-t border-slate-100 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-750">
									{#each queryColumns as col}
										<td class="px-3 py-2 max-w-xs truncate font-mono text-slate-800 dark:text-slate-200">
											{item[col] ? cellValue(item[col]) : '—'}
										</td>
									{/each}
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
				<p class="text-xs text-slate-500 dark:text-slate-400">{queryResults.length} result{queryResults.length !== 1 ? 's' : ''}</p>
			{:else if !queryLoading}
				<p class="text-sm text-slate-500 dark:text-slate-400">No results yet. Run a query above.</p>
			{/if}
		</section>
	{/if}

	<!-- ── Backups tab ─────────────────────────────────────────────────────── -->
	{#if activeTab === 'backups'}
		<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm space-y-4">
			<div class="flex items-center justify-between">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">Backups</h3>
				<button type="button" class="text-xs border border-slate-300 rounded-lg px-3 py-1.5 text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700" onclick={loadBackups} disabled={backupsLoading}>Refresh</button>
			</div>
			<div class="flex gap-2">
				<input class="flex-1 rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={newBackupName} placeholder="Backup name (optional)" />
				<button type="button" class="rounded-lg bg-blue-600 text-white px-4 py-1.5 text-sm hover:bg-blue-700 transition-colors" onclick={createBackup}>Create Backup</button>
			</div>
			{#if backupsLoading}
				<div class="text-sm text-slate-500 dark:text-slate-400">Loading backups…</div>
			{:else if backups.length === 0}
				<div class="text-sm text-slate-500 dark:text-slate-400">No backups found.</div>
			{:else}
				<table class="w-full text-xs">
					<thead><tr class="border-b border-slate-200 dark:border-slate-700">
						<th class="text-left text-slate-500 pb-2">Name</th>
						<th class="text-left text-slate-500 pb-2">Status</th>
						<th class="text-left text-slate-500 pb-2">Created</th>
						<th class="text-left text-slate-500 pb-2">Size</th>
					</tr></thead>
					<tbody>
						{#each backups as b}
							<tr class="border-b border-slate-100 dark:border-slate-700 last:border-0">
								<td class="py-2 font-mono text-slate-800 dark:text-slate-200 truncate max-w-xs">{b.BackupName}</td>
								<td class="py-2"><span class="text-xs rounded bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300 px-1.5">{b.BackupStatus}</span></td>
								<td class="py-2 text-slate-500 dark:text-slate-400">{formatDate(b.BackupCreationDateTime)}</td>
								<td class="py-2 text-slate-500 dark:text-slate-400">{formatBytes(b.BackupSizeBytes ?? 0)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</section>
	{/if}

	<!-- ── Stream Events tab ───────────────────────────────────────────────── -->
	{#if activeTab === 'streams'}
		<div class="space-y-4">
			<!-- Header -->
			<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
				<div class="flex items-center justify-between mb-1">
					<div class="flex items-center gap-3">
						<h3 class="text-sm font-semibold text-slate-900 dark:text-white">DynamoDB Streams</h3>
						{#if streamsEnabled}
							<span class="text-xs bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300 px-2 py-0.5 rounded font-medium">ENABLED</span>
							<span class="text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300 px-2 py-0.5 rounded font-medium">{streamViewType}</span>
							<span class="flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
								<span class="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>Auto-refresh 3s
							</span>
						{:else}
							<span class="text-xs bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-400 px-2 py-0.5 rounded font-medium">DISABLED</span>
						{/if}
					</div>
				</div>
				{#if tableDesc?.LatestStreamArn}
					<p class="text-xs font-mono text-slate-400 dark:text-slate-500 break-all">{tableDesc.LatestStreamArn}</p>
				{/if}
			</section>

			{#if !streamsEnabled}
				<div class="rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-700 px-4 py-3 text-sm text-amber-700 dark:text-amber-400">
					Streams are not enabled for this table. Enable them in the Overview tab to see events.
				</div>
			{:else}
				<!-- Stats Cards -->
				{#if streamInfo}
					<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
						<div class="p-4 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
							<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Buffered Events</div>
							<div class="text-2xl font-bold text-blue-600 dark:text-blue-400 mt-1">{streamInfo.eventCount}</div>
						</div>
						<div class="p-4 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
							<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Active Shards</div>
							<div class="text-2xl font-bold text-purple-600 dark:text-purple-400 mt-1">{streamInfo.shards?.length ?? 0}</div>
						</div>
						<div class="p-4 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
							<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Consumption Lag</div>
							{#if streamInfo.eventCount === 0}
								<div class="text-2xl font-bold text-slate-400 dark:text-slate-500 mt-1">—</div>
							{:else if streamInfo.lagSeconds < 5}
								<div class="text-2xl font-bold text-green-600 dark:text-green-400 mt-1">&lt;5s</div>
							{:else}
								<div class="text-2xl font-bold text-amber-600 dark:text-amber-400 mt-1">{streamInfo.lagSeconds}s</div>
							{/if}
						</div>
						<div class="p-4 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
							<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Iterator Expiry</div>
							<div class="text-lg font-bold text-slate-700 dark:text-slate-300 mt-1">15 min</div>
							<div class="text-xs text-slate-400 dark:text-slate-500">from creation</div>
						</div>
					</div>

					<!-- Event Type Breakdown -->
					{#if streamInfo.eventCount > 0 && streamInfo.eventTypes}
						<div class="flex flex-wrap gap-3 text-xs">
							<div class="flex items-center gap-1.5 px-3 py-1.5 bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-700 rounded-lg">
								<span class="font-semibold text-green-700 dark:text-green-400">INSERT</span>
								<span class="text-green-600 dark:text-green-300 font-mono">{streamInfo.eventTypes.insert}</span>
							</div>
							<div class="flex items-center gap-1.5 px-3 py-1.5 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700 rounded-lg">
								<span class="font-semibold text-blue-700 dark:text-blue-400">MODIFY</span>
								<span class="text-blue-600 dark:text-blue-300 font-mono">{streamInfo.eventTypes.modify}</span>
							</div>
							<div class="flex items-center gap-1.5 px-3 py-1.5 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-700 rounded-lg">
								<span class="font-semibold text-red-700 dark:text-red-400">REMOVE</span>
								<span class="text-red-600 dark:text-red-300 font-mono">{streamInfo.eventTypes.remove}</span>
							</div>
							{#if streamInfo.latestEventUnix > 0}
								<div class="flex items-center gap-1.5 px-3 py-1.5 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg ml-auto">
									<span class="text-slate-500 dark:text-slate-400">Last event:</span>
									<span class="text-slate-700 dark:text-slate-300 font-mono">{new Date(streamInfo.latestEventUnix * 1000).toLocaleTimeString()}</span>
								</div>
							{/if}
						</div>
					{/if}

					<!-- Shards Table -->
					{#if streamInfo.shards && streamInfo.shards.length > 0}
						<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
							<h4 class="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-3">Shard Breakdown</h4>
							<div class="overflow-x-auto">
								<table class="w-full text-xs font-mono">
									<thead>
										<tr class="border-b border-slate-200 dark:border-slate-600">
											<th class="text-left pb-2 pr-4 text-slate-500 dark:text-slate-400 font-medium">Shard ID</th>
											<th class="text-left pb-2 pr-4 text-slate-500 dark:text-slate-400 font-medium">Start Sequence</th>
											<th class="text-left pb-2 pr-4 text-slate-500 dark:text-slate-400 font-medium">End Sequence</th>
											<th class="text-left pb-2 text-slate-500 dark:text-slate-400 font-medium">Iterator Expiry</th>
										</tr>
									</thead>
									<tbody>
										{#each streamInfo.shards as shard}
											<tr class="border-b border-slate-100 dark:border-slate-600 hover:bg-slate-50 dark:hover:bg-slate-600 transition-colors">
												<td class="py-2 pr-4 text-slate-700 dark:text-slate-300 truncate max-w-[200px]" title={shard.shardId}>{shard.shardId}</td>
												<td class="py-2 pr-4 text-slate-500 dark:text-slate-400">{shard.startSeq || '—'}</td>
												<td class="py-2 pr-4 text-slate-500 dark:text-slate-400">{shard.endSeq || '(open)'}</td>
												<td class="py-2 text-amber-600 dark:text-amber-400">15 min from creation</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
							{#if streamInfo.latestSeq}
								<div class="mt-3 flex gap-4 text-xs text-slate-500 dark:text-slate-400">
									<span>Oldest seq: <span class="font-mono">{streamInfo.oldestSeq || '—'}</span></span>
									<span>Latest seq: <span class="font-mono">{streamInfo.latestSeq || '—'}</span></span>
								</div>
							{/if}
						</section>
					{/if}
				{/if}

				<!-- Recent Events -->
				<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm">
					<div class="flex items-center justify-between mb-3">
						<h4 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Recent Events</h4>
						<div class="flex items-center gap-2">
							{#each ['ALL', 'INSERT', 'MODIFY', 'REMOVE'] as filter}
								<button
									onclick={() => { streamEventFilter = filter; }}
									class="text-xs px-2 py-0.5 rounded font-medium transition-colors {streamEventFilter === filter
										? 'bg-blue-600 text-white'
										: 'bg-slate-200 dark:bg-slate-600 text-slate-600 dark:text-slate-300 hover:bg-slate-300 dark:hover:bg-slate-500'}"
								>{filter}</button>
							{/each}
						</div>
					</div>
					<div class="min-h-[80px] text-xs text-slate-700 dark:text-slate-300 stream-events-filter" data-filter={streamEventFilter}>
						{@html streamEventsHtml || '<span class="text-slate-400 dark:text-slate-500">No recent stream events. Events appear here as you write to the table.</span>'}
					</div>
				</section>
			{/if}
		</div>
	{/if}

	<!-- ── PartiQL tab ─────────────────────────────────────────────────────── -->
	{#if activeTab === 'partiql'}
		<section class="rounded-xl border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 shadow-sm space-y-3">
			<div class="flex items-center justify-between">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white">PartiQL Editor</h3>
				{#if partiqlDurationMs > 0}
					<span class="text-xs text-slate-500 dark:text-slate-400">{partiqlDurationMs}ms</span>
				{/if}
			</div>
			<textarea
				name="statement"
				class="h-32 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm font-mono dark:bg-slate-700 dark:border-slate-600 dark:text-white focus:ring-2 focus:ring-blue-500 focus:outline-none"
				bind:value={statement}
				placeholder='SELECT * FROM "TableName"'
			></textarea>
			<button id="partiql-execute" type="button" class="rounded-lg bg-blue-600 text-white px-4 py-1.5 text-sm hover:bg-blue-700 transition-colors" onclick={executePartiQL} disabled={partiqlLoading}>
				{partiqlLoading ? 'Running…' : 'Execute'}
			</button>
			<div id="partiql-output" class="rounded-lg border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 p-3 text-xs min-h-[60px] overflow-x-auto">
				{#if partiqlOutput}
					<pre class="text-slate-800 dark:text-slate-200 whitespace-pre-wrap">{partiqlOutput}</pre>
				{:else}
					<span class="text-slate-400">Results will appear here…</span>
				{/if}
			</div>
		</section>
	{/if}
</div>

<!-- Delete Confirm Dialog -->
{#if showDeleteConfirm}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-xl p-6 w-full max-w-md mx-4">
			<h3 class="text-lg font-semibold text-red-600 dark:text-red-400 mb-2">Delete Table</h3>
			<p class="text-sm text-slate-700 dark:text-slate-300 mb-4">This action is irreversible. Type <strong>{tableName}</strong> to confirm deletion.</p>
			<input class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white mb-4 focus:ring-2 focus:ring-red-500 focus:outline-none" bind:value={deleteConfirmInput} placeholder={tableName} />
			<div class="flex gap-3 justify-end">
				<button type="button" class="rounded-lg border border-slate-300 px-4 py-2 text-sm text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700" onclick={() => { showDeleteConfirm = false; deleteConfirmInput = ''; }}>Cancel</button>
				<button type="button" class="rounded-lg bg-red-600 text-white px-4 py-2 text-sm hover:bg-red-700 disabled:opacity-50 transition-colors" onclick={deleteTable} disabled={deleteConfirmInput !== tableName || deleting}>
					{deleting ? 'Deleting…' : 'Delete'}
				</button>
			</div>
		</div>
	</div>
{/if}
