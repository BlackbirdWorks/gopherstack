<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount, onDestroy } from 'svelte';
	import { newDynamoDBClient, getStoredRegion } from '$lib/aws/client';
	import {
		ListTablesCommand,
		DescribeTableCommand,
		CreateTableCommand,
		DeleteTableCommand,
		DeleteItemCommand,
		BatchWriteItemCommand,
		ScanCommand,
		QueryCommand,
		PutItemCommand,
		ExecuteStatementCommand,
		DescribeTimeToLiveCommand,
		UpdateTimeToLiveCommand,
		UpdateTableCommand,
		ListBackupsCommand,
		CreateBackupCommand,
		DeleteBackupCommand,
		type TableDescription,
		type KeySchemaElement,
		type ScalarAttributeType,
		type AttributeValue,
		type StreamSpecification,
		type StreamViewType,
		type BackupSummary
	} from '@aws-sdk/client-dynamodb';
	import { toast } from 'svelte-sonner';
	import { avToJson, itemToJson, jsonToAv, jsonToItem, getColumns, getKeySchema, resolveKeySchema, buildKeyCondition } from '$lib/dynamodb';

	let ddb = $state(newDynamoDBClient());
	let currentRegion = $state(getStoredRegion());

	// Table List State
	let tableNames = $state<string[]>([]);
	let tableDetails = $state<Map<string, TableDescription>>(new Map());
	let loading = $state(true);
	let searchQuery = $state('');
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newTableName = $state('');
	let partitionKey = $state('');
	let partitionKeyType = $state<ScalarAttributeType>('S');
	let sortKey = $state('');
	let sortKeyType = $state<ScalarAttributeType>('S');

	// Detail State
	let selectedTable = $state<string | null>(null);
	let selectedTableDesc = $state<TableDescription | null>(null);
	let activeTab = $state<string>('overview');
	let showTableDetails = $state(false);

	// Query State
	let queryIndexName = $state('');
	let queryPKValue = $state('');
	let querySKOperator = $state('');
	let querySKValue = $state('');
	let querySKValue2 = $state('');
	let queryFilterExp = $state('');
	let queryLimit = $state(100);
	let queryResults = $state<Record<string, unknown>[]>([]);
	let queryLoading = $state(false);
	let queryCount = $state(0);
	let querySortOrder = $state<'ASC' | 'DESC'>('ASC');

	// Scan State
	let scanFilterExp = $state('');
	let scanProjectionExp = $state('');
	let scanLimit = $state(100);
	let scanResults = $state<Record<string, unknown>[]>([]);
	let scanLoading = $state(false);
	let scanCount = $state(0);
	let scanScannedCount = $state(0);

	// Items Tab State
	let itemsResults = $state<Record<string, unknown>[]>([]);
	let itemsLastKey = $state<Record<string, AttributeValue> | null>(null);
	let itemsLoading = $state(false);
	let itemsSelectedKeys = $state<Set<string>>(new Set());

	// Backups State
	let backups = $state<BackupSummary[]>([]);
	let backupsLoading = $state(false);
	let newBackupName = $state('');

	// PartiQL State
	let partiqlStatement = $state('');
	let partiqlResults = $state<Record<string, unknown>[]>([]);
	let partiqlLoading = $state(false);
	let partiqlCount = $state(0);
	let partiqlDuration = $state(0);

	// Stream Events State
	let streamEventsHtml = $state('');
	let streamEventsLoading = $state(false);
	let streamBackendUnavailable = $state(false);
	let streamPollTimer: ReturnType<typeof setInterval> | undefined;
	let streamFetchController: AbortController | undefined;

	// Stream Info State
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
	let streamInfoLoading = $state(false);
	let streamEventFilter = $state<string>('ALL');

	// Overview Config State
	let ttlAttribute = $state('');
	let ttlEnabled = $state(false);
	let streamsViewType = $state('NEW_AND_OLD_IMAGES');
	let streamsEnabled = $state(false);
	let streamARN = $state('');

	// Modals
	let showNewItemModal = $state(false);
	let newItemJson = $state('');
	let showImportModal = $state(false);
	let importJson = $state('');
	let showEditModal = $state(false);
	let editItemJson = $state('');

	// GSI Create Modal State
	let showCreateGsiModal = $state(false);
	let gsiIndexName = $state('');
	let gsiPkAttr = $state('');
	let gsiPkType = $state<ScalarAttributeType>('S');
	let gsiSkAttr = $state('');
	let gsiSkType = $state<ScalarAttributeType>('S');
	let gsiProjection = $state<'ALL' | 'KEYS_ONLY' | 'INCLUDE'>('ALL');
	let gsiNonKeyAttrs = $state('');
	let creatingGsi = $state(false);

	// Create Table Billing Mode
	let createTableBillingMode = $state<'PAY_PER_REQUEST' | 'PROVISIONED'>('PAY_PER_REQUEST');
	let createTableRcu = $state(5);
	let createTableWcu = $state(5);

	// Items Filter
	let filterItems = $state('');

	// Table Sort
	let tableSortOrder = $state<'alpha' | 'itemCount'>('alpha');

	// Derived
	const filteredTables = $derived.by(() => {
		const filtered = tableNames.filter((t) => !searchQuery || t.toLowerCase().includes(searchQuery.toLowerCase()));
		if (tableSortOrder === 'itemCount') {
			return filtered.toSorted((a, b) => (tableDetails.get(b)?.ItemCount ?? 0) - (tableDetails.get(a)?.ItemCount ?? 0));
		}
		return filtered.toSorted((a, b) => a.localeCompare(b));
	});

	// Pagination state
	const tablePageSize = 10;
	let tablePage = $state(0);
	const pagedTables = $derived(filteredTables.slice(tablePage * tablePageSize, (tablePage + 1) * tablePageSize));
	const totalTablePages = $derived(Math.ceil(filteredTables.length / tablePageSize));


	const filteredItemsResults = $derived(
		filterItems ? itemsResults.filter(item =>
			Object.values(item).some(v => String(v).toLowerCase().includes(filterItems.toLowerCase()))
		) : itemsResults
	);
	const currentKeySchema = $derived(resolveKeySchema(selectedTableDesc, queryIndexName || undefined));
	const tableKeySchema = $derived(resolveKeySchema(selectedTableDesc));

	// Helpers
	function formatBytes(bytes: number): string {
		if (bytes === 0) return '0 B';
		if (bytes < 1024) return `${bytes} B`;
		if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
		if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
		return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
	}

	function buildItemKey(item: Record<string, unknown>): Record<string, AttributeValue> {
		const key: Record<string, AttributeValue> = {};
		const { pkName, skName } = tableKeySchema;
		if (pkName && item[pkName] !== undefined) key[pkName] = jsonToAv(item[pkName]);
		if (skName && item[skName] !== undefined) key[skName] = jsonToAv(item[skName]);
		return key;
	}

	function exportJson(data: Record<string, unknown>[], filename: string): void {
		const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = filename;
		a.click();
		URL.revokeObjectURL(url);
	}

	// Table List Operations
	async function loadTables() {
		loading = true;
		try {
			const res = await ddb.send(new ListTablesCommand({}));
			tableNames = res.TableNames ?? [];
			const details = new Map<string, TableDescription>();
			for (const name of tableNames) {
				try {
					const desc = await ddb.send(new DescribeTableCommand({ TableName: name }));
					if (desc.Table) details.set(name, desc.Table);
				} catch {
                                    // skip
                                }
			}
			tableDetails = details;
			tablePage = 0;
		} catch (err: unknown) {
			toast.error(`Failed to list tables: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createTable() {
		if (!newTableName.trim() || !partitionKey.trim()) return;
		creating = true;
		try {
			const ks: KeySchemaElement[] = [{ AttributeName: partitionKey.trim(), KeyType: 'HASH' }];
			const ad = [{ AttributeName: partitionKey.trim(), AttributeType: partitionKeyType }];
			if (sortKey.trim()) {
				ks.push({ AttributeName: sortKey.trim(), KeyType: 'RANGE' });
				ad.push({ AttributeName: sortKey.trim(), AttributeType: sortKeyType });
			}
			await ddb.send(new CreateTableCommand({
				TableName: newTableName.trim(),
				KeySchema: ks,
				AttributeDefinitions: ad,
				BillingMode: createTableBillingMode,
				...(createTableBillingMode === 'PROVISIONED' ? {
					ProvisionedThroughput: { ReadCapacityUnits: createTableRcu, WriteCapacityUnits: createTableWcu }
				} : {})
			}));
			toast.success(`Table "${newTableName.trim()}" created`);
			showCreateModal = false;
			newTableName = ''; partitionKey = ''; partitionKeyType = 'S'; sortKey = ''; sortKeyType = 'S';
			createTableBillingMode = 'PAY_PER_REQUEST'; createTableRcu = 5; createTableWcu = 5;
			await loadTables();
		} catch (err: unknown) {
			toast.error(`Failed to create table: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function purgeAll() {
		if (!await confirmDestructive({ title: 'Delete All Tables', message: 'Delete ALL DynamoDB tables? This cannot be undone.', confirmLabel: 'Delete All' })) return;
		try {
			for (const name of tableNames) await ddb.send(new DeleteTableCommand({ TableName: name }));
			toast.success('All tables purged');
			selectedTable = null;
			await loadTables();
		} catch (err: unknown) {
			toast.error(`Failed to purge: ${(err as Error).message}`);
		}
	}

	async function deleteTable(name: string) {
		if (!await confirmDestructive({ title: 'Delete Table', message: `Delete table "${name}"? All items and indexes will be permanently removed.` })) return;
		try {
			await ddb.send(new DeleteTableCommand({ TableName: name }));
			toast.success(`Table "${name}" deleted`);
			if (selectedTable === name) { selectedTable = null; selectedTableDesc = null; }
			await loadTables();
		} catch (err: unknown) {
			toast.error(`Failed to delete table: ${(err as Error).message}`);
		}
	}

	// Detail Operations
	async function openTable(name: string) {
		selectedTable = name;
		activeTab = 'overview';
		queryResults = []; scanResults = []; partiqlResults = []; itemsResults = []; backups = [];
		itemsLastKey = null; itemsSelectedKeys = new Set();
		streamEventsHtml = ''; streamInfo = null;
		partiqlStatement = `SELECT * FROM "${name}"`;
		try {
			const desc = await ddb.send(new DescribeTableCommand({ TableName: name }));
			selectedTableDesc = desc.Table ?? null;
			try {
				const ttl = await ddb.send(new DescribeTimeToLiveCommand({ TableName: name }));
				ttlEnabled = ttl.TimeToLiveDescription?.TimeToLiveStatus === 'ENABLED';
				ttlAttribute = ttl.TimeToLiveDescription?.AttributeName ?? '';
			} catch {
                    // ignore
                }
			streamsEnabled = desc.Table?.StreamSpecification?.StreamEnabled ?? false;
			streamsViewType = desc.Table?.StreamSpecification?.StreamViewType ?? 'NEW_AND_OLD_IMAGES';
			streamARN = desc.Table?.LatestStreamArn ?? '';
		} catch (err: unknown) {
			toast.error(`Failed to describe table: ${(err as Error).message}`);
		}
	}

	async function refreshTable(): Promise<void> {
		if (selectedTable) await openTable(selectedTable);
	}

	// Query
	async function executeQuery() {
		if (!selectedTable || !queryPKValue) return;
		queryLoading = true;
		try {
			const { pkName, skName, pkType, skType } = currentKeySchema;
			const kc = buildKeyCondition(pkName, queryPKValue, pkType, skName, querySKOperator, querySKValue, querySKValue2, skType);
			const input = {
				TableName: selectedTable,
				KeyConditionExpression: kc.expression,
				ExpressionAttributeNames: kc.names,
				ExpressionAttributeValues: kc.values,
				Limit: queryLimit,
				ScanIndexForward: querySortOrder === 'ASC',
				...(queryIndexName ? { IndexName: queryIndexName } : {}),
				...(queryFilterExp ? { FilterExpression: queryFilterExp } : {})
			};
			const res = await ddb.send(new QueryCommand(input));
			queryResults = (res.Items ?? []).map((item) => itemToJson(item));
			queryCount = res.Count ?? 0;
		} catch (err: unknown) {
			toast.error(`Query failed: ${(err as Error).message}`);
		} finally {
			queryLoading = false;
		}
	}

	// Scan
	async function executeScan() {
		if (!selectedTable) return;
		scanLoading = true;
		try {
			const input = {
				TableName: selectedTable,
				Limit: scanLimit,
				...(scanFilterExp ? { FilterExpression: scanFilterExp } : {}),
				...(scanProjectionExp ? { ProjectionExpression: scanProjectionExp } : {})
			};
			const res = await ddb.send(new ScanCommand(input));
			scanResults = (res.Items ?? []).map((item) => itemToJson(item));
			scanCount = res.Count ?? 0;
			scanScannedCount = res.ScannedCount ?? 0;
		} catch (err: unknown) {
			toast.error(`Scan failed: ${(err as Error).message}`);
		} finally {
			scanLoading = false;
		}
	}

	// Items Tab
	async function loadItems(reset = true): Promise<void> {
		if (!selectedTable) return;
		itemsLoading = true;
		if (reset) {
			itemsResults = [];
			itemsLastKey = null;
			itemsSelectedKeys = new Set();
		}
		try {
			const res = await ddb.send(new ScanCommand({
				TableName: selectedTable,
				Limit: 25,
				...(itemsLastKey && !reset ? { ExclusiveStartKey: itemsLastKey } : {})
			}));
			const newItems = (res.Items ?? []).map((item) => itemToJson(item));
			itemsResults = reset ? newItems : [...itemsResults, ...newItems];
			itemsLastKey = res.LastEvaluatedKey ?? null;
		} catch (err: unknown) {
			toast.error(`Failed to load items: ${(err as Error).message}`);
		} finally {
			itemsLoading = false;
		}
	}

	function itemStableKey(item: Record<string, unknown>): string {
		const pkAttr = selectedTableDesc?.KeySchema?.[0]?.AttributeName ?? '';
		const skAttr = selectedTableDesc?.KeySchema?.[1]?.AttributeName ?? '';
		const pk = pkAttr ? String(item[pkAttr] ?? '') : '';
		const sk = skAttr ? String(item[skAttr] ?? '') : '';
		return pkAttr ? `${pk}\u0000${sk}` : JSON.stringify(item);
	}

	function toggleItemRow(item: Record<string, unknown>): void {
		const key = itemStableKey(item);
		const next = new Set(itemsSelectedKeys);
		if (next.has(key)) next.delete(key);
		else next.add(key);
		itemsSelectedKeys = next;
	}

	async function deleteItem(item: Record<string, unknown>): Promise<void> {
		if (!selectedTable) return;
		if (!await confirmDestructive({ title: 'Delete Item', message: 'Delete this item? This cannot be undone.', confirmLabel: 'Delete Item' })) return;
		try {
			await ddb.send(new DeleteItemCommand({ TableName: selectedTable, Key: buildItemKey(item) }));
			toast.success('Item deleted');
			if (activeTab === 'items') await loadItems(true);
			else if (activeTab === 'scan') await executeScan();
			else if (activeTab === 'query') await executeQuery();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	// Batch delete every selected item using BatchWriteItem in groups of 25
	// (the AWS-imposed maximum per request).
	async function batchDeleteSelectedItems(): Promise<void> {
		if (!selectedTable || itemsSelectedKeys.size === 0) return;
		const targets = filteredItemsResults.filter((it) => itemsSelectedKeys.has(itemStableKey(it)));
		if (targets.length === 0) return;
		if (!await confirmDestructive({
			title: 'Delete Selected',
			message: `Delete ${targets.length} item${targets.length === 1 ? '' : 's'}? This cannot be undone.`,
			confirmLabel: 'Delete'
		})) return;

		const tableName = selectedTable;
		try {
			let deleted = 0;
			for (let i = 0; i < targets.length; i += 25) {
				const chunk = targets.slice(i, i + 25);
				const requests = chunk.map((it) => ({ DeleteRequest: { Key: buildItemKey(it) } }));
				const out = await ddb.send(new BatchWriteItemCommand({ RequestItems: { [tableName]: requests } }));
				const unprocessed = out.UnprocessedItems?.[tableName]?.length ?? 0;
				deleted += chunk.length - unprocessed;
			}
			toast.success(`Deleted ${deleted} item${deleted === 1 ? '' : 's'}`);
			itemsSelectedKeys = new Set();
			if (activeTab === 'items') await loadItems(true);
			else if (activeTab === 'scan') await executeScan();
			else if (activeTab === 'query') await executeQuery();
		} catch (err: unknown) {
			toast.error(`Batch delete failed: ${(err as Error).message}`);
		}
	}

	// Backups
	async function loadBackups(): Promise<void> {
		if (!selectedTable) return;
		backupsLoading = true;
		try {
			const res = await ddb.send(new ListBackupsCommand({ TableName: selectedTable }));
			backups = res.BackupSummaries ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load backups: ${(err as Error).message}`);
		} finally {
			backupsLoading = false;
		}
	}

	async function createBackup(): Promise<void> {
		if (!selectedTable || !newBackupName.trim()) return;
		try {
			await ddb.send(new CreateBackupCommand({ TableName: selectedTable, BackupName: newBackupName.trim() }));
			toast.success(`Backup "${newBackupName.trim()}" created`);
			newBackupName = '';
			await loadBackups();
		} catch (err: unknown) {
			toast.error(`Create backup failed: ${(err as Error).message}`);
		}
	}

	async function deleteBackup(arn: string): Promise<void> {
		if (!await confirmDestructive({ title: 'Delete Backup', message: 'Delete this backup? This cannot be undone.' })) return;
		try {
			await ddb.send(new DeleteBackupCommand({ BackupArn: arn }));
			toast.success('Backup deleted');
			await loadBackups();
		} catch (err: unknown) {
			toast.error(`Delete backup failed: ${(err as Error).message}`);
		}
	}

	// TTL test
	async function testTTL(): Promise<void> {
		if (!selectedTable || !ttlAttribute.trim()) {
			toast.error('No TTL attribute configured');
			return;
		}
		try {
			const now = Math.floor(Date.now() / 1000);
			const oneHourFromNow = now + 3600;
			const res = await ddb.send(new ScanCommand({
				TableName: selectedTable,
				FilterExpression: '#ttlAttr BETWEEN :now AND :plus1h',
				ExpressionAttributeNames: { '#ttlAttr': ttlAttribute },
				ExpressionAttributeValues: { ':now': { N: String(now) }, ':plus1h': { N: String(oneHourFromNow) } }
			}));
			const count = res.Count ?? 0;
			toast.success(`${count} item${count === 1 ? '' : 's'} will expire in the next hour`);
		} catch (err: unknown) {
			toast.error(`TTL test failed: ${(err as Error).message}`);
		}
	}


	// PartiQL
	async function executePartiQL() {
		if (!partiqlStatement.trim()) return;
		partiqlLoading = true;
		partiqlDuration = 0;
		const t0 = Date.now();
		try {
			const res = await ddb.send(new ExecuteStatementCommand({ Statement: partiqlStatement }));
			partiqlDuration = Date.now() - t0;
			partiqlResults = (res.Items ?? []).map((item) => itemToJson(item));
			partiqlCount = res.Items?.length ?? 0;
		} catch (err: unknown) {
			toast.error(`PartiQL failed: ${(err as Error).message}`);
		} finally {
			partiqlLoading = false;
		}
	}

	// Stream Events
	async function loadStreamEvents() {
		if (!selectedTable) return;
		if (!streamEventsHtml) streamEventsLoading = true;
		const signal = streamFetchController?.signal;
		try {
			const res = await fetch(`/dashboard/dynamodb/table/${selectedTable}/stream-events`, { signal });
			if (res.ok) {
				const text = await res.text();
				if (signal?.aborted) return;
				if (text.includes('unavailable') || text.includes('Streams backend unavailable')) {
					streamBackendUnavailable = true;
					streamEventsHtml = '';
					stopStreamPolling();
				} else if (text === 'No recent stream events.' || text.trim() === '') {
					streamEventsHtml = '';
				} else {
					streamEventsHtml = text;
				}
			}
		} catch (err) {
			if ((err as Error)?.name === 'AbortError') return;
		}
		streamEventsLoading = false;
	}

	async function loadStreamInfo() {
		if (!selectedTable) return;
		streamInfoLoading = true;
		const signal = streamFetchController?.signal;
		try {
			const res = await fetch(`/dashboard/dynamodb/table/${selectedTable}/stream-info`, { signal });
			if (res.ok && !signal?.aborted) streamInfo = await res.json();
		} catch (err) {
			if ((err as Error)?.name === 'AbortError') return;
		}
		streamInfoLoading = false;
	}

	function stopStreamPolling() {
		if (streamPollTimer) {
			clearInterval(streamPollTimer);
			streamPollTimer = undefined;
		}
		if (streamFetchController) {
			streamFetchController.abort();
			streamFetchController = undefined;
		}
	}

	$effect(() => {
		if (activeTab === 'streams' && selectedTable && streamsEnabled) {
			stopStreamPolling();
			streamFetchController = new AbortController();
			streamBackendUnavailable = false;
			streamEventsHtml = '';
			streamInfo = null;
			streamEventFilter = 'ALL';
			loadStreamInfo();
			loadStreamEvents();
			streamPollTimer = setInterval(() => { loadStreamEvents(); loadStreamInfo(); }, 3000);
			return () => { stopStreamPolling(); };
		}
	});

	// Item CRUD
	async function createItem() {
		if (!selectedTable || !newItemJson.trim()) return;
		try {
			const json = JSON.parse(newItemJson);
			await ddb.send(new PutItemCommand({ TableName: selectedTable, Item: jsonToItem(json) }));
			toast.success('Item created');
			showNewItemModal = false;
			newItemJson = '';
			if (activeTab === 'scan') await executeScan();
		} catch (err: unknown) {
			toast.error(`Failed: ${(err as Error).message}`);
		}
	}

	async function importItems() {
		if (!selectedTable || !importJson.trim()) return;
		try {
			const items: unknown[] = JSON.parse(importJson);
			if (!Array.isArray(items)) throw new Error('Expected a JSON array');
			for (const item of items) {
				await ddb.send(new PutItemCommand({ TableName: selectedTable, Item: jsonToItem(item as Record<string, unknown>) }));
			}
			toast.success(`${items.length} items imported`);
			showImportModal = false;
			importJson = '';
			if (activeTab === 'scan') await executeScan();
		} catch (err: unknown) {
			toast.error(`Import failed: ${(err as Error).message}`);
		}
	}

	async function saveEditItem() {
		if (!selectedTable || !editItemJson.trim()) return;
		try {
			const json = JSON.parse(editItemJson);
			await ddb.send(new PutItemCommand({ TableName: selectedTable, Item: jsonToItem(json) }));
			toast.success('Item updated');
			showEditModal = false;
			editItemJson = '';
			if (activeTab === 'scan') await executeScan();
			else if (activeTab === 'query') await executeQuery();
			else if (activeTab === 'items') await loadItems(true);
		} catch (err: unknown) {
			toast.error(`Failed: ${(err as Error).message}`);
		}
	}

	function openEditItem(item: Record<string, unknown>) {
		editItemJson = JSON.stringify(item, null, 2);
		showEditModal = true;
	}

	function setPartiqlExample(query: string) {
		partiqlStatement = query;
	}

	// Config Updates
	async function updateTTL() {
		if (!selectedTable || !ttlAttribute.trim()) return;
		try {
			await ddb.send(new UpdateTimeToLiveCommand({
				TableName: selectedTable,
				TimeToLiveSpecification: { Enabled: ttlEnabled, AttributeName: ttlAttribute.trim() }
			}));
			toast.success(`TTL ${ttlEnabled ? 'enabled successfully' : 'disabled successfully'}`);
		} catch (err: unknown) {
			toast.error(`TTL update failed: ${(err as Error).message}`);
		}
	}

	async function updateStreams() {
		if (!selectedTable) return;
		try {
			const spec: StreamSpecification = { StreamEnabled: streamsEnabled };
			if (streamsEnabled) spec.StreamViewType = streamsViewType as StreamViewType;
			await ddb.send(new UpdateTableCommand({ TableName: selectedTable, StreamSpecification: spec }));
			toast.success(`Streams ${streamsEnabled ? 'enabled successfully' : 'disabled successfully'}`);
			const desc = await ddb.send(new DescribeTableCommand({ TableName: selectedTable }));
			selectedTableDesc = desc.Table ?? null;
			streamARN = desc.Table?.LatestStreamArn ?? '';
		} catch (err: unknown) {
			toast.error(`Streams update failed: ${(err as Error).message}`);
		}
	}

	$effect(() => {
		if (activeTab === 'items' && selectedTable) {
			void loadItems(true);
		}
	});

	$effect(() => {
		if (activeTab === 'backups' && selectedTable) {
			void loadBackups();
		}
	});

	function copyToClipboard(text: string): void {
		navigator.clipboard.writeText(text).then(() => toast.success('Copied to clipboard')).catch(() => toast.error('Failed to copy'));
	}

	async function deleteGsi(indexName: string): Promise<void> {
		if (!selectedTable) return;
		if (!await confirmDestructive({ title: 'Delete Global Secondary Index', message: `Delete GSI "${indexName}"? This cannot be undone and will impact existing queries.` })) return;
		try {
			await ddb.send(new UpdateTableCommand({
				TableName: selectedTable,
				GlobalSecondaryIndexUpdates: [{ Delete: { IndexName: indexName } }]
			}));
			toast.success(`GSI "${indexName}" deletion initiated`);
			await refreshTable();
		} catch (err: unknown) {
			toast.error(`Delete GSI failed: ${(err as Error).message}`);
		}
	}

	async function createGsi(): Promise<void> {
		if (!selectedTable || !gsiIndexName.trim() || !gsiPkAttr.trim()) return;
		creatingGsi = true;
		try {
			const keySchema: KeySchemaElement[] = [{ AttributeName: gsiPkAttr.trim(), KeyType: 'HASH' }];
			const attrDefs = [{ AttributeName: gsiPkAttr.trim(), AttributeType: gsiPkType }];
			if (gsiSkAttr.trim()) {
				keySchema.push({ AttributeName: gsiSkAttr.trim(), KeyType: 'RANGE' });
				attrDefs.push({ AttributeName: gsiSkAttr.trim(), AttributeType: gsiSkType });
			}
			const projection = gsiProjection === 'INCLUDE'
				? { ProjectionType: gsiProjection as 'INCLUDE', NonKeyAttributes: gsiNonKeyAttrs.split(',').map(s => s.trim()).filter(Boolean) }
				: { ProjectionType: gsiProjection as 'ALL' | 'KEYS_ONLY' };
			await ddb.send(new UpdateTableCommand({
				TableName: selectedTable,
				AttributeDefinitions: attrDefs,
				GlobalSecondaryIndexUpdates: [{
					Create: { IndexName: gsiIndexName.trim(), KeySchema: keySchema, Projection: projection }
				}]
			}));
			toast.success(`GSI "${gsiIndexName.trim()}" creation initiated`);
			showCreateGsiModal = false;
			gsiIndexName = ''; gsiPkAttr = ''; gsiPkType = 'S'; gsiSkAttr = ''; gsiSkType = 'S';
			gsiProjection = 'ALL'; gsiNonKeyAttrs = '';
			await refreshTable();
		} catch (err: unknown) {
			toast.error(`Create GSI failed: ${(err as Error).message}`);
		} finally {
			creatingGsi = false;
		}
	}

	onMount(() => {
		currentRegion = getStoredRegion();
		loadTables();

		function refreshRegionClient(region: string | null | undefined): void {
			if (!region || region === currentRegion) return;
			currentRegion = region;
			ddb = newDynamoDBClient(region);
			void loadTables();
		}

		const handleStorage = (e: StorageEvent) => {
			if (e.key === 'gopherstack_region') refreshRegionClient(e.newValue);
		};
		const handleRegionChange = (e: Event) => {
			const region = e instanceof CustomEvent && typeof e.detail === 'string'
				? e.detail
				: getStoredRegion();
			refreshRegionClient(region);
		};

		window.addEventListener('storage', handleStorage);
		window.addEventListener('gopherstack:region-change', handleRegionChange);
		return () => {
			window.removeEventListener('storage', handleStorage);
			window.removeEventListener('gopherstack:region-change', handleRegionChange);
		};
	});
	onDestroy(() => { stopStreamPolling(); });

	$effect(() => {
		if (searchQuery || tableSortOrder) {
			tablePage = 0;

			return;
		}

		tablePage = 0;
	});

	$effect(() => {
		if (tablePage < totalTablePages - 1) {
			return;
		}

		tablePage = Math.max(0, totalTablePages - 1);
	});

	// Apply client-side event-type filter to the rendered HTML events table.
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
{#snippet resultsTable(items: Record<string, unknown>[])}
	{#if items.length === 0}
		<p class="text-sm text-slate-500 dark:text-slate-400 py-4">No results.</p>
	{:else}
		<div class="overflow-x-auto mt-4">
			<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
				<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
					<tr>
						{#each getColumns(items) as col}
							<th class="px-4 py-3">{col}</th>
						{/each}
						<th class="px-4 py-3">Actions</th>
					</tr>
				</thead>
				<tbody>
					{#each items as item}
						<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700/50">
							{#each getColumns(items) as col}
								<td class="px-4 py-3 font-mono text-xs max-w-[200px] truncate" title={String(item[col] ?? '')}>{item[col] ?? ''}</td>
							{/each}
							<td class="px-4 py-3 whitespace-nowrap">
								<button onclick={() => openEditItem(item)} class="text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 mr-2">Edit</button>
								<button onclick={() => deleteItem(item)} class="text-xs text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300">Delete</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
{/snippet}

<div class="space-y-6">
	{#if selectedTable}
		<nav class="flex" aria-label="Breadcrumb">
			<ol class="inline-flex items-center space-x-1 md:space-x-2">
				<li class="inline-flex items-center">
					<button onclick={() => { selectedTable = null; selectedTableDesc = null; }}
						class="inline-flex items-center text-sm font-medium text-slate-700 hover:text-blue-600 dark:text-slate-400 dark:hover:text-white">
						<svg class="w-3 h-3 me-2.5" fill="currentColor" viewBox="0 0 20 20"><path d="m19.707 9.293-2-2-7-7a1 1 0 0 0-1.414 0l-7 7-2 2a1 1 0 0 0 1.414 1.414L2 10.414V18a2 2 0 0 0 2 2h3a1 1 0 0 0 1-1v-4a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v4a1 1 0 0 0 1 1h3a2 2 0 0 0 2-2v-7.586l.293.293a1 1 0 0 0 1.414-1.414Z" /></svg>
						Tables
					</button>
				</li>
				<li>
					<div class="flex items-center">
						<svg class="w-3 h-3 text-slate-400 mx-1" fill="none" viewBox="0 0 6 10"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 9 4-4-4-4" /></svg>
						<span class="ms-1 text-sm font-medium text-slate-500 dark:text-slate-400">{selectedTable}</span>
					</div>
				</li>
			</ol>
		</nav>

		<div class="flex justify-between items-center flex-wrap gap-4">
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">{selectedTable}</h1>
			<div class="flex gap-2 flex-wrap">
				<button onclick={() => { showNewItemModal = true; }} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-4 py-2 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800">New Item</button>
				<button onclick={() => { showImportModal = true; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700">Import JSON</button>
				<a href="/dashboard/dynamodb/table/{selectedTable}/export" target="_blank" class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700">Export JSON</a>
				<button onclick={() => deleteTable(selectedTable!)} class="text-white bg-red-700 hover:bg-red-800 focus:ring-4 focus:ring-red-300 font-medium rounded-lg text-sm px-4 py-2 dark:bg-red-600 dark:hover:bg-red-700 dark:focus:ring-red-900">Delete Table</button>
			</div>
		</div>

		<div class="mb-4 border-b border-slate-200 dark:border-slate-700">
			<ul class="flex flex-wrap -mb-px text-sm font-medium text-center">
				{#each [['overview', 'Overview'], ['query', 'Query'], ['scan', 'Scan'], ['items', 'Items'], ['streams', 'Stream Events'], ['partiql', 'PartiQL'], ['metrics', 'Metrics'], ['backups', 'Backups']] as [id, label]}
					<li class="me-2">
						<button onclick={() => { activeTab = id; }}
							class="inline-block p-4 border-b-2 rounded-t-lg {activeTab === id ? 'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' : 'border-transparent hover:text-slate-600 hover:border-slate-300 dark:hover:text-slate-300'}">
							{label}
						</button>
					</li>
				{/each}
			</ul>
		</div>

		{#if activeTab === 'overview'}
			<div class="space-y-6">
				<div class="flex justify-end">
					<button onclick={refreshTable} class="py-1.5 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700 flex items-center gap-2">
						<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
						Refresh
					</button>
				</div>
				<!-- Table Details expandable -->
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<button type="button" onclick={() => { showTableDetails = !showTableDetails; }} class="flex items-center gap-2 text-base font-bold text-slate-900 dark:text-white w-full text-left">
						<svg class="w-4 h-4 transition-transform {showTableDetails ? 'rotate-90' : ''}" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M9 5l7 7-7 7" /></svg>
						Table Details
					</button>
					{#if showTableDetails}
						<dl class="mt-4 grid grid-cols-1 md:grid-cols-2 gap-x-8 gap-y-3 text-sm">
							<div>
								<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
								<dd class="font-mono text-xs break-all text-slate-900 dark:text-white mt-0.5 flex items-start gap-2">
									<span>{selectedTableDesc?.TableArn ?? '-'}</span>
									{#if selectedTableDesc?.TableArn}
										<button onclick={() => copyToClipboard(selectedTableDesc?.TableArn ?? '')} class="text-slate-400 hover:text-blue-600 dark:hover:text-blue-400 shrink-0 mt-0.5" title="Copy ARN">
											<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>
										</button>
									{/if}
								</dd>
							</div>
							<div><dt class="text-slate-500 dark:text-slate-400">Creation Date</dt><dd class="text-slate-900 dark:text-white mt-0.5">{selectedTableDesc?.CreationDateTime ? new Date(selectedTableDesc.CreationDateTime).toLocaleString() : '-'}</dd></div>
							<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="mt-0.5"><span class="bg-green-100 text-green-800 text-xs font-medium px-2 py-0.5 rounded dark:bg-green-900 dark:text-green-300">{selectedTableDesc?.TableStatus ?? '-'}</span></dd></div>
							<div><dt class="text-slate-500 dark:text-slate-400">Billing Mode</dt><dd class="mt-0.5"><span class="bg-blue-100 text-blue-800 text-xs font-medium px-2 py-0.5 rounded dark:bg-blue-900 dark:text-blue-300">{selectedTableDesc?.BillingModeSummary?.BillingMode ?? 'PROVISIONED'}</span></dd></div>
							<div><dt class="text-slate-500 dark:text-slate-400">Table Size</dt><dd class="text-slate-900 dark:text-white mt-0.5">{formatBytes(selectedTableDesc?.TableSizeBytes ?? 0)}</dd></div>
						</dl>
					{/if}
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<h2 class="mb-4 text-xl font-bold text-slate-900 dark:text-white">Key Schema</h2>
					<div class="overflow-x-auto">
						<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
							<thead class="text-xs text-slate-700 uppercase bg-slate-50/50 dark:bg-slate-800/50 dark:text-slate-400">
								<tr><th class="px-6 py-3">Attribute Name</th><th class="px-6 py-3">Key Type</th><th class="px-6 py-3">Attribute Type</th></tr>
							</thead>
							<tbody>
								<tr class="border-b border-slate-200 dark:border-slate-700">
									<td class="px-6 py-4 font-mono">{tableKeySchema.pkName}</td>
									<td class="px-6 py-4"><span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-blue-900 dark:text-blue-300">HASH</span></td>
									<td class="px-6 py-4"><span class="bg-slate-100 text-slate-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-slate-700 dark:text-slate-300">{tableKeySchema.pkType}</span></td>
								</tr>
								{#if tableKeySchema.skName}
									<tr class="border-b border-slate-200 dark:border-slate-700">
										<td class="px-6 py-4 font-mono">{tableKeySchema.skName}</td>
										<td class="px-6 py-4"><span class="bg-purple-100 text-purple-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-purple-900 dark:text-purple-300">RANGE</span></td>
										<td class="px-6 py-4"><span class="bg-slate-100 text-slate-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-slate-700 dark:text-slate-300">{tableKeySchema.skType}</span></td>
									</tr>
								{/if}
							</tbody>
						</table>
					</div>
				</div>
				<!-- Hero stats grid -->
				<div class="grid grid-cols-2 md:grid-cols-4 gap-6">
					<div class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Item Count</div>
						<div class="text-3xl font-bold text-blue-600 dark:text-blue-400 mt-2">{selectedTableDesc?.ItemCount ?? 0}</div>
						<div class="text-xs text-slate-400 dark:text-slate-500 mt-1">records stored</div>
					</div>
					<div class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Table Size</div>
						<div class="text-3xl font-bold text-emerald-600 dark:text-emerald-400 mt-2">{formatBytes(selectedTableDesc?.TableSizeBytes ?? 0)}</div>
						<div class="text-xs text-slate-400 dark:text-slate-500 mt-1">approximate size</div>
					</div>
					<div class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Billing Mode</div>
						<div class="text-xl font-bold text-slate-800 dark:text-slate-100 mt-2">
							{selectedTableDesc?.BillingModeSummary?.BillingMode === 'PAY_PER_REQUEST' ? 'On-Demand' : 'Provisioned'}
						</div>
						{#if selectedTableDesc?.BillingModeSummary?.BillingMode !== 'PAY_PER_REQUEST'}
							<div class="text-xs text-slate-400 dark:text-slate-500 mt-1">{selectedTableDesc?.ProvisionedThroughput?.ReadCapacityUnits ?? 0} RCU / {selectedTableDesc?.ProvisionedThroughput?.WriteCapacityUnits ?? 0} WCU</div>
						{:else}
							<div class="text-xs text-slate-400 dark:text-slate-500 mt-1">pay per request</div>
						{/if}
					</div>
					<div class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Table Status</div>
						<div class="flex items-center gap-2 mt-2">
							{#if selectedTableDesc?.TableStatus === 'ACTIVE'}
								<span class="w-3 h-3 rounded-full bg-green-500 shrink-0"></span>
							{:else if selectedTableDesc?.TableStatus === 'CREATING' || selectedTableDesc?.TableStatus === 'UPDATING'}
								<span class="w-3 h-3 rounded-full bg-yellow-400 animate-spin shrink-0"></span>
							{:else}
								<span class="w-3 h-3 rounded-full bg-slate-400 shrink-0"></span>
							{/if}
							<span class="text-lg font-bold text-slate-800 dark:text-slate-100">{selectedTableDesc?.TableStatus ?? '-'}</span>
						</div>
					</div>
				</div>
				<!-- Secondary stats grid -->
				<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">GSIs</div>
						<div class="text-2xl font-bold text-purple-600 dark:text-purple-400 mt-1">{selectedTableDesc?.GlobalSecondaryIndexes?.length ?? 0}</div>
						<div class="text-xs text-slate-400 dark:text-slate-500 mt-0.5">global indexes</div>
					</div>
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">LSIs</div>
						<div class="text-2xl font-bold text-yellow-600 dark:text-yellow-400 mt-1">{selectedTableDesc?.LocalSecondaryIndexes?.length ?? 0}</div>
						<div class="text-xs text-slate-400 dark:text-slate-500 mt-0.5">local indexes</div>
					</div>
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">TTL</div>
						<div class="mt-1">
							{#if ttlEnabled}
								<span class="text-sm font-bold text-green-600 dark:text-green-400">Enabled</span>
								<div class="text-xs text-slate-400 dark:text-slate-500 mt-0.5 truncate">{ttlAttribute}</div>
							{:else}
								<span class="text-sm font-bold text-slate-500 dark:text-slate-400">Disabled</span>
							{/if}
						</div>
					</div>
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Streams</div>
						<div class="mt-1">
							{#if streamsEnabled}
								<div class="flex items-center gap-1.5">
									<span class="w-2 h-2 rounded-full bg-blue-500 animate-pulse shrink-0"></span>
									<span class="text-sm font-bold text-blue-600 dark:text-blue-400">Enabled</span>
								</div>
								<div class="text-xs text-slate-400 dark:text-slate-500 mt-0.5 truncate">{streamsViewType}</div>
							{:else}
								<span class="text-sm font-bold text-slate-500 dark:text-slate-400">Disabled</span>
							{/if}
						</div>
					</div>
				</div>
								<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<h2 class="mb-4 text-sm font-bold tracking-tight text-slate-900 dark:text-white uppercase">Time To Live (TTL) Configuration</h2>
					<form onsubmit={(e) => { e.preventDefault(); updateTTL(); }} class="flex items-end gap-4 flex-wrap">
						<div class="flex-1 min-w-[200px]">
							<label for="ttl-attr" class="block mb-2 text-xs font-medium text-slate-900 dark:text-white">TTL Attribute Name</label>
							<input type="text" id="ttl-attr" name="attributeName" bind:value={ttlAttribute} placeholder="e.g. expires" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div class="flex items-center h-10">
							<input id="ttl-enabled" type="checkbox" bind:checked={ttlEnabled} class="w-4 h-4 text-blue-600 bg-slate-100 border-slate-300 rounded focus:ring-blue-500 dark:bg-slate-700 dark:border-slate-600" />
							<label for="ttl-enabled" class="ms-2 text-sm font-medium text-slate-900 dark:text-slate-300">Enabled</label>
						</div>
						<button type="submit" class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-4 py-2 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800">Update TTL</button>
						<button type="button" onclick={testTTL} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Test TTL</button>
					</form>
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<h2 class="mb-4 text-sm font-bold tracking-tight text-slate-900 dark:text-white uppercase">DynamoDB Streams Configuration</h2>
					<form onsubmit={(e) => { e.preventDefault(); updateStreams(); }} class="flex items-end gap-4 flex-wrap">
						<div class="flex-1 min-w-[200px]">
							<label for="streams-vt" class="block mb-2 text-xs font-medium text-slate-900 dark:text-white">View Type</label>
							<select id="streams-vt" name="viewType" bind:value={streamsViewType} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2 dark:bg-slate-700 dark:border-slate-600 dark:text-white">
								<option value="NEW_AND_OLD_IMAGES">NEW_AND_OLD_IMAGES</option>
								<option value="NEW_IMAGE">NEW_IMAGE</option>
								<option value="OLD_IMAGE">OLD_IMAGE</option>
								<option value="KEYS_ONLY">KEYS_ONLY</option>
							</select>
						</div>
						<div class="flex items-center h-10">
							<input id="streams-enabled" type="checkbox" bind:checked={streamsEnabled} class="w-4 h-4 text-blue-600 bg-slate-100 border-slate-300 rounded focus:ring-blue-500 dark:bg-slate-700 dark:border-slate-600" />
							<label for="streams-enabled" class="ms-2 text-sm font-medium text-slate-900 dark:text-slate-300">Enabled</label>
						</div>
						<button type="submit" class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-4 py-2 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800">Update Streams</button>
					</form>
					{#if streamARN}
						<div class="mt-4 p-2 bg-slate-50 dark:bg-slate-700 rounded border border-slate-200 dark:border-slate-600">
							<div class="text-[10px] text-slate-500 dark:text-slate-400 font-mono break-all">ARN: {streamARN}</div>
						</div>
					{/if}
				</div>
				<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<div class="flex items-center justify-between mb-4">
						<h2 class="text-xl font-bold text-slate-900 dark:text-white">Global Secondary Indexes</h2>
						<button onclick={() => { showCreateGsiModal = true; }} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-xs px-3 py-1.5 dark:bg-blue-600 dark:hover:bg-blue-700">+ Add GSI</button>
					</div>
					{#if selectedTableDesc?.GlobalSecondaryIndexes?.length}
						<div class="overflow-x-auto">
							<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
								<thead class="text-xs text-slate-700 uppercase bg-slate-50/50 dark:bg-slate-800/50 dark:text-slate-400">
									<tr><th class="px-6 py-3">Index Name</th><th class="px-6 py-3">Partition Key</th><th class="px-6 py-3">Sort Key</th><th class="px-6 py-3">Status</th><th class="px-6 py-3">Item Count</th><th class="px-6 py-3">Projection</th><th class="px-6 py-3">Actions</th></tr>
								</thead>
								<tbody>
									{#each selectedTableDesc.GlobalSecondaryIndexes ?? [] as gsi}
										<tr class="border-b border-slate-200 dark:border-slate-700">
											<td class="px-6 py-4 font-mono">{gsi.IndexName}</td>
											<td class="px-6 py-4 font-mono">{gsi.KeySchema?.find((k) => k.KeyType === 'HASH')?.AttributeName ?? ''}</td>
											<td class="px-6 py-4 font-mono">{gsi.KeySchema?.find((k) => k.KeyType === 'RANGE')?.AttributeName ?? '-'}</td>
											<td class="px-6 py-4"><span class="bg-green-100 text-green-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-green-900 dark:text-green-300">{gsi.IndexStatus ?? 'ACTIVE'}</span></td>
											<td class="px-6 py-4">{gsi.ItemCount ?? 0}</td>
											<td class="px-6 py-4"><span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-blue-900 dark:text-blue-300">{gsi.Projection?.ProjectionType ?? ''}</span></td>
											<td class="px-6 py-4">
												<button onclick={() => gsi.IndexName && deleteGsi(gsi.IndexName)} class="text-xs text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300">Delete</button>
											</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{:else}
						<p class="text-sm text-slate-500 dark:text-slate-400">No global secondary indexes defined.</p>
					{/if}
				</div>
				{#if selectedTableDesc?.LocalSecondaryIndexes?.length}
					<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="flex items-start justify-between mb-4 gap-4">
							<h2 class="text-xl font-bold text-slate-900 dark:text-white">Local Secondary Indexes</h2>
							<div class="flex items-center gap-2 text-xs text-amber-700 dark:text-amber-400 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded-lg px-3 py-1.5 shrink-0">
								<svg class="w-3.5 h-3.5 shrink-0" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 5zm0 9a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
								LSIs cannot be modified after table creation
							</div>
						</div>
						<div class="overflow-x-auto">
							<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
								<thead class="text-xs text-slate-700 uppercase bg-slate-50/50 dark:bg-slate-800/50 dark:text-slate-400">
									<tr><th class="px-6 py-3">Index Name</th><th class="px-6 py-3">SK Attr</th><th class="px-6 py-3">Projection Type</th></tr>
								</thead>
								<tbody>
									{#each selectedTableDesc.LocalSecondaryIndexes ?? [] as lsi}
										<tr class="border-b border-slate-200 dark:border-slate-700">
											<td class="px-6 py-4 font-mono">{lsi.IndexName}</td>
											<td class="px-6 py-4 font-mono">{lsi.KeySchema?.find((k) => k.KeyType === 'RANGE')?.AttributeName ?? '-'}</td>
											<td class="px-6 py-4"><span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-blue-900 dark:text-blue-300">{lsi.Projection?.ProjectionType ?? ''}</span></td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{/if}
			</div>
		{:else if activeTab === 'query'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800 space-y-6">
				<div class="flex items-center p-4 text-sm text-blue-800 border border-blue-300 rounded-lg bg-blue-50 dark:bg-slate-800 dark:text-blue-400 dark:border-blue-800">
					<svg class="flex-shrink-0 w-4 h-4 me-3" fill="currentColor" viewBox="0 0 20 20"><path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z" /></svg>
					<div>Query requires a partition key value. Sort key conditions are optional.</div>
				</div>
				<form onsubmit={(e) => { e.preventDefault(); executeQuery(); }} class="space-y-4">
					<div>
						<label for="q-index" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Index (Optional)</label>
						<select id="q-index" bind:value={queryIndexName} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:text-white">
							<option value="">Table (Default)</option>
							{#each selectedTableDesc?.GlobalSecondaryIndexes ?? [] as gsi}
								<option value={gsi.IndexName}>{gsi.IndexName} (GSI)</option>
							{/each}
							{#each selectedTableDesc?.LocalSecondaryIndexes ?? [] as lsi}
								<option value={lsi.IndexName}>{lsi.IndexName} (LSI)</option>
							{/each}
						</select>
					</div>
					<div>
						<label for="q-pk" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Partition Key Value ({currentKeySchema.pkName})</label>
						<input type="text" id="q-pk" bind:value={queryPKValue} required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
					</div>
					{#if currentKeySchema.skName}
						<div>
							<label for="q-sk-op" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort Key Condition ({currentKeySchema.skName})</label>
							<div class="flex gap-2">
								<select id="q-sk-op" bind:value={querySKOperator} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-32 p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:text-white">
									<option value="">None</option>
									<option value="=">=</option>
									<option value="<">&lt;</option>
									<option value="<=">&lt;=</option>
									<option value=">">&gt;</option>
									<option value=">=">&gt;=</option>
									<option value="BETWEEN">BETWEEN</option>
									<option value="begins_with">begins_with</option>
								</select>
								<input type="text" bind:value={querySKValue} placeholder="Value" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block flex-1 p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
								<input type="text" bind:value={querySKValue2} placeholder="Value 2 (for BETWEEN)" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block flex-1 p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
							</div>
						</div>
					{/if}
					<div>
						<label for="q-filter" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Filter Expression (Optional)</label>
						<textarea id="q-filter" bind:value={queryFilterExp} rows="2" placeholder="attribute_exists(email)" class="block p-2.5 w-full text-sm text-slate-900 bg-slate-50 rounded-lg border border-slate-300 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white"></textarea>
					</div>
					<div>
						<label for="q-limit" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Limit</label>
						<input type="number" id="q-limit" bind:value={queryLimit} min="1" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:text-white" />
					</div>
					<div>
						<p class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort Order</p>
						<div class="flex gap-6">
							<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer">
								<input type="radio" bind:group={querySortOrder} value="ASC" class="w-4 h-4 text-blue-600" /> Ascending
							</label>
							<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer">
								<input type="radio" bind:group={querySortOrder} value="DESC" class="w-4 h-4 text-blue-600" /> Descending
							</label>
						</div>
					</div>
					<button type="submit" disabled={queryLoading} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800 disabled:opacity-50">
						{queryLoading ? 'Querying...' : 'Execute Query'}
					</button>
				</form>
				{#if queryCount > 0}
					<div class="flex items-center justify-between">
						<p class="text-sm text-slate-600 dark:text-slate-400">Showing {queryResults.length} of {queryCount} result{queryCount !== 1 ? 's' : ''}</p>
						<div class="flex gap-2">
							<button onclick={() => { queryResults = []; queryCount = 0; }} class="py-1.5 px-3 text-xs font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Clear</button>
							<button onclick={() => exportJson(queryResults, `${selectedTable ?? 'query'}-results.json`)} class="py-1.5 px-3 text-xs font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Export JSON</button>
						</div>
					</div>
				{/if}
				{@render resultsTable(queryResults)}
			</div>
		{:else if activeTab === 'scan'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800 space-y-6">
				<div class="flex items-center p-4 text-sm text-yellow-800 border border-yellow-300 rounded-lg bg-yellow-50 dark:bg-slate-800 dark:text-yellow-300 dark:border-yellow-800">
					<svg class="flex-shrink-0 w-4 h-4 me-3" fill="currentColor" viewBox="0 0 20 20"><path d="M10 .5a9.5 9.5 0 1 0 9.5 9.5A9.51 9.51 0 0 0 10 .5ZM9.5 4a1.5 1.5 0 1 1 0 3 1.5 1.5 0 0 1 0-3ZM12 15H8a1 1 0 0 1 0-2h1v-3H8a1 1 0 0 1 0-2h2a1 1 0 0 1 1 1v4h1a1 1 0 0 1 0 2Z" /></svg>
					<div>Scan operations read every item in the table and can be expensive for large tables.</div>
				</div>
				<form onsubmit={(e) => { e.preventDefault(); executeScan(); }} class="space-y-4">
					<div>
						<label for="s-filter" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Filter Expression (Optional)</label>
						<textarea id="s-filter" bind:value={scanFilterExp} rows="2" placeholder="age > :minAge" class="block p-2.5 w-full text-sm text-slate-900 bg-slate-50 rounded-lg border border-slate-300 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white"></textarea>
					</div>
					<div>
						<label for="s-proj" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Projection Expression (Optional)</label>
						<input type="text" id="s-proj" bind:value={scanProjectionExp} placeholder="id, name, email" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
					</div>
					<div>
						<label for="s-limit" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Limit</label>
						<input type="number" id="s-limit" bind:value={scanLimit} min="1" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:text-white" />
					</div>
					<button type="submit" disabled={scanLoading} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800 disabled:opacity-50">
						{scanLoading ? 'Scanning...' : 'Execute Scan'}
					</button>
				</form>
				{#if scanCount > 0}
					<div class="flex items-center justify-between">
						<p class="text-sm text-slate-600 dark:text-slate-400">{scanCount} matched / {scanScannedCount} scanned</p>
						<div class="flex gap-2">
							<button onclick={() => { scanResults = []; scanCount = 0; scanScannedCount = 0; }} class="py-1.5 px-3 text-xs font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Clear</button>
							<button onclick={() => exportJson(scanResults, `${selectedTable ?? 'scan'}-results.json`)} class="py-1.5 px-3 text-xs font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Export JSON</button>
						</div>
					</div>
				{/if}
				{@render resultsTable(scanResults)}
			</div>
		{:else if activeTab === 'items'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800 space-y-4">
				<div class="flex items-center justify-between">
					<h3 class="text-lg font-semibold text-slate-900 dark:text-white">
						All Items
						{#if itemsSelectedKeys.size > 0}
							<span class="ml-2 text-sm font-normal text-slate-500 dark:text-slate-400">{itemsSelectedKeys.size} of {filteredItemsResults.length} selected</span>
						{/if}
					</h3>
					<div class="flex items-center gap-2">
						{#if itemsSelectedKeys.size > 0}
							<button onclick={() => batchDeleteSelectedItems()} class="py-1.5 px-4 text-sm font-medium text-white bg-red-600 hover:bg-red-700 rounded-lg">
								Delete Selected ({itemsSelectedKeys.size})
							</button>
						{/if}
						<button onclick={() => loadItems(true)} disabled={itemsLoading} class="py-1.5 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 disabled:opacity-50 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">
							{itemsLoading ? 'Loading...' : 'Reload'}
						</button>
					</div>
				</div>
				<div>
					<input type="text" bind:value={filterItems} placeholder="Filter items..." class="block w-full p-2.5 text-sm text-slate-900 border border-slate-300 rounded-lg bg-white focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
				</div>
				{#if itemsLoading && itemsResults.length === 0}
					<div class="flex justify-center p-8">
						<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" /><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" /></svg>
					</div>
				{:else if itemsResults.length > 0}
					<p class="text-sm text-slate-600 dark:text-slate-400">Showing {filteredItemsResults.length}{filterItems ? ` of ${itemsResults.length}` : ''} items{itemsLastKey ? ' (more available)' : ''}</p>
					<div class="overflow-x-auto">
						<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
							<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
								<tr>
									<th class="px-3 py-3 w-8"><input type="checkbox" class="w-4 h-4" onchange={(e) => { const cb = e.currentTarget as HTMLInputElement; itemsSelectedKeys = cb.checked ? new Set(filteredItemsResults.map(item => itemStableKey(item))) : new Set(); }} /></th>
									{#each getColumns(filteredItemsResults) as col}
										<th class="px-4 py-3">{col}</th>
									{/each}
									<th class="px-4 py-3">Actions</th>
								</tr>
							</thead>
							<tbody>
								{#each filteredItemsResults as item}
									<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700/50 {itemsSelectedKeys.has(itemStableKey(item)) ? 'bg-blue-50 dark:bg-blue-900/20' : ''}">
										<td class="px-3 py-3"><input type="checkbox" class="w-4 h-4" checked={itemsSelectedKeys.has(itemStableKey(item))} onchange={() => toggleItemRow(item)} /></td>
										{#each getColumns(filteredItemsResults) as col}
											<td class="px-4 py-3 font-mono text-xs max-w-[200px] truncate" title={String(item[col] ?? '')}>{item[col] ?? ''}</td>
										{/each}
										<td class="px-4 py-3 whitespace-nowrap">
											<button onclick={() => openEditItem(item)} class="text-xs text-blue-600 hover:text-blue-800 dark:text-blue-400 mr-2">Edit</button>
											<button onclick={() => deleteItem(item)} class="text-xs text-red-600 hover:text-red-800 dark:text-red-400">Delete</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
					{#if itemsLastKey}
						<button onclick={() => loadItems(false)} disabled={itemsLoading} class="text-white bg-blue-700 hover:bg-blue-800 font-medium rounded-lg text-sm px-4 py-2 dark:bg-blue-600 dark:hover:bg-blue-700 disabled:opacity-50">
							{itemsLoading ? 'Loading...' : 'Load More'}
						</button>
					{/if}
				{:else}
					<p class="text-sm text-slate-500 dark:text-slate-400 py-4">No items found in this table.</p>
				{/if}
			</div>
		{:else if activeTab === 'streams'}
			<div class="space-y-4">
				<!-- Stream Header -->
				<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800">
					<div class="flex items-center justify-between mb-1">
						<div class="flex items-center gap-3">
							<h3 class="text-lg font-semibold text-slate-900 dark:text-white">DynamoDB Streams</h3>
							{#if streamsEnabled}
								<span class="text-xs bg-green-100 text-green-800 dark:bg-green-900 dark:text-green-300 px-2 py-0.5 rounded font-medium">ENABLED</span>
								<span class="text-xs bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-300 px-2 py-0.5 rounded font-medium">{streamsViewType}</span>
								{#if !streamBackendUnavailable}
									<span class="flex items-center gap-1 text-xs text-green-600 dark:text-green-400">
										<span class="w-2 h-2 rounded-full bg-green-500 animate-pulse"></span>Auto-refresh 3s
									</span>
								{/if}
							{:else}
								<span class="text-xs bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-400 px-2 py-0.5 rounded font-medium">DISABLED</span>
							{/if}
						</div>
						{#if streamARN}
							<button onclick={() => copyToClipboard(streamARN)} class="flex items-center gap-1.5 text-xs text-slate-500 hover:text-blue-600 dark:text-slate-400 dark:hover:text-blue-400">
								<svg class="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M8 16H6a2 2 0 01-2-2V6a2 2 0 012-2h8a2 2 0 012 2v2m-6 12h8a2 2 0 002-2v-8a2 2 0 00-2-2h-8a2 2 0 00-2 2v8a2 2 0 002 2z" /></svg>
								Copy ARN
							</button>
						{/if}
					</div>
					{#if streamARN}
						<p class="text-xs font-mono text-slate-400 dark:text-slate-500 break-all">{streamARN}</p>
					{/if}
				</div>

				{#if !streamsEnabled}
					<div class="p-6 bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700 rounded-xl">
						<p class="text-sm text-blue-800 dark:text-blue-300 mb-3">DynamoDB Streams are not enabled for this table.</p>
						<button onclick={() => { activeTab = 'overview'; }} class="text-sm text-blue-600 dark:text-blue-400 underline hover:no-underline">Enable streams in the Overview tab →</button>
					</div>
				{:else if streamBackendUnavailable}
					<div class="p-6 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded-xl">
						<div class="flex items-start gap-3">
							<svg class="w-5 h-5 text-amber-600 dark:text-amber-400 shrink-0 mt-0.5" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M8.485 2.495c.673-1.167 2.357-1.167 3.03 0l6.28 10.875c.673 1.167-.17 2.625-1.516 2.625H3.72c-1.347 0-2.189-1.458-1.515-2.625L8.485 2.495zM10 5a.75.75 0 01.75.75v3.5a.75.75 0 01-1.5 0v-3.5A.75.75 0 0110 5zm0 9a1 1 0 100-2 1 1 0 000 2z" clip-rule="evenodd" /></svg>
							<div>
								<p class="text-sm font-semibold text-amber-800 dark:text-amber-300 mb-1">Streams backend not available</p>
								<p class="text-sm text-amber-700 dark:text-amber-400 mb-2">The DynamoDB Streams polling backend is not configured in this GopherStack instance. Use the AWS SDK directly to read stream records.</p>
								{#if streamARN}<p class="text-xs font-mono text-amber-600 dark:text-amber-500 break-all">ARN: {streamARN}</p>{/if}
							</div>
						</div>
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
							<div class="p-4 rounded-lg bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600">
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
							</div>
						{/if}
					{:else if streamInfoLoading}
						<div class="flex items-center gap-2 text-xs text-slate-400 dark:text-slate-500 px-1">
							<svg class="w-4 h-4 animate-spin" viewBox="0 0 24 24" fill="none"><circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle><path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z"></path></svg>
							Loading stream info...
						</div>
					{/if}

					<!-- Recent Events -->
					<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800">
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
						{#if streamEventsLoading && !streamEventsHtml}
							<div class="flex items-center justify-center p-8">
								<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" /><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" /></svg>
							</div>
						{:else if streamEventsHtml}
							<div class="overflow-auto max-h-96 stream-events-filter" data-filter={streamEventFilter}>{@html streamEventsHtml}</div>
						{:else}
							<div class="p-6 text-center text-slate-500 dark:text-slate-400">
								<p class="text-sm">No recent stream events. Events will appear here as you write to the table.</p>
							</div>
						{/if}
					</div>
				{/if}
			</div>
		{:else if activeTab === 'partiql'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800 space-y-4">
				<div>
					<h3 class="text-lg font-semibold text-slate-900 dark:text-white mb-1">PartiQL Editor</h3>
					<p class="text-sm text-slate-500 dark:text-slate-400 mb-2">Run SQL-style queries against DynamoDB using PartiQL syntax.</p>
					<div class="flex flex-wrap gap-2 text-xs">
						<span class="text-slate-500 dark:text-slate-400 self-center">Examples:</span>
						<button onclick={() => setPartiqlExample('SELECT * FROM "' + selectedTable + '"')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">SELECT *</button>
						<button onclick={() => setPartiqlExample('SELECT * FROM "' + selectedTable + '" WHERE id=\'value\'')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">SELECT ... WHERE</button>
						<button onclick={() => setPartiqlExample('INSERT INTO "' + selectedTable + '" VALUE {\'id\': \'new-id\', \'attr\': \'val\'}')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">INSERT</button>
						<button onclick={() => setPartiqlExample('UPDATE "' + selectedTable + '" SET attr=\'new-value\' WHERE id=\'value\'')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">UPDATE</button>
						<button onclick={() => setPartiqlExample('DELETE FROM "' + selectedTable + '" WHERE id=\'value\'')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">DELETE</button>
					</div>
				</div>
				<form onsubmit={(e) => { e.preventDefault(); executePartiQL(); }}>
					<div class="mb-3">
						<label for="partiql-stmt" class="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Statement</label>
						<textarea id="partiql-stmt" bind:value={partiqlStatement} rows="4" class="w-full font-mono text-sm p-3 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white focus:ring-2 focus:ring-blue-500" placeholder='SELECT * FROM "table"'></textarea>
					</div>
					<div class="flex gap-2">
						<button type="submit" disabled={partiqlLoading} class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:ring-2 focus:ring-blue-300 text-sm font-medium disabled:opacity-50">{partiqlLoading ? 'Executing...' : 'Execute'}</button>
						{#if partiqlResults.length > 0}
							<button type="button" onclick={() => exportJson(partiqlResults, `${selectedTable ?? 'partiql'}-results.json`)} class="px-4 py-2 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Download Results</button>
						{/if}
					</div>
				</form>
				{#if partiqlCount > 0}
					<p class="text-sm text-slate-600 dark:text-slate-400">{partiqlCount} result{partiqlCount !== 1 ? 's' : ''}{partiqlDuration > 0 ? ` · ${partiqlDuration} ms` : ''}</p>
				{/if}
				{@render resultsTable(partiqlResults)}
			</div>
		{:else if activeTab === 'metrics'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800 space-y-6">
				<h3 class="text-lg font-semibold text-slate-900 dark:text-white">Table Metrics</h3>
				<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
					<div class="p-5 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Total Items</div>
						<div class="text-3xl font-bold text-blue-600 dark:text-blue-400 mt-2">{selectedTableDesc?.ItemCount ?? 0}</div>
					</div>
					<div class="p-5 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Table Size</div>
						<div class="text-3xl font-bold text-green-600 dark:text-green-400 mt-2">{formatBytes(selectedTableDesc?.TableSizeBytes ?? 0)}</div>
					</div>
					<div class="p-5 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Capacity Mode</div>
						<div class="mt-2">
							{#if selectedTableDesc?.BillingModeSummary?.BillingMode === 'PAY_PER_REQUEST'}
								<span class="text-lg font-bold text-purple-600 dark:text-purple-400">ON_DEMAND</span>
							{:else}
								<div class="space-y-1">
									<div class="text-sm text-slate-700 dark:text-slate-300">Read: <span class="font-bold">{selectedTableDesc?.ProvisionedThroughput?.ReadCapacityUnits ?? 0} RCU</span></div>
									<div class="text-sm text-slate-700 dark:text-slate-300">Write: <span class="font-bold">{selectedTableDesc?.ProvisionedThroughput?.WriteCapacityUnits ?? 0} WCU</span></div>
								</div>
							{/if}
						</div>
					</div>
					<div class="p-5 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">GSI Count</div>
						<div class="text-3xl font-bold text-yellow-600 dark:text-yellow-400 mt-2">{selectedTableDesc?.GlobalSecondaryIndexes?.length ?? 0}</div>
					</div>
					<div class="p-5 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">Stream Status</div>
						<div class="mt-2">
							{#if streamsEnabled}
								<span class="inline-block bg-blue-100 text-blue-800 text-sm font-bold px-3 py-1 rounded dark:bg-blue-900 dark:text-blue-300">ENABLED</span>
								<div class="text-xs text-slate-500 dark:text-slate-400 mt-1">{streamsViewType}</div>
							{:else}
								<span class="inline-block bg-slate-100 text-slate-600 text-sm font-bold px-3 py-1 rounded dark:bg-slate-600 dark:text-slate-300">DISABLED</span>
							{/if}
						</div>
					</div>
					<div class="p-5 bg-white dark:bg-slate-700 rounded-xl border border-slate-200 dark:border-slate-600 shadow-sm">
						<div class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wider">TTL Status</div>
						<div class="mt-2">
							{#if ttlEnabled}
								<span class="inline-block bg-green-100 text-green-800 text-sm font-bold px-3 py-1 rounded dark:bg-green-900 dark:text-green-300">ENABLED</span>
								<div class="text-xs text-slate-500 dark:text-slate-400 mt-1">Attr: {ttlAttribute}</div>
							{:else}
								<span class="inline-block bg-slate-100 text-slate-600 text-sm font-bold px-3 py-1 rounded dark:bg-slate-600 dark:text-slate-300">DISABLED</span>
							{/if}
						</div>
					</div>
				</div>
			</div>
		{:else if activeTab === 'backups'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800 space-y-6">
				<div class="flex items-center justify-between">
					<h3 class="text-lg font-semibold text-slate-900 dark:text-white">Backups</h3>
				</div>
				<form onsubmit={(e) => { e.preventDefault(); createBackup(); }} class="flex gap-3 items-end">
					<div class="flex-1 max-w-xs">
						<label for="backup-name" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Backup Name</label>
						<input type="text" id="backup-name" bind:value={newBackupName} placeholder="my-backup-2024" required class="bg-white border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:text-white" />
					</div>
					<button type="submit" class="text-white bg-blue-700 hover:bg-blue-800 font-medium rounded-lg text-sm px-4 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700">Create Backup</button>
					<button type="button" onclick={() => toast.success('Restore not supported in local emulator')} class="py-2.5 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Restore</button>
				</form>
				{#if backupsLoading}
					<div class="flex justify-center p-8">
						<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" /><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" /></svg>
					</div>
				{:else if backups.length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-400 py-4">No backups found for this table.</p>
				{:else}
					<div class="overflow-x-auto">
						<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
							<thead class="text-xs text-slate-700 uppercase bg-slate-100 dark:bg-slate-700 dark:text-slate-400">
								<tr>
									<th class="px-6 py-3">Backup Name</th>
									<th class="px-6 py-3">Status</th>
									<th class="px-6 py-3">Creation Date</th>
									<th class="px-6 py-3">ARN</th>
									<th class="px-6 py-3">Actions</th>
								</tr>
							</thead>
							<tbody>
								{#each backups as backup}
									<tr class="border-b border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800">
										<td class="px-6 py-4 font-medium text-slate-900 dark:text-white">{backup.BackupName ?? '-'}</td>
										<td class="px-6 py-4"><span class="bg-green-100 text-green-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-green-900 dark:text-green-300">{backup.BackupStatus ?? '-'}</span></td>
										<td class="px-6 py-4">{backup.BackupCreationDateTime ? new Date(backup.BackupCreationDateTime).toLocaleString() : '-'}</td>
										<td class="px-6 py-4 font-mono text-xs max-w-[200px] truncate" title={backup.BackupArn ?? ''}>{backup.BackupArn ?? '-'}</td>
										<td class="px-6 py-4">
											<button onclick={() => backup.BackupArn && deleteBackup(backup.BackupArn)} class="text-xs text-red-600 hover:text-red-800 dark:text-red-400">Delete</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		{/if}
	{:else}
		<div class="bg-white/80 dark:bg-slate-800/80 backdrop-blur-md p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-8">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
					<img src="/dashboard/static/icons/dynamodb.svg" class="w-8 h-8 rounded-md shadow-sm" alt="dynamodb" />
					DynamoDB Tables
				</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-400 flex items-center gap-2">
					Manage your DynamoDB tables.
					<span class="text-xs bg-indigo-100 text-indigo-800 dark:bg-indigo-900 dark:text-indigo-300 px-2 py-0.5 rounded-full font-medium">{currentRegion}</span>
				</p>
			</div>
			<div class="flex gap-2">
				<button id="purge-all-btn" onclick={purgeAll} class="text-white bg-red-700 hover:bg-red-800 focus:ring-4 focus:ring-red-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-red-600 dark:hover:bg-red-700 focus:outline-none dark:focus:ring-red-900">Purge All</button>
				<button id="create-table-btn" onclick={() => { showCreateModal = true; }} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800">+ Create Table</button>
			</div>
		</div>
		<div class="flex flex-col md:flex-row justify-between items-end gap-4">
			<div class="w-full max-w-xs">
				<label for="table-search" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Search Tables</label>
				<div class="relative">
					<div class="absolute inset-y-0 start-0 flex items-center ps-3 pointer-events-none">
						<svg class="w-4 h-4 text-slate-500 dark:text-slate-400" fill="none" viewBox="0 0 20 20"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m19 19-4-4m0-7A7 7 0 1 1 1 8a7 7 0 0 1 14 0Z" /></svg>
					</div>
					<input type="text" id="table-search" placeholder="Search tables..." bind:value={searchQuery} class="block w-full p-2.5 ps-10 text-sm text-slate-900 border border-slate-300 rounded-lg bg-slate-50 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
				</div>
			</div>
			<div class="flex items-end gap-2">
				<div>
					<label for="table-sort" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort</label>
					<select id="table-sort" bind:value={tableSortOrder} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:text-white">
						<option value="alpha">A → Z</option>
						<option value="itemCount">Item Count ↓</option>
					</select>
				</div>
				<button onclick={() => loadTables()} class="py-2.5 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700 flex items-center gap-2">
					<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" /></svg>
					Refresh
				</button>
			</div>
		</div>
		{#if loading}
			<div class="flex items-center justify-center p-8">
				<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" /><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" /></svg>
			</div>
		{:else if filteredTables.length === 0}
			<div class="text-center py-12 text-slate-500">
				<svg class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" /></svg>
				<p class="text-lg font-medium">No tables found</p>
				<p class="text-sm mt-1">Create your first table to get started</p>
			</div>
		{:else}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each pagedTables as tableName}
					{@const desc = tableDetails.get(tableName)}
					<div id="table-{tableName}" class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl hover:shadow-md transition-shadow cursor-pointer group">
						<div class="flex justify-between items-start">
							<button onclick={() => openTable(tableName)} class="flex-1 text-left">
								<h3 class="text-base font-semibold text-slate-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">{tableName}</h3>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-1 font-mono">{getKeySchema(desc)}</p>
								<div class="flex items-center gap-2 mt-2 flex-wrap">
									<span class="text-xs px-2 py-0.5 rounded-full bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 font-medium">{desc?.TableStatus ?? 'UNKNOWN'}</span>
									<span class="text-xs text-slate-500">{desc?.ItemCount ?? 0} items</span>
									{#if (desc?.GlobalSecondaryIndexes?.length ?? 0) > 0}
										<span class="text-xs px-2 py-0.5 rounded-full bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-400 font-medium">{desc?.GlobalSecondaryIndexes?.length} GSI{(desc?.GlobalSecondaryIndexes?.length ?? 0) > 1 ? 's' : ''}</span>
									{/if}
								</div>
							</button>
							<button onclick={() => deleteTable(tableName)} class="text-slate-400 hover:text-red-500 dark:hover:text-red-400 p-1 opacity-0 group-hover:opacity-100 transition-opacity" title="Delete table">
								<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
							</button>
						</div>
					</div>
				{/each}
			</div>
			{#if totalTablePages > 1}
				<div class="flex items-center justify-between mt-4">
					<span class="text-sm text-slate-500 dark:text-slate-400">Page {tablePage + 1} of {totalTablePages} ({filteredTables.length} tables)</span>
					<div class="flex gap-2">
						<button onclick={() => { tablePage = Math.max(0, tablePage - 1); }} disabled={tablePage === 0} class="px-3 py-1.5 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-100 disabled:opacity-50 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Prev</button>
						<button onclick={() => { tablePage = Math.min(totalTablePages - 1, tablePage + 1); }} disabled={tablePage >= totalTablePages - 1} class="px-3 py-1.5 text-sm font-medium text-slate-700 bg-white border border-slate-300 rounded-lg hover:bg-slate-100 disabled:opacity-50 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700">Next</button>
					</div>
				</div>
			{/if}
		{/if}
	{/if}
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showCreateModal = false; }}></button>
		<div class="relative p-4 w-full max-w-md z-10" role="dialog" aria-modal="true" tabindex="-1">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Table</h3>
					<button onclick={() => { showCreateModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4 md:p-5">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createTable(); }}>
						<div>
							<label for="tableName" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Table Name</label>
							<input type="text" id="tableName" bind:value={newTableName} placeholder="users" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div>
							<label for="partitionKey" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Partition Key</label>
							<input type="text" id="partitionKey" bind:value={partitionKey} placeholder="id" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div>
							<label for="partitionKeyType" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Partition Key Type</label>
							<select id="partitionKeyType" bind:value={partitionKeyType} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
								<option value="S">String</option><option value="N">Number</option><option value="B">Binary</option>
							</select>
						</div>
						<div>
							<label for="sortKey" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort Key (Optional)</label>
							<input type="text" id="sortKey" bind:value={sortKey} placeholder="timestamp" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div>
							<label for="sortKeyType" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort Key Type</label>
							<select id="sortKeyType" bind:value={sortKeyType} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
								<option value="S">String</option><option value="N">Number</option><option value="B">Binary</option>
							</select>
						</div>
						<div>
							<p class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Billing Mode</p>
							<div class="flex gap-6">
								<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer">
									<input type="radio" bind:group={createTableBillingMode} value="PAY_PER_REQUEST" class="w-4 h-4 text-blue-600" /> On-Demand
								</label>
								<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 cursor-pointer">
									<input type="radio" bind:group={createTableBillingMode} value="PROVISIONED" class="w-4 h-4 text-blue-600" /> Provisioned
								</label>
							</div>
						</div>
						{#if createTableBillingMode === 'PROVISIONED'}
							<div class="flex gap-4">
								<div class="flex-1">
									<label for="create-rcu" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Read Capacity Units</label>
									<input type="number" id="create-rcu" bind:value={createTableRcu} min="1" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
								</div>
								<div class="flex-1">
									<label for="create-wcu" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Write Capacity Units</label>
									<input type="number" id="create-wcu" bind:value={createTableWcu} min="1" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white" />
								</div>
							</div>
						{/if}
						<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
							<button type="button" onclick={() => { showCreateModal = false; }} class="text-slate-500 bg-white hover:bg-slate-100 border border-slate-200 text-sm font-medium px-5 py-2.5 rounded-lg dark:bg-slate-700 dark:text-slate-300 dark:border-slate-500 dark:hover:text-white dark:hover:bg-slate-600">Cancel</button>
							<button id="confirm-create-table-btn" type="submit" disabled={creating} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800 disabled:opacity-50">{creating ? 'Creating...' : 'Create'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

{#if showNewItemModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showNewItemModal = false; }}></button>
		<div class="relative p-4 w-full max-w-2xl z-10" role="dialog" aria-modal="true" tabindex="-1">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create New Item</h3>
					<button onclick={() => { showNewItemModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4 md:p-5">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createItem(); }}>
						<div>
							<label for="newItemJson" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Item JSON <span class="text-red-600">*</span></label>
							<textarea id="newItemJson" bind:value={newItemJson} rows="12" required class="block p-2.5 w-full text-sm text-slate-900 bg-slate-50 rounded-lg border border-slate-300 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white font-mono" placeholder={'{"id": "123", "name": "Example"}'}></textarea>
						</div>
						<div class="flex justify-end gap-2 pt-4">
							<button type="button" onclick={() => { showNewItemModal = false; }} class="text-slate-500 bg-white hover:bg-slate-100 border border-slate-200 text-sm font-medium px-5 py-2.5 rounded-lg dark:bg-slate-700 dark:text-slate-300 dark:border-slate-500 dark:hover:text-white dark:hover:bg-slate-600">Cancel</button>
							<button type="submit" class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800">Create Item</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

{#if showImportModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showImportModal = false; }}></button>
		<div class="relative p-4 w-full max-w-2xl z-10" role="dialog" aria-modal="true" tabindex="-1">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Import Items (JSON Array)</h3>
					<button onclick={() => { showImportModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4 md:p-5">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); importItems(); }}>
						<div>
							<label for="importData" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">JSON Array <span class="text-red-600">*</span></label>
							<textarea id="importData" bind:value={importJson} rows="12" required class="block p-2.5 w-full text-sm text-slate-900 bg-slate-50 rounded-lg border border-slate-300 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white font-mono" placeholder={'[{"id": "1", "name": "Item 1"}, {"id": "2", "name": "Item 2"}]'}></textarea>
						</div>
						<div class="flex justify-end gap-2 pt-4">
							<button type="button" onclick={() => { showImportModal = false; }} class="text-slate-500 bg-white hover:bg-slate-100 border border-slate-200 text-sm font-medium px-5 py-2.5 rounded-lg dark:bg-slate-700 dark:text-slate-300 dark:border-slate-500 dark:hover:text-white dark:hover:bg-slate-600">Cancel</button>
							<button type="submit" class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800">Import Items</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

{#if showEditModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showEditModal = false; }}></button>
		<div class="relative p-4 w-full max-w-2xl z-10" role="dialog" aria-modal="true" tabindex="-1">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Edit Item</h3>
					<button onclick={() => { showEditModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
				</div>
				<div class="p-4 md:p-5">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); saveEditItem(); }}>
						<div>
							<label for="editItemJson" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Item JSON</label>
							<textarea id="editItemJson" bind:value={editItemJson} rows="12" class="block p-2.5 w-full text-sm text-slate-900 bg-slate-50 rounded-lg border border-slate-300 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white font-mono"></textarea>
						</div>
						<div class="flex justify-end gap-2 pt-4">
							<button type="button" onclick={() => { showEditModal = false; }} class="text-slate-500 bg-white hover:bg-slate-100 border border-slate-200 text-sm font-medium px-5 py-2.5 rounded-lg dark:bg-slate-700 dark:text-slate-300 dark:border-slate-500 dark:hover:text-white dark:hover:bg-slate-600">Cancel</button>
							<button type="submit" class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800">Save Item</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

{#if showCreateGsiModal}
<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showCreateGsiModal = false; }}></button>
<div class="relative p-4 w-full max-w-lg z-10" role="dialog" aria-modal="true" tabindex="-1">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Global Secondary Index</h3>
<button onclick={() => { showCreateGsiModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
</div>
<div class="p-4 md:p-5">
<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createGsi(); }}>
<div>
<label for="gsi-name" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Index Name <span class="text-red-600">*</span></label>
<input type="text" id="gsi-name" bind:value={gsiIndexName} placeholder="email-index" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div class="flex gap-4">
<div class="flex-1">
<label for="gsi-pk" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Partition Key <span class="text-red-600">*</span></label>
<input type="text" id="gsi-pk" bind:value={gsiPkAttr} placeholder="email" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div>
<label for="gsi-pk-type" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Type</label>
<select id="gsi-pk-type" bind:value={gsiPkType} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
<option value="S">String</option><option value="N">Number</option><option value="B">Binary</option>
</select>
</div>
</div>
<div class="flex gap-4">
<div class="flex-1">
<label for="gsi-sk" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort Key (Optional)</label>
<input type="text" id="gsi-sk" bind:value={gsiSkAttr} placeholder="createdAt" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div>
<label for="gsi-sk-type" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Type</label>
<select id="gsi-sk-type" bind:value={gsiSkType} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
<option value="S">String</option><option value="N">Number</option><option value="B">Binary</option>
</select>
</div>
</div>
<div>
<label for="gsi-projection" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Projection Type</label>
<select id="gsi-projection" bind:value={gsiProjection} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
<option value="ALL">ALL</option>
<option value="KEYS_ONLY">KEYS_ONLY</option>
<option value="INCLUDE">INCLUDE</option>
</select>
</div>
{#if gsiProjection === 'INCLUDE'}
<div>
<label for="gsi-nonkey" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Non-Key Attributes (comma-separated)</label>
<input type="text" id="gsi-nonkey" bind:value={gsiNonKeyAttrs} placeholder="attr1, attr2" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
{/if}
<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
<button type="button" onclick={() => { showCreateGsiModal = false; }} class="text-slate-500 bg-white hover:bg-slate-100 border border-slate-200 text-sm font-medium px-5 py-2.5 rounded-lg dark:bg-slate-700 dark:text-slate-300 dark:border-slate-500 dark:hover:text-white dark:hover:bg-slate-600">Cancel</button>
<button type="submit" disabled={creatingGsi} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800 disabled:opacity-50">{creatingGsi ? 'Creating...' : 'Create GSI'}</button>
</div>
</form>
</div>
</div>
</div>
</div>
{/if}
