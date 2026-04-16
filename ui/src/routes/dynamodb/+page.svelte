<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { newDynamoDBClient } from '$lib/aws/client';
	import {
		ListTablesCommand,
		DescribeTableCommand,
		CreateTableCommand,
		DeleteTableCommand,
		ScanCommand,
		QueryCommand,
		PutItemCommand,
		ExecuteStatementCommand,
		DescribeTimeToLiveCommand,
		UpdateTimeToLiveCommand,
		UpdateTableCommand,
		type TableDescription,
		type KeySchemaElement,
		type ScalarAttributeType,
		type AttributeValue,
		type StreamSpecification
	} from '@aws-sdk/client-dynamodb';
	import { toast } from 'svelte-sonner';
	import { avToJson, itemToJson, jsonToAv, jsonToItem, getColumns, getKeySchema, resolveKeySchema, buildKeyCondition } from '$lib/dynamodb';

	const ddb = newDynamoDBClient();

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

	// Scan State
	let scanFilterExp = $state('');
	let scanProjectionExp = $state('');
	let scanLimit = $state(100);
	let scanResults = $state<Record<string, unknown>[]>([]);
	let scanLoading = $state(false);
	let scanCount = $state(0);

	// PartiQL State
	let partiqlStatement = $state('');
	let partiqlResults = $state<Record<string, unknown>[]>([]);
	let partiqlLoading = $state(false);
	let partiqlCount = $state(0);

	// Stream Events State
	let streamEventsHtml = $state('');
	let streamEventsLoading = $state(false);
	let streamPollTimer: ReturnType<typeof setInterval> | undefined;

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

	// Derived
	const filteredTables = $derived(
		tableNames.filter((t) => !searchQuery || t.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// Pagination state
	const tablePageSize = 10;
	let tablePage = $state(0);
	const pagedTables = $derived(filteredTables.slice(tablePage * tablePageSize, (tablePage + 1) * tablePageSize));
	const totalTablePages = $derived(Math.ceil(filteredTables.length / tablePageSize));


	const currentKeySchema = $derived(resolveKeySchema(selectedTableDesc, queryIndexName || undefined));
	const tableKeySchema = $derived(resolveKeySchema(selectedTableDesc));

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
				TableName: newTableName.trim(), KeySchema: ks, AttributeDefinitions: ad, BillingMode: 'PAY_PER_REQUEST'
			}));
			toast.success(`Table "${newTableName.trim()}" created`);
			showCreateModal = false;
			newTableName = ''; partitionKey = ''; partitionKeyType = 'S'; sortKey = ''; sortKeyType = 'S';
			await loadTables();
		} catch (err: unknown) {
			toast.error(`Failed to create table: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function purgeAll() {
		if (!confirm('Are you sure you want to delete ALL tables? This cannot be undone.')) return;
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
		if (!confirm(`Delete table "${name}"?`)) return;
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
		queryResults = []; scanResults = []; partiqlResults = [];
		streamEventsHtml = '';
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
		} catch (err: unknown) {
			toast.error(`Scan failed: ${(err as Error).message}`);
		} finally {
			scanLoading = false;
		}
	}

	// PartiQL
	async function executePartiQL() {
		if (!partiqlStatement.trim()) return;
		partiqlLoading = true;
		try {
			const res = await ddb.send(new ExecuteStatementCommand({ Statement: partiqlStatement }));
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
		try {
			const res = await fetch(`/dashboard/dynamodb/table/${selectedTable}/stream-events`);
			if (res.ok) streamEventsHtml = await res.text();
		} catch {
                    // ignore
                }
		streamEventsLoading = false;
	}

	$effect(() => {
		if (activeTab === 'streams' && selectedTable) {
			loadStreamEvents();
			streamPollTimer = setInterval(loadStreamEvents, 3000);
			return () => { if (streamPollTimer) clearInterval(streamPollTimer); };
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
			const spec: Record<string, unknown> = { StreamEnabled: streamsEnabled };
			if (streamsEnabled) spec['StreamViewType'] = streamsViewType;
			await ddb.send(new UpdateTableCommand({ TableName: selectedTable, StreamSpecification: spec as StreamSpecification }));
			toast.success(`Streams ${streamsEnabled ? 'enabled successfully' : 'disabled successfully'}`);
			const desc = await ddb.send(new DescribeTableCommand({ TableName: selectedTable }));
			selectedTableDesc = desc.Table ?? null;
			streamARN = desc.Table?.LatestStreamArn ?? '';
		} catch (err: unknown) {
			toast.error(`Streams update failed: ${(err as Error).message}`);
		}
	}

	onMount(() => { loadTables(); });
	onDestroy(() => { if (streamPollTimer) clearInterval(streamPollTimer); });
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
					</tr>
				</thead>
				<tbody>
					{#each items as item}
						<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700/50 cursor-pointer"
							onclick={() => openEditItem(item)}>
							{#each getColumns(items) as col}
								<td class="px-4 py-3 font-mono text-xs max-w-[200px] truncate" title={String(item[col] ?? '')}>{item[col] ?? ''}</td>
							{/each}
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
				{#each [['overview', 'Overview'], ['query', 'Query'], ['scan', 'Scan'], ['streams', 'Stream Events'], ['partiql', 'PartiQL']] as [id, label]}
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
				<div class="grid grid-cols-1 md:grid-cols-3 lg:grid-cols-5 gap-4">
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Item Count</div>
						<div class="text-2xl font-bold text-blue-600 dark:text-blue-400">{selectedTableDesc?.ItemCount ?? 0}</div>
					</div>
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-sm font-medium text-slate-500 dark:text-slate-400">GSIs</div>
						<div class="text-2xl font-bold text-purple-600 dark:text-purple-400">{selectedTableDesc?.GlobalSecondaryIndexes?.length ?? 0}</div>
					</div>
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-sm font-medium text-slate-500 dark:text-slate-400">LSIs</div>
						<div class="text-2xl font-bold text-yellow-600 dark:text-yellow-400">{selectedTableDesc?.LocalSecondaryIndexes?.length ?? 0}</div>
					</div>
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-sm font-medium text-slate-500 dark:text-slate-400">TTL Status</div>
						<div class="mt-1">
							{#if ttlEnabled}
								<span class="bg-green-100 text-green-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-green-900 dark:text-green-300">ENABLED ({ttlAttribute})</span>
							{:else}
								<span class="bg-slate-100 text-slate-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-slate-700 dark:text-slate-300">DISABLED</span>
							{/if}
						</div>
					</div>
					<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<div class="text-sm font-medium text-slate-500 dark:text-slate-400">Streams</div>
						<div class="mt-1">
							{#if streamsEnabled}
								<span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-blue-900 dark:text-blue-300">ENABLED</span>
								<span class="text-[10px] text-slate-500 dark:text-slate-400 block mt-1 truncate">{streamsViewType}</span>
							{:else}
								<span class="bg-slate-100 text-slate-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-slate-700 dark:text-slate-300">DISABLED</span>
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
				{#if selectedTableDesc?.GlobalSecondaryIndexes?.length}
					<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<h2 class="mb-4 text-xl font-bold text-slate-900 dark:text-white">Global Secondary Indexes</h2>
						<div class="overflow-x-auto">
							<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
								<thead class="text-xs text-slate-700 uppercase bg-slate-50/50 dark:bg-slate-800/50 dark:text-slate-400">
									<tr><th class="px-6 py-3">Index Name</th><th class="px-6 py-3">Partition Key</th><th class="px-6 py-3">Sort Key</th><th class="px-6 py-3">Projection</th></tr>
								</thead>
								<tbody>
									{#each selectedTableDesc.GlobalSecondaryIndexes ?? [] as gsi}
										<tr class="border-b border-slate-200 dark:border-slate-700">
											<td class="px-6 py-4 font-mono">{gsi.IndexName}</td>
											<td class="px-6 py-4 font-mono">{gsi.KeySchema?.find((k) => k.KeyType === 'HASH')?.AttributeName ?? ''}</td>
											<td class="px-6 py-4 font-mono">{gsi.KeySchema?.find((k) => k.KeyType === 'RANGE')?.AttributeName ?? '-'}</td>
											<td class="px-6 py-4"><span class="bg-blue-100 text-blue-800 text-xs font-medium px-2.5 py-0.5 rounded dark:bg-blue-900 dark:text-blue-300">{gsi.Projection?.ProjectionType ?? ''}</span></td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{/if}
				{#if selectedTableDesc?.LocalSecondaryIndexes?.length}
					<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
						<h2 class="mb-4 text-xl font-bold text-slate-900 dark:text-white">Local Secondary Indexes</h2>
						<div class="overflow-x-auto">
							<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
								<thead class="text-xs text-slate-700 uppercase bg-slate-50/50 dark:bg-slate-800/50 dark:text-slate-400">
									<tr><th class="px-6 py-3">Index Name</th><th class="px-6 py-3">Sort Key</th><th class="px-6 py-3">Projection</th></tr>
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
					<button type="submit" disabled={queryLoading} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800 disabled:opacity-50">
						{queryLoading ? 'Querying...' : 'Execute Query'}
					</button>
				</form>
				{#if queryCount > 0}
					<p class="text-sm text-slate-600 dark:text-slate-400">{queryCount} result{queryCount !== 1 ? 's' : ''}</p>
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
					<p class="text-sm text-slate-600 dark:text-slate-400">{scanCount} result{scanCount !== 1 ? 's' : ''}</p>
				{/if}
				{@render resultsTable(scanResults)}
			</div>
		{:else if activeTab === 'streams'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800">
				{#if streamEventsLoading}
					<div class="flex items-center justify-center p-8">
						<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none"><path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" /><path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" /></svg>
					</div>
				{:else if streamEventsHtml}
					<div class="stream-events-content">{@html streamEventsHtml}</div>
				{:else}
					<p class="text-sm text-slate-500 dark:text-slate-400 p-4">No stream events available. Enable streams in the Overview tab.</p>
				{/if}
			</div>
		{:else if activeTab === 'partiql'}
			<div class="p-4 rounded-lg bg-slate-50 dark:bg-slate-800 space-y-4">
				<div>
					<h3 class="text-lg font-semibold text-slate-900 dark:text-white mb-1">PartiQL Editor</h3>
					<p class="text-sm text-slate-500 dark:text-slate-400 mb-2">Run SQL-style queries against DynamoDB using PartiQL syntax.</p>
					<div class="flex flex-wrap gap-2 text-xs">
						<span class="text-slate-500 dark:text-slate-400 self-center">Examples:</span>
						<button onclick={() => setPartiqlExample('SELECT * FROM "' + selectedTable + '"')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">SELECT * FROM "{selectedTable}"</button>
						<button onclick={() => setPartiqlExample('SELECT * FROM "' + selectedTable + '" WHERE id=\'value\'')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">SELECT * ... WHERE id='value'</button>
						<button onclick={() => setPartiqlExample('INSERT INTO "' + selectedTable + '" VALUE {\'id\': \'new-id\', \'attr\': \'val\'}')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">INSERT INTO ...</button>
						<button onclick={() => setPartiqlExample('DELETE FROM "' + selectedTable + '" WHERE id=\'value\'')} class="font-mono bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded hover:bg-blue-100 dark:hover:bg-blue-900 text-slate-700 dark:text-slate-300">DELETE FROM ...</button>
					</div>
				</div>
				<form onsubmit={(e) => { e.preventDefault(); executePartiQL(); }}>
					<div class="mb-3">
						<label for="partiql-stmt" class="block text-sm font-medium text-slate-600 dark:text-slate-300 mb-1">Statement</label>
						<textarea id="partiql-stmt" bind:value={partiqlStatement} rows="4" class="w-full font-mono text-sm p-3 border border-slate-300 dark:border-slate-600 rounded-lg bg-white dark:bg-slate-700 text-slate-900 dark:text-white focus:ring-2 focus:ring-blue-500" placeholder='SELECT * FROM "table"'></textarea>
					</div>
					<button type="submit" disabled={partiqlLoading} class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:ring-2 focus:ring-blue-300 text-sm font-medium disabled:opacity-50">{partiqlLoading ? 'Executing...' : 'Execute'}</button>
				</form>
				{#if partiqlCount > 0}
					<p class="text-sm text-slate-600 dark:text-slate-400">{partiqlCount} result{partiqlCount !== 1 ? 's' : ''}</p>
				{/if}
				{@render resultsTable(partiqlResults)}
			</div>
		{/if}
	{:else}
		<div class="bg-white/80 dark:bg-slate-800/80 backdrop-blur-md p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-8">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
					<img src="/dashboard/static/icons/dynamodb.svg" class="w-8 h-8 rounded-md shadow-sm" alt="dynamodb" />
					DynamoDB Tables
				</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-400">Manage your DynamoDB tables.</p>
			</div>
			<div class="flex gap-2">
				<button onclick={purgeAll} class="text-white bg-red-700 hover:bg-red-800 focus:ring-4 focus:ring-red-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-red-600 dark:hover:bg-red-700 focus:outline-none dark:focus:ring-red-800">Purge All</button>
				<button onclick={() => { showCreateModal = true; }} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800">+ Create Table</button>
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
					<div class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl hover:shadow-md transition-shadow cursor-pointer group">
						<div class="flex justify-between items-start">
							<button onclick={() => openTable(tableName)} class="flex-1 text-left">
								<h3 class="text-base font-semibold text-slate-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">{tableName}</h3>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-1 font-mono">{getKeySchema(desc)}</p>
								<div class="flex items-center gap-3 mt-2">
									<span class="text-xs px-2 py-0.5 rounded-full bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 font-medium">{desc?.TableStatus ?? 'UNKNOWN'}</span>
									<span class="text-xs text-slate-500">{desc?.ItemCount ?? 0} items</span>
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
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showCreateModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-md" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Table</h3>
					<button onclick={() => { showCreateModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
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
						<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
							<button type="button" onclick={() => { showCreateModal = false; }} class="text-slate-500 bg-white hover:bg-slate-100 border border-slate-200 text-sm font-medium px-5 py-2.5 rounded-lg dark:bg-slate-700 dark:text-slate-300 dark:border-slate-500 dark:hover:text-white dark:hover:bg-slate-600">Cancel</button>
							<button type="submit" disabled={creating} class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800 disabled:opacity-50">{creating ? 'Creating...' : 'Create'}</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

{#if showNewItemModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showNewItemModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-2xl" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create New Item</h3>
					<button onclick={() => { showNewItemModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
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
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showImportModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-2xl" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Import Items (JSON Array)</h3>
					<button onclick={() => { showImportModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
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
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showEditModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-2xl" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Edit Item</h3>
					<button onclick={() => { showEditModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
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
