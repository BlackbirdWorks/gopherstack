<script lang="ts">
	import { onMount } from 'svelte';
	import { newDynamoDBClient } from '$lib/aws/client';
	import {
		ListTablesCommand,
		DescribeTableCommand,
		CreateTableCommand,
		DeleteTableCommand,
		ScanCommand,
		type TableDescription,
		type AttributeDefinition,
		type KeySchemaElement,
		type ScalarAttributeType
	} from '@aws-sdk/client-dynamodb';
	import { toast } from 'svelte-sonner';

	const ddb = newDynamoDBClient();

	let tableNames = $state<string[]>([]);
	let tableDetails = $state<Map<string, TableDescription>>(new Map());
	let loading = $state(true);
	let searchQuery = $state('');
	let showCreateModal = $state(false);
	let creating = $state(false);

	// Create table form
	let newTableName = $state('');
	let partitionKey = $state('');
	let partitionKeyType = $state<ScalarAttributeType>('S');
	let sortKey = $state('');
	let sortKeyType = $state<ScalarAttributeType>('S');

	// Table detail view
	let selectedTable = $state<string | null>(null);
	let selectedTableDesc = $state<TableDescription | null>(null);
	let tableItems = $state<Record<string, unknown>[]>([]);
	let loadingItems = $state(false);

	const filteredTables = $derived(
		tableNames.filter((t) => !searchQuery || t.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadTables() {
		loading = true;
		try {
			const res = await ddb.send(new ListTablesCommand({}));
			tableNames = res.TableNames ?? [];

			const details = new Map<string, TableDescription>();
			for (const name of tableNames) {
				try {
					const desc = await ddb.send(new DescribeTableCommand({ TableName: name }));
					if (desc.Table) {
						details.set(name, desc.Table);
					}
				} catch {
					// Skip tables that fail to describe
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
			const keySchema: KeySchemaElement[] = [{ AttributeName: partitionKey.trim(), KeyType: 'HASH' }];
			const attrDefs: AttributeDefinition[] = [
				{ AttributeName: partitionKey.trim(), AttributeType: partitionKeyType }
			];

			if (sortKey.trim()) {
				keySchema.push({ AttributeName: sortKey.trim(), KeyType: 'RANGE' });
				attrDefs.push({ AttributeName: sortKey.trim(), AttributeType: sortKeyType });
			}

			await ddb.send(
				new CreateTableCommand({
					TableName: newTableName.trim(),
					KeySchema: keySchema,
					AttributeDefinitions: attrDefs,
					BillingMode: 'PAY_PER_REQUEST'
				})
			);
			toast.success(`Table "${newTableName.trim()}" created`);
			showCreateModal = false;
			newTableName = '';
			partitionKey = '';
			partitionKeyType = 'S';
			sortKey = '';
			sortKeyType = 'S';
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
			for (const name of tableNames) {
				await ddb.send(new DeleteTableCommand({ TableName: name }));
			}
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
			if (selectedTable === name) {
				selectedTable = null;
				selectedTableDesc = null;
			}
			await loadTables();
		} catch (err: unknown) {
			toast.error(`Failed to delete table: ${(err as Error).message}`);
		}
	}

	async function openTable(name: string) {
		selectedTable = name;
		selectedTableDesc = tableDetails.get(name) ?? null;
		loadingItems = true;
		try {
			const res = await ddb.send(new ScanCommand({ TableName: name, Limit: 50 }));
			tableItems = (res.Items ?? []).map((item) => {
				const row: Record<string, unknown> = {};
				for (const [k, v] of Object.entries(item)) {
					if (v.S !== undefined) row[k] = v.S;
					else if (v.N !== undefined) row[k] = v.N;
					else if (v.BOOL !== undefined) row[k] = v.BOOL;
					else if (v.NULL) row[k] = null;
					else if (v.L) row[k] = JSON.stringify(v.L);
					else if (v.M) row[k] = JSON.stringify(v.M);
					else if (v.B) row[k] = '(binary)';
					else row[k] = JSON.stringify(v);
				}
				return row;
			});
		} catch (err: unknown) {
			toast.error(`Failed to scan table: ${(err as Error).message}`);
		} finally {
			loadingItems = false;
		}
	}

	function getKeySchema(desc: TableDescription | undefined): string {
		if (!desc?.KeySchema) return '';
		return desc.KeySchema.map((k) => `${k.AttributeName} (${k.KeyType})`).join(', ');
	}

	function getItemCount(desc: TableDescription | undefined): number {
		return desc?.ItemCount ?? 0;
	}

	function getTableStatus(desc: TableDescription | undefined): string {
		return desc?.TableStatus ?? 'UNKNOWN';
	}

	function getItemColumns(): string[] {
		const cols = new Set<string>();
		for (const item of tableItems) {
			for (const key of Object.keys(item)) {
				cols.add(key);
			}
		}
		return Array.from(cols);
	}

	onMount(() => {
		loadTables();
	});
</script>

<div class="space-y-6">
	{#if selectedTable}
		<!-- Table Detail View -->
		<nav class="flex" aria-label="Breadcrumb">
			<ol class="inline-flex items-center space-x-1 md:space-x-2">
				<li class="inline-flex items-center">
					<button
						onclick={() => { selectedTable = null; selectedTableDesc = null; }}
						class="inline-flex items-center text-sm font-medium text-slate-700 hover:text-blue-600 dark:text-slate-400 dark:hover:text-white"
					>
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

		<div class="flex justify-between items-center">
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">{selectedTable}</h1>
			<button
				onclick={() => { selectedTable = null; selectedTableDesc = null; }}
				class="text-slate-700 bg-white hover:bg-slate-100 border border-slate-300 focus:ring-4 focus:ring-slate-200 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700 dark:focus:ring-slate-700"
			>
				Back
			</button>
		</div>

		<!-- Table Info -->
		{#if selectedTableDesc}
			<div class="grid grid-cols-1 sm:grid-cols-3 gap-4">
				<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<p class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Status</p>
					<p class="text-lg font-bold text-green-600 dark:text-green-400">{getTableStatus(selectedTableDesc)}</p>
				</div>
				<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<p class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Key Schema</p>
					<p class="text-sm font-mono text-slate-900 dark:text-white mt-1">{getKeySchema(selectedTableDesc)}</p>
				</div>
				<div class="p-4 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
					<p class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">Item Count</p>
					<p class="text-lg font-bold text-slate-900 dark:text-white">{getItemCount(selectedTableDesc)}</p>
				</div>
			</div>
		{/if}

		<!-- Table Items -->
		<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
			<h2 class="text-lg font-bold mb-4 text-slate-900 dark:text-white">Items (first 50)</h2>
			{#if loadingItems}
				<div class="flex items-center justify-center p-8">
					<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none">
						<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
						<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
					</svg>
				</div>
			{:else if tableItems.length === 0}
				<div class="text-center py-12 text-slate-500">
					<p class="text-lg font-medium">No items in this table</p>
					<p class="text-sm mt-1">Add items using the AWS CLI or SDK</p>
				</div>
			{:else}
				<div class="overflow-x-auto">
					<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
						<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
							<tr>
								{#each getItemColumns() as col}
									<th class="px-4 py-3">{col}</th>
								{/each}
							</tr>
						</thead>
						<tbody>
							{#each tableItems as item}
								<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700">
									{#each getItemColumns() as col}
										<td class="px-4 py-3 font-mono text-xs max-w-[200px] truncate" title={String(item[col] ?? '')}>
											{item[col] ?? ''}
										</td>
									{/each}
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{:else}
		<!-- Table List View -->
		<div class="bg-white/80 dark:bg-slate-800/80 backdrop-blur-md p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-8">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
					<img src="/dashboard/static/icons/dynamodb.svg" class="w-8 h-8 rounded-md shadow-sm" alt="dynamodb" />
					DynamoDB Tables
				</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-400">Manage your DynamoDB tables.</p>
			</div>
			<div class="flex gap-2">
				<button
					onclick={purgeAll}
					class="text-white bg-red-700 hover:bg-red-800 focus:ring-4 focus:ring-red-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-red-600 dark:hover:bg-red-700 focus:outline-none dark:focus:ring-red-800"
				>
					Purge All
				</button>
				<button
					onclick={() => { showCreateModal = true; }}
					class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800"
				>
					+ Create Table
				</button>
			</div>
		</div>

		<div class="flex flex-col md:flex-row justify-between items-end gap-4">
			<div class="w-full max-w-xs">
				<label for="table-search" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Search Tables</label>
				<div class="relative">
					<div class="absolute inset-y-0 start-0 flex items-center ps-3 pointer-events-none">
						<svg class="w-4 h-4 text-slate-500 dark:text-slate-400" fill="none" viewBox="0 0 20 20"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m19 19-4-4m0-7A7 7 0 1 1 1 8a7 7 0 0 1 14 0Z" /></svg>
					</div>
					<input
						type="text"
						id="table-search"
						placeholder="Search tables..."
						bind:value={searchQuery}
						class="block w-full p-2.5 ps-10 text-sm text-slate-900 border border-slate-300 rounded-lg bg-slate-50 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
					/>
				</div>
			</div>
		</div>

		{#if loading}
			<div class="flex items-center justify-center p-8">
				<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none">
					<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
					<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
				</svg>
			</div>
		{:else if filteredTables.length === 0}
			<div class="text-center py-12 text-slate-500">
				<svg class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4" /></svg>
				<p class="text-lg font-medium">No tables found</p>
				<p class="text-sm mt-1">Create your first table to get started</p>
			</div>
		{:else}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each filteredTables as tableName}
					{@const desc = tableDetails.get(tableName)}
					<div class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl hover:shadow-md transition-shadow cursor-pointer group">
						<div class="flex justify-between items-start">
							<button onclick={() => openTable(tableName)} class="flex-1 text-left">
								<h3 class="text-base font-semibold text-slate-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
									{tableName}
								</h3>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-1 font-mono">
									{getKeySchema(desc)}
								</p>
								<div class="flex items-center gap-3 mt-2">
									<span class="text-xs px-2 py-0.5 rounded-full bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-400 font-medium">
										{getTableStatus(desc)}
									</span>
									<span class="text-xs text-slate-500">
										{getItemCount(desc)} items
									</span>
								</div>
							</button>
							<button
								onclick={() => deleteTable(tableName)}
								class="text-slate-400 hover:text-red-500 dark:hover:text-red-400 p-1 opacity-0 group-hover:opacity-100 transition-opacity"
								title="Delete table"
							>
								<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 7l-.867 12.142A2 2 0 0116.138 21H7.862a2 2 0 01-1.995-1.858L5 7m5 4v6m4-6v6m1-10V4a1 1 0 00-1-1h-4a1 1 0 00-1 1v3M4 7h16" /></svg>
							</button>
						</div>
					</div>
				{/each}
			</div>
		{/if}
	{/if}
</div>

<!-- Create Table Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showCreateModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-md" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Table</h3>
					<button onclick={() => { showCreateModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
						<svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
					</button>
				</div>
				<div class="p-4 md:p-5">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createTable(); }}>
						<div>
							<label for="tableName" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Table Name</label>
							<input type="text" id="tableName" bind:value={newTableName} placeholder="users" required
								class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div>
							<label for="partitionKey" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Partition Key</label>
							<input type="text" id="partitionKey" bind:value={partitionKey} placeholder="id" required
								class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div>
							<label for="partitionKeyType" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Partition Key Type</label>
							<select id="partitionKeyType" bind:value={partitionKeyType}
								class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
								<option value="S">String</option>
								<option value="N">Number</option>
								<option value="B">Binary</option>
							</select>
						</div>
						<div>
							<label for="sortKey" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort Key (Optional)</label>
							<input type="text" id="sortKey" bind:value={sortKey} placeholder="timestamp"
								class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div>
							<label for="sortKeyType" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort Key Type</label>
							<select id="sortKeyType" bind:value={sortKeyType}
								class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
								<option value="S">String</option>
								<option value="N">Number</option>
								<option value="B">Binary</option>
							</select>
						</div>
						<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
							<button type="button" onclick={() => { showCreateModal = false; }}
								class="text-slate-500 bg-white hover:bg-slate-100 border border-slate-200 text-sm font-medium px-5 py-2.5 rounded-lg dark:bg-slate-700 dark:text-slate-300 dark:border-slate-500 dark:hover:text-white dark:hover:bg-slate-600">
								Cancel
							</button>
							<button type="submit" disabled={creating}
								class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800 disabled:opacity-50">
								{creating ? 'Creating...' : 'Create'}
							</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}
