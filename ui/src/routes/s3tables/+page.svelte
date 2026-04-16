<script lang="ts">
	import { onMount } from 'svelte';
	import { getS3TablesClient } from '$lib/aws-client';
	import {
		ListTableBucketsCommand,
		ListTablesCommand,
		type TableBucketSummary,
		type TableSummary
	} from '@aws-sdk/client-s3tables';
	import { toast } from 'svelte-sonner';
	import { Table2, RefreshCw, Search, Database, Layers } from 'lucide-svelte';

	const s3t = getS3TablesClient();

	let loading = $state(false);
	let activeTab = $state<'buckets' | 'tables'>('buckets');
	let searchQuery = $state('');
	let buckets = $state<TableBucketSummary[]>([]);
	let tables = $state<TableSummary[]>([]);
	let selectedBucketArn = $state<string | null>(null);

	const filteredBuckets = $derived(buckets.filter((b) => (b.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredTables = $derived(tables.filter((t) => (t.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const resp = await s3t.send(new ListTableBucketsCommand({}));
			buckets = resp.tableBuckets ?? [];
		} catch (e) {
			toast.error('Failed to load S3 Tables data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadTables(bucketArn: string) {
		selectedBucketArn = bucketArn;
		try {
			const resp = await s3t.send(new ListTablesCommand({ tableBucketARN: bucketArn }));
			tables = resp.tables ?? [];
			activeTab = 'tables';
		} catch (e) {
			toast.error('Failed to load tables: ' + String(e));
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Table2 class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon S3 Tables</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Managed Apache Iceberg tables optimized for analytics workloads</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Database class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{buckets.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Table Buckets</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Table2 class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{tables.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Tables (selected)</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Layers class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{selectedBucketArn ? '1' : '0'}</p><p class="text-sm text-gray-500 dark:text-gray-400">Selected Bucket</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['buckets', 'Table Buckets'], ['tables', 'Tables']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'buckets'}
				{#if filteredBuckets.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No table buckets found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredBuckets as bucket}
							<button onclick={() => loadTables(bucket.arn ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<Database class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{bucket.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{bucket.arn}</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">View tables →</span>
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'tables'}
				{#if filteredTables.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No tables found. Select a table bucket to view its tables.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredTables as table}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Table2 class="w-5 h-5 text-green-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{table.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{table.namespace?.join('.')} · {table.tableARN}</p>
									</div>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
