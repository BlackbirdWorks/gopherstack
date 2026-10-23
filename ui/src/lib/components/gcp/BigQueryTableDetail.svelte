<script lang="ts">
	import { RefreshCw, ChevronLeft, ChevronRight } from 'lucide-svelte';
	import BigQueryResultGrid from '#lib/components/gcp/BigQueryResultGrid.svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		PREVIEW_PAGE,
		flattenSchema,
		getTable,
		previewTable,
		typeLabel,
		type TableDetail,
		type TablePage
	} from '#lib/gcpbigquery.ts';

	let { project, dataset, table, refresh = 0 }: { project: string; dataset: string; table: string; refresh?: number } = $props();

	let detail = $state<TableDetail | null>(null);
	let pageData = $state<TablePage | null>(null);
	let tokens = $state<string[]>(['']);
	let index = $state(0);
	let loading = $state(false);
	let error = $state('');

	const cell = 'px-4 py-2';
	const pager = 'rounded-md border border-gray-200 dark:border-gray-700 p-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-40';

	async function loadPage(i: number) {
		if (!detail) return;
		pageData = await previewTable(project, dataset, table, detail.schema, tokens[i] ?? '');
		index = i;
		if (pageData.nextToken) tokens[i + 1] = pageData.nextToken;
	}

	async function load() {
		loading = true;
		error = '';
		tokens = [''];
		try {
			detail = await getTable(project, dataset, table);
			await loadPage(0);
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function go(i: number) {
		try {
			await loadPage(i);
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	$effect(() => {
		void [project, dataset, table, refresh];
		void load();
	});

	const from = $derived(index * PREVIEW_PAGE + 1);
</script>

<div class="space-y-5" data-testid="bq-table-detail">
	<div class="flex items-start justify-between gap-4">
		<div class="min-w-0">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{table}</h3>
			<p class="font-mono text-xs text-gray-500 dark:text-gray-400 break-all">{project}.{dataset}.{table}</p>
		</div>
		<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={load} aria-label="Reload table"><RefreshCw class="h-4 w-4 {loading ? 'animate-spin' : ''}" /></button>
	</div>

	{#if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="bq-detail-error">{error}</div>
	{/if}

	{#if detail}
		<dl class="grid grid-cols-2 gap-3 text-sm sm:grid-cols-4">
			{#each [['Type', detail.kind], ['Rows', detail.numRows], ['Bytes', detail.numBytes], ['Location', detail.location || '—']] as [k, v] (k)}
				<div class="rounded-lg border border-gray-200 dark:border-gray-700 px-3 py-2">
					<dt class="text-xs text-gray-500 dark:text-gray-400">{k}</dt>
					<dd class="font-medium text-gray-900 dark:text-gray-100 break-all" data-testid="bq-stat-{k.toLowerCase()}">{v}</dd>
				</div>
			{/each}
		</dl>

		<div class="space-y-2" data-testid="bq-schema">
			<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Schema</h4>
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Field</th><th class={cell}>Type</th><th class={cell}>Mode</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each flattenSchema(detail.schema) as row (row.path)}
							<tr data-testid="bq-schema-row">
								<td class="{cell} font-mono" style="padding-left: {1 + row.depth * 1.25}rem">{row.field.name}</td>
								<td class="{cell} font-mono text-xs">{typeLabel(row.field)}</td>
								<td class="{cell} text-xs">{row.field.mode}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		</div>

		<div class="space-y-2" data-testid="bq-preview">
			<div class="flex items-center justify-between">
				<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Preview</h4>
				{#if pageData && pageData.total > 0}
					<div class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
						<span data-testid="bq-preview-range">{from}–{from + pageData.rows.length - 1} of {pageData.total}</span>
						<button class={pager} disabled={index === 0} onclick={() => go(index - 1)} aria-label="Previous page"><ChevronLeft class="h-4 w-4" /></button>
						<button class={pager} disabled={!pageData.nextToken} onclick={() => go(index + 1)} aria-label="Next page"><ChevronRight class="h-4 w-4" /></button>
					</div>
				{/if}
			</div>
			{#if pageData && pageData.rows.length > 0}
				<BigQueryResultGrid schema={detail.schema} rows={pageData.rows} testId="bq-preview-grid" />
			{:else}
				<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="bq-preview-empty">No rows</p>
			{/if}
		</div>
	{/if}
</div>
