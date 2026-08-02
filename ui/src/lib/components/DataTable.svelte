<script lang="ts" generics="T">
	// Generic table matching the table markup used across service pages
	// (e.g. cloudwatchlogs/+page.svelte). Uses a *keyed* {#each} on rows —
	// unlike the vast majority of the 1272 {#each} blocks in the existing
	// route pages, only 10 of which are keyed.
	import type { Column } from './data-table';

	type Props = {
		rows: T[];
		rowKey: (row: T) => string;
		columns: Column<T>[];
		loading: boolean;
		emptyMessage: string;
	};

	let { rows, rowKey, columns, loading, emptyMessage }: Props = $props();

	function cellValue(row: T, key: string): unknown {
		return (row as Record<string, unknown>)[key];
	}
</script>

{#if loading}
	<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
{:else if rows.length === 0}
	<div class="text-center py-8 text-gray-500 dark:text-gray-400">{emptyMessage}</div>
{:else}
	<div class="overflow-x-auto">
		<table class="w-full text-sm">
			<thead>
				<tr
					class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400"
				>
					{#each columns as column (column.key)}
						<th class="px-4 py-2 font-medium">{column.label}</th>
					{/each}
				</tr>
			</thead>
			<tbody>
				{#each rows as row (rowKey(row))}
					<tr class="border-b border-gray-100 dark:border-gray-800">
						{#each columns as column (column.key)}
							<td class="px-4 py-3 text-gray-700 dark:text-gray-300">
								{#if column.render}
									{@render column.render(row)}
								{:else}
									{String(cellValue(row, column.key) ?? '—')}
								{/if}
							</td>
						{/each}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/if}
