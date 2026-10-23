<script lang="ts">
	import { isNested, renderCell, typeLabel, type Cell, type SchemaField } from '#lib/gcpbigquery.ts';

	let { schema, rows, testId = 'bq-grid' }: { schema: SchemaField[]; rows: Cell[][]; testId?: string } = $props();

	const cell = 'px-3 py-2 align-top';
</script>

<div class="overflow-auto rounded-lg border border-gray-200 dark:border-gray-700 max-h-[28rem]" data-testid={testId}>
	<table class="w-full text-sm">
		<thead class="sticky top-0 bg-gray-50 dark:bg-slate-800 text-left text-gray-500 dark:text-gray-400">
			<tr>
				<th class="{cell} w-10 text-right font-normal">#</th>
				{#each schema as f (f.name)}
					<th class="{cell} whitespace-nowrap font-semibold">{f.name} <span class="font-mono text-[10px] font-normal text-gray-400">{typeLabel(f)}</span></th>
				{/each}
			</tr>
		</thead>
		<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
			{#each rows as row, i (i)}
				<tr data-testid="{testId}-row">
					<td class="{cell} text-right text-xs text-gray-400">{i + 1}</td>
					{#each row as value, c (c)}
						<td class="{cell} font-mono text-xs {value === null ? 'italic text-gray-400' : ''} {isNested(value) ? 'whitespace-pre-wrap break-all text-violet-700 dark:text-violet-300' : 'whitespace-nowrap'}" title={isNested(value) ? JSON.stringify(value) : undefined} data-testid="{testId}-cell">{renderCell(value)}</td>
					{/each}
				</tr>
			{/each}
		</tbody>
	</table>
</div>
