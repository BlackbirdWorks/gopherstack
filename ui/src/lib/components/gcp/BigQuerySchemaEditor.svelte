<script lang="ts">
	import { Plus, X } from 'lucide-svelte';
	import { FIELD_MODES, FIELD_TYPES, newDraft, type FieldDraft } from '#lib/gcpbigquery.ts';

	let { fields }: { fields: FieldDraft[] } = $props();

	const cell =
		'rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';

	function remove(list: FieldDraft[], id: number) {
		const i = list.findIndex((f) => f.id === id);
		if (i >= 0) list.splice(i, 1);
	}

	function typeChanged(f: FieldDraft) {
		if (f.type === 'RECORD' && f.children.length === 0) f.children.push(newDraft());
	}
</script>

{#snippet level(list: FieldDraft[], depth: number)}
	<div class="space-y-2 {depth > 0 ? 'ml-5 border-l-2 border-blue-200 dark:border-blue-900 pl-3' : ''}" data-testid="bq-schema-level">
		{#each list as f (f.id)}
			<div class="space-y-2">
				<div class="flex flex-wrap items-center gap-2">
					<input class="{cell} w-40 font-mono" bind:value={f.name} placeholder="field_name" aria-label="Field name" autocomplete="off" data-testid="bq-field-name" />
					<select class={cell} bind:value={f.type} onchange={() => typeChanged(f)} aria-label="Field type" data-testid="bq-field-type">
						{#each FIELD_TYPES as t (t)}<option value={t}>{t}</option>{/each}
					</select>
					<select class={cell} bind:value={f.mode} aria-label="Field mode" data-testid="bq-field-mode">
						{#each FIELD_MODES as m (m)}<option value={m}>{m}</option>{/each}
					</select>
					<button type="button" class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove field {f.name}" onclick={() => remove(list, f.id)}><X class="h-4 w-4" /></button>
				</div>
				{#if f.type === 'RECORD'}
					{@render level(f.children, depth + 1)}
				{/if}
			</div>
		{/each}
		<button type="button" class="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400 hover:underline" onclick={() => list.push(newDraft())} data-testid="bq-field-add">
			<Plus class="h-3.5 w-3.5" /> {depth > 0 ? 'Add nested field' : 'Add field'}
		</button>
	</div>
{/snippet}

{@render level(fields, 0)}
