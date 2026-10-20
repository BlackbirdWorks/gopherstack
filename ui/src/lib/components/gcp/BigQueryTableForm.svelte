<script lang="ts">
	import Modal from '#lib/components/Modal.svelte';
	import BigQuerySchemaEditor from '#lib/components/gcp/BigQuerySchemaEditor.svelte';
	import { GCP_GHOST, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import {
		draftsToSchema,
		newDraft,
		validateBqId,
		validateDrafts,
		type FieldDraft,
		type SchemaField
	} from '#lib/gcpbigquery.ts';

	let { dataset, busy = false, onsubmit }: { dataset: string; busy?: boolean; onsubmit: (id: string, schema: SchemaField[]) => void } = $props();

	let modal = $state<Modal | null>(null);
	let id = $state('');
	let fields = $state<FieldDraft[]>([]);
	let touched = $state(false);

	const idError = $derived(validateBqId(id.trim(), 'Table ID'));
	const schemaError = $derived(fields.length === 0 ? 'Add at least one field' : validateDrafts(fields));

	export function open() {
		id = '';
		fields = [newDraft('id', 'INTEGER', 'REQUIRED')];
		touched = false;
		modal?.open();
	}

	export function close() {
		modal?.close('done');
	}

	function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (idError || schemaError) return;
		onsubmit(id.trim(), draftsToSchema(fields));
	}
</script>

<Modal bind:this={modal} title="New table in {dataset}" titleId="bq-table-title" widthClass="max-w-2xl">
	{#snippet children()}
		<form id="bq-table-form" class="space-y-4" onsubmit={submit} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Table ID</span>
				<input class={GCP_INPUT} bind:value={id} placeholder="events" autocomplete="off" aria-label="Table ID" data-testid="bq-table-id" />
				{#if touched && idError}<span class="block text-xs text-red-600 dark:text-red-400">{idError}</span>{/if}
			</label>
			<div class="space-y-2">
				<span class="text-sm font-medium text-gray-700 dark:text-gray-300">Schema</span>
				<BigQuerySchemaEditor {fields} />
				{#if touched && schemaError}<span class="block text-xs text-red-600 dark:text-red-400" data-testid="bq-schema-error">{schemaError}</span>{/if}
			</div>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={close}>Cancel</button>
		<button type="submit" form="bq-table-form" class={GCP_PRIMARY} disabled={busy} data-testid="bq-table-submit">{busy ? 'Creating…' : 'Create table'}</button>
	{/snippet}
</Modal>
