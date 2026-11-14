<script lang="ts">
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import CloudBuildRunForm from '#lib/components/gcp/CloudBuildRunForm.svelte';
	import { GCP_MINI } from '#lib/gcp-action.ts';
	import type { RunSource, TriggerInfo } from '#lib/gcpcloudbuild.ts';

	type Props = {
		triggers: TriggerInfo[];
		rows: ResourceRow[];
		loading: boolean;
		error: string;
		onRetry: () => void;
		onRun: (t: TriggerInfo, src: RunSource) => Promise<void>;
		onDelete: (row: ResourceRow) => void;
	};
	let { triggers, rows, loading, error, onRetry, onRun, onDelete }: Props = $props();

	let target = $state<TriggerInfo | null>(null);

	async function submit(t: TriggerInfo, src: RunSource) {
		await onRun(t, src);
		target = null;
	}
</script>

{#if target}
	{@const t = target}
	<CloudBuildRunForm trigger={t} onRun={(src) => submit(t, src)} onCancel={() => (target = null)} />
{/if}
<GcpResourceTable testId="cloudbuild-trigger" noun="triggers" columns={['Trigger', 'Event', 'Filter', 'State', 'Last run', 'Description']} {rows} {loading} {error} {onRetry} {onDelete}>
	{#snippet actions(row)}
		{@const t = triggers.find((x) => x.id === row.id)}
		{#if t}<button class={GCP_MINI} onclick={() => (target = t)} disabled={t.disabled} data-testid="cloudbuild-run">Run</button>{/if}
	{/snippet}
</GcpResourceTable>
