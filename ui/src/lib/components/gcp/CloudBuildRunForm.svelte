<script lang="ts">
	import { GCP_GHOST, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { parseSubstitutions, type RunSource, type TriggerInfo } from '#lib/gcpcloudbuild.ts';

	type Props = { trigger: TriggerInfo; onRun: (src: RunSource) => void; onCancel: () => void };
	let { trigger, onRun, onCancel }: Props = $props();

	let kind = $state<'branch' | 'tag' | 'commit'>('branch');
	let revision = $state('');
	let subsText = $state('');
	let error = $state('');

	function submit(event: Event) {
		event.preventDefault();
		const subs = parseSubstitutions(subsText);
		if (subs.error) {
			error = subs.error;
			return;
		}
		error = '';
		const rev = revision.trim();
		onRun({ [kind]: rev || undefined, substitutions: subs.values });
	}
</script>

<form class="space-y-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700" onsubmit={submit} data-testid="cloudbuild-run-form">
	<h3 class="font-medium">Run trigger {trigger.displayName || trigger.id}</h3>
	<div class="flex flex-wrap gap-2">
		<label class="sr-only" for="cloudbuild-run-kind">Revision kind</label>
		<select id="cloudbuild-run-kind" class={GCP_INPUT + ' !w-auto'} bind:value={kind} data-testid="cloudbuild-run-kind">
			<option value="branch">Branch</option><option value="tag">Tag</option><option value="commit">Commit SHA</option>
		</select>
		<label class="sr-only" for="cloudbuild-run-revision">Revision</label>
		<input id="cloudbuild-run-revision" class="{GCP_INPUT} max-w-xs" placeholder="trigger default" bind:value={revision} data-testid="cloudbuild-run-revision" />
	</div>
	<div>
		<label class="text-xs text-gray-500" for="cloudbuild-run-subs">Substitution overrides (one _KEY=value per line)</label>
		<textarea id="cloudbuild-run-subs" class="{GCP_INPUT} font-mono" rows="3" bind:value={subsText} data-testid="cloudbuild-run-subs"></textarea>
	</div>
	{#if error}<p class="text-xs text-red-600" role="alert" data-testid="cloudbuild-run-error">{error}</p>{/if}
	<div class="flex gap-2">
		<button type="submit" class={GCP_PRIMARY} data-testid="cloudbuild-run-submit">Run</button>
		<button type="button" class={GCP_GHOST} onclick={onCancel}>Cancel</button>
	</div>
</form>
