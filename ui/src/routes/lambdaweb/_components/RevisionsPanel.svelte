<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { codeLocation, defaultLogGroup, listRevisions, logGroupLink, stateTone, type Revision } from '#lib/lambdaweb.ts';

	let { fn, region }: { fn: string; region: string } = $props();

	let revisions = $state<Revision[]>([]);
	let loading = $state(true);
	let error = $state('');
	let open = $state('');

	const tones = {
		good: 'text-emerald-700 dark:text-emerald-300',
		busy: 'text-amber-700 dark:text-amber-300',
		bad: 'text-red-700 dark:text-red-300',
		muted: 'text-gray-500'
	};
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');

	async function load() {
		try {
			revisions = await listRevisions(fn, region);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	onMount(load);

	const cell = 'px-4 py-3';
</script>

<div class="space-y-3" data-testid="lambdaweb-revisions">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="lambdaweb-revisions-error">Could not load revisions: {error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Revision</th><th class={cell}>Status</th><th class={cell}>Code location</th><th class={cell}>Runtime</th><th class={cell}>Created</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each revisions as r (r.id)}
						<tr data-testid="lambdaweb-revision">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (open = open === r.id ? '' : r.id)} aria-expanded={open === r.id} data-testid="lambdaweb-revision-toggle">{r.id}</button></td>
							<td class="{cell} text-xs {tones[stateTone(r.state)]}" data-testid="lambdaweb-revision-state" title={r.stateReason}>{r.state}</td>
							<td class="{cell} font-mono text-xs break-all" data-testid="lambdaweb-revision-code">{codeLocation(r)}</td>
							<td class="{cell} text-xs">{r.runtime || '—'}</td>
							<td class="{cell} text-xs">{when(r.created)}</td>
						</tr>
						{#if open === r.id}
							<tr>
								<td colspan="5" class={cell} data-testid="lambdaweb-revision-detail">
									<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-xs">
										<dt class="text-gray-500">ARN</dt><dd class="font-mono break-all">{r.arn}</dd>
										<dt class="text-gray-500">Execution role</dt><dd class="font-mono break-all">{r.role || '—'}</dd>
										<dt class="text-gray-500">Timeout</dt><dd>{r.timeout === undefined ? '—' : `${r.timeout} s`}</dd>
										<dt class="text-gray-500">Concurrency per environment</dt><dd>{r.concurrency ?? '—'}</dd>
										<dt class="text-gray-500">Environment variables</dt><dd class="font-mono">{r.envNames.length > 0 ? `${r.envNames.join(', ')} (values hidden)` : 'none'}</dd>
										<dt class="text-gray-500">Log group</dt>
										<dd><a class="font-mono text-sky-700 underline dark:text-sky-400" href={logGroupLink(r.logGroup || defaultLogGroup(fn))} data-testid="lambdaweb-log-link">{r.logGroup || defaultLogGroup(fn)}</a></dd>
										{#if r.description}<dt class="text-gray-500">Description</dt><dd>{r.description}</dd>{/if}
										{#each r.errors as e (e)}<dt class="text-red-600">Error</dt><dd class="text-red-600">{e}</dd>{/each}
									</dl>
								</td>
							</tr>
						{/if}
					{:else}
						<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="lambdaweb-revisions-empty">No revisions.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
		<p class="text-xs text-gray-500">The control plane does not report execution environments, so none are listed.</p>
	{/if}
</div>
