<script lang="ts">
	import type { TrafficShare } from '#lib/gcprun.ts';

	let { traffic }: { traffic: TrafficShare[] } = $props();

	const COLORS = ['bg-blue-500', 'bg-emerald-500', 'bg-amber-500', 'bg-violet-500', 'bg-rose-500', 'bg-cyan-500'];
	const shares = $derived(traffic.filter((t) => t.percent > 0));
</script>

{#if shares.length === 0}
	<span class="text-xs text-gray-500 dark:text-gray-400">no traffic</span>
{:else}
	<div class="min-w-[10rem] space-y-1" data-testid="run-traffic">
		<div class="flex h-2.5 overflow-hidden rounded-full bg-gray-200 dark:bg-slate-700" role="img" aria-label="Traffic split">
			{#each shares as t, i (i)}
				<div class={COLORS[i % COLORS.length]} style="width: {t.percent}%"></div>
			{/each}
		</div>
		<ul class="space-y-0.5 text-xs">
			{#each shares as t, i (i)}
				<li class="flex items-center gap-1.5" data-testid="run-traffic-share">
					<span class="inline-block h-2 w-2 rounded-full {COLORS[i % COLORS.length]}"></span>
					<span class="font-mono">{t.percent}%</span>
					<span class="truncate text-gray-600 dark:text-gray-300" title={t.revision}>{t.revision || 'latest'}{t.latest ? ' (latest)' : ''}</span>
				</li>
			{/each}
		</ul>
	</div>
{/if}
