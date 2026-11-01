<script lang="ts">
	import { chartPoints, seriesLabel, type Series } from '#lib/ocimonitoring.ts';

	type Props = { series: Series[] };
	let { series }: Props = $props();

	const W = 640;
	const H = 140;
	const COLORS = ['text-blue-600', 'text-emerald-600', 'text-amber-600', 'text-purple-600', 'text-rose-600'];
</script>

<div class="space-y-4" data-testid="monitoring-chart">
	{#each series as s, i (i)}
		<div data-testid="monitoring-series">
			<p class="font-mono text-xs break-all" data-testid="monitoring-series-label">{seriesLabel(s)} <span class="text-gray-400">({s.resolution || 'raw'}, {s.points.length} points)</span></p>
			{#if s.points.length > 0}
				<svg viewBox="0 0 {W} {H}" class="w-full max-w-2xl {COLORS[i % COLORS.length]}" role="img" aria-label="Datapoints for {seriesLabel(s)}">
					<polyline fill="none" stroke="currentColor" stroke-width="2" points={chartPoints(s.points, W, H)} data-testid="monitoring-line" />
				</svg>
				<table class="mt-1 text-xs">
					<tbody>
						{#each s.points.toReversed().slice(0, 8) as p (p.t)}
							<tr data-testid="monitoring-point"><td class="pr-4 font-mono">{new Date(p.t).toISOString()}</td><td class="font-mono">{p.v}</td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{/each}
</div>
