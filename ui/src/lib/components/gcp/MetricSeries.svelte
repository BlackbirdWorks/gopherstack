<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { WINDOWS, queryTimeSeries, type TimeSeries } from '#lib/gcpmonitoring.ts';

	type Props = { project: string; metricType: string };
	let { project, metricType }: Props = $props();

	const W = 640;
	const H = 140;

	let windowId = $state('1h');
	let series = $state<TimeSeries[]>([]);
	let loading = $state(false);
	let error = $state('');

	async function load() {
		loading = true;
		error = '';
		try {
			const seconds = WINDOWS.find((w) => w.id === windowId)?.seconds ?? 3600;
			series = await queryTimeSeries(project, metricType, seconds);
		} catch (e) {
			error = describeGcpError(e);
			series = [];
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void metricType;
		void windowId;
		void load();
	});

	const label = (s: TimeSeries) => {
		const parts = [...Object.entries(s.metricLabels), ...Object.entries(s.resourceLabels)].map(([k, v]) => `${k}=${v}`);
		return `${s.resourceType || 'resource'}${parts.length > 0 ? ` {${parts.join(', ')}}` : ''}`;
	};

	function path(s: TimeSeries): string {
		const pts = s.points.filter((p) => p.value !== null);
		if (pts.length === 0) return '';
		const times = pts.map((p) => Date.parse(p.time));
		const vals = pts.map((p) => p.value as number);
		const t0 = Math.min(...times);
		const tSpan = Math.max(...times) - t0 || 1;
		const v0 = Math.min(...vals, 0);
		const vSpan = Math.max(...vals) - v0 || 1;
		return pts
			.map((_, i) => `${(((times[i] - t0) / tSpan) * (W - 8) + 4).toFixed(1)},${(H - 4 - ((vals[i] - v0) / vSpan) * (H - 8)).toFixed(1)}`)
			.join(' ');
	}
</script>

<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="monitoring-series">
	<div class="flex flex-wrap items-center gap-3">
		<h2 class="font-mono text-sm font-semibold">{metricType}</h2>
		<label class="text-xs text-gray-500 dark:text-gray-400" for="monitoring-window">Window</label>
		<select id="monitoring-window" class="{GCP_INPUT} !w-auto" bind:value={windowId} data-testid="monitoring-window">
			{#each WINDOWS as w (w.id)}<option value={w.id}>{w.label}</option>{/each}
		</select>
	</div>
	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="monitoring-series-error">{error}</p>
	{:else if loading}
		<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
	{:else if series.length === 0}
		<p class="py-6 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="monitoring-series-empty">No data points in this window.</p>
	{:else}
		{#each series as s, i (i)}
			<div data-testid="monitoring-ts">
				<p class="font-mono text-xs break-all">{label(s)}</p>
				{#if path(s)}
					<svg viewBox="0 0 {W} {H}" class="w-full max-w-2xl text-blue-600" role="img" aria-label="Time series for {label(s)}">
						<polyline fill="none" stroke="currentColor" stroke-width="2" points={path(s)} />
					</svg>
				{/if}
				<table class="mt-1 w-full text-xs">
					<tbody>
						{#each s.points.toReversed().slice(0, 10) as p (p.time)}
							<tr data-testid="monitoring-point"><td class="pr-4 font-mono">{p.time}</td><td class="font-mono">{p.text}</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/each}
	{/if}
</div>
