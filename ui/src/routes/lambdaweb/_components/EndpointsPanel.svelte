<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';
	import { GetMetricStatisticsCommand } from '@aws-sdk/client-cloudwatch';
	import { getCloudWatchClient } from '#lib/aws-client.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		listEndpoints,
		NAMESPACE,
		sparkline,
		stateTone,
		weightColor,
		weightSegments,
		type Endpoint,
		type Point
	} from '#lib/lambdaweb.ts';

	let { fn, region }: { fn: string; region: string } = $props();

	const WINDOW_MS = 3 * 3600 * 1000;
	const PERIOD_S = 300;

	let endpoints = $state<Endpoint[]>([]);
	let series = $state<Record<string, Point[]>>({});
	let loading = $state(true);
	let error = $state('');

	const tones = {
		good: 'text-emerald-700 dark:text-emerald-300',
		busy: 'text-amber-700 dark:text-amber-300',
		bad: 'text-red-700 dark:text-red-300',
		muted: 'text-gray-500'
	};

	async function requests(name: string): Promise<Point[]> {
		const end = new Date();
		const res = await getCloudWatchClient(region).send(
			new GetMetricStatisticsCommand({
				Namespace: NAMESPACE,
				MetricName: 'ExecutedRequests',
				Dimensions: [
					{ Name: 'FunctionName', Value: fn },
					{ Name: 'EndpointName', Value: name }
				],
				StartTime: new Date(end.getTime() - WINDOW_MS),
				EndTime: end,
				Period: PERIOD_S,
				Statistics: ['Sum']
			})
		);
		return (res.Datapoints ?? []).map((d) => ({ time: d.Timestamp?.getTime() ?? 0, value: d.Sum ?? 0 }));
	}

	async function load() {
		try {
			endpoints = await listEndpoints(fn, region);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
			return;
		} finally {
			loading = false;
		}
		const entries = await Promise.all(endpoints.map(async (e) => [e.name, await requests(e.name).catch(() => [])] as const));
		series = Object.fromEntries(entries);
	}

	onMount(load);

	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');
	const total = (name: string) => (series[name] ?? []).reduce((s, p) => s + p.value, 0);
</script>

<div class="space-y-4" data-testid="lambdaweb-endpoints">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="lambdaweb-endpoints-error">Could not load endpoints: {error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		{#each endpoints as e (e.name)}
			<section class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="lambdaweb-endpoint">
				<div class="flex flex-wrap items-baseline justify-between gap-2">
					<h3 class="font-semibold" data-testid="lambdaweb-endpoint-name">{e.name}</h3>
					<span class="text-xs {tones[stateTone(e.state)]}" data-testid="lambdaweb-endpoint-state" title={e.stateReason}>{e.state}{e.updateStatus ? ` · update ${e.updateStatus}` : ''}</span>
				</div>
				<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-xs">
					<dt class="text-gray-500">Domain</dt><dd class="font-mono break-all" data-testid="lambdaweb-endpoint-domain">{e.domain || '—'}</dd>
					<dt class="text-gray-500">Type</dt><dd>{e.type}{e.regions.length > 0 ? ` · ${e.regions.join(', ')}` : ''}</dd>
					<dt class="text-gray-500">Auth type</dt><dd data-testid="lambdaweb-endpoint-auth">{e.auth}</dd>
					<dt class="text-gray-500">Scaling</dt><dd>max environments {e.maxEnvironments ?? 'default'}</dd>
					<dt class="text-gray-500">Throttle</dt><dd>rate limit {e.rateLimit === undefined ? 'none' : `${e.rateLimit} req/s`}</dd>
					<dt class="text-gray-500">Updated</dt><dd>{when(e.updated)}</dd>
				</dl>

				<div class="space-y-1">
					<p class="text-xs text-gray-500">Routing weights</p>
					<div class="flex h-4 w-full overflow-hidden rounded bg-gray-100 dark:bg-slate-800" role="img" aria-label="Routing weights" data-testid="lambdaweb-weights">
						{#each weightSegments(e.weights) as seg, i (seg.revisionId)}
							<div style="width: {seg.percent}%; background: {weightColor(i)}" title="{seg.revisionId}: {seg.weight}" data-testid="lambdaweb-weight-segment"></div>
						{/each}
					</div>
					<ul class="flex flex-wrap gap-x-4 text-xs">
						{#each weightSegments(e.weights) as seg, i (seg.revisionId)}
							<li class="flex items-center gap-1" data-testid="lambdaweb-weight"><span class="inline-block h-2 w-2 rounded-sm" style="background: {weightColor(i)}"></span><span class="font-mono">{seg.revisionId}</span> {seg.weight} ({seg.percent.toFixed(0)}%)</li>
						{:else}
							<li class="text-gray-500">No revisions routed.</li>
						{/each}
					</ul>
				</div>

				<div class="space-y-1">
					<p class="text-xs text-gray-500">Executed requests, last 3 hours: <span data-testid="lambdaweb-requests-total">{total(e.name)}</span></p>
					{#if (series[e.name] ?? []).length > 0}
						<svg viewBox="0 0 200 32" class="h-8 w-full max-w-sm text-orange-500" role="img" aria-label="Executed requests over time" data-testid="lambdaweb-sparkline">
							<polyline fill="none" stroke="currentColor" stroke-width="1.5" points={sparkline(series[e.name] ?? [], 200, 30)} />
						</svg>
					{:else}
						<p class="text-xs text-gray-400" data-testid="lambdaweb-sparkline-empty">No metric datapoints yet.</p>
					{/if}
				</div>
			</section>
		{:else}
			<p class="py-6 text-center text-gray-500" data-testid="lambdaweb-endpoints-empty">No endpoints.</p>
		{/each}
	{/if}
</div>
