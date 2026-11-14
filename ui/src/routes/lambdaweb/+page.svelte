<script lang="ts">
	import { Globe, RefreshCw } from 'lucide-svelte';
	import EndpointsPanel from './_components/EndpointsPanel.svelte';
	import InvokePanel from './_components/InvokePanel.svelte';
	import RevisionsPanel from './_components/RevisionsPanel.svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listFunctions, stateTone, type WebFunction } from '#lib/lambdaweb.ts';
	import { currentRegion } from '#lib/region.svelte.ts';
	import { onRegionChange } from '#lib/region-effect.svelte.ts';

	const TABS = [
		{ id: 'revisions', label: 'Revisions' },
		{ id: 'endpoints', label: 'Endpoints' },
		{ id: 'invoke', label: 'Invoke' }
	];
	const tones = {
		good: 'text-emerald-700 dark:text-emerald-300',
		busy: 'text-amber-700 dark:text-amber-300',
		bad: 'text-red-700 dark:text-red-300',
		muted: 'text-gray-500'
	};

	let functions = $state<WebFunction[]>([]);
	let selected = $state('');
	let tab = $state('revisions');
	let loading = $state(true);
	let error = $state('');

	const region = $derived(currentRegion());
	const current = $derived(functions.find((f) => f.name === selected));
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');

	async function load() {
		try {
			functions = await listFunctions(currentRegion());
			error = '';
			if (!functions.some((f) => f.name === selected)) selected = '';
		} catch (e) {
			functions = [];
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	onRegionChange(() => void load());

	const cell = 'px-4 py-3';
</script>

<div class="space-y-6 pb-20" data-testid="lambdaweb-page">
	<PageHeader icon={Globe} title="Lambda Web Functions" description="Web functions, revisions, endpoints with routing weights, and an invoke panel" color="orange" onRefresh={load} />

	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		{#if error}
			<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="lambdaweb-error">
				<p class="font-medium">Could not load web functions</p>
				<p>{error}</p>
				<button class="mt-2 underline" onclick={load}>Retry</button>
			</div>
		{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="lambdaweb-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Function</th><th class={cell}>State</th><th class={cell}>Created</th><th class={cell}>Updated</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each functions as f (f.name)}
						<tr class={selected === f.name ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="lambdaweb-function">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = selected === f.name ? '' : f.name)} aria-expanded={selected === f.name} data-testid="lambdaweb-select">{f.name}</button></td>
							<td class="{cell} text-xs {tones[stateTone(f.state)]}" data-testid="lambdaweb-state" title={f.stateReason}>{f.state}</td>
							<td class="{cell} text-xs">{when(f.created)}</td>
							<td class="{cell} text-xs">{when(f.updated)}</td>
						</tr>
					{:else}
						<tr><td colspan="4" class="{cell} text-center text-gray-500" data-testid="lambdaweb-empty">No web functions in {region}.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			{#key `${region}/${selected}`}
				<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="lambdaweb-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name}</h2>
					<p class="font-mono text-xs text-gray-500 break-all">{current.arn}</p>
					<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="orange" />
					{#if tab === 'revisions'}
						<RevisionsPanel fn={current.name} {region} />
					{:else if tab === 'endpoints'}
						<EndpointsPanel fn={current.name} {region} />
					{:else}
						<InvokePanel fn={current.name} {region} />
					{/if}
				</div>
			{/key}
		{/if}
	{/if}
</div>
