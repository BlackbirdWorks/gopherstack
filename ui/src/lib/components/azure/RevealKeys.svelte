<script lang="ts">
	import { attempt, copyText, GCP_MINI } from '#lib/gcp-action.ts';

	type Pair = { label: string; value: string };
	let { fetchKeys, testid }: { fetchKeys: () => Promise<Pair[]>; testid: string } = $props();

	let pairs = $state<Pair[] | undefined>();

	async function show() {
		await attempt(async () => {
			pairs = await fetchKeys();
		}, '', 'Failed to get the keys');
	}
</script>

<div class="space-y-1" data-testid={testid}>
	{#if pairs}
		<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 font-mono text-xs">
			{#each pairs as p (p.label)}
				<dt>{p.label}</dt><dd class="break-all" data-testid="{testid}-value">{p.value}</dd>
			{/each}
		</dl>
		<button class={GCP_MINI} onclick={() => (pairs = undefined)} data-testid="{testid}-hide">Hide</button>
		<button class={GCP_MINI} onclick={() => copyText(pairs?.[0]?.value ?? '')} data-testid="{testid}-copy">Copy first</button>
	{:else}
		<span class="font-mono text-xs text-gray-500" data-testid="{testid}-masked">••••••••••••</span>
		<button class={GCP_MINI} onclick={show} data-testid="{testid}-reveal">Show keys</button>
	{/if}
</div>
