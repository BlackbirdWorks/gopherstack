<script lang="ts">
	import { attempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { revealMetadata, type Instance } from '#lib/gcpcompute.ts';

	type Props = { project: string; instance: Instance; labels: string };
	let { project, instance, labels }: Props = $props();

	let revealed = $state<Record<string, string>>({});

	async function reveal(key: string) {
		let value = '';
		const ok = await attempt(async () => (value = await revealMetadata(project, instance.zone, instance.name, key)), '', `Failed to read ${key}`);
		if (ok) revealed[key] = value;
	}

	function hide(key: string) {
		delete revealed[key];
	}

	const th = 'py-1 pr-3 font-medium';
</script>

<div class="space-y-3" data-testid="compute-instance-info">
	<div class="grid gap-3 sm:grid-cols-2">
		<div>
			<p class="mb-1 font-medium">Disks</p>
			{#if instance.disks.length === 0}<p class="text-gray-500">No disks</p>{:else}
				<table class="w-full">
					<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={th}>Name</th><th class={th}>Boot</th><th class={th}>Mode</th><th class={th}>Size</th></tr></thead>
					<tbody>
						{#each instance.disks as d (d.name)}
							<tr data-testid="compute-instance-disk"><td class="py-1 pr-3 font-mono">{d.name}</td><td class="py-1 pr-3">{d.boot ? 'boot' : '—'}</td><td class="py-1 pr-3">{d.mode || '—'}</td><td class="py-1 pr-3">{d.sizeGb ? `${d.sizeGb} GB` : '—'}</td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
		<div>
			<p class="mb-1 font-medium">Network interfaces</p>
			{#if instance.nics.length === 0}<p class="text-gray-500">No network interfaces</p>{:else}
				<table class="w-full">
					<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={th}>Interface</th><th class={th}>Network</th><th class={th}>Subnetwork</th><th class={th}>Internal IP</th><th class={th}>External IP</th></tr></thead>
					<tbody>
						{#each instance.nics as n, i (i)}
							<tr data-testid="compute-instance-nic"><td class="py-1 pr-3 font-mono">{n.name || `nic${i}`}</td><td class="py-1 pr-3">{n.network || '—'}</td><td class="py-1 pr-3">{n.subnetwork || '—'}</td><td class="py-1 pr-3 font-mono">{n.internalIp || '—'}</td><td class="py-1 pr-3 font-mono">{n.externalIps.join(', ') || '—'}</td></tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	</div>
	{#if labels}<p><span class="font-medium">Labels</span> <span class="font-mono">{labels}</span></p>{/if}
	{#if instance.tags.length > 0}<p><span class="font-medium">Network tags</span> <span class="font-mono">{instance.tags.join(', ')}</span></p>{/if}
	<div>
		<p class="mb-1 font-medium">Metadata <span class="font-normal text-gray-500">(values are hidden; a key can be revealed on request, SSH keys never)</span></p>
		{#if instance.metadata.length === 0}<p class="text-gray-500" data-testid="compute-metadata-empty">No metadata</p>{:else}
			<ul class="space-y-1">
				{#each instance.metadata as m (m.key)}
					<li class="flex flex-wrap items-center gap-2" data-testid="compute-metadata-entry">
						<span class="font-mono" data-testid="compute-metadata-key">{m.key}</span>
						<span class="text-gray-500">{m.length} bytes</span>
						{#if m.key in revealed}
							<button class={GCP_MINI} onclick={() => hide(m.key)} data-testid="compute-metadata-hide">Hide</button>
							<pre class="w-full max-h-40 overflow-auto whitespace-pre-wrap break-all rounded-md bg-white p-2 font-mono dark:bg-slate-900" data-testid="compute-metadata-value">{revealed[m.key]}</pre>
						{:else if m.revealable}
							<button class={GCP_MINI} onclick={() => reveal(m.key)} data-testid="compute-metadata-reveal">Reveal</button>
						{:else}
							<span class="text-gray-500" data-testid="compute-metadata-protected">never shown</span>
						{/if}
					</li>
				{/each}
			</ul>
		{/if}
	</div>
</div>
