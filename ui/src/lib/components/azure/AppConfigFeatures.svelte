<script lang="ts">
	import { onMount } from 'svelte';
	import { Flag } from 'lucide-svelte';
	import { attempt } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listFeatures, toggleFeature, type AcFeature } from '#lib/azureappconfig.ts';

	let { store }: { store: string } = $props();

	let flags = $state<AcFeature[]>([]);
	let loading = $state(true);
	let error = $state('');

	async function load() {
		try {
			flags = await listFeatures(store);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function flip(f: AcFeature) {
		if (await attempt(() => toggleFeature(store, f.id, f.label, !f.enabled), `${f.id} ${f.enabled ? 'disabled' : 'enabled'}`, `Failed to update ${f.id}`)) await load();
	}

	onMount(() => void load());
</script>

<div data-testid="appconfig-features">
	{#if error}
		<p class="text-sm text-red-600" role="alert">{error}</p>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Feature</th><th class="px-4 py-2">Label</th><th class="px-4 py-2">Description</th><th class="px-4 py-2">Filters</th><th class="px-4 py-2">Enabled</th></tr></thead>
				<tbody>
					{#each flags as f (f.id + '\0' + f.label)}
						<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="appconfig-feature">
							<td class="px-4 py-2 font-mono"><Flag class="mr-1 inline h-3 w-3" />{f.id}</td>
							<td class="px-4 py-2">{f.label || '(none)'}</td>
							<td class="px-4 py-2">{f.description || '-'}</td>
							<td class="px-4 py-2">{f.filterCount}</td>
							<td class="px-4 py-2">
								<button role="switch" aria-checked={f.enabled} aria-label="Toggle {f.id}" disabled={f.locked} onclick={() => flip(f)}
									class="relative inline-flex h-5 w-9 items-center rounded-full transition disabled:opacity-50 {f.enabled ? 'bg-green-600' : 'bg-gray-300 dark:bg-gray-600'}" data-testid="appconfig-feature-toggle">
									<span class="inline-block h-4 w-4 rounded-full bg-white transition {f.enabled ? 'translate-x-4' : 'translate-x-0.5'}"></span>
								</button>
								<span class="ml-2 text-xs" data-testid="appconfig-feature-state">{f.enabled ? 'On' : 'Off'}</span>
							</td>
						</tr>
					{:else}
						<tr><td class="px-4 py-6 text-center text-gray-500" colspan="5" data-testid="appconfig-features-empty">No feature flags in this store.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
