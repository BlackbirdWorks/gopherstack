<script lang="ts">
	import { onMount } from 'svelte';
	import { attempt, copyText, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { listAccessKeys, revealAccessKey, type AcAccessKey } from '#lib/azureappconfig.ts';

	let { store }: { store: string } = $props();

	let keys = $state<AcAccessKey[]>([]);
	let secrets = $state<Record<string, string>>({});
	let loading = $state(true);
	let error = $state('');

	async function load() {
		try {
			keys = await listAccessKeys(store);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function show(k: AcAccessKey) {
		await attempt(async () => {
			secrets[k.id] = await revealAccessKey(store, k.id);
		}, '', 'Failed to get the key');
	}

	function hide(id: string) {
		delete secrets[id];
	}

	onMount(() => void load());
</script>

<div data-testid="appconfig-keys">
	{#if error}
		<p class="text-sm text-red-600" role="alert">{error}</p>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">Id</th><th class="px-4 py-2">Access</th><th class="px-4 py-2">Secret</th></tr></thead>
				<tbody>
					{#each keys as k (k.id)}
						<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="appconfig-key-row">
							<td class="px-4 py-2">{k.name}</td>
							<td class="px-4 py-2 font-mono text-xs">{k.id}</td>
							<td class="px-4 py-2">{k.readOnly ? 'Read only' : 'Read-write'}</td>
							<td class="px-4 py-2 font-mono text-xs">
								{#if secrets[k.id] !== undefined}
									<span class="break-all" data-testid="appconfig-key-secret">{secrets[k.id]}</span>
									<button class={GCP_MINI} onclick={() => hide(k.id)} data-testid="appconfig-key-hide">Hide</button>
									<button class={GCP_MINI} onclick={() => copyText(secrets[k.id] ?? '')}>Copy</button>
								{:else}
									<span data-testid="appconfig-key-masked">{k.masked}</span>
									<button class={GCP_MINI} onclick={() => show(k)} data-testid="appconfig-key-reveal">Show</button>
								{/if}
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>
