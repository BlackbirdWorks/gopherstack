<script lang="ts">
	// Shared "pick a Global Network" control. Sites/Devices/Links/
	// Connections/Associations/Route Analysis/Network Insights are all
	// scoped under a GlobalNetworkId (services/networkmanager/PARITY.md's
	// family A is the root container every one of those hangs off of), so
	// rather than duplicate a fetch-and-select block in six panels, it lives
	// here once. Defaults to the first global network once loaded; each
	// consuming panel reacts to `value` changing via its own `$effect`.
	import { DescribeGlobalNetworksCommand, type NetworkManagerClient } from '@aws-sdk/client-networkmanager';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		value: string;
		id?: string;
	};

	let { client, value = $bindable(), id = 'nm-global-network-select' }: Props = $props();

	let options = $state<{ id: string; label: string }[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	async function load(): Promise<void> {
		loading = true;
		error = null;
		try {
			const resp = await client().send(new DescribeGlobalNetworksCommand({ MaxResults: 50 }));
			options = (resp.GlobalNetworks ?? []).map((n) => ({
				id: n.GlobalNetworkId ?? '',
				label: n.Description ? `${n.GlobalNetworkId} — ${n.Description}` : (n.GlobalNetworkId ?? '')
			}));
			if (!value || !options.some((o) => o.id === value)) {
				value = options[0]?.id ?? '';
			}
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	onRegionChange(() => void load());
</script>

<div class="flex flex-col gap-1 text-sm">
	<label for={id} class="text-slate-500 dark:text-slate-400">Global network</label>
	<select
		{id}
		bind:value
		disabled={loading}
		class="px-2 py-1 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
	>
		{#if options.length === 0}
			<option value="">No global networks</option>
		{/if}
		{#each options as opt (opt.id)}
			<option value={opt.id}>{opt.label}</option>
		{/each}
	</select>
	{#if error}<p class="text-xs text-red-600 dark:text-red-400">{error}</p>{/if}
</div>
