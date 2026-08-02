<script lang="ts">
	// VPC peering -- family X (3 ops: PeerVpc, UnpeerVpc, IsVpcPeered). All
	// three take zero input fields: this models a single implicit
	// account-wide peering connection to the default EC2 VPC, not a named
	// resource (services/lightsail/PARITY.md family X) -- so this panel is a
	// singleton status view, not a DataTable.
	import { PeerVpcCommand, UnpeerVpcCommand, IsVpcPeeredCommand, type LightsailClient } from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client }: Props = $props();

	let peered = $state<boolean | null>(null);
	let loading = $state(false);
	let busy = $state(false);
	let error = $state<string | null>(null);

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			const resp = await client().send(new IsVpcPeeredCommand({}));
			peered = resp.isPeered ?? false;
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	onRegionChange(() => void refresh());

	async function doPeer(): Promise<void> {
		busy = true;
		try {
			await client().send(new PeerVpcCommand({}));
			toast.success('VPC peering requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			busy = false;
		}
	}

	async function doUnpeer(): Promise<void> {
		busy = true;
		try {
			await client().send(new UnpeerVpcCommand({}));
			toast.success('VPC unpeering requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			busy = false;
		}
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4 max-w-xl">
	<p class="text-sm text-slate-500 dark:text-slate-400">
		Lightsail models a single account-wide peering connection to the default EC2 VPC in this region --
		not a named, listable resource. PeerVpc/UnpeerVpc/IsVpcPeered all take zero input fields.
	</p>
	{#if loading}
		<p class="text-sm text-slate-500">Loading…</p>
	{:else}
		<p class="text-sm">
			Status:
			<span class="font-medium {peered ? 'text-green-600 dark:text-green-400' : 'text-slate-600 dark:text-slate-400'}">
				{peered ? 'Peered' : 'Not peered'}
			</span>
		</p>
		<div class="flex gap-2">
			<button
				onclick={doPeer}
				disabled={busy || peered === true}
				class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
			>
				Peer VPC
			</button>
			<button
				onclick={doUnpeer}
				disabled={busy || peered === false}
				class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600 disabled:opacity-50"
			>
				Unpeer VPC
			</button>
		</div>
	{/if}
</div>
