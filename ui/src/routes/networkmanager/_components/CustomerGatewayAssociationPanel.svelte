<script lang="ts">
	// Customer gateway associations -- one of the five kinds AssociationsPanel
	// composes (services/networkmanager/PARITY.md family G). Binds an
	// existing (EC2-side) customer gateway ARN to a Device, optionally a Link.
	import {
		GetCustomerGatewayAssociationsCommand,
		AssociateCustomerGatewayCommand,
		DisassociateCustomerGatewayCommand,
		type CustomerGatewayAssociation,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import Modal from '$lib/components/Modal.svelte';
	import { describeError, matchesSearch } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		globalNetworkId: string;
		searchQuery: string;
	};

	let { client, globalNetworkId, searchQuery }: Props = $props();

	let customerGatewayAssociations = $state<CustomerGatewayAssociation[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	async function load(): Promise<void> {
		if (!globalNetworkId) return;
		loading = true;
		error = null;
		try {
			const resp = await client().send(
				new GetCustomerGatewayAssociationsCommand({ GlobalNetworkId: globalNetworkId })
			);
			customerGatewayAssociations = resp.CustomerGatewayAssociations ?? [];
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	export async function refresh(): Promise<void> {
		await load();
	}

	$effect(() => {
		void load();
	});

	const rows = $derived(
		customerGatewayAssociations.filter((a) => matchesSearch(searchQuery, a.CustomerGatewayArn, a.DeviceId))
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let formForeignId = $state('');
	let formDeviceId = $state('');
	let formLinkId = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		formForeignId = '';
		formDeviceId = '';
		formLinkId = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!globalNetworkId) return;
		createBusy = true;
		createError = null;
		try {
			if (!formForeignId.trim() || !formDeviceId.trim()) {
				createError = 'Customer gateway ARN and Device ID are required.';
				return;
			}
			await client().send(
				new AssociateCustomerGatewayCommand({
					GlobalNetworkId: globalNetworkId,
					CustomerGatewayArn: formForeignId.trim(),
					DeviceId: formDeviceId.trim(),
					LinkId: formLinkId || undefined
				})
			);
			toast.success('Association created');
			createModal?.close();
			await load();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function disassociateCustomerGateway(a: CustomerGatewayAssociation): Promise<void> {
		if (!a.CustomerGatewayArn) return;
		const confirmed = await confirmDestructive(`Disassociate customer gateway ${a.CustomerGatewayArn}?`);
		if (!confirmed) return;
		try {
			await client().send(
				new DisassociateCustomerGatewayCommand({
					GlobalNetworkId: globalNetworkId,
					CustomerGatewayArn: a.CustomerGatewayArn
				})
			);
			toast.success('Disassociated');
			await load();
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="space-y-3">
	<div class="flex justify-end">
		<button
			onclick={openCreate}
			disabled={!globalNetworkId}
			class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
		>
			Create customer gateway association
		</button>
	</div>

	{#if error}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load data</p>
			<p>{error}</p>
		</div>
	{:else if loading}
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
	{:else if rows.length === 0}
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">No customer gateway associations found</div>
	{:else}
		<table class="w-full text-sm">
			<thead>
				<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
					<th class="px-4 py-2 font-medium">Customer gateway ARN</th>
					<th class="px-4 py-2 font-medium">Device</th>
					<th class="px-4 py-2 font-medium">Link</th>
					<th class="px-4 py-2 font-medium">State</th>
					<th class="px-4 py-2 font-medium"></th>
				</tr>
			</thead>
			<tbody>
				{#each rows as a (a.CustomerGatewayArn)}
					<tr class="border-b border-gray-100 dark:border-gray-800">
						<td class="px-4 py-3 break-all">{a.CustomerGatewayArn}</td>
						<td class="px-4 py-3">{a.DeviceId}</td>
						<td class="px-4 py-3">{a.LinkId ?? '—'}</td>
						<td class="px-4 py-3">{a.State ?? '—'}</td>
						<td class="px-4 py-3 text-right">
							<button onclick={() => disassociateCustomerGateway(a)} class="text-red-600 hover:underline text-sm">Disassociate</button>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</div>

<Modal bind:this={createModal} title="Create Customer Gateway association">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-assoc-cgw-arn">
				Customer gateway ARN *
				<input id="nm-assoc-cgw-arn" bind:value={formForeignId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-assoc-cgw-device">
				Device ID *
				<input id="nm-assoc-cgw-device" bind:value={formDeviceId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-assoc-cgw-link">
				Link ID (optional)
				<input id="nm-assoc-cgw-link" bind:value={formLinkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>
