<script lang="ts">
	// Link associations -- one of the five kinds AssociationsPanel composes
	// (services/networkmanager/PARITY.md family E). Binds a Device+Link pair
	// together directly (no foreign resource involved, unlike its siblings).
	import {
		GetLinkAssociationsCommand,
		AssociateLinkCommand,
		DisassociateLinkCommand,
		type LinkAssociation,
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

	let linkAssociations = $state<LinkAssociation[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	async function load(): Promise<void> {
		if (!globalNetworkId) return;
		loading = true;
		error = null;
		try {
			const resp = await client().send(new GetLinkAssociationsCommand({ GlobalNetworkId: globalNetworkId }));
			linkAssociations = resp.LinkAssociations ?? [];
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

	const rows = $derived(linkAssociations.filter((a) => matchesSearch(searchQuery, a.DeviceId, a.LinkId)));

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let formDeviceId = $state('');
	let formLinkId = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
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
			if (!formDeviceId.trim() || !formLinkId.trim()) {
				createError = 'Device ID and Link ID are required.';
				return;
			}
			await client().send(
				new AssociateLinkCommand({
					GlobalNetworkId: globalNetworkId,
					DeviceId: formDeviceId.trim(),
					LinkId: formLinkId.trim()
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

	async function disassociateLink(a: LinkAssociation): Promise<void> {
		if (!a.DeviceId || !a.LinkId) return;
		const confirmed = await confirmDestructive(`Disassociate link ${a.LinkId} from device ${a.DeviceId}?`);
		if (!confirmed) return;
		try {
			await client().send(
				new DisassociateLinkCommand({ GlobalNetworkId: globalNetworkId, DeviceId: a.DeviceId, LinkId: a.LinkId })
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
			Create link association
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
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">No link associations found</div>
	{:else}
		<table class="w-full text-sm">
			<thead>
				<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
					<th class="px-4 py-2 font-medium">Device</th>
					<th class="px-4 py-2 font-medium">Link</th>
					<th class="px-4 py-2 font-medium">State</th>
					<th class="px-4 py-2 font-medium"></th>
				</tr>
			</thead>
			<tbody>
				{#each rows as a (a.DeviceId + '/' + a.LinkId)}
					<tr class="border-b border-gray-100 dark:border-gray-800">
						<td class="px-4 py-3">{a.DeviceId}</td>
						<td class="px-4 py-3">{a.LinkId}</td>
						<td class="px-4 py-3">{a.LinkAssociationState ?? '—'}</td>
						<td class="px-4 py-3 text-right">
							<button onclick={() => disassociateLink(a)} class="text-red-600 hover:underline text-sm">Disassociate</button>
						</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{/if}
</div>

<Modal bind:this={createModal} title="Create Link association">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-assoc-link-device">
				Device ID *
				<input id="nm-assoc-link-device" bind:value={formDeviceId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-assoc-link-link">
				Link ID *
				<input id="nm-assoc-link-link" bind:value={formLinkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>
