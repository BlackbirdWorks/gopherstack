<script lang="ts">
	// Connect attachment creation fields -- one of the five subtypes
	// AttachmentsPanel's create modal composes (services/networkmanager/
	// PARITY.md family Q2). GRE is the only transport protocol this service
	// supports, so it is hardcoded rather than exposed as a field.
	import { CreateConnectAttachmentCommand, type NetworkManagerClient } from '@aws-sdk/client-networkmanager';

	type Props = {
		client: () => NetworkManagerClient;
	};

	let { client }: Props = $props();

	let coreNetworkId = $state('');
	let edgeLocation = $state('');
	let transportAttachmentId = $state('');

	export function reset(): void {
		coreNetworkId = '';
		edgeLocation = '';
		transportAttachmentId = '';
	}

	export async function submit(): Promise<string | null> {
		if (!coreNetworkId.trim() || !edgeLocation.trim() || !transportAttachmentId.trim()) {
			return 'Core network ID, edge location and transport attachment ID are required.';
		}
		await client().send(
			new CreateConnectAttachmentCommand({
				CoreNetworkId: coreNetworkId.trim(),
				EdgeLocation: edgeLocation.trim(),
				TransportAttachmentId: transportAttachmentId.trim(),
				Options: { Protocol: 'GRE' }
			})
		);
		return null;
	}
</script>

<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-connect">Core network ID *
	<input id="nm-att-cn-connect" bind:value={coreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-edge">Edge location *
	<input id="nm-att-edge" bind:value={edgeLocation} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-transport">Transport attachment ID *
	<input id="nm-att-transport" bind:value={transportAttachmentId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
