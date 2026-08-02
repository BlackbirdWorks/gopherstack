<script lang="ts">
	// Direct Connect gateway attachment creation fields -- one of the five
	// subtypes AttachmentsPanel's create modal composes
	// (services/networkmanager/PARITY.md family Q4). The DCG ARN is
	// unvalidated client-side since there is no services/directconnect
	// backend yet to check it against.
	import {
		CreateDirectConnectGatewayAttachmentCommand,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';

	type Props = {
		client: () => NetworkManagerClient;
	};

	let { client }: Props = $props();

	let coreNetworkId = $state('');
	let dcgArn = $state('');
	let edgeLocations = $state('');

	export function reset(): void {
		coreNetworkId = '';
		dcgArn = '';
		edgeLocations = '';
	}

	export async function submit(): Promise<string | null> {
		if (!coreNetworkId.trim() || !dcgArn.trim() || !edgeLocations.trim()) {
			return 'Core network ID, Direct Connect gateway ARN and at least one edge location are required.';
		}
		await client().send(
			new CreateDirectConnectGatewayAttachmentCommand({
				CoreNetworkId: coreNetworkId.trim(),
				DirectConnectGatewayArn: dcgArn.trim(),
				EdgeLocations: edgeLocations
					.split(',')
					.map((s) => s.trim())
					.filter(Boolean)
			})
		);
		return null;
	}
</script>

<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-dcg">Core network ID *
	<input id="nm-att-cn-dcg" bind:value={coreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-dcg-arn">Direct Connect gateway ARN * <span class="text-xs text-amber-600 dark:text-amber-400">(unvalidated -- no services/directconnect backend yet)</span>
	<input id="nm-att-dcg-arn" bind:value={dcgArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-edges">Edge locations (comma-separated) *
	<input id="nm-att-edges" bind:value={edgeLocations} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
