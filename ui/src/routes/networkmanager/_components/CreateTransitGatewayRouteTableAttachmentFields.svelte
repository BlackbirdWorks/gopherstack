<script lang="ts">
	// Transit gateway route table attachment creation fields -- one of the
	// five subtypes AttachmentsPanel's create modal composes
	// (services/networkmanager/PARITY.md family Q5). Unlike its four
	// siblings this one hangs off an existing Peering, not a Core network ID
	// directly.
	import {
		CreateTransitGatewayRouteTableAttachmentCommand,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';

	type Props = {
		client: () => NetworkManagerClient;
	};

	let { client }: Props = $props();

	let peeringId = $state('');
	let tgwRouteTableArn = $state('');

	export function reset(): void {
		peeringId = '';
		tgwRouteTableArn = '';
	}

	export async function submit(): Promise<string | null> {
		if (!peeringId.trim() || !tgwRouteTableArn.trim()) {
			return 'Peering ID and transit gateway route table ARN are required.';
		}
		await client().send(
			new CreateTransitGatewayRouteTableAttachmentCommand({
				PeeringId: peeringId.trim(),
				TransitGatewayRouteTableArn: tgwRouteTableArn.trim()
			})
		);
		return null;
	}
</script>

<label class="flex flex-col gap-1 text-sm" for="nm-att-peering">Peering ID *
	<input id="nm-att-peering" bind:value={peeringId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-tgw-rt">Transit gateway route table ARN *
	<input id="nm-att-tgw-rt" bind:value={tgwRouteTableArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
