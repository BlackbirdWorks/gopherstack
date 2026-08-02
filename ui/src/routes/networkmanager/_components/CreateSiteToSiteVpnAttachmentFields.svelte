<script lang="ts">
	// Site-to-Site VPN attachment creation fields -- one of the five
	// subtypes AttachmentsPanel's create modal composes
	// (services/networkmanager/PARITY.md family Q3).
	import { CreateSiteToSiteVpnAttachmentCommand, type NetworkManagerClient } from '@aws-sdk/client-networkmanager';

	type Props = {
		client: () => NetworkManagerClient;
	};

	let { client }: Props = $props();

	let coreNetworkId = $state('');
	let vpnConnectionArn = $state('');

	export function reset(): void {
		coreNetworkId = '';
		vpnConnectionArn = '';
	}

	export async function submit(): Promise<string | null> {
		if (!coreNetworkId.trim() || !vpnConnectionArn.trim()) {
			return 'Core network ID and VPN connection ARN are required.';
		}
		await client().send(
			new CreateSiteToSiteVpnAttachmentCommand({
				CoreNetworkId: coreNetworkId.trim(),
				VpnConnectionArn: vpnConnectionArn.trim()
			})
		);
		return null;
	}
</script>

<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-vpn">Core network ID *
	<input id="nm-att-cn-vpn" bind:value={coreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-vpn-arn">VPN connection ARN *
	<input id="nm-att-vpn-arn" bind:value={vpnConnectionArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
