<script lang="ts">
	// VPC attachment creation fields -- one of the five subtypes
	// AttachmentsPanel's create modal composes (services/networkmanager/
	// PARITY.md family Q1). Owns its own fields, validation and the actual
	// CreateVpcAttachmentCommand call so the parent's submitCreate no longer
	// has to branch on attachment kind.
	import { CreateVpcAttachmentCommand, type NetworkManagerClient } from '@aws-sdk/client-networkmanager';

	type Props = {
		client: () => NetworkManagerClient;
	};

	let { client }: Props = $props();

	let coreNetworkId = $state('');
	let vpcArn = $state('');
	let subnetArns = $state('');

	export function reset(): void {
		coreNetworkId = '';
		vpcArn = '';
		subnetArns = '';
	}

	// Returns a validation error message if the form is incomplete, or
	// `null` after successfully sending the create call. Thrown AWS errors
	// propagate to the caller, matching every other panel's submitCreate.
	export async function submit(): Promise<string | null> {
		if (!coreNetworkId.trim() || !vpcArn.trim() || !subnetArns.trim()) {
			return 'Core network ID, VPC ARN and at least one subnet ARN are required.';
		}
		await client().send(
			new CreateVpcAttachmentCommand({
				CoreNetworkId: coreNetworkId.trim(),
				VpcArn: vpcArn.trim(),
				SubnetArns: subnetArns
					.split(',')
					.map((s) => s.trim())
					.filter(Boolean)
			})
		);
		return null;
	}
</script>

<label class="flex flex-col gap-1 text-sm" for="nm-att-cn-vpc">Core network ID *
	<input id="nm-att-cn-vpc" bind:value={coreNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-vpc-arn">VPC ARN *
	<input id="nm-att-vpc-arn" bind:value={vpcArn} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
<label class="flex flex-col gap-1 text-sm" for="nm-att-subnets">Subnet ARNs (comma-separated) *
	<input id="nm-att-subnets" bind:value={subnetArns} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
</label>
