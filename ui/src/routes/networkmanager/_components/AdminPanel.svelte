<script lang="ts">
	// Two small administrative surfaces bundled into one tab since neither is
	// a listable resource family on its own:
	//
	// - AWS Organizations integration (services/networkmanager/PARITY.md
	//   family V, 2 ops): enables/disables the Network Manager service-linked
	//   role trust relationship org-wide.
	// - Resource-based policy (family W, 3 ops): a cross-account-sharing IAM
	//   policy document keyed by any Network Manager resource ARN --
	//   structurally unrelated to the CoreNetworkPolicy network-configuration
	//   document on the Core Networks tab, despite both being called
	//   "policy".
	import {
		ListOrganizationServiceAccessStatusCommand,
		StartOrganizationServiceAccessUpdateCommand,
		GetResourcePolicyCommand,
		PutResourcePolicyCommand,
		DeleteResourcePolicyCommand,
		type OrganizationStatus,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client }: Props = $props();

	// ------------------------- Organization access --------------------------

	let orgStatus = $state<OrganizationStatus | null>(null);
	let orgLoading = $state(false);
	let orgError = $state<string | null>(null);
	let orgBusy = $state(false);

	async function loadOrgStatus(): Promise<void> {
		orgLoading = true;
		orgError = null;
		try {
			const resp = await client().send(new ListOrganizationServiceAccessStatusCommand({}));
			orgStatus = resp.OrganizationStatus ?? null;
		} catch (e) {
			orgError = describeError(e);
		} finally {
			orgLoading = false;
		}
	}

	export async function refresh(): Promise<void> {
		await loadOrgStatus();
	}

	onRegionChange(() => void refresh());

	async function updateOrgAccess(action: 'ENABLE' | 'DISABLE'): Promise<void> {
		orgBusy = true;
		orgError = null;
		try {
			const resp = await client().send(new StartOrganizationServiceAccessUpdateCommand({ Action: action }));
			orgStatus = resp.OrganizationStatus ?? orgStatus;
			toast.success(`Organization service access ${action.toLowerCase()}d`);
		} catch (e) {
			orgError = describeError(e);
			toast.error(orgError);
		} finally {
			orgBusy = false;
		}
	}

	// ---------------------------- Resource policy ----------------------------

	let policyResourceArn = $state('');
	let policyDocument = $state('');
	let policyLoading = $state(false);
	let policyError = $state<string | null>(null);
	let policyBusy = $state(false);
	let policyFound = $state(false);

	async function lookupPolicy(): Promise<void> {
		if (!policyResourceArn.trim()) return;
		policyLoading = true;
		policyError = null;
		policyFound = false;
		try {
			const resp = await client().send(new GetResourcePolicyCommand({ ResourceArn: policyResourceArn.trim() }));
			policyDocument = resp.PolicyDocument ?? '';
			policyFound = true;
		} catch (e) {
			policyError = describeError(e);
			policyDocument = '';
		} finally {
			policyLoading = false;
		}
	}

	async function savePolicy(): Promise<void> {
		if (!policyResourceArn.trim() || !policyDocument.trim()) {
			policyError = 'Resource ARN and policy document are both required.';
			return;
		}
		policyBusy = true;
		policyError = null;
		try {
			await client().send(
				new PutResourcePolicyCommand({ ResourceArn: policyResourceArn.trim(), PolicyDocument: policyDocument })
			);
			toast.success('Resource policy saved');
			policyFound = true;
		} catch (e) {
			policyError = describeError(e);
			toast.error(policyError);
		} finally {
			policyBusy = false;
		}
	}

	async function deletePolicy(): Promise<void> {
		if (!policyResourceArn.trim()) return;
		const confirmed = await confirmDestructive(`Delete the resource policy on ${policyResourceArn}?`);
		if (!confirmed) return;
		try {
			await client().send(new DeleteResourcePolicyCommand({ ResourceArn: policyResourceArn.trim() }));
			toast.success('Resource policy deleted');
			policyDocument = '';
			policyFound = false;
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="space-y-6">
	<section class="rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-3">
		<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">AWS Organizations integration</h3>
		{#if orgError}<p class="text-sm text-red-600 dark:text-red-400">{orgError}</p>{/if}
		{#if orgLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if orgStatus}
			<dl class="grid grid-cols-2 gap-2 text-sm">
				<div><dt class="text-slate-500">Organization ID</dt><dd>{orgStatus.OrganizationId ?? '—'}</dd></div>
				<div><dt class="text-slate-500">Service access</dt><dd>{orgStatus.OrganizationAwsServiceAccessStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500">SLR deployment</dt><dd>{orgStatus.SLRDeploymentStatus ?? '—'}</dd></div>
			</dl>
		{:else}
			<p class="text-sm text-slate-500 dark:text-slate-400">No organization status available.</p>
		{/if}
		<div class="flex gap-2">
			<button onclick={() => updateOrgAccess('ENABLE')} disabled={orgBusy} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Enable</button>
			<button onclick={() => updateOrgAccess('DISABLE')} disabled={orgBusy} class="px-3 py-1.5 text-sm rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Disable</button>
		</div>
	</section>

	<section class="rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-3">
		<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Resource-based policy</h3>
		<p class="text-xs text-slate-500 dark:text-slate-400">
			A cross-account-sharing IAM policy document keyed by any Network Manager resource ARN --
			unrelated to the Core Network Policy (network configuration) on the Core Networks tab.
		</p>
		<label class="flex flex-col gap-1 text-sm" for="nm-rp-arn">
			Resource ARN
			<div class="flex gap-2">
				<input id="nm-rp-arn" bind:value={policyResourceArn} class="flex-1 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				<button onclick={lookupPolicy} disabled={policyLoading || !policyResourceArn.trim()} class="px-3 py-1 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Look up</button>
			</div>
		</label>
		<label class="flex flex-col gap-1 text-sm" for="nm-rp-doc">
			Policy document (JSON)
			<textarea id="nm-rp-doc" bind:value={policyDocument} rows="5" class="px-2 py-1 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
		</label>
		{#if policyError}<p class="text-sm text-red-600 dark:text-red-400">{policyError}</p>{/if}
		<div class="flex gap-2">
			<button onclick={savePolicy} disabled={policyBusy} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
			<button onclick={deletePolicy} disabled={!policyFound} class="px-3 py-1.5 text-sm rounded-lg bg-red-600 text-white hover:bg-red-700 disabled:opacity-50">Delete</button>
		</div>
	</section>
</div>
