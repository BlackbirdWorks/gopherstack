<script lang="ts">
	import { onMount } from 'svelte';
	import { getELBClient } from '$lib/aws-client';
	import { DescribeLoadBalancersCommand, CreateLoadBalancerCommand, DeleteLoadBalancerCommand } from '@aws-sdk/client-elastic-load-balancing';
	import { toast } from 'svelte-sonner';

	const elb = getELBClient();
	let loadBalancers = $state<Array<{ LoadBalancerName?: string; DNSName?: string }>>([]);
	let showCreateModal = $state(false);
	let name = $state('');

	async function loadBalancersList() {
		const out = await elb.send(new DescribeLoadBalancersCommand({}));
		loadBalancers = out.LoadBalancerDescriptions ?? [];
	}

	async function createLoadBalancer() {
		try {
			await elb.send(new CreateLoadBalancerCommand({ LoadBalancerName: name, AvailabilityZones: ['us-east-1a'] }));
			showCreateModal = false;
			name = '';
			await loadBalancersList();
		} catch (err: unknown) {
			toast.error(`Failed to create load balancer: ${(err as Error).message}`);
		}
	}

	async function deleteLoadBalancer(loadBalancerName: string) {
		try {
			await elb.send(new DeleteLoadBalancerCommand({ LoadBalancerName: loadBalancerName }));
			await loadBalancersList();
		} catch (err: unknown) {
			toast.error(`Failed to delete load balancer: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void loadBalancersList();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Elastic Load Balancers</h1>
	</div>
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="mb-4 flex justify-end"><button type="button" onclick={() => (showCreateModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Create Load Balancer</button></div>
		<table class="w-full text-left text-sm"><tbody>{#if loadBalancers.length === 0}<tr><td class="py-3 text-slate-500 dark:text-slate-400">No load balancers</td></tr>{:else}{#each loadBalancers as loadBalancer}<tr class="border-b border-slate-100 dark:border-slate-800"><td class="py-3">{loadBalancer.LoadBalancerName}</td><td class="py-3"><button type="button" onclick={() => deleteLoadBalancer(loadBalancer.LoadBalancerName ?? '')} class="rounded-lg border px-3 py-1 text-sm">Delete</button></td></tr>{/each}{/if}</tbody></table>
	</div>
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<form onsubmit={(event) => { event.preventDefault(); createLoadBalancer(); }} class="space-y-4">
				<input id="name" bind:value={name} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Load balancer name" />
				<div class="flex justify-end gap-3"><button type="button" onclick={() => (showCreateModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
			</form>
		</div>
	</div>
{/if}
