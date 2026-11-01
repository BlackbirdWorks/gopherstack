<script lang="ts">
	import NetworkAppGateways from './NetworkAppGateways.svelte';
	import NetworkFirewalls from './NetworkFirewalls.svelte';
	import NetworkLoadBalancers from './NetworkLoadBalancers.svelte';
	import NetworkNsgs from './NetworkNsgs.svelte';
	import NetworkPublicIps from './NetworkPublicIps.svelte';
	import NetworkVnets from './NetworkVnets.svelte';

	let { tab, subscription, group }: { tab: string; subscription: string; group: string } = $props();
</script>

{#if tab === 'vnets'}
	<NetworkVnets {subscription} {group} />
{:else if tab === 'nsgs'}
	<NetworkNsgs {subscription} {group} />
{:else if tab === 'ips'}
	<NetworkPublicIps {subscription} {group} />
{:else if tab === 'lbs'}
	<NetworkLoadBalancers {subscription} {group} />
{:else if tab === 'agws'}
	<NetworkAppGateways {subscription} {group} />
{:else if tab === 'firewalls'}
	<NetworkFirewalls {subscription} {group} />
{/if}
