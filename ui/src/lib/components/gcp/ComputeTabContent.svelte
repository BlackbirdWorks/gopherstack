<script lang="ts">
	import ComputeArmor from '#lib/components/gcp/ComputeArmor.svelte';
	import ComputeInstances from '#lib/components/gcp/ComputeInstances.svelte';
	import ComputeLoadBalancing from '#lib/components/gcp/ComputeLoadBalancing.svelte';
	import ComputeNetworks from '#lib/components/gcp/ComputeNetworks.svelte';
	import ComputeOperations from '#lib/components/gcp/ComputeOperations.svelte';
	import ComputeStorage from '#lib/components/gcp/ComputeStorage.svelte';

	type Props = { tab: string; project: string; zone: string; region: string; zones: string[]; refresh: number };
	let { tab, project, zone, region, zones, refresh }: Props = $props();
</script>

{#if tab === 'instances'}
	<ComputeInstances {project} {zone} {region} {zones} {refresh} />
{:else if tab === 'storage'}
	<ComputeStorage {project} {zone} {region} {refresh} />
{:else if tab === 'networks'}
	<ComputeNetworks {project} {region} {refresh} />
{:else if tab === 'lb'}
	<ComputeLoadBalancing {project} {region} {refresh} />
{:else if tab === 'armor'}
	<ComputeArmor {project} {refresh} />
{:else}
	<ComputeOperations {project} {zone} {region} {refresh} />
{/if}
