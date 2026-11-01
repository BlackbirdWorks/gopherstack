<script lang="ts">
	import EntraApplications from './EntraApplications.svelte';
	import EntraDirectory from './EntraDirectory.svelte';
	import EntraLocks from './EntraLocks.svelte';
	import EntraPolicy from './EntraPolicy.svelte';
	import EntraRoles from './EntraRoles.svelte';
	import EntraServicePrincipals from './EntraServicePrincipals.svelte';

	type Props = { tab: string; scope: string; subscription: string };
	let { tab, scope, subscription }: Props = $props();
</script>

{#if tab === 'apps'}
	<EntraApplications />
{:else if tab === 'sps'}
	<EntraServicePrincipals />
{:else if tab === 'directory'}
	<EntraDirectory />
{:else if tab === 'roles' && scope}
	{#key scope}<EntraRoles {scope} {subscription} />{/key}
{:else if tab === 'locks' && scope}
	{#key scope}<EntraLocks {scope} />{/key}
{:else if tab === 'policy' && scope}
	{#key scope}<EntraPolicy {scope} {subscription} />{/key}
{/if}
