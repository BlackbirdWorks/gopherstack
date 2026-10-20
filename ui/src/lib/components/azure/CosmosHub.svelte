<script lang="ts">
	import Tabs from '#lib/components/Tabs.svelte';
	import CosmosDashboard from '#lib/components/azure/CosmosDashboard.svelte';
	import TableDashboard from '#lib/components/azure/TableDashboard.svelte';
	import { COSMOS_TABLE_ACCOUNT } from '#lib/azuretable.ts';

	const tabs = [
		{ id: 'sql', label: 'SQL API' },
		{ id: 'table', label: 'Table API' }
	];

	let api = $state('sql');
</script>

<div class="space-y-4" data-testid="cosmos-hub">
	<Tabs {tabs} active={api} onSelect={(id) => (api = id)} color="blue" />

	{#if api === 'table'}
		<TableDashboard account={COSMOS_TABLE_ACCOUNT} />
	{:else}
		<CosmosDashboard />
	{/if}
</div>
