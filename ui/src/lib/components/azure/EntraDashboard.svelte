<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw, ShieldCheck } from 'lucide-svelte';
	import EntraTabBody from './EntraTabBody.svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { getSubscriptionId, listResourceGroups } from '#lib/azurearm.ts';
	import { groupScope, subscriptionScope } from '#lib/azureentra.ts';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'apps', label: 'Applications' },
		{ id: 'sps', label: 'Service principals' },
		{ id: 'directory', label: 'Users & groups' },
		{ id: 'roles', label: 'Role assignments' },
		{ id: 'locks', label: 'Locks' },
		{ id: 'policy', label: 'Policy' }
	];
	const SCOPED = new Set(['roles', 'locks', 'policy']);

	let tab = $state('apps');
	let subscription = $state('');
	let groups = $state<string[]>([]);
	let group = $state('');
	let loading = $state(true);
	let error = $state('');

	const scope = $derived(subscription ? (group ? groupScope(subscription, group) : subscriptionScope(subscription)) : '');

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			groups = (await listResourceGroups(subscription)).map((g) => g.name).toSorted();
			if (group && !groups.includes(group)) group = '';
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="entra-dashboard">
	<PageHeader icon={ShieldCheck} title="Entra ID & access" description="Applications, service principals, users, groups, role assignments, locks and policy" color="indigo" onRefresh={load} />

	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		{#if error}
			<p class="text-sm text-red-600" role="alert" data-testid="entra-error">Could not load resource groups: {error}</p>
		{/if}
		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="indigo" />

		{#if SCOPED.has(tab) && subscription}
			<div class="flex flex-wrap items-center gap-2 text-xs text-gray-500">
				<label for="entra-scope">Scope</label>
				<select id="entra-scope" class="{GCP_INPUT} !w-72 !py-1 text-xs" bind:value={group} data-testid="entra-scope">
					<option value="">Subscription {subscription}</option>
					{#each groups as g (g)}<option value={g}>Resource group {g}</option>{/each}
				</select>
				<span class="font-mono" data-testid="entra-scope-id">{scope}</span>
			</div>
		{/if}

		<EntraTabBody {tab} {scope} {subscription} />
	{/if}
</div>
