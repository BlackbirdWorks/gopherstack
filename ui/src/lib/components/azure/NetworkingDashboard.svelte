<script lang="ts">
	import { onMount } from 'svelte';
	import { Network, RefreshCw } from 'lucide-svelte';
	import NetworkTabBody from './NetworkTabBody.svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { getSubscriptionId, listResourceGroups } from '#lib/azurearm.ts';
	import { GCP_INPUT } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'vnets', label: 'Virtual networks' },
		{ id: 'nsgs', label: 'Network security groups' },
		{ id: 'ips', label: 'Public IPs' },
		{ id: 'lbs', label: 'Load balancers' },
		{ id: 'agws', label: 'Application gateways' },
		{ id: 'firewalls', label: 'Firewalls' }
	];

	let tab = $state('vnets');
	let subscription = $state('');
	let groups = $state<string[]>([]);
	let group = $state('');
	let loading = $state(true);
	let error = $state('');
	let reloads = $state(0);

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			groups = (await listResourceGroups(subscription)).map((g) => g.name).toSorted();
			if (!groups.includes(group)) group = groups[0] ?? '';
			error = '';
			reloads += 1;
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="network-dashboard">
	<PageHeader icon={Network} title="Networking" description="Virtual networks, security groups, public IPs, load balancers, application gateways and firewalls" color="sky" onRefresh={load} />

	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="network-error">
			<p class="font-medium">Could not load resource groups</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if groups.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="network-empty">No resource groups. Create one in Resource Manager first.</p>
	{:else}
		<div class="flex flex-wrap items-center gap-2 text-xs text-gray-500">
			<span>Subscription</span><span class="font-mono" data-testid="network-subscription">{subscription}</span>
			<label for="network-group" class="ml-4">Resource group</label>
			<select id="network-group" class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={group} data-testid="network-group">
				{#each groups as g (g)}<option value={g}>{g}</option>{/each}
			</select>
		</div>
		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="sky" />
		{#key `${group}:${reloads}`}
			<NetworkTabBody {tab} {subscription} {group} />
		{/key}
	{/if}
</div>
