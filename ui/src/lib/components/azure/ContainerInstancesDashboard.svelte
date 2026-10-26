<script lang="ts">
	import { onMount } from 'svelte';
	import { Container, RefreshCw, Trash2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import AciGroupDetail from '#lib/components/azure/AciGroupDetail.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import { byResourceGroup, deleteGroup, groupAction, listGroups, type AciAction, type AciGroup } from '#lib/azureaci.ts';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let subscription = $state('');
	let groups = $state<AciGroup[]>([]);
	let loading = $state(true);
	let error = $state('');
	let filter = $state('');
	let selected = $state('');

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			groups = await listGroups(subscription);
			error = '';
			if (!groups.some((g) => g.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function act(g: AciGroup, action: AciAction) {
		if (await attempt(() => groupAction(subscription, g, action), `${action[0].toUpperCase()}${action.slice(1)} requested for ${g.name}`, `Failed to ${action} ${g.name}`)) {
			await load();
			for (const ms of [1500, 5000, 12000]) setTimeout(() => void load(), ms);
		}
	}

	async function remove(g: AciGroup) {
		if (!(await confirmDestructive({ title: 'Delete container group', message: `Delete container group "${g.name}" and its containers?` }))) return;
		if (await attempt(() => deleteGroup(subscription, g), `Deleted ${g.name}`, `Failed to delete ${g.name}`)) await load();
	}

	const rgs = $derived([...new Set(groups.map((g) => g.resourceGroup))]);
	const shown = $derived(byResourceGroup(groups.filter((g) => !filter || g.resourceGroup === filter)));
	const current = $derived(groups.find((g) => g.id === selected));

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'Running' || s === 'Succeeded' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'Failed' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="aci-dashboard">
	<PageHeader icon={Container} title="Container Instances" description="Azure Container Instances: container groups, container state, events and logs" color="sky" onRefresh={load} />

	{#if error && groups.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="aci-error">
			<p class="font-medium">Could not load container groups</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if groups.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="aci-empty">No container groups. Create one through Resource Manager (Microsoft.ContainerInstance/containerGroups).</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Resource group
			<select class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={filter} data-testid="aci-rg-filter">
				<option value="">All resource groups</option>
				{#each rgs as rg (rg)}<option value={rg}>{rg}</option>{/each}
			</select>
		</label>

		{#each shown as bucket (bucket.resourceGroup)}
			<section class="space-y-2" data-testid="aci-rg">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300 font-mono">{bucket.resourceGroup}</h2>
				<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
							<tr><th class={cell}>Group</th><th class={cell}>Provisioning</th><th class={cell}>State</th><th class={cell}>Restart policy</th><th class={cell}>IP / FQDN</th><th class={cell}>Ports</th><th class={cell}>Containers</th><th class="{cell} text-right">Actions</th></tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each bucket.groups as g (g.id)}
								<tr class={selected === g.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="aci-group">
									<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = selected === g.id ? '' : g.id)} data-testid="aci-select">{g.name}</button></td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(g.provisioningState)}" data-testid="aci-provisioning">{g.provisioningState || '—'}</span></td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(g.instanceState)}" data-testid="aci-state">{g.instanceState || '—'}</span></td>
									<td class="{cell} text-xs">{g.restartPolicy}</td>
									<td class="{cell} font-mono text-xs break-all">{g.fqdn || g.ip || '—'}{g.fqdn && g.ip ? ` (${g.ip})` : ''}</td>
									<td class="{cell} text-xs">{g.ports.join(', ') || '—'}</td>
									<td class="{cell} text-xs">{g.containers.map((c) => c.name).join(', ')}</td>
									<td class="{cell} text-right whitespace-nowrap space-x-1">
										<button class={GCP_MINI} onclick={() => act(g, 'start')} data-testid="aci-start">Start</button>
										<button class={GCP_MINI} onclick={() => act(g, 'stop')} data-testid="aci-stop">Stop</button>
										<button class={GCP_MINI} onclick={() => act(g, 'restart')} data-testid="aci-restart">Restart</button>
										<button class="{GCP_MINI} text-red-600" onclick={() => remove(g)} aria-label="Delete {g.name}" data-testid="aci-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</section>
		{/each}

		{#if current}
			<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-4 space-y-3">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name}</h2>
				<AciGroupDetail {subscription} group={current} />
			</div>
		{/if}
	{/if}
</div>
