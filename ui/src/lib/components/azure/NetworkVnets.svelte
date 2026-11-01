<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { DEFAULT_LOCATION, LOCATIONS } from '#lib/azurearm.ts';
	import {
		DELEGATION_SERVICES,
		deleteSubnet,
		deleteVnet,
		listNatGateways,
		listNsgs,
		listVnets,
		putSubnet,
		putVnet,
		validateSubnet,
		validateVnet,
		type NatGateway,
		type Nsg,
		type SubnetInput,
		type Vnet,
		type VnetInput
	} from '#lib/azurenetwork.ts';
	import { attempt, confirmedAttempt, GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let { subscription, group }: { subscription: string; group: string } = $props();

	const blankVnet = (): VnetInput => ({ name: '', location: DEFAULT_LOCATION, addressSpace: '10.0.0.0/16', subnetName: '', subnetPrefix: '' });
	const blankSubnet = (): SubnetInput => ({ name: '', prefix: '', nsg: '', nat: '', delegation: '' });

	let vnets = $state<Vnet[]>([]);
	let nsgs = $state<Nsg[]>([]);
	let nats = $state<NatGateway[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let vnetDraft = $state<VnetInput>(blankVnet());
	let subnetDraft = $state<SubnetInput>(blankSubnet());

	const current = $derived(vnets.find((v) => v.name === selected));
	const vnetError = $derived(validateVnet(vnetDraft));
	const subnetError = $derived(validateSubnet(subnetDraft));

	async function load() {
		loading = true;
		try {
			[vnets, nsgs, nats] = await Promise.all([listVnets(subscription, group), listNsgs(subscription, group), listNatGateways(subscription, group)]);
			error = '';
			if (!vnets.some((v) => v.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function createVnet(e: Event) {
		e.preventDefault();
		if (vnetError) return;
		if (await attempt(() => putVnet(subscription, group, vnetDraft), `Virtual network ${vnetDraft.name} created`, 'Failed to create virtual network')) {
			selected = vnetDraft.name;
			vnetDraft = blankVnet();
			await load();
		}
	}

	async function removeVnet(v: Vnet) {
		const ok = await confirmedAttempt({ title: 'Delete virtual network', message: `Delete "${v.name}" and its ${v.subnets.length} subnet(s)?`, confirmLabel: 'Delete' }, () => deleteVnet(subscription, group, v.name), `Virtual network ${v.name} deleted`, `Failed to delete ${v.name}`);
		if (ok) await load();
	}

	async function createSubnet(e: Event, v: Vnet) {
		e.preventDefault();
		if (subnetError) return;
		if (await attempt(() => putSubnet(subscription, group, v.name, subnetDraft), `Subnet ${subnetDraft.name} created`, 'Failed to create subnet')) {
			subnetDraft = blankSubnet();
			await load();
		}
	}

	async function removeSubnet(v: Vnet, name: string) {
		const ok = await confirmedAttempt({ title: 'Delete subnet', message: `Delete subnet "${name}" from "${v.name}"?`, confirmLabel: 'Delete' }, () => deleteSubnet(subscription, group, v.name, name), `Subnet ${name} deleted`, `Failed to delete ${name}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';

	$effect(() => {
		void group;
		void load();
	});
</script>

<div class="space-y-4" data-testid="net-vnets">
	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="net-vnets-error">Could not load virtual networks: {error}</p>
	{/if}
	{#if loading && vnets.length === 0}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if vnets.length === 0}
		<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="net-vnets-empty">No virtual networks in this resource group.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="net-vnet-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Location</th><th class={cell}>Address space</th><th class={cell}>Subnets</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each vnets as v (v.id)}
						<tr class={selected === v.name ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="net-vnet">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = selected === v.name ? '' : v.name)} aria-expanded={selected === v.name} data-testid="net-vnet-select">{v.name}</button></td>
							<td class="{cell} text-xs">{v.location}</td>
							<td class="{cell} font-mono text-xs" data-testid="net-vnet-space">{v.addressSpace.join(', ') || '—'}</td>
							<td class="{cell} text-xs" data-testid="net-vnet-subnet-count">{v.subnets.length}</td>
							<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeVnet(v)} data-testid="net-vnet-delete">Delete</button></td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	{#if current}
		<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="net-vnet-detail">
			<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Subnets of {current.name}</h3>
			{#if current.subnets.length === 0}
				<p class="text-sm text-gray-500" data-testid="net-subnets-empty">This virtual network has no subnets.</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="net-subnet-table">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Subnet</th><th class={cell}>Prefix</th><th class={cell}>Delegations</th><th class={cell}>NSG</th><th class={cell}>NAT gateway</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each current.subnets as s (s.name)}
								<tr data-testid="net-subnet">
									<td class="{cell} font-mono text-xs">{s.name}</td>
									<td class="{cell} font-mono text-xs">{s.addressPrefix}</td>
									<td class="{cell} text-xs" data-testid="net-subnet-delegations">{s.delegations.map((d) => d.service).join(', ') || '—'}</td>
									<td class="{cell} text-xs" data-testid="net-subnet-nsg">{s.nsg || '—'}</td>
									<td class="{cell} text-xs" data-testid="net-subnet-nat">{s.natGateway || '—'}</td>
									<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeSubnet(current, s.name)} data-testid="net-subnet-delete">Delete</button></td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}

			<form class="flex flex-wrap items-end gap-2" onsubmit={(e) => createSubnet(e, current)} data-testid="net-subnet-form">
				<label class="flex flex-col gap-1 text-xs">Name<input class="{GCP_INPUT} !w-36" bind:value={subnetDraft.name} data-testid="net-subnet-name" /></label>
				<label class="flex flex-col gap-1 text-xs">Prefix<input class="{GCP_INPUT} !w-36" placeholder="10.0.1.0/24" bind:value={subnetDraft.prefix} data-testid="net-subnet-prefix" /></label>
				<label class="flex flex-col gap-1 text-xs">NSG
					<select class="{GCP_INPUT} !w-36" bind:value={subnetDraft.nsg} data-testid="net-subnet-nsg-select">
						<option value="">None</option>
						{#each nsgs as n (n.id)}<option value={n.id}>{n.name}</option>{/each}
					</select>
				</label>
				<label class="flex flex-col gap-1 text-xs">NAT gateway
					<select class="{GCP_INPUT} !w-36" bind:value={subnetDraft.nat} data-testid="net-subnet-nat-select">
						<option value="">None</option>
						{#each nats as n (n.id)}<option value={n.id}>{n.name}</option>{/each}
					</select>
				</label>
				<label class="flex flex-col gap-1 text-xs">Delegation
					<select class="{GCP_INPUT} !w-56" bind:value={subnetDraft.delegation} data-testid="net-subnet-delegation-select">
						<option value="">None</option>
						{#each DELEGATION_SERVICES as d (d)}<option value={d}>{d}</option>{/each}
					</select>
				</label>
				<button class={GCP_PRIMARY} type="submit" disabled={subnetError !== null} data-testid="net-subnet-create">Add subnet</button>
			</form>
			{#if subnetDraft.name || subnetDraft.prefix}
				{#if subnetError}<p class="text-xs text-amber-700 dark:text-amber-400" data-testid="net-subnet-hint">{subnetError}</p>{/if}
			{/if}
		</div>
	{/if}

	<form class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={createVnet} data-testid="net-vnet-form">
		<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Create virtual network</h3>
		<div class="flex flex-wrap items-end gap-2">
			<label class="flex flex-col gap-1 text-xs">Name<input class="{GCP_INPUT} !w-40" bind:value={vnetDraft.name} data-testid="net-vnet-name" /></label>
			<label class="flex flex-col gap-1 text-xs">Location
				<select class="{GCP_INPUT} !w-40" bind:value={vnetDraft.location} data-testid="net-vnet-location">
					{#each LOCATIONS as l (l)}<option value={l}>{l}</option>{/each}
				</select>
			</label>
			<label class="flex flex-col gap-1 text-xs">Address space<input class="{GCP_INPUT} !w-44" bind:value={vnetDraft.addressSpace} data-testid="net-vnet-address-space" /></label>
			<label class="flex flex-col gap-1 text-xs">First subnet (optional)<input class="{GCP_INPUT} !w-36" bind:value={vnetDraft.subnetName} data-testid="net-vnet-subnet-name" /></label>
			<label class="flex flex-col gap-1 text-xs">Subnet prefix<input class="{GCP_INPUT} !w-36" placeholder="10.0.0.0/24" bind:value={vnetDraft.subnetPrefix} data-testid="net-vnet-subnet-prefix" /></label>
			<button class={GCP_PRIMARY} type="submit" disabled={vnetError !== null} data-testid="net-vnet-create">Create</button>
			<button class={GCP_GHOST} type="button" onclick={() => (vnetDraft = blankVnet())}>Reset</button>
		</div>
		{#if vnetDraft.name && vnetError}<p class="text-xs text-amber-700 dark:text-amber-400" data-testid="net-vnet-hint">{vnetError}</p>{/if}
	</form>
</div>
