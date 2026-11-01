<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw, Server } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import {
		allowedActions,
		listDisks,
		listNics,
		listNsgs,
		listPublicIps,
		listVms,
		listVnets,
		powerAction,
		powerLabel,
		vmAddresses,
		type Disk,
		type Nic,
		type PowerAction,
		type PublicIp,
		type SecurityGroup,
		type VirtualMachine,
		type VirtualNetwork
	} from '#lib/azurevm.ts';
	import { attempt, confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'vms', label: 'Virtual machines' },
		{ id: 'vnets', label: 'Virtual networks' },
		{ id: 'nsgs', label: 'Security groups' },
		{ id: 'disks', label: 'Disks' }
	];

	let subscription = $state('');
	let tab = $state('vms');
	let vms = $state<VirtualMachine[]>([]);
	let nics = $state<Nic[]>([]);
	let ips = $state<PublicIp[]>([]);
	let vnets = $state<VirtualNetwork[]>([]);
	let nsgs = $state<SecurityGroup[]>([]);
	let disks = $state<Disk[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');

	const current = $derived(vms.find((v) => v.id === selected));

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			[vms, nics, ips, vnets, nsgs, disks] = await Promise.all([
				listVms(subscription),
				listNics(subscription),
				listPublicIps(subscription),
				listVnets(subscription),
				listNsgs(subscription),
				listDisks(subscription)
			]);
			error = '';
			if (!vms.some((v) => v.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function act(vm: VirtualMachine, action: PowerAction) {
		const verbs: Record<PowerAction, string> = { start: 'Start', powerOff: 'Stop', deallocate: 'Deallocate', restart: 'Restart' };
		const verb = verbs[action];
		const run = () => powerAction(subscription, vm, action);
		const ok =
			action === 'start'
				? await attempt(run, `Start requested for ${vm.name}`, `Failed to start ${vm.name}`)
				: await confirmedAttempt({ title: `${verb} virtual machine`, message: `${verb} "${vm.name}"?`, confirmLabel: verb }, run, `${verb} requested for ${vm.name}`, `Failed to ${verb.toLowerCase()} ${vm.name}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const labels: Record<PowerAction, string> = { start: 'Start', powerOff: 'Stop', deallocate: 'Deallocate', restart: 'Restart' };
	const pill = (s: string) =>
		s === 'running' || s === 'Succeeded' || s === 'Attached' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'stopped' || s === 'deallocated' ? 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="azurevm-dashboard">
	<PageHeader icon={Server} title="Virtual Machines" description="Azure virtual machines with power state, addresses and disks, plus networks and security groups" color="sky" onRefresh={load} />

	{#if error && vms.length === 0 && vnets.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="azurevm-error">
			<p class="font-medium">Could not load virtual machines</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="sky" />

		{#if tab === 'vms'}
			{#if vms.length === 0}
				<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="azurevm-empty">No virtual machines. Create one through Resource Manager (Microsoft.Compute/virtualMachines).</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="azurevm-vms">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Resource group</th><th class={cell}>Size</th><th class={cell}>Power state</th><th class={cell}>Private IP</th><th class={cell}>Public IP</th><th class={cell}>Disks</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each vms as vm (vm.id)}
								{@const addr = vmAddresses(vm, nics, ips)}
								<tr class={selected === vm.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="azurevm-vm">
									<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => (selected = selected === vm.id ? '' : vm.id)} aria-expanded={selected === vm.id} data-testid="azurevm-select">{vm.name}</button></td>
									<td class="{cell} font-mono text-xs">{vm.resourceGroup}</td>
									<td class="{cell} font-mono text-xs" data-testid="azurevm-size">{vm.size || '—'}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(vm.power)}" data-testid="azurevm-power">{powerLabel(vm.power)}</span></td>
									<td class="{cell} font-mono text-xs" data-testid="azurevm-private">{addr.privateIps.join(', ') || '—'}</td>
									<td class="{cell} font-mono text-xs" data-testid="azurevm-public">{addr.publicIps.join(', ') || '—'}</td>
									<td class="{cell} text-xs" data-testid="azurevm-disks">{vm.disks.length}</td>
									<td class="{cell} text-right whitespace-nowrap space-x-1">
										{#each allowedActions(vm.power) as a (a)}
											<button class={GCP_MINI} onclick={() => act(vm, a)} data-testid="azurevm-{a}">{labels[a]}</button>
										{/each}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>

				{#if current}
					<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="azurevm-detail">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name}</h2>
						<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
							<dt class="text-gray-500">Location</dt><dd>{current.location}{current.zones.length ? ` (zone ${current.zones.join(', ')})` : ''}</dd>
							<dt class="text-gray-500">OS</dt><dd>{current.osType || '—'}{current.image ? ` · ${current.image}` : ''}</dd>
							<dt class="text-gray-500">Computer name</dt><dd class="font-mono">{current.computerName || '—'}</dd>
							<dt class="text-gray-500">Admin user</dt><dd class="font-mono">{current.adminUsername || '—'}</dd>
						</dl>
						<table class="w-full text-xs" data-testid="azurevm-vm-disks">
							<thead class="text-left text-gray-500"><tr><th class="py-1 pr-4">Disk</th><th class="py-1 pr-4">Kind</th><th class="py-1 pr-4">Size</th><th class="py-1">Type</th></tr></thead>
							<tbody>
								{#each current.disks as d (d.name)}
									<tr data-testid="azurevm-vm-disk"><td class="py-1 pr-4 font-mono">{d.name}</td><td class="py-1 pr-4">{d.kind}</td><td class="py-1 pr-4">{d.sizeGb ? `${d.sizeGb} GB` : '—'}</td><td class="py-1">{d.type || '—'}</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{/if}
		{:else if tab === 'vnets'}
			{#if vnets.length === 0}
				<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="azurevm-vnets-empty">No virtual networks</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="azurevm-vnets">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Network</th><th class={cell}>Resource group</th><th class={cell}>Location</th><th class={cell}>Address space</th><th class={cell}>Subnets</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each vnets as v (v.id)}
								<tr data-testid="azurevm-vnet">
									<td class="{cell} font-mono text-xs">{v.name}</td>
									<td class="{cell} font-mono text-xs">{v.resourceGroup}</td>
									<td class="{cell} text-xs">{v.location}</td>
									<td class="{cell} font-mono text-xs">{v.prefixes.join(', ') || '—'}</td>
									<td class="{cell} text-xs">
										{#each v.subnets as s (s.name)}<div class="font-mono" data-testid="azurevm-subnet">{s.name} {s.prefix}{s.nsg ? ` (nsg ${s.nsg})` : ''}</div>{:else}—{/each}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if tab === 'nsgs'}
			{#if nsgs.length === 0}
				<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="azurevm-nsgs-empty">No network security groups</p>
			{:else}
				{#each nsgs as g (g.id)}
					<section class="space-y-2" data-testid="azurevm-nsg">
						<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">{g.name} <span class="font-normal text-gray-400">({g.resourceGroup}, {g.location})</span></h2>
						{#if g.rules.length === 0}
							<p class="text-sm text-gray-500">No custom rules</p>
						{:else}
							<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
								<table class="w-full text-xs" data-testid="azurevm-nsg-rules">
									<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Priority</th><th class={cell}>Name</th><th class={cell}>Direction</th><th class={cell}>Access</th><th class={cell}>Protocol</th><th class={cell}>Source</th><th class={cell}>Destination</th><th class={cell}>Ports</th></tr></thead>
									<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
										{#each g.rules as r (r.name)}
											<tr data-testid="azurevm-nsg-rule"><td class={cell}>{r.priority}</td><td class="{cell} font-mono">{r.name}</td><td class={cell}>{r.direction}</td><td class={cell}>{r.access}</td><td class={cell}>{r.protocol}</td><td class="{cell} font-mono">{r.source}</td><td class="{cell} font-mono">{r.destination}</td><td class="{cell} font-mono">{r.ports}</td></tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</section>
				{/each}
			{/if}
		{:else if disks.length === 0}
			<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="azurevm-disks-empty">No managed disks</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="azurevm-disk-list">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Disk</th><th class={cell}>Resource group</th><th class={cell}>Size</th><th class={cell}>SKU</th><th class={cell}>State</th><th class={cell}>Attached to</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each disks as d (d.id)}
							<tr data-testid="azurevm-disk"><td class="{cell} font-mono text-xs">{d.name}</td><td class="{cell} font-mono text-xs">{d.resourceGroup}</td><td class="{cell} text-xs">{d.sizeGb} GB</td><td class="{cell} text-xs">{d.sku || '—'}</td><td class="{cell} text-xs">{d.state || '—'}</td><td class="{cell} font-mono text-xs">{d.attachedTo || '—'}</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
