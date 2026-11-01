<script lang="ts">
	import { untrack } from 'svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { confirmedAttempt } from '#lib/gcp-action.ts';
	import { deleteAddress, inRegionScope, listAddresses, listFirewalls, listNetworks, listRouters, listRoutes, listSubnetworks, type Address, type Firewall, type Network, type Route, type Router, type Subnetwork } from '#lib/gcpcompute.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	type Props = { project: string; region: string; refresh: number };
	let { project, region, refresh }: Props = $props();

	const TABS = [
		{ id: 'vpcs', label: 'VPC networks' },
		{ id: 'subnets', label: 'Subnets' },
		{ id: 'firewalls', label: 'Firewall rules' },
		{ id: 'routes', label: 'Routes' },
		{ id: 'peering', label: 'Peering' },
		{ id: 'routers', label: 'Routers / NAT' },
		{ id: 'addresses', label: 'Addresses' }
	];
	let tab = $state('vpcs');
	let loading = $state(true);
	let error = $state('');
	let networks = $state<Network[]>([]);
	let subnets = $state<Subnetwork[]>([]);
	let firewalls = $state<Firewall[]>([]);
	let routes = $state<Route[]>([]);
	let routers = $state<Router[]>([]);
	let addresses = $state<Address[]>([]);

	async function load(quiet = false) {
		if (!quiet) loading = true;
		try {
			switch (tab) {
				case 'vpcs':
				case 'peering':
					[networks, subnets] = await Promise.all([listNetworks(project), listSubnetworks(project)]);
					break;
				case 'subnets':
					subnets = await listSubnetworks(project);
					break;
				case 'firewalls':
					firewalls = await listFirewalls(project);
					break;
				case 'routes':
					routes = await listRoutes(project);
					break;
				case 'routers':
					routers = await listRouters(project);
					break;
				default:
					addresses = await listAddresses(project);
			}
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	$effect(() => {
		void project;
		void refresh;
		untrack(() => void load());
	});

	function selectTab(id: string) {
		tab = id;
		void load();
	}

	const addrById = $derived(new Map(addresses.map((a) => [`${a.scope}/${a.name}`, a])));
	const networkRows = $derived(networks.map((n) => ({ id: n.name, cells: [n.name, n.mode, n.routing, String(subnets.filter((s) => s.network === n.name).length), n.mtu ? String(n.mtu) : '', String(n.peerings.length), n.created], raw: n.raw })));
	const subnetRows = $derived(subnets.filter((s) => inRegionScope(s.region, region)).map((s) => ({ id: `${s.region}/${s.name}`, cells: [s.name, s.region, s.network, s.cidr, s.gateway, s.privateGoogleAccess ? 'on' : 'off'], raw: s.raw })));
	const firewallRows = $derived(
		firewalls.map((f) => ({
			id: f.name,
			cells: [f.name, f.network, f.direction, String(f.priority), f.rules.join(', '), f.ranges.join(', '), [...f.sourceTags.map((t) => `from:${t}`), ...f.targets.map((t) => `to:${t}`)].join(', '), f.disabled ? 'disabled' : 'enabled'],
			raw: f.raw
		}))
	);
	const routeRows = $derived(routes.map((r) => ({ id: r.name, cells: [r.name, r.routeType, r.network, r.destRange, String(r.priority), r.nextHop, r.tags.join(', ')], raw: r.raw })));
	const peeringRows = $derived(
		networks.flatMap((n) =>
			n.peerings.map((p) => ({
				id: `${n.name}/${p.name}`,
				cells: [p.name, n.name, p.peer, p.state, p.detail, `${p.exportCustomRoutes ? 'export' : '—'} / ${p.importCustomRoutes ? 'import' : '—'}`],
				raw: n.raw
			}))
		)
	);
	const routerRows = $derived(routers.filter((r) => inRegionScope(r.region, region)).map((r) => ({ id: `${r.region}/${r.name}`, cells: [r.name, r.region, r.network, r.nats.join('; '), r.asn ? String(r.asn) : ''], raw: r.raw })));
	const addressRows = $derived(addresses.filter((a) => inRegionScope(a.scope, region)).map((a) => ({ id: `${a.scope}/${a.name}`, cells: [a.name, a.scope, a.address, a.type, a.status, a.purpose, a.users.join(', ')], raw: a.raw })));

	async function removeAddress(a: Address) {
		if (await confirmedAttempt({ title: 'Delete address', message: `Delete address "${a.name}"?` }, () => deleteAddress(project, a.scope, a.name), `Deleted ${a.name}`, `Failed to delete ${a.name}`)) await load(true);
	}
</script>

<div class="space-y-4">
	<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />
	{#if tab === 'vpcs'}
		<GcpResourceTable testId="compute-network" noun="networks" columns={['Name', 'Mode', 'Routing', 'Subnetworks', 'MTU', 'Peerings', 'Created']} rows={networkRows} {loading} {error} onRetry={() => load()}>
			{#snippet detail(row: ResourceRow)}
				{@const subs = subnets.filter((s) => s.network === row.id)}
				{#if subs.length === 0}
					<p class="text-gray-500 dark:text-gray-400" data-testid="compute-subnet-empty">No subnetworks</p>
				{:else}
					<table class="w-full text-xs" data-testid="compute-subnets">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="py-1 pr-3">Subnetwork</th><th class="py-1 pr-3">Region</th><th class="py-1 pr-3">Range</th><th class="py-1 pr-3">Gateway</th><th class="py-1">Private Google access</th></tr></thead>
						<tbody>
							{#each subs as s (`${s.region}/${s.name}`)}
								<tr data-testid="compute-subnet"><td class="py-1 pr-3 font-mono">{s.name}</td><td class="py-1 pr-3">{s.region}</td><td class="py-1 pr-3 font-mono">{s.cidr}</td><td class="py-1 pr-3 font-mono">{s.gateway || '—'}</td><td class="py-1">{s.privateGoogleAccess ? 'on' : 'off'}</td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{/snippet}
		</GcpResourceTable>
	{:else if tab === 'subnets'}
		<GcpResourceTable testId="compute-subnetwork" noun="subnetworks" columns={['Name', 'Region', 'Network', 'Range', 'Gateway', 'Private Google access']} rows={subnetRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'firewalls'}
		<GcpResourceTable testId="compute-firewall" noun="firewall rules" columns={['Name', 'Network', 'Direction', 'Priority', 'Allow / deny', 'Ranges', 'Tags', 'State']} rows={firewallRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'routes'}
		<GcpResourceTable testId="compute-route" noun="routes" columns={['Name', 'Type', 'Network', 'Destination', 'Priority', 'Next hop', 'Tags']} rows={routeRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'peering'}
		<GcpResourceTable testId="compute-peering" noun="VPC peerings" columns={['Name', 'Network', 'Peer network', 'State', 'Detail', 'Custom routes']} rows={peeringRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'routers'}
		<GcpResourceTable testId="compute-router" noun="routers" columns={['Name', 'Region', 'Network', 'NAT gateways', 'BGP ASN']} rows={routerRows} {loading} {error} onRetry={() => load()} />
	{:else}
		<GcpResourceTable testId="compute-address" noun="addresses" columns={['Name', 'Region', 'Address', 'Type', 'Status', 'Purpose', 'In use by']} rows={addressRows} {loading} {error} onRetry={() => load()} onDelete={(r) => { const a = addrById.get(r.id); if (a) void removeAddress(a); }} />
	{/if}
</div>
