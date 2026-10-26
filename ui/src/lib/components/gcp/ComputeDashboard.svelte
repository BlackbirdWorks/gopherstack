<script lang="ts">
	import { onMount } from 'svelte';
	import { Server } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		ALL_ZONES,
		deleteAddress,
		deleteDisk,
		deleteInstance,
		deleteSnapshot,
		instanceAction,
		labelText,
		listAddresses,
		listDisks,
		listFirewalls,
		listInstances,
		listNetworks,
		listOperations,
		listRouters,
		listRoutes,
		listSnapshots,
		listSubnetworks,
		listZoneNames,
		readStoredComputeScope,
		regionOfZone,
		writeStoredComputeScope,
		type Address,
		type ComputeOperation,
		type Disk,
		type Firewall,
		type Instance,
		type InstanceAction,
		type Network,
		type Router,
		type Route,
		type Snapshot,
		type Subnetwork
	} from '#lib/gcpcompute.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'instances', label: 'Instances' },
		{ id: 'disks', label: 'Disks' },
		{ id: 'snapshots', label: 'Snapshots' },
		{ id: 'networks', label: 'Networks' },
		{ id: 'firewalls', label: 'Firewalls' },
		{ id: 'routes', label: 'Routes' },
		{ id: 'routers', label: 'Routers / NAT' },
		{ id: 'addresses', label: 'Addresses' },
		{ id: 'operations', label: 'Operations' }
	];

	const stored = readStoredComputeScope();
	let project = $state(stored.project);
	let zone = $state(stored.zone);
	let zones = $state<string[]>([]);
	let tab = $state('instances');
	let loading = $state(true);
	let error = $state('');

	let instances = $state<Instance[]>([]);
	let disks = $state<Disk[]>([]);
	let snapshots = $state<Snapshot[]>([]);
	let networks = $state<Network[]>([]);
	let subnets = $state<Subnetwork[]>([]);
	let firewalls = $state<Firewall[]>([]);
	let routes = $state<Route[]>([]);
	let routers = $state<Router[]>([]);
	let addresses = $state<Address[]>([]);
	let operations = $state<ComputeOperation[]>([]);

	const region = $derived(zone ? regionOfZone(zone) : '');
	const inZone = (z: string) => !zone || z === zone;
	const inRegion = (r: string) => !region || r === region || r === 'global';

	async function load(quiet = false) {
		if (!quiet) loading = true;
		try {
			switch (tab) {
				case 'instances':
					instances = await listInstances(project);
					break;
				case 'disks':
					disks = await listDisks(project);
					break;
				case 'snapshots':
					snapshots = await listSnapshots(project);
					break;
				case 'networks':
					[networks, subnets] = await Promise.all([listNetworks(project), listSubnetworks(project)]);
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
				case 'addresses':
					addresses = await listAddresses(project);
					break;
				default:
					operations = await listOperations(project);
			}
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadZones() {
		try {
			zones = await listZoneNames(project);
		} catch {
			zones = [];
		}
	}

	function applyScope(p: string) {
		project = p;
		writeStoredComputeScope(project, zone);
		void loadZones();
		void load();
	}

	function pickZone(event: Event) {
		zone = (event.currentTarget as HTMLSelectElement).value;
		writeStoredComputeScope(project, zone);
	}

	function selectTab(id: string) {
		tab = id;
		void load();
	}

	async function run(action: InstanceAction, i: Instance) {
		if (await attempt(() => instanceAction(project, i.zone, i.name, action), `${action[0].toUpperCase()}${action.slice(1)} requested for ${i.name}`, `Failed to ${action} ${i.name}`)) {
			await load(true);
			for (const ms of [1500, 5000, 12000]) setTimeout(() => void load(true), ms);
		}
	}

	async function removeWith(kind: string, name: string, fn: () => Promise<unknown>) {
		if (!(await confirmDestructive({ title: `Delete ${kind}`, message: `Delete ${kind} "${name}"?` }))) return;
		if (await attempt(fn, `Deleted ${name}`, `Failed to delete ${name}`)) await load(true);
	}

	const instById = $derived(new Map(instances.map((i) => [`${i.zone}/${i.name}`, i])));
	const diskById = $derived(new Map(disks.map((d) => [`${d.zone}/${d.name}`, d])));
	const addrById = $derived(new Map(addresses.map((a) => [`${a.scope}/${a.name}`, a])));

	const instanceRows = $derived(
		instances
			.filter((i) => inZone(i.zone))
			.map((i) => ({
				id: `${i.zone}/${i.name}`,
				cells: [i.name, i.zone, i.status, i.machineType, i.internalIps.join(', '), i.externalIps.join(', '), i.disks.map((d) => d.name).join(', '), labelText(i.labels), i.metadataKeys.join(', ')],
				raw: i.raw
			}))
	);
	const diskRows = $derived(
		disks
			.filter((d) => inZone(d.zone))
			.map((d) => ({ id: `${d.zone}/${d.name}`, cells: [d.name, d.zone, d.status, `${d.sizeGb} GB`, d.type, d.users.join(', '), labelText(d.labels)], raw: d.raw }))
	);
	const snapshotRows = $derived(snapshots.map((s) => ({ id: s.name, cells: [s.name, s.status, s.sourceDisk, `${s.sizeGb} GB`, s.created], raw: s.raw })));
	const networkRows = $derived(networks.map((n) => ({ id: n.name, cells: [n.name, n.mode, n.routing, String(subnets.filter((s) => s.network === n.name).length), n.mtu ? String(n.mtu) : '', n.created], raw: n.raw })));
	const firewallRows = $derived(
		firewalls.map((f) => ({ id: f.name, cells: [f.name, f.network, f.direction, String(f.priority), f.rules.join(', '), f.ranges.join(', '), f.targets.join(', '), f.disabled ? 'disabled' : 'enabled'], raw: f.raw }))
	);
	const routeRows = $derived(routes.map((r) => ({ id: r.name, cells: [r.name, r.network, r.destRange, String(r.priority), r.nextHop, r.tags.join(', ')], raw: r.raw })));
	const routerRows = $derived(
		routers
			.filter((r) => inRegion(r.region))
			.map((r) => ({ id: `${r.region}/${r.name}`, cells: [r.name, r.region, r.network, r.nats.join('; '), r.asn ? String(r.asn) : ''], raw: r.raw }))
	);
	const addressRows = $derived(
		addresses
			.filter((a) => inRegion(a.scope))
			.map((a) => ({ id: `${a.scope}/${a.name}`, cells: [a.name, a.scope, a.address, a.type, a.status, a.purpose, a.users.join(', ')], raw: a.raw }))
	);
	const operationRows = $derived(
		operations
			.filter((o) => !zone || o.scope === zone || o.scope === region || o.scope === 'global')
			.map((o) => ({ id: o.name, cells: [o.type, o.target, o.scope, o.progress === 100 ? o.status : `${o.status} ${o.progress}%`, o.started, o.ended, o.error], raw: o.raw }))
	);

	onMount(() => {
		void loadZones();
		void load();
	});
</script>

<div class="space-y-6 pb-20" data-testid="compute-dashboard">
	<PageHeader icon={Server} title="Compute Engine" description="Google Compute Engine: instances, disks, snapshots, networks, addresses and operations" color="blue" onRefresh={() => load()} />
	<div class="flex flex-wrap items-center gap-4">
		<GcpScopeBar prefix="compute" {project} hint="Resources are listed per project." onApply={applyScope} />
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Zone
			<select class="{GCP_INPUT} w-48 !py-1 font-mono text-xs" value={zone} onchange={pickZone} data-testid="compute-zone">
				<option value={ALL_ZONES}>All zones</option>
				{#each zones as z (z)}<option value={z}>{z}</option>{/each}
			</select>
		</label>
	</div>
	<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />

	{#if tab === 'instances'}
		<GcpResourceTable testId="compute-instance" noun="instances" columns={['Name', 'Zone', 'Status', 'Machine type', 'Internal IP', 'External IP', 'Disks', 'Labels', 'Metadata keys']} rows={instanceRows} {loading} {error} onRetry={() => load()} onDelete={(r) => { const i = instById.get(r.id); if (i) void removeWith('instance', i.name, () => deleteInstance(project, i.zone, i.name)); }}>
			{#snippet actions(row: ResourceRow)}
				{@const i = instById.get(row.id)}
				{#if i}
					<button class={GCP_MINI} disabled={i.status !== 'TERMINATED'} onclick={() => run('start', i)} data-testid="compute-instance-start">Start</button>
					<button class={GCP_MINI} disabled={i.status !== 'RUNNING'} onclick={() => run('stop', i)} data-testid="compute-instance-stop">Stop</button>
					<button class={GCP_MINI} disabled={i.status !== 'RUNNING'} onclick={() => run('reset', i)} data-testid="compute-instance-reset">Reset</button>
				{/if}
			{/snippet}
		</GcpResourceTable>
	{:else if tab === 'disks'}
		<GcpResourceTable testId="compute-disk" noun="disks" columns={['Name', 'Zone', 'Status', 'Size', 'Type', 'In use by', 'Labels']} rows={diskRows} {loading} {error} onRetry={() => load()} onDelete={(r) => { const d = diskById.get(r.id); if (d) void removeWith('disk', d.name, () => deleteDisk(project, d.zone, d.name)); }} />
	{:else if tab === 'snapshots'}
		<GcpResourceTable testId="compute-snapshot" noun="snapshots" columns={['Name', 'Status', 'Source disk', 'Size', 'Created']} rows={snapshotRows} {loading} {error} onRetry={() => load()} onDelete={(r) => void removeWith('snapshot', r.id, () => deleteSnapshot(project, r.id))} />
	{:else if tab === 'networks'}
		<GcpResourceTable testId="compute-network" noun="networks" columns={['Name', 'Mode', 'Routing', 'Subnetworks', 'MTU', 'Created']} rows={networkRows} {loading} {error} onRetry={() => load()}>
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
	{:else if tab === 'firewalls'}
		<GcpResourceTable testId="compute-firewall" noun="firewall rules" columns={['Name', 'Network', 'Direction', 'Priority', 'Rules', 'Ranges', 'Target tags', 'State']} rows={firewallRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'routes'}
		<GcpResourceTable testId="compute-route" noun="routes" columns={['Name', 'Network', 'Destination', 'Priority', 'Next hop', 'Tags']} rows={routeRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'routers'}
		<GcpResourceTable testId="compute-router" noun="routers" columns={['Name', 'Region', 'Network', 'NAT gateways', 'BGP ASN']} rows={routerRows} {loading} {error} onRetry={() => load()} />
	{:else if tab === 'addresses'}
		<GcpResourceTable testId="compute-address" noun="addresses" columns={['Name', 'Region', 'Address', 'Type', 'Status', 'Purpose', 'In use by']} rows={addressRows} {loading} {error} onRetry={() => load()} onDelete={(r) => { const a = addrById.get(r.id); if (a) void removeWith('address', a.name, () => deleteAddress(project, a.scope, a.name)); }} />
	{:else}
		<GcpResourceTable testId="compute-operation" noun="operations" columns={['Operation', 'Target', 'Scope', 'Status', 'Started', 'Finished', 'Error']} rows={operationRows} {loading} {error} onRetry={() => load()} />
	{/if}
</div>
