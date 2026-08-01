<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDirectConnectClient } from '$lib/aws-client';
	import {
		DescribeConnectionsCommand,
		DescribeVirtualInterfacesCommand,
		DescribeDirectConnectGatewaysCommand,
		DescribeLagsCommand,
		CreateConnectionCommand,
		UpdateConnectionCommand,
		DeleteConnectionCommand,
		CreatePrivateVirtualInterfaceCommand,
		CreateTransitVirtualInterfaceCommand,
		UpdateVirtualInterfaceAttributesCommand,
		DeleteVirtualInterfaceCommand,
		CreateDirectConnectGatewayCommand,
		UpdateDirectConnectGatewayCommand,
		DeleteDirectConnectGatewayCommand,
		CreateLagCommand,
		UpdateLagCommand,
		DeleteLagCommand,
		type Connection,
		type VirtualInterface,
		type DirectConnectGateway,
		type Lag
	} from '@aws-sdk/client-direct-connect';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { Cable, Plus, Trash2, Eye, Pencil, Router, Network, Link2 } from 'lucide-svelte';

	// Direct Connect's real listable resource families: connections, virtual
	// interfaces, Direct Connect gateways, and link aggregation groups (LAGs).
	// Interconnects (DescribeInterconnects) exist on the wire too but are a
	// Direct Connect Partner concept, not something an ordinary account can
	// create -- left out of scope here (reported, not built). Public virtual
	// interface creation needs a routeFilterPrefixes list editor beyond this
	// pass's scope -- only private and transit VIF creation are offered
	// (reported below).
	const client = regionalClient(getDirectConnectClient);

	type TabId = 'connections' | 'vifs' | 'gateways' | 'lags';

	const tabs: TabDef[] = [
		{ id: 'connections', label: 'Connections' },
		{ id: 'vifs', label: 'Virtual Interfaces' },
		{ id: 'gateways', label: 'Gateways' },
		{ id: 'lags', label: 'LAGs' }
	];

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	function stateClass(state: string | undefined): string {
		if (state === 'available') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'down' || state === 'deleting') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('connections');
	let searchQuery = $state('');

	let connections = $state<Connection[]>([]);
	let vifs = $state<VirtualInterface[]>([]);
	let gateways = $state<DirectConnectGateway[]>([]);
	let lags = $state<Lag[]>([]);

	async function fetchConnections(): Promise<void> {
		const resp = await client().send(new DescribeConnectionsCommand({}));
		connections = resp.connections ?? [];
	}
	async function fetchVifs(): Promise<void> {
		const resp = await client().send(new DescribeVirtualInterfacesCommand({}));
		vifs = resp.virtualInterfaces ?? [];
	}
	async function fetchGateways(): Promise<void> {
		const resp = await client().send(new DescribeDirectConnectGatewaysCommand({}));
		gateways = resp.directConnectGateways ?? [];
	}
	async function fetchLags(): Promise<void> {
		const resp = await client().send(new DescribeLagsCommand({}));
		lags = resp.lags ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		connections: () => fetchConnections().catch(rethrowDescribed),
		vifs: () => fetchVifs().catch(rethrowDescribed),
		gateways: () => fetchGateways().catch(rethrowDescribed),
		lags: () => fetchLags().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		tabLoader.refresh('connections');
	});

	const filteredConnections = $derived(
		connections.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(c.connectionName ?? '').toLowerCase().includes(q) ||
				(c.connectionId ?? '').toLowerCase().includes(q) ||
				(c.location ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredVifs = $derived(
		vifs.filter((v) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(v.virtualInterfaceName ?? '').toLowerCase().includes(q) ||
				(v.virtualInterfaceId ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredGateways = $derived(
		gateways.filter((g) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(g.directConnectGatewayName ?? '').toLowerCase().includes(q) ||
				(g.directConnectGatewayId ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredLags = $derived(
		lags.filter((l) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (l.lagName ?? '').toLowerCase().includes(q) || (l.lagId ?? '').toLowerCase().includes(q);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Connections ---

	let connCreateModal = $state<Modal | null>(null);
	let connCreating = $state(false);
	let connCreateError = $state<string | null>(null);
	let newConnName = $state('');
	let newConnLocation = $state('');
	let newConnBandwidth = $state('1Gbps');

	function openConnCreateModal(): void {
		connCreateError = null;
		newConnName = '';
		newConnLocation = '';
		newConnBandwidth = '1Gbps';
		connCreateModal?.open();
	}

	async function submitCreateConn(): Promise<void> {
		if (!newConnName || !newConnLocation) {
			connCreateError = 'Name and location are required.';
			return;
		}
		connCreating = true;
		connCreateError = null;
		try {
			await client().send(
				new CreateConnectionCommand({
					connectionName: newConnName,
					location: newConnLocation,
					bandwidth: newConnBandwidth
				})
			);
			toast.success('Connection created');
			connCreateModal?.close();
			await tabLoader.refresh('connections');
		} catch (e) {
			const msg = describeError(e);
			connCreateError = msg;
			toast.error(msg);
		} finally {
			connCreating = false;
		}
	}

	let connEditModal = $state<Modal | null>(null);
	let connEditing = $state(false);
	let connEditError = $state<string | null>(null);
	let editConnId = $state('');
	let editConnName = $state('');

	function openConnEditModal(c: Connection): void {
		connEditError = null;
		editConnId = c.connectionId ?? '';
		editConnName = c.connectionName ?? '';
		connEditModal?.open();
	}

	async function submitEditConn(): Promise<void> {
		if (!editConnId) return;
		connEditing = true;
		connEditError = null;
		try {
			await client().send(
				new UpdateConnectionCommand({ connectionId: editConnId, connectionName: editConnName })
			);
			toast.success('Connection updated');
			connEditModal?.close();
			await tabLoader.refresh('connections');
		} catch (e) {
			const msg = describeError(e);
			connEditError = msg;
			toast.error(msg);
		} finally {
			connEditing = false;
		}
	}

	async function deleteConnection(c: Connection): Promise<void> {
		if (!c.connectionId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete connection',
			message: `Delete connection ${c.connectionName ?? c.connectionId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteConnectionCommand({ connectionId: c.connectionId }));
			toast.success('Connection deleted');
			await tabLoader.refresh('connections');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Virtual interfaces ---

	let vifCreateModal = $state<Modal | null>(null);
	let vifCreating = $state(false);
	let vifCreateError = $state<string | null>(null);
	let newVifType = $state<'private' | 'transit'>('private');
	let newVifConnectionId = $state('');
	let newVifName = $state('');
	let newVifVlan = $state(100);
	let newVifAsn = $state(65000);
	let newVifGatewayId = $state('');

	function openVifCreateModal(): void {
		vifCreateError = null;
		newVifType = 'private';
		newVifConnectionId = connections[0]?.connectionId ?? '';
		newVifName = '';
		newVifVlan = 100;
		newVifAsn = 65000;
		newVifGatewayId = '';
		vifCreateModal?.open();
	}

	async function submitCreateVif(): Promise<void> {
		if (!newVifConnectionId || !newVifName) {
			vifCreateError = 'Connection and name are required.';
			return;
		}
		vifCreating = true;
		vifCreateError = null;
		try {
			if (newVifType === 'private') {
				await client().send(
					new CreatePrivateVirtualInterfaceCommand({
						connectionId: newVifConnectionId,
						newPrivateVirtualInterface: {
							virtualInterfaceName: newVifName,
							vlan: newVifVlan,
							asn: newVifAsn,
							directConnectGatewayId: newVifGatewayId || undefined
						}
					})
				);
			} else {
				await client().send(
					new CreateTransitVirtualInterfaceCommand({
						connectionId: newVifConnectionId,
						newTransitVirtualInterface: {
							virtualInterfaceName: newVifName,
							vlan: newVifVlan,
							asn: newVifAsn,
							directConnectGatewayId: newVifGatewayId || undefined
						}
					})
				);
			}
			toast.success('Virtual interface created');
			vifCreateModal?.close();
			await tabLoader.refresh('vifs');
		} catch (e) {
			const msg = describeError(e);
			vifCreateError = msg;
			toast.error(msg);
		} finally {
			vifCreating = false;
		}
	}

	let vifEditModal = $state<Modal | null>(null);
	let vifEditing = $state(false);
	let vifEditError = $state<string | null>(null);
	let editVifId = $state('');
	let editVifName = $state('');
	let editVifMtu = $state(1500);

	function openVifEditModal(v: VirtualInterface): void {
		vifEditError = null;
		editVifId = v.virtualInterfaceId ?? '';
		editVifName = v.virtualInterfaceName ?? '';
		editVifMtu = v.mtu ?? 1500;
		vifEditModal?.open();
	}

	async function submitEditVif(): Promise<void> {
		if (!editVifId) return;
		vifEditing = true;
		vifEditError = null;
		try {
			await client().send(
				new UpdateVirtualInterfaceAttributesCommand({
					virtualInterfaceId: editVifId,
					virtualInterfaceName: editVifName,
					mtu: editVifMtu
				})
			);
			toast.success('Virtual interface updated');
			vifEditModal?.close();
			await tabLoader.refresh('vifs');
		} catch (e) {
			const msg = describeError(e);
			vifEditError = msg;
			toast.error(msg);
		} finally {
			vifEditing = false;
		}
	}

	async function deleteVif(v: VirtualInterface): Promise<void> {
		if (!v.virtualInterfaceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete virtual interface',
			message: `Delete virtual interface ${v.virtualInterfaceName ?? v.virtualInterfaceId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteVirtualInterfaceCommand({ virtualInterfaceId: v.virtualInterfaceId }));
			toast.success('Virtual interface deleted');
			await tabLoader.refresh('vifs');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Gateways ---

	let gwCreateModal = $state<Modal | null>(null);
	let gwCreating = $state(false);
	let gwCreateError = $state<string | null>(null);
	let newGwName = $state('');
	let newGwAsn = $state(64512);

	function openGwCreateModal(): void {
		gwCreateError = null;
		newGwName = '';
		newGwAsn = 64512;
		gwCreateModal?.open();
	}

	async function submitCreateGw(): Promise<void> {
		if (!newGwName) {
			gwCreateError = 'Name is required.';
			return;
		}
		gwCreating = true;
		gwCreateError = null;
		try {
			await client().send(
				new CreateDirectConnectGatewayCommand({
					directConnectGatewayName: newGwName,
					amazonSideAsn: newGwAsn
				})
			);
			toast.success('Direct Connect gateway created');
			gwCreateModal?.close();
			await tabLoader.refresh('gateways');
		} catch (e) {
			const msg = describeError(e);
			gwCreateError = msg;
			toast.error(msg);
		} finally {
			gwCreating = false;
		}
	}

	let gwEditModal = $state<Modal | null>(null);
	let gwEditing = $state(false);
	let gwEditError = $state<string | null>(null);
	let editGwId = $state('');
	let editGwName = $state('');

	function openGwEditModal(g: DirectConnectGateway): void {
		gwEditError = null;
		editGwId = g.directConnectGatewayId ?? '';
		editGwName = g.directConnectGatewayName ?? '';
		gwEditModal?.open();
	}

	async function submitEditGw(): Promise<void> {
		if (!editGwId || !editGwName) return;
		gwEditing = true;
		gwEditError = null;
		try {
			await client().send(
				new UpdateDirectConnectGatewayCommand({
					directConnectGatewayId: editGwId,
					newDirectConnectGatewayName: editGwName
				})
			);
			toast.success('Direct Connect gateway updated');
			gwEditModal?.close();
			await tabLoader.refresh('gateways');
		} catch (e) {
			const msg = describeError(e);
			gwEditError = msg;
			toast.error(msg);
		} finally {
			gwEditing = false;
		}
	}

	async function deleteGateway(g: DirectConnectGateway): Promise<void> {
		if (!g.directConnectGatewayId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Direct Connect gateway',
			message: `Delete gateway ${g.directConnectGatewayName ?? g.directConnectGatewayId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDirectConnectGatewayCommand({ directConnectGatewayId: g.directConnectGatewayId }));
			toast.success('Direct Connect gateway deleted');
			await tabLoader.refresh('gateways');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- LAGs ---

	let lagCreateModal = $state<Modal | null>(null);
	let lagCreating = $state(false);
	let lagCreateError = $state<string | null>(null);
	let newLagName = $state('');
	let newLagLocation = $state('');
	let newLagBandwidth = $state('1Gbps');
	let newLagConnections = $state(1);

	function openLagCreateModal(): void {
		lagCreateError = null;
		newLagName = '';
		newLagLocation = '';
		newLagBandwidth = '1Gbps';
		newLagConnections = 1;
		lagCreateModal?.open();
	}

	async function submitCreateLag(): Promise<void> {
		if (!newLagName || !newLagLocation) {
			lagCreateError = 'Name and location are required.';
			return;
		}
		lagCreating = true;
		lagCreateError = null;
		try {
			await client().send(
				new CreateLagCommand({
					lagName: newLagName,
					location: newLagLocation,
					connectionsBandwidth: newLagBandwidth,
					numberOfConnections: newLagConnections
				})
			);
			toast.success('LAG created');
			lagCreateModal?.close();
			await tabLoader.refresh('lags');
		} catch (e) {
			const msg = describeError(e);
			lagCreateError = msg;
			toast.error(msg);
		} finally {
			lagCreating = false;
		}
	}

	let lagEditModal = $state<Modal | null>(null);
	let lagEditing = $state(false);
	let lagEditError = $state<string | null>(null);
	let editLagId = $state('');
	let editLagName = $state('');
	let editLagMinLinks = $state(0);

	function openLagEditModal(l: Lag): void {
		lagEditError = null;
		editLagId = l.lagId ?? '';
		editLagName = l.lagName ?? '';
		editLagMinLinks = l.minimumLinks ?? 0;
		lagEditModal?.open();
	}

	async function submitEditLag(): Promise<void> {
		if (!editLagId) return;
		lagEditing = true;
		lagEditError = null;
		try {
			await client().send(
				new UpdateLagCommand({ lagId: editLagId, lagName: editLagName, minimumLinks: editLagMinLinks })
			);
			toast.success('LAG updated');
			lagEditModal?.close();
			await tabLoader.refresh('lags');
		} catch (e) {
			const msg = describeError(e);
			lagEditError = msg;
			toast.error(msg);
		} finally {
			lagEditing = false;
		}
	}

	async function deleteLag(l: Lag): Promise<void> {
		if (!l.lagId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete LAG',
			message: `Delete LAG ${l.lagName ?? l.lagId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLagCommand({ lagId: l.lagId }));
			toast.success('LAG deleted');
			await tabLoader.refresh('lags');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Cable}
		title="AWS Direct Connect"
		description="Dedicated network connections to AWS"
		onRefresh={handleRefresh}
		color="teal"
	>
		{#snippet actions()}
			{#if activeTab === 'connections'}
				<button
					onclick={openConnCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create connection
				</button>
			{:else if activeTab === 'vifs'}
				<button
					onclick={openVifCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create virtual interface
				</button>
			{:else if activeTab === 'gateways'}
				<button
					onclick={openGwCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create gateway
				</button>
			{:else if activeTab === 'lags'}
				<button
					onclick={openLagCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-teal-600 text-white hover:bg-teal-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create LAG
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="teal" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'connections'}
				{#snippet connStateCell(c: Connection)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(c.connectionState)}">{c.connectionState ?? '—'}</span>
				{/snippet}
				{#snippet connActionsCell(c: Connection)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openConnEditModal(c)} title="Edit" aria-label="Edit connection {c.connectionName}" class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteConnection(c)} title="Delete" aria-label="Delete connection {c.connectionName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const connColumns = defineColumns<Connection>([
					{ key: 'connectionName', label: 'Name' },
					{ key: 'bandwidth', label: 'Bandwidth' },
					{ key: 'location', label: 'Location' },
					{ key: 'connectionState', label: 'State', render: connStateCell },
					{ key: 'actions', label: '', render: connActionsCell }
				])}
				<DataTable
					rows={filteredConnections}
					rowKey={(c) => c.connectionId ?? ''}
					columns={connColumns}
					loading={tabLoader.isLoading('connections')}
					emptyMessage="No connections found"
				/>
			{:else if activeTab === 'vifs'}
				{#snippet vifStateCell(v: VirtualInterface)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(v.virtualInterfaceState)}">{v.virtualInterfaceState ?? '—'}</span>
				{/snippet}
				{#snippet vifActionsCell(v: VirtualInterface)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openVifEditModal(v)} title="Edit" aria-label="Edit virtual interface {v.virtualInterfaceName}" class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteVif(v)} title="Delete" aria-label="Delete virtual interface {v.virtualInterfaceName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const vifColumns = defineColumns<VirtualInterface>([
					{ key: 'virtualInterfaceName', label: 'Name' },
					{ key: 'virtualInterfaceType', label: 'Type' },
					{ key: 'vlan', label: 'VLAN' },
					{ key: 'connectionId', label: 'Connection' },
					{ key: 'virtualInterfaceState', label: 'State', render: vifStateCell },
					{ key: 'actions', label: '', render: vifActionsCell }
				])}
				<DataTable
					rows={filteredVifs}
					rowKey={(v) => v.virtualInterfaceId ?? ''}
					columns={vifColumns}
					loading={tabLoader.isLoading('vifs')}
					emptyMessage="No virtual interfaces found"
				/>
			{:else if activeTab === 'gateways'}
				{#snippet gwActionsCell(g: DirectConnectGateway)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openGwEditModal(g)} title="Edit" aria-label="Edit gateway {g.directConnectGatewayName}" class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteGateway(g)} title="Delete" aria-label="Delete gateway {g.directConnectGatewayName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const gwColumns = defineColumns<DirectConnectGateway>([
					{ key: 'directConnectGatewayName', label: 'Name' },
					{ key: 'directConnectGatewayId', label: 'ID' },
					{ key: 'amazonSideAsn', label: 'ASN' },
					{ key: 'directConnectGatewayState', label: 'State' },
					{ key: 'actions', label: '', render: gwActionsCell }
				])}
				<DataTable
					rows={filteredGateways}
					rowKey={(g) => g.directConnectGatewayId ?? ''}
					columns={gwColumns}
					loading={tabLoader.isLoading('gateways')}
					emptyMessage="No Direct Connect gateways found"
				/>
			{:else if activeTab === 'lags'}
				{#snippet lagStateCell(l: Lag)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(l.lagState)}">{l.lagState ?? '—'}</span>
				{/snippet}
				{#snippet lagActionsCell(l: Lag)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openLagEditModal(l)} title="Edit" aria-label="Edit LAG {l.lagName}" class="text-gray-400 hover:text-teal-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteLag(l)} title="Delete" aria-label="Delete LAG {l.lagName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const lagColumns = defineColumns<Lag>([
					{ key: 'lagName', label: 'Name' },
					{ key: 'connectionsBandwidth', label: 'Bandwidth' },
					{ key: 'numberOfConnections', label: 'Connections' },
					{ key: 'lagState', label: 'State', render: lagStateCell },
					{ key: 'actions', label: '', render: lagActionsCell }
				])}
				<DataTable
					rows={filteredLags}
					rowKey={(l) => l.lagId ?? ''}
					columns={lagColumns}
					loading={tabLoader.isLoading('lags')}
					emptyMessage="No LAGs found"
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={connCreateModal} title="Create Connection">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-conn-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-conn-new-name" bind:value={newConnName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dc-conn-new-location" class="text-sm text-slate-600 dark:text-slate-300">Location</label>
				<input id="dc-conn-new-location" bind:value={newConnLocation} placeholder="EqDC2" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dc-conn-new-bandwidth" class="text-sm text-slate-600 dark:text-slate-300">Bandwidth</label>
				<select id="dc-conn-new-bandwidth" bind:value={newConnBandwidth} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="50Mbps">50Mbps</option>
					<option value="100Mbps">100Mbps</option>
					<option value="500Mbps">500Mbps</option>
					<option value="1Gbps">1Gbps</option>
					<option value="10Gbps">10Gbps</option>
					<option value="100Gbps">100Gbps</option>
				</select>
			</div>
			{#if connCreateError}<p class="text-sm text-red-600 dark:text-red-400">{connCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => connCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateConn} disabled={connCreating} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{connCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={connEditModal} title="Edit Connection">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-conn-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-conn-edit-name" bind:value={editConnName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if connEditError}<p class="text-sm text-red-600 dark:text-red-400">{connEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => connEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditConn} disabled={connEditing} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{connEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={vifCreateModal} title="Create Virtual Interface">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-vif-new-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select id="dc-vif-new-type" bind:value={newVifType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="private">Private</option>
					<option value="transit">Transit</option>
				</select>
			</div>
			<div>
				<label for="dc-vif-new-conn" class="text-sm text-slate-600 dark:text-slate-300">Connection ID</label>
				<input id="dc-vif-new-conn" bind:value={newVifConnectionId} placeholder="dxcon-xxxxxxxx" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dc-vif-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-vif-new-name" bind:value={newVifName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="dc-vif-new-vlan" class="text-sm text-slate-600 dark:text-slate-300">VLAN</label>
					<input id="dc-vif-new-vlan" type="number" bind:value={newVifVlan} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="dc-vif-new-asn" class="text-sm text-slate-600 dark:text-slate-300">ASN</label>
					<input id="dc-vif-new-asn" type="number" bind:value={newVifAsn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div>
				<label for="dc-vif-new-gw" class="text-sm text-slate-600 dark:text-slate-300">Direct Connect gateway ID</label>
				<input id="dc-vif-new-gw" bind:value={newVifGatewayId} placeholder="optional" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if vifCreateError}<p class="text-sm text-red-600 dark:text-red-400">{vifCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => vifCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateVif} disabled={vifCreating} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{vifCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={vifEditModal} title="Edit Virtual Interface">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-vif-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-vif-edit-name" bind:value={editVifName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dc-vif-edit-mtu" class="text-sm text-slate-600 dark:text-slate-300">MTU</label>
				<select id="dc-vif-edit-mtu" bind:value={editVifMtu} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value={1500}>1500</option>
					<option value={8500}>8500 (jumbo)</option>
				</select>
			</div>
			{#if vifEditError}<p class="text-sm text-red-600 dark:text-red-400">{vifEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => vifEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditVif} disabled={vifEditing} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{vifEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={gwCreateModal} title="Create Direct Connect Gateway">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-gw-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-gw-new-name" bind:value={newGwName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dc-gw-new-asn" class="text-sm text-slate-600 dark:text-slate-300">Amazon side ASN</label>
				<input id="dc-gw-new-asn" type="number" bind:value={newGwAsn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if gwCreateError}<p class="text-sm text-red-600 dark:text-red-400">{gwCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => gwCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateGw} disabled={gwCreating} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{gwCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={gwEditModal} title="Edit Direct Connect Gateway">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-gw-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-gw-edit-name" bind:value={editGwName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if gwEditError}<p class="text-sm text-red-600 dark:text-red-400">{gwEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => gwEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditGw} disabled={gwEditing} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{gwEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={lagCreateModal} title="Create LAG">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-lag-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-lag-new-name" bind:value={newLagName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dc-lag-new-location" class="text-sm text-slate-600 dark:text-slate-300">Location</label>
				<input id="dc-lag-new-location" bind:value={newLagLocation} placeholder="EqDC2" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="dc-lag-new-bandwidth" class="text-sm text-slate-600 dark:text-slate-300">Bandwidth</label>
					<select id="dc-lag-new-bandwidth" bind:value={newLagBandwidth} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="1Gbps">1Gbps</option>
						<option value="10Gbps">10Gbps</option>
						<option value="100Gbps">100Gbps</option>
						<option value="400Gbps">400Gbps</option>
					</select>
				</div>
				<div>
					<label for="dc-lag-new-count" class="text-sm text-slate-600 dark:text-slate-300"># Connections</label>
					<input id="dc-lag-new-count" type="number" bind:value={newLagConnections} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if lagCreateError}<p class="text-sm text-red-600 dark:text-red-400">{lagCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => lagCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateLag} disabled={lagCreating} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{lagCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={lagEditModal} title="Edit LAG">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dc-lag-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="dc-lag-edit-name" bind:value={editLagName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dc-lag-edit-minlinks" class="text-sm text-slate-600 dark:text-slate-300">Minimum links</label>
				<input id="dc-lag-edit-minlinks" type="number" bind:value={editLagMinLinks} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if lagEditError}<p class="text-sm text-red-600 dark:text-red-400">{lagEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => lagEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditLag} disabled={lagEditing} class="rounded-lg bg-teal-600 px-4 py-2 text-sm font-semibold text-white hover:bg-teal-700 disabled:opacity-50">{lagEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
