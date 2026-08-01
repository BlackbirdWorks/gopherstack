<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getNetworkManagerClient } from '$lib/aws-client';
	import {
		DescribeGlobalNetworksCommand,
		GetSitesCommand,
		GetDevicesCommand,
		GetLinksCommand,
		CreateGlobalNetworkCommand,
		UpdateGlobalNetworkCommand,
		DeleteGlobalNetworkCommand,
		CreateSiteCommand,
		UpdateSiteCommand,
		DeleteSiteCommand,
		CreateDeviceCommand,
		UpdateDeviceCommand,
		DeleteDeviceCommand,
		CreateLinkCommand,
		UpdateLinkCommand,
		DeleteLinkCommand,
		type GlobalNetwork,
		type Site,
		type Device,
		type Link
	} from '@aws-sdk/client-networkmanager';
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
	import { Globe, Plus, Trash2, Eye, Pencil, ArrowLeft } from 'lucide-svelte';

	// Network Manager's global network is a container: sites, devices and
	// links all nest under one. Sites/Devices/Links tabs only make sense once
	// a global network is selected -- and because that selection is a
	// GlobalNetworkId (not globally unique across regions any more than any
	// other resource id), it MUST be cleared on region change, same as every
	// other per-region drill-down on this codebase. onRegionChange below
	// resets selectedNetwork and returns to the networks list rather than
	// leaving a stale id from the old region selected.
	//
	// Real Network Manager also has Core Networks, attachments (VPC/Connect/
	// Site-to-Site VPN/Transit Gateway), peerings and transit gateway
	// registrations -- a second, much larger subsystem (Cloud WAN) layered
	// on top of the classic global-network/site/device/link model. That
	// subsystem is out of scope for this pass (reported, not built); this
	// page covers exactly the family the task named: global networks with
	// nested sites, links and devices.
	const client = regionalClient(getNetworkManagerClient);

	type TabId = 'networks' | 'sites' | 'devices' | 'links';

	let activeTab = $state<TabId>('networks');
	let searchQuery = $state('');
	let selectedNetwork = $state<GlobalNetwork | null>(null);

	const tabs = $derived<TabDef[]>(
		selectedNetwork
			? [
					{ id: 'networks', label: 'Global Networks' },
					{ id: 'sites', label: 'Sites' },
					{ id: 'devices', label: 'Devices' },
					{ id: 'links', label: 'Links' }
				]
			: [{ id: 'networks', label: 'Global Networks' }]
	);

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
		if (state === 'AVAILABLE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'DELETING' || state === 'UPDATING')
			return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let globalNetworks = $state<GlobalNetwork[]>([]);
	let sites = $state<Site[]>([]);
	let devices = $state<Device[]>([]);
	let links = $state<Link[]>([]);

	async function fetchNetworks(): Promise<void> {
		const resp = await client().send(new DescribeGlobalNetworksCommand({}));
		globalNetworks = resp.GlobalNetworks ?? [];
	}

	async function fetchSites(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId) {
			sites = [];
			return;
		}
		const resp = await client().send(new GetSitesCommand({ GlobalNetworkId: selectedNetwork.GlobalNetworkId }));
		sites = resp.Sites ?? [];
	}

	async function fetchDevices(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId) {
			devices = [];
			return;
		}
		const resp = await client().send(new GetDevicesCommand({ GlobalNetworkId: selectedNetwork.GlobalNetworkId }));
		devices = resp.Devices ?? [];
	}

	async function fetchLinks(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId) {
			links = [];
			return;
		}
		const resp = await client().send(new GetLinksCommand({ GlobalNetworkId: selectedNetwork.GlobalNetworkId }));
		links = resp.Links ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		networks: () => fetchNetworks().catch(rethrowDescribed),
		sites: () => fetchSites().catch(rethrowDescribed),
		devices: () => fetchDevices().catch(rethrowDescribed),
		links: () => fetchLinks().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function openNetwork(n: GlobalNetwork): void {
		selectedNetwork = n;
		activeTab = 'sites';
		searchQuery = '';
		void tabLoader.refresh('sites');
		void tabLoader.refresh('devices');
		void tabLoader.refresh('links');
	}

	function backToNetworks(): void {
		selectedNetwork = null;
		activeTab = 'networks';
		searchQuery = '';
	}

	// Region change invalidates selectedNetwork.GlobalNetworkId (ids are not
	// unique across regions) -- always drop back to the networks list.
	onRegionChange(() => {
		selectedNetwork = null;
		activeTab = 'networks';
		searchQuery = '';
		tabLoader.refresh('networks');
	});

	const filteredNetworks = $derived(
		globalNetworks.filter((n) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(n.GlobalNetworkId ?? '').toLowerCase().includes(q) ||
				(n.Description ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredSites = $derived(
		sites.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (s.SiteId ?? '').toLowerCase().includes(q) || (s.Description ?? '').toLowerCase().includes(q);
		})
	);
	const filteredDevices = $derived(
		devices.filter((d) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (d.DeviceId ?? '').toLowerCase().includes(q) || (d.Description ?? '').toLowerCase().includes(q);
		})
	);
	const filteredLinks = $derived(
		links.filter((l) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (l.LinkId ?? '').toLowerCase().includes(q) || (l.Description ?? '').toLowerCase().includes(q);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Global networks ---

	let netCreateModal = $state<Modal | null>(null);
	let netCreating = $state(false);
	let netCreateError = $state<string | null>(null);
	let newNetDescription = $state('');

	function openNetCreateModal(): void {
		netCreateError = null;
		newNetDescription = '';
		netCreateModal?.open();
	}

	async function submitCreateNet(): Promise<void> {
		netCreating = true;
		netCreateError = null;
		try {
			await client().send(new CreateGlobalNetworkCommand({ Description: newNetDescription || undefined }));
			toast.success('Global network created');
			netCreateModal?.close();
			await tabLoader.refresh('networks');
		} catch (e) {
			const msg = describeError(e);
			netCreateError = msg;
			toast.error(msg);
		} finally {
			netCreating = false;
		}
	}

	let netEditModal = $state<Modal | null>(null);
	let netEditing = $state(false);
	let netEditError = $state<string | null>(null);
	let editNetId = $state('');
	let editNetDescription = $state('');

	function openNetEditModal(n: GlobalNetwork): void {
		netEditError = null;
		editNetId = n.GlobalNetworkId ?? '';
		editNetDescription = n.Description ?? '';
		netEditModal?.open();
	}

	async function submitEditNet(): Promise<void> {
		if (!editNetId) return;
		netEditing = true;
		netEditError = null;
		try {
			await client().send(
				new UpdateGlobalNetworkCommand({ GlobalNetworkId: editNetId, Description: editNetDescription || undefined })
			);
			toast.success('Global network updated');
			netEditModal?.close();
			await tabLoader.refresh('networks');
		} catch (e) {
			const msg = describeError(e);
			netEditError = msg;
			toast.error(msg);
		} finally {
			netEditing = false;
		}
	}

	async function deleteNetwork(n: GlobalNetwork): Promise<void> {
		if (!n.GlobalNetworkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete global network',
			message: `Delete global network ${n.GlobalNetworkId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteGlobalNetworkCommand({ GlobalNetworkId: n.GlobalNetworkId }));
			toast.success('Global network deleted');
			if (selectedNetwork?.GlobalNetworkId === n.GlobalNetworkId) backToNetworks();
			await tabLoader.refresh('networks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Sites ---

	let siteCreateModal = $state<Modal | null>(null);
	let siteCreating = $state(false);
	let siteCreateError = $state<string | null>(null);
	let newSiteDescription = $state('');
	let newSiteAddress = $state('');

	function openSiteCreateModal(): void {
		siteCreateError = null;
		newSiteDescription = '';
		newSiteAddress = '';
		siteCreateModal?.open();
	}

	async function submitCreateSite(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId) return;
		siteCreating = true;
		siteCreateError = null;
		try {
			await client().send(
				new CreateSiteCommand({
					GlobalNetworkId: selectedNetwork.GlobalNetworkId,
					Description: newSiteDescription || undefined,
					Location: newSiteAddress ? { Address: newSiteAddress } : undefined
				})
			);
			toast.success('Site created');
			siteCreateModal?.close();
			await tabLoader.refresh('sites');
		} catch (e) {
			const msg = describeError(e);
			siteCreateError = msg;
			toast.error(msg);
		} finally {
			siteCreating = false;
		}
	}

	let siteEditModal = $state<Modal | null>(null);
	let siteEditing = $state(false);
	let siteEditError = $state<string | null>(null);
	let editSiteId = $state('');
	let editSiteDescription = $state('');

	function openSiteEditModal(s: Site): void {
		siteEditError = null;
		editSiteId = s.SiteId ?? '';
		editSiteDescription = s.Description ?? '';
		siteEditModal?.open();
	}

	async function submitEditSite(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId || !editSiteId) return;
		siteEditing = true;
		siteEditError = null;
		try {
			await client().send(
				new UpdateSiteCommand({
					GlobalNetworkId: selectedNetwork.GlobalNetworkId,
					SiteId: editSiteId,
					Description: editSiteDescription || undefined
				})
			);
			toast.success('Site updated');
			siteEditModal?.close();
			await tabLoader.refresh('sites');
		} catch (e) {
			const msg = describeError(e);
			siteEditError = msg;
			toast.error(msg);
		} finally {
			siteEditing = false;
		}
	}

	async function deleteSite(s: Site): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId || !s.SiteId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete site',
			message: `Delete site ${s.SiteId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteSiteCommand({ GlobalNetworkId: selectedNetwork.GlobalNetworkId, SiteId: s.SiteId })
			);
			toast.success('Site deleted');
			await tabLoader.refresh('sites');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Devices ---

	let deviceCreateModal = $state<Modal | null>(null);
	let deviceCreating = $state(false);
	let deviceCreateError = $state<string | null>(null);
	let newDeviceDescription = $state('');
	let newDeviceType = $state('');
	let newDeviceSiteId = $state('');

	function openDeviceCreateModal(): void {
		deviceCreateError = null;
		newDeviceDescription = '';
		newDeviceType = '';
		newDeviceSiteId = sites[0]?.SiteId ?? '';
		deviceCreateModal?.open();
	}

	async function submitCreateDevice(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId) return;
		deviceCreating = true;
		deviceCreateError = null;
		try {
			await client().send(
				new CreateDeviceCommand({
					GlobalNetworkId: selectedNetwork.GlobalNetworkId,
					Description: newDeviceDescription || undefined,
					Type: newDeviceType || undefined,
					SiteId: newDeviceSiteId || undefined
				})
			);
			toast.success('Device created');
			deviceCreateModal?.close();
			await tabLoader.refresh('devices');
		} catch (e) {
			const msg = describeError(e);
			deviceCreateError = msg;
			toast.error(msg);
		} finally {
			deviceCreating = false;
		}
	}

	let deviceEditModal = $state<Modal | null>(null);
	let deviceEditing = $state(false);
	let deviceEditError = $state<string | null>(null);
	let editDeviceId = $state('');
	let editDeviceDescription = $state('');

	function openDeviceEditModal(d: Device): void {
		deviceEditError = null;
		editDeviceId = d.DeviceId ?? '';
		editDeviceDescription = d.Description ?? '';
		deviceEditModal?.open();
	}

	async function submitEditDevice(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId || !editDeviceId) return;
		deviceEditing = true;
		deviceEditError = null;
		try {
			await client().send(
				new UpdateDeviceCommand({
					GlobalNetworkId: selectedNetwork.GlobalNetworkId,
					DeviceId: editDeviceId,
					Description: editDeviceDescription || undefined
				})
			);
			toast.success('Device updated');
			deviceEditModal?.close();
			await tabLoader.refresh('devices');
		} catch (e) {
			const msg = describeError(e);
			deviceEditError = msg;
			toast.error(msg);
		} finally {
			deviceEditing = false;
		}
	}

	async function deleteDevice(d: Device): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId || !d.DeviceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete device',
			message: `Delete device ${d.DeviceId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDeviceCommand({ GlobalNetworkId: selectedNetwork.GlobalNetworkId, DeviceId: d.DeviceId })
			);
			toast.success('Device deleted');
			await tabLoader.refresh('devices');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Links ---

	let linkCreateModal = $state<Modal | null>(null);
	let linkCreating = $state(false);
	let linkCreateError = $state<string | null>(null);
	let newLinkSiteId = $state('');
	let newLinkDescription = $state('');
	let newLinkUploadSpeed = $state(100);
	let newLinkDownloadSpeed = $state(100);

	function openLinkCreateModal(): void {
		linkCreateError = null;
		newLinkSiteId = sites[0]?.SiteId ?? '';
		newLinkDescription = '';
		newLinkUploadSpeed = 100;
		newLinkDownloadSpeed = 100;
		linkCreateModal?.open();
	}

	async function submitCreateLink(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId) return;
		if (!newLinkSiteId) {
			linkCreateError = 'A site is required -- create a site first.';
			return;
		}
		linkCreating = true;
		linkCreateError = null;
		try {
			await client().send(
				new CreateLinkCommand({
					GlobalNetworkId: selectedNetwork.GlobalNetworkId,
					SiteId: newLinkSiteId,
					Description: newLinkDescription || undefined,
					Bandwidth: { UploadSpeed: newLinkUploadSpeed, DownloadSpeed: newLinkDownloadSpeed }
				})
			);
			toast.success('Link created');
			linkCreateModal?.close();
			await tabLoader.refresh('links');
		} catch (e) {
			const msg = describeError(e);
			linkCreateError = msg;
			toast.error(msg);
		} finally {
			linkCreating = false;
		}
	}

	let linkEditModal = $state<Modal | null>(null);
	let linkEditing = $state(false);
	let linkEditError = $state<string | null>(null);
	let editLinkId = $state('');
	let editLinkDescription = $state('');

	function openLinkEditModal(l: Link): void {
		linkEditError = null;
		editLinkId = l.LinkId ?? '';
		editLinkDescription = l.Description ?? '';
		linkEditModal?.open();
	}

	async function submitEditLink(): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId || !editLinkId) return;
		linkEditing = true;
		linkEditError = null;
		try {
			await client().send(
				new UpdateLinkCommand({
					GlobalNetworkId: selectedNetwork.GlobalNetworkId,
					LinkId: editLinkId,
					Description: editLinkDescription || undefined
				})
			);
			toast.success('Link updated');
			linkEditModal?.close();
			await tabLoader.refresh('links');
		} catch (e) {
			const msg = describeError(e);
			linkEditError = msg;
			toast.error(msg);
		} finally {
			linkEditing = false;
		}
	}

	async function deleteLink(l: Link): Promise<void> {
		if (!selectedNetwork?.GlobalNetworkId || !l.LinkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete link',
			message: `Delete link ${l.LinkId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteLinkCommand({ GlobalNetworkId: selectedNetwork.GlobalNetworkId, LinkId: l.LinkId })
			);
			toast.success('Link deleted');
			await tabLoader.refresh('links');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Globe}
		title="AWS Network Manager"
		description="Centrally manage your global network across AWS and on-premises"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'networks'}
				<button
					onclick={openNetCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create global network
				</button>
			{:else if activeTab === 'sites'}
				<button
					onclick={openSiteCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create site
				</button>
			{:else if activeTab === 'devices'}
				<button
					onclick={openDeviceCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create device
				</button>
			{:else if activeTab === 'links'}
				<button
					onclick={openLinkCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create link
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	{#if selectedNetwork}
		<div class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
			<button onclick={backToNetworks} class="flex items-center gap-1 hover:text-blue-500">
				<ArrowLeft class="w-4 h-4" /> All global networks
			</button>
			<span>/</span>
			<span class="font-medium text-slate-900 dark:text-white">{selectedNetwork.GlobalNetworkId}</span>
		</div>
	{/if}

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs tabs={tabs} active={activeTab} onSelect={switchTab} color="blue" />
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

			{#if activeTab === 'networks'}
				{#snippet netStateCell(n: GlobalNetwork)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(n.State)}">{n.State ?? '—'}</span>
				{/snippet}
				{#snippet netActionsCell(n: GlobalNetwork)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openNetwork(n)} title="Open" aria-label="Open global network {n.GlobalNetworkId}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openNetEditModal(n)} title="Edit" aria-label="Edit global network {n.GlobalNetworkId}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteNetwork(n)} title="Delete" aria-label="Delete global network {n.GlobalNetworkId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const netColumns = defineColumns<GlobalNetwork>([
					{ key: 'GlobalNetworkId', label: 'ID' },
					{ key: 'Description', label: 'Description' },
					{ key: 'State', label: 'State', render: netStateCell },
					{ key: 'actions', label: '', render: netActionsCell }
				])}
				<DataTable
					rows={filteredNetworks}
					rowKey={(n) => n.GlobalNetworkId ?? ''}
					columns={netColumns}
					loading={tabLoader.isLoading('networks')}
					emptyMessage="No global networks found"
				/>
			{:else if activeTab === 'sites'}
				{#snippet siteStateCell(s: Site)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(s.State)}">{s.State ?? '—'}</span>
				{/snippet}
				{#snippet siteActionsCell(s: Site)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSiteEditModal(s)} title="Edit" aria-label="Edit site {s.SiteId}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteSite(s)} title="Delete" aria-label="Delete site {s.SiteId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const siteColumns = defineColumns<Site>([
					{ key: 'SiteId', label: 'ID' },
					{ key: 'Description', label: 'Description' },
					{ key: 'State', label: 'State', render: siteStateCell },
					{ key: 'actions', label: '', render: siteActionsCell }
				])}
				<DataTable
					rows={filteredSites}
					rowKey={(s) => s.SiteId ?? ''}
					columns={siteColumns}
					loading={tabLoader.isLoading('sites')}
					emptyMessage="No sites found"
				/>
			{:else if activeTab === 'devices'}
				{#snippet deviceStateCell(d: Device)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(d.State)}">{d.State ?? '—'}</span>
				{/snippet}
				{#snippet deviceActionsCell(d: Device)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDeviceEditModal(d)} title="Edit" aria-label="Edit device {d.DeviceId}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteDevice(d)} title="Delete" aria-label="Delete device {d.DeviceId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const deviceColumns = defineColumns<Device>([
					{ key: 'DeviceId', label: 'ID' },
					{ key: 'Type', label: 'Type' },
					{ key: 'SiteId', label: 'Site' },
					{ key: 'State', label: 'State', render: deviceStateCell },
					{ key: 'actions', label: '', render: deviceActionsCell }
				])}
				<DataTable
					rows={filteredDevices}
					rowKey={(d) => d.DeviceId ?? ''}
					columns={deviceColumns}
					loading={tabLoader.isLoading('devices')}
					emptyMessage="No devices found"
				/>
			{:else if activeTab === 'links'}
				{#snippet linkBandwidthCell(l: Link)}
					<span class="text-xs text-gray-500 dark:text-gray-400"
						>{l.Bandwidth?.UploadSpeed ?? '—'} / {l.Bandwidth?.DownloadSpeed ?? '—'} Mbps</span
					>
				{/snippet}
				{#snippet linkStateCell(l: Link)}
					<span class="text-xs px-2 py-1 rounded-full {stateClass(l.State)}">{l.State ?? '—'}</span>
				{/snippet}
				{#snippet linkActionsCell(l: Link)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openLinkEditModal(l)} title="Edit" aria-label="Edit link {l.LinkId}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteLink(l)} title="Delete" aria-label="Delete link {l.LinkId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const linkColumns = defineColumns<Link>([
					{ key: 'LinkId', label: 'ID' },
					{ key: 'SiteId', label: 'Site' },
					{ key: 'Bandwidth', label: 'Bandwidth (up/down)', render: linkBandwidthCell },
					{ key: 'State', label: 'State', render: linkStateCell },
					{ key: 'actions', label: '', render: linkActionsCell }
				])}
				<DataTable
					rows={filteredLinks}
					rowKey={(l) => l.LinkId ?? ''}
					columns={linkColumns}
					loading={tabLoader.isLoading('links')}
					emptyMessage="No links found"
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={netCreateModal} title="Create Global Network">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-net-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-net-new-desc" bind:value={newNetDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if netCreateError}<p class="text-sm text-red-600 dark:text-red-400">{netCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => netCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateNet} disabled={netCreating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{netCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={netEditModal} title="Edit Global Network">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-net-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-net-edit-desc" bind:value={editNetDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if netEditError}<p class="text-sm text-red-600 dark:text-red-400">{netEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => netEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditNet} disabled={netEditing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{netEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={siteCreateModal} title="Create Site">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-site-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-site-new-desc" bind:value={newSiteDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="nm-site-new-address" class="text-sm text-slate-600 dark:text-slate-300">Address</label>
				<input id="nm-site-new-address" bind:value={newSiteAddress} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if siteCreateError}<p class="text-sm text-red-600 dark:text-red-400">{siteCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => siteCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateSite} disabled={siteCreating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{siteCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={siteEditModal} title="Edit Site">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-site-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-site-edit-desc" bind:value={editSiteDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if siteEditError}<p class="text-sm text-red-600 dark:text-red-400">{siteEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => siteEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditSite} disabled={siteEditing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{siteEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={deviceCreateModal} title="Create Device">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-device-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-device-new-desc" bind:value={newDeviceDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="nm-device-new-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<input id="nm-device-new-type" bind:value={newDeviceType} placeholder="router" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="nm-device-new-site" class="text-sm text-slate-600 dark:text-slate-300">Site</label>
				<select id="nm-device-new-site" bind:value={newDeviceSiteId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">None</option>
					{#each sites as s (s.SiteId)}
						<option value={s.SiteId}>{s.SiteId}</option>
					{/each}
				</select>
			</div>
			{#if deviceCreateError}<p class="text-sm text-red-600 dark:text-red-400">{deviceCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => deviceCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateDevice} disabled={deviceCreating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{deviceCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={deviceEditModal} title="Edit Device">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-device-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-device-edit-desc" bind:value={editDeviceDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if deviceEditError}<p class="text-sm text-red-600 dark:text-red-400">{deviceEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => deviceEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditDevice} disabled={deviceEditing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{deviceEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={linkCreateModal} title="Create Link">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-link-new-site" class="text-sm text-slate-600 dark:text-slate-300">Site</label>
				<select id="nm-link-new-site" bind:value={newLinkSiteId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each sites as s (s.SiteId)}
						<option value={s.SiteId}>{s.SiteId}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="nm-link-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-link-new-desc" bind:value={newLinkDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="nm-link-new-upload" class="text-sm text-slate-600 dark:text-slate-300">Upload (Mbps)</label>
					<input id="nm-link-new-upload" type="number" bind:value={newLinkUploadSpeed} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="nm-link-new-download" class="text-sm text-slate-600 dark:text-slate-300">Download (Mbps)</label>
					<input id="nm-link-new-download" type="number" bind:value={newLinkDownloadSpeed} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if linkCreateError}<p class="text-sm text-red-600 dark:text-red-400">{linkCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => linkCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateLink} disabled={linkCreating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{linkCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={linkEditModal} title="Edit Link">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="nm-link-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="nm-link-edit-desc" bind:value={editLinkDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if linkEditError}<p class="text-sm text-red-600 dark:text-red-400">{linkEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => linkEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditLink} disabled={linkEditing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{linkEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
