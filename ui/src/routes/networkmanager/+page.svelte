<script lang="ts">
	// AWS Network Manager dashboard.
	//
	// Network Manager genuinely spans two related but historically distinct
	// products (services/networkmanager/PARITY.md): "Global Networks" is the
	// on-premises modeling half (sites, devices, links, customer-gateway/
	// transit-gateway registrations); "Cloud WAN" is the managed-backbone
	// half (core networks, policy lifecycle, segments, attachments,
	// peerings, Connect peers). This page keeps them as two distinct tab
	// GROUPS, matching mgn's own Replication/Network-Migration split, rather
	// than merging 95 operations' worth of tabs into one flat bar. The one
	// real structural bridge between the halves -- AssociateConnectPeer/
	// DisassociateConnectPeer, which binds a Cloud WAN ConnectPeer to an
	// on-prem Device/Link -- lives inside the Global Networks group's
	// Associations tab (it is fundamentally a Device/Link-scoped
	// association, same shape as its four siblings there), with an inline
	// note pointing at the Cloud WAN Connect Peers tab for the other side.
	//
	// Composition + shared chrome only, same as mgn/+page.svelte: every
	// tab's own state, data fetching and markup live in
	// ./_components/*Panel.svelte. Only the ACTIVE tab's panel is ever
	// mounted via a real `{#if}`/`{:else if}` chain (not a CSS `hidden`
	// class -- this app's test environment does not load Tailwind, so
	// `hidden` has no effect in jsdom and would leave every tab
	// simultaneously queryable, breaking `getByText`/`getByRole` isolation).
	// Switching tabs destroys the previous panel and mounts a new one, which
	// fetches on its own via `onRegionChange` (or, for panels scoped under a
	// user-picked Global Network, a plain `$effect` reacting to that pick).
	import { regionalClient } from '$lib/region-effect.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import { getNetworkManagerClient } from '$lib/aws-client';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import { Network } from 'lucide-svelte';
	import GlobalNetworksPanel from './_components/GlobalNetworksPanel.svelte';
	import SitesPanel from './_components/SitesPanel.svelte';
	import DevicesPanel from './_components/DevicesPanel.svelte';
	import LinksPanel from './_components/LinksPanel.svelte';
	import ConnectionsPanel from './_components/ConnectionsPanel.svelte';
	import AssociationsPanel from './_components/AssociationsPanel.svelte';
	import CoreNetworksPanel from './_components/CoreNetworksPanel.svelte';
	import ConnectPeersPanel from './_components/ConnectPeersPanel.svelte';
	import AttachmentsPanel from './_components/AttachmentsPanel.svelte';
	import PeeringsPanel from './_components/PeeringsPanel.svelte';
	import RouteAnalysisPanel from './_components/RouteAnalysisPanel.svelte';
	import NetworkInsightsPanel from './_components/NetworkInsightsPanel.svelte';
	import AdminPanel from './_components/AdminPanel.svelte';

	type GlobalNetworksTabId = 'globalNetworks' | 'sites' | 'devices' | 'links' | 'connections' | 'associations';
	type CloudWanTabId =
		| 'coreNetworks'
		| 'connectPeers'
		| 'attachments'
		| 'peerings'
		| 'routeAnalysis'
		| 'networkInsights'
		| 'admin';
	type TabId = GlobalNetworksTabId | CloudWanTabId;
	type Group = 'globalNetworks' | 'cloudWan';

	const CLOUD_WAN_TAB_IDS: readonly CloudWanTabId[] = [
		'coreNetworks',
		'connectPeers',
		'attachments',
		'peerings',
		'routeAnalysis',
		'networkInsights',
		'admin'
	];

	function groupOf(tab: TabId): Group {
		return (CLOUD_WAN_TAB_IDS as readonly TabId[]).includes(tab) ? 'cloudWan' : 'globalNetworks';
	}

	const groupTabs: TabDef[] = [
		{ id: 'globalNetworks', label: 'Global Networks' },
		{ id: 'cloudWan', label: 'Cloud WAN' }
	];
	const globalNetworksTabs: TabDef[] = [
		{ id: 'globalNetworks', label: 'Global Networks' },
		{ id: 'sites', label: 'Sites' },
		{ id: 'devices', label: 'Devices' },
		{ id: 'links', label: 'Links' },
		{ id: 'connections', label: 'Connections' },
		{ id: 'associations', label: 'Associations' }
	];
	const cloudWanTabs: TabDef[] = [
		{ id: 'coreNetworks', label: 'Core Networks' },
		{ id: 'connectPeers', label: 'Connect Peers' },
		{ id: 'attachments', label: 'Attachments' },
		{ id: 'peerings', label: 'Peerings' },
		{ id: 'routeAnalysis', label: 'Route Analysis' },
		{ id: 'networkInsights', label: 'Network Insights' },
		{ id: 'admin', label: 'Admin' }
	];

	const client = regionalClient(getNetworkManagerClient);

	// URL-backed (?tab=...). See url-state.svelte.ts.
	const pageTabParam = urlState<TabId>('tab', 'globalNetworks');
	let activeTab = $derived(pageTabParam.get());
	let activeGroup = $derived(groupOf(activeTab));
	let searchQuery = $state('');

	// Bound to whichever panel is currently mounted, so the shared
	// PageHeader Refresh button can force a reload without each panel
	// needing to be individually wired here.
	let activePanelRef = $state<{ refresh: () => Promise<void> } | null>(null);

	function switchTab(id: string): void {
		pageTabParam.set(id as TabId);
		searchQuery = '';
	}

	function switchGroup(id: string): void {
		const g = id as Group;
		const first = g === 'cloudWan' ? cloudWanTabs[0] : globalNetworksTabs[0];
		switchTab(first.id);
	}

	function handleRefresh(): void {
		void activePanelRef?.refresh();
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Network}
		title="AWS Network Manager"
		description="Global Networks (on-premises sites, devices, links) and Cloud WAN (managed core network backbone)"
		onRefresh={handleRefresh}
		color="cyan"
		service="networkmanager"
	/>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 space-y-3">
			<Tabs tabs={groupTabs} active={activeGroup} onSelect={switchGroup} color="indigo" />
			<div class="flex flex-col sm:flex-row gap-3 justify-between">
				{#if activeGroup === 'globalNetworks'}
					<Tabs tabs={globalNetworksTabs} active={activeTab} onSelect={switchTab} color="cyan" />
				{:else}
					<Tabs tabs={cloudWanTabs} active={activeTab} onSelect={switchTab} color="violet" />
				{/if}
				<SearchInput bind:value={searchQuery} />
			</div>
		</div>

		<div class="p-4 space-y-4">
			{#if activeTab === 'globalNetworks'}
				<GlobalNetworksPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'sites'}
				<SitesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'devices'}
				<DevicesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'links'}
				<LinksPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'connections'}
				<ConnectionsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'associations'}
				<AssociationsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'coreNetworks'}
				<CoreNetworksPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'connectPeers'}
				<ConnectPeersPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'attachments'}
				<AttachmentsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'peerings'}
				<PeeringsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'routeAnalysis'}
				<RouteAnalysisPanel {client} {searchQuery} />
			{:else if activeTab === 'networkInsights'}
				<NetworkInsightsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'admin'}
				<AdminPanel bind:this={activePanelRef} {client} {searchQuery} />
			{/if}
		</div>
	</div>
</div>
