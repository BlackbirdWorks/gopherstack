<script lang="ts">
	// AWS Application Migration Service (MGN) dashboard.
	//
	// MGN is really two products bolted onto one API namespace
	// (services/mgn/PARITY.md): 70 "legacy" ops are source-server replication
	// and migration orchestration; a structurally separate 25-op family
	// under /network-migration/ does network-topology analysis, code
	// generation, and deployment. This page keeps them as two distinct tab
	// GROUPS rather than merging them into one flat tab bar, matching the
	// task's explicit steer not to present them as one workflow. The tagging
	// trio (TagResource/UntagResource/ListTagsForResource, 3 ops) is not its
	// own tab -- it is exposed via the shared TagEditor component embedded in
	// every taggable resource kind's own detail view (12 kinds total).
	//
	// This file is composition + shared chrome only: every tab's own state,
	// data fetching, and markup live in ./_components/*Panel.svelte (95 ops
	// is far too many for one file -- the four prior route restores already
	// hit 3350-4384 lines each without anywhere near this op count). Only
	// the ACTIVE tab's panel is ever mounted (an `{#if}`/`{:else if}` chain,
	// same as every other multi-tab page in this codebase, e.g.
	// dax/+page.svelte) -- switching tabs destroys the previous panel and
	// creates the new one, which re-fetches on mount via its own
	// `onRegionChange`. This is a deliberate choice over keeping every panel
	// permanently mounted behind a CSS `hidden` class: this app's test
	// environment does not load Tailwind's stylesheet, so a `hidden` class
	// has no actual effect in jsdom and would leave every tab's content
	// simultaneously queryable, breaking `getByText`/`getByRole` across the
	// whole suite. `{#if}` genuinely removes inactive tabs from the DOM,
	// matching what every existing page.test.ts in this codebase already
	// assumes.
	import { regionalClient } from '$lib/region-effect.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import { getMGNClient } from '$lib/aws-client';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import { Server } from 'lucide-svelte';
	import SourceServersPanel from './_components/SourceServersPanel.svelte';
	import JobsPanel from './_components/JobsPanel.svelte';
	import ApplicationsPanel from './_components/ApplicationsPanel.svelte';
	import WavesPanel from './_components/WavesPanel.svelte';
	import LaunchTemplatesPanel from './_components/LaunchTemplatesPanel.svelte';
	import ReplicationTemplatesPanel from './_components/ReplicationTemplatesPanel.svelte';
	import ConnectorsPanel from './_components/ConnectorsPanel.svelte';
	import VcenterClientsPanel from './_components/VcenterClientsPanel.svelte';
	import ImportExportPanel from './_components/ImportExportPanel.svelte';
	import ServiceInitPanel from './_components/ServiceInitPanel.svelte';
	import NetworkMigrationDefinitionsPanel from './_components/NetworkMigrationDefinitionsPanel.svelte';
	import NetworkMigrationMapperPanel from './_components/NetworkMigrationMapperPanel.svelte';
	import NetworkMigrationMappingsPanel from './_components/NetworkMigrationMappingsPanel.svelte';
	import NetworkMigrationAnalysisPanel from './_components/NetworkMigrationAnalysisPanel.svelte';
	import NetworkMigrationCodeGenPanel from './_components/NetworkMigrationCodeGenPanel.svelte';
	import NetworkMigrationDeployPanel from './_components/NetworkMigrationDeployPanel.svelte';

	type ReplicationTabId =
		| 'sourceServers'
		| 'jobs'
		| 'applications'
		| 'waves'
		| 'launchTemplates'
		| 'replicationTemplates'
		| 'connectors'
		| 'vcenterClients'
		| 'importExport'
		| 'serviceInit';
	type NetworkMigrationTabId =
		| 'nmDefinitions'
		| 'nmMapper'
		| 'nmMappings'
		| 'nmAnalysis'
		| 'nmCodeGen'
		| 'nmDeploy';
	type TabId = ReplicationTabId | NetworkMigrationTabId;
	type Group = 'replication' | 'networkMigration';

	const NETWORK_MIGRATION_TAB_IDS: readonly NetworkMigrationTabId[] = [
		'nmDefinitions',
		'nmMapper',
		'nmMappings',
		'nmAnalysis',
		'nmCodeGen',
		'nmDeploy'
	];

	function groupOf(tab: TabId): Group {
		return (NETWORK_MIGRATION_TAB_IDS as readonly TabId[]).includes(tab) ? 'networkMigration' : 'replication';
	}

	const groupTabs: TabDef[] = [
		{ id: 'replication', label: 'Replication' },
		{ id: 'networkMigration', label: 'Network Migration' }
	];
	const replicationTabs: TabDef[] = [
		{ id: 'sourceServers', label: 'Source Servers' },
		{ id: 'jobs', label: 'Jobs' },
		{ id: 'applications', label: 'Applications' },
		{ id: 'waves', label: 'Waves' },
		{ id: 'launchTemplates', label: 'Launch Templates' },
		{ id: 'replicationTemplates', label: 'Replication Templates' },
		{ id: 'connectors', label: 'Connectors' },
		{ id: 'vcenterClients', label: 'vCenter Clients' },
		{ id: 'importExport', label: 'Import / Export' },
		{ id: 'serviceInit', label: 'Service Init' }
	];
	const networkMigrationTabs: TabDef[] = [
		{ id: 'nmDefinitions', label: 'Definitions' },
		{ id: 'nmMapper', label: 'Mapper Segments' },
		{ id: 'nmMappings', label: 'Mappings' },
		{ id: 'nmAnalysis', label: 'Analysis' },
		{ id: 'nmCodeGen', label: 'Code Generation' },
		{ id: 'nmDeploy', label: 'Deployment' }
	];

	const client = regionalClient(getMGNClient);

	// URL-backed (?tab=...). See url-state.svelte.ts.
	const pageTabParam = urlState<TabId>('tab', 'sourceServers');
	let activeTab = $derived(pageTabParam.get());
	let activeGroup = $derived(groupOf(activeTab));
	let searchQuery = $state('');

	// Bound to whichever of the 11 "list-backed" panels (every tab except
	// the five scoped-only Network Migration sub-tabs -- Mapper Segments,
	// Mappings, Analysis, Code Generation, Deployment -- which have no
	// account-wide list to refresh) is currently mounted, so the shared
	// PageHeader Refresh button can force a reload without each panel
	// needing to be individually wired here. Typed structurally (just the
	// one method every such panel exports) rather than as a big union of
	// concrete component types.
	let activePanelRef = $state<{ refresh: () => Promise<void> } | null>(null);

	function switchTab(id: string): void {
		pageTabParam.set(id as TabId);
		searchQuery = '';
	}

	function switchGroup(id: string): void {
		const g = id as Group;
		const first = g === 'networkMigration' ? networkMigrationTabs[0] : replicationTabs[0];
		switchTab(first.id);
	}

	function handleRefresh(): void {
		void activePanelRef?.refresh();
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Server}
		title="AWS Application Migration Service"
		description="Lift-and-shift migration: source-server replication and cutover, plus network-topology migration planning"
		onRefresh={handleRefresh}
		color="blue"
		service="mgn"
	/>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 space-y-3">
			<Tabs tabs={groupTabs} active={activeGroup} onSelect={switchGroup} color="indigo" />
			<div class="flex flex-col sm:flex-row gap-3 justify-between">
				{#if activeGroup === 'replication'}
					<Tabs tabs={replicationTabs} active={activeTab} onSelect={switchTab} color="blue" />
				{:else}
					<Tabs tabs={networkMigrationTabs} active={activeTab} onSelect={switchTab} color="violet" />
				{/if}
				<SearchInput bind:value={searchQuery} />
			</div>
		</div>

		<div class="p-4 space-y-4">
			{#if activeTab === 'sourceServers'}
				<SourceServersPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'jobs'}
				<JobsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'applications'}
				<ApplicationsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'waves'}
				<WavesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'launchTemplates'}
				<LaunchTemplatesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'replicationTemplates'}
				<ReplicationTemplatesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'connectors'}
				<ConnectorsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'vcenterClients'}
				<VcenterClientsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'importExport'}
				<ImportExportPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'serviceInit'}
				<ServiceInitPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'nmDefinitions'}
				<NetworkMigrationDefinitionsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'nmMapper'}
				<NetworkMigrationMapperPanel {client} {searchQuery} />
			{:else if activeTab === 'nmMappings'}
				<NetworkMigrationMappingsPanel {client} {searchQuery} />
			{:else if activeTab === 'nmAnalysis'}
				<NetworkMigrationAnalysisPanel {client} {searchQuery} />
			{:else if activeTab === 'nmCodeGen'}
				<NetworkMigrationCodeGenPanel {client} {searchQuery} />
			{:else if activeTab === 'nmDeploy'}
				<NetworkMigrationDeployPanel {client} {searchQuery} />
			{/if}
		</div>
	</div>
</div>
