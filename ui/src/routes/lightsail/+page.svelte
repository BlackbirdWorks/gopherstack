<script lang="ts">
	// Amazon Lightsail dashboard -- 161 operations across 28 op-families
	// (services/lightsail/PARITY.md), the largest service in this codebase.
	// Grouped into 8 tab groups (compute, storage, networking, databases,
	// containers, distributions, domains, monitoring) rather than 20+ flat
	// tabs, following mgn's/networkmanager's own group-of-groups pattern.
	//
	// Composition + shared chrome only: every tab's own state, data fetching
	// and markup live in ./_components/*Panel.svelte. Only the ACTIVE tab's
	// panel is ever mounted via a real `{#if}`/`{:else if}` chain (not a CSS
	// `hidden` class -- this app's test environment does not load Tailwind,
	// so `hidden` has no effect in jsdom and would leave every tab
	// simultaneously queryable, breaking `getByText`/`getByRole` isolation).
	// Switching tabs destroys the previous panel and mounts a new one, which
	// fetches on its own via `onRegionChange`.
	//
	// Several op-families are deliberately folded into a parent panel's
	// detail view rather than getting their own top-level tab -- see each
	// panel's own doc comment for which ops it absorbed (e.g. InstancesPanel
	// folds in instance ports/access/metrics/add-ons/GUI-sessions;
	// LoadBalancersPanel folds in LB TLS certificates; DatabasesPanel folds
	// in events/logs/parameters/master-password/metrics).
	import { regionalClient } from '$lib/region-effect.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import { getLightsailClient } from '$lib/aws-client';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import { Server } from 'lucide-svelte';
	import InstancesPanel from './_components/InstancesPanel.svelte';
	import InstanceSnapshotsPanel from './_components/InstanceSnapshotsPanel.svelte';
	import KeyPairsPanel from './_components/KeyPairsPanel.svelte';
	import StaticIpsPanel from './_components/StaticIpsPanel.svelte';
	import DisksPanel from './_components/DisksPanel.svelte';
	import DiskSnapshotsPanel from './_components/DiskSnapshotsPanel.svelte';
	import BucketsPanel from './_components/BucketsPanel.svelte';
	import LoadBalancersPanel from './_components/LoadBalancersPanel.svelte';
	import VpcPeeringPanel from './_components/VpcPeeringPanel.svelte';
	import DatabasesPanel from './_components/DatabasesPanel.svelte';
	import DatabaseSnapshotsPanel from './_components/DatabaseSnapshotsPanel.svelte';
	import ContainerServicesPanel from './_components/ContainerServicesPanel.svelte';
	import DistributionsPanel from './_components/DistributionsPanel.svelte';
	import CertificatesPanel from './_components/CertificatesPanel.svelte';
	import DomainsPanel from './_components/DomainsPanel.svelte';
	import AlarmsPanel from './_components/AlarmsPanel.svelte';
	import ContactMethodsPanel from './_components/ContactMethodsPanel.svelte';
	import OperationsPanel from './_components/OperationsPanel.svelte';
	import ReferenceDataPanel from './_components/ReferenceDataPanel.svelte';
	import ExportCfnPanel from './_components/ExportCfnPanel.svelte';
	import AccountPanel from './_components/AccountPanel.svelte';

	type ComputeTabId = 'instances' | 'instanceSnapshots' | 'keyPairs' | 'staticIps';
	type StorageTabId = 'disks' | 'diskSnapshots' | 'buckets';
	type NetworkingTabId = 'loadBalancers' | 'vpcPeering';
	type DatabaseTabId = 'databases' | 'databaseSnapshots';
	type ContainerTabId = 'containerServices';
	type DistributionTabId = 'distributions' | 'certificates';
	type DomainTabId = 'domains';
	type MonitoringTabId = 'alarms' | 'contactMethods' | 'operations' | 'referenceData' | 'exportCfn' | 'account';
	type TabId =
		| ComputeTabId
		| StorageTabId
		| NetworkingTabId
		| DatabaseTabId
		| ContainerTabId
		| DistributionTabId
		| DomainTabId
		| MonitoringTabId;
	type Group =
		| 'compute'
		| 'storage'
		| 'networking'
		| 'databases'
		| 'containers'
		| 'distributions'
		| 'domains'
		| 'monitoring';

	const TAB_GROUPS: Record<Group, readonly TabId[]> = {
		compute: ['instances', 'instanceSnapshots', 'keyPairs', 'staticIps'],
		storage: ['disks', 'diskSnapshots', 'buckets'],
		networking: ['loadBalancers', 'vpcPeering'],
		databases: ['databases', 'databaseSnapshots'],
		containers: ['containerServices'],
		distributions: ['distributions', 'certificates'],
		domains: ['domains'],
		monitoring: ['alarms', 'contactMethods', 'operations', 'referenceData', 'exportCfn', 'account']
	};

	function groupOf(tab: TabId): Group {
		for (const [group, tabs] of Object.entries(TAB_GROUPS) as [Group, readonly TabId[]][]) {
			if ((tabs as readonly string[]).includes(tab)) return group;
		}
		return 'compute';
	}

	const groupTabs: TabDef[] = [
		{ id: 'compute', label: 'Compute' },
		{ id: 'storage', label: 'Storage' },
		{ id: 'networking', label: 'Networking' },
		{ id: 'databases', label: 'Databases' },
		{ id: 'containers', label: 'Containers' },
		{ id: 'distributions', label: 'Distributions' },
		{ id: 'domains', label: 'Domains' },
		{ id: 'monitoring', label: 'Monitoring' }
	];

	const tabLabels: Record<TabId, string> = {
		instances: 'Instances',
		instanceSnapshots: 'Instance Snapshots',
		keyPairs: 'Key Pairs',
		staticIps: 'Static IPs',
		disks: 'Disks',
		diskSnapshots: 'Disk Snapshots',
		buckets: 'Buckets',
		loadBalancers: 'Load Balancers',
		vpcPeering: 'VPC Peering',
		databases: 'Databases',
		databaseSnapshots: 'Database Snapshots',
		containerServices: 'Container Services',
		distributions: 'Distributions',
		certificates: 'Certificates',
		domains: 'Domains',
		alarms: 'Alarms',
		contactMethods: 'Contact Methods',
		operations: 'Operations',
		referenceData: 'Reference Data',
		exportCfn: 'Export & CloudFormation',
		account: 'Account'
	};

	function tabsFor(group: Group): TabDef[] {
		return TAB_GROUPS[group].map((id) => ({ id, label: tabLabels[id] }));
	}

	const client = regionalClient(getLightsailClient);

	// URL-backed (?tab=...). See url-state.svelte.ts.
	const pageTabParam = urlState<TabId>('tab', 'instances');
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
		switchTab(TAB_GROUPS[g][0]);
	}

	function handleRefresh(): void {
		void activePanelRef?.refresh();
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Server}
		title="Amazon Lightsail"
		description="Simplified VPS instances, block storage, managed databases, load balancers, a CDN, object storage, and container hosting"
		onRefresh={handleRefresh}
		color="teal"
		service="lightsail"
	/>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 space-y-3">
			<Tabs tabs={groupTabs} active={activeGroup} onSelect={switchGroup} color="indigo" />
			<div class="flex flex-col sm:flex-row gap-3 justify-between">
				<Tabs tabs={tabsFor(activeGroup)} active={activeTab} onSelect={switchTab} color="teal" />
				<SearchInput bind:value={searchQuery} />
			</div>
		</div>

		<div class="p-4 space-y-4">
			{#if activeTab === 'instances'}
				<InstancesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'instanceSnapshots'}
				<InstanceSnapshotsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'keyPairs'}
				<KeyPairsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'staticIps'}
				<StaticIpsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'disks'}
				<DisksPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'diskSnapshots'}
				<DiskSnapshotsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'buckets'}
				<BucketsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'loadBalancers'}
				<LoadBalancersPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'vpcPeering'}
				<VpcPeeringPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'databases'}
				<DatabasesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'databaseSnapshots'}
				<DatabaseSnapshotsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'containerServices'}
				<ContainerServicesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'distributions'}
				<DistributionsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'certificates'}
				<CertificatesPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'domains'}
				<DomainsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'alarms'}
				<AlarmsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'contactMethods'}
				<ContactMethodsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'operations'}
				<OperationsPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'referenceData'}
				<ReferenceDataPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'exportCfn'}
				<ExportCfnPanel bind:this={activePanelRef} {client} {searchQuery} />
			{:else if activeTab === 'account'}
				<AccountPanel bind:this={activePanelRef} {client} {searchQuery} />
			{/if}
		</div>
	</div>
</div>
