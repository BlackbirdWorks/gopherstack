<script lang="ts">
	// MediaTailor's floor keeps the four tabs the read-only baseline already
	// had (Channels, Source Locations, Playback Configs, VOD Sources) and
	// gives each full CRUD. VOD Sources are children of a Source Location
	// (ListVodSources requires SourceLocationName -- there is no
	// "list every VOD source across all locations" op), so that tab keeps
	// the existing source-location filter input driving both the list and
	// the create form's default. Channels have a real Start/Stop lifecycle
	// (ChannelState RUNNING/STOPPED); Delete is rejected by the real API
	// while a channel is RUNNING, so the detail view surfaces Stop
	// alongside Delete rather than only Delete.
	//
	// PutPlaybackConfiguration is AWS's own create-or-replace op (there is
	// no separate Create/Update pair for this resource) -- the Create and
	// Edit modals both call it, matching that shape rather than inventing
	// an Update op that doesn't exist.
	import { untrack } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMediaTailorClient } from '$lib/aws-client';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import {
		ListChannelsCommand,
		DescribeChannelCommand,
		CreateChannelCommand,
		UpdateChannelCommand,
		DeleteChannelCommand,
		StartChannelCommand,
		StopChannelCommand,
		ListSourceLocationsCommand,
		DescribeSourceLocationCommand,
		CreateSourceLocationCommand,
		UpdateSourceLocationCommand,
		DeleteSourceLocationCommand,
		ListPlaybackConfigurationsCommand,
		GetPlaybackConfigurationCommand,
		PutPlaybackConfigurationCommand,
		DeletePlaybackConfigurationCommand,
		ListVodSourcesCommand,
		DescribeVodSourceCommand,
		CreateVodSourceCommand,
		UpdateVodSourceCommand,
		DeleteVodSourceCommand,
		PlaybackMode,
		Tier,
		Type as PackageType,
		type Channel,
		type DescribeChannelResponse,
		type SourceLocation,
		type DescribeSourceLocationResponse,
		type PlaybackConfiguration,
		type GetPlaybackConfigurationResponse,
		type VodSource,
		type DescribeVodSourceResponse
	} from '@aws-sdk/client-mediatailor';
	import { toast } from 'svelte-sonner';
	import { Tv, Plus, Trash2, Eye, Pencil, Play, Square } from 'lucide-svelte';

	const client = regionalClient(getMediaTailorClient);

	type TabId = 'channels' | 'sources' | 'configs' | 'vod';

	const tabs: TabDef[] = [
		{ id: 'channels', label: 'Channels' },
		{ id: 'sources', label: 'Source Locations' },
		{ id: 'configs', label: 'Playback Configs' },
		{ id: 'vod', label: 'VOD Sources' }
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

	function statusClass(s: unknown): string {
		return s === 'RUNNING'
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('channels');
	let searchQuery = $state('');
	let sourceLocationFilter = $state('');

	// --- Channels ---
	let channels = $state<Channel[]>([]);
	async function fetchChannels(): Promise<void> {
		const resp = await client().send(new ListChannelsCommand({}));
		channels = resp.Items ?? [];
	}

	// --- Source Locations ---
	let sourceLocations = $state<SourceLocation[]>([]);
	async function fetchSourceLocations(): Promise<void> {
		const resp = await client().send(new ListSourceLocationsCommand({}));
		sourceLocations = resp.Items ?? [];
	}

	// --- Playback Configs ---
	let playbackConfigs = $state<PlaybackConfiguration[]>([]);
	async function fetchPlaybackConfigs(): Promise<void> {
		const resp = await client().send(new ListPlaybackConfigurationsCommand({}));
		playbackConfigs = resp.Items ?? [];
	}

	// --- VOD Sources (require a source location) ---
	let vodSources = $state<VodSource[]>([]);
	async function fetchVodSources(): Promise<void> {
		const loc = untrack(() => sourceLocationFilter);
		if (!loc) {
			vodSources = [];
			return;
		}
		const resp = await client().send(new ListVodSourcesCommand({ SourceLocationName: loc }));
		vodSources = resp.Items ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		channels: () => fetchChannels().catch(rethrowDescribed),
		sources: () => fetchSourceLocations().catch(rethrowDescribed),
		configs: () => fetchPlaybackConfigs().catch(rethrowDescribed),
		vod: () => fetchVodSources().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}
	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}
	function handleSourceLocationFilterChange(): void {
		tabLoader.refresh('vod');
	}

	onRegionChange(() => {
		viewedChannel = null;
		viewedSourceLocation = null;
		viewedPlaybackConfig = null;
		viewedVodSource = null;
		sourceLocationFilter = '';
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredChannels = $derived(
		channels.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (c.ChannelName ?? '').toLowerCase().includes(q);
		})
	);
	const filteredSourceLocations = $derived(
		sourceLocations.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (s.SourceLocationName ?? '').toLowerCase().includes(q);
		})
	);
	const filteredPlaybackConfigs = $derived(
		playbackConfigs.filter((p) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (p.Name ?? '').toLowerCase().includes(q);
		})
	);
	const filteredVodSources = $derived(
		vodSources.filter((v) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (v.VodSourceName ?? '').toLowerCase().includes(q);
		})
	);

	// ── Channels: create / detail / edit / delete / start / stop ────────────
	let createChannelModal = $state<Modal | null>(null);
	let creatingChannel = $state(false);
	let createChannelError = $state<string | null>(null);
	let newChannelName = $state('');
	let newChannelPlaybackMode = $state<'LINEAR' | 'LOOP'>('LINEAR');
	let newChannelTier = $state<'BASIC' | 'STANDARD'>('BASIC');
	let newChannelManifestName = $state('index');
	let newChannelSourceGroup = $state('default');

	function openCreateChannelModal(): void {
		createChannelError = null;
		newChannelName = '';
		newChannelPlaybackMode = 'LINEAR';
		newChannelTier = 'BASIC';
		newChannelManifestName = 'index';
		newChannelSourceGroup = 'default';
		createChannelModal?.open();
	}

	async function submitCreateChannel(): Promise<void> {
		if (!newChannelName || !newChannelManifestName || !newChannelSourceGroup) {
			createChannelError = 'Name, manifest name, and source group are required.';
			return;
		}
		creatingChannel = true;
		createChannelError = null;
		try {
			await client().send(
				new CreateChannelCommand({
					ChannelName: newChannelName,
					PlaybackMode: newChannelPlaybackMode,
					Tier: newChannelTier,
					Outputs: [{ ManifestName: newChannelManifestName, SourceGroup: newChannelSourceGroup }]
				})
			);
			toast.success('Channel created');
			createChannelModal?.close();
			await tabLoader.refresh('channels');
		} catch (e) {
			const msg = describeError(e);
			createChannelError = msg;
			toast.error(msg);
		} finally {
			creatingChannel = false;
		}
	}

	let channelDetailModal = $state<Modal | null>(null);
	let viewedChannel = $state<DescribeChannelResponse | null>(null);
	let channelDetailLoading = $state(false);
	let channelDetailError = $state<string | null>(null);
	let channelActionError = $state<string | null>(null);

	async function openChannelDetail(c: Channel): Promise<void> {
		viewedChannel = null;
		channelDetailError = null;
		channelActionError = null;
		channelDetailModal?.open();
		if (!c.ChannelName) return;
		channelDetailLoading = true;
		try {
			const resp = await client().send(new DescribeChannelCommand({ ChannelName: c.ChannelName }));
			viewedChannel = resp;
		} catch (e) {
			channelDetailError = describeError(e);
		} finally {
			channelDetailLoading = false;
		}
	}

	async function refreshChannelDetail(): Promise<void> {
		if (!viewedChannel?.ChannelName) return;
		const resp = await client().send(new DescribeChannelCommand({ ChannelName: viewedChannel.ChannelName }));
		viewedChannel = resp;
	}

	let editChannelModal = $state<Modal | null>(null);
	let editingChannel = $state(false);
	let editChannelError = $state<string | null>(null);
	let editChannelName = $state('');
	let editChannelManifestName = $state('');
	let editChannelSourceGroup = $state('');

	function openEditChannelModal(c: DescribeChannelResponse): void {
		editChannelError = null;
		editChannelName = c.ChannelName ?? '';
		editChannelManifestName = c.Outputs?.[0]?.ManifestName ?? 'index';
		editChannelSourceGroup = c.Outputs?.[0]?.SourceGroup ?? 'default';
		editChannelModal?.open();
	}

	async function submitEditChannel(): Promise<void> {
		if (!editChannelName || !editChannelManifestName || !editChannelSourceGroup) {
			editChannelError = 'Manifest name and source group are required.';
			return;
		}
		editingChannel = true;
		editChannelError = null;
		try {
			await client().send(
				new UpdateChannelCommand({
					ChannelName: editChannelName,
					Outputs: [{ ManifestName: editChannelManifestName, SourceGroup: editChannelSourceGroup }]
				})
			);
			toast.success('Channel updated');
			editChannelModal?.close();
			await tabLoader.refresh('channels');
			await refreshChannelDetail();
		} catch (e) {
			const msg = describeError(e);
			editChannelError = msg;
			toast.error(msg);
		} finally {
			editingChannel = false;
		}
	}

	async function startChannel(): Promise<void> {
		if (!viewedChannel?.ChannelName) return;
		channelActionError = null;
		try {
			await client().send(new StartChannelCommand({ ChannelName: viewedChannel.ChannelName }));
			toast.success('Channel started');
			await refreshChannelDetail();
			await tabLoader.refresh('channels');
		} catch (e) {
			const msg = describeError(e);
			channelActionError = msg;
			toast.error(msg);
		}
	}

	async function stopChannel(): Promise<void> {
		if (!viewedChannel?.ChannelName) return;
		channelActionError = null;
		try {
			await client().send(new StopChannelCommand({ ChannelName: viewedChannel.ChannelName }));
			toast.success('Channel stopped');
			await refreshChannelDetail();
			await tabLoader.refresh('channels');
		} catch (e) {
			const msg = describeError(e);
			channelActionError = msg;
			toast.error(msg);
		}
	}

	async function deleteChannel(c: { ChannelName?: string }): Promise<void> {
		if (!c.ChannelName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete channel',
			message: `Delete channel "${c.ChannelName}"? The channel must be stopped first.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteChannelCommand({ ChannelName: c.ChannelName }));
			toast.success('Channel deleted');
			channelDetailModal?.close();
			await tabLoader.refresh('channels');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Source Locations: create / detail / edit / delete ──────────────────
	let createSourceModal = $state<Modal | null>(null);
	let creatingSource = $state(false);
	let createSourceError = $state<string | null>(null);
	let newSourceName = $state('');
	let newSourceBaseUrl = $state('');

	function openCreateSourceModal(): void {
		createSourceError = null;
		newSourceName = '';
		newSourceBaseUrl = '';
		createSourceModal?.open();
	}

	async function submitCreateSource(): Promise<void> {
		if (!newSourceName || !newSourceBaseUrl) {
			createSourceError = 'Name and base URL are required.';
			return;
		}
		creatingSource = true;
		createSourceError = null;
		try {
			await client().send(
				new CreateSourceLocationCommand({
					SourceLocationName: newSourceName,
					HttpConfiguration: { BaseUrl: newSourceBaseUrl }
				})
			);
			toast.success('Source location created');
			createSourceModal?.close();
			await tabLoader.refresh('sources');
		} catch (e) {
			const msg = describeError(e);
			createSourceError = msg;
			toast.error(msg);
		} finally {
			creatingSource = false;
		}
	}

	let sourceDetailModal = $state<Modal | null>(null);
	let viewedSourceLocation = $state<DescribeSourceLocationResponse | null>(null);
	let sourceDetailLoading = $state(false);
	let sourceDetailError = $state<string | null>(null);

	async function openSourceDetail(s: SourceLocation): Promise<void> {
		viewedSourceLocation = null;
		sourceDetailError = null;
		sourceDetailModal?.open();
		if (!s.SourceLocationName) return;
		sourceDetailLoading = true;
		try {
			const resp = await client().send(new DescribeSourceLocationCommand({ SourceLocationName: s.SourceLocationName }));
			viewedSourceLocation = resp;
		} catch (e) {
			sourceDetailError = describeError(e);
		} finally {
			sourceDetailLoading = false;
		}
	}

	let editSourceModal = $state<Modal | null>(null);
	let editingSource = $state(false);
	let editSourceError = $state<string | null>(null);
	let editSourceName = $state('');
	let editSourceBaseUrl = $state('');

	function openEditSourceModal(s: DescribeSourceLocationResponse): void {
		editSourceError = null;
		editSourceName = s.SourceLocationName ?? '';
		editSourceBaseUrl = s.HttpConfiguration?.BaseUrl ?? '';
		editSourceModal?.open();
	}

	async function submitEditSource(): Promise<void> {
		if (!editSourceName || !editSourceBaseUrl) {
			editSourceError = 'Base URL is required.';
			return;
		}
		editingSource = true;
		editSourceError = null;
		try {
			await client().send(
				new UpdateSourceLocationCommand({
					SourceLocationName: editSourceName,
					HttpConfiguration: { BaseUrl: editSourceBaseUrl }
				})
			);
			toast.success('Source location updated');
			editSourceModal?.close();
			await tabLoader.refresh('sources');
			const resp = await client().send(new DescribeSourceLocationCommand({ SourceLocationName: editSourceName }));
			viewedSourceLocation = resp;
		} catch (e) {
			const msg = describeError(e);
			editSourceError = msg;
			toast.error(msg);
		} finally {
			editingSource = false;
		}
	}

	async function deleteSource(s: { SourceLocationName?: string }): Promise<void> {
		if (!s.SourceLocationName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete source location',
			message: `Delete source location "${s.SourceLocationName}"? It must have no attached VOD or live sources.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSourceLocationCommand({ SourceLocationName: s.SourceLocationName }));
			toast.success('Source location deleted');
			sourceDetailModal?.close();
			await tabLoader.refresh('sources');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Playback Configs: create/edit both use Put (upsert), plus detail/delete ──
	let createConfigModal = $state<Modal | null>(null);
	let creatingConfig = $state(false);
	let createConfigError = $state<string | null>(null);
	let newConfigName = $state('');
	let newConfigVideoUrl = $state('');
	let newConfigAdDecisionUrl = $state('');

	function openCreateConfigModal(): void {
		createConfigError = null;
		newConfigName = '';
		newConfigVideoUrl = '';
		newConfigAdDecisionUrl = '';
		createConfigModal?.open();
	}

	async function submitCreateConfig(): Promise<void> {
		if (!newConfigName) {
			createConfigError = 'Name is required.';
			return;
		}
		creatingConfig = true;
		createConfigError = null;
		try {
			await client().send(
				new PutPlaybackConfigurationCommand({
					Name: newConfigName,
					VideoContentSourceUrl: newConfigVideoUrl || undefined,
					AdDecisionServerUrl: newConfigAdDecisionUrl || undefined
				})
			);
			toast.success('Playback configuration saved');
			createConfigModal?.close();
			await tabLoader.refresh('configs');
		} catch (e) {
			const msg = describeError(e);
			createConfigError = msg;
			toast.error(msg);
		} finally {
			creatingConfig = false;
		}
	}

	let configDetailModal = $state<Modal | null>(null);
	let viewedPlaybackConfig = $state<GetPlaybackConfigurationResponse | null>(null);
	let configDetailLoading = $state(false);
	let configDetailError = $state<string | null>(null);

	async function openConfigDetail(p: PlaybackConfiguration): Promise<void> {
		viewedPlaybackConfig = null;
		configDetailError = null;
		configDetailModal?.open();
		if (!p.Name) return;
		configDetailLoading = true;
		try {
			const resp = await client().send(new GetPlaybackConfigurationCommand({ Name: p.Name }));
			viewedPlaybackConfig = resp;
		} catch (e) {
			configDetailError = describeError(e);
		} finally {
			configDetailLoading = false;
		}
	}

	let editConfigModal = $state<Modal | null>(null);
	let editingConfig = $state(false);
	let editConfigError = $state<string | null>(null);
	let editConfigName = $state('');
	let editConfigVideoUrl = $state('');
	let editConfigAdDecisionUrl = $state('');

	function openEditConfigModal(p: GetPlaybackConfigurationResponse): void {
		editConfigError = null;
		editConfigName = p.Name ?? '';
		editConfigVideoUrl = p.VideoContentSourceUrl ?? '';
		editConfigAdDecisionUrl = p.AdDecisionServerUrl ?? '';
		editConfigModal?.open();
	}

	async function submitEditConfig(): Promise<void> {
		if (!editConfigName) return;
		editingConfig = true;
		editConfigError = null;
		try {
			await client().send(
				new PutPlaybackConfigurationCommand({
					Name: editConfigName,
					VideoContentSourceUrl: editConfigVideoUrl || undefined,
					AdDecisionServerUrl: editConfigAdDecisionUrl || undefined
				})
			);
			toast.success('Playback configuration updated');
			editConfigModal?.close();
			await tabLoader.refresh('configs');
			const resp = await client().send(new GetPlaybackConfigurationCommand({ Name: editConfigName }));
			viewedPlaybackConfig = resp;
		} catch (e) {
			const msg = describeError(e);
			editConfigError = msg;
			toast.error(msg);
		} finally {
			editingConfig = false;
		}
	}

	async function deleteConfig(p: PlaybackConfiguration): Promise<void> {
		if (!p.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete playback configuration',
			message: `Delete playback configuration "${p.Name}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeletePlaybackConfigurationCommand({ Name: p.Name }));
			toast.success('Playback configuration deleted');
			configDetailModal?.close();
			await tabLoader.refresh('configs');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── VOD Sources: create / detail / edit / delete ────────────────────────
	let createVodModal = $state<Modal | null>(null);
	let creatingVod = $state(false);
	let createVodError = $state<string | null>(null);
	let newVodName = $state('');
	let newVodSourceLocation = $state('');
	let newVodPath = $state('');
	let newVodSourceGroup = $state('default');
	let newVodType = $state<'DASH' | 'HLS'>('HLS');

	function openCreateVodModal(): void {
		createVodError = null;
		newVodName = '';
		newVodSourceLocation = sourceLocationFilter;
		newVodPath = '';
		newVodSourceGroup = 'default';
		newVodType = 'HLS';
		createVodModal?.open();
	}

	async function submitCreateVod(): Promise<void> {
		if (!newVodName || !newVodSourceLocation || !newVodPath || !newVodSourceGroup) {
			createVodError = 'All fields are required.';
			return;
		}
		creatingVod = true;
		createVodError = null;
		try {
			await client().send(
				new CreateVodSourceCommand({
					VodSourceName: newVodName,
					SourceLocationName: newVodSourceLocation,
					HttpPackageConfigurations: [{ Path: newVodPath, SourceGroup: newVodSourceGroup, Type: newVodType }]
				})
			);
			toast.success('VOD source created');
			createVodModal?.close();
			sourceLocationFilter = newVodSourceLocation;
			await tabLoader.refresh('vod');
		} catch (e) {
			const msg = describeError(e);
			createVodError = msg;
			toast.error(msg);
		} finally {
			creatingVod = false;
		}
	}

	let vodDetailModal = $state<Modal | null>(null);
	let viewedVodSource = $state<DescribeVodSourceResponse | null>(null);
	let vodDetailLoading = $state(false);
	let vodDetailError = $state<string | null>(null);

	async function openVodDetail(v: VodSource): Promise<void> {
		viewedVodSource = null;
		vodDetailError = null;
		vodDetailModal?.open();
		if (!v.VodSourceName || !v.SourceLocationName) return;
		vodDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVodSourceCommand({ VodSourceName: v.VodSourceName, SourceLocationName: v.SourceLocationName })
			);
			viewedVodSource = resp;
		} catch (e) {
			vodDetailError = describeError(e);
		} finally {
			vodDetailLoading = false;
		}
	}

	let editVodModal = $state<Modal | null>(null);
	let editingVod = $state(false);
	let editVodError = $state<string | null>(null);
	let editVodName = $state('');
	let editVodSourceLocation = $state('');
	let editVodPath = $state('');
	let editVodSourceGroup = $state('');
	let editVodType = $state<'DASH' | 'HLS'>('HLS');

	function openEditVodModal(v: DescribeVodSourceResponse): void {
		editVodError = null;
		editVodName = v.VodSourceName ?? '';
		editVodSourceLocation = v.SourceLocationName ?? '';
		editVodPath = v.HttpPackageConfigurations?.[0]?.Path ?? '';
		editVodSourceGroup = v.HttpPackageConfigurations?.[0]?.SourceGroup ?? 'default';
		editVodType = (v.HttpPackageConfigurations?.[0]?.Type as 'DASH' | 'HLS') ?? 'HLS';
		editVodModal?.open();
	}

	async function submitEditVod(): Promise<void> {
		if (!editVodName || !editVodSourceLocation || !editVodPath || !editVodSourceGroup) {
			editVodError = 'All fields are required.';
			return;
		}
		editingVod = true;
		editVodError = null;
		try {
			await client().send(
				new UpdateVodSourceCommand({
					VodSourceName: editVodName,
					SourceLocationName: editVodSourceLocation,
					HttpPackageConfigurations: [{ Path: editVodPath, SourceGroup: editVodSourceGroup, Type: editVodType }]
				})
			);
			toast.success('VOD source updated');
			editVodModal?.close();
			await tabLoader.refresh('vod');
			const resp = await client().send(
				new DescribeVodSourceCommand({ VodSourceName: editVodName, SourceLocationName: editVodSourceLocation })
			);
			viewedVodSource = resp;
		} catch (e) {
			const msg = describeError(e);
			editVodError = msg;
			toast.error(msg);
		} finally {
			editingVod = false;
		}
	}

	async function deleteVod(v: { VodSourceName?: string; SourceLocationName?: string }): Promise<void> {
		if (!v.VodSourceName || !v.SourceLocationName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete VOD source',
			message: `Delete VOD source "${v.VodSourceName}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteVodSourceCommand({ VodSourceName: v.VodSourceName, SourceLocationName: v.SourceLocationName })
			);
			toast.success('VOD source deleted');
			vodDetailModal?.close();
			await tabLoader.refresh('vod');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Tv}
		title="AWS Elemental MediaTailor"
		description="Personalized ad insertion"
		onRefresh={handleRefresh}
		color="fuchsia"
	>
		{#snippet actions()}
			{#if activeTab === 'channels'}
				<button onclick={openCreateChannelModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-fuchsia-600 text-white hover:bg-fuchsia-700 text-sm">
					<Plus class="w-4 h-4" /> Create channel
				</button>
			{:else if activeTab === 'sources'}
				<button onclick={openCreateSourceModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-fuchsia-600 text-white hover:bg-fuchsia-700 text-sm">
					<Plus class="w-4 h-4" /> Create source location
				</button>
			{:else if activeTab === 'configs'}
				<button onclick={openCreateConfigModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-fuchsia-600 text-white hover:bg-fuchsia-700 text-sm">
					<Plus class="w-4 h-4" /> Create playback config
				</button>
			{:else if activeTab === 'vod'}
				<button onclick={openCreateVodModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-fuchsia-600 text-white hover:bg-fuchsia-700 text-sm">
					<Plus class="w-4 h-4" /> Create VOD source
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	{#if activeTab === 'vod'}
		<div class="flex items-center gap-2">
			<label for="source-location-filter" class="text-xs text-gray-500 dark:text-gray-400">Source location</label>
			<input
				id="source-location-filter"
				bind:value={sourceLocationFilter}
				onchange={handleSourceLocationFilterChange}
				placeholder="my-source-location"
				class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-56"
			/>
			<span class="text-xs text-gray-500 dark:text-gray-400">VOD sources are listed per source location -- there is no cross-location list.</span>
		</div>
	{/if}

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="fuchsia" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'channels'}
				{#snippet channelStateCell(c: Channel)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(c.ChannelState)}">{c.ChannelState ?? '—'}</span>
				{/snippet}
				{#snippet channelActionsCell(c: Channel)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openChannelDetail(c)} title="View" aria-label="View channel {c.ChannelName}" class="text-gray-400 hover:text-fuchsia-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteChannel(c)} title="Delete" aria-label="Delete channel {c.ChannelName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const channelColumns = defineColumns<Channel>([
					{ key: 'ChannelName', label: 'Name' },
					{ key: 'PlaybackMode', label: 'Playback Mode' },
					{ key: 'Tier', label: 'Tier' },
					{ key: 'ChannelState', label: 'State', render: channelStateCell },
					{ key: 'actions', label: '', render: channelActionsCell }
				])}
				<DataTable
					rows={filteredChannels}
					rowKey={(c) => c.ChannelName ?? ''}
					columns={channelColumns}
					loading={tabLoader.isLoading('channels')}
					emptyMessage="No channels found"
				/>
			{:else if activeTab === 'sources'}
				{#snippet sourceActionsCell(s: SourceLocation)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSourceDetail(s)} title="View" aria-label="View source location {s.SourceLocationName}" class="text-gray-400 hover:text-fuchsia-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteSource(s)} title="Delete" aria-label="Delete source location {s.SourceLocationName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const sourceColumns = defineColumns<SourceLocation>([
					{ key: 'SourceLocationName', label: 'Name' },
					{ key: 'Arn', label: 'ARN' },
					{ key: 'actions', label: '', render: sourceActionsCell }
				])}
				<DataTable
					rows={filteredSourceLocations}
					rowKey={(s) => s.SourceLocationName ?? ''}
					columns={sourceColumns}
					loading={tabLoader.isLoading('sources')}
					emptyMessage="No source locations found"
				/>
			{:else if activeTab === 'configs'}
				{#snippet configActionsCell(p: PlaybackConfiguration)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openConfigDetail(p)} title="View" aria-label="View playback config {p.Name}" class="text-gray-400 hover:text-fuchsia-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteConfig(p)} title="Delete" aria-label="Delete playback config {p.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const configColumns = defineColumns<PlaybackConfiguration>([
					{ key: 'Name', label: 'Name' },
					{ key: 'PlaybackConfigurationArn', label: 'ARN' },
					{ key: 'actions', label: '', render: configActionsCell }
				])}
				<DataTable
					rows={filteredPlaybackConfigs}
					rowKey={(p) => p.Name ?? ''}
					columns={configColumns}
					loading={tabLoader.isLoading('configs')}
					emptyMessage="No playback configs found"
				/>
			{:else if activeTab === 'vod'}
				{#snippet vodActionsCell(v: VodSource)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openVodDetail(v)} title="View" aria-label="View VOD source {v.VodSourceName}" class="text-gray-400 hover:text-fuchsia-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteVod(v)} title="Delete" aria-label="Delete VOD source {v.VodSourceName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const vodColumns = defineColumns<VodSource>([
					{ key: 'VodSourceName', label: 'Name' },
					{ key: 'SourceLocationName', label: 'Source Location' },
					{ key: 'actions', label: '', render: vodActionsCell }
				])}
				<DataTable
					rows={filteredVodSources}
					rowKey={(v) => v.VodSourceName ?? ''}
					columns={vodColumns}
					loading={tabLoader.isLoading('vod')}
					emptyMessage={sourceLocationFilter ? 'No VOD sources found' : 'Enter a source location above to list its VOD sources'}
				/>
			{/if}
		</div>
	</div>
</div>

<!-- Create Channel -->
<Modal bind:this={createChannelModal} title="Create Channel">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mt-ch-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="mt-ch-name" bind:value={newChannelName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="mt-ch-mode" class="text-sm text-slate-600 dark:text-slate-300">Playback mode</label>
					<select id="mt-ch-mode" bind:value={newChannelPlaybackMode} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value={PlaybackMode.LINEAR}>LINEAR</option>
						<option value={PlaybackMode.LOOP}>LOOP</option>
					</select>
				</div>
				<div>
					<label for="mt-ch-tier" class="text-sm text-slate-600 dark:text-slate-300">Tier</label>
					<select id="mt-ch-tier" bind:value={newChannelTier} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value={Tier.BASIC}>BASIC</option>
						<option value={Tier.STANDARD}>STANDARD</option>
					</select>
				</div>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="mt-ch-manifest" class="text-sm text-slate-600 dark:text-slate-300">Output manifest name</label>
					<input id="mt-ch-manifest" bind:value={newChannelManifestName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="mt-ch-sourcegroup" class="text-sm text-slate-600 dark:text-slate-300">Output source group</label>
					<input id="mt-ch-sourcegroup" bind:value={newChannelSourceGroup} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if createChannelError}
				<p class="text-sm text-red-600 dark:text-red-400">{createChannelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createChannelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateChannel} disabled={creatingChannel} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{creatingChannel ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Channel detail -->
<Modal bind:this={channelDetailModal} title="Channel">
	{#snippet children()}
		{#if channelDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if channelDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{channelDetailError}</p>
		{:else if viewedChannel}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.ChannelName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedChannel.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.ChannelState ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Playback mode</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.PlaybackMode ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Tier</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.Tier ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedChannel.CreationTime)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Outputs</dt>
					<dd class="text-slate-900 dark:text-white">
						{#each viewedChannel.Outputs ?? [] as o (o.ManifestName)}
							<div class="break-all">{o.ManifestName} ({o.SourceGroup}): {o.PlaybackUrl}</div>
						{/each}
					</dd>
				</div>
				{#if channelActionError}
					<p class="text-sm text-red-600 dark:text-red-400">{channelActionError}</p>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => channelDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedChannel}
			{#if viewedChannel.ChannelState === 'STOPPED'}
				<button type="button" onclick={startChannel} class="flex items-center gap-2 rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white hover:bg-green-700"><Play class="w-4 h-4" /> Start</button>
			{:else if viewedChannel.ChannelState === 'RUNNING'}
				<button type="button" onclick={stopChannel} class="flex items-center gap-2 rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700"><Square class="w-4 h-4" /> Stop</button>
			{/if}
			<button type="button" onclick={() => viewedChannel && openEditChannelModal(viewedChannel)} class="flex items-center gap-2 rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedChannel && deleteChannel(viewedChannel)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Channel -->
<Modal bind:this={editChannelModal} title="Edit Channel">
	{#snippet children()}
		<div class="space-y-3">
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="mt-ch-edit-manifest" class="text-sm text-slate-600 dark:text-slate-300">Output manifest name</label>
					<input id="mt-ch-edit-manifest" bind:value={editChannelManifestName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="mt-ch-edit-sourcegroup" class="text-sm text-slate-600 dark:text-slate-300">Output source group</label>
					<input id="mt-ch-edit-sourcegroup" bind:value={editChannelSourceGroup} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if editChannelError}
				<p class="text-sm text-red-600 dark:text-red-400">{editChannelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editChannelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditChannel} disabled={editingChannel} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{editingChannel ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Source Location -->
<Modal bind:this={createSourceModal} title="Create Source Location">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mt-src-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="mt-src-name" bind:value={newSourceName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mt-src-baseurl" class="text-sm text-slate-600 dark:text-slate-300">Base URL</label>
				<input id="mt-src-baseurl" bind:value={newSourceBaseUrl} placeholder="https://example.com/media/" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createSourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createSourceModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateSource} disabled={creatingSource} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{creatingSource ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Source Location detail -->
<Modal bind:this={sourceDetailModal} title="Source Location">
	{#snippet children()}
		{#if sourceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if sourceDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{sourceDetailError}</p>
		{:else if viewedSourceLocation}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedSourceLocation.SourceLocationName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedSourceLocation.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Base URL</dt><dd class="text-slate-900 dark:text-white break-all">{viewedSourceLocation.HttpConfiguration?.BaseUrl ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedSourceLocation.CreationTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => sourceDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedSourceLocation}
			<button type="button" onclick={() => viewedSourceLocation && openEditSourceModal(viewedSourceLocation)} class="flex items-center gap-2 rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedSourceLocation && deleteSource(viewedSourceLocation)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Source Location -->
<Modal bind:this={editSourceModal} title="Edit Source Location">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mt-src-edit-baseurl" class="text-sm text-slate-600 dark:text-slate-300">Base URL</label>
				<input id="mt-src-edit-baseurl" bind:value={editSourceBaseUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editSourceError}
				<p class="text-sm text-red-600 dark:text-red-400">{editSourceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editSourceModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditSource} disabled={editingSource} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{editingSource ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Playback Config -->
<Modal bind:this={createConfigModal} title="Create Playback Configuration">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mt-cfg-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="mt-cfg-name" bind:value={newConfigName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mt-cfg-video" class="text-sm text-slate-600 dark:text-slate-300">Video content source URL</label>
				<input id="mt-cfg-video" bind:value={newConfigVideoUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mt-cfg-ads" class="text-sm text-slate-600 dark:text-slate-300">Ad decision server URL</label>
				<input id="mt-cfg-ads" bind:value={newConfigAdDecisionUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createConfigError}
				<p class="text-sm text-red-600 dark:text-red-400">{createConfigError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createConfigModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateConfig} disabled={creatingConfig} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{creatingConfig ? 'Saving…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Playback Config detail -->
<Modal bind:this={configDetailModal} title="Playback Configuration">
	{#snippet children()}
		{#if configDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if configDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{configDetailError}</p>
		{:else if viewedPlaybackConfig}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedPlaybackConfig.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedPlaybackConfig.PlaybackConfigurationArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Video content source URL</dt><dd class="text-slate-900 dark:text-white break-all">{viewedPlaybackConfig.VideoContentSourceUrl ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Ad decision server URL</dt><dd class="text-slate-900 dark:text-white break-all">{viewedPlaybackConfig.AdDecisionServerUrl ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Playback endpoint prefix</dt><dd class="text-slate-900 dark:text-white break-all">{viewedPlaybackConfig.PlaybackEndpointPrefix ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => configDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedPlaybackConfig}
			<button type="button" onclick={() => viewedPlaybackConfig && openEditConfigModal(viewedPlaybackConfig)} class="flex items-center gap-2 rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedPlaybackConfig && deleteConfig(viewedPlaybackConfig)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Playback Config -->
<Modal bind:this={editConfigModal} title="Edit Playback Configuration">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mt-cfg-edit-video" class="text-sm text-slate-600 dark:text-slate-300">Video content source URL</label>
				<input id="mt-cfg-edit-video" bind:value={editConfigVideoUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mt-cfg-edit-ads" class="text-sm text-slate-600 dark:text-slate-300">Ad decision server URL</label>
				<input id="mt-cfg-edit-ads" bind:value={editConfigAdDecisionUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Playback configurations have no separate update operation -- saving re-issues PutPlaybackConfiguration for this name, the same call Create uses.</p>
			{#if editConfigError}
				<p class="text-sm text-red-600 dark:text-red-400">{editConfigError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editConfigModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditConfig} disabled={editingConfig} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{editingConfig ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create VOD Source -->
<Modal bind:this={createVodModal} title="Create VOD Source">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mt-vod-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="mt-vod-name" bind:value={newVodName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mt-vod-source-location" class="text-sm text-slate-600 dark:text-slate-300">Source location</label>
				<input id="mt-vod-source-location" bind:value={newVodSourceLocation} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mt-vod-path" class="text-sm text-slate-600 dark:text-slate-300">Path (relative to source location base URL)</label>
				<input id="mt-vod-path" bind:value={newVodPath} placeholder="/media/asset.m3u8" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="mt-vod-sourcegroup" class="text-sm text-slate-600 dark:text-slate-300">Source group</label>
					<input id="mt-vod-sourcegroup" bind:value={newVodSourceGroup} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="mt-vod-type" class="text-sm text-slate-600 dark:text-slate-300">Package type</label>
					<select id="mt-vod-type" bind:value={newVodType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value={PackageType.HLS}>HLS</option>
						<option value={PackageType.DASH}>DASH</option>
					</select>
				</div>
			</div>
			{#if createVodError}
				<p class="text-sm text-red-600 dark:text-red-400">{createVodError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createVodModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateVod} disabled={creatingVod} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{creatingVod ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- VOD Source detail -->
<Modal bind:this={vodDetailModal} title="VOD Source">
	{#snippet children()}
		{#if vodDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if vodDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{vodDetailError}</p>
		{:else if viewedVodSource}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedVodSource.VodSourceName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Source location</dt><dd class="text-slate-900 dark:text-white">{viewedVodSource.SourceLocationName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedVodSource.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedVodSource.CreationTime)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Package configurations</dt>
					<dd class="text-slate-900 dark:text-white">
						{#each viewedVodSource.HttpPackageConfigurations ?? [] as h (h.Path)}
							<div>{h.Type} · {h.SourceGroup} · {h.Path}</div>
						{/each}
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => vodDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedVodSource}
			<button type="button" onclick={() => viewedVodSource && openEditVodModal(viewedVodSource)} class="flex items-center gap-2 rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedVodSource && deleteVod(viewedVodSource)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit VOD Source -->
<Modal bind:this={editVodModal} title="Edit VOD Source">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mt-vod-edit-path" class="text-sm text-slate-600 dark:text-slate-300">Path (relative to source location base URL)</label>
				<input id="mt-vod-edit-path" bind:value={editVodPath} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="mt-vod-edit-sourcegroup" class="text-sm text-slate-600 dark:text-slate-300">Source group</label>
					<input id="mt-vod-edit-sourcegroup" bind:value={editVodSourceGroup} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="mt-vod-edit-type" class="text-sm text-slate-600 dark:text-slate-300">Package type</label>
					<select id="mt-vod-edit-type" bind:value={editVodType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value={PackageType.HLS}>HLS</option>
						<option value={PackageType.DASH}>DASH</option>
					</select>
				</div>
			</div>
			{#if editVodError}
				<p class="text-sm text-red-600 dark:text-red-400">{editVodError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editVodModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditVod} disabled={editingVod} class="rounded-lg bg-fuchsia-600 px-4 py-2 text-sm font-semibold text-white hover:bg-fuchsia-700 disabled:opacity-50">{editingVod ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
