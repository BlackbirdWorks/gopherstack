<script lang="ts">
	// MediaPackage has three listable families: Channels, Origin Endpoints,
	// and Harvest Jobs. Channels and Origin Endpoints get full CRUD plus
	// their real lifecycle actions (ConfigureLogs, RotateChannelCredentials,
	// RotateIngestEndpointCredentials). Harvest Jobs have no Update or
	// Delete operation in the real API at all (confirmed against the SDK's
	// command list) -- once submitted a harvest job can only be described,
	// so that tab is create + read-only detail, no edit/delete affordance.
	//
	// Packaging protocol config (HlsPackage/DashPackage/CmafPackage/
	// MssPackage/Authorization) is modeled here as raw JSON, matching the
	// real backend's own storage: PARITY.md documents these as an opaque
	// map[string]any passthrough with no semantic validation, so a
	// structured sub-form would imply validation the emulator doesn't do.
	import { untrack } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMediaPackageClient } from '$lib/aws-client';
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
		ConfigureLogsCommand,
		RotateChannelCredentialsCommand,
		RotateIngestEndpointCredentialsCommand,
		ListOriginEndpointsCommand,
		DescribeOriginEndpointCommand,
		CreateOriginEndpointCommand,
		UpdateOriginEndpointCommand,
		DeleteOriginEndpointCommand,
		ListHarvestJobsCommand,
		DescribeHarvestJobCommand,
		CreateHarvestJobCommand,
		type Channel,
		type OriginEndpoint,
		type HarvestJob
	} from '@aws-sdk/client-mediapackage';
	import { toast } from 'svelte-sonner';
	import { Package, Plus, Trash2, Eye, Pencil, FileText, KeyRound } from 'lucide-svelte';

	const client = regionalClient(getMediaPackageClient);

	type TabId = 'channels' | 'endpoints' | 'harvest';

	const tabs: TabDef[] = [
		{ id: 'channels', label: 'Channels' },
		{ id: 'endpoints', label: 'Origin Endpoints' },
		{ id: 'harvest', label: 'Harvest Jobs' }
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

	const activeStatuses = new Set(['SUCCEEDED']);
	function statusClass(s: unknown): string {
		if (s === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return activeStatuses.has(String(s))
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('channels');
	let searchQuery = $state('');

	// --- Channels ---
	let channels = $state<Channel[]>([]);
	async function fetchChannels(): Promise<void> {
		const resp = await client().send(new ListChannelsCommand({}));
		channels = resp.Channels ?? [];
	}

	// --- Origin Endpoints ---
	let endpoints = $state<OriginEndpoint[]>([]);
	async function fetchEndpoints(): Promise<void> {
		const resp = await client().send(new ListOriginEndpointsCommand({}));
		endpoints = resp.OriginEndpoints ?? [];
	}

	// --- Harvest Jobs ---
	let harvestJobs = $state<HarvestJob[]>([]);
	async function fetchHarvestJobs(): Promise<void> {
		const resp = await client().send(new ListHarvestJobsCommand({}));
		harvestJobs = resp.HarvestJobs ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		channels: () => fetchChannels().catch(rethrowDescribed),
		endpoints: () => fetchEndpoints().catch(rethrowDescribed),
		harvest: () => fetchHarvestJobs().catch(rethrowDescribed)
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
		viewedChannel = null;
		viewedEndpoint = null;
		viewedHarvestJob = null;
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredChannels = $derived(
		channels.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (c.Id ?? '').toLowerCase().includes(q) || (c.Description ?? '').toLowerCase().includes(q);
		})
	);
	const filteredEndpoints = $derived(
		endpoints.filter((e) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(e.Id ?? '').toLowerCase().includes(q) ||
				(e.ChannelId ?? '').toLowerCase().includes(q) ||
				(e.ManifestName ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredHarvestJobs = $derived(
		harvestJobs.filter((h) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(h.Id ?? '').toLowerCase().includes(q) ||
				(h.ChannelId ?? '').toLowerCase().includes(q) ||
				(h.OriginEndpointId ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ── Channels: create / detail / edit / delete / logs / credential rotation ──
	let createChannelModal = $state<Modal | null>(null);
	let creatingChannel = $state(false);
	let createChannelError = $state<string | null>(null);
	let newChannelId = $state('');
	let newChannelDescription = $state('');

	function openCreateChannelModal(): void {
		createChannelError = null;
		newChannelId = '';
		newChannelDescription = '';
		createChannelModal?.open();
	}

	async function submitCreateChannel(): Promise<void> {
		if (!newChannelId) {
			createChannelError = 'ID is required.';
			return;
		}
		creatingChannel = true;
		createChannelError = null;
		try {
			await client().send(
				new CreateChannelCommand({ Id: newChannelId, Description: newChannelDescription || undefined })
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
	let viewedChannel = $state<Channel | null>(null);
	let channelDetailLoading = $state(false);
	let channelDetailError = $state<string | null>(null);
	let channelActionError = $state<string | null>(null);

	async function openChannelDetail(c: Channel): Promise<void> {
		viewedChannel = null;
		channelDetailError = null;
		channelActionError = null;
		channelDetailModal?.open();
		if (!c.Id) return;
		channelDetailLoading = true;
		try {
			const resp = await client().send(new DescribeChannelCommand({ Id: c.Id }));
			viewedChannel = resp;
		} catch (e) {
			channelDetailError = describeError(e);
		} finally {
			channelDetailLoading = false;
		}
	}

	async function refreshChannelDetail(): Promise<void> {
		if (!viewedChannel?.Id) return;
		const resp = await client().send(new DescribeChannelCommand({ Id: viewedChannel.Id }));
		viewedChannel = resp;
	}

	let editChannelModal = $state<Modal | null>(null);
	let editingChannel = $state(false);
	let editChannelError = $state<string | null>(null);
	let editChannelId = $state('');
	let editChannelDescription = $state('');

	function openEditChannelModal(c: Channel): void {
		editChannelError = null;
		editChannelId = c.Id ?? '';
		editChannelDescription = c.Description ?? '';
		editChannelModal?.open();
	}

	async function submitEditChannel(): Promise<void> {
		if (!editChannelId) return;
		editingChannel = true;
		editChannelError = null;
		try {
			await client().send(
				new UpdateChannelCommand({ Id: editChannelId, Description: editChannelDescription || undefined })
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

	async function deleteChannel(c: Channel): Promise<void> {
		if (!c.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete channel',
			message: `Delete channel "${c.Id}"? Its origin endpoints will be deleted too.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteChannelCommand({ Id: c.Id }));
			toast.success('Channel deleted');
			channelDetailModal?.close();
			await tabLoader.refresh('channels');
			await tabLoader.refresh('endpoints');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let configureLogsModal = $state<Modal | null>(null);
	let configuringLogs = $state(false);
	let configureLogsError = $state<string | null>(null);
	let egressLogGroup = $state('');
	let ingressLogGroup = $state('');

	function openConfigureLogsModal(): void {
		configureLogsError = null;
		egressLogGroup = viewedChannel?.EgressAccessLogs?.LogGroupName ?? '';
		ingressLogGroup = viewedChannel?.IngressAccessLogs?.LogGroupName ?? '';
		configureLogsModal?.open();
	}

	async function submitConfigureLogs(): Promise<void> {
		if (!viewedChannel?.Id) return;
		configuringLogs = true;
		configureLogsError = null;
		try {
			await client().send(
				new ConfigureLogsCommand({
					Id: viewedChannel.Id,
					EgressAccessLogs: egressLogGroup ? { LogGroupName: egressLogGroup } : undefined,
					IngressAccessLogs: ingressLogGroup ? { LogGroupName: ingressLogGroup } : undefined
				})
			);
			toast.success('Log configuration updated');
			configureLogsModal?.close();
			await refreshChannelDetail();
		} catch (e) {
			const msg = describeError(e);
			configureLogsError = msg;
			toast.error(msg);
		} finally {
			configuringLogs = false;
		}
	}

	async function rotateChannelCredentials(): Promise<void> {
		if (!viewedChannel?.Id) return;
		channelActionError = null;
		try {
			await client().send(new RotateChannelCredentialsCommand({ Id: viewedChannel.Id }));
			toast.success("Channel's first ingest endpoint credentials rotated");
			await refreshChannelDetail();
		} catch (e) {
			const msg = describeError(e);
			channelActionError = msg;
			toast.error(msg);
		}
	}

	async function rotateIngestEndpointCredentials(ingestEndpointId: string | undefined): Promise<void> {
		if (!viewedChannel?.Id || !ingestEndpointId) return;
		channelActionError = null;
		try {
			await client().send(
				new RotateIngestEndpointCredentialsCommand({ Id: viewedChannel.Id, IngestEndpointId: ingestEndpointId })
			);
			toast.success('Ingest endpoint credentials rotated');
			await refreshChannelDetail();
		} catch (e) {
			const msg = describeError(e);
			channelActionError = msg;
			toast.error(msg);
		}
	}

	// ── Origin Endpoints: create / detail / edit / delete ───────────────────
	let createEndpointModal = $state<Modal | null>(null);
	let creatingEndpoint = $state(false);
	let createEndpointError = $state<string | null>(null);
	let newEndpointId = $state('');
	let newEndpointChannelId = $state('');
	let newEndpointDescription = $state('');
	let newEndpointManifestName = $state('');
	let newEndpointHlsPackageJson = $state('');

	function openCreateEndpointModal(): void {
		createEndpointError = null;
		newEndpointId = '';
		newEndpointChannelId = channels[0]?.Id ?? '';
		newEndpointDescription = '';
		newEndpointManifestName = '';
		newEndpointHlsPackageJson = '';
		createEndpointModal?.open();
	}

	function parseHlsPackage(json: string): Record<string, unknown> | undefined {
		if (!json.trim()) return undefined;
		return JSON.parse(json);
	}

	async function submitCreateEndpoint(): Promise<void> {
		if (!newEndpointId || !newEndpointChannelId) {
			createEndpointError = 'ID and Channel ID are required.';
			return;
		}
		let hlsPackage: Record<string, unknown> | undefined;
		try {
			hlsPackage = parseHlsPackage(newEndpointHlsPackageJson);
		} catch {
			createEndpointError = 'HLS Package must be valid JSON.';
			return;
		}
		creatingEndpoint = true;
		createEndpointError = null;
		try {
			await client().send(
				new CreateOriginEndpointCommand({
					Id: newEndpointId,
					ChannelId: newEndpointChannelId,
					Description: newEndpointDescription || undefined,
					ManifestName: newEndpointManifestName || undefined,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					HlsPackage: hlsPackage as any
				})
			);
			toast.success('Origin endpoint created');
			createEndpointModal?.close();
			await tabLoader.refresh('endpoints');
		} catch (e) {
			const msg = describeError(e);
			createEndpointError = msg;
			toast.error(msg);
		} finally {
			creatingEndpoint = false;
		}
	}

	let endpointDetailModal = $state<Modal | null>(null);
	let viewedEndpoint = $state<OriginEndpoint | null>(null);
	let endpointDetailLoading = $state(false);
	let endpointDetailError = $state<string | null>(null);

	async function openEndpointDetail(e: OriginEndpoint): Promise<void> {
		viewedEndpoint = null;
		endpointDetailError = null;
		endpointDetailModal?.open();
		if (!e.Id) return;
		endpointDetailLoading = true;
		try {
			const resp = await client().send(new DescribeOriginEndpointCommand({ Id: e.Id }));
			viewedEndpoint = resp;
		} catch (err) {
			endpointDetailError = describeError(err);
		} finally {
			endpointDetailLoading = false;
		}
	}

	let editEndpointModal = $state<Modal | null>(null);
	let editingEndpoint = $state(false);
	let editEndpointError = $state<string | null>(null);
	let editEndpointId = $state('');
	let editEndpointDescription = $state('');
	let editEndpointManifestName = $state('');
	let editEndpointHlsPackageJson = $state('');

	function openEditEndpointModal(e: OriginEndpoint): void {
		editEndpointError = null;
		editEndpointId = e.Id ?? '';
		editEndpointDescription = e.Description ?? '';
		editEndpointManifestName = e.ManifestName ?? '';
		editEndpointHlsPackageJson = e.HlsPackage ? JSON.stringify(e.HlsPackage, null, 2) : '';
		editEndpointModal?.open();
	}

	async function submitEditEndpoint(): Promise<void> {
		if (!editEndpointId) return;
		let hlsPackage: Record<string, unknown> | undefined;
		try {
			hlsPackage = parseHlsPackage(editEndpointHlsPackageJson);
		} catch {
			editEndpointError = 'HLS Package must be valid JSON.';
			return;
		}
		editingEndpoint = true;
		editEndpointError = null;
		try {
			await client().send(
				new UpdateOriginEndpointCommand({
					Id: editEndpointId,
					Description: editEndpointDescription || undefined,
					ManifestName: editEndpointManifestName || undefined,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					HlsPackage: hlsPackage as any
				})
			);
			toast.success('Origin endpoint updated');
			editEndpointModal?.close();
			await tabLoader.refresh('endpoints');
			const resp = await client().send(new DescribeOriginEndpointCommand({ Id: editEndpointId }));
			viewedEndpoint = resp;
		} catch (e) {
			const msg = describeError(e);
			editEndpointError = msg;
			toast.error(msg);
		} finally {
			editingEndpoint = false;
		}
	}

	async function deleteEndpoint(e: OriginEndpoint): Promise<void> {
		if (!e.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete origin endpoint',
			message: `Delete origin endpoint "${e.Id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteOriginEndpointCommand({ Id: e.Id }));
			toast.success('Origin endpoint deleted');
			endpointDetailModal?.close();
			await tabLoader.refresh('endpoints');
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	// ── Harvest Jobs: create / detail (no update/delete op exists) ─────────
	let createHarvestModal = $state<Modal | null>(null);
	let creatingHarvest = $state(false);
	let createHarvestError = $state<string | null>(null);
	let newHarvestId = $state('');
	let newHarvestOriginEndpointId = $state('');
	let newHarvestStartTime = $state('');
	let newHarvestEndTime = $state('');
	let newHarvestBucketName = $state('');
	let newHarvestManifestKey = $state('');
	let newHarvestRoleArn = $state('');

	function openCreateHarvestModal(): void {
		createHarvestError = null;
		newHarvestId = '';
		newHarvestOriginEndpointId = endpoints[0]?.Id ?? '';
		newHarvestStartTime = '';
		newHarvestEndTime = '';
		newHarvestBucketName = '';
		newHarvestManifestKey = '';
		newHarvestRoleArn = '';
		createHarvestModal?.open();
	}

	async function submitCreateHarvest(): Promise<void> {
		if (
			!newHarvestId ||
			!newHarvestOriginEndpointId ||
			!newHarvestStartTime ||
			!newHarvestEndTime ||
			!newHarvestBucketName ||
			!newHarvestManifestKey ||
			!newHarvestRoleArn
		) {
			createHarvestError = 'All fields are required.';
			return;
		}
		creatingHarvest = true;
		createHarvestError = null;
		try {
			await client().send(
				new CreateHarvestJobCommand({
					Id: newHarvestId,
					OriginEndpointId: newHarvestOriginEndpointId,
					StartTime: newHarvestStartTime,
					EndTime: newHarvestEndTime,
					S3Destination: {
						BucketName: newHarvestBucketName,
						ManifestKey: newHarvestManifestKey,
						RoleArn: newHarvestRoleArn
					}
				})
			);
			toast.success('Harvest job created');
			createHarvestModal?.close();
			await tabLoader.refresh('harvest');
		} catch (e) {
			const msg = describeError(e);
			createHarvestError = msg;
			toast.error(msg);
		} finally {
			creatingHarvest = false;
		}
	}

	let harvestDetailModal = $state<Modal | null>(null);
	let viewedHarvestJob = $state<HarvestJob | null>(null);
	let harvestDetailLoading = $state(false);
	let harvestDetailError = $state<string | null>(null);

	async function openHarvestDetail(h: HarvestJob): Promise<void> {
		viewedHarvestJob = null;
		harvestDetailError = null;
		harvestDetailModal?.open();
		if (!h.Id) return;
		harvestDetailLoading = true;
		try {
			const resp = await client().send(new DescribeHarvestJobCommand({ Id: h.Id }));
			viewedHarvestJob = resp;
		} catch (e) {
			harvestDetailError = describeError(e);
		} finally {
			harvestDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Package}
		title="AWS Elemental MediaPackage"
		description="Video origination and packaging"
		onRefresh={handleRefresh}
		color="pink"
	>
		{#snippet actions()}
			{#if activeTab === 'channels'}
				<button onclick={openCreateChannelModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-pink-600 text-white hover:bg-pink-700 text-sm">
					<Plus class="w-4 h-4" /> Create channel
				</button>
			{:else if activeTab === 'endpoints'}
				<button onclick={openCreateEndpointModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-pink-600 text-white hover:bg-pink-700 text-sm">
					<Plus class="w-4 h-4" /> Create origin endpoint
				</button>
			{:else if activeTab === 'harvest'}
				<button onclick={openCreateHarvestModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-pink-600 text-white hover:bg-pink-700 text-sm">
					<Plus class="w-4 h-4" /> Create harvest job
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="pink" />
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
				{#snippet channelActionsCell(c: Channel)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openChannelDetail(c)} title="View" aria-label="View channel {c.Id}" class="text-gray-400 hover:text-pink-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteChannel(c)} title="Delete" aria-label="Delete channel {c.Id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const channelColumns = defineColumns<Channel>([
					{ key: 'Id', label: 'ID' },
					{ key: 'Description', label: 'Description' },
					{ key: 'Arn', label: 'ARN' },
					{ key: 'actions', label: '', render: channelActionsCell }
				])}
				<DataTable
					rows={filteredChannels}
					rowKey={(c) => c.Id ?? ''}
					columns={channelColumns}
					loading={tabLoader.isLoading('channels')}
					emptyMessage="No channels found"
				/>
			{:else if activeTab === 'endpoints'}
				{#snippet endpointActionsCell(e: OriginEndpoint)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openEndpointDetail(e)} title="View" aria-label="View origin endpoint {e.Id}" class="text-gray-400 hover:text-pink-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteEndpoint(e)} title="Delete" aria-label="Delete origin endpoint {e.Id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const endpointColumns = defineColumns<OriginEndpoint>([
					{ key: 'Id', label: 'ID' },
					{ key: 'ChannelId', label: 'Channel' },
					{ key: 'ManifestName', label: 'Manifest' },
					{ key: 'Url', label: 'URL' },
					{ key: 'actions', label: '', render: endpointActionsCell }
				])}
				<DataTable
					rows={filteredEndpoints}
					rowKey={(e) => e.Id ?? ''}
					columns={endpointColumns}
					loading={tabLoader.isLoading('endpoints')}
					emptyMessage="No origin endpoints found"
				/>
			{:else if activeTab === 'harvest'}
				{#snippet harvestStatusCell(h: HarvestJob)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(h.Status)}">{h.Status ?? '—'}</span>
				{/snippet}
				{#snippet harvestActionsCell(h: HarvestJob)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openHarvestDetail(h)} title="View" aria-label="View harvest job {h.Id}" class="text-gray-400 hover:text-pink-500"><Eye class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const harvestColumns = defineColumns<HarvestJob>([
					{ key: 'Id', label: 'ID' },
					{ key: 'ChannelId', label: 'Channel' },
					{ key: 'OriginEndpointId', label: 'Origin Endpoint' },
					{ key: 'Status', label: 'Status', render: harvestStatusCell },
					{ key: 'actions', label: '', render: harvestActionsCell }
				])}
				<DataTable
					rows={filteredHarvestJobs}
					rowKey={(h) => h.Id ?? ''}
					columns={harvestColumns}
					loading={tabLoader.isLoading('harvest')}
					emptyMessage="No harvest jobs found"
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
				<label for="ch-id" class="text-sm text-slate-600 dark:text-slate-300">ID</label>
				<input id="ch-id" bind:value={newChannelId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ch-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="ch-description" bind:value={newChannelDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createChannelError}
				<p class="text-sm text-red-600 dark:text-red-400">{createChannelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createChannelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateChannel} disabled={creatingChannel} class="rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700 disabled:opacity-50">{creatingChannel ? 'Creating…' : 'Create'}</button>
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
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedChannel.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.Description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedChannel.CreatedAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Egress log group</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.EgressAccessLogs?.LogGroupName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Ingress log group</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.IngressAccessLogs?.LogGroupName ?? '—'}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Ingest endpoints</dt>
					<dd class="text-slate-900 dark:text-white">
						{#if (viewedChannel.HlsIngest?.IngestEndpoints ?? []).length === 0}
							<span class="text-slate-500 dark:text-slate-400">None</span>
						{:else}
							<ul class="space-y-2">
								{#each viewedChannel.HlsIngest?.IngestEndpoints ?? [] as ie (ie.Id)}
									<li class="flex items-center justify-between gap-2">
										<span class="font-mono text-xs break-all">{ie.Id}: {ie.Url ?? '—'}</span>
										<button onclick={() => rotateIngestEndpointCredentials(ie.Id)} title="Rotate credentials" aria-label="Rotate credentials for ingest endpoint {ie.Id}" class="shrink-0 text-gray-400 hover:text-pink-500"><KeyRound class="w-4 h-4" /></button>
									</li>
								{/each}
							</ul>
						{/if}
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
			<button type="button" onclick={openConfigureLogsModal} class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><FileText class="w-4 h-4" /> Configure logs</button>
			<button type="button" onclick={rotateChannelCredentials} title="Deprecated: rotates only the first ingest endpoint" class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><KeyRound class="w-4 h-4" /> Rotate credentials</button>
			<button type="button" onclick={() => viewedChannel && openEditChannelModal(viewedChannel)} class="flex items-center gap-2 rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedChannel && deleteChannel(viewedChannel)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Channel -->
<Modal bind:this={editChannelModal} title="Edit Channel">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ch-edit-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="ch-edit-description" bind:value={editChannelDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editChannelError}
				<p class="text-sm text-red-600 dark:text-red-400">{editChannelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editChannelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditChannel} disabled={editingChannel} class="rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700 disabled:opacity-50">{editingChannel ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Configure Logs -->
<Modal bind:this={configureLogsModal} title="Configure Logs">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="egress-log-group" class="text-sm text-slate-600 dark:text-slate-300">Egress log group name</label>
				<input id="egress-log-group" bind:value={egressLogGroup} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ingress-log-group" class="text-sm text-slate-600 dark:text-slate-300">Ingress log group name</label>
				<input id="ingress-log-group" bind:value={ingressLogGroup} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if configureLogsError}
				<p class="text-sm text-red-600 dark:text-red-400">{configureLogsError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => configureLogsModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitConfigureLogs} disabled={configuringLogs} class="rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700 disabled:opacity-50">{configuringLogs ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Origin Endpoint -->
<Modal bind:this={createEndpointModal} title="Create Origin Endpoint">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ep-id" class="text-sm text-slate-600 dark:text-slate-300">ID</label>
				<input id="ep-id" bind:value={newEndpointId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-channel" class="text-sm text-slate-600 dark:text-slate-300">Channel ID</label>
				<input id="ep-channel" bind:value={newEndpointChannelId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="ep-description" bind:value={newEndpointDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-manifest" class="text-sm text-slate-600 dark:text-slate-300">Manifest name</label>
				<input id="ep-manifest" bind:value={newEndpointManifestName} placeholder="index" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-hls" class="text-sm text-slate-600 dark:text-slate-300">HLS package (JSON, optional)</label>
				<textarea id="ep-hls" bind:value={newEndpointHlsPackageJson} rows={4} placeholder={'{\n  "SegmentDurationSeconds": 6\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createEndpointError}
				<p class="text-sm text-red-600 dark:text-red-400">{createEndpointError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createEndpointModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateEndpoint} disabled={creatingEndpoint} class="rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700 disabled:opacity-50">{creatingEndpoint ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Origin Endpoint detail -->
<Modal bind:this={endpointDetailModal} title="Origin Endpoint">
	{#snippet children()}
		{#if endpointDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if endpointDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{endpointDetailError}</p>
		{:else if viewedEndpoint}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedEndpoint.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Channel</dt><dd class="text-slate-900 dark:text-white">{viewedEndpoint.ChannelId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">URL</dt><dd class="text-slate-900 dark:text-white break-all">{viewedEndpoint.Url ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Manifest name</dt><dd class="text-slate-900 dark:text-white">{viewedEndpoint.ManifestName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedEndpoint.Description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedEndpoint.CreatedAt)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Packaging config</dt>
					<dd class="text-slate-900 dark:text-white">
						<pre class="mt-1 max-h-40 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(
								{
									HlsPackage: viewedEndpoint.HlsPackage,
									DashPackage: viewedEndpoint.DashPackage,
									CmafPackage: viewedEndpoint.CmafPackage,
									MssPackage: viewedEndpoint.MssPackage
								},
								null,
								2
							)}</pre>
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => endpointDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedEndpoint}
			<button type="button" onclick={() => viewedEndpoint && openEditEndpointModal(viewedEndpoint)} class="flex items-center gap-2 rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedEndpoint && deleteEndpoint(viewedEndpoint)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Origin Endpoint -->
<Modal bind:this={editEndpointModal} title="Edit Origin Endpoint">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ep-edit-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="ep-edit-description" bind:value={editEndpointDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-edit-manifest" class="text-sm text-slate-600 dark:text-slate-300">Manifest name</label>
				<input id="ep-edit-manifest" bind:value={editEndpointManifestName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ep-edit-hls" class="text-sm text-slate-600 dark:text-slate-300">HLS package (JSON, optional)</label>
				<textarea id="ep-edit-hls" bind:value={editEndpointHlsPackageJson} rows={4} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if editEndpointError}
				<p class="text-sm text-red-600 dark:text-red-400">{editEndpointError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editEndpointModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditEndpoint} disabled={editingEndpoint} class="rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700 disabled:opacity-50">{editingEndpoint ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Harvest Job -->
<Modal bind:this={createHarvestModal} title="Create Harvest Job">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="hj-id" class="text-sm text-slate-600 dark:text-slate-300">ID</label>
				<input id="hj-id" bind:value={newHarvestId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="hj-endpoint" class="text-sm text-slate-600 dark:text-slate-300">Origin endpoint ID</label>
				<input id="hj-endpoint" bind:value={newHarvestOriginEndpointId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="hj-start" class="text-sm text-slate-600 dark:text-slate-300">Start time (ISO8601)</label>
					<input id="hj-start" bind:value={newHarvestStartTime} placeholder="2026-01-01T00:00:00Z" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="hj-end" class="text-sm text-slate-600 dark:text-slate-300">End time (ISO8601)</label>
					<input id="hj-end" bind:value={newHarvestEndTime} placeholder="2026-01-01T01:00:00Z" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div>
				<label for="hj-bucket" class="text-sm text-slate-600 dark:text-slate-300">S3 bucket name</label>
				<input id="hj-bucket" bind:value={newHarvestBucketName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="hj-manifest-key" class="text-sm text-slate-600 dark:text-slate-300">S3 manifest key</label>
				<input id="hj-manifest-key" bind:value={newHarvestManifestKey} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="hj-role-arn" class="text-sm text-slate-600 dark:text-slate-300">S3 role ARN</label>
				<input id="hj-role-arn" bind:value={newHarvestRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Harvest jobs cannot be edited or deleted once submitted -- the real API has no Update or Delete operation for this resource.</p>
			{#if createHarvestError}
				<p class="text-sm text-red-600 dark:text-red-400">{createHarvestError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createHarvestModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateHarvest} disabled={creatingHarvest} class="rounded-lg bg-pink-600 px-4 py-2 text-sm font-semibold text-white hover:bg-pink-700 disabled:opacity-50">{creatingHarvest ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Harvest Job detail -->
<Modal bind:this={harvestDetailModal} title="Harvest Job">
	{#snippet children()}
		{#if harvestDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if harvestDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{harvestDetailError}</p>
		{:else if viewedHarvestJob}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedHarvestJob.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Channel</dt><dd class="text-slate-900 dark:text-white">{viewedHarvestJob.ChannelId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Origin endpoint</dt><dd class="text-slate-900 dark:text-white">{viewedHarvestJob.OriginEndpointId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedHarvestJob.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Start time</dt><dd class="text-slate-900 dark:text-white">{viewedHarvestJob.StartTime ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">End time</dt><dd class="text-slate-900 dark:text-white">{viewedHarvestJob.EndTime ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedHarvestJob.CreatedAt)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">S3 destination</dt>
					<dd class="text-slate-900 dark:text-white">
						<pre class="mt-1 max-h-40 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedHarvestJob.S3Destination ?? {}, null, 2)}</pre>
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => harvestDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
