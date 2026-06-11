<script lang="ts">
	import { onMount } from 'svelte';
	import { getPinpointClient } from '$lib/aws-client';
	import {
		GetAppsCommand,
		GetCampaignsCommand,
		GetSegmentsCommand,
		CreateCampaignCommand,
		DeleteCampaignCommand,
		CreateSegmentCommand,
		DeleteSegmentCommand,
		ListJourneysCommand,
		CreateJourneyCommand,
		DeleteJourneyCommand,
		GetApplicationDateRangeKpiCommand,
		UpdateCampaignCommand,
		type PinpointClient,
		type ApplicationResponse,
		type CampaignResponse,
		type SegmentResponse,
		type JourneyResponse,
		type Frequency
	} from '@aws-sdk/client-pinpoint';
	import { toast } from 'svelte-sonner';
	import { MapPin, RefreshCw, Search, Megaphone, Users, Activity, GitBranch, Plus, Trash2, BarChart2, CalendarClock } from 'lucide-svelte';

	let ppClient: PinpointClient | undefined;
	function pp(): PinpointClient {
		return (ppClient ??= getPinpointClient());
	}

	// ── State ──────────────────────────────────────────────────────────────────
	let loading = $state(false);
	let activeTab = $state<'apps' | 'campaigns' | 'segments' | 'journeys' | 'kpis'>('apps');
	let searchQuery = $state('');
	let apps = $state<ApplicationResponse[]>([]);
	let campaigns = $state<CampaignResponse[]>([]);
	let segments = $state<SegmentResponse[]>([]);
	let journeys = $state<JourneyResponse[]>([]);
	let selectedAppId = $state<string | null>(null);

	// Create-campaign form
	let showCampaignForm = $state(false);
	let newCampaignName = $state('');
	let newCampaignSegId = $state('');

	// Create-segment form
	let showSegmentForm = $state(false);
	let newSegmentName = $state('');

	// Journey builder stub
	let showJourneyForm = $state(false);
	let newJourneyName = $state('');

	// KPI dashboard
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let kpiData = $state<any | null>(null);
	let kpiLoading = $state(false);

	// ── Derived ────────────────────────────────────────────────────────────────
	const q = $derived(searchQuery.toLowerCase());
	const filteredApps = $derived(apps.filter((a) => (a.Name ?? '').toLowerCase().includes(q)));
	const filteredCampaigns = $derived(campaigns.filter((c) => (c.Name ?? '').toLowerCase().includes(q)));
	const filteredSegments = $derived(segments.filter((s) => (s.Name ?? '').toLowerCase().includes(q)));
	const filteredJourneys = $derived(journeys.filter((j) => (j.Name ?? '').toLowerCase().includes(q)));

	// ── Load helpers ───────────────────────────────────────────────────────────
	async function loadData() {
		loading = true;
		try {
			const resp = await pp().send(new GetAppsCommand({}));
			apps = resp.ApplicationsResponse?.Item ?? [];
		} catch (e) {
			toast.error('Failed to load Pinpoint apps: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadAppDetails(appId: string) {
		selectedAppId = appId;
		loading = true;
		try {
			const [campResp, segResp, journeyResp] = await Promise.all([
				pp().send(new GetCampaignsCommand({ ApplicationId: appId })),
				pp().send(new GetSegmentsCommand({ ApplicationId: appId })),
				pp().send(new ListJourneysCommand({ ApplicationId: appId }))
			]);
			campaigns = campResp.CampaignsResponse?.Item ?? [];
			segments = segResp.SegmentsResponse?.Item ?? [];
			journeys = journeyResp.JourneysResponse?.Item ?? [];
			activeTab = 'campaigns';
		} catch (e) {
			toast.error('Failed to load app details: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadKPIs() {
		if (!selectedAppId) {
			toast.error('Select an application first');
			return;
		}
		kpiLoading = true;
		try {
			const end = new Date().toISOString();
			const start = new Date(Date.now() - 30 * 24 * 60 * 60 * 1000).toISOString();
			const resp = await pp().send(
				new GetApplicationDateRangeKpiCommand({
					ApplicationId: selectedAppId,
					KpiName: 'successful-delivery-rate',
					StartTime: new Date(start),
					EndTime: new Date(end)
				})
			);
			kpiData = resp.ApplicationDateRangeKpiResponse ?? null;
			activeTab = 'kpis';
		} catch (e) {
			toast.error('Failed to load KPIs: ' + String(e));
		} finally {
			kpiLoading = false;
		}
	}

	// ── Campaign CRUD ──────────────────────────────────────────────────────────
	async function createCampaign() {
		if (!selectedAppId || !newCampaignName.trim()) return;
		try {
			await pp().send(
				new CreateCampaignCommand({
					ApplicationId: selectedAppId,
					WriteCampaignRequest: {
						Name: newCampaignName.trim(),
						SegmentId: newCampaignSegId.trim() || undefined
					}
				})
			);
			toast.success('Campaign created');
			newCampaignName = '';
			newCampaignSegId = '';
			showCampaignForm = false;
			await loadAppDetails(selectedAppId);
		} catch (e) {
			toast.error('Failed to create campaign: ' + String(e));
		}
	}

	async function deleteCampaign(campaignId: string) {
		if (!selectedAppId) return;
		try {
			await pp().send(new DeleteCampaignCommand({ ApplicationId: selectedAppId, CampaignId: campaignId }));
			toast.success('Campaign deleted');
			campaigns = campaigns.filter((c) => c.Id !== campaignId);
		} catch (e) {
			toast.error('Failed to delete campaign: ' + String(e));
		}
	}

	// ── Campaign schedule editor ────────────────────────────────────────────────
	let showScheduleModal = $state(false);
	let scheduleCampaign = $state<CampaignResponse | null>(null);
	let schedStart = $state('');
	let schedEnd = $state('');
	let schedFrequency = $state<Frequency>('ONCE');
	let savingSchedule = $state(false);
	const frequencyOptions: Frequency[] = ['ONCE', 'HOURLY', 'DAILY', 'WEEKLY', 'MONTHLY'];

	function openScheduleEditor(campaign: CampaignResponse) {
		scheduleCampaign = campaign;
		const sch = campaign.Schedule;
		schedStart = sch?.StartTime && sch.StartTime !== 'IMMEDIATE' ? sch.StartTime.slice(0, 16) : '';
		schedEnd = sch?.EndTime ? sch.EndTime.slice(0, 16) : '';
		schedFrequency = (sch?.Frequency as Frequency) ?? 'ONCE';
		showScheduleModal = true;
	}

	async function saveSchedule() {
		if (!selectedAppId || !scheduleCampaign?.Id) return;
		savingSchedule = true;
		try {
			await pp().send(
				new UpdateCampaignCommand({
					ApplicationId: selectedAppId,
					CampaignId: scheduleCampaign.Id,
					WriteCampaignRequest: {
						Name: scheduleCampaign.Name,
						SegmentId: scheduleCampaign.SegmentId,
						Schedule: {
							StartTime: schedStart ? new Date(schedStart).toISOString() : 'IMMEDIATE',
							EndTime: schedEnd ? new Date(schedEnd).toISOString() : undefined,
							Frequency: schedFrequency
						}
					}
				})
			);
			toast.success('Campaign schedule updated');
			showScheduleModal = false;
			await loadAppDetails(selectedAppId);
		} catch (e) {
			toast.error('Failed to update schedule: ' + String(e));
		} finally {
			savingSchedule = false;
		}
	}

	// ── Segment CRUD ───────────────────────────────────────────────────────────
	async function createSegment() {
		if (!selectedAppId || !newSegmentName.trim()) return;
		try {
			await pp().send(
				new CreateSegmentCommand({
					ApplicationId: selectedAppId,
					WriteSegmentRequest: { Name: newSegmentName.trim() }
				})
			);
			toast.success('Segment created');
			newSegmentName = '';
			showSegmentForm = false;
			await loadAppDetails(selectedAppId);
		} catch (e) {
			toast.error('Failed to create segment: ' + String(e));
		}
	}

	async function deleteSegment(segmentId: string) {
		if (!selectedAppId) return;
		try {
			await pp().send(new DeleteSegmentCommand({ ApplicationId: selectedAppId, SegmentId: segmentId }));
			toast.success('Segment deleted');
			segments = segments.filter((s) => s.Id !== segmentId);
		} catch (e) {
			toast.error('Failed to delete segment: ' + String(e));
		}
	}

	// ── Journey builder stub ───────────────────────────────────────────────────
	async function createJourney() {
		if (!selectedAppId || !newJourneyName.trim()) return;
		try {
			await pp().send(
				new CreateJourneyCommand({
					ApplicationId: selectedAppId,
					WriteJourneyRequest: { Name: newJourneyName.trim() }
				})
			);
			toast.success('Journey created (draft)');
			newJourneyName = '';
			showJourneyForm = false;
			await loadAppDetails(selectedAppId);
		} catch (e) {
			toast.error('Failed to create journey: ' + String(e));
		}
	}

	async function deleteJourney(journeyId: string) {
		if (!selectedAppId) return;
		try {
			await pp().send(new DeleteJourneyCommand({ ApplicationId: selectedAppId, JourneyId: journeyId }));
			toast.success('Journey deleted');
			journeys = journeys.filter((j) => j.Id !== journeyId);
		} catch (e) {
			toast.error('Failed to delete journey: ' + String(e));
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<MapPin class="w-7 h-7 text-pink-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Pinpoint</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Multichannel marketing communications service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			{#if selectedAppId}
				<button
					onclick={loadKPIs}
					disabled={kpiLoading}
					class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
				>
					<BarChart2 class="w-4 h-4" />
					{kpiLoading ? 'Loading…' : 'KPIs'}
				</button>
			{/if}
			<button
				onclick={loadData}
				title="Refresh"
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<!-- KPI Summary Cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg"><MapPin class="w-5 h-5 text-pink-600 dark:text-pink-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{apps.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Megaphone class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{campaigns.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Campaigns</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Users class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{segments.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Segments</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><GitBranch class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{journeys.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Journeys</p></div>
		</div>
	</div>

	<!-- Main Content -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<!-- Tabs + Search -->
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['apps', 'Applications'], ['campaigns', 'Campaigns'], ['segments', 'Segments'], ['journeys', 'Journeys'], ['kpis', 'KPI Dashboard']] as [tab, label]}
					<button
						onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-pink-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}"
					>
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input
					bind:value={searchQuery}
					placeholder="Search..."
					class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64"
				/>
			</div>
		</div>

		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>

			<!-- Applications Tab -->
			{:else if activeTab === 'apps'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<button
								onclick={() => loadAppDetails(app.Id ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left {selectedAppId === app.Id ? 'ring-2 ring-pink-500' : ''}"
							>
								<div class="flex items-center gap-3">
									<MapPin class="w-5 h-5 text-pink-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{app.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{app.Id}</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">Load details →</span>
							</button>
						{/each}
					</div>
				{/if}

			<!-- Campaigns Tab -->
			{:else if activeTab === 'campaigns'}
				<div class="mb-3 flex items-center justify-between">
					<p class="text-sm text-gray-500 dark:text-gray-400">
						{selectedAppId ? `${filteredCampaigns.length} campaigns` : 'Select an app to view campaigns'}
					</p>
					{#if selectedAppId}
						<button
							onclick={() => (showCampaignForm = !showCampaignForm)}
							class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-pink-600 text-white text-sm hover:bg-pink-700"
						>
							<Plus class="w-4 h-4" /> New Campaign
						</button>
					{/if}
				</div>
				{#if showCampaignForm}
					<div class="mb-4 p-4 rounded-lg border border-pink-200 dark:border-pink-800 bg-pink-50 dark:bg-pink-900/20 space-y-2">
						<h3 class="font-medium text-gray-900 dark:text-white text-sm">Create Campaign</h3>
						<input
							bind:value={newCampaignName}
							placeholder="Campaign name *"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<input
							bind:value={newCampaignSegId}
							placeholder="Segment ID (optional)"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<div class="flex gap-2">
							<button
								onclick={createCampaign}
								disabled={!newCampaignName.trim()}
								class="px-3 py-1.5 rounded-lg bg-pink-600 text-white text-sm hover:bg-pink-700 disabled:opacity-50"
							>Create</button>
							<button
								onclick={() => (showCampaignForm = false)}
								class="px-3 py-1.5 rounded-lg bg-gray-200 dark:bg-slate-600 text-gray-700 dark:text-gray-300 text-sm"
							>Cancel</button>
						</div>
					</div>
				{/if}
				{#if filteredCampaigns.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No campaigns found.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredCampaigns as camp}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Megaphone class="w-5 h-5 text-orange-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{camp.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{camp.Id}</p>
										{#if camp.Schedule}
											<p class="text-xs text-gray-400">
												{camp.Schedule.Frequency ?? 'ONCE'}
												{#if camp.Schedule.StartTime && camp.Schedule.StartTime !== 'IMMEDIATE'}· starts {camp.Schedule.StartTime}{/if}
											</p>
										{/if}
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{camp.State?.CampaignStatus}</span>
									<button
										onclick={() => openScheduleEditor(camp)}
										class="text-pink-500 hover:text-pink-700 p-1 rounded"
										title="Edit schedule"
									><CalendarClock class="w-4 h-4" /></button>
									<button
										onclick={() => deleteCampaign(camp.Id ?? '')}
										class="text-red-400 hover:text-red-600 p-1 rounded"
										title="Delete campaign"
									><Trash2 class="w-4 h-4" /></button>
								</div>
							</div>
						{/each}
					</div>
				{/if}

			<!-- Segments Tab -->
			{:else if activeTab === 'segments'}
				<div class="mb-3 flex items-center justify-between">
					<p class="text-sm text-gray-500 dark:text-gray-400">
						{selectedAppId ? `${filteredSegments.length} segments` : 'Select an app to view segments'}
					</p>
					{#if selectedAppId}
						<button
							onclick={() => (showSegmentForm = !showSegmentForm)}
							class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700"
						>
							<Plus class="w-4 h-4" /> New Segment
						</button>
					{/if}
				</div>
				{#if showSegmentForm}
					<div class="mb-4 p-4 rounded-lg border border-blue-200 dark:border-blue-800 bg-blue-50 dark:bg-blue-900/20 space-y-2">
						<h3 class="font-medium text-gray-900 dark:text-white text-sm">Create Segment</h3>
						<input
							bind:value={newSegmentName}
							placeholder="Segment name *"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<div class="flex gap-2">
							<button
								onclick={createSegment}
								disabled={!newSegmentName.trim()}
								class="px-3 py-1.5 rounded-lg bg-blue-600 text-white text-sm hover:bg-blue-700 disabled:opacity-50"
							>Create</button>
							<button
								onclick={() => (showSegmentForm = false)}
								class="px-3 py-1.5 rounded-lg bg-gray-200 dark:bg-slate-600 text-gray-700 dark:text-gray-300 text-sm"
							>Cancel</button>
						</div>
					</div>
				{/if}
				{#if filteredSegments.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No segments found.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSegments as seg}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Users class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{seg.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{seg.SegmentType} · {seg.Id}</p>
									</div>
								</div>
								<button
									onclick={() => deleteSegment(seg.Id ?? '')}
									class="text-red-400 hover:text-red-600 p-1 rounded"
									title="Delete segment"
								><Trash2 class="w-4 h-4" /></button>
							</div>
						{/each}
					</div>
				{/if}

			<!-- Journeys Tab (builder stub) -->
			{:else if activeTab === 'journeys'}
				<div class="mb-3 flex items-center justify-between">
					<p class="text-sm text-gray-500 dark:text-gray-400">
						{selectedAppId ? `${filteredJourneys.length} journeys` : 'Select an app to view journeys'}
					</p>
					{#if selectedAppId}
						<button
							onclick={() => (showJourneyForm = !showJourneyForm)}
							class="flex items-center gap-1 px-3 py-1.5 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700"
						>
							<Plus class="w-4 h-4" /> New Journey
						</button>
					{/if}
				</div>
				{#if showJourneyForm}
					<div class="mb-4 p-4 rounded-lg border border-purple-200 dark:border-purple-800 bg-purple-50 dark:bg-purple-900/20 space-y-2">
						<h3 class="font-medium text-gray-900 dark:text-white text-sm">Create Journey (Draft)</h3>
						<p class="text-xs text-gray-500 dark:text-gray-400">A new journey will be created in DRAFT state. Use the AWS console or SDK to add activities and publish.</p>
						<input
							bind:value={newJourneyName}
							placeholder="Journey name *"
							class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<div class="flex gap-2">
							<button
								onclick={createJourney}
								disabled={!newJourneyName.trim()}
								class="px-3 py-1.5 rounded-lg bg-purple-600 text-white text-sm hover:bg-purple-700 disabled:opacity-50"
							>Create Draft</button>
							<button
								onclick={() => (showJourneyForm = false)}
								class="px-3 py-1.5 rounded-lg bg-gray-200 dark:bg-slate-600 text-gray-700 dark:text-gray-300 text-sm"
							>Cancel</button>
						</div>
					</div>
				{/if}
				{#if filteredJourneys.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No journeys found.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredJourneys as journey}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<GitBranch class="w-5 h-5 text-purple-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{journey.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{journey.State ?? 'DRAFT'} · {journey.Id}</p>
									</div>
								</div>
								<button
									onclick={() => deleteJourney(journey.Id ?? '')}
									class="text-red-400 hover:text-red-600 p-1 rounded"
									title="Delete journey"
								><Trash2 class="w-4 h-4" /></button>
							</div>
						{/each}
					</div>
				{/if}

			<!-- KPI Dashboard -->
			{:else if activeTab === 'kpis'}
				{#if !selectedAppId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select an application to view KPIs.</div>
				{:else if kpiLoading}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading KPI data...</div>
				{:else if !kpiData}
					<div class="text-center py-8 space-y-3">
						<Activity class="w-12 h-12 text-gray-300 mx-auto" />
						<p class="text-gray-500 dark:text-gray-400">No KPI data loaded yet.</p>
						<button
							onclick={loadKPIs}
							class="px-4 py-2 rounded-lg bg-pink-600 text-white text-sm hover:bg-pink-700"
						>Load KPIs</button>
					</div>
				{:else}
					<div class="space-y-4">
						<div class="flex items-center justify-between">
							<h3 class="font-semibold text-gray-900 dark:text-white">
								{kpiData.KpiName ?? 'KPI'} — last 30 days
							</h3>
							<button
								onclick={loadKPIs}
								class="flex items-center gap-1 text-sm text-pink-600 hover:text-pink-700"
							>
								<RefreshCw class="w-3 h-3" /> Refresh
							</button>
						</div>
						{#if (kpiData.KpiResult?.Rows?.length ?? 0) === 0}
							<p class="text-sm text-gray-500 dark:text-gray-400">No data rows returned for this KPI.</p>
						{:else}
							<div class="overflow-x-auto">
								<table class="w-full text-sm text-left">
									<thead class="text-xs text-gray-500 dark:text-gray-400 border-b border-slate-200 dark:border-slate-700">
										<tr>
											<th class="pb-2 pr-4">Key</th>
											<th class="pb-2 pr-4">Value</th>
											<th class="pb-2">Type</th>
										</tr>
									</thead>
									<tbody>
										{#each kpiData.KpiResult?.Rows ?? [] as row}
											{#each row.Values ?? [] as val}
												<tr class="border-b border-slate-100 dark:border-slate-800">
													<td class="py-2 pr-4 font-mono">{val.Key}</td>
													<td class="py-2 pr-4 font-mono">{val.Value}</td>
													<td class="py-2 text-gray-500">{val.Type}</td>
												</tr>
											{/each}
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Campaign Schedule Editor -->
{#if showScheduleModal && scheduleCampaign}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-xl bg-white dark:bg-slate-800 p-6 shadow-xl space-y-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Campaign Schedule</h2>
			<p class="text-sm text-gray-500 dark:text-gray-400">{scheduleCampaign.Name}</p>
			<div>
				<label for="sched-start" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Start Time (blank = immediate)</label>
				<input id="sched-start" type="datetime-local" bind:value={schedStart} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-slate-700 text-sm" />
			</div>
			<div>
				<label for="sched-end" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">End Time (optional)</label>
				<input id="sched-end" type="datetime-local" bind:value={schedEnd} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-slate-700 text-sm" />
			</div>
			<div>
				<label for="sched-freq" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Frequency</label>
				<select id="sched-freq" bind:value={schedFrequency} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-slate-700 text-sm">
					{#each frequencyOptions as f}<option value={f}>{f}</option>{/each}
				</select>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showScheduleModal = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-700">Cancel</button>
				<button onclick={saveSchedule} disabled={savingSchedule} class="flex-1 px-4 py-2 rounded-lg bg-pink-600 text-white text-sm font-medium hover:bg-pink-700 disabled:opacity-50">
					{savingSchedule ? 'Saving…' : 'Save Schedule'}
				</button>
			</div>
		</div>
	</div>
{/if}
