<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getCloudTrailClient } from '$lib/aws-client';
	import {
		DescribeTrailsCommand,
		GetTrailStatusCommand,
		GetEventSelectorsCommand,
		GetInsightSelectorsCommand,
		LookupEventsCommand,
		StartLoggingCommand,
		StopLoggingCommand,
		CreateTrailCommand,
		DeleteTrailCommand,
		ListEventDataStoresCommand,
		CreateEventDataStoreCommand,
		DeleteEventDataStoreCommand,
		StartEventDataStoreIngestionCommand,
		StopEventDataStoreIngestionCommand,
		type Trail,
		type EventDataStore,
		type EventSelector,
		type AdvancedEventSelector,
		type InsightSelector,
		type Event as CloudTrailEvent,
		type LookupAttribute
	} from '@aws-sdk/client-cloudtrail';
	import { toast } from 'svelte-sonner';
	import {
		Activity,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Play,
		Square,
		CheckCircle,
		XCircle,
		Clock,
		Filter,
		Database
	} from 'lucide-svelte';

	const ct = getCloudTrailClient();

	let loading = $state(false);
	let activeTab = $state<'trails' | 'events' | 'datastores'>('trails');
	let searchQuery = $state('');

	// Trails
	let trails = $state<Trail[]>([]);
	let trailStatuses = $state<Record<string, { IsLogging?: boolean; LatestDeliveryTime?: Date; LatestNotificationTime?: Date; StartLoggingTime?: Date; StopLoggingTime?: Date; LatestDeliveryError?: string }>>({});
	let selectedTrail = $state<Trail | null>(null);
	let trailEventSelectors = $state<EventSelector[]>([]);
	let trailAdvancedSelectors = $state<AdvancedEventSelector[]>([]);
	let trailInsightSelectors = $state<InsightSelector[]>([]);
	let loadingTrailDetail = $state(false);
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newTrailName = $state('');
	let newTrailBucket = $state('');
	let newTrailMultiRegion = $state(false);

	// Events
	let events = $state<CloudTrailEvent[]>([]);
	let loadingEvents = $state(false);
	let eventFilter = $state('');
	let eventStartTime = $state('');
	let eventEndTime = $state('');
	let maxResults = $state(50);

	// Event Data Stores
	let eventDataStores = $state<EventDataStore[]>([]);
	let loadingDataStores = $state(false);
	let dsSearchQuery = $state('');
	let showCreateDSModal = $state(false);
	let creatingDS = $state(false);
	let newDSName = $state('');
	let newDSMultiRegion = $state(false);
	let newDSRetentionPeriod = $state(90);

	const filteredTrails = $derived(
		trails.filter(
			(t) =>
				(t.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(t.HomeRegion ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredEvents = $derived(
		events.filter(
			(e) =>
				(e.EventName ?? '').toLowerCase().includes(eventFilter.toLowerCase()) ||
				(e.Username ?? '').toLowerCase().includes(eventFilter.toLowerCase()) ||
				(e.EventSource ?? '').toLowerCase().includes(eventFilter.toLowerCase())
		)
	);

	const filteredDataStores = $derived(
		eventDataStores.filter((ds) =>
			(ds.Name ?? '').toLowerCase().includes(dsSearchQuery.toLowerCase())
		)
	);

	async function loadTrails() {
		loading = true;
		try {
			const res = await ct.send(new DescribeTrailsCommand({ includeShadowTrails: false }));
			trails = res.trailList ?? [];
			// Load statuses for all trails
			await Promise.all(
				trails.map(async (trail) => {
					if (!trail.TrailARN) return;
					try {
						const status = await ct.send(
							new GetTrailStatusCommand({ Name: trail.TrailARN })
						);
						trailStatuses[trail.TrailARN] = {
							IsLogging: status.IsLogging,
							LatestDeliveryTime: status.LatestDeliveryTime,
							LatestNotificationTime: status.LatestNotificationTime,
							StartLoggingTime: status.StartLoggingTime,
							StopLoggingTime: status.StopLoggingTime,
							LatestDeliveryError: status.LatestDeliveryError
						};
					} catch {
						// ignore per-trail errors
					}
				})
			);
		} catch (e) {
			toast.error(`Failed to load trails: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function selectTrail(trail: Trail) {
		if (selectedTrail?.TrailARN === trail.TrailARN) { selectedTrail = null; return; }
		selectedTrail = trail;
		loadingTrailDetail = true;
		trailEventSelectors = [];
		trailAdvancedSelectors = [];
		trailInsightSelectors = [];
		try {
			const [esRes, isRes] = await Promise.allSettled([
				ct.send(new GetEventSelectorsCommand({ TrailName: trail.TrailARN })),
				ct.send(new GetInsightSelectorsCommand({ TrailName: trail.TrailARN }))
			]);
			if (esRes.status === 'fulfilled') {
				trailEventSelectors = esRes.value.EventSelectors ?? [];
				trailAdvancedSelectors = esRes.value.AdvancedEventSelectors ?? [];
			}
			if (isRes.status === 'fulfilled') trailInsightSelectors = isRes.value.InsightSelectors ?? [];
		} catch {
			// ignore
		} finally {
			loadingTrailDetail = false;
		}
	}

	async function lookupEvents() {
		loadingEvents = true;
		events = [];
		try {
			const params = { 
				MaxResults: maxResults,
				StartTime: eventStartTime ? new Date(eventStartTime) : undefined,
				EndTime: eventEndTime ? new Date(eventEndTime) : undefined,
				LookupAttributes: [] as LookupAttribute[]
			};
			const res = await ct.send(new LookupEventsCommand(params));
			events = res.Events ?? [];
		} catch (e) {
			toast.error(`Failed to lookup events: ${e}`);
		} finally {
			loadingEvents = false;
		}
	}

	async function toggleLogging(trail: Trail) {
		if (!trail.TrailARN) return;
		const isLogging = trailStatuses[trail.TrailARN]?.IsLogging;
		try {
			if (isLogging) {
				await ct.send(new StopLoggingCommand({ Name: trail.TrailARN }));
				toast.success(`Stopped logging for ${trail.Name}`);
			} else {
				await ct.send(new StartLoggingCommand({ Name: trail.TrailARN }));
				toast.success(`Started logging for ${trail.Name}`);
			}
			await loadTrails();
		} catch (e) {
			toast.error(`Failed to toggle logging: ${e}`);
		}
	}

	async function createTrail() {
		if (!newTrailName.trim() || !newTrailBucket.trim()) return;
		creating = true;
		try {
			await ct.send(
				new CreateTrailCommand({
					Name: newTrailName.trim(),
					S3BucketName: newTrailBucket.trim(),
					IsMultiRegionTrail: newTrailMultiRegion
				})
			);
			toast.success(`Trail "${newTrailName}" created`);
			showCreateModal = false;
			newTrailName = '';
			newTrailBucket = '';
			newTrailMultiRegion = false;
			await loadTrails();
		} catch (e) {
			toast.error(`Failed to create trail: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteTrail(trail: Trail) {
		if (!trail.TrailARN || !await confirmDestructive({ title: 'Delete Trail', message: `Delete trail "${trail.Name}"? API activity will no longer be logged to this trail.` })) return;
		try {
			await ct.send(new DeleteTrailCommand({ Name: trail.TrailARN }));
			toast.success(`Trail "${trail.Name}" deleted`);
			await loadTrails();
		} catch (e) {
			toast.error(`Failed to delete trail: ${e}`);
		}
	}

	async function loadDataStores() {
		loadingDataStores = true;
		try {
			const res = await ct.send(new ListEventDataStoresCommand({}));
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			eventDataStores = (res.EventDataStores ?? []) as any;
		} catch (e) {
			toast.error(`Failed to load event data stores: ${e}`);
		} finally {
			loadingDataStores = false;
		}
	}

	async function createEventDataStore() {
		if (!newDSName.trim()) return;
		creatingDS = true;
		try {
			await ct.send(
				new CreateEventDataStoreCommand({
					Name: newDSName.trim(),
					MultiRegionEnabled: newDSMultiRegion,
					RetentionPeriod: newDSRetentionPeriod
				})
			);
			toast.success(`Event data store "${newDSName}" created`);
			showCreateDSModal = false;
			newDSName = '';
			newDSMultiRegion = false;
			newDSRetentionPeriod = 90;
			await loadDataStores();
		} catch (e) {
			toast.error(`Failed to create event data store: ${e}`);
		} finally {
			creatingDS = false;
		}
	}

	async function deleteEventDataStore(ds: EventDataStore) {
		if (
			!ds.EventDataStoreArn ||
			!(await confirmDestructive({
				title: 'Delete Event Data Store',
				message: `Delete event data store "${ds.Name}"? All stored events will be lost.`
			}))
		)
			return;
		try {
			await ct.send(new DeleteEventDataStoreCommand({ EventDataStore: ds.EventDataStoreArn }));
			toast.success(`Event data store "${ds.Name}" deleted`);
			await loadDataStores();
		} catch (e) {
			toast.error(`Failed to delete event data store: ${e}`);
		}
	}

	async function toggleDSIngestion(ds: EventDataStore) {
		if (!ds.EventDataStoreArn) return;
		const isEnabled = ds.Status === 'ENABLED';
		try {
			if (isEnabled) {
				await ct.send(
					new StopEventDataStoreIngestionCommand({ EventDataStore: ds.EventDataStoreArn })
				);
				toast.success(`Stopped ingestion for "${ds.Name}"`);
			} else {
				await ct.send(
					new StartEventDataStoreIngestionCommand({ EventDataStore: ds.EventDataStoreArn })
				);
				toast.success(`Started ingestion for "${ds.Name}"`);
			}
			await loadDataStores();
		} catch (e) {
			toast.error(`Failed to toggle ingestion: ${e}`);
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		selectedTrail = null;
		if (tab === 'trails') await loadTrails();
		else if (tab === 'events') await lookupEvents();
		else await loadDataStores();
	}

	onMount(() => loadTrails());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Activity class="h-8 w-8 text-blue-600" />
			<div>
				<h1 class="text-2xl font-bold">CloudTrail</h1>
				<p class="text-sm text-muted-foreground">Audit API activity and event history</p>
			</div>
		</div>
		<button
			onclick={() => (activeTab === 'trails' ? loadTrails() : lookupEvents())}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'trails', label: 'Trails' }, { id: 'events', label: 'Event History' }, { id: 'datastores', label: 'Event Data Stores' }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Trails Tab -->
	{#if activeTab === 'trails'}
		<div class="flex items-center justify-between gap-4">
			<div class="relative flex-1">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search trails..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
			>
				<Plus class="h-4 w-4" />
				Create Trail
			</button>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredTrails.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Activity class="h-12 w-12 mb-3 opacity-30" />
				<p>No trails found</p>
				<p class="text-sm">Create a trail to start logging API activity</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Name</th>
							<th class="px-4 py-3 text-left font-medium">Region</th>
							<th class="px-4 py-3 text-left font-medium">S3 Bucket</th>
							<th class="px-4 py-3 text-left font-medium">Logging</th>
							<th class="px-4 py-3 text-left font-medium">Last Delivery</th>
							<th class="px-4 py-3 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredTrails as trail}
							{@const status = trailStatuses[trail.TrailARN ?? '']}
							<tr
								class="hover:bg-muted/30 cursor-pointer {selectedTrail?.TrailARN === trail.TrailARN ? 'bg-muted/50' : ''}"
								onclick={() => selectTrail(trail)}
							>
								<td class="px-4 py-3 font-medium">{trail.Name}</td>
								<td class="px-4 py-3 text-muted-foreground">{trail.HomeRegion ?? '—'}</td>
								<td class="px-4 py-3 text-muted-foreground truncate max-w-[180px]">
									{trail.S3BucketName ?? '—'}
								</td>
								<td class="px-4 py-3">
									{#if status?.IsLogging}
										<span class="flex items-center gap-1 text-green-600">
											<CheckCircle class="h-4 w-4" />
											Active
										</span>
									{:else}
										<span class="flex items-center gap-1 text-muted-foreground">
											<XCircle class="h-4 w-4" />
											Stopped
										</span>
									{/if}
								</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{status?.LatestDeliveryTime
										? new Date(status.LatestDeliveryTime).toLocaleString()
										: '—'}
								</td>
								<td class="px-4 py-3 text-right flex justify-end gap-1">
									<button
										onclick={(e) => { e.stopPropagation(); toggleLogging(trail); }}
										class="rounded p-1 hover:bg-accent"
										title={status?.IsLogging ? 'Stop logging' : 'Start logging'}
									>
										{#if status?.IsLogging}
											<Square class="h-4 w-4 text-yellow-500" />
										{:else}
											<Play class="h-4 w-4 text-green-500" />
										{/if}
									</button>
									<button
										onclick={(e) => { e.stopPropagation(); deleteTrail(trail); }}
										class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										title="Delete trail"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		<!-- Trail Detail Panel -->
		{#if selectedTrail}
			{@const status = trailStatuses[selectedTrail.TrailARN ?? '']}
			<div class="rounded-lg border p-5 space-y-5">
				<div class="flex items-center justify-between">
					<h3 class="font-semibold text-lg">{selectedTrail.Name}</h3>
					<button onclick={() => { selectedTrail = null; }} class="text-xs text-muted-foreground hover:text-foreground">Close</button>
				</div>

				<!-- Config grid -->
				<div class="grid grid-cols-2 sm:grid-cols-4 gap-3">
					{#each [
						['S3 Bucket', selectedTrail.S3BucketName ?? '—'],
						['S3 Key Prefix', selectedTrail.S3KeyPrefix ?? '(none)'],
						['Multi-Region', selectedTrail.IsMultiRegionTrail ? 'Yes' : 'No'],
						['Log Validation', selectedTrail.LogFileValidationEnabled ? 'Enabled' : 'Disabled'],
						['Global Services', selectedTrail.IncludeGlobalServiceEvents ? 'Yes' : 'No'],
						['Org Trail', selectedTrail.IsOrganizationTrail ? 'Yes' : 'No'],
						['Custom Selectors', selectedTrail.HasCustomEventSelectors ? 'Yes' : 'No'],
						['Insight Selectors', selectedTrail.HasInsightSelectors ? 'Yes' : 'No'],
					] as [label, value]}
						<div class="rounded bg-muted/50 p-3">
							<p class="text-xs text-muted-foreground">{label}</p>
							<p class="text-sm font-semibold mt-0.5">{value}</p>
						</div>
					{/each}
				</div>

				{#if selectedTrail.TrailARN}
					<div class="rounded bg-muted/50 px-3 py-2">
						<p class="text-xs text-muted-foreground mb-0.5">Trail ARN</p>
						<p class="text-xs font-mono break-all">{selectedTrail.TrailARN}</p>
					</div>
				{/if}

				{#if selectedTrail.CloudWatchLogsLogGroupArn}
					<div class="rounded bg-muted/50 px-3 py-2">
						<p class="text-xs text-muted-foreground mb-0.5">CloudWatch Logs Group ARN</p>
						<p class="text-xs font-mono break-all">{selectedTrail.CloudWatchLogsLogGroupArn}</p>
					</div>
				{/if}

				{#if selectedTrail.KmsKeyId}
					<div class="rounded bg-muted/50 px-3 py-2">
						<p class="text-xs text-muted-foreground mb-0.5">KMS Key ID</p>
						<p class="text-xs font-mono break-all">{selectedTrail.KmsKeyId}</p>
					</div>
				{/if}

				<!-- Status -->
				{#if status}
					<div>
						<h4 class="text-sm font-medium mb-2">Logging Status</h4>
						<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
							{#each [
								['Status', status.IsLogging ? 'Logging' : 'Stopped'],
								['Last Delivery', status.LatestDeliveryTime ? new Date(status.LatestDeliveryTime).toLocaleString() : '—'],
								['Last Notification', status.LatestNotificationTime ? new Date(status.LatestNotificationTime).toLocaleString() : '—'],
								['Logging Started', status.StartLoggingTime ? new Date(status.StartLoggingTime).toLocaleString() : '—'],
								['Logging Stopped', status.StopLoggingTime ? new Date(status.StopLoggingTime).toLocaleString() : '—'],
								['Delivery Error', status.LatestDeliveryError ?? 'None'],
							] as [label, value]}
								<div class="rounded bg-muted/50 p-3">
									<p class="text-xs text-muted-foreground">{label}</p>
									<p class="text-sm font-medium mt-0.5 truncate" title={value}>{value}</p>
								</div>
							{/each}
						</div>
					</div>
				{/if}

				{#if loadingTrailDetail}
					<div class="flex items-center gap-2 text-sm text-muted-foreground">
						<RefreshCw class="h-4 w-4 animate-spin" />
						Loading selectors...
					</div>
				{:else}
					<!-- Event Selectors -->
					{#if trailEventSelectors.length > 0}
						<div>
							<h4 class="text-sm font-medium mb-2">Event Selectors</h4>
							<div class="rounded border overflow-hidden">
								<table class="w-full text-sm">
									<thead class="bg-muted/50">
										<tr>
											<th class="px-3 py-2 text-left font-medium">Read/Write Type</th>
											<th class="px-3 py-2 text-left font-medium">Management Events</th>
											<th class="px-3 py-2 text-left font-medium">Data Resources</th>
										</tr>
									</thead>
									<tbody class="divide-y">
										{#each trailEventSelectors as sel}
											<tr>
												<td class="px-3 py-2">{sel.ReadWriteType ?? '—'}</td>
												<td class="px-3 py-2">{sel.IncludeManagementEvents ? 'Yes' : 'No'}</td>
												<td class="px-3 py-2 text-xs">
													{#if (sel.DataResources ?? []).length > 0}
														{#each sel.DataResources ?? [] as dr}
															<div><span class="font-medium">{dr.Type}</span>: {(dr.Values ?? []).join(', ') || 'All'}</div>
														{/each}
													{:else}
														None
													{/if}
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						</div>
					{/if}

					<!-- Advanced Event Selectors -->
					{#if trailAdvancedSelectors.length > 0}
						<div>
							<h4 class="text-sm font-medium mb-2">Advanced Event Selectors ({trailAdvancedSelectors.length})</h4>
							<div class="space-y-2">
								{#each trailAdvancedSelectors as sel}
									<div class="rounded bg-muted/50 px-3 py-2">
										<p class="text-sm font-medium">{sel.Name ?? '(unnamed)'}</p>
										<p class="text-xs text-muted-foreground mt-0.5">{(sel.FieldSelectors ?? []).length} field selector(s)</p>
									</div>
								{/each}
							</div>
						</div>
					{/if}

					<!-- Insight Selectors -->
					{#if trailInsightSelectors.length > 0}
						<div>
							<h4 class="text-sm font-medium mb-2">Insight Selectors</h4>
							<div class="flex flex-wrap gap-2">
								{#each trailInsightSelectors as ins}
									<span class="px-2 py-1 text-xs rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{ins.InsightType ?? '—'}</span>
								{/each}
							</div>
						</div>
					{/if}
				{/if}
			</div>
		{/if}
	{/if}

	<!-- Events Tab -->
	{#if activeTab === 'events'}
		<div class="space-y-4">
			<div class="flex flex-wrap items-end gap-3 rounded-lg border p-4 bg-muted/20">
				<div class="flex-1 min-w-[200px]">
					<label for="event-filter" class="block text-sm font-medium mb-1">Filter (name/user/source)</label>
					<div class="relative">
						<Filter class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
						<input
							id="event-filter"
							type="text"
							bind:value={eventFilter}
							placeholder="e.g. RunInstances"
							class="w-full rounded-md border bg-background pl-9 pr-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
				</div>
				<div>
					<label for="event-start" class="block text-sm font-medium mb-1">Start Time</label>
					<input
						id="event-start"
						type="datetime-local"
						bind:value={eventStartTime}
						class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="event-end" class="block text-sm font-medium mb-1">End Time</label>
					<input
						id="event-end"
						type="datetime-local"
						bind:value={eventEndTime}
						class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="max-results" class="block text-sm font-medium mb-1">Max Results</label>
					<select
						id="max-results"
						bind:value={maxResults}
						class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						{#each [10, 25, 50, 100] as n}
							<option value={n}>{n}</option>
						{/each}
					</select>
				</div>
				<button
					onclick={lookupEvents}
					disabled={loadingEvents}
					class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					<Search class="h-4 w-4" />
					{loadingEvents ? 'Loading...' : 'Search Events'}
				</button>
			</div>

			{#if loadingEvents}
				<div class="flex justify-center py-12">
					<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
				</div>
			{:else if events.length === 0}
				<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
					<Clock class="h-12 w-12 mb-3 opacity-30" />
					<p>No events found</p>
					<p class="text-sm">Adjust filters and search to find events</p>
				</div>
			{:else}
				<p class="text-sm text-muted-foreground">
					Showing {filteredEvents.length} of {events.length} events
				</p>
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Time</th>
								<th class="px-4 py-3 text-left font-medium">Event Name</th>
								<th class="px-4 py-3 text-left font-medium">Source</th>
								<th class="px-4 py-3 text-left font-medium">User</th>
								<th class="px-4 py-3 text-left font-medium">Resource</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each filteredEvents as event}
								<tr class="hover:bg-muted/30">
									<td class="px-4 py-3 text-xs text-muted-foreground whitespace-nowrap">
										{event.EventTime ? new Date(event.EventTime).toLocaleString() : '—'}
									</td>
									<td class="px-4 py-3 font-medium">{event.EventName ?? '—'}</td>
									<td class="px-4 py-3 text-muted-foreground text-xs">{event.EventSource ?? '—'}</td>
									<td class="px-4 py-3">{event.Username ?? '—'}</td>
									<td class="px-4 py-3 text-muted-foreground text-xs">
										{event.Resources?.[0]?.ResourceName ?? '—'}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Create Event Data Store Modal -->
{#if showCreateDSModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Event Data Store</h2>
			<div class="space-y-3">
				<div>
					<label for="ds-name" class="block text-sm font-medium mb-1">Name *</label>
					<input
						id="ds-name"
						type="text"
						bind:value={newDSName}
						placeholder="my-event-data-store"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="ds-retention" class="block text-sm font-medium mb-1"
						>Retention Period (days)</label
					>
					<input
						id="ds-retention"
						type="number"
						bind:value={newDSRetentionPeriod}
						min="7"
						max="2557"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div class="flex items-center gap-2">
					<input
						id="ds-multi"
						type="checkbox"
						bind:checked={newDSMultiRegion}
						class="rounded"
					/>
					<label for="ds-multi" class="text-sm">Multi-region enabled</label>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateDSModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createEventDataStore}
					disabled={creatingDS || !newDSName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingDS ? 'Creating...' : 'Create Store'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Event Data Stores Tab -->
{#if activeTab === 'datastores'}
	<div class="flex items-center justify-between gap-4">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search event data stores..."
				bind:value={dsSearchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>
		<button
			onclick={() => (showCreateDSModal = true)}
			class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
		>
			<Plus class="h-4 w-4" />
			Create Store
		</button>
	</div>

	{#if loadingDataStores}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if filteredDataStores.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Database class="h-12 w-12 mb-3 opacity-30" />
			<p>No event data stores found</p>
			<p class="text-sm">Create an event data store to collect and query events</p>
		</div>
	{:else}
		<div class="rounded-lg border overflow-hidden">
			<table class="w-full text-sm">
				<thead class="bg-muted/50">
					<tr>
						<th class="px-4 py-3 text-left font-medium">Name</th>
						<th class="px-4 py-3 text-left font-medium">Status</th>
						<th class="px-4 py-3 text-left font-medium">Retention (days)</th>
						<th class="px-4 py-3 text-left font-medium">Multi-Region</th>
						<th class="px-4 py-3 text-right font-medium">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y">
					{#each filteredDataStores as ds}
						<tr class="hover:bg-muted/30">
							<td class="px-4 py-3 font-medium">{ds.Name ?? '—'}</td>
							<td class="px-4 py-3">
								{#if ds.Status === 'ENABLED'}
									<span class="flex items-center gap-1 text-green-600">
										<CheckCircle class="h-4 w-4" />
										Enabled
									</span>
								{:else}
									<span class="flex items-center gap-1 text-muted-foreground">
										<XCircle class="h-4 w-4" />
										{ds.Status ?? 'Unknown'}
									</span>
								{/if}
							</td>
							<td class="px-4 py-3 text-muted-foreground">{ds.RetentionPeriod ?? '—'}</td>
							<td class="px-4 py-3 text-muted-foreground">
								{ds.MultiRegionEnabled ? 'Yes' : 'No'}
							</td>
							<td class="px-4 py-3 text-right flex justify-end gap-1">
								<button
									onclick={() => toggleDSIngestion(ds)}
									class="rounded p-1 hover:bg-accent"
									title={ds.Status === 'ENABLED' ? 'Stop ingestion' : 'Start ingestion'}
								>
									{#if ds.Status === 'ENABLED'}
										<Square class="h-4 w-4 text-yellow-500" />
									{:else}
										<Play class="h-4 w-4 text-green-500" />
									{/if}
								</button>
								<button
									onclick={() => deleteEventDataStore(ds)}
									class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
									title="Delete event data store"
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
{/if}

<!-- Create Trail Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Trail</h2>
			<div class="space-y-3">
				<div>
					<label for="trail-name" class="block text-sm font-medium mb-1">Trail Name *</label>
					<input
						id="trail-name"
						type="text"
						bind:value={newTrailName}
						placeholder="my-audit-trail"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="trail-bucket" class="block text-sm font-medium mb-1">S3 Bucket *</label>
					<input
						id="trail-bucket"
						type="text"
						bind:value={newTrailBucket}
						placeholder="my-cloudtrail-bucket"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div class="flex items-center gap-2">
					<input
						id="trail-multi"
						type="checkbox"
						bind:checked={newTrailMultiRegion}
						class="rounded"
					/>
					<label for="trail-multi" class="text-sm">Multi-region trail</label>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createTrail}
					disabled={creating || !newTrailName.trim() || !newTrailBucket.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Trail'}
				</button>
			</div>
		</div>
	</div>
{/if}
