<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getIoTAnalyticsClient } from '$lib/aws-client';
	import {
		ListChannelsCommand,
		CreateChannelCommand,
		DeleteChannelCommand,
		BatchPutMessageCommand,
		SampleChannelDataCommand,
		ListDatastoresCommand,
		CreateDatastoreCommand,
		DeleteDatastoreCommand,
		ListDatasetsCommand,
		CreateDatasetCommand,
		DeleteDatasetCommand,
		CreateDatasetContentCommand,
		ListDatasetContentsCommand,
		ListPipelinesCommand,
		CreatePipelineCommand,
		DeletePipelineCommand,
		StartPipelineReprocessingCommand,
		CancelPipelineReprocessingCommand,
		DescribeLoggingOptionsCommand,
		PutLoggingOptionsCommand,
		LoggingLevel,
		type ReprocessingSummary as SDKReprocessingSummary
	} from '@aws-sdk/client-iotanalytics';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Trash2, Plus, Database, GitBranch, Radio, BarChart2, Settings, Send, Eye } from 'lucide-svelte';

	const iotAnalytics = regionalClient(getIoTAnalyticsClient);

	type Tab = 'channels' | 'datastores' | 'datasets' | 'pipelines' | 'logging';
	let activeTab = $state<Tab>('channels');

	// ---- focus action ----
	function focusOnMount(el: HTMLElement) {
		el.focus();
	}

	// ---- Channels ----
	let channels = $state<Array<{ channelName?: string; status?: string }>>([]);
	let channelsBusy = $state(false);
	let deletingChannel = $state<string | null>(null);
	let newChannelName = $state('');
	let showCreateChannel = $state(false);

	// Batch ingestion state
	let batchChannelName = $state('');
	let batchPayload = $state('{"temperature": 22.5, "ts": 0}');
	let batchMessageId = $state('msg-001');
	let batchBusy = $state(false);

	// Sample data state
	let sampleChannelTarget = $state('');
	let sampleMaxMessages = $state(10);
	let sampleBusy = $state(false);
	let sampleResults = $state<string[]>([]);

	async function loadChannels() {
		channelsBusy = true;
		try {
			const out = await iotAnalytics().send(new ListChannelsCommand({}));
			channels = out.channelSummaries ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load channels: ${(err as Error).message}`);
		} finally {
			channelsBusy = false;
		}
	}

	async function createChannel() {
		if (!newChannelName.trim()) { toast.error('Channel name is required'); return; }
		try {
			await iotAnalytics().send(new CreateChannelCommand({ channelName: newChannelName.trim() }));
			showCreateChannel = false;
			newChannelName = '';
			await loadChannels();
			toast.success('Channel created');
		} catch (err: unknown) {
			toast.error(`Failed to create channel: ${(err as Error).message}`);
		}
	}

	async function deleteChannel(name: string) {
		deletingChannel = name;
		try {
			await iotAnalytics().send(new DeleteChannelCommand({ channelName: name }));
			await loadChannels();
			if (sampleChannelTarget === name) sampleChannelTarget = '';
			if (batchChannelName === name) batchChannelName = '';
			toast.success(`Channel "${name}" deleted`);
		} catch (err: unknown) {
			toast.error(`Failed to delete channel: ${(err as Error).message}`);
		} finally {
			deletingChannel = null;
		}
	}

	async function batchIngest() {
		if (!batchChannelName.trim()) { toast.error('Channel name is required'); return; }
		if (!batchMessageId.trim()) { toast.error('Message ID is required'); return; }
		batchBusy = true;
		try {
			const encoder = new TextEncoder();
			await iotAnalytics().send(new BatchPutMessageCommand({
				channelName: batchChannelName.trim(),
				messages: [{ messageId: batchMessageId.trim(), payload: encoder.encode(batchPayload) }]
			}));
			toast.success('Message ingested');
			batchMessageId = 'msg-' + Date.now().toString().slice(-6);
		} catch (err: unknown) {
			toast.error(`Ingest failed: ${(err as Error).message}`);
		} finally {
			batchBusy = false;
		}
	}

	async function sampleData() {
		if (!sampleChannelTarget.trim()) { toast.error('Channel name is required'); return; }
		sampleBusy = true;
		sampleResults = [];
		try {
			const out = await iotAnalytics().send(new SampleChannelDataCommand({
				channelName: sampleChannelTarget.trim(),
				maxMessages: sampleMaxMessages
			}));
			const decoder = new TextDecoder();
			sampleResults = (out.payloads ?? []).map((p) => {
				try { return JSON.stringify(JSON.parse(decoder.decode(p)), null, 2); } catch { return decoder.decode(p); }
			});
			if (sampleResults.length === 0) toast.info('No messages in channel');
		} catch (err: unknown) {
			toast.error(`Sample failed: ${(err as Error).message}`);
		} finally {
			sampleBusy = false;
		}
	}

	// ---- Datastores ----
	let datastores = $state<Array<{ datastoreName?: string; status?: string }>>([]);
	let datastoresBusy = $state(false);
	let deletingDatastore = $state<string | null>(null);
	let newDatastoreName = $state('');
	let showCreateDatastore = $state(false);

	async function loadDatastores() {
		datastoresBusy = true;
		try {
			const out = await iotAnalytics().send(new ListDatastoresCommand({}));
			datastores = out.datastoreSummaries ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load datastores: ${(err as Error).message}`);
		} finally {
			datastoresBusy = false;
		}
	}

	async function createDatastore() {
		if (!newDatastoreName.trim()) { toast.error('Datastore name is required'); return; }
		try {
			await iotAnalytics().send(new CreateDatastoreCommand({ datastoreName: newDatastoreName.trim() }));
			showCreateDatastore = false;
			newDatastoreName = '';
			await loadDatastores();
			toast.success('Datastore created');
		} catch (err: unknown) {
			toast.error(`Failed to create datastore: ${(err as Error).message}`);
		}
	}

	async function deleteDatastore(name: string) {
		deletingDatastore = name;
		try {
			await iotAnalytics().send(new DeleteDatastoreCommand({ datastoreName: name }));
			await loadDatastores();
			toast.success(`Datastore "${name}" deleted`);
		} catch (err: unknown) {
			toast.error(`Failed to delete datastore: ${(err as Error).message}`);
		} finally {
			deletingDatastore = null;
		}
	}

	// ---- Datasets ----
	type DatasetSummary = { datasetName?: string; status?: string };
	type ContentSummary = { version?: string; status?: { state?: string }; creationTime?: Date; completionTime?: Date };

	let datasets = $state<DatasetSummary[]>([]);
	let datasetsBusy = $state(false);
	let deletingDataset = $state<string | null>(null);
	let newDatasetName = $state('');
	let showCreateDataset = $state(false);
	let selectedDataset = $state<string | null>(null);
	let datasetContents = $state<ContentSummary[]>([]);
	let contentsBusy = $state(false);
	let triggerBusy = $state<string | null>(null);

	async function loadDatasets() {
		datasetsBusy = true;
		try {
			const out = await iotAnalytics().send(new ListDatasetsCommand({}));
			datasets = out.datasetSummaries ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load datasets: ${(err as Error).message}`);
		} finally {
			datasetsBusy = false;
		}
	}

	async function createDataset() {
		if (!newDatasetName.trim()) { toast.error('Dataset name is required'); return; }
		try {
			await iotAnalytics().send(new CreateDatasetCommand({
				datasetName: newDatasetName.trim(),
				actions: [{ actionName: 'default' }]
			}));
			showCreateDataset = false;
			newDatasetName = '';
			await loadDatasets();
			toast.success('Dataset created');
		} catch (err: unknown) {
			toast.error(`Failed to create dataset: ${(err as Error).message}`);
		}
	}

	async function deleteDataset(name: string) {
		deletingDataset = name;
		try {
			await iotAnalytics().send(new DeleteDatasetCommand({ datasetName: name }));
			if (selectedDataset === name) { selectedDataset = null; datasetContents = []; }
			await loadDatasets();
			toast.success(`Dataset "${name}" deleted`);
		} catch (err: unknown) {
			toast.error(`Failed to delete dataset: ${(err as Error).message}`);
		} finally {
			deletingDataset = null;
		}
	}

	async function selectDataset(name: string) {
		selectedDataset = name;
		await loadContents(name);
	}

	async function loadContents(name: string) {
		contentsBusy = true;
		try {
			const out = await iotAnalytics().send(new ListDatasetContentsCommand({ datasetName: name }));
			datasetContents = (out.datasetContentSummaries ?? []).map((c) => ({
				version: c.version,
				status: c.status ? { state: c.status.state } : undefined,
				creationTime: c.creationTime,
				completionTime: c.completionTime
			}));
		} catch (err: unknown) {
			toast.error(`Failed to load contents: ${(err as Error).message}`);
		} finally {
			contentsBusy = false;
		}
	}

	async function triggerContent(name: string) {
		triggerBusy = name;
		try {
			await iotAnalytics().send(new CreateDatasetContentCommand({ datasetName: name }));
			toast.success('Content generation triggered');
			if (selectedDataset === name) await loadContents(name);
			else await selectDataset(name);
		} catch (err: unknown) {
			toast.error(`Failed to trigger content: ${(err as Error).message}`);
		} finally {
			triggerBusy = null;
		}
	}

	function contentStateBadge(state?: string): string {
		switch (state) {
			case 'SUCCEEDED': return 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300';
			case 'FAILED': return 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300';
			case 'CREATING': return 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300';
			default: return 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
		}
	}

	// ---- Pipelines ----
	type PipelineSummary = { pipelineName?: string; creationTime?: Date; reprocessingSummaries?: SDKReprocessingSummary[] };

	let pipelines = $state<PipelineSummary[]>([]);
	let pipelinesBusy = $state(false);
	let deletingPipeline = $state<string | null>(null);
	let newPipelineName = $state('');
	let showCreatePipeline = $state(false);
	let reprocessBusy = $state<string | null>(null);
	let cancelBusy = $state<string | null>(null);
	let expandedPipeline = $state<string | null>(null);
	let pipelineReprocessings = $state<Record<string, { id?: string; status?: string }[]>>({});

	async function loadPipelines() {
		pipelinesBusy = true;
		try {
			const out = await iotAnalytics().send(new ListPipelinesCommand({}));
			pipelines = out.pipelineSummaries ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load pipelines: ${(err as Error).message}`);
		} finally {
			pipelinesBusy = false;
		}
	}

	async function createPipeline() {
		if (!newPipelineName.trim()) { toast.error('Pipeline name is required'); return; }
		try {
			await iotAnalytics().send(new CreatePipelineCommand({
				pipelineName: newPipelineName.trim(),
				pipelineActivities: [{ channel: { name: 'channel', channelName: 'default', next: '' } }]
			}));
			showCreatePipeline = false;
			newPipelineName = '';
			await loadPipelines();
			toast.success('Pipeline created');
		} catch (err: unknown) {
			toast.error(`Failed to create pipeline: ${(err as Error).message}`);
		}
	}

	async function deletePipeline(name: string) {
		deletingPipeline = name;
		try {
			await iotAnalytics().send(new DeletePipelineCommand({ pipelineName: name }));
			if (expandedPipeline === name) expandedPipeline = null;
			await loadPipelines();
			toast.success(`Pipeline "${name}" deleted`);
		} catch (err: unknown) {
			toast.error(`Failed to delete pipeline: ${(err as Error).message}`);
		} finally {
			deletingPipeline = null;
		}
	}

	async function startReprocessing(name: string) {
		reprocessBusy = name;
		try {
			const out = await iotAnalytics().send(new StartPipelineReprocessingCommand({ pipelineName: name }));
			toast.success(`Reprocessing started: ${out.reprocessingId}`);
			await loadPipelines();
			if (expandedPipeline === name) await expandPipeline(name);
		} catch (err: unknown) {
			toast.error(`Failed to start reprocessing: ${(err as Error).message}`);
		} finally {
			reprocessBusy = null;
		}
	}

	async function cancelReprocessing(pipelineName: string, reprocessingId: string) {
		cancelBusy = reprocessingId;
		try {
			await iotAnalytics().send(new CancelPipelineReprocessingCommand({ pipelineName, reprocessingId }));
			toast.success('Reprocessing cancelled');
			await expandPipeline(pipelineName);
		} catch (err: unknown) {
			toast.error(`Failed to cancel: ${(err as Error).message}`);
		} finally {
			cancelBusy = null;
		}
	}

	function expandPipeline(name: string) {
		if (expandedPipeline === name) { expandedPipeline = null; return; }
		expandedPipeline = name;
		const p = pipelines.find((pl) => pl.pipelineName === name);
		pipelineReprocessings[name] = (p?.reprocessingSummaries ?? []).map((rp) => ({
			id: rp.id,
			status: rp.status
		}));
	}

	function reprocessingStatusBadge(status?: string): string {
		switch (status) {
			case 'RUNNING': return 'bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300';
			case 'SUCCEEDED': return 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300';
			case 'CANCELLED': return 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
			case 'FAILED': return 'bg-red-100 text-red-700 dark:bg-red-900 dark:text-red-300';
			default: return 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
		}
	}

	// ---- Logging Options ----
	let loggingBusy = $state(false);
	let loggingRoleArn = $state('');
	let loggingLevel = $state('ERROR');
	let loggingEnabled = $state(false);
	let loggingLoaded = $state(false);

	async function loadLogging() {
		loggingBusy = true;
		try {
			const out = await iotAnalytics().send(new DescribeLoggingOptionsCommand({}));
			loggingRoleArn = out.loggingOptions?.roleArn ?? '';
			loggingLevel = out.loggingOptions?.level ?? 'ERROR';
			loggingEnabled = out.loggingOptions?.enabled ?? false;
			loggingLoaded = true;
		} catch (err: unknown) {
			const msg = (err as Error).message ?? '';
			if (!msg.includes('not found') && !msg.includes('404')) {
				toast.error(`Failed to load logging options: ${msg}`);
			}
			loggingLoaded = true;
		} finally {
			loggingBusy = false;
		}
	}

	async function saveLogging() {
		loggingBusy = true;
		try {
			await iotAnalytics().send(new PutLoggingOptionsCommand({
				loggingOptions: { roleArn: loggingRoleArn, level: loggingLevel as LoggingLevel, enabled: loggingEnabled }
			}));
			toast.success('Logging options saved');
		} catch (err: unknown) {
			toast.error(`Failed to save logging options: ${(err as Error).message}`);
		} finally {
			loggingBusy = false;
		}
	}

	// Selected dataset / expanded pipeline are only meaningful within the
	// region they were fetched in, and the pre-filled channel-name inputs
	// point at channels from the old region — clear all of it on a region
	// change rather than just re-listing.
	onRegionChange(() => {
		selectedDataset = null;
		datasetContents = [];
		expandedPipeline = null;
		pipelineReprocessings = {};
		batchChannelName = '';
		sampleChannelTarget = '';
		sampleResults = [];
		void loadChannels();
		void loadDatastores();
		void loadDatasets();
		void loadPipelines();
		void loadLogging();
	});

	const tabs: { id: Tab; label: string; icon: typeof Radio }[] = [
		{ id: 'channels', label: 'Channels', icon: Radio },
		{ id: 'datastores', label: 'Datastores', icon: Database },
		{ id: 'datasets', label: 'Datasets', icon: BarChart2 },
		{ id: 'pipelines', label: 'Pipelines', icon: GitBranch },
		{ id: 'logging', label: 'Logging', icon: Settings }
	];
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center gap-3">
		<Radio class="h-8 w-8 text-indigo-600" />
		<div>
			<h1 class="text-2xl font-bold">IoT Analytics</h1>
			<p class="text-sm text-muted-foreground">Channels, datastores, datasets, pipelines, and logging</p>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex border-b gap-1">
		{#each tabs as tab}
			{@const Icon = tab.icon}
			<button
				onclick={() => (activeTab = tab.id)}
				class="flex items-center gap-1.5 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id
					? 'border-primary text-primary'
					: 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<Icon class="h-3.5 w-3.5" />
				{tab.label}
				{#if tab.id === 'channels' && channels.length > 0}
					<span class="rounded-full bg-muted px-1.5 py-0.5 text-xs">{channels.length}</span>
				{/if}
				{#if tab.id === 'datastores' && datastores.length > 0}
					<span class="rounded-full bg-muted px-1.5 py-0.5 text-xs">{datastores.length}</span>
				{/if}
				{#if tab.id === 'datasets' && datasets.length > 0}
					<span class="rounded-full bg-muted px-1.5 py-0.5 text-xs">{datasets.length}</span>
				{/if}
				{#if tab.id === 'pipelines' && pipelines.length > 0}
					<span class="rounded-full bg-muted px-1.5 py-0.5 text-xs">{pipelines.length}</span>
				{/if}
			</button>
		{/each}
	</div>

	<!-- CHANNELS TAB -->
	{#if activeTab === 'channels'}
		<div class="space-y-4">
			<!-- Channel list card -->
			<div class="rounded-lg border p-5 space-y-4">
				<div class="flex items-center justify-between">
					<h2 class="font-semibold flex items-center gap-2"><Radio class="h-4 w-4 text-indigo-600" /> Channels</h2>
					<div class="flex gap-2">
						<button onclick={loadChannels} disabled={channelsBusy}
							class="rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50">
							<RefreshCw class="h-4 w-4 {channelsBusy ? 'animate-spin' : ''}" />
						</button>
						<button onclick={() => (showCreateChannel = true)}
							class="flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700">
							+ Create Channel
						</button>
					</div>
				</div>
				{#if channels.length === 0}
					<div class="flex flex-col items-center py-8 text-muted-foreground">
						<Radio class="h-10 w-10 mb-2 opacity-20" />
						<p class="text-sm">No channels found</p>
					</div>
				{:else}
					<div class="rounded-lg border overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-muted/50">
								<tr>
									<th class="px-4 py-3 text-left font-medium">Name</th>
									<th class="px-4 py-3 text-left font-medium">Status</th>
									<th class="px-4 py-3 text-right font-medium">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y">
								{#each channels as ch}
									<tr class="hover:bg-muted/30">
										<td class="px-4 py-3 font-medium font-mono text-xs">{ch.channelName}</td>
										<td class="px-4 py-3">
											<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900 dark:text-green-300">
												{ch.status ?? 'ACTIVE'}
											</span>
										</td>
										<td class="px-4 py-3 text-right">
											<div class="flex justify-end gap-1">
												<button onclick={() => { sampleChannelTarget = ch.channelName ?? ''; }}
													class="rounded border px-2 py-1 text-xs hover:bg-accent" title="Sample data">
													<Eye class="h-3 w-3 inline" /> Sample
												</button>
												<button onclick={() => { batchChannelName = ch.channelName ?? ''; }}
													class="rounded border px-2 py-1 text-xs hover:bg-accent" title="Ingest message">
													<Send class="h-3 w-3 inline" /> Ingest
												</button>
												<button onclick={() => deleteChannel(ch.channelName ?? '')}
													disabled={deletingChannel === ch.channelName}
													class="rounded border px-2 py-1 text-xs text-red-500 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50">
													{#if deletingChannel === ch.channelName}
														<RefreshCw class="h-3 w-3 animate-spin inline" />
													{:else}
														Delete
													{/if}
												</button>
											</div>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>

			<!-- Batch Ingest card -->
			<div class="rounded-lg border p-5 space-y-3">
				<h2 class="font-semibold flex items-center gap-2"><Send class="h-4 w-4 text-indigo-600" /> Batch Ingest</h2>
				<div class="grid gap-3 sm:grid-cols-2">
					<div>
						<label for="batch-channel" class="block text-xs font-medium mb-1">Channel name *</label>
						<input id="batch-channel" bind:value={batchChannelName} placeholder="my-channel"
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<div>
						<label for="batch-msgid" class="block text-xs font-medium mb-1">Message ID *</label>
						<input id="batch-msgid" bind:value={batchMessageId} placeholder="msg-001"
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
				</div>
				<div>
					<label for="batch-payload" class="block text-xs font-medium mb-1">Payload (JSON)</label>
					<textarea id="batch-payload" bind:value={batchPayload} rows={3}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"></textarea>
				</div>
				<button onclick={batchIngest} disabled={batchBusy}
					class="flex items-center gap-2 rounded-md bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700 disabled:opacity-50">
					{#if batchBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Send class="h-4 w-4" />{/if}
					Ingest Message
				</button>
			</div>

			<!-- Sample Channel Data card -->
			<div class="rounded-lg border p-5 space-y-3">
				<h2 class="font-semibold flex items-center gap-2"><Eye class="h-4 w-4 text-indigo-600" /> Sample Channel Data</h2>
				<div class="flex gap-3 flex-wrap items-end">
					<div class="flex-1 min-w-36">
						<label for="sample-channel" class="block text-xs font-medium mb-1">Channel name *</label>
						<input id="sample-channel" bind:value={sampleChannelTarget} placeholder="my-channel"
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<div>
						<label for="sample-max" class="block text-xs font-medium mb-1">Max messages (1-10)</label>
						<input id="sample-max" type="number" min={1} max={10} bind:value={sampleMaxMessages}
							class="w-24 rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<button onclick={sampleData} disabled={sampleBusy}
						class="flex items-center gap-2 rounded-md border px-4 py-2 text-sm hover:bg-accent disabled:opacity-50">
						{#if sampleBusy}<RefreshCw class="h-4 w-4 animate-spin" />{:else}<Eye class="h-4 w-4" />{/if}
						Sample
					</button>
				</div>
				{#if sampleResults.length > 0}
					<div class="space-y-2">
						<p class="text-xs text-muted-foreground">{sampleResults.length} message(s)</p>
						{#each sampleResults as payload, i}
							<details class="rounded-md border">
								<summary class="cursor-pointer px-3 py-2 text-xs font-medium hover:bg-accent">Message {i + 1}</summary>
								<pre class="px-3 pb-2 text-xs overflow-auto max-h-32">{payload}</pre>
							</details>
						{/each}
					</div>
				{/if}
			</div>
		</div>
	{/if}

	<!-- DATASTORES TAB -->
	{#if activeTab === 'datastores'}
		<div class="rounded-lg border p-5 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="font-semibold flex items-center gap-2"><Database class="h-4 w-4 text-indigo-600" /> Datastores</h2>
				<div class="flex gap-2">
					<button onclick={loadDatastores} disabled={datastoresBusy}
						class="rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50">
						<RefreshCw class="h-4 w-4 {datastoresBusy ? 'animate-spin' : ''}" />
					</button>
					<button onclick={() => (showCreateDatastore = true)}
						class="flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700">
						<Plus class="h-4 w-4" /> Create
					</button>
				</div>
			</div>
			{#if datastores.length === 0}
				<div class="flex flex-col items-center py-8 text-muted-foreground">
					<Database class="h-10 w-10 mb-2 opacity-20" />
					<p class="text-sm">No datastores</p>
				</div>
			{:else}
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Name</th>
								<th class="px-4 py-3 text-left font-medium">Status</th>
								<th class="px-4 py-3 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each datastores as ds}
								<tr class="hover:bg-muted/30">
									<td class="px-4 py-3 font-mono text-xs">{ds.datastoreName}</td>
									<td class="px-4 py-3">
										<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900 dark:text-green-300">
											{ds.status ?? 'ACTIVE'}
										</span>
									</td>
									<td class="px-4 py-3 text-right">
										<button onclick={() => deleteDatastore(ds.datastoreName ?? '')}
											disabled={deletingDatastore === ds.datastoreName}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50">
											{#if deletingDatastore === ds.datastoreName}
												<RefreshCw class="h-4 w-4 animate-spin" />
											{:else}
												<Trash2 class="h-4 w-4" />
											{/if}
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- DATASETS TAB -->
	{#if activeTab === 'datasets'}
		<div class="space-y-4">
			<div class="rounded-lg border p-5 space-y-4">
				<div class="flex items-center justify-between">
					<h2 class="font-semibold flex items-center gap-2"><BarChart2 class="h-4 w-4 text-indigo-600" /> Datasets</h2>
					<div class="flex gap-2">
						<button onclick={loadDatasets} disabled={datasetsBusy}
							class="rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50">
							<RefreshCw class="h-4 w-4 {datasetsBusy ? 'animate-spin' : ''}" />
						</button>
						<button onclick={() => (showCreateDataset = true)}
							class="flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700">
							<Plus class="h-4 w-4" /> Create
						</button>
					</div>
				</div>
				{#if datasets.length === 0}
					<div class="flex flex-col items-center py-8 text-muted-foreground">
						<BarChart2 class="h-10 w-10 mb-2 opacity-20" />
						<p class="text-sm">No datasets</p>
					</div>
				{:else}
					<div class="rounded-lg border overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-muted/50">
								<tr>
									<th class="px-4 py-3 text-left font-medium">Name</th>
									<th class="px-4 py-3 text-left font-medium">Status</th>
									<th class="px-4 py-3 text-right font-medium">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y">
								{#each datasets as ds}
									<tr onclick={() => selectDataset(ds.datasetName ?? '')}
										class="hover:bg-muted/30 cursor-pointer {selectedDataset === ds.datasetName ? 'bg-indigo-50 dark:bg-indigo-950/30' : ''}">
										<td class="px-4 py-3 font-mono text-xs">{ds.datasetName}</td>
										<td class="px-4 py-3">
											<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900 dark:text-green-300">
												{ds.status ?? 'ACTIVE'}
											</span>
										</td>
										<td class="px-4 py-3 text-right">
											<div class="flex justify-end gap-1">
												<button onclick={(e) => { e.stopPropagation(); triggerContent(ds.datasetName ?? ''); }}
													disabled={triggerBusy === ds.datasetName}
													class="rounded border px-2 py-1 text-xs hover:bg-accent disabled:opacity-50">
													{#if triggerBusy === ds.datasetName}
														<RefreshCw class="h-3 w-3 animate-spin inline" />
													{:else}
														Run
													{/if}
												</button>
												<button onclick={(e) => { e.stopPropagation(); deleteDataset(ds.datasetName ?? ''); }}
													disabled={deletingDataset === ds.datasetName}
													class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50">
													{#if deletingDataset === ds.datasetName}
														<RefreshCw class="h-4 w-4 animate-spin" />
													{:else}
														<Trash2 class="h-4 w-4" />
													{/if}
												</button>
											</div>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>

			<!-- Dataset contents panel -->
			{#if selectedDataset}
				<div class="rounded-lg border bg-muted/30 p-5 space-y-3">
					<div class="flex items-center justify-between">
						<span class="text-sm font-medium">Content versions — <span class="font-mono text-indigo-600">{selectedDataset}</span></span>
						<div class="flex gap-2">
							<button onclick={() => loadContents(selectedDataset ?? '')} disabled={contentsBusy}
								class="rounded p-1 hover:bg-accent disabled:opacity-50">
								<RefreshCw class="h-4 w-4 {contentsBusy ? 'animate-spin' : ''}" />
							</button>
							<button onclick={() => { selectedDataset = null; datasetContents = []; }}
								class="text-xs text-muted-foreground hover:text-foreground">Close</button>
						</div>
					</div>
					{#if contentsBusy}
						<div class="flex items-center gap-2 text-sm text-muted-foreground py-4 justify-center">
							<RefreshCw class="h-4 w-4 animate-spin" /> Loading...
						</div>
					{:else if datasetContents.length === 0}
						<p class="text-sm text-muted-foreground py-4 text-center">No content versions. Click "Run" to generate content.</p>
					{:else}
						<div class="rounded-lg border overflow-hidden">
							<table class="w-full text-xs">
								<thead class="bg-muted/50">
									<tr>
										<th class="px-3 py-2 text-left font-medium">Version ID</th>
										<th class="px-3 py-2 text-left font-medium">State</th>
										<th class="px-3 py-2 text-left font-medium">Created</th>
										<th class="px-3 py-2 text-left font-medium">Completed</th>
									</tr>
								</thead>
								<tbody class="divide-y">
									{#each datasetContents as c}
										<tr class="hover:bg-muted/30">
											<td class="px-3 py-2 font-mono">{c.version ?? '—'}</td>
											<td class="px-3 py-2">
												<span class="rounded-full px-2 py-0.5 text-xs {contentStateBadge(c.status?.state)}">
													{c.status?.state ?? '—'}
												</span>
											</td>
											<td class="px-3 py-2 text-muted-foreground">
												{c.creationTime ? new Date(c.creationTime).toLocaleString() : '—'}
											</td>
											<td class="px-3 py-2 text-muted-foreground">
												{c.completionTime ? new Date(c.completionTime).toLocaleString() : '—'}
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
	{/if}

	<!-- PIPELINES TAB -->
	{#if activeTab === 'pipelines'}
		<div class="rounded-lg border p-5 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="font-semibold flex items-center gap-2"><GitBranch class="h-4 w-4 text-indigo-600" /> Pipelines</h2>
				<div class="flex gap-2">
					<button onclick={loadPipelines} disabled={pipelinesBusy}
						class="rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50">
						<RefreshCw class="h-4 w-4 {pipelinesBusy ? 'animate-spin' : ''}" />
					</button>
					<button onclick={() => (showCreatePipeline = true)}
						class="flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700">
						<Plus class="h-4 w-4" /> Create
					</button>
				</div>
			</div>
			{#if pipelines.length === 0}
				<div class="flex flex-col items-center py-8 text-muted-foreground">
					<GitBranch class="h-10 w-10 mb-2 opacity-20" />
					<p class="text-sm">No pipelines</p>
				</div>
			{:else}
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Name</th>
								<th class="px-4 py-3 text-left font-medium">Reprocessings</th>
								<th class="px-4 py-3 text-left font-medium">Created</th>
								<th class="px-4 py-3 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody>
							{#each pipelines as p}
								<tr class="border-b hover:bg-muted/30">
									<td class="px-4 py-3 font-mono text-xs">{p.pipelineName}</td>
									<td class="px-4 py-3 text-xs">
										{#if (p.reprocessingSummaries?.length ?? 0) > 0}
											<button onclick={() => expandPipeline(p.pipelineName ?? '')}
												class="text-indigo-600 hover:underline">
												{p.reprocessingSummaries?.length ?? 0} job(s) ↓
											</button>
										{:else}
											<span class="text-muted-foreground">None</span>
										{/if}
									</td>
									<td class="px-4 py-3 text-xs text-muted-foreground">
										{p.creationTime ? new Date(p.creationTime).toLocaleString() : '—'}
									</td>
									<td class="px-4 py-3 text-right">
										<div class="flex justify-end gap-1">
											<button onclick={() => startReprocessing(p.pipelineName ?? '')}
												disabled={reprocessBusy === p.pipelineName}
												class="rounded border px-2 py-1 text-xs hover:bg-accent disabled:opacity-50">
												{#if reprocessBusy === p.pipelineName}
													<RefreshCw class="h-3 w-3 animate-spin inline" />
												{:else}
													Reprocess
												{/if}
											</button>
											<button onclick={() => deletePipeline(p.pipelineName ?? '')}
												disabled={deletingPipeline === p.pipelineName}
												class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950 disabled:opacity-50">
												{#if deletingPipeline === p.pipelineName}
													<RefreshCw class="h-4 w-4 animate-spin" />
												{:else}
													<Trash2 class="h-4 w-4" />
												{/if}
											</button>
										</div>
									</td>
								</tr>
								{#if expandedPipeline === p.pipelineName}
									<tr class="border-b bg-muted/20">
										<td colspan={4} class="px-6 py-3">
											{#if (pipelineReprocessings[p.pipelineName ?? ''] ?? []).length === 0}
												<p class="text-xs text-muted-foreground">No reprocessing details available.</p>
											{:else}
												<div class="space-y-1">
													{#each pipelineReprocessings[p.pipelineName ?? ''] ?? [] as rp}
														<div class="flex items-center gap-3 text-xs">
															<span class="font-mono text-muted-foreground">{rp.id}</span>
															<span class="rounded-full px-2 py-0.5 {reprocessingStatusBadge(rp.status)}">{rp.status}</span>
															<button onclick={() => cancelReprocessing(p.pipelineName ?? '', rp.id ?? '')}
																disabled={cancelBusy === rp.id || rp.status === 'CANCELLED'}
																class="ml-auto rounded border px-2 py-0.5 hover:bg-accent disabled:opacity-50">
																{cancelBusy === rp.id ? 'Cancelling...' : 'Cancel'}
															</button>
														</div>
													{/each}
												</div>
											{/if}
										</td>
									</tr>
								{/if}
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- LOGGING TAB -->
	{#if activeTab === 'logging'}
		<div class="rounded-lg border p-5 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="font-semibold flex items-center gap-2"><Settings class="h-4 w-4 text-indigo-600" /> Logging Options</h2>
				<button onclick={loadLogging} disabled={loggingBusy}
					class="rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50">
					<RefreshCw class="h-4 w-4 {loggingBusy ? 'animate-spin' : ''}" />
				</button>
			</div>
			{#if loggingLoaded}
				<form onsubmit={(e) => { e.preventDefault(); saveLogging(); }} class="space-y-4">
					<div>
						<label for="log-role" class="block text-sm font-medium mb-1">Role ARN</label>
						<input id="log-role" bind:value={loggingRoleArn} placeholder="arn:aws:iam::000000000000:role/IoTAnalyticsRole"
							class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary" />
					</div>
					<div class="grid gap-4 sm:grid-cols-2">
						<div>
							<label for="log-level" class="block text-sm font-medium mb-1">Level</label>
							<select id="log-level" bind:value={loggingLevel}
								class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary">
								<option value="ERROR">ERROR</option>
								<option value="WARNING">WARNING</option>
								<option value="INFO">INFO</option>
							</select>
						</div>
						<div class="flex items-center gap-3 pt-6">
							<input id="log-enabled" type="checkbox" bind:checked={loggingEnabled} class="h-4 w-4 rounded" />
							<label for="log-enabled" class="text-sm font-medium">Enabled</label>
						</div>
					</div>
					<button type="submit" disabled={loggingBusy}
						class="flex items-center gap-2 rounded-md bg-indigo-600 px-4 py-2 text-sm text-white hover:bg-indigo-700 disabled:opacity-50">
						{#if loggingBusy}<RefreshCw class="h-4 w-4 animate-spin" />{/if}
						Save Logging Options
					</button>
				</form>
			{:else}
				<div class="flex items-center gap-2 text-sm text-muted-foreground py-4 justify-center">
					<RefreshCw class="h-4 w-4 animate-spin" /> Loading...
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Modals -->
{#if showCreateChannel}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold">Create Channel</h3>
			<form onsubmit={(e) => { e.preventDefault(); createChannel(); }} class="space-y-4">
				<input id="channel-name" use:focusOnMount bind:value={newChannelName} placeholder="Channel name"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
				<p class="text-xs text-muted-foreground">Letters, digits, underscores, hyphens. 1-128 chars.</p>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateChannel = false; newChannelName = ''; }} class="px-4 py-2 text-sm">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showCreateDatastore}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold">Create Datastore</h3>
			<form onsubmit={(e) => { e.preventDefault(); createDatastore(); }} class="space-y-4">
				<input use:focusOnMount bind:value={newDatastoreName} placeholder="Datastore name"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
				<p class="text-xs text-muted-foreground">Letters, digits, underscores, hyphens. 1-128 chars.</p>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateDatastore = false; newDatastoreName = ''; }} class="px-4 py-2 text-sm">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showCreateDataset}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold">Create Dataset</h3>
			<form onsubmit={(e) => { e.preventDefault(); createDataset(); }} class="space-y-4">
				<input use:focusOnMount bind:value={newDatasetName} placeholder="Dataset name"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
				<p class="text-xs text-muted-foreground">Letters, digits, underscores, hyphens. 1-128 chars.</p>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateDataset = false; newDatasetName = ''; }} class="px-4 py-2 text-sm">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}

{#if showCreatePipeline}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold">Create Pipeline</h3>
			<form onsubmit={(e) => { e.preventDefault(); createPipeline(); }} class="space-y-4">
				<input use:focusOnMount bind:value={newPipelineName} placeholder="Pipeline name"
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
				<p class="text-xs text-muted-foreground">Letters, digits, underscores, hyphens. 1-128 chars.</p>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreatePipeline = false; newPipelineName = ''; }} class="px-4 py-2 text-sm">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}
