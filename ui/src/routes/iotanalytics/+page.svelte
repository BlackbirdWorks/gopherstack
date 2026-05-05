<script lang="ts">
	import { onMount } from 'svelte';
	import { getIoTAnalyticsClient } from '$lib/aws-client';
	import {
		ListChannelsCommand,
		CreateChannelCommand,
		DeleteChannelCommand,
		ListDatasetsCommand,
		CreateDatasetCommand,
		DeleteDatasetCommand,
		CreateDatasetContentCommand,
		ListDatasetContentsCommand,
		ListPipelinesCommand,
		CreatePipelineCommand,
		DeletePipelineCommand,
		StartPipelineReprocessingCommand
	} from '@aws-sdk/client-iotanalytics';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Trash2, Plus, Database, GitBranch, Radio } from 'lucide-svelte';

	const iotAnalytics = getIoTAnalyticsClient();

	type Tab = 'channels' | 'datasets' | 'pipelines';
	let activeTab = $state<Tab>('channels');

	// ---- Channels ----
	let channels = $state<Array<{ channelName?: string }>>([]);
	let channelsBusy = $state(false);
	let newChannelName = $state('');
	let showCreateChannel = $state(false);

	async function loadChannels() {
		channelsBusy = true;
		try {
			const out = await iotAnalytics.send(new ListChannelsCommand({}));
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
			await iotAnalytics.send(new CreateChannelCommand({ channelName: newChannelName.trim() }));
			showCreateChannel = false;
			newChannelName = '';
			await loadChannels();
			toast.success('Channel created');
		} catch (err: unknown) {
			toast.error(`Failed to create channel: ${(err as Error).message}`);
		}
	}

	async function deleteChannel(name: string) {
		try {
			await iotAnalytics.send(new DeleteChannelCommand({ channelName: name }));
			await loadChannels();
			toast.success(`Channel "${name}" deleted`);
		} catch (err: unknown) {
			toast.error(`Failed to delete channel: ${(err as Error).message}`);
		}
	}

	// ---- Datasets ----
	type DatasetSummary = { datasetName?: string; status?: string };
	type ContentSummary = { version?: string; status?: { state?: string }; creationTime?: Date };

	let datasets = $state<DatasetSummary[]>([]);
	let datasetsBusy = $state(false);
	let newDatasetName = $state('');
	let showCreateDataset = $state(false);
	let selectedDataset = $state<string | null>(null);
	let datasetContents = $state<ContentSummary[]>([]);
	let contentsBusy = $state(false);
	let triggerBusy = $state<string | null>(null);

	async function loadDatasets() {
		datasetsBusy = true;
		try {
			const out = await iotAnalytics.send(new ListDatasetsCommand({}));
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
			await iotAnalytics.send(new CreateDatasetCommand({ datasetName: newDatasetName.trim(), actions: [{ actionName: 'default' }] }));
			showCreateDataset = false;
			newDatasetName = '';
			await loadDatasets();
			toast.success('Dataset created');
		} catch (err: unknown) {
			toast.error(`Failed to create dataset: ${(err as Error).message}`);
		}
	}

	async function deleteDataset(name: string) {
		try {
			await iotAnalytics.send(new DeleteDatasetCommand({ datasetName: name }));
			if (selectedDataset === name) { selectedDataset = null; datasetContents = []; }
			await loadDatasets();
			toast.success(`Dataset "${name}" deleted`);
		} catch (err: unknown) {
			toast.error(`Failed to delete dataset: ${(err as Error).message}`);
		}
	}

	async function selectDataset(name: string) {
		selectedDataset = name;
		await loadContents(name);
	}

	async function loadContents(name: string) {
		contentsBusy = true;
		try {
			const out = await iotAnalytics.send(new ListDatasetContentsCommand({ datasetName: name }));
			datasetContents = (out.datasetContentSummaries ?? []).map((c) => ({
				version: c.version,
				status: c.status ? { state: c.status.state } : undefined,
				creationTime: c.creationTime
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
			await iotAnalytics.send(new CreateDatasetContentCommand({ datasetName: name }));
			toast.success('Content generation triggered');
			await loadContents(name);
		} catch (err: unknown) {
			toast.error(`Failed to trigger content: ${(err as Error).message}`);
		} finally {
			triggerBusy = null;
		}
	}

	// ---- Pipelines ----
	type PipelineSummary = { pipelineName?: string; creationTime?: Date };

	let pipelines = $state<PipelineSummary[]>([]);
	let pipelinesBusy = $state(false);
	let newPipelineName = $state('');
	let showCreatePipeline = $state(false);
	let reprocessBusy = $state<string | null>(null);

	async function loadPipelines() {
		pipelinesBusy = true;
		try {
			const out = await iotAnalytics.send(new ListPipelinesCommand({}));
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
			await iotAnalytics.send(new CreatePipelineCommand({ pipelineName: newPipelineName.trim(), pipelineActivities: [{ channel: { name: 'channel', channelName: 'default', next: '' } }] }));
			showCreatePipeline = false;
			newPipelineName = '';
			await loadPipelines();
			toast.success('Pipeline created');
		} catch (err: unknown) {
			toast.error(`Failed to create pipeline: ${(err as Error).message}`);
		}
	}

	async function deletePipeline(name: string) {
		try {
			await iotAnalytics.send(new DeletePipelineCommand({ pipelineName: name }));
			await loadPipelines();
			toast.success(`Pipeline "${name}" deleted`);
		} catch (err: unknown) {
			toast.error(`Failed to delete pipeline: ${(err as Error).message}`);
		}
	}

	async function startReprocessing(name: string) {
		reprocessBusy = name;
		try {
			await iotAnalytics.send(new StartPipelineReprocessingCommand({ pipelineName: name }));
			toast.success(`Reprocessing started for "${name}"`);
		} catch (err: unknown) {
			toast.error(`Failed to start reprocessing: ${(err as Error).message}`);
		} finally {
			reprocessBusy = null;
		}
	}

	onMount(() => {
		void loadChannels();
		void loadDatasets();
		void loadPipelines();
	});

	const tabs: { id: Tab; label: string }[] = [
		{ id: 'channels', label: 'Channels' },
		{ id: 'datasets', label: 'Datasets' },
		{ id: 'pipelines', label: 'Pipelines' }
	];
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center gap-3">
		<Radio class="h-8 w-8 text-indigo-600" />
		<div>
			<h1 class="text-2xl font-bold">IoT Analytics</h1>
			<p class="text-sm text-muted-foreground">Manage channels, datasets, and pipelines</p>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex border-b">
		{#each tabs as tab}
			<button
				onclick={() => (activeTab = tab.id)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id
					? 'border-primary text-primary'
					: 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Channels Tab -->
	{#if activeTab === 'channels'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2">
					<Radio class="h-5 w-5 text-indigo-600" />
					<h2 class="font-semibold">Channels</h2>
					<span class="rounded-full bg-muted px-2 py-0.5 text-xs">{channels.length}</span>
				</div>
				<div class="flex gap-2">
					<button
						onclick={loadChannels}
						disabled={channelsBusy}
						class="flex items-center gap-1 rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
					>
						<RefreshCw class="h-4 w-4 {channelsBusy ? 'animate-spin' : ''}" />
					</button>
					<button
						onclick={() => (showCreateChannel = true)}
						class="flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700"
					>
						<Plus class="h-4 w-4" />
						Create
					</button>
				</div>
			</div>

			{#if channels.length === 0}
				<div class="flex flex-col items-center justify-center py-10 text-muted-foreground">
					<Radio class="h-10 w-10 mb-2 opacity-30" />
					<p class="text-sm">No channels found</p>
				</div>
			{:else}
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Name</th>
								<th class="px-4 py-3 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each channels as ch}
								<tr class="hover:bg-muted/30">
									<td class="px-4 py-3 font-mono text-xs">{ch.channelName}</td>
									<td class="px-4 py-3 text-right">
										<button
											onclick={() => deleteChannel(ch.channelName ?? '')}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											title="Delete channel"
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
		</div>
	{/if}

	<!-- Datasets Tab -->
	{#if activeTab === 'datasets'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2">
					<Database class="h-5 w-5 text-indigo-600" />
					<h2 class="font-semibold">Datasets</h2>
					<span class="rounded-full bg-muted px-2 py-0.5 text-xs">{datasets.length}</span>
				</div>
				<div class="flex gap-2">
					<button
						onclick={loadDatasets}
						disabled={datasetsBusy}
						class="flex items-center gap-1 rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
					>
						<RefreshCw class="h-4 w-4 {datasetsBusy ? 'animate-spin' : ''}" />
					</button>
					<button
						onclick={() => (showCreateDataset = true)}
						class="flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700"
					>
						<Plus class="h-4 w-4" />
						Create
					</button>
				</div>
			</div>

			{#if datasets.length === 0}
				<div class="flex flex-col items-center justify-center py-10 text-muted-foreground">
					<Database class="h-10 w-10 mb-2 opacity-30" />
					<p class="text-sm">No datasets found</p>
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
								<tr
									onclick={() => selectDataset(ds.datasetName ?? '')}
									class="hover:bg-muted/30 cursor-pointer {selectedDataset === ds.datasetName ? 'bg-indigo-50 dark:bg-indigo-950/30' : ''}"
								>
									<td class="px-4 py-3 font-mono text-xs">{ds.datasetName}</td>
									<td class="px-4 py-3">
										<span class="rounded-full bg-green-100 px-2 py-0.5 text-xs text-green-700 dark:bg-green-900 dark:text-green-300">
											{ds.status ?? 'ACTIVE'}
										</span>
									</td>
									<td class="px-4 py-3 text-right">
										<div class="flex justify-end gap-1">
											<button
												onclick={(e) => { e.stopPropagation(); triggerContent(ds.datasetName ?? ''); selectDataset(ds.datasetName ?? ''); }}
												disabled={triggerBusy === ds.datasetName}
												class="rounded border px-2 py-1 text-xs hover:bg-accent disabled:opacity-50"
												title="Trigger content generation"
											>
												{#if triggerBusy === ds.datasetName}
													<RefreshCw class="h-3 w-3 animate-spin inline" />
												{:else}
													Run
												{/if}
											</button>
											<button
												onclick={(e) => { e.stopPropagation(); deleteDataset(ds.datasetName ?? ''); }}
												class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
												title="Delete dataset"
											>
												<Trash2 class="h-4 w-4" />
											</button>
										</div>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}

			<!-- Dataset contents panel -->
			{#if selectedDataset}
				<div class="rounded-lg border bg-muted/30 p-4 space-y-3">
					<div class="flex items-center justify-between">
						<span class="text-sm font-medium">Contents — {selectedDataset}</span>
						<button
							onclick={() => loadContents(selectedDataset ?? '')}
							disabled={contentsBusy}
							class="rounded p-1 hover:bg-accent disabled:opacity-50"
						>
							<RefreshCw class="h-4 w-4 {contentsBusy ? 'animate-spin' : ''}" />
						</button>
					</div>

					{#if contentsBusy}
						<div class="flex items-center gap-2 text-sm text-muted-foreground">
							<RefreshCw class="h-4 w-4 animate-spin" /> Loading...
						</div>
					{:else if datasetContents.length === 0}
						<p class="text-sm text-muted-foreground">No content versions. Click "Run" to generate.</p>
					{:else}
						<div class="overflow-hidden rounded border">
							<table class="w-full text-xs">
								<thead class="bg-muted/50">
									<tr>
										<th class="px-3 py-2 text-left font-medium">Version ID</th>
										<th class="px-3 py-2 text-left font-medium">State</th>
										<th class="px-3 py-2 text-left font-medium">Created</th>
									</tr>
								</thead>
								<tbody class="divide-y">
									{#each datasetContents as c}
										<tr class="hover:bg-muted/30">
											<td class="px-3 py-2 font-mono">{c.version ?? '—'}</td>
											<td class="px-3 py-2">
												<span class="rounded-full bg-green-100 px-2 py-0.5 text-green-700 dark:bg-green-900 dark:text-green-300">
													{c.status?.state ?? '—'}
												</span>
											</td>
											<td class="px-3 py-2 text-muted-foreground">
												{c.creationTime ? new Date(c.creationTime).toLocaleString() : '—'}
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

	<!-- Pipelines Tab -->
	{#if activeTab === 'pipelines'}
		<div class="space-y-4 rounded-lg border p-6">
			<div class="flex items-center justify-between">
				<div class="flex items-center gap-2">
					<GitBranch class="h-5 w-5 text-indigo-600" />
					<h2 class="font-semibold">Pipelines</h2>
					<span class="rounded-full bg-muted px-2 py-0.5 text-xs">{pipelines.length}</span>
				</div>
				<div class="flex gap-2">
					<button
						onclick={loadPipelines}
						disabled={pipelinesBusy}
						class="flex items-center gap-1 rounded-md border px-3 py-1.5 text-sm hover:bg-accent disabled:opacity-50"
					>
						<RefreshCw class="h-4 w-4 {pipelinesBusy ? 'animate-spin' : ''}" />
					</button>
					<button
						onclick={() => (showCreatePipeline = true)}
						class="flex items-center gap-1 rounded-md bg-indigo-600 px-3 py-1.5 text-sm text-white hover:bg-indigo-700"
					>
						<Plus class="h-4 w-4" />
						Create
					</button>
				</div>
			</div>

			{#if pipelines.length === 0}
				<div class="flex flex-col items-center justify-center py-10 text-muted-foreground">
					<GitBranch class="h-10 w-10 mb-2 opacity-30" />
					<p class="text-sm">No pipelines found</p>
				</div>
			{:else}
				<div class="rounded-lg border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-4 py-3 text-left font-medium">Name</th>
								<th class="px-4 py-3 text-left font-medium">Created</th>
								<th class="px-4 py-3 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each pipelines as p}
								<tr class="hover:bg-muted/30">
									<td class="px-4 py-3 font-mono text-xs">{p.pipelineName}</td>
									<td class="px-4 py-3 text-xs text-muted-foreground">
										{p.creationTime ? new Date(p.creationTime).toLocaleString() : '—'}
									</td>
									<td class="px-4 py-3 text-right">
										<div class="flex justify-end gap-1">
											<button
												onclick={() => startReprocessing(p.pipelineName ?? '')}
												disabled={reprocessBusy === p.pipelineName}
												class="rounded border px-2 py-1 text-xs hover:bg-accent disabled:opacity-50"
												title="Start reprocessing"
											>
												{#if reprocessBusy === p.pipelineName}
													<RefreshCw class="h-3 w-3 animate-spin inline" />
												{:else}
													Reprocess
												{/if}
											</button>
											<button
												onclick={() => deletePipeline(p.pipelineName ?? '')}
												class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
												title="Delete pipeline"
											>
												<Trash2 class="h-4 w-4" />
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
	{/if}
</div>

<!-- Create Channel Modal -->
{#if showCreateChannel}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold">Create Channel</h3>
			<form onsubmit={(e) => { e.preventDefault(); createChannel(); }} class="space-y-4">
				<input
					bind:value={newChannelName}
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
					placeholder="Channel name"
				/>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateChannel = false; newChannelName = ''; }} class="px-4 py-2 text-sm">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Dataset Modal -->
{#if showCreateDataset}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold">Create Dataset</h3>
			<form onsubmit={(e) => { e.preventDefault(); createDataset(); }} class="space-y-4">
				<input
					bind:value={newDatasetName}
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
					placeholder="Dataset name"
				/>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreateDataset = false; newDatasetName = ''; }} class="px-4 py-2 text-sm">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Create Pipeline Modal -->
{#if showCreatePipeline}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold">Create Pipeline</h3>
			<form onsubmit={(e) => { e.preventDefault(); createPipeline(); }} class="space-y-4">
				<input
					bind:value={newPipelineName}
					class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
					placeholder="Pipeline name"
				/>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => { showCreatePipeline = false; newPipelineName = ''; }} class="px-4 py-2 text-sm">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}
