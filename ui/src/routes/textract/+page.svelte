<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getTextractClient } from '$lib/aws-client';
	import {
		ListAdaptersCommand,
		ListAdapterVersionsCommand,
		StartDocumentAnalysisCommand,
		GetDocumentAnalysisCommand,
		AnalyzeDocumentCommand,
		type AdapterOverview,
		type AdapterVersionOverview,
		type FeatureType,
		type Block
	} from '@aws-sdk/client-textract';
	import { toast } from 'svelte-sonner';
	import { ScanLine, RefreshCw, Search, FileText, Layers, Activity, Play, CheckCircle, XCircle, Upload, Download } from 'lucide-svelte';

	const client = regionalClient(getTextractClient);

	let loading = $state(false);
	let activeTab = $state<'adapters' | 'versions' | 'analysis'>('adapters');
	let searchQuery = $state('');
	let adapters = $state<AdapterOverview[]>([]);
	let versions = $state<AdapterVersionOverview[]>([]);
	let selectedAdapterId = $state<string | null>(null);

	// Document analysis state
	let analysisMode = $state<'s3' | 'upload'>('s3');
	let analysisBucket = $state('');
	let analysisKey = $state('');
	let analysisJobId = $state('');
	let analysisBlocks = $state<Block[]>([]);
	let analysisStatus = $state('');
	let analysisLoading = $state(false);
	// Feature-type selection (was hard-coded TABLES+FORMS).
	let featTables = $state(true);
	let featForms = $state(true);
	let featSignatures = $state(false);
	let featLayout = $state(false);
	// Local upload state.
	let uploadFileName = $state('');
	let uploadBytes = $state<Uint8Array | null>(null);

	const selectedFeatures = $derived(
		[
			featTables ? 'TABLES' : null,
			featForms ? 'FORMS' : null,
			featSignatures ? 'SIGNATURES' : null,
			featLayout ? 'LAYOUT' : null
		].filter(Boolean) as FeatureType[]
	);

	const filteredAdapters = $derived(adapters.filter((a) => (a.AdapterId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredVersions = $derived(versions.filter((v) => (v.AdapterId ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const adaptersResp = await client().send(new ListAdaptersCommand({}));
			adapters = adaptersResp.Adapters ?? [];
			if (adapters.length > 0 && selectedAdapterId === null) {
				selectedAdapterId = adapters[0].AdapterId ?? null;
			}
			if (selectedAdapterId) {
				const versionsResp = await client().send(new ListAdapterVersionsCommand({ AdapterId: selectedAdapterId }));
				versions = versionsResp.AdapterVersions ?? [];
			} else {
				versions = [];
			}
		} catch (e) {
			toast.error('Failed to load Textract data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function onFileSelected(e: Event) {
		const input = e.target as HTMLInputElement;
		const file = input.files?.[0];
		if (!file) return;
		uploadFileName = file.name;
		uploadBytes = new Uint8Array(await file.arrayBuffer());
	}

	async function startAnalysis() {
		if (selectedFeatures.length === 0) {
			toast.error('Select at least one feature type');
			return;
		}
		analysisLoading = true;
		analysisBlocks = [];
		analysisStatus = '';
		analysisJobId = '';
		try {
			if (analysisMode === 'upload') {
				// Synchronous analysis on locally-uploaded document bytes.
				if (!uploadBytes) {
					toast.error('Choose a document file first');
					return;
				}
				const resp = await client().send(
					new AnalyzeDocumentCommand({
						Document: { Bytes: uploadBytes },
						FeatureTypes: selectedFeatures
					})
				);
				analysisStatus = 'SUCCEEDED';
				analysisBlocks = resp.Blocks ?? [];
				toast.success(`Analyzed "${uploadFileName}" (${analysisBlocks.length} blocks)`);
			} else {
				if (!analysisBucket || !analysisKey) {
					toast.error('S3 Bucket and Key are required');
					return;
				}
				const resp = await client().send(
					new StartDocumentAnalysisCommand({
						DocumentLocation: { S3Object: { Bucket: analysisBucket, Name: analysisKey } },
						FeatureTypes: selectedFeatures
					})
				);
				const jobId = resp.JobId ?? '';
				analysisJobId = jobId;
				toast.success('Analysis job started: ' + jobId);
				await pollAnalysis(jobId);
			}
		} catch (e) {
			toast.error('Failed to analyze document: ' + String(e));
		} finally {
			analysisLoading = false;
		}
	}

	async function pollAnalysis(jobId: string) {
		const resp = await client().send(new GetDocumentAnalysisCommand({ JobId: jobId }));
		analysisStatus = resp.JobStatus ?? '';
		analysisBlocks = resp.Blocks ?? [];
	}

	function exportResultJson() {
		const blob = new Blob([JSON.stringify(analysisBlocks, null, 2)], { type: 'application/json' });
		const url = URL.createObjectURL(blob);
		const a = document.createElement('a');
		a.href = url;
		a.download = `textract-result-${analysisJobId || uploadFileName || 'analysis'}.json`;
		a.click();
		URL.revokeObjectURL(url);
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ScanLine class="w-7 h-7 text-blue-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Textract</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Automatically extract text and data from scanned documents</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Layers class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{adapters.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Adapters</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Activity class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{versions.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Adapter Versions</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><FileText class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{versions.filter((v) => v.Status === 'ACTIVE').length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active Versions</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['adapters', 'Adapters'], ['versions', 'Adapter Versions'], ['analysis', 'Document Analysis']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			{#if activeTab !== 'analysis'}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
				</div>
			{/if}
		</div>
		<div class="p-4">
			{#if loading && activeTab !== 'analysis'}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'adapters'}
				{#if filteredAdapters.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No adapters found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAdapters as adapter}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Layers class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-gray-900 dark:text-white">{adapter.AdapterId}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{adapter.AdapterName}</p>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'versions'}
				{#if filteredVersions.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No adapter versions found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredVersions as ver}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-green-500" />
									<p class="font-medium text-gray-900 dark:text-white">{ver.AdapterVersion}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {ver.Status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{ver.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
				{:else if activeTab === 'analysis'}
					<div class="space-y-6">
						<div class="space-y-4">
							<div class="flex items-center gap-2">
								<h2 class="text-base font-semibold text-gray-900 dark:text-white">Document Input</h2>
								<div class="flex gap-1 ml-auto">
									{#each [['s3', 'S3 Object'], ['upload', 'Local Upload']] as [m, lbl]}
										<button onclick={() => (analysisMode = m as typeof analysisMode)} class="px-3 py-1 text-xs rounded-lg font-medium {analysisMode === m ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300'}">{lbl}</button>
									{/each}
								</div>
							</div>

							{#if analysisMode === 's3'}
								<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
									<div>
										<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="analysis-bucket">S3 Bucket</label>
										<input id="analysis-bucket" bind:value={analysisBucket} placeholder="my-bucket" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									</div>
									<div>
										<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="analysis-key">S3 Key</label>
										<input id="analysis-key" bind:value={analysisKey} placeholder="documents/file.pdf" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									</div>
								</div>
							{:else}
								<div>
									<label class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1" for="analysis-file">Document file (PNG / JPEG / PDF)</label>
									<input id="analysis-file" type="file" accept="image/png,image/jpeg,application/pdf" onchange={onFileSelected} class="block w-full text-sm text-gray-700 dark:text-gray-300 file:mr-3 file:rounded-lg file:border-0 file:bg-blue-600 file:px-3 file:py-2 file:text-white" />
									{#if uploadFileName}
										<p class="mt-1 text-xs text-gray-500 dark:text-gray-400 flex items-center gap-1"><Upload class="w-3.5 h-3.5" /> {uploadFileName} ({uploadBytes?.length ?? 0} bytes)</p>
									{/if}
								</div>
							{/if}

							<div>
								<p class="text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Feature types</p>
								<div class="flex flex-wrap gap-4">
									<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={featTables} class="rounded" /> Tables</label>
									<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={featForms} class="rounded" /> Forms</label>
									<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={featSignatures} class="rounded" /> Signatures</label>
									<label class="flex items-center gap-1.5 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={featLayout} class="rounded" /> Layout</label>
								</div>
							</div>

							<button
								onclick={startAnalysis}
								disabled={analysisLoading}
								class="flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed">
								<Play class="w-4 h-4" />
								{analysisLoading ? 'Running...' : analysisMode === 'upload' ? 'Analyze Document' : 'Start Document Analysis'}
							</button>
						</div>

						{#if analysisStatus || analysisBlocks.length > 0}
							<div class="border-t border-slate-200 dark:border-slate-700 pt-4 space-y-3">
								<div class="flex items-center gap-2">
									{#if analysisJobId}
										<span class="text-sm font-medium text-gray-700 dark:text-gray-300">Job ID:</span>
										<span class="text-sm font-mono text-gray-900 dark:text-white">{analysisJobId}</span>
									{/if}
									{#if analysisStatus === 'SUCCEEDED'}
										<CheckCircle class="w-4 h-4 text-green-500" />
									{:else if analysisStatus === 'FAILED'}
										<XCircle class="w-4 h-4 text-red-500" />
									{/if}
									{#if analysisStatus}
										<span class="text-xs px-2 py-0.5 rounded-full {analysisStatus === 'SUCCEEDED' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : analysisStatus === 'FAILED' ? 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400' : 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400'}">{analysisStatus}</span>
									{/if}
									{#if analysisBlocks.length > 0}
										<button onclick={exportResultJson} class="ml-auto flex items-center gap-1.5 px-3 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">
											<Download class="w-3.5 h-3.5" /> Export JSON
										</button>
									{/if}
								</div>

								{#if analysisBlocks.length > 0}
									<div>
										<h3 class="text-sm font-semibold text-gray-900 dark:text-white mb-2">Result Blocks ({analysisBlocks.length})</h3>
										<div class="space-y-1 max-h-96 overflow-y-auto">
											{#each analysisBlocks as block}
												<div class="flex items-start gap-3 p-2 rounded bg-gray-50 dark:bg-slate-700/50 text-sm">
													<span class="text-xs font-mono px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400 shrink-0">{block.BlockType}</span>
													{#if block.Text}
														<span class="text-gray-900 dark:text-white flex-1 break-all">{block.Text}</span>
													{/if}
													{#if block.Confidence != null}
														<span class="text-xs text-gray-400 shrink-0">{block.Confidence?.toFixed(1)}%</span>
													{/if}
												</div>
											{/each}
										</div>
									</div>
								{/if}
							</div>
						{/if}
					</div>
			{/if}
		</div>
	</div>
</div>
