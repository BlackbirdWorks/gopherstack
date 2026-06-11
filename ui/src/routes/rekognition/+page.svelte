<script lang="ts">
	import { onMount } from 'svelte';
	import { getRekognitionClient } from '$lib/aws-client';
	import {
		ListCollectionsCommand,
		ListFacesCommand,
		ListStreamProcessorsCommand,
		DetectFacesCommand,
		StartStreamProcessorCommand,
		StopStreamProcessorCommand,
		type RekognitionClient,
		type StreamProcessor,
		type FaceDetail
	} from '@aws-sdk/client-rekognition';
	import { toast } from 'svelte-sonner';
	import { Eye, RefreshCw, Search, Users, Video, Database, ScanFace, Play, Square } from 'lucide-svelte';

	let reko: RekognitionClient | undefined;
	function client(): RekognitionClient {
		return (reko ??= getRekognitionClient());
	}

	let loading = $state(false);
	let activeTab = $state<'collections' | 'processors' | 'detect'>('collections');
	let searchQuery = $state('');
	let collections = $state<string[]>([]);
	let faces = $state<{ FaceId?: string; FaceModelVersion?: string }[]>([]);
	let processors = $state<StreamProcessor[]>([]);
	let selectedCollection = $state<string | null>(null);

	// Detect Faces (confidence + attributes) against an S3 image.
	let detectBucket = $state('');
	let detectKey = $state('');
	let detecting = $state(false);
	let detectedFaces = $state<FaceDetail[]>([]);
	let detectRan = $state(false);

	const filteredCollections = $derived(collections.filter((c) => c.toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredProcessors = $derived(processors.filter((p) => (p.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [collResp, procResp] = await Promise.all([
				client().send(new ListCollectionsCommand({})),
				client().send(new ListStreamProcessorsCommand({}))
			]);
			collections = collResp.CollectionIds ?? [];
			processors = procResp.StreamProcessors ?? [];
		} catch (e) {
			toast.error('Failed to load Rekognition data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadFaces(collectionId: string) {
		selectedCollection = collectionId;
		try {
			const resp = await client().send(new ListFacesCommand({ CollectionId: collectionId }));
			faces = resp.Faces ?? [];
		} catch (e) {
			toast.error('Failed to load faces: ' + String(e));
		}
	}

	async function detectFaces() {
		if (!detectBucket.trim() || !detectKey.trim()) {
			toast.error('S3 bucket and key are required');
			return;
		}
		detecting = true;
		detectRan = false;
		detectedFaces = [];
		try {
			const resp = await client().send(
				new DetectFacesCommand({
					Image: { S3Object: { Bucket: detectBucket.trim(), Name: detectKey.trim() } },
					Attributes: ['ALL']
				})
			);
			detectedFaces = resp.FaceDetails ?? [];
			detectRan = true;
			toast.success(`Detected ${detectedFaces.length} face(s)`);
		} catch (e) {
			toast.error('Failed to detect faces: ' + String(e));
		} finally {
			detecting = false;
		}
	}

	async function startProcessor(name: string) {
		try {
			await client().send(new StartStreamProcessorCommand({ Name: name }));
			toast.success(`Started ${name}`);
			await loadData();
		} catch (e) {
			toast.error('Failed to start processor: ' + String(e));
		}
	}

	async function stopProcessor(name: string) {
		try {
			await client().send(new StopStreamProcessorCommand({ Name: name }));
			toast.success(`Stopped ${name}`);
			await loadData();
		} catch (e) {
			toast.error('Failed to stop processor: ' + String(e));
		}
	}

	const pct = (v: number | undefined) => (v === undefined || v === null ? '-' : v.toFixed(1) + '%');
	const topEmotion = (f: FaceDetail) =>
		(f.Emotions ?? []).toSorted((a, b) => (b.Confidence ?? 0) - (a.Confidence ?? 0))[0]?.Type ?? '-';

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Eye class="w-7 h-7 text-violet-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Rekognition</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Image and video analysis using deep learning</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-violet-100 dark:bg-violet-900/30 rounded-lg"><Database class="w-5 h-5 text-violet-600 dark:text-violet-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{collections.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Face Collections</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Users class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{faces.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Indexed Faces</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Video class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{processors.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Stream Processors</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['collections', 'Face Collections'], ['processors', 'Stream Processors'], ['detect', 'Detect Faces']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-violet-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			{#if activeTab !== 'detect'}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
				</div>
			{/if}
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'collections'}
				{#if filteredCollections.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No face collections found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredCollections as collId}
							<button onclick={() => loadFaces(collId)}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left {selectedCollection === collId ? 'border border-violet-300 dark:border-violet-600' : ''}">
								<div class="flex items-center gap-3">
									<Database class="w-5 h-5 text-violet-500" />
									<p class="font-medium text-gray-900 dark:text-white">{collId}</p>
								</div>
								<span class="text-xs text-gray-400">Click to view faces →</span>
							</button>
						{/each}
					</div>
					{#if selectedCollection && faces.length > 0}
						<div class="mt-4">
							<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Faces in {selectedCollection} ({faces.length})</h3>
							<div class="space-y-2">
								{#each faces as face}
									<div class="flex items-center gap-3 p-2 rounded-lg bg-violet-50 dark:bg-violet-900/20">
										<Users class="w-4 h-4 text-violet-500" />
										<p class="text-sm text-gray-700 dark:text-gray-300">{face.FaceId}</p>
									</div>
								{/each}
							</div>
						</div>
					{/if}
				{/if}
			{:else if activeTab === 'processors'}
				{#if filteredProcessors.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No stream processors found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredProcessors as proc}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Video class="w-5 h-5 text-green-500" />
									<p class="font-medium text-gray-900 dark:text-white">{proc.Name}</p>
								</div>
								<div class="flex items-center gap-2">
									<span class="text-xs px-2 py-1 rounded-full {proc.Status === 'RUNNING' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{proc.Status}</span>
									{#if proc.Status === 'RUNNING'}
										<button onclick={() => proc.Name && stopProcessor(proc.Name)} title="Stop processor"
											class="flex items-center gap-1 px-2 py-1 rounded-lg bg-red-50 dark:bg-red-900/20 text-red-600 dark:text-red-400 text-xs hover:bg-red-100 dark:hover:bg-red-900/40">
											<Square class="w-3.5 h-3.5" /> Stop
										</button>
									{:else}
										<button onclick={() => proc.Name && startProcessor(proc.Name)} title="Start processor"
											class="flex items-center gap-1 px-2 py-1 rounded-lg bg-green-50 dark:bg-green-900/20 text-green-600 dark:text-green-400 text-xs hover:bg-green-100 dark:hover:bg-green-900/40">
											<Play class="w-3.5 h-3.5" /> Start
										</button>
									{/if}
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'detect'}
				<div class="space-y-4">
					<div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
						<div>
							<label for="detect-bucket" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Bucket</label>
							<input id="detect-bucket" bind:value={detectBucket} placeholder="my-bucket" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						</div>
						<div>
							<label for="detect-key" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Key (image)</label>
							<input id="detect-key" bind:value={detectKey} placeholder="images/photo.jpg" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						</div>
					</div>
					<button onclick={detectFaces} disabled={detecting}
						class="flex items-center gap-2 px-4 py-2 bg-violet-600 text-white rounded-lg text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
						<ScanFace class="w-4 h-4" /> {detecting ? 'Detecting...' : 'Detect Faces'}
					</button>

					{#if detectRan}
						{#if detectedFaces.length === 0}
							<div class="text-center py-8 text-gray-500 dark:text-gray-400">No faces detected</div>
						{:else}
							<div class="space-y-3">
								{#each detectedFaces as face, i}
									<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 space-y-2">
										<div class="flex items-center justify-between">
											<p class="font-medium text-gray-900 dark:text-white flex items-center gap-2"><Users class="w-4 h-4 text-violet-500" /> Face {i + 1}</p>
											<span class="text-xs text-gray-500 dark:text-gray-400">Confidence {pct(face.Confidence)}</span>
										</div>
										<div class="grid grid-cols-2 sm:grid-cols-3 gap-2 text-xs">
											<div><span class="text-gray-500 dark:text-gray-400">Age range</span><p class="text-gray-900 dark:text-white">{face.AgeRange?.Low ?? '-'}–{face.AgeRange?.High ?? '-'}</p></div>
											<div><span class="text-gray-500 dark:text-gray-400">Gender</span><p class="text-gray-900 dark:text-white">{face.Gender?.Value ?? '-'} ({pct(face.Gender?.Confidence)})</p></div>
											<div><span class="text-gray-500 dark:text-gray-400">Smile</span><p class="text-gray-900 dark:text-white">{face.Smile?.Value ? 'Yes' : 'No'} ({pct(face.Smile?.Confidence)})</p></div>
											<div><span class="text-gray-500 dark:text-gray-400">Eyeglasses</span><p class="text-gray-900 dark:text-white">{face.Eyeglasses?.Value ? 'Yes' : 'No'}</p></div>
											<div><span class="text-gray-500 dark:text-gray-400">Eyes open</span><p class="text-gray-900 dark:text-white">{face.EyesOpen?.Value ? 'Yes' : 'No'}</p></div>
											<div><span class="text-gray-500 dark:text-gray-400">Top emotion</span><p class="text-gray-900 dark:text-white">{topEmotion(face)}</p></div>
										</div>
									</div>
								{/each}
							</div>
						{/if}
					{/if}
				</div>
			{/if}
		</div>
	</div>
</div>
