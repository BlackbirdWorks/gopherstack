<script lang="ts">
	import { onMount } from 'svelte';
	import { getRekognitionClient } from '$lib/aws-client';
	import {
		ListCollectionsCommand,
		ListFacesCommand,
		ListStreamProcessorsCommand,
		type StreamProcessor
	} from '@aws-sdk/client-rekognition';
	import { toast } from 'svelte-sonner';
	import { Eye, RefreshCw, Search, Users, Video, Database } from 'lucide-svelte';

	const reko = getRekognitionClient();

	let loading = $state(false);
	let activeTab = $state<'collections' | 'processors'>('collections');
	let searchQuery = $state('');
	let collections = $state<string[]>([]);
	let faces = $state<unknown[]>([]);
	let processors = $state<StreamProcessor[]>([]);
	let selectedCollection = $state<string | null>(null);

	const filteredCollections = $derived(collections.filter((c) => c.toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredProcessors = $derived(processors.filter((p) => (p.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [collResp, procResp] = await Promise.all([
				reko.send(new ListCollectionsCommand({})),
				reko.send(new ListStreamProcessorsCommand({}))
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
			const resp = await reko.send(new ListFacesCommand({ CollectionId: collectionId }));
			faces = resp.Faces ?? [];
		} catch (e) {
			toast.error('Failed to load faces: ' + String(e));
		}
	}

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
				{#each [['collections', 'Face Collections'], ['processors', 'Stream Processors']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-violet-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
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
								<span class="text-xs px-2 py-1 rounded-full {proc.Status === 'RUNNING' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{proc.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
