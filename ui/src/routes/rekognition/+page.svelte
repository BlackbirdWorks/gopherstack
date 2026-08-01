<script lang="ts">
	// Amazon Rekognition mixes stateless image analysis (Detect*/Compare/
	// Recognize -- no backing resource, one-shot mocks per parity-principles
	// rule 4) with stored, CRUD-able resources: face Collections (with
	// indexed Faces and, per-collection, face-search Users), and Stream
	// Processors (a standing Kinesis Video -> Kinesis Data/S3 analysis
	// pipeline). This page's floor: full CRUD on Collections/Faces/Users/
	// Stream Processors, plus the existing Detect Faces stateless tester.
	//
	// Out of scope for this page (real, implemented backend families, just
	// not surfaced here -- Custom Labels training and async video jobs are
	// both heavier, project/job-lifecycle flows distinct from the
	// collections+faces+streaming shape this page covers): Project/
	// ProjectVersion (Custom Labels model training), Dataset (Custom Labels
	// training data), the async video job family (Start*/Get* for
	// CelebrityRecognition/ContentModeration/FaceDetection/FaceSearch/
	// LabelDetection/PersonTracking/SegmentDetection/TextDetection),
	// MediaAnalysisJob, and FaceLivenessSession.
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getRekognitionClient } from '$lib/aws-client';
	import {
		ListCollectionsCommand,
		DescribeCollectionCommand,
		CreateCollectionCommand,
		DeleteCollectionCommand,
		ListFacesCommand,
		IndexFacesCommand,
		DeleteFacesCommand,
		ListStreamProcessorsCommand,
		DescribeStreamProcessorCommand,
		CreateStreamProcessorCommand,
		DeleteStreamProcessorCommand,
		StartStreamProcessorCommand,
		StopStreamProcessorCommand,
		ListUsersCommand,
		CreateUserCommand,
		DeleteUserCommand,
		DetectFacesCommand,
		type StreamProcessor,
		type FaceDetail,
		type Face,
		type User,
		type DescribeCollectionCommandOutput,
		type DescribeStreamProcessorCommandOutput
	} from '@aws-sdk/client-rekognition';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { Eye, RefreshCw, Users, Video, Database, ScanFace, Play, Square, Plus, Trash2 } from 'lucide-svelte';

	const client = regionalClient(getRekognitionClient);

	type TabId = 'collections' | 'processors' | 'users' | 'detect';

	const tabs: TabDef[] = [
		{ id: 'collections', label: 'Face Collections' },
		{ id: 'processors', label: 'Stream Processors' },
		{ id: 'users', label: 'Users' },
		{ id: 'detect', label: 'Detect Faces' }
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

	function statusClass(status: string | undefined): string {
		if (status === 'RUNNING' || status === 'ACTIVE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status?.includes('FAILED')) return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('collections');
	let searchQuery = $state('');

	let collections = $state<string[]>([]);
	let processors = $state<StreamProcessor[]>([]);

	async function fetchCollections(): Promise<void> {
		const resp = await client().send(new ListCollectionsCommand({}));
		collections = resp.CollectionIds ?? [];
	}
	async function fetchProcessors(): Promise<void> {
		const resp = await client().send(new ListStreamProcessorsCommand({}));
		processors = resp.StreamProcessors ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		collections: () => fetchCollections().catch(rethrowDescribed),
		processors: () => fetchProcessors().catch(rethrowDescribed),
		users: () => Promise.resolve(),
		detect: () => Promise.resolve()
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		if (activeTab === 'users' && usersCollectionId) {
			void loadUsers();
			return;
		}
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		collectionDetailModal?.close();
		selectedCollectionId = null;
		users = [];
		usersCollectionId = '';
		tabLoader.refresh(untrack(() => activeTab));
	});

	function matches(q: string, ...fields: (string | undefined)[]): boolean {
		if (!q) return true;
		return fields.some((f) => (f ?? '').toLowerCase().includes(q));
	}

	const filteredCollections = $derived(collections.filter((c) => matches(searchQuery.toLowerCase(), c)));
	const filteredProcessors = $derived(processors.filter((p) => matches(searchQuery.toLowerCase(), p.Name)));

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Collections: create / delete / detail (faces + index) ---

	let createCollectionModal = $state<Modal | null>(null);
	let creatingCollection = $state(false);
	let createCollectionError = $state<string | null>(null);
	let newCollectionId = $state('');

	function openCreateCollectionModal(): void {
		createCollectionError = null;
		newCollectionId = '';
		createCollectionModal?.open();
	}

	async function submitCreateCollection(): Promise<void> {
		if (!newCollectionId) {
			createCollectionError = 'Collection ID is required.';
			return;
		}
		creatingCollection = true;
		createCollectionError = null;
		try {
			await client().send(new CreateCollectionCommand({ CollectionId: newCollectionId }));
			toast.success('Collection created');
			createCollectionModal?.close();
			await tabLoader.refresh('collections');
		} catch (e) {
			const msg = describeError(e);
			createCollectionError = msg;
			toast.error(msg);
		} finally {
			creatingCollection = false;
		}
	}

	async function deleteCollection(collId: string): Promise<void> {
		const confirmed = await confirmDestructive({
			title: 'Delete collection',
			message: `Delete collection "${collId}"? All indexed faces are permanently lost.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCollectionCommand({ CollectionId: collId }));
			toast.success('Collection deleted');
			if (selectedCollectionId === collId) {
				collectionDetailModal?.close();
				selectedCollectionId = null;
			}
			await tabLoader.refresh('collections');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let collectionDetailModal = $state<Modal | null>(null);
	let selectedCollectionId = $state<string | null>(null);
	let collectionDetail = $state<DescribeCollectionCommandOutput | null>(null);
	let faces = $state<Face[]>([]);
	let loadingCollectionDetail = $state(false);
	let collectionDetailError = $state<string | null>(null);

	async function openCollectionDetail(collId: string): Promise<void> {
		selectedCollectionId = collId;
		collectionDetail = null;
		faces = [];
		collectionDetailError = null;
		collectionDetailModal?.open();
		await refreshCollectionDetail();
	}

	async function refreshCollectionDetail(): Promise<void> {
		if (!selectedCollectionId) return;
		loadingCollectionDetail = true;
		collectionDetailError = null;
		try {
			const [descResp, facesResp] = await Promise.all([
				client().send(new DescribeCollectionCommand({ CollectionId: selectedCollectionId })),
				client().send(new ListFacesCommand({ CollectionId: selectedCollectionId }))
			]);
			collectionDetail = descResp;
			faces = facesResp.Faces ?? [];
		} catch (e) {
			collectionDetailError = describeError(e);
		} finally {
			loadingCollectionDetail = false;
		}
	}

	async function deleteFace(faceId: string | undefined): Promise<void> {
		if (!faceId || !selectedCollectionId) return;
		const confirmed = await confirmDestructive({ title: 'Delete face', message: `Delete face "${faceId}"?` });
		if (!confirmed) return;
		try {
			await client().send(new DeleteFacesCommand({ CollectionId: selectedCollectionId, FaceIds: [faceId] }));
			toast.success('Face deleted');
			await refreshCollectionDetail();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let indexBucket = $state('');
	let indexKey = $state('');
	let indexExternalId = $state('');
	let indexing = $state(false);
	let indexError = $state<string | null>(null);

	async function indexFace(): Promise<void> {
		if (!selectedCollectionId || !indexBucket.trim() || !indexKey.trim()) {
			indexError = 'S3 bucket and key are required.';
			return;
		}
		indexing = true;
		indexError = null;
		try {
			await client().send(
				new IndexFacesCommand({
					CollectionId: selectedCollectionId,
					Image: { S3Object: { Bucket: indexBucket.trim(), Name: indexKey.trim() } },
					ExternalImageId: indexExternalId.trim() || undefined
				})
			);
			toast.success('Face indexed');
			indexBucket = '';
			indexKey = '';
			indexExternalId = '';
			await refreshCollectionDetail();
		} catch (e) {
			const msg = describeError(e);
			indexError = msg;
			toast.error(msg);
		} finally {
			indexing = false;
		}
	}

	// --- Stream Processors: create / delete / start / stop / detail ---

	let createProcessorModal = $state<Modal | null>(null);
	let creatingProcessor = $state(false);
	let createProcessorError = $state<string | null>(null);
	let newProcessorName = $state('');
	let newProcessorInputArn = $state('');
	let newProcessorOutputArn = $state('');
	let newProcessorRoleArn = $state('');
	let newProcessorMode = $state<'FaceSearch' | 'ConnectedHome'>('FaceSearch');
	let newProcessorCollectionId = $state('');
	let newProcessorLabels = $state('PERSON,ALL');

	function openCreateProcessorModal(): void {
		createProcessorError = null;
		newProcessorName = '';
		newProcessorInputArn = '';
		newProcessorOutputArn = '';
		newProcessorRoleArn = '';
		newProcessorMode = 'FaceSearch';
		newProcessorCollectionId = '';
		newProcessorLabels = 'PERSON,ALL';
		createProcessorModal?.open();
	}

	async function submitCreateProcessor(): Promise<void> {
		if (!newProcessorName || !newProcessorInputArn || !newProcessorOutputArn || !newProcessorRoleArn) {
			createProcessorError = 'Name, input stream ARN, output stream ARN, and role ARN are all required.';
			return;
		}
		if (newProcessorMode === 'FaceSearch' && !newProcessorCollectionId) {
			createProcessorError = 'A collection ID is required for Face Search mode.';
			return;
		}
		creatingProcessor = true;
		createProcessorError = null;
		try {
			await client().send(
				new CreateStreamProcessorCommand({
					Name: newProcessorName,
					Input: { KinesisVideoStream: { Arn: newProcessorInputArn } },
					Output: { KinesisDataStream: { Arn: newProcessorOutputArn } },
					RoleArn: newProcessorRoleArn,
					Settings:
						newProcessorMode === 'FaceSearch'
							? { FaceSearch: { CollectionId: newProcessorCollectionId } }
							: { ConnectedHome: { Labels: newProcessorLabels.split(',').map((l) => l.trim()).filter(Boolean) } }
				})
			);
			toast.success('Stream processor created');
			createProcessorModal?.close();
			await tabLoader.refresh('processors');
		} catch (e) {
			const msg = describeError(e);
			createProcessorError = msg;
			toast.error(msg);
		} finally {
			creatingProcessor = false;
		}
	}

	async function deleteProcessor(name: string | undefined): Promise<void> {
		if (!name) return;
		const confirmed = await confirmDestructive({ title: 'Delete stream processor', message: `Delete stream processor "${name}"?` });
		if (!confirmed) return;
		try {
			await client().send(new DeleteStreamProcessorCommand({ Name: name }));
			toast.success('Stream processor deleted');
			await tabLoader.refresh('processors');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function startProcessor(name: string): Promise<void> {
		try {
			await client().send(new StartStreamProcessorCommand({ Name: name }));
			toast.success(`Started ${name}`);
			await tabLoader.refresh('processors');
		} catch (e) {
			toast.error('Failed to start processor: ' + describeError(e));
		}
	}

	async function stopProcessor(name: string): Promise<void> {
		try {
			await client().send(new StopStreamProcessorCommand({ Name: name }));
			toast.success(`Stopped ${name}`);
			await tabLoader.refresh('processors');
		} catch (e) {
			toast.error('Failed to stop processor: ' + describeError(e));
		}
	}

	let processorDetailModal = $state<Modal | null>(null);
	let viewedProcessor = $state<DescribeStreamProcessorCommandOutput | null>(null);
	let loadingProcessorDetail = $state(false);
	let processorDetailError = $state<string | null>(null);

	async function openProcessorDetail(name: string | undefined): Promise<void> {
		if (!name) return;
		viewedProcessor = null;
		processorDetailError = null;
		processorDetailModal?.open();
		loadingProcessorDetail = true;
		try {
			viewedProcessor = await client().send(new DescribeStreamProcessorCommand({ Name: name }));
		} catch (e) {
			processorDetailError = describeError(e);
		} finally {
			loadingProcessorDetail = false;
		}
	}

	// --- Users (face-search users, scoped to a collection) ---

	let usersCollectionId = $state('');
	let users = $state<User[]>([]);
	let loadingUsers = $state(false);
	let usersError = $state<string | null>(null);

	async function loadUsers(): Promise<void> {
		if (!usersCollectionId) {
			usersError = 'Select a collection first.';
			return;
		}
		loadingUsers = true;
		usersError = null;
		try {
			const resp = await client().send(new ListUsersCommand({ CollectionId: usersCollectionId }));
			users = resp.Users ?? [];
		} catch (e) {
			usersError = describeError(e);
		} finally {
			loadingUsers = false;
		}
	}

	let createUserModal = $state<Modal | null>(null);
	let creatingUser = $state(false);
	let createUserError = $state<string | null>(null);
	let newUserId = $state('');

	function openCreateUserModal(): void {
		createUserError = null;
		newUserId = '';
		createUserModal?.open();
	}

	async function submitCreateUser(): Promise<void> {
		if (!usersCollectionId || !newUserId) {
			createUserError = 'A collection and a user ID are required.';
			return;
		}
		creatingUser = true;
		createUserError = null;
		try {
			await client().send(new CreateUserCommand({ CollectionId: usersCollectionId, UserId: newUserId }));
			toast.success('User created');
			createUserModal?.close();
			await loadUsers();
		} catch (e) {
			const msg = describeError(e);
			createUserError = msg;
			toast.error(msg);
		} finally {
			creatingUser = false;
		}
	}

	async function deleteUser(userId: string | undefined): Promise<void> {
		if (!userId || !usersCollectionId) return;
		const confirmed = await confirmDestructive({ title: 'Delete user', message: `Delete user "${userId}"?` });
		if (!confirmed) return;
		try {
			await client().send(new DeleteUserCommand({ CollectionId: usersCollectionId, UserId: userId }));
			toast.success('User deleted');
			await loadUsers();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Detect Faces (stateless tester, unchanged) ---

	let detectBucket = $state('');
	let detectKey = $state('');
	let detecting = $state(false);
	let detectedFaces = $state<FaceDetail[]>([]);
	let detectRan = $state(false);

	async function detectFaces(): Promise<void> {
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
			toast.error('Failed to detect faces: ' + describeError(e));
		} finally {
			detecting = false;
		}
	}

	const pct = (v: number | undefined) => (v === undefined || v === null ? '-' : v.toFixed(1) + '%');
	const topEmotion = (f: FaceDetail) => (f.Emotions ?? []).toSorted((a, b) => (b.Confidence ?? 0) - (a.Confidence ?? 0))[0]?.Type ?? '-';
</script>

<div class="p-6 space-y-6">
	<PageHeader icon={Eye} title="Amazon Rekognition" description="Image and video analysis using deep learning" onRefresh={handleRefresh} color="violet">
		{#snippet actions()}
			{#if activeTab === 'collections'}
				<button onclick={openCreateCollectionModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Create collection
				</button>
			{:else if activeTab === 'processors'}
				<button onclick={openCreateProcessorModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Create stream processor
				</button>
			{:else if activeTab === 'users' && usersCollectionId}
				<button onclick={openCreateUserModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-violet-600 text-white hover:bg-violet-700 text-sm">
					<Plus class="w-4 h-4" /> Create user
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-violet-100 dark:bg-violet-900/30 rounded-lg"><Database class="w-5 h-5 text-violet-600 dark:text-violet-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{collections.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Face Collections</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Users class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{faces.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Faces (selected collection)</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Video class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{processors.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Stream Processors</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="violet" />
			{#if activeTab === 'collections' || activeTab === 'processors'}
				<SearchInput bind:value={searchQuery} />
			{/if}
		</div>
		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'collections'}
				{#snippet collectionIdCell(collId: string)}
					<span class="font-medium text-gray-900 dark:text-white">{collId}</span>
				{/snippet}
				{#snippet collectionActionsCell(collId: string)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openCollectionDetail(collId)} title="View" aria-label="View collection {collId}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteCollection(collId)} title="Delete" aria-label="Delete collection {collId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const collectionColumns = defineColumns<string>([
					{ key: 'id', label: 'Collection ID', render: collectionIdCell },
					{ key: 'actions', label: '', render: collectionActionsCell }
				])}
				<DataTable rows={filteredCollections} rowKey={(c) => c} columns={collectionColumns} loading={tabLoader.isLoading('collections')} emptyMessage="No face collections found" />
			{:else if activeTab === 'processors'}
				{#snippet processorStatusCell(p: StreamProcessor)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(p.Status)}">{p.Status ?? '—'}</span>
				{/snippet}
				{#snippet processorActionsCell(p: StreamProcessor)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openProcessorDetail(p.Name)} title="View" aria-label="View stream processor {p.Name}" class="text-gray-400 hover:text-violet-500"><Eye class="w-4 h-4" /></button>
						{#if p.Status === 'RUNNING'}
							<button onclick={() => p.Name && stopProcessor(p.Name)} title="Stop processor" class="text-gray-400 hover:text-amber-500"><Square class="w-4 h-4" /></button>
						{:else}
							<button onclick={() => p.Name && startProcessor(p.Name)} title="Start processor" class="text-gray-400 hover:text-green-500"><Play class="w-4 h-4" /></button>
						{/if}
						<button onclick={() => deleteProcessor(p.Name)} title="Delete" aria-label="Delete stream processor {p.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const processorColumns = defineColumns<StreamProcessor>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Status', label: 'Status', render: processorStatusCell },
					{ key: 'actions', label: '', render: processorActionsCell }
				])}
				<DataTable rows={filteredProcessors} rowKey={(p) => p.Name ?? ''} columns={processorColumns} loading={tabLoader.isLoading('processors')} emptyMessage="No stream processors found" />
			{:else if activeTab === 'users'}
				<div class="flex items-end gap-2">
					<div class="flex-1">
						<label for="rk-users-collection" class="text-sm text-slate-600 dark:text-slate-300">Collection</label>
						<select id="rk-users-collection" bind:value={usersCollectionId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							<option value="">Select a collection…</option>
							{#each collections as c (c)}<option value={c}>{c}</option>{/each}
						</select>
					</div>
					<button onclick={loadUsers} class="px-3 py-2 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700">Load Users</button>
				</div>
				{#if usersError}<p class="text-sm text-red-600 dark:text-red-400">{usersError}</p>{/if}
				{#snippet userActionsCell(u: User)}
					<button onclick={() => deleteUser(u.UserId)} title="Delete" aria-label="Delete user {u.UserId}" class="text-gray-400 hover:text-red-500 float-right"><Trash2 class="w-4 h-4" /></button>
				{/snippet}
				{@const userColumns = defineColumns<User>([
					{ key: 'UserId', label: 'User ID' },
					{ key: 'UserStatus', label: 'Status' },
					{ key: 'actions', label: '', render: userActionsCell }
				])}
				<DataTable rows={users} rowKey={(u) => u.UserId ?? ''} columns={userColumns} loading={loadingUsers} emptyMessage={usersCollectionId ? 'No users found' : 'Select a collection to list its users'} />
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
					<button onclick={detectFaces} disabled={detecting} class="flex items-center gap-2 px-4 py-2 bg-violet-600 text-white rounded-lg text-sm font-medium hover:bg-violet-700 disabled:opacity-50">
						<ScanFace class="w-4 h-4" /> {detecting ? 'Detecting...' : 'Detect Faces'}
					</button>

					{#if detectRan}
						{#if detectedFaces.length === 0}
							<div class="text-center py-8 text-gray-500 dark:text-gray-400">No faces detected</div>
						{:else}
							<div class="space-y-3">
								{#each detectedFaces as face, i (i)}
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

<Modal bind:this={createCollectionModal} title="Create Collection">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rk-coll-id" class="text-sm text-slate-600 dark:text-slate-300">Collection ID</label>
				<input id="rk-coll-id" bind:value={newCollectionId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createCollectionError}<p class="text-sm text-red-600 dark:text-red-400">{createCollectionError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createCollectionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateCollection} disabled={creatingCollection} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingCollection ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createProcessorModal} title="Create Stream Processor">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="rk-proc-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="rk-proc-name" bind:value={newProcessorName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rk-proc-input" class="text-sm text-slate-600 dark:text-slate-300">Input Kinesis Video Stream ARN</label>
				<input id="rk-proc-input" bind:value={newProcessorInputArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rk-proc-output" class="text-sm text-slate-600 dark:text-slate-300">Output Kinesis Data Stream ARN</label>
				<input id="rk-proc-output" bind:value={newProcessorOutputArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rk-proc-role" class="text-sm text-slate-600 dark:text-slate-300">Role ARN</label>
				<input id="rk-proc-role" bind:value={newProcessorRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="rk-proc-mode" class="text-sm text-slate-600 dark:text-slate-300">Analysis mode</label>
				<select id="rk-proc-mode" bind:value={newProcessorMode} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="FaceSearch">Face Search</option>
					<option value="ConnectedHome">Connected Home (label detection)</option>
				</select>
			</div>
			{#if newProcessorMode === 'FaceSearch'}
				<div>
					<label for="rk-proc-collection" class="text-sm text-slate-600 dark:text-slate-300">Collection ID</label>
					<select id="rk-proc-collection" bind:value={newProcessorCollectionId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="">Select a collection…</option>
						{#each collections as c (c)}<option value={c}>{c}</option>{/each}
					</select>
				</div>
			{:else}
				<div>
					<label for="rk-proc-labels" class="text-sm text-slate-600 dark:text-slate-300">Labels (comma-separated, e.g. PERSON,ALL)</label>
					<input id="rk-proc-labels" bind:value={newProcessorLabels} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if createProcessorError}<p class="text-sm text-red-600 dark:text-red-400">{createProcessorError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createProcessorModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateProcessor} disabled={creatingProcessor} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingProcessor ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={createUserModal} title="Create User">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-xs text-slate-500 dark:text-slate-400">In collection: <span class="font-mono">{usersCollectionId}</span></p>
			<div>
				<label for="rk-user-id" class="text-sm text-slate-600 dark:text-slate-300">User ID</label>
				<input id="rk-user-id" bind:value={newUserId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createUserError}<p class="text-sm text-red-600 dark:text-red-400">{createUserError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createUserModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateUser} disabled={creatingUser} class="rounded-lg bg-violet-600 px-4 py-2 text-sm font-semibold text-white hover:bg-violet-700 disabled:opacity-50">{creatingUser ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={collectionDetailModal} title="Face Collection">
	{#snippet children()}
		{#if loadingCollectionDetail}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if collectionDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{collectionDetailError}</p>
		{:else}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Collection ID</dt><dd class="text-slate-900 dark:text-white">{selectedCollectionId}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{collectionDetail?.CollectionARN ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Face count</dt><dd class="text-slate-900 dark:text-white">{collectionDetail?.FaceCount ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Face model version</dt><dd class="text-slate-900 dark:text-white">{collectionDetail?.FaceModelVersion ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(collectionDetail?.CreationTimestamp)}</dd></div>
			</dl>

			<div class="mt-4 space-y-2">
				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Index a face from S3</h3>
				<div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
					<input bind:value={indexBucket} placeholder="S3 bucket" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					<input bind:value={indexKey} placeholder="S3 key" class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<input bind:value={indexExternalId} placeholder="External image ID (optional)" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				<button onclick={indexFace} disabled={indexing} class="px-3 py-1.5 text-sm rounded-lg bg-violet-600 text-white hover:bg-violet-700 disabled:opacity-50">{indexing ? 'Indexing…' : 'Index Face'}</button>
				{#if indexError}<p class="text-sm text-red-600 dark:text-red-400">{indexError}</p>{/if}
			</div>

			<div class="mt-4">
				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Faces ({faces.length})</h3>
				{#if faces.length === 0}
					<p class="text-xs text-slate-500 dark:text-slate-400">No faces indexed</p>
				{:else}
					<div class="space-y-1">
						{#each faces as f (f.FaceId)}
							<div class="flex items-center justify-between text-xs p-2 rounded bg-gray-50 dark:bg-slate-700/50">
								<span class="truncate font-mono">{f.FaceId}</span>
								<button onclick={() => deleteFace(f.FaceId)} class="text-red-500 hover:underline shrink-0 ml-2">Delete</button>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => collectionDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		<button type="button" onclick={refreshCollectionDetail} class="flex items-center gap-2 rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"><RefreshCw class="w-4 h-4" /> Refresh</button>
	{/snippet}
</Modal>

<Modal bind:this={processorDetailModal} title="Stream Processor">
	{#snippet children()}
		{#if loadingProcessorDetail}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if processorDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{processorDetailError}</p>
		{:else if viewedProcessor}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedProcessor.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedProcessor.StreamProcessorArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white"><span class="text-xs px-2 py-1 rounded-full {statusClass(viewedProcessor.Status)}">{viewedProcessor.Status ?? '—'}</span></dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Role ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedProcessor.RoleArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Input stream ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedProcessor.Input?.KinesisVideoStream?.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Output stream ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedProcessor.Output?.KinesisDataStream?.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Face Search collection</dt><dd class="text-slate-900 dark:text-white">{viewedProcessor.Settings?.FaceSearch?.CollectionId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedProcessor.CreationTimestamp)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => processorDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
