<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAppStreamClient } from '$lib/aws-client';
	import {
		DescribeStacksCommand,
		DescribeFleetsCommand,
		DescribeImagesCommand,
		CreateStackCommand,
		UpdateStackCommand,
		DeleteStackCommand,
		CreateFleetCommand,
		UpdateFleetCommand,
		DeleteFleetCommand,
		StartFleetCommand,
		StopFleetCommand,
		CopyImageCommand,
		DeleteImageCommand,
		type Stack,
		type Fleet,
		type Image as AppStreamImage
	} from '@aws-sdk/client-appstream';
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
	import { Monitor, Plus, Trash2, Eye, Pencil, Server, Image, Play, Square, Copy } from 'lucide-svelte';

	// AppStream's backend (services/appstream) is far bigger than three tabs
	// -- it also has AppBlocks, AppBlockBuilders, Applications, Entitlements,
	// DirectoryConfigs, ImageBuilders, ExportImageTasks, Users,
	// UserStackAssociations and Sessions, all independently listable and
	// CRUD-capable. This pass covers the three families the task explicitly
	// named -- Stacks, Fleets, Images -- to the CRUD floor; the rest are
	// real, backend-supported gaps left for a follow-up (reported, not
	// silently dropped).
	//
	// Images have no CreateImage/UpdateImage operation on the wire at all --
	// images come from an ImageBuilder, an import, or CopyImage. "Copy
	// image" is offered here as the closest create-equivalent; there is no
	// edit action for an image (only UpdateImagePermissions, which manages
	// cross-account sharing rather than the image itself, is out of scope).
	const client = regionalClient(getAppStreamClient);

	type TabId = 'stacks' | 'fleets' | 'images';

	const tabs: TabDef[] = [
		{ id: 'stacks', label: 'Stacks' },
		{ id: 'fleets', label: 'Fleets' },
		{ id: 'images', label: 'Images' }
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

	function fleetStateClass(state: string | undefined): string {
		if (state === 'RUNNING') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'STOPPED') return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
		return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
	}

	function imageStateClass(state: string | undefined): string {
		if (state === 'AVAILABLE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('stacks');
	let searchQuery = $state('');

	let stacks = $state<Stack[]>([]);
	let fleets = $state<Fleet[]>([]);
	let images = $state<AppStreamImage[]>([]);

	async function fetchStacks(): Promise<void> {
		const resp = await client().send(new DescribeStacksCommand({}));
		stacks = resp.Stacks ?? [];
	}
	async function fetchFleets(): Promise<void> {
		const resp = await client().send(new DescribeFleetsCommand({}));
		fleets = resp.Fleets ?? [];
	}
	async function fetchImages(): Promise<void> {
		const resp = await client().send(new DescribeImagesCommand({}));
		images = resp.Images ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		stacks: () => fetchStacks().catch(rethrowDescribed),
		fleets: () => fetchFleets().catch(rethrowDescribed),
		images: () => fetchImages().catch(rethrowDescribed)
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
		tabLoader.refresh('stacks');
	});

	const filteredStacks = $derived(
		stacks.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (s.Name ?? '').toLowerCase().includes(q) || (s.Description ?? '').toLowerCase().includes(q);
		})
	);
	const filteredFleets = $derived(
		fleets.filter((f) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (f.Name ?? '').toLowerCase().includes(q) || (f.ImageName ?? '').toLowerCase().includes(q);
		})
	);
	const filteredImages = $derived(
		images.filter((i) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (i.Name ?? '').toLowerCase().includes(q) || (i.Platform ?? '').toLowerCase().includes(q);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Stacks ---

	let stackCreateModal = $state<Modal | null>(null);
	let stackCreating = $state(false);
	let stackCreateError = $state<string | null>(null);
	let newStackName = $state('');
	let newStackDescription = $state('');

	function openStackCreateModal(): void {
		stackCreateError = null;
		newStackName = '';
		newStackDescription = '';
		stackCreateModal?.open();
	}

	async function submitCreateStack(): Promise<void> {
		if (!newStackName) {
			stackCreateError = 'Name is required.';
			return;
		}
		stackCreating = true;
		stackCreateError = null;
		try {
			await client().send(
				new CreateStackCommand({ Name: newStackName, Description: newStackDescription || undefined })
			);
			toast.success('Stack created');
			stackCreateModal?.close();
			await tabLoader.refresh('stacks');
		} catch (e) {
			const msg = describeError(e);
			stackCreateError = msg;
			toast.error(msg);
		} finally {
			stackCreating = false;
		}
	}

	let stackEditModal = $state<Modal | null>(null);
	let stackEditing = $state(false);
	let stackEditError = $state<string | null>(null);
	let editStackName = $state('');
	let editStackDescription = $state('');

	function openStackEditModal(s: Stack): void {
		stackEditError = null;
		editStackName = s.Name ?? '';
		editStackDescription = s.Description ?? '';
		stackEditModal?.open();
	}

	async function submitEditStack(): Promise<void> {
		if (!editStackName) return;
		stackEditing = true;
		stackEditError = null;
		try {
			await client().send(
				new UpdateStackCommand({ Name: editStackName, Description: editStackDescription || undefined })
			);
			toast.success('Stack updated');
			stackEditModal?.close();
			await tabLoader.refresh('stacks');
		} catch (e) {
			const msg = describeError(e);
			stackEditError = msg;
			toast.error(msg);
		} finally {
			stackEditing = false;
		}
	}

	async function deleteStack(s: Stack): Promise<void> {
		if (!s.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete stack',
			message: `Delete stack ${s.Name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteStackCommand({ Name: s.Name }));
			toast.success('Stack deleted');
			await tabLoader.refresh('stacks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Fleets ---

	let fleetCreateModal = $state<Modal | null>(null);
	let fleetCreating = $state(false);
	let fleetCreateError = $state<string | null>(null);
	let newFleetName = $state('');
	let newFleetImageName = $state('');
	let newFleetInstanceType = $state('stream.standard.medium');
	let newFleetDesiredInstances = $state(1);

	function openFleetCreateModal(): void {
		fleetCreateError = null;
		newFleetName = '';
		newFleetImageName = '';
		newFleetInstanceType = 'stream.standard.medium';
		newFleetDesiredInstances = 1;
		fleetCreateModal?.open();
	}

	async function submitCreateFleet(): Promise<void> {
		if (!newFleetName || !newFleetImageName) {
			fleetCreateError = 'Name and image name are required.';
			return;
		}
		fleetCreating = true;
		fleetCreateError = null;
		try {
			await client().send(
				new CreateFleetCommand({
					Name: newFleetName,
					ImageName: newFleetImageName,
					InstanceType: newFleetInstanceType,
					ComputeCapacity: { DesiredInstances: newFleetDesiredInstances }
				})
			);
			toast.success('Fleet created');
			fleetCreateModal?.close();
			await tabLoader.refresh('fleets');
		} catch (e) {
			const msg = describeError(e);
			fleetCreateError = msg;
			toast.error(msg);
		} finally {
			fleetCreating = false;
		}
	}

	let fleetEditModal = $state<Modal | null>(null);
	let fleetEditing = $state(false);
	let fleetEditError = $state<string | null>(null);
	let editFleetName = $state('');
	let editFleetInstanceType = $state('');
	let editFleetDesiredInstances = $state(1);

	function openFleetEditModal(f: Fleet): void {
		fleetEditError = null;
		editFleetName = f.Name ?? '';
		editFleetInstanceType = f.InstanceType ?? '';
		editFleetDesiredInstances = f.ComputeCapacityStatus?.Desired ?? 1;
		fleetEditModal?.open();
	}

	async function submitEditFleet(): Promise<void> {
		if (!editFleetName) return;
		fleetEditing = true;
		fleetEditError = null;
		try {
			await client().send(
				new UpdateFleetCommand({
					Name: editFleetName,
					InstanceType: editFleetInstanceType,
					ComputeCapacity: { DesiredInstances: editFleetDesiredInstances }
				})
			);
			toast.success('Fleet updated');
			fleetEditModal?.close();
			await tabLoader.refresh('fleets');
		} catch (e) {
			const msg = describeError(e);
			fleetEditError = msg;
			toast.error(msg);
		} finally {
			fleetEditing = false;
		}
	}

	async function toggleFleetState(f: Fleet): Promise<void> {
		if (!f.Name) return;
		try {
			if (f.State === 'RUNNING') {
				await client().send(new StopFleetCommand({ Name: f.Name }));
				toast.success('Fleet stopping');
			} else {
				await client().send(new StartFleetCommand({ Name: f.Name }));
				toast.success('Fleet starting');
			}
			await tabLoader.refresh('fleets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteFleet(f: Fleet): Promise<void> {
		if (!f.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete fleet',
			message: `Delete fleet ${f.Name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteFleetCommand({ Name: f.Name }));
			toast.success('Fleet deleted');
			await tabLoader.refresh('fleets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Images ---

	let imageCopyModal = $state<Modal | null>(null);
	let imageCopying = $state(false);
	let imageCopyError = $state<string | null>(null);
	let copySourceImageName = $state('');
	let copyDestinationImageName = $state('');
	let copyDestinationRegion = $state('');

	function openImageCopyModal(): void {
		imageCopyError = null;
		copySourceImageName = images[0]?.Name ?? '';
		copyDestinationImageName = '';
		copyDestinationRegion = '';
		imageCopyModal?.open();
	}

	async function submitCopyImage(): Promise<void> {
		if (!copySourceImageName || !copyDestinationImageName || !copyDestinationRegion) {
			imageCopyError = 'Source image, destination name and destination region are required.';
			return;
		}
		imageCopying = true;
		imageCopyError = null;
		try {
			await client().send(
				new CopyImageCommand({
					SourceImageName: copySourceImageName,
					DestinationImageName: copyDestinationImageName,
					DestinationRegion: copyDestinationRegion
				})
			);
			toast.success('Image copy started');
			imageCopyModal?.close();
			await tabLoader.refresh('images');
		} catch (e) {
			const msg = describeError(e);
			imageCopyError = msg;
			toast.error(msg);
		} finally {
			imageCopying = false;
		}
	}

	async function deleteImage(i: AppStreamImage): Promise<void> {
		if (!i.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete image',
			message: `Delete image ${i.Name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteImageCommand({ Name: i.Name }));
			toast.success('Image deleted');
			await tabLoader.refresh('images');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Monitor}
		title="Amazon AppStream 2.0"
		description="Fully managed application and desktop streaming service"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'stacks'}
				<button
					onclick={openStackCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create stack
				</button>
			{:else if activeTab === 'fleets'}
				<button
					onclick={openFleetCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create fleet
				</button>
			{:else if activeTab === 'images'}
				<button
					onclick={openImageCopyModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Copy class="w-4 h-4" /> Copy image
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'stacks'}
				{#snippet stackCreatedCell(s: Stack)}
					<span class="text-xs text-gray-500 dark:text-gray-400">{formatDate(s.CreatedTime)}</span>
				{/snippet}
				{#snippet stackActionsCell(s: Stack)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openStackEditModal(s)} title="Edit" aria-label="Edit stack {s.Name}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteStack(s)} title="Delete" aria-label="Delete stack {s.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const stackColumns = defineColumns<Stack>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Description', label: 'Description' },
					{ key: 'CreatedTime', label: 'Created', render: stackCreatedCell },
					{ key: 'actions', label: '', render: stackActionsCell }
				])}
				<DataTable
					rows={filteredStacks}
					rowKey={(s) => s.Name ?? ''}
					columns={stackColumns}
					loading={tabLoader.isLoading('stacks')}
					emptyMessage="No stacks found"
				/>
			{:else if activeTab === 'fleets'}
				{#snippet fleetStateCell(f: Fleet)}
					<span class="text-xs px-2 py-1 rounded-full {fleetStateClass(f.State)}">{f.State ?? '—'}</span>
				{/snippet}
				{#snippet fleetActionsCell(f: Fleet)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => toggleFleetState(f)}
							title={f.State === 'RUNNING' ? 'Stop' : 'Start'}
							aria-label={f.State === 'RUNNING' ? `Stop fleet ${f.Name}` : `Start fleet ${f.Name}`}
							class="text-gray-400 hover:text-blue-500"
						>
							{#if f.State === 'RUNNING'}<Square class="w-4 h-4" />{:else}<Play class="w-4 h-4" />{/if}
						</button>
						<button onclick={() => openFleetEditModal(f)} title="Edit" aria-label="Edit fleet {f.Name}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => deleteFleet(f)} title="Delete" aria-label="Delete fleet {f.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const fleetColumns = defineColumns<Fleet>([
					{ key: 'Name', label: 'Name' },
					{ key: 'ImageName', label: 'Image' },
					{ key: 'InstanceType', label: 'Instance Type' },
					{ key: 'State', label: 'State', render: fleetStateCell },
					{ key: 'actions', label: '', render: fleetActionsCell }
				])}
				<DataTable
					rows={filteredFleets}
					rowKey={(f) => f.Name ?? ''}
					columns={fleetColumns}
					loading={tabLoader.isLoading('fleets')}
					emptyMessage="No fleets found"
				/>
			{:else if activeTab === 'images'}
				{#snippet imageStateCell(i: AppStreamImage)}
					<span class="text-xs px-2 py-1 rounded-full {imageStateClass(i.State)}">{i.State ?? '—'}</span>
				{/snippet}
				{#snippet imageActionsCell(i: AppStreamImage)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => deleteImage(i)} title="Delete" aria-label="Delete image {i.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const imageColumns = defineColumns<AppStreamImage>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Platform', label: 'Platform' },
					{ key: 'State', label: 'State', render: imageStateCell },
					{ key: 'actions', label: '', render: imageActionsCell }
				])}
				<DataTable
					rows={filteredImages}
					rowKey={(i) => i.Name ?? ''}
					columns={imageColumns}
					loading={tabLoader.isLoading('images')}
					emptyMessage="No images found"
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={stackCreateModal} title="Create Stack">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="as-stack-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="as-stack-new-name" bind:value={newStackName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="as-stack-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="as-stack-new-desc" bind:value={newStackDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if stackCreateError}<p class="text-sm text-red-600 dark:text-red-400">{stackCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => stackCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateStack} disabled={stackCreating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{stackCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={stackEditModal} title="Edit Stack">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="as-stack-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="as-stack-edit-desc" bind:value={editStackDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if stackEditError}<p class="text-sm text-red-600 dark:text-red-400">{stackEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => stackEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditStack} disabled={stackEditing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{stackEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={fleetCreateModal} title="Create Fleet">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="as-fleet-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="as-fleet-new-name" bind:value={newFleetName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="as-fleet-new-image" class="text-sm text-slate-600 dark:text-slate-300">Image name</label>
				<input id="as-fleet-new-image" bind:value={newFleetImageName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="as-fleet-new-type" class="text-sm text-slate-600 dark:text-slate-300">Instance type</label>
				<input id="as-fleet-new-type" bind:value={newFleetInstanceType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="as-fleet-new-desired" class="text-sm text-slate-600 dark:text-slate-300">Desired instances</label>
				<input id="as-fleet-new-desired" type="number" bind:value={newFleetDesiredInstances} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if fleetCreateError}<p class="text-sm text-red-600 dark:text-red-400">{fleetCreateError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => fleetCreateModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateFleet} disabled={fleetCreating} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{fleetCreating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={fleetEditModal} title="Edit Fleet">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="as-fleet-edit-type" class="text-sm text-slate-600 dark:text-slate-300">Instance type</label>
				<input id="as-fleet-edit-type" bind:value={editFleetInstanceType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="as-fleet-edit-desired" class="text-sm text-slate-600 dark:text-slate-300">Desired instances</label>
				<input id="as-fleet-edit-desired" type="number" bind:value={editFleetDesiredInstances} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if fleetEditError}<p class="text-sm text-red-600 dark:text-red-400">{fleetEditError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => fleetEditModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditFleet} disabled={fleetEditing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{fleetEditing ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={imageCopyModal} title="Copy Image">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="as-image-copy-source" class="text-sm text-slate-600 dark:text-slate-300">Source image name</label>
				<input id="as-image-copy-source" bind:value={copySourceImageName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="as-image-copy-dest" class="text-sm text-slate-600 dark:text-slate-300">Destination image name</label>
				<input id="as-image-copy-dest" bind:value={copyDestinationImageName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="as-image-copy-region" class="text-sm text-slate-600 dark:text-slate-300">Destination region</label>
				<input id="as-image-copy-region" bind:value={copyDestinationRegion} placeholder="us-west-2" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if imageCopyError}<p class="text-sm text-red-600 dark:text-red-400">{imageCopyError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => imageCopyModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCopyImage} disabled={imageCopying} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{imageCopying ? 'Copying…' : 'Copy'}</button>
	{/snippet}
</Modal>
