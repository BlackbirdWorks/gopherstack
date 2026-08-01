<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMGNClient } from '$lib/aws-client';
	import {
		DescribeSourceServersCommand,
		ListApplicationsCommand,
		ListWavesCommand,
		CreateApplicationCommand,
		UpdateApplicationCommand,
		DeleteApplicationCommand,
		ArchiveApplicationCommand,
		UnarchiveApplicationCommand,
		CreateWaveCommand,
		UpdateWaveCommand,
		DeleteWaveCommand,
		ArchiveWaveCommand,
		UnarchiveWaveCommand,
		StartTestCommand,
		StartCutoverCommand,
		FinalizeCutoverCommand,
		TerminateTargetInstancesCommand,
		DisconnectFromServiceCommand,
		ChangeServerLifeCycleStateCommand,
		DeleteSourceServerCommand,
		type SourceServer,
		type Application,
		type Wave,
		type ChangeServerLifeCycleStateSourceServerLifecycleState
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { Server, Plus, Trash2, Eye, Pencil, Box, Waves, Archive, ArchiveRestore } from 'lucide-svelte';

	// MGN is a migration *lifecycle* service, not a CRUD resource store for
	// source servers: they are discovered by the replication agent
	// (DescribeSourceServers) and driven through states (test -> cutover ->
	// finalize) by dedicated action operations, not Create/UpdateSourceServer
	// (neither exists in the SDK). DeleteSourceServer does exist (removing a
	// disconnected server's record) so that alone is offered as a destructive
	// action. Applications and Waves, by contrast, are real CRUD resources
	// (Create/Update/Delete + Archive/Unarchive) used to group source
	// servers for migration planning.
	const client = regionalClient(getMGNClient);

	type TabId = 'servers' | 'applications' | 'waves';

	const tabs: TabDef[] = [
		{ id: 'servers', label: 'Source Servers' },
		{ id: 'applications', label: 'Applications' },
		{ id: 'waves', label: 'Waves' }
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

	function lifecycleClass(state: string | undefined): string {
		if (state === 'CUTOVER') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'READY_FOR_CUTOVER' || state === 'READY_FOR_TEST')
			return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400';
		if (state === 'NOT_READY' || state === 'DISCONNECTED')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	let activeTab = $state<TabId>('servers');
	let searchQuery = $state('');

	let servers = $state<SourceServer[]>([]);
	let applications = $state<Application[]>([]);
	let waves = $state<Wave[]>([]);

	async function fetchServers(): Promise<void> {
		const resp = await client().send(new DescribeSourceServersCommand({ filters: {} }));
		servers = resp.items ?? [];
	}

	async function fetchApplications(): Promise<void> {
		const resp = await client().send(new ListApplicationsCommand({}));
		applications = resp.items ?? [];
	}

	async function fetchWaves(): Promise<void> {
		const resp = await client().send(new ListWavesCommand({}));
		waves = resp.items ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		servers: () => fetchServers().catch(rethrowDescribed),
		applications: () => fetchApplications().catch(rethrowDescribed),
		waves: () => fetchWaves().catch(rethrowDescribed)
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
		tabLoader.refresh('servers');
	});

	const filteredServers = $derived(
		servers.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(s.sourceServerID ?? '').toLowerCase().includes(q) ||
				(s.lifeCycle?.state ?? '').toLowerCase().includes(q) ||
				(s.applicationID ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredApps = $derived(
		applications.filter((a) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(a.name ?? '').toLowerCase().includes(q) || (a.description ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredWaves = $derived(
		waves.filter((w) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(w.name ?? '').toLowerCase().includes(q) || (w.description ?? '').toLowerCase().includes(q)
			);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Source server detail + lifecycle actions ---

	let serverDetailModal = $state<Modal | null>(null);
	let viewedServer = $state<SourceServer | null>(null);
	let serverActionError = $state<string | null>(null);
	let serverActionBusy = $state(false);
	let newLifecycleState = $state<ChangeServerLifeCycleStateSourceServerLifecycleState>('READY_FOR_TEST');

	function openServerDetail(s: SourceServer): void {
		viewedServer = s;
		serverActionError = null;
		newLifecycleState = 'READY_FOR_TEST';
		serverDetailModal?.open();
	}

	async function runServerAction(action: () => Promise<unknown>, successMessage: string): Promise<void> {
		if (!viewedServer?.sourceServerID) return;
		serverActionBusy = true;
		serverActionError = null;
		try {
			await action();
			toast.success(successMessage);
			await tabLoader.refresh('servers');
			const updated = servers.find((s) => s.sourceServerID === viewedServer?.sourceServerID);
			if (updated) viewedServer = updated;
		} catch (e) {
			const msg = describeError(e);
			serverActionError = msg;
			toast.error(msg);
		} finally {
			serverActionBusy = false;
		}
	}

	function startTest(): void {
		const id = viewedServer?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new StartTestCommand({ sourceServerIDs: [id] })),
			'Test launch started'
		);
	}

	function startCutover(): void {
		const id = viewedServer?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new StartCutoverCommand({ sourceServerIDs: [id] })),
			'Cutover started'
		);
	}

	function finalizeCutover(): void {
		const id = viewedServer?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new FinalizeCutoverCommand({ sourceServerID: id })),
			'Cutover finalized'
		);
	}

	function terminateTargetInstances(): void {
		const id = viewedServer?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new TerminateTargetInstancesCommand({ sourceServerIDs: [id] })),
			'Target instance termination started'
		);
	}

	function disconnectFromService(): void {
		const id = viewedServer?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new DisconnectFromServiceCommand({ sourceServerID: id })),
			'Source server disconnected'
		);
	}

	function applyLifecycleState(): void {
		const id = viewedServer?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() =>
				client().send(
					new ChangeServerLifeCycleStateCommand({
						sourceServerID: id,
						lifeCycle: { state: newLifecycleState }
					})
				),
			'Lifecycle state updated'
		);
	}

	async function deleteServer(s: SourceServer): Promise<void> {
		if (!s.sourceServerID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete source server',
			message: `Delete source server ${s.sourceServerID}? This removes it from MGN and cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSourceServerCommand({ sourceServerID: s.sourceServerID }));
			toast.success('Source server deleted');
			await tabLoader.refresh('servers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Applications: create/edit/archive/delete ---

	let appCreateModal = $state<Modal | null>(null);
	let appCreating = $state(false);
	let appCreateError = $state<string | null>(null);
	let newAppName = $state('');
	let newAppDescription = $state('');

	function openAppCreateModal(): void {
		appCreateError = null;
		newAppName = '';
		newAppDescription = '';
		appCreateModal?.open();
	}

	async function submitCreateApp(): Promise<void> {
		if (!newAppName) {
			appCreateError = 'Name is required.';
			return;
		}
		appCreating = true;
		appCreateError = null;
		try {
			await client().send(
				new CreateApplicationCommand({ name: newAppName, description: newAppDescription || undefined })
			);
			toast.success('Application created');
			appCreateModal?.close();
			await tabLoader.refresh('applications');
		} catch (e) {
			const msg = describeError(e);
			appCreateError = msg;
			toast.error(msg);
		} finally {
			appCreating = false;
		}
	}

	let appEditModal = $state<Modal | null>(null);
	let appEditing = $state(false);
	let appEditError = $state<string | null>(null);
	let editAppId = $state('');
	let editAppName = $state('');
	let editAppDescription = $state('');

	function openAppEditModal(a: Application): void {
		appEditError = null;
		editAppId = a.applicationID ?? '';
		editAppName = a.name ?? '';
		editAppDescription = a.description ?? '';
		appEditModal?.open();
	}

	async function submitEditApp(): Promise<void> {
		if (!editAppId) return;
		appEditing = true;
		appEditError = null;
		try {
			await client().send(
				new UpdateApplicationCommand({
					applicationID: editAppId,
					name: editAppName || undefined,
					description: editAppDescription || undefined
				})
			);
			toast.success('Application updated');
			appEditModal?.close();
			await tabLoader.refresh('applications');
		} catch (e) {
			const msg = describeError(e);
			appEditError = msg;
			toast.error(msg);
		} finally {
			appEditing = false;
		}
	}

	async function toggleAppArchive(a: Application): Promise<void> {
		if (!a.applicationID) return;
		try {
			if (a.isArchived) {
				await client().send(new UnarchiveApplicationCommand({ applicationID: a.applicationID }));
				toast.success('Application unarchived');
			} else {
				await client().send(new ArchiveApplicationCommand({ applicationID: a.applicationID }));
				toast.success('Application archived');
			}
			await tabLoader.refresh('applications');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteApplication(a: Application): Promise<void> {
		if (!a.applicationID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete application',
			message: `Delete application ${a.name ?? a.applicationID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteApplicationCommand({ applicationID: a.applicationID }));
			toast.success('Application deleted');
			await tabLoader.refresh('applications');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Waves: create/edit/archive/delete ---

	let waveCreateModal = $state<Modal | null>(null);
	let waveCreating = $state(false);
	let waveCreateError = $state<string | null>(null);
	let newWaveName = $state('');
	let newWaveDescription = $state('');

	function openWaveCreateModal(): void {
		waveCreateError = null;
		newWaveName = '';
		newWaveDescription = '';
		waveCreateModal?.open();
	}

	async function submitCreateWave(): Promise<void> {
		if (!newWaveName) {
			waveCreateError = 'Name is required.';
			return;
		}
		waveCreating = true;
		waveCreateError = null;
		try {
			await client().send(
				new CreateWaveCommand({ name: newWaveName, description: newWaveDescription || undefined })
			);
			toast.success('Wave created');
			waveCreateModal?.close();
			await tabLoader.refresh('waves');
		} catch (e) {
			const msg = describeError(e);
			waveCreateError = msg;
			toast.error(msg);
		} finally {
			waveCreating = false;
		}
	}

	let waveEditModal = $state<Modal | null>(null);
	let waveEditing = $state(false);
	let waveEditError = $state<string | null>(null);
	let editWaveId = $state('');
	let editWaveName = $state('');
	let editWaveDescription = $state('');

	function openWaveEditModal(w: Wave): void {
		waveEditError = null;
		editWaveId = w.waveID ?? '';
		editWaveName = w.name ?? '';
		editWaveDescription = w.description ?? '';
		waveEditModal?.open();
	}

	async function submitEditWave(): Promise<void> {
		if (!editWaveId) return;
		waveEditing = true;
		waveEditError = null;
		try {
			await client().send(
				new UpdateWaveCommand({
					waveID: editWaveId,
					name: editWaveName || undefined,
					description: editWaveDescription || undefined
				})
			);
			toast.success('Wave updated');
			waveEditModal?.close();
			await tabLoader.refresh('waves');
		} catch (e) {
			const msg = describeError(e);
			waveEditError = msg;
			toast.error(msg);
		} finally {
			waveEditing = false;
		}
	}

	async function toggleWaveArchive(w: Wave): Promise<void> {
		if (!w.waveID) return;
		try {
			if (w.isArchived) {
				await client().send(new UnarchiveWaveCommand({ waveID: w.waveID }));
				toast.success('Wave unarchived');
			} else {
				await client().send(new ArchiveWaveCommand({ waveID: w.waveID }));
				toast.success('Wave archived');
			}
			await tabLoader.refresh('waves');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteWave(w: Wave): Promise<void> {
		if (!w.waveID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete wave',
			message: `Delete wave ${w.name ?? w.waveID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteWaveCommand({ waveID: w.waveID }));
			toast.success('Wave deleted');
			await tabLoader.refresh('waves');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Server}
		title="AWS Application Migration Service"
		description="Lift-and-shift migration service to AWS"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'applications'}
				<button
					onclick={openAppCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create application
				</button>
			{:else if activeTab === 'waves'}
				<button
					onclick={openWaveCreateModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create wave
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

			{#if activeTab === 'servers'}
				{#snippet serverStateCell(s: SourceServer)}
					<span class="text-xs px-2 py-1 rounded-full {lifecycleClass(s.lifeCycle?.state)}"
						>{s.lifeCycle?.state ?? '—'}</span
					>
				{/snippet}
				{#snippet serverReplCell(s: SourceServer)}
					<span class="text-xs text-gray-500 dark:text-gray-400"
						>{s.dataReplicationInfo?.dataReplicationState ?? '—'}</span
					>
				{/snippet}
				{#snippet serverActionsCell(s: SourceServer)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openServerDetail(s)}
							title="View"
							aria-label="View source server {s.sourceServerID}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => deleteServer(s)}
							title="Delete"
							aria-label="Delete source server {s.sourceServerID}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const serverColumns = defineColumns<SourceServer>([
					{ key: 'sourceServerID', label: 'Source Server ID' },
					{ key: 'lifeCycle', label: 'Lifecycle State', render: serverStateCell },
					{ key: 'dataReplicationInfo', label: 'Replication', render: serverReplCell },
					{ key: 'applicationID', label: 'Application' },
					{ key: 'actions', label: '', render: serverActionsCell }
				])}
				<DataTable
					rows={filteredServers}
					rowKey={(s) => s.sourceServerID ?? ''}
					columns={serverColumns}
					loading={tabLoader.isLoading('servers')}
					emptyMessage="No source servers found"
				/>
			{:else if activeTab === 'applications'}
				{#snippet appArchivedCell(a: Application)}
					<span
						class="text-xs px-2 py-1 rounded-full {a.isArchived
							? 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
							: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'}"
						>{a.isArchived ? 'Archived' : 'Active'}</span
					>
				{/snippet}
				{#snippet appActionsCell(a: Application)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openAppEditModal(a)}
							title="Edit"
							aria-label="Edit application {a.name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => toggleAppArchive(a)}
							title={a.isArchived ? 'Unarchive' : 'Archive'}
							aria-label={a.isArchived ? `Unarchive application ${a.name}` : `Archive application ${a.name}`}
							class="text-gray-400 hover:text-blue-500"
						>
							{#if a.isArchived}<ArchiveRestore class="w-4 h-4" />{:else}<Archive class="w-4 h-4" />{/if}
						</button>
						<button
							onclick={() => deleteApplication(a)}
							title="Delete"
							aria-label="Delete application {a.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const appColumns = defineColumns<Application>([
					{ key: 'name', label: 'Name' },
					{ key: 'description', label: 'Description' },
					{ key: 'waveID', label: 'Wave' },
					{ key: 'isArchived', label: 'Status', render: appArchivedCell },
					{ key: 'actions', label: '', render: appActionsCell }
				])}
				<DataTable
					rows={filteredApps}
					rowKey={(a) => a.applicationID ?? ''}
					columns={appColumns}
					loading={tabLoader.isLoading('applications')}
					emptyMessage="No applications found"
				/>
			{:else if activeTab === 'waves'}
				{#snippet waveArchivedCell(w: Wave)}
					<span
						class="text-xs px-2 py-1 rounded-full {w.isArchived
							? 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'
							: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'}"
						>{w.isArchived ? 'Archived' : 'Active'}</span
					>
				{/snippet}
				{#snippet waveActionsCell(w: Wave)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openWaveEditModal(w)}
							title="Edit"
							aria-label="Edit wave {w.name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => toggleWaveArchive(w)}
							title={w.isArchived ? 'Unarchive' : 'Archive'}
							aria-label={w.isArchived ? `Unarchive wave ${w.name}` : `Archive wave ${w.name}`}
							class="text-gray-400 hover:text-blue-500"
						>
							{#if w.isArchived}<ArchiveRestore class="w-4 h-4" />{:else}<Archive class="w-4 h-4" />{/if}
						</button>
						<button
							onclick={() => deleteWave(w)}
							title="Delete"
							aria-label="Delete wave {w.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const waveColumns = defineColumns<Wave>([
					{ key: 'name', label: 'Name' },
					{ key: 'description', label: 'Description' },
					{ key: 'isArchived', label: 'Status', render: waveArchivedCell },
					{ key: 'actions', label: '', render: waveActionsCell }
				])}
				<DataTable
					rows={filteredWaves}
					rowKey={(w) => w.waveID ?? ''}
					columns={waveColumns}
					loading={tabLoader.isLoading('waves')}
					emptyMessage="No waves found"
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={serverDetailModal} title="Source Server">
	{#snippet children()}
		{#if viewedServer}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Source Server ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedServer.sourceServerID ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Lifecycle state</dt>
					<dd class="text-slate-900 dark:text-white">{viewedServer.lifeCycle?.state ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Replication state</dt>
					<dd class="text-slate-900 dark:text-white">
						{viewedServer.dataReplicationInfo?.dataReplicationState ?? '—'}
					</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Application ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedServer.applicationID ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">OS</dt>
					<dd class="text-slate-900 dark:text-white">{viewedServer.sourceProperties?.os?.fullString ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Launched instance</dt>
					<dd class="text-slate-900 dark:text-white">
						{viewedServer.launchedInstance?.ec2InstanceID ?? 'None'}
					</dd>
				</div>
			</dl>

			<div class="mt-4 border-t border-slate-200 dark:border-slate-700 pt-4 space-y-3">
				<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Lifecycle actions</p>
				<div class="flex flex-wrap gap-2">
					<button
						onclick={startTest}
						disabled={serverActionBusy}
						class="px-3 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
						>Start test</button
					>
					<button
						onclick={startCutover}
						disabled={serverActionBusy}
						class="px-3 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
						>Start cutover</button
					>
					<button
						onclick={finalizeCutover}
						disabled={serverActionBusy}
						class="px-3 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
						>Finalize cutover</button
					>
					<button
						onclick={terminateTargetInstances}
						disabled={serverActionBusy}
						class="px-3 py-1.5 text-xs rounded-lg bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50"
						>Terminate target instances</button
					>
					<button
						onclick={disconnectFromService}
						disabled={serverActionBusy}
						class="px-3 py-1.5 text-xs rounded-lg bg-red-600 text-white hover:bg-red-700 disabled:opacity-50"
						>Disconnect from service</button
					>
				</div>
				<div class="flex items-center gap-2">
					<label for="mgn-lifecycle-select" class="text-xs text-slate-500 dark:text-slate-400"
						>Set lifecycle state</label
					>
					<select
						id="mgn-lifecycle-select"
						bind:value={newLifecycleState}
						class="px-2 py-1 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="READY_FOR_TEST">READY_FOR_TEST</option>
						<option value="READY_FOR_CUTOVER">READY_FOR_CUTOVER</option>
						<option value="CUTOVER">CUTOVER</option>
					</select>
					<button
						onclick={applyLifecycleState}
						disabled={serverActionBusy}
						class="px-3 py-1 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50"
						>Apply</button
					>
				</div>
				{#if serverActionError}
					<p class="text-sm text-red-600 dark:text-red-400">{serverActionError}</p>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => serverDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={appCreateModal} title="Create Application">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mgn-app-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="mgn-app-new-name"
					bind:value={newAppName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="mgn-app-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input
					id="mgn-app-new-desc"
					bind:value={newAppDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if appCreateError}
				<p class="text-sm text-red-600 dark:text-red-400">{appCreateError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => appCreateModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateApp}
			disabled={appCreating}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{appCreating ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={appEditModal} title="Edit Application">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mgn-app-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="mgn-app-edit-name"
					bind:value={editAppName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="mgn-app-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input
					id="mgn-app-edit-desc"
					bind:value={editAppDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if appEditError}
				<p class="text-sm text-red-600 dark:text-red-400">{appEditError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => appEditModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditApp}
			disabled={appEditing}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{appEditing ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={waveCreateModal} title="Create Wave">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mgn-wave-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="mgn-wave-new-name"
					bind:value={newWaveName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="mgn-wave-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input
					id="mgn-wave-new-desc"
					bind:value={newWaveDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if waveCreateError}
				<p class="text-sm text-red-600 dark:text-red-400">{waveCreateError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => waveCreateModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateWave}
			disabled={waveCreating}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{waveCreating ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={waveEditModal} title="Edit Wave">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mgn-wave-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="mgn-wave-edit-name"
					bind:value={editWaveName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="mgn-wave-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input
					id="mgn-wave-edit-desc"
					bind:value={editWaveDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if waveEditError}
				<p class="text-sm text-red-600 dark:text-red-400">{waveEditError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => waveEditModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditWave}
			disabled={waveEditing}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{waveEditing ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>
