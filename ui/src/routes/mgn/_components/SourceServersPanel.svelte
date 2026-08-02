<script lang="ts">
	// Source server lifecycle & data replication control (services/mgn/PARITY.md
	// family A, 16 ops) plus the three per-server-scoped families that only make
	// sense inside a source server's own detail view: per-server Launch/
	// Replication configuration (GetLaunchConfiguration/UpdateLaunchConfiguration,
	// GetReplicationConfiguration/UpdateReplicationConfiguration -- family C/D's
	// per-server halves) and post-launch custom actions
	// (PutSourceServerAction/ListSourceServerActions/RemoveSourceServerAction --
	// family J's server-scoped half). 23 ops in total live in this one panel.
	//
	// No AWS operation in this 95-op surface creates a SourceServer the way real
	// AWS does (an on-prem replication agent registering itself) -- the only
	// public-API creation path is StartImport's CSV bulk load (see the
	// Import/Export tab). This panel has no "Create" button; it explains that
	// up front instead of a create button that would silently do nothing.
	import {
		DescribeSourceServersCommand,
		DeleteSourceServerCommand,
		ChangeServerLifeCycleStateCommand,
		UpdateSourceServerReplicationTypeCommand,
		DisconnectFromServiceCommand,
		FinalizeCutoverCommand,
		MarkAsArchivedCommand,
		StartTestCommand,
		StartCutoverCommand,
		StartReplicationCommand,
		StopReplicationCommand,
		PauseReplicationCommand,
		ResumeReplicationCommand,
		RetryDataReplicationCommand,
		TerminateTargetInstancesCommand,
		GetLaunchConfigurationCommand,
		UpdateLaunchConfigurationCommand,
		GetReplicationConfigurationCommand,
		UpdateReplicationConfigurationCommand,
		ListSourceServerActionsCommand,
		PutSourceServerActionCommand,
		RemoveSourceServerActionCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type SourceServer,
		type LaunchConfiguration,
		type ReplicationConfiguration,
		type SourceServerActionDocument,
		type ChangeServerLifeCycleStateSourceServerLifecycleState,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import { formatBytes } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = {
		client: () => MgnClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let servers = $state<SourceServer[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchServers(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeSourceServersCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		servers = reset ? (resp.items ?? []) : [...servers, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchServers(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchServers(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	// Fires once on mount (this panel only exists in the DOM while its tab is
	// active -- +page.svelte swaps panels via {#if}, not CSS visibility) and
	// again on any real region change. No untrack() dance is needed here:
	// unlike a page-level onRegionChange that reads a persistent tab
	// selector (dax/+page.svelte's documented hazard), this effect owns no
	// state that changing tabs would perturb -- the whole component is
	// torn down when the user navigates away from this tab.
	onRegionChange(() => void refresh());

	const filtered = $derived(
		servers.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(s.sourceServerID ?? '').toLowerCase().includes(q) ||
				(s.lifeCycle?.state ?? '').toLowerCase().includes(q) ||
				(s.applicationID ?? '').toLowerCase().includes(q) ||
				(s.userProvidedID ?? '').toLowerCase().includes(q) ||
				(s.sourceProperties?.identificationHints?.hostname ?? '').toLowerCase().includes(q)
			);
		})
	);

	function lifecycleClass(state: string | undefined): string {
		if (state === 'CUTOVER')
			return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (state === 'READY_FOR_CUTOVER' || state === 'READY_FOR_TEST')
			return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400';
		if (state === 'NOT_READY' || state === 'DISCONNECTED' || state === 'STOPPED')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	async function deleteServer(s: SourceServer): Promise<void> {
		if (!s.sourceServerID) return;
		const confirmed = await confirmDestructive({
			title: 'Delete source server',
			message: `Delete source server ${s.sourceServerID}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSourceServerCommand({ sourceServerID: s.sourceServerID }));
			toast.success('Source server deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ---------------------------- Detail modal -----------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<SourceServer | null>(null);
	let detailLoading = $state(false);
	let detailError = $state<string | null>(null);
	let actionBusy = $state(false);
	let actionError = $state<string | null>(null);
	let newLifecycleState = $state<ChangeServerLifeCycleStateSourceServerLifecycleState>('READY_FOR_TEST');
	let newReplicationType = $state<'AGENT_BASED' | 'SNAPSHOT_SHIPPING'>('AGENT_BASED');

	let launchConfig = $state<LaunchConfiguration | null>(null);
	let launchConfigError = $state<string | null>(null);
	let lcName = $state('');
	let lcBootMode = $state<'LEGACY_BIOS' | 'UEFI' | 'USE_SOURCE'>('USE_SOURCE');
	let lcLaunchDisposition = $state<'STARTED' | 'STOPPED'>('STARTED');
	let lcCopyPrivateIp = $state(false);
	let lcCopyTags = $state(false);

	let replicationConfig = $state<ReplicationConfiguration | null>(null);
	let replicationConfigError = $state<string | null>(null);
	let rcName = $state('');
	let rcInstanceType = $state('');
	let rcBandwidthThrottling = $state(0);
	let rcDataPlaneRouting = $state<'PRIVATE_IP' | 'PUBLIC_IP'>('PRIVATE_IP');
	let rcCreatePublicIP = $state(false);
	let rcUseDedicated = $state(false);

	let actions = $state<SourceServerActionDocument[]>([]);
	let actionsError = $state<string | null>(null);
	let newActionID = $state('');
	let newActionName = $state('');
	let newActionDoc = $state('');
	let newActionOrder = $state(100);

	async function openDetail(s: SourceServer): Promise<void> {
		if (!s.sourceServerID) return;
		viewed = s;
		detailError = null;
		actionError = null;
		launchConfigError = null;
		replicationConfigError = null;
		actionsError = null;
		newLifecycleState = 'READY_FOR_TEST';
		newReplicationType = (s.replicationType as typeof newReplicationType) ?? 'AGENT_BASED';
		detailModal?.open();
		detailLoading = true;
		try {
			const resp = await client().send(
				new DescribeSourceServersCommand({ filters: { sourceServerIDs: [s.sourceServerID] } })
			);
			viewed = resp.items?.[0] ?? s;
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailLoading = false;
		}
		await Promise.all([loadLaunchConfig(), loadReplicationConfig(), loadActions()]);
	}

	async function loadLaunchConfig(): Promise<void> {
		if (!viewed?.sourceServerID) return;
		try {
			const resp = await client().send(
				new GetLaunchConfigurationCommand({ sourceServerID: viewed.sourceServerID })
			);
			launchConfig = resp;
			lcName = resp.name ?? '';
			lcBootMode = (resp.bootMode as typeof lcBootMode) ?? 'USE_SOURCE';
			lcLaunchDisposition = (resp.launchDisposition as typeof lcLaunchDisposition) ?? 'STARTED';
			lcCopyPrivateIp = resp.copyPrivateIp ?? false;
			lcCopyTags = resp.copyTags ?? false;
		} catch (e) {
			launchConfigError = describeError(e);
		}
	}

	async function loadReplicationConfig(): Promise<void> {
		if (!viewed?.sourceServerID) return;
		try {
			const resp = await client().send(
				new GetReplicationConfigurationCommand({ sourceServerID: viewed.sourceServerID })
			);
			replicationConfig = resp;
			rcName = resp.name ?? '';
			rcInstanceType = resp.replicationServerInstanceType ?? '';
			rcBandwidthThrottling = resp.bandwidthThrottling ?? 0;
			rcDataPlaneRouting = (resp.dataPlaneRouting as typeof rcDataPlaneRouting) ?? 'PRIVATE_IP';
			rcCreatePublicIP = resp.createPublicIP ?? false;
			rcUseDedicated = resp.useDedicatedReplicationServer ?? false;
		} catch (e) {
			replicationConfigError = describeError(e);
		}
	}

	async function loadActions(): Promise<void> {
		if (!viewed?.sourceServerID) return;
		try {
			const resp = await client().send(
				new ListSourceServerActionsCommand({ sourceServerID: viewed.sourceServerID })
			);
			actions = resp.items ?? [];
		} catch (e) {
			actionsError = describeError(e);
		}
	}

	async function runServerAction(action: () => Promise<SourceServer | void>, successMessage: string): Promise<void> {
		if (!viewed?.sourceServerID) return;
		actionBusy = true;
		actionError = null;
		try {
			const updated = await action();
			toast.success(successMessage);
			if (updated) viewed = updated;
			await refresh();
		} catch (e) {
			const msg = describeError(e);
			actionError = msg;
			toast.error(msg);
		} finally {
			actionBusy = false;
		}
	}

	function changeLifecycleState(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() =>
				client().send(
					new ChangeServerLifeCycleStateCommand({ sourceServerID: id, lifeCycle: { state: newLifecycleState } })
				),
			'Lifecycle state updated'
		);
	}

	function updateReplicationType(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() =>
				client().send(
					new UpdateSourceServerReplicationTypeCommand({ sourceServerID: id, replicationType: newReplicationType })
				),
			'Replication type updated'
		);
	}

	function startTest(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(async () => {
			const resp = await client().send(new StartTestCommand({ sourceServerIDs: [id] }));
			toast.message(`Job ${resp.job?.jobID ?? '?'} started -- see the Jobs tab for progress`);
		}, 'Test launch started');
	}

	function startCutover(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(async () => {
			const resp = await client().send(new StartCutoverCommand({ sourceServerIDs: [id] }));
			toast.message(`Job ${resp.job?.jobID ?? '?'} started -- see the Jobs tab for progress`);
		}, 'Cutover started');
	}

	function terminateTargetInstances(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(async () => {
			const resp = await client().send(new TerminateTargetInstancesCommand({ sourceServerIDs: [id] }));
			toast.message(`Job ${resp.job?.jobID ?? '?'} started -- see the Jobs tab for progress`);
		}, 'Target instance termination started');
	}

	function finalizeCutover(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new FinalizeCutoverCommand({ sourceServerID: id })),
			'Cutover finalized'
		);
	}

	function disconnectFromService(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new DisconnectFromServiceCommand({ sourceServerID: id })),
			'Source server disconnected'
		);
	}

	function markAsArchived(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new MarkAsArchivedCommand({ sourceServerID: id })),
			'Marked as archived'
		);
	}

	function startReplication(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(async () => {
			await client().send(new StartReplicationCommand({ sourceServerID: id }));
		}, 'Replication started');
	}

	function stopReplication(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new StopReplicationCommand({ sourceServerID: id })),
			'Replication stopped'
		);
	}

	function pauseReplication(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new PauseReplicationCommand({ sourceServerID: id })),
			'Replication paused'
		);
	}

	function resumeReplication(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new ResumeReplicationCommand({ sourceServerID: id })),
			'Replication resumed'
		);
	}

	function retryDataReplication(): void {
		const id = viewed?.sourceServerID;
		if (!id) return;
		void runServerAction(
			() => client().send(new RetryDataReplicationCommand({ sourceServerID: id })),
			'Data replication retry requested'
		);
	}

	async function submitLaunchConfig(): Promise<void> {
		if (!viewed?.sourceServerID) return;
		launchConfigError = null;
		try {
			launchConfig = await client().send(
				new UpdateLaunchConfigurationCommand({
					sourceServerID: viewed.sourceServerID,
					name: lcName || undefined,
					bootMode: lcBootMode,
					launchDisposition: lcLaunchDisposition,
					copyPrivateIp: lcCopyPrivateIp,
					copyTags: lcCopyTags
				})
			);
			toast.success('Launch configuration updated');
		} catch (e) {
			launchConfigError = describeError(e);
			toast.error(launchConfigError);
		}
	}

	async function submitReplicationConfig(): Promise<void> {
		if (!viewed?.sourceServerID) return;
		replicationConfigError = null;
		try {
			replicationConfig = await client().send(
				new UpdateReplicationConfigurationCommand({
					sourceServerID: viewed.sourceServerID,
					name: rcName || undefined,
					replicationServerInstanceType: rcInstanceType || undefined,
					bandwidthThrottling: rcBandwidthThrottling || undefined,
					dataPlaneRouting: rcDataPlaneRouting,
					createPublicIP: rcCreatePublicIP,
					useDedicatedReplicationServer: rcUseDedicated
				})
			);
			toast.success('Replication configuration updated');
		} catch (e) {
			replicationConfigError = describeError(e);
			toast.error(replicationConfigError);
		}
	}

	async function submitNewAction(): Promise<void> {
		if (!viewed?.sourceServerID || !newActionID.trim() || !newActionName.trim() || !newActionDoc.trim()) {
			actionsError = 'Action ID, name and document identifier are required.';
			return;
		}
		try {
			await client().send(
				new PutSourceServerActionCommand({
					sourceServerID: viewed.sourceServerID,
					actionID: newActionID.trim(),
					actionName: newActionName.trim(),
					documentIdentifier: newActionDoc.trim(),
					order: newActionOrder
				})
			);
			toast.success('Post-launch action saved');
			newActionID = '';
			newActionName = '';
			newActionDoc = '';
			await loadActions();
		} catch (e) {
			actionsError = describeError(e);
			toast.error(actionsError);
		}
	}

	async function removeAction(actionID: string | undefined): Promise<void> {
		if (!viewed?.sourceServerID || !actionID) return;
		try {
			await client().send(
				new RemoveSourceServerActionCommand({ sourceServerID: viewed.sourceServerID, actionID })
			);
			toast.success('Post-launch action removed');
			await loadActions();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.sourceServerID) return;
		const arn = taggableArn('source-server', viewed.sourceServerID, currentRegion());
		await client().send(new TagResourceCommand({ resourceArn: arn, tags: { [key]: value } }));
		viewed = { ...viewed, tags: { ...viewed.tags, [key]: value } };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.sourceServerID) return;
		const arn = taggableArn('source-server', viewed.sourceServerID, currentRegion());
		await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
		const rest = { ...viewed.tags };
		delete rest[key];
		viewed = { ...viewed, tags: rest };
	}

	const actionColumns = defineColumns<SourceServerActionDocument>([
		{ key: 'order', label: 'Order' },
		{ key: 'actionID', label: 'Action ID' },
		{ key: 'actionName', label: 'Name' },
		{ key: 'documentIdentifier', label: 'Document' }
	]);
</script>

<div class="rounded-lg border border-blue-200 dark:border-blue-900 bg-blue-50 dark:bg-blue-950/30 px-4 py-3 text-sm text-blue-800 dark:text-blue-300">
	No AWS operation registers a source server the way real AWS does (an on-prem
	replication agent calling in). The only way to create source servers here is
	<strong>StartImport</strong>, on the Import/Export tab, which reads a CSV
	from S3.
</div>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

{#snippet stateCell(s: SourceServer)}
	<span class="text-xs px-2 py-1 rounded-full {lifecycleClass(s.lifeCycle?.state)}">{s.lifeCycle?.state ?? '—'}</span>
{/snippet}
{#snippet replCell(s: SourceServer)}
	<span class="text-xs text-gray-500 dark:text-gray-400">{s.dataReplicationInfo?.dataReplicationState ?? '—'}</span>
{/snippet}
{#snippet hostCell(s: SourceServer)}
	{s.sourceProperties?.identificationHints?.hostname ?? '—'}
{/snippet}
{#snippet rowActions(s: SourceServer)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(s)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteServer(s)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(s) => s.sourceServerID ?? ''}
	columns={defineColumns<SourceServer>([
		{ key: 'sourceServerID', label: 'Source Server ID' },
		{ key: 'hostname', label: 'Hostname', render: hostCell },
		{ key: 'lifeCycle', label: 'Lifecycle', render: stateCell },
		{ key: 'dataReplicationInfo', label: 'Replication (simulated)', render: replCell },
		{ key: 'applicationID', label: 'Application' },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No source servers found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={detailModal} title="Source server {viewed?.sourceServerID ?? ''}">
	{#snippet children()}
		{#if detailLoading}
			<p class="text-sm text-slate-500">Loading…</p>
		{:else if detailError}
			<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>
		{:else if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Lifecycle state</dt><dd>{viewed.lifeCycle?.state ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Replication type</dt><dd>{viewed.replicationType ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Application ID</dt><dd>{viewed.applicationID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">User provided ID</dt><dd>{viewed.userProvidedID ?? '—'}</dd></div>
					<div><dt class="text-slate-500">OS</dt><dd>{viewed.sourceProperties?.os?.fullString ?? '—'}</dd></div>
					<div><dt class="text-slate-500">RAM</dt><dd>{formatBytes(viewed.sourceProperties?.ramBytes)}</dd></div>
					<div><dt class="text-slate-500">Archived</dt><dd>{viewed.isArchived ? 'Yes' : 'No'}</dd></div>
					<div><dt class="text-slate-500">Launched instance</dt><dd>{viewed.launchedInstance?.ec2InstanceID ?? 'None'}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">
						Data replication
						<span class="ml-1 text-xs font-normal text-amber-600 dark:text-amber-400"
							>(simulated, time-based progression -- not real block transfer)</span
						>
					</p>
					<p class="text-sm text-slate-600 dark:text-slate-300">
						State: {viewed.dataReplicationInfo?.dataReplicationState ?? '—'}
					</p>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Lifecycle &amp; replication actions</p>
					<div class="flex flex-wrap gap-2">
						<button onclick={startTest} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Start test</button>
						<button onclick={startCutover} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Start cutover</button>
						<button onclick={finalizeCutover} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Finalize cutover</button>
						<button onclick={terminateTargetInstances} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50">Terminate target instances</button>
						<button onclick={disconnectFromService} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-red-600 text-white hover:bg-red-700 disabled:opacity-50">Disconnect from service</button>
						<button onclick={markAsArchived} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Mark as archived</button>
						<button onclick={startReplication} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-emerald-600 text-white hover:bg-emerald-700 disabled:opacity-50">Start replication</button>
						<button onclick={stopReplication} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Stop replication</button>
						<button onclick={pauseReplication} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Pause replication</button>
						<button onclick={resumeReplication} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Resume replication</button>
						<button onclick={retryDataReplication} disabled={actionBusy} class="px-3 py-1.5 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Retry data replication</button>
					</div>
					<div class="flex flex-wrap items-center gap-2">
						<label for="mgn-ss-lifecycle" class="text-xs text-slate-500">Set lifecycle state</label>
						<select id="mgn-ss-lifecycle" bind:value={newLifecycleState} class="px-2 py-1 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							<option value="READY_FOR_TEST">READY_FOR_TEST</option>
							<option value="READY_FOR_CUTOVER">READY_FOR_CUTOVER</option>
							<option value="CUTOVER">CUTOVER</option>
						</select>
						<button onclick={changeLifecycleState} disabled={actionBusy} class="px-3 py-1 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Apply</button>
					</div>
					<div class="flex flex-wrap items-center gap-2">
						<label for="mgn-ss-repltype" class="text-xs text-slate-500">Replication type</label>
						<select id="mgn-ss-repltype" bind:value={newReplicationType} class="px-2 py-1 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							<option value="AGENT_BASED">AGENT_BASED</option>
							<option value="SNAPSHOT_SHIPPING">SNAPSHOT_SHIPPING</option>
						</select>
						<button onclick={updateReplicationType} disabled={actionBusy} class="px-3 py-1 text-xs rounded-lg bg-slate-600 text-white hover:bg-slate-700 disabled:opacity-50">Apply</button>
					</div>
					{#if actionError}<p class="text-sm text-red-600 dark:text-red-400">{actionError}</p>{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Launch configuration</p>
					{#if launchConfigError}<p class="text-sm text-red-600 dark:text-red-400">{launchConfigError}</p>{/if}
					{#if launchConfig}
						<div class="grid grid-cols-2 gap-2 text-sm">
							<label class="flex flex-col gap-1">Name
								<input bind:value={lcName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</label>
							<label class="flex flex-col gap-1">Boot mode
								<select bind:value={lcBootMode} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
									<option value="USE_SOURCE">USE_SOURCE</option>
									<option value="LEGACY_BIOS">LEGACY_BIOS</option>
									<option value="UEFI">UEFI</option>
								</select>
							</label>
							<label class="flex flex-col gap-1">Launch disposition
								<select bind:value={lcLaunchDisposition} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
									<option value="STARTED">STARTED</option>
									<option value="STOPPED">STOPPED</option>
								</select>
							</label>
							<label class="flex items-center gap-2"><input type="checkbox" bind:checked={lcCopyPrivateIp} /> Copy private IP</label>
							<label class="flex items-center gap-2"><input type="checkbox" bind:checked={lcCopyTags} /> Copy tags</label>
						</div>
						<button onclick={submitLaunchConfig} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Save launch configuration</button>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Replication configuration</p>
					{#if replicationConfigError}<p class="text-sm text-red-600 dark:text-red-400">{replicationConfigError}</p>{/if}
					{#if replicationConfig}
						<div class="grid grid-cols-2 gap-2 text-sm">
							<label class="flex flex-col gap-1">Name
								<input bind:value={rcName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</label>
							<label class="flex flex-col gap-1">Replication server instance type
								<input bind:value={rcInstanceType} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</label>
							<label class="flex flex-col gap-1">Bandwidth throttling (Mbps, 0 = unlimited)
								<input type="number" min="0" bind:value={rcBandwidthThrottling} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							</label>
							<label class="flex flex-col gap-1">Data plane routing
								<select bind:value={rcDataPlaneRouting} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
									<option value="PRIVATE_IP">PRIVATE_IP</option>
									<option value="PUBLIC_IP">PUBLIC_IP</option>
								</select>
							</label>
							<label class="flex items-center gap-2"><input type="checkbox" bind:checked={rcCreatePublicIP} /> Create public IP</label>
							<label class="flex items-center gap-2"><input type="checkbox" bind:checked={rcUseDedicated} /> Use dedicated replication server</label>
						</div>
						<button onclick={submitReplicationConfig} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Save replication configuration</button>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Post-launch actions</p>
					{#if actionsError}<p class="text-sm text-red-600 dark:text-red-400">{actionsError}</p>{/if}
					{#snippet actionRowActions(a: SourceServerActionDocument)}
						<button onclick={() => removeAction(a.actionID)} class="text-red-600 hover:underline text-sm">Remove</button>
					{/snippet}
					<DataTable
						rows={actions}
						rowKey={(a) => a.actionID ?? ''}
						columns={[...actionColumns, { key: 'remove', label: '', render: actionRowActions }]}
						loading={false}
						emptyMessage="No post-launch actions configured"
					/>
					<div class="grid grid-cols-4 gap-2">
						<input bind:value={newActionID} placeholder="Action ID" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input bind:value={newActionName} placeholder="Action name" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input bind:value={newActionDoc} placeholder="SSM document identifier" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input type="number" bind:value={newActionOrder} placeholder="Order" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
					<button onclick={submitNewAction} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Add action</button>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={viewed.tags ?? {}} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
