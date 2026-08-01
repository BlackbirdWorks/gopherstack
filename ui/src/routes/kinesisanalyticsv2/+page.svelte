<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getKinesisAnalyticsV2Client } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		CreateApplicationCommand,
		DeleteApplicationCommand,
		DescribeApplicationCommand,
		StartApplicationCommand,
		StopApplicationCommand,
		CreateApplicationSnapshotCommand,
		ListApplicationSnapshotsCommand,
		DeleteApplicationSnapshotCommand,
		RollbackApplicationCommand,
		UpdateApplicationMaintenanceConfigurationCommand,
		ListApplicationVersionsCommand,
		DiscoverInputSchemaCommand,
		type ApplicationSummary,
		type SnapshotDetails
	} from '@aws-sdk/client-kinesis-analytics-v2';
	import { toast } from 'svelte-sonner';
	import {
		RefreshCw,
		Trash2,
		Plus,
		Play,
		Square,
		Camera,
		RotateCcw,
		Settings,
		ChevronRight,
		Layers,
		Search
	} from 'lucide-svelte';

	const kda = regionalClient(getKinesisAnalyticsV2Client);

	// Applications list
	let loading = $state(false);
	let applications = $state<ApplicationSummary[]>([]);
	let selectedApp = $state<string | null>(null);

	// Create application form
	let showCreateForm = $state(false);
	let newAppName = $state('');
	let newAppRuntime = $state('FLINK-1_18');
	let newAppRole = $state('');
	let creating = $state(false);

	// Snapshots panel
	let snapshots = $state<SnapshotDetails[]>([]);
	let loadingSnapshots = $state(false);
	let newSnapshotName = $state('');

	// Versions panel
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let versions = $state<any[]>([]);
	let showVersions = $state(false);

	// Maintenance window
	let showMaintenance = $state(false);
	let maintenanceWindow = $state('');
	let updatingMaintenance = $state(false);

	// Schema discovery
	let showSchemaDiscovery = $state(false);
	let schemaResourceARN = $state('');
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let discoveredSchema = $state<any | null>(null);
	let discoveringSchema = $state(false);

	// Runtime environments
	const runtimes = [
		'FLINK-1_6',
		'FLINK-1_8',
		'FLINK-1_11',
		'FLINK-1_13',
		'FLINK-1_15',
		'FLINK-1_18',
		'SQL-1_0',
		'ZEPPELIN-FLINK-1_0',
		'ZEPPELIN-FLINK-2_0',
		'ZEPPELIN-FLINK-3_0'
	];

	async function loadApplications() {
		loading = true;
		try {
			const resp = await kda().send(new ListApplicationsCommand({}));
			applications = resp.ApplicationSummaries ?? [];
		} catch (e) {
			toast.error(`Failed to load applications: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function createApplication() {
		if (!newAppName.trim()) {
			toast.error('Application name is required');
			return;
		}
		creating = true;
		try {
			await kda().send(
				new CreateApplicationCommand({
					ApplicationName: newAppName.trim(),
					RuntimeEnvironment: newAppRuntime as never,
					ServiceExecutionRole: newAppRole.trim() || undefined
				})
			);
			toast.success(`Application "${newAppName}" created`);
			newAppName = '';
			newAppRole = '';
			showCreateForm = false;
			await loadApplications();
		} catch (e) {
			toast.error(`Failed to create application: ${e}`);
		} finally {
			creating = false;
		}
	}

	async function deleteApplication(name: string) {
		try {
			const detail = await kda().send(new DescribeApplicationCommand({ ApplicationName: name }));
			const ts = detail.ApplicationDetail?.CreateTimestamp;
			await kda().send(
				new DeleteApplicationCommand({
					ApplicationName: name,
					CreateTimestamp: ts ?? new Date()
				})
			);
			toast.success(`Application "${name}" deleted`);
			if (selectedApp === name) selectedApp = null;
			await loadApplications();
		} catch (e) {
			toast.error(`Failed to delete application: ${e}`);
		}
	}

	async function startApplication(name: string) {
		try {
			await kda().send(new StartApplicationCommand({ ApplicationName: name }));
			toast.success(`Application "${name}" started`);
			await loadApplications();
		} catch (e) {
			toast.error(`Failed to start application: ${e}`);
		}
	}

	async function stopApplication(name: string) {
		try {
			await kda().send(new StopApplicationCommand({ ApplicationName: name }));
			toast.success(`Application "${name}" stopped`);
			await loadApplications();
		} catch (e) {
			toast.error(`Failed to stop application: ${e}`);
		}
	}

	async function loadSnapshots(name: string) {
		loadingSnapshots = true;
		try {
			const resp = await kda().send(
				new ListApplicationSnapshotsCommand({ ApplicationName: name })
			);
			snapshots = resp.SnapshotSummaries ?? [];
		} catch (e) {
			toast.error(`Failed to load snapshots: ${e}`);
		} finally {
			loadingSnapshots = false;
		}
	}

	async function createSnapshot(name: string) {
		if (!newSnapshotName.trim()) {
			toast.error('Snapshot name is required');
			return;
		}
		try {
			await kda().send(
				new CreateApplicationSnapshotCommand({
					ApplicationName: name,
					SnapshotName: newSnapshotName.trim()
				})
			);
			toast.success(`Snapshot "${newSnapshotName}" created`);
			newSnapshotName = '';
			await loadSnapshots(name);
		} catch (e) {
			toast.error(`Failed to create snapshot: ${e}`);
		}
	}

	async function deleteSnapshot(appName: string, snapshotName: string, snapshotTs: Date | undefined) {
		try {
			await kda().send(
				new DeleteApplicationSnapshotCommand({
					ApplicationName: appName,
					SnapshotName: snapshotName,
					SnapshotCreationTimestamp: snapshotTs ?? new Date()
				})
			);
			toast.success(`Snapshot "${snapshotName}" deleted`);
			await loadSnapshots(appName);
		} catch (e) {
			toast.error(`Failed to delete snapshot: ${e}`);
		}
	}

	async function rollbackApp(name: string) {
		try {
			const detail = await kda().send(new DescribeApplicationCommand({ ApplicationName: name }));
			const versionId = detail.ApplicationDetail?.ApplicationVersionId ?? 1;
			await kda().send(
				new RollbackApplicationCommand({
					ApplicationName: name,
					CurrentApplicationVersionId: versionId
				})
			);
			toast.success(`Application "${name}" rolled back`);
			await loadApplications();
		} catch (e) {
			toast.error(`Failed to rollback application: ${e}`);
		}
	}

	async function loadVersions(name: string) {
		try {
			const resp = await kda().send(new ListApplicationVersionsCommand({ ApplicationName: name }));
			versions = resp.ApplicationVersionSummaries ?? [];
			showVersions = true;
		} catch (e) {
			toast.error(`Failed to load versions: ${e}`);
		}
	}

	async function updateMaintenance(name: string) {
		if (!maintenanceWindow.trim()) {
			toast.error('Maintenance window start time is required (e.g. 06:00)');
			return;
		}
		updatingMaintenance = true;
		try {
			await kda().send(
				new UpdateApplicationMaintenanceConfigurationCommand({
					ApplicationName: name,
					ApplicationMaintenanceConfigurationUpdate: {
						ApplicationMaintenanceWindowStartTimeUpdate: maintenanceWindow.trim()
					}
				})
			);
			toast.success('Maintenance window updated');
			showMaintenance = false;
		} catch (e) {
			toast.error(`Failed to update maintenance window: ${e}`);
		} finally {
			updatingMaintenance = false;
		}
	}

	async function discoverSchema() {
		if (!schemaResourceARN.trim()) {
			toast.error('Resource ARN is required');
			return;
		}
		discoveringSchema = true;
		try {
			const resp = await kda().send(
				new DiscoverInputSchemaCommand({
					ResourceARN: schemaResourceARN.trim(),
					ServiceExecutionRole: '',
					InputStartingPositionConfiguration: { InputStartingPosition: 'NOW' }
				})
			);
			discoveredSchema = resp.InputSchema ?? null;
			toast.success('Schema discovered');
		} catch (e) {
			toast.error(`Failed to discover schema: ${e}`);
		} finally {
			discoveringSchema = false;
		}
	}

	function selectApp(name: string) {
		selectedApp = selectedApp === name ? null : name;
		if (selectedApp) {
			snapshots = [];
			versions = [];
			showVersions = false;
			showMaintenance = false;
			loadSnapshots(name);
		}
	}

	function statusColor(status: string) {
		switch (status) {
			case 'RUNNING':
				return 'text-green-600';
			case 'READY':
				return 'text-blue-600';
			case 'DELETING':
				return 'text-red-600';
			default:
				return 'text-gray-500';
		}
	}

	onRegionChange(loadApplications);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Layers class="h-6 w-6 text-blue-600" />
			<h1 class="text-2xl font-semibold">Kinesis Data Analytics v2 (Flink)</h1>
		</div>
		<div class="flex gap-2">
			<button
				onclick={() => (showSchemaDiscovery = !showSchemaDiscovery)}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-gray-50"
			>
				<Search class="h-4 w-4" />
				Discover Schema
			</button>
			<button
				onclick={() => loadApplications()}
				class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-gray-50"
				disabled={loading}
			>
				<RefreshCw class="h-4 w-4 {loading ? 'animate-spin' : ''}" />
				Refresh
			</button>
			<button
				onclick={() => (showCreateForm = !showCreateForm)}
				class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700"
			>
				<Plus class="h-4 w-4" />
				Create Application
			</button>
		</div>
	</div>

	<!-- Schema Discovery Panel -->
	{#if showSchemaDiscovery}
		<div class="rounded-lg border bg-gray-50 p-4 space-y-3">
			<h2 class="font-medium flex items-center gap-2"><Search class="h-4 w-4" /> Discover Input Schema</h2>
			<div class="flex gap-2">
				<input
					bind:value={schemaResourceARN}
					placeholder="Kinesis Stream ARN"
					class="flex-1 rounded border px-3 py-1.5 text-sm"
				/>
				<button
					onclick={discoverSchema}
					disabled={discoveringSchema}
					class="rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
				>
					{discoveringSchema ? 'Discovering...' : 'Discover'}
				</button>
			</div>
			{#if discoveredSchema}
				<pre class="rounded bg-white border p-3 text-xs overflow-auto">{JSON.stringify(discoveredSchema, null, 2)}</pre>
			{/if}
		</div>
	{/if}

	<!-- Create Application Form -->
	{#if showCreateForm}
		<div class="rounded-lg border bg-gray-50 p-4 space-y-3">
			<h2 class="font-medium">New Application</h2>
			<div class="grid grid-cols-3 gap-3">
				<div>
					<label class="block text-xs text-gray-600 mb-1">Application Name *</label>
					<input
						bind:value={newAppName}
						placeholder="my-flink-app"
						class="w-full rounded border px-3 py-1.5 text-sm"
					/>
				</div>
				<div>
					<label class="block text-xs text-gray-600 mb-1">Runtime Environment *</label>
					<select bind:value={newAppRuntime} class="w-full rounded border px-3 py-1.5 text-sm">
						{#each runtimes as rt}
							<option value={rt}>{rt}</option>
						{/each}
					</select>
				</div>
				<div>
					<label class="block text-xs text-gray-600 mb-1">Service Execution Role ARN</label>
					<input
						bind:value={newAppRole}
						placeholder="arn:aws:iam::..."
						class="w-full rounded border px-3 py-1.5 text-sm"
					/>
				</div>
			</div>
			<div class="flex gap-2">
				<button
					onclick={createApplication}
					disabled={creating}
					class="rounded bg-blue-600 px-4 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create'}
				</button>
				<button
					onclick={() => (showCreateForm = false)}
					class="rounded border px-4 py-1.5 text-sm hover:bg-gray-100"
				>
					Cancel
				</button>
			</div>
		</div>
	{/if}

	<!-- Applications Table -->
	<div class="rounded-lg border overflow-hidden">
		<table class="w-full text-sm">
			<thead class="bg-gray-50 border-b">
				<tr>
					<th class="px-4 py-2 text-left font-medium text-gray-700">Name</th>
					<th class="px-4 py-2 text-left font-medium text-gray-700">Status</th>
					<th class="px-4 py-2 text-left font-medium text-gray-700">Runtime</th>
					<th class="px-4 py-2 text-left font-medium text-gray-700">Version</th>
					<th class="px-4 py-2 text-left font-medium text-gray-700">Actions</th>
				</tr>
			</thead>
			<tbody class="divide-y">
				{#if loading}
					<tr>
						<td colspan="5" class="px-4 py-8 text-center text-gray-500">Loading...</td>
					</tr>
				{:else if applications.length === 0}
					<tr>
						<td colspan="5" class="px-4 py-8 text-center text-gray-500">No applications found</td>
					</tr>
				{:else}
					{#each applications as app}
						<tr
							class="hover:bg-gray-50 cursor-pointer {selectedApp === app.ApplicationName
								? 'bg-blue-50'
								: ''}"
						>
							<td class="px-4 py-2">
								<button
									onclick={() => selectApp(app.ApplicationName ?? '')}
									class="flex items-center gap-1 font-medium text-blue-700 hover:underline"
								>
									<ChevronRight
										class="h-3 w-3 transition-transform {selectedApp === app.ApplicationName
											? 'rotate-90'
											: ''}"
									/>
									{app.ApplicationName}
								</button>
							</td>
							<td class="px-4 py-2">
								<span class="font-medium {statusColor(app.ApplicationStatus ?? '')}">
									{app.ApplicationStatus ?? '-'}
								</span>
							</td>
							<td class="px-4 py-2 text-gray-600">{app.RuntimeEnvironment ?? '-'}</td>
							<td class="px-4 py-2 text-gray-600">v{app.ApplicationVersionId ?? 1}</td>
							<td class="px-4 py-2">
								<div class="flex items-center gap-1">
									{#if app.ApplicationStatus === 'READY'}
										<button
											onclick={() => startApplication(app.ApplicationName ?? '')}
											title="Start"
											class="rounded p-1 hover:bg-green-100 text-green-700"
										>
											<Play class="h-4 w-4" />
										</button>
									{:else if app.ApplicationStatus === 'RUNNING'}
										<button
											onclick={() => stopApplication(app.ApplicationName ?? '')}
											title="Stop"
											class="rounded p-1 hover:bg-yellow-100 text-yellow-700"
										>
											<Square class="h-4 w-4" />
										</button>
									{/if}
									<button
										onclick={() => rollbackApp(app.ApplicationName ?? '')}
										title="Rollback"
										class="rounded p-1 hover:bg-purple-100 text-purple-700"
									>
										<RotateCcw class="h-4 w-4" />
									</button>
									<button
										onclick={() => deleteApplication(app.ApplicationName ?? '')}
										title="Delete"
										class="rounded p-1 hover:bg-red-100 text-red-700"
									>
										<Trash2 class="h-4 w-4" />
									</button>
								</div>
							</td>
						</tr>

						<!-- Expanded detail panel -->
						{#if selectedApp === app.ApplicationName}
							<tr>
								<td colspan="5" class="px-4 py-4 bg-blue-50 border-t">
									<div class="space-y-4">
										<!-- Snapshot section -->
										<div>
											<h3 class="text-sm font-semibold flex items-center gap-2 mb-2">
												<Camera class="h-4 w-4" /> Snapshots
											</h3>
											<div class="flex gap-2 mb-2">
												<input
													bind:value={newSnapshotName}
													placeholder="snapshot-name"
													class="rounded border px-2 py-1 text-xs"
												/>
												<button
													onclick={() => createSnapshot(app.ApplicationName ?? '')}
													class="rounded bg-blue-600 px-3 py-1 text-xs text-white hover:bg-blue-700"
												>
													Create Snapshot
												</button>
											</div>
											{#if loadingSnapshots}
												<p class="text-xs text-gray-500">Loading snapshots...</p>
											{:else if snapshots.length === 0}
												<p class="text-xs text-gray-500">No snapshots</p>
											{:else}
												<div class="space-y-1">
													{#each snapshots as snap}
														<div class="flex items-center justify-between rounded bg-white border px-3 py-1.5 text-xs">
															<span class="font-medium">{snap.SnapshotName}</span>
															<span class="text-gray-500">{snap.SnapshotStatus}</span>
															<button
																onclick={() =>
																	deleteSnapshot(
																		app.ApplicationName ?? '',
																		snap.SnapshotName ?? '',
																		snap.SnapshotCreationTimestamp
																	)}
																class="text-red-600 hover:text-red-800"
															>
																<Trash2 class="h-3 w-3" />
															</button>
														</div>
													{/each}
												</div>
											{/if}
										</div>

										<!-- Version history -->
										<div>
											<button
												onclick={() => loadVersions(app.ApplicationName ?? '')}
												class="text-sm font-semibold flex items-center gap-2 hover:underline"
											>
												<Layers class="h-4 w-4" /> Version History
											</button>
											{#if showVersions && versions.length > 0}
												<div class="mt-2 space-y-1">
													{#each versions as ver}
														<div class="rounded bg-white border px-3 py-1.5 text-xs flex gap-4">
															<span class="font-medium">v{ver.ApplicationVersionId}</span>
															<span class="text-gray-600">{ver.ApplicationStatus}</span>
														</div>
													{/each}
												</div>
											{:else if showVersions}
												<p class="text-xs text-gray-500 mt-1">No versions found</p>
											{/if}
										</div>

										<!-- Maintenance window -->
										<div>
											<button
												onclick={() => (showMaintenance = !showMaintenance)}
												class="text-sm font-semibold flex items-center gap-2 hover:underline"
											>
												<Settings class="h-4 w-4" /> Maintenance Window
											</button>
											{#if showMaintenance}
												<div class="flex gap-2 mt-2">
													<input
														bind:value={maintenanceWindow}
														placeholder="06:00 (UTC)"
														class="rounded border px-2 py-1 text-xs"
													/>
													<button
														onclick={() => updateMaintenance(app.ApplicationName ?? '')}
														disabled={updatingMaintenance}
														class="rounded bg-blue-600 px-3 py-1 text-xs text-white hover:bg-blue-700 disabled:opacity-50"
													>
														{updatingMaintenance ? 'Saving...' : 'Update'}
													</button>
												</div>
											{/if}
										</div>
									</div>
								</td>
							</tr>
						{/if}
					{/each}
				{/if}
			</tbody>
		</table>
	</div>
</div>
