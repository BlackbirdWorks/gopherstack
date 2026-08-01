<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getKinesisAnalyticsClient } from '$lib/aws-client';
	import {
		ListApplicationsCommand,
		CreateApplicationCommand,
		DeleteApplicationCommand,
		DescribeApplicationCommand,
		StartApplicationCommand,
		StopApplicationCommand,
		AddApplicationInputCommand,
		AddApplicationOutputCommand,
		AddApplicationCloudWatchLoggingOptionCommand,
		DeleteApplicationOutputCommand,
		DeleteApplicationCloudWatchLoggingOptionCommand,
		DiscoverInputSchemaCommand,
		type ApplicationSummary
	} from '@aws-sdk/client-kinesis-analytics';
	import { toast } from 'svelte-sonner';
	import {
		RefreshCw,
		Trash2,
		Plus,
		Play,
		Square,
		ChevronRight,
		Layers,
		Search,
		Activity,
		Radio
	} from 'lucide-svelte';

	const kda = regionalClient(getKinesisAnalyticsClient);

	// Applications list
	let loading = $state(false);
	let applications = $state<ApplicationSummary[]>([]);
	let selectedApp = $state<string | null>(null);

	// Create application form
	let showCreateForm = $state(false);
	let newAppName = $state('');
	let newAppCode = $state('');
	let newAppDescription = $state('');
	let creating = $state(false);

	// Application detail
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let appDetail = $state<any | null>(null);
	let loadingDetail = $state(false);

	// Add input form
	let showAddInput = $state(false);
	let inputNamePrefix = $state('');
	let inputStreamARN = $state('');
	let inputStreamRole = $state('');

	// Add output form
	let showAddOutput = $state(false);
	let outputName = $state('');
	let outputStreamARN = $state('');
	let outputStreamRole = $state('');

	// CloudWatch logging
	let showAddCWL = $state(false);
	let cwlLogStreamARN = $state('');
	let cwlRoleARN = $state('');

	// Schema discovery
	let showSchemaDiscovery = $state(false);
	let schemaResourceARN = $state('');
	let schemaRoleARN = $state('');
	// eslint-disable-next-line @typescript-eslint/no-explicit-any
	let discoveredSchema = $state<any | null>(null);
	let discoveringSchema = $state(false);

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
					ApplicationCode: newAppCode.trim() || undefined,
					ApplicationDescription: newAppDescription.trim() || undefined
				})
			);
			toast.success(`Application "${newAppName}" created`);
			newAppName = '';
			newAppCode = '';
			newAppDescription = '';
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
			if (selectedApp === name) {
				selectedApp = null;
				appDetail = null;
			}
			await loadApplications();
		} catch (e) {
			toast.error(`Failed to delete application: ${e}`);
		}
	}

	async function startApplication(name: string) {
		try {
			await kda().send(
				new StartApplicationCommand({
					ApplicationName: name,
					InputConfigurations: []
				})
			);
			toast.success(`Application "${name}" started`);
			await loadApplications();
			if (selectedApp === name) await loadAppDetail(name);
		} catch (e) {
			toast.error(`Failed to start application: ${e}`);
		}
	}

	async function stopApplication(name: string) {
		try {
			await kda().send(new StopApplicationCommand({ ApplicationName: name }));
			toast.success(`Application "${name}" stopped`);
			await loadApplications();
			if (selectedApp === name) await loadAppDetail(name);
		} catch (e) {
			toast.error(`Failed to stop application: ${e}`);
		}
	}

	async function loadAppDetail(name: string) {
		loadingDetail = true;
		try {
			const resp = await kda().send(new DescribeApplicationCommand({ ApplicationName: name }));
			appDetail = resp.ApplicationDetail ?? null;
		} catch (e) {
			toast.error(`Failed to load application detail: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function addInput() {
		if (!selectedApp) return;
		if (!inputNamePrefix.trim()) {
			toast.error('Name prefix is required');
			return;
		}
		try {
			const versionId = appDetail?.ApplicationVersionId ?? 1;
			await kda().send(
				new AddApplicationInputCommand({
					ApplicationName: selectedApp,
					CurrentApplicationVersionId: versionId,
					Input: {
						NamePrefix: inputNamePrefix.trim(),
						KinesisStreamsInput: inputStreamARN.trim()
							? {
									ResourceARN: inputStreamARN.trim(),
									RoleARN: inputStreamRole.trim()
								}
							: undefined,
						InputSchema: {
							RecordFormat: {
								RecordFormatType: 'JSON',
								MappingParameters: {
									JSONMappingParameters: { RecordRowPath: '$' }
								}
							},
							RecordColumns: [{ Name: 'COL_1', SqlType: 'VARCHAR(4)', Mapping: '$.col1' }]
						}
					}
				})
			);
			toast.success('Input added');
			inputNamePrefix = '';
			inputStreamARN = '';
			inputStreamRole = '';
			showAddInput = false;
			await loadAppDetail(selectedApp);
		} catch (e) {
			toast.error(`Failed to add input: ${e}`);
		}
	}

	async function addOutput() {
		if (!selectedApp) return;
		if (!outputName.trim()) {
			toast.error('Output name is required');
			return;
		}
		try {
			const versionId = appDetail?.ApplicationVersionId ?? 1;
			await kda().send(
				new AddApplicationOutputCommand({
					ApplicationName: selectedApp,
					CurrentApplicationVersionId: versionId,
					Output: {
						Name: outputName.trim(),
						KinesisStreamsOutput: outputStreamARN.trim()
							? {
									ResourceARN: outputStreamARN.trim(),
									RoleARN: outputStreamRole.trim()
								}
							: undefined,
						DestinationSchema: { RecordFormatType: 'JSON' }
					}
				})
			);
			toast.success('Output added');
			outputName = '';
			outputStreamARN = '';
			outputStreamRole = '';
			showAddOutput = false;
			await loadAppDetail(selectedApp);
		} catch (e) {
			toast.error(`Failed to add output: ${e}`);
		}
	}

	async function deleteOutput(outputId: string) {
		if (!selectedApp) return;
		try {
			const versionId = appDetail?.ApplicationVersionId ?? 1;
			await kda().send(
				new DeleteApplicationOutputCommand({
					ApplicationName: selectedApp,
					CurrentApplicationVersionId: versionId,
					OutputId: outputId
				})
			);
			toast.success('Output deleted');
			await loadAppDetail(selectedApp);
		} catch (e) {
			toast.error(`Failed to delete output: ${e}`);
		}
	}

	async function addCloudWatchLogging() {
		if (!selectedApp) return;
		if (!cwlLogStreamARN.trim()) {
			toast.error('Log stream ARN is required');
			return;
		}
		try {
			const versionId = appDetail?.ApplicationVersionId ?? 1;
			await kda().send(
				new AddApplicationCloudWatchLoggingOptionCommand({
					ApplicationName: selectedApp,
					CurrentApplicationVersionId: versionId,
					CloudWatchLoggingOption: {
						LogStreamARN: cwlLogStreamARN.trim(),
						RoleARN: cwlRoleARN.trim()
					}
				})
			);
			toast.success('CloudWatch logging added');
			cwlLogStreamARN = '';
			cwlRoleARN = '';
			showAddCWL = false;
			await loadAppDetail(selectedApp);
		} catch (e) {
			toast.error(`Failed to add CloudWatch logging: ${e}`);
		}
	}

	async function deleteCWLOption(cwlId: string) {
		if (!selectedApp) return;
		try {
			const versionId = appDetail?.ApplicationVersionId ?? 1;
			await kda().send(
				new DeleteApplicationCloudWatchLoggingOptionCommand({
					ApplicationName: selectedApp,
					CurrentApplicationVersionId: versionId,
					CloudWatchLoggingOptionId: cwlId
				})
			);
			toast.success('CloudWatch logging option removed');
			await loadAppDetail(selectedApp);
		} catch (e) {
			toast.error(`Failed to remove CloudWatch logging: ${e}`);
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
					RoleARN: schemaRoleARN.trim() || undefined,
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
		if (selectedApp === name) {
			selectedApp = null;
			appDetail = null;
		} else {
			selectedApp = name;
			appDetail = null;
			showAddInput = false;
			showAddOutput = false;
			showAddCWL = false;
			loadAppDetail(name);
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

	onRegionChange(() => {
		loadApplications();
	});
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Layers class="h-6 w-6 text-orange-600" />
			<h1 class="text-2xl font-semibold">Kinesis Data Analytics (SQL / v1)</h1>
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
				class="flex items-center gap-2 rounded-md bg-orange-600 px-3 py-2 text-sm text-white hover:bg-orange-700"
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
			<div class="grid grid-cols-2 gap-2">
				<input
					bind:value={schemaResourceARN}
					placeholder="Kinesis Stream ARN"
					class="rounded border px-3 py-1.5 text-sm"
				/>
				<input
					bind:value={schemaRoleARN}
					placeholder="Role ARN (optional)"
					class="rounded border px-3 py-1.5 text-sm"
				/>
			</div>
			<button
				onclick={discoverSchema}
				disabled={discoveringSchema}
				class="rounded bg-orange-600 px-3 py-1.5 text-sm text-white hover:bg-orange-700 disabled:opacity-50"
			>
				{discoveringSchema ? 'Discovering...' : 'Discover'}
			</button>
			{#if discoveredSchema}
				<pre class="rounded bg-white border p-3 text-xs overflow-auto">{JSON.stringify(discoveredSchema, null, 2)}</pre>
			{/if}
		</div>
	{/if}

	<!-- Create Application Form -->
	{#if showCreateForm}
		<div class="rounded-lg border bg-gray-50 p-4 space-y-3">
			<h2 class="font-medium">New SQL Application</h2>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label class="block text-xs text-gray-600 mb-1" for="new-app-name">Application Name *</label>
					<input id="new-app-name"
						bind:value={newAppName}
						placeholder="my-sql-app"
						class="w-full rounded border px-3 py-1.5 text-sm"
					/>
				</div>
				<div>
					<label class="block text-xs text-gray-600 mb-1" for="new-app-description">Description</label>
					<input id="new-app-description"
						bind:value={newAppDescription}
						placeholder="Optional description"
						class="w-full rounded border px-3 py-1.5 text-sm"
					/>
				</div>
			</div>
			<div>
				<label class="block text-xs text-gray-600 mb-1" for="new-app-code">Application SQL Code</label>
				<textarea id="new-app-code"
					bind:value={newAppCode}
					placeholder="CREATE OR REPLACE STREAM ..."
					rows={4}
					class="w-full rounded border px-3 py-1.5 text-sm font-mono"
				></textarea>
			</div>
			<div class="flex gap-2">
				<button
					onclick={createApplication}
					disabled={creating}
					class="rounded bg-orange-600 px-4 py-1.5 text-sm text-white hover:bg-orange-700 disabled:opacity-50"
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
					<th class="px-4 py-2 text-left font-medium text-gray-700">Actions</th>
				</tr>
			</thead>
			<tbody class="divide-y">
				{#if loading}
					<tr>
						<td colspan="3" class="px-4 py-8 text-center text-gray-500">Loading...</td>
					</tr>
				{:else if applications.length === 0}
					<tr>
						<td colspan="3" class="px-4 py-8 text-center text-gray-500">No applications found</td>
					</tr>
				{:else}
					{#each applications as app}
						<tr
							class="hover:bg-gray-50 cursor-pointer {selectedApp === app.ApplicationName
								? 'bg-orange-50'
								: ''}"
						>
							<td class="px-4 py-2">
								<button
									onclick={() => selectApp(app.ApplicationName ?? '')}
									class="flex items-center gap-1 font-medium text-orange-700 hover:underline"
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
								<td colspan="3" class="px-4 py-4 bg-orange-50 border-t">
									{#if loadingDetail}
										<p class="text-sm text-gray-500">Loading details...</p>
									{:else if appDetail}
										<div class="space-y-5">
											<!-- App Info -->
											<div class="grid grid-cols-3 gap-4 text-xs">
												<div>
													<span class="text-gray-500">ARN</span>
													<p class="font-mono break-all">{appDetail.ApplicationARN ?? '-'}</p>
												</div>
												<div>
													<span class="text-gray-500">Version</span>
													<p class="font-medium">v{appDetail.ApplicationVersionId ?? 1}</p>
												</div>
												<div>
													<span class="text-gray-500">Description</span>
													<p>{appDetail.ApplicationDescription || '—'}</p>
												</div>
											</div>

											<!-- SQL Code -->
											{#if appDetail.ApplicationCode}
												<div>
													<h3 class="text-sm font-semibold mb-1">SQL Code</h3>
													<pre class="rounded bg-white border p-3 text-xs overflow-auto font-mono">{appDetail.ApplicationCode}</pre>
												</div>
											{/if}

											<!-- Inputs -->
											<div>
												<div class="flex items-center justify-between mb-2">
													<h3 class="text-sm font-semibold flex items-center gap-2">
														<Radio class="h-4 w-4" /> Inputs
													</h3>
													<button
														onclick={() => (showAddInput = !showAddInput)}
														class="text-xs rounded bg-orange-600 text-white px-2 py-1 hover:bg-orange-700"
													>
														<Plus class="h-3 w-3 inline" /> Add Input
													</button>
												</div>
												{#if showAddInput}
													<div class="rounded bg-white border p-3 mb-2 space-y-2">
														<div class="grid grid-cols-3 gap-2">
															<input
																bind:value={inputNamePrefix}
																placeholder="Name prefix *"
																class="rounded border px-2 py-1 text-xs"
															/>
															<input
																bind:value={inputStreamARN}
																placeholder="Kinesis Stream ARN"
																class="rounded border px-2 py-1 text-xs"
															/>
															<input
																bind:value={inputStreamRole}
																placeholder="Role ARN"
																class="rounded border px-2 py-1 text-xs"
															/>
														</div>
														<div class="flex gap-2">
															<button
																onclick={addInput}
																class="text-xs rounded bg-orange-600 text-white px-3 py-1 hover:bg-orange-700"
															>
																Add
															</button>
															<button
																onclick={() => (showAddInput = false)}
																class="text-xs rounded border px-3 py-1 hover:bg-gray-100"
															>
																Cancel
															</button>
														</div>
													</div>
												{/if}
												{#if (appDetail.InputDescriptions ?? []).length === 0}
													<p class="text-xs text-gray-500">No inputs configured</p>
												{:else}
													<div class="space-y-1">
														{#each appDetail.InputDescriptions as inp}
															<div class="rounded bg-white border px-3 py-2 text-xs">
																<span class="font-medium">{inp.NamePrefix ?? inp.InputId}</span>
																{#if inp.KinesisStreamsInputDescription}
																	<span class="ml-2 text-gray-500">Kinesis: {inp.KinesisStreamsInputDescription.ResourceARN}</span>
																{/if}
																{#if inp.KinesisFirehoseInputDescription}
																	<span class="ml-2 text-gray-500">Firehose: {inp.KinesisFirehoseInputDescription.ResourceARN}</span>
																{/if}
															</div>
														{/each}
													</div>
												{/if}
											</div>

											<!-- Outputs -->
											<div>
												<div class="flex items-center justify-between mb-2">
													<h3 class="text-sm font-semibold flex items-center gap-2">
														<Activity class="h-4 w-4" /> Outputs
													</h3>
													<button
														onclick={() => (showAddOutput = !showAddOutput)}
														class="text-xs rounded bg-orange-600 text-white px-2 py-1 hover:bg-orange-700"
													>
														<Plus class="h-3 w-3 inline" /> Add Output
													</button>
												</div>
												{#if showAddOutput}
													<div class="rounded bg-white border p-3 mb-2 space-y-2">
														<div class="grid grid-cols-3 gap-2">
															<input
																bind:value={outputName}
																placeholder="Output name *"
																class="rounded border px-2 py-1 text-xs"
															/>
															<input
																bind:value={outputStreamARN}
																placeholder="Kinesis Stream ARN"
																class="rounded border px-2 py-1 text-xs"
															/>
															<input
																bind:value={outputStreamRole}
																placeholder="Role ARN"
																class="rounded border px-2 py-1 text-xs"
															/>
														</div>
														<div class="flex gap-2">
															<button
																onclick={addOutput}
																class="text-xs rounded bg-orange-600 text-white px-3 py-1 hover:bg-orange-700"
															>
																Add
															</button>
															<button
																onclick={() => (showAddOutput = false)}
																class="text-xs rounded border px-3 py-1 hover:bg-gray-100"
															>
																Cancel
															</button>
														</div>
													</div>
												{/if}
												{#if (appDetail.OutputDescriptions ?? []).length === 0}
													<p class="text-xs text-gray-500">No outputs configured</p>
												{:else}
													<div class="space-y-1">
														{#each appDetail.OutputDescriptions as out}
															<div class="flex items-center justify-between rounded bg-white border px-3 py-2 text-xs">
																<div>
																	<span class="font-medium">{out.Name ?? out.OutputId}</span>
																	{#if out.KinesisStreamsOutputDescription}
																		<span class="ml-2 text-gray-500">Kinesis: {out.KinesisStreamsOutputDescription.ResourceARN}</span>
																	{/if}
																	{#if out.KinesisFirehoseOutputDescription}
																		<span class="ml-2 text-gray-500">Firehose: {out.KinesisFirehoseOutputDescription.ResourceARN}</span>
																	{/if}
																	{#if out.LambdaOutputDescription}
																		<span class="ml-2 text-gray-500">Lambda: {out.LambdaOutputDescription.ResourceARN}</span>
																	{/if}
																</div>
																<button
																	onclick={() => deleteOutput(out.OutputId ?? '')}
																	class="text-red-600 hover:text-red-800"
																>
																	<Trash2 class="h-3 w-3" />
																</button>
															</div>
														{/each}
													</div>
												{/if}
											</div>

											<!-- CloudWatch Logging -->
											<div>
												<div class="flex items-center justify-between mb-2">
													<h3 class="text-sm font-semibold">CloudWatch Logging</h3>
													<button
														onclick={() => (showAddCWL = !showAddCWL)}
														class="text-xs rounded bg-orange-600 text-white px-2 py-1 hover:bg-orange-700"
													>
														<Plus class="h-3 w-3 inline" /> Add
													</button>
												</div>
												{#if showAddCWL}
													<div class="rounded bg-white border p-3 mb-2 space-y-2">
														<div class="grid grid-cols-2 gap-2">
															<input
																bind:value={cwlLogStreamARN}
																placeholder="Log Stream ARN *"
																class="rounded border px-2 py-1 text-xs"
															/>
															<input
																bind:value={cwlRoleARN}
																placeholder="Role ARN"
																class="rounded border px-2 py-1 text-xs"
															/>
														</div>
														<div class="flex gap-2">
															<button
																onclick={addCloudWatchLogging}
																class="text-xs rounded bg-orange-600 text-white px-3 py-1 hover:bg-orange-700"
															>
																Add
															</button>
															<button
																onclick={() => (showAddCWL = false)}
																class="text-xs rounded border px-3 py-1 hover:bg-gray-100"
															>
																Cancel
															</button>
														</div>
													</div>
												{/if}
												{#if (appDetail.CloudWatchLoggingOptionDescriptions ?? []).length === 0}
													<p class="text-xs text-gray-500">No CloudWatch logging configured</p>
												{:else}
													<div class="space-y-1">
														{#each appDetail.CloudWatchLoggingOptionDescriptions as cwl}
															<div class="flex items-center justify-between rounded bg-white border px-3 py-2 text-xs">
																<span class="font-mono break-all">{cwl.LogStreamARN}</span>
																<button
																	onclick={() => deleteCWLOption(cwl.CloudWatchLoggingOptionId ?? '')}
																	class="text-red-600 hover:text-red-800 ml-2 flex-shrink-0"
																>
																	<Trash2 class="h-3 w-3" />
																</button>
															</div>
														{/each}
													</div>
												{/if}
											</div>
										</div>
									{/if}
								</td>
							</tr>
						{/if}
					{/each}
				{/if}
			</tbody>
		</table>
	</div>
</div>
