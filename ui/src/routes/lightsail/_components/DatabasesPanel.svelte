<script lang="ts">
	// Relational databases -- family N (9 ops: core lifecycle), with family
	// O's 7 detail-only ops (events, log streams/events, parameters, master
	// password reveal, metrics) folded into the detail view.
	//
	// RelationalDatabase.state is a bare *string on the wire -- NO typed
	// RelationalDatabaseState enum exists anywhere in this SDK module
	// (services/lightsail/models.go's doc comment) -- this panel renders
	// whatever string comes back rather than hard-coding a state list the
	// backend might not match.
	import {
		GetRelationalDatabasesCommand,
		CreateRelationalDatabaseCommand,
		DeleteRelationalDatabaseCommand,
		StartRelationalDatabaseCommand,
		StopRelationalDatabaseCommand,
		RebootRelationalDatabaseCommand,
		GetRelationalDatabaseEventsCommand,
		GetRelationalDatabaseLogStreamsCommand,
		GetRelationalDatabaseLogEventsCommand,
		GetRelationalDatabaseMasterUserPasswordCommand,
		GetRelationalDatabaseParametersCommand,
		UpdateRelationalDatabaseParametersCommand,
		GetRelationalDatabaseMetricDataCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type RelationalDatabase,
		type RelationalDatabaseEvent,
		type RelationalDatabaseParameter,
		type LogEvent,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, tagsToRecord } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let databases = $state<RelationalDatabase[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchDatabases(reset: boolean): Promise<void> {
		const resp = await client().send(new GetRelationalDatabasesCommand({ pageToken: reset ? undefined : nextToken }));
		databases = reset ? (resp.relationalDatabases ?? []) : [...databases, ...(resp.relationalDatabases ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchDatabases(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchDatabases(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		databases.filter((d) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (d.name ?? '').toLowerCase().includes(q) || (d.state ?? '').toLowerCase().includes(q);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createBlueprintId = $state('');
	let createBundleId = $state('');
	let createMasterDbName = $state('');
	let createMasterUsername = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createBlueprintId = '';
		createBundleId = '';
		createMasterDbName = '';
		createMasterUsername = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateRelationalDatabaseCommand({
					relationalDatabaseName: createName,
					relationalDatabaseBlueprintId: createBlueprintId,
					relationalDatabaseBundleId: createBundleId,
					masterDatabaseName: createMasterDbName,
					masterUsername: createMasterUsername
				})
			);
			toast.success('Database creation started');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteDb(d: RelationalDatabase): Promise<void> {
		if (!d.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete database',
			message: `Delete database ${d.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteRelationalDatabaseCommand({ relationalDatabaseName: d.name, skipFinalSnapshot: true }));
			toast.success('Database deletion started');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Lifecycle -------------------------------

	async function doStart(d: RelationalDatabase): Promise<void> {
		if (!d.name) return;
		try {
			await client().send(new StartRelationalDatabaseCommand({ relationalDatabaseName: d.name }));
			toast.success('Start requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function doStop(d: RelationalDatabase): Promise<void> {
		if (!d.name) return;
		try {
			await client().send(new StopRelationalDatabaseCommand({ relationalDatabaseName: d.name }));
			toast.success('Stop requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function doReboot(d: RelationalDatabase): Promise<void> {
		if (!d.name) return;
		try {
			await client().send(new RebootRelationalDatabaseCommand({ relationalDatabaseName: d.name }));
			toast.success('Reboot requested');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<RelationalDatabase | null>(null);
	let events = $state<RelationalDatabaseEvent[]>([]);
	let logStreams = $state<string[]>([]);
	let selectedLogStream = $state('');
	let logEvents = $state<LogEvent[]>([]);
	let parameters = $state<RelationalDatabaseParameter[]>([]);
	let masterPassword = $state<string | null>(null);
	let metricsChecked = $state(false);
	let metricsEmpty = $state(true);

	async function openDetail(d: RelationalDatabase): Promise<void> {
		viewed = d;
		events = [];
		logStreams = [];
		selectedLogStream = '';
		logEvents = [];
		parameters = [];
		masterPassword = null;
		metricsChecked = false;
		metricsEmpty = true;
		detailModal?.open();
		if (!d.name) return;
		try {
			const [ev, streams, params] = await Promise.all([
				client().send(new GetRelationalDatabaseEventsCommand({ relationalDatabaseName: d.name })),
				client().send(new GetRelationalDatabaseLogStreamsCommand({ relationalDatabaseName: d.name })),
				client().send(new GetRelationalDatabaseParametersCommand({ relationalDatabaseName: d.name }))
			]);
			events = ev.relationalDatabaseEvents ?? [];
			logStreams = streams.logStreams ?? [];
			parameters = params.parameters ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function loadLogEvents(): Promise<void> {
		if (!viewed?.name || !selectedLogStream) return;
		try {
			const resp = await client().send(
				new GetRelationalDatabaseLogEventsCommand({ relationalDatabaseName: viewed.name, logStreamName: selectedLogStream })
			);
			logEvents = resp.resourceLogEvents ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function revealMasterPassword(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(
				new GetRelationalDatabaseMasterUserPasswordCommand({ relationalDatabaseName: viewed.name })
			);
			masterPassword = resp.masterUserPassword ?? '';
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function saveParameter(p: RelationalDatabaseParameter, value: string): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(
				new UpdateRelationalDatabaseParametersCommand({
					relationalDatabaseName: viewed.name,
					parameters: [{ parameterName: p.parameterName, parameterValue: value, applyMethod: p.applyMethod }]
				})
			);
			toast.success('Parameter updated');
			parameters = parameters.map((x) => (x.parameterName === p.parameterName ? { ...x, parameterValue: value } : x));
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// GetRelationalDatabaseMetricData always returns an honest empty
	// MetricData series in this emulator -- never a fabricated chart.
	async function checkMetrics(): Promise<void> {
		if (!viewed?.name) return;
		try {
			const resp = await client().send(
				new GetRelationalDatabaseMetricDataCommand({
					relationalDatabaseName: viewed.name,
					metricName: 'CPUUtilization',
					period: 300,
					startTime: new Date(Date.now() - 3600_000),
					endTime: new Date(),
					unit: 'Percent',
					statistics: ['Average']
				})
			);
			metricsEmpty = (resp.metricData ?? []).length === 0;
			metricsChecked = true;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new TagResourceCommand({ resourceName: viewed.name, tags: [{ key, value }] }));
		viewed = { ...viewed, tags: [...(viewed.tags ?? []).filter((t) => t.key !== key), { key, value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new UntagResourceCommand({ resourceName: viewed.name, tagKeys: [key] }));
		viewed = { ...viewed, tags: (viewed.tags ?? []).filter((t) => t.key !== key) };
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create database
	</button>
</div>

{#snippet createdCell(d: RelationalDatabase)}
	{formatDate(d.createdAt)}
{/snippet}
{#snippet rowActions(d: RelationalDatabase)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => doStart(d)} class="text-green-600 hover:underline text-sm">Start</button>
		<button onclick={() => doStop(d)} class="text-amber-600 hover:underline text-sm">Stop</button>
		<button onclick={() => doReboot(d)} class="text-slate-600 hover:underline text-sm">Reboot</button>
		<button onclick={() => openDetail(d)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteDb(d)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(d) => d.name ?? ''}
	columns={defineColumns<RelationalDatabase>([
		{ key: 'name', label: 'Name' },
		{ key: 'engine', label: 'Engine' },
		{ key: 'relationalDatabaseBundleId', label: 'Bundle' },
		{ key: 'state', label: 'State' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No databases found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create database">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-db-name">
				Name
				<input id="ls-db-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-db-blueprint">
				Blueprint ID -- see the Reference Data tab
				<input id="ls-db-blueprint" bind:value={createBlueprintId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-db-bundle">
				Bundle ID -- see the Reference Data tab
				<input id="ls-db-bundle" bind:value={createBundleId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-db-master-name">
				Master database name
				<input id="ls-db-master-name" bind:value={createMasterDbName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-db-master-user">
				Master username
				<input id="ls-db-master-user" bind:value={createMasterUsername} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createBlueprintId || !createBundleId} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Database {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.state ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Endpoint</dt><dd>{viewed.masterEndpoint?.address ?? '—'}:{viewed.masterEndpoint?.port ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Master username</dt><dd>{viewed.masterUsername ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Backup retention</dt><dd>{viewed.backupRetentionEnabled ? 'Enabled' : 'Disabled'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Master password</p>
					<button onclick={revealMasterPassword} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Reveal current password</button>
					{#if masterPassword !== null}<p class="text-xs break-all">{masterPassword || '(none)'}</p>{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Recent events</p>
					<ul class="text-sm space-y-1 max-h-32 overflow-y-auto">
						{#each events as ev, i (i)}
							<li>{formatDate(ev.createdAt)} -- {ev.message}</li>
						{:else}
							<li class="text-slate-500">No events</li>
						{/each}
					</ul>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Log streams</p>
					<div class="flex items-center gap-2">
						<select bind:value={selectedLogStream} aria-label="Log stream" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 flex-1">
							<option value="">Select a log stream</option>
							{#each logStreams as s (s)}
								<option value={s}>{s}</option>
							{/each}
						</select>
						<button onclick={loadLogEvents} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Load</button>
					</div>
					<ul class="text-sm space-y-1 max-h-32 overflow-y-auto">
						{#each logEvents as ev, i (i)}
							<li>{formatDate(ev.createdAt)} -- {ev.message}</li>
						{:else}
							<li class="text-slate-500">No log events loaded</li>
						{/each}
					</ul>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Parameters</p>
					<ul class="text-sm space-y-1 max-h-40 overflow-y-auto">
						{#each parameters as p (p.parameterName)}
							<li class="flex items-center justify-between gap-2">
								<span class="truncate">{p.parameterName}</span>
								{#if p.isModifiable}
									<input
										value={p.parameterValue}
										onchange={(e) => saveParameter(p, (e.target as HTMLInputElement).value)}
										aria-label="Value for {p.parameterName}"
										class="w-32 px-2 py-1 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
									/>
								{:else}
									<span class="text-slate-500">{p.parameterValue}</span>
								{/if}
							</li>
						{:else}
							<li class="text-slate-500">No parameters loaded</li>
						{/each}
					</ul>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Metrics</p>
					<button onclick={checkMetrics} class="px-2 py-1 text-xs rounded-lg border border-slate-300 dark:border-slate-600">Check CPU utilization</button>
					{#if metricsChecked && metricsEmpty}
						<p class="text-xs text-slate-500">
							This emulator does not produce real metric datapoints -- GetRelationalDatabaseMetricData returned an honest empty series.
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={tagsToRecord(viewed.tags)} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
