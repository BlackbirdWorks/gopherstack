<script lang="ts">
	import { onMount } from 'svelte';
	import { getTimestreamQueryClient } from '$lib/aws-client';
	import {
		QueryCommand,
		CancelQueryCommand,
		PrepareQueryCommand,
		ListScheduledQueriesCommand,
		CreateScheduledQueryCommand,
		DeleteScheduledQueryCommand,
		ExecuteScheduledQueryCommand,
		UpdateScheduledQueryCommand,
		DescribeAccountSettingsCommand,
		UpdateAccountSettingsCommand,
		type ColumnInfo,
		type Row,
		type ScheduledQuery,
		type QueryStatus
	} from '@aws-sdk/client-timestream-query';
	import { toast } from 'svelte-sonner';
	import {
		Play,
		StopCircle,
		RefreshCw,
		Plus,
		Trash2,
		Clock,
		Settings,
		ChevronRight,
		ChevronLeft,
		Eye,
		Activity
	} from 'lucide-svelte';

	const tsQuery = getTimestreamQueryClient();

	// SQL editor state
	let sqlText = $state('SELECT * FROM "mydb"."mytable" ORDER BY time DESC LIMIT 100');
	let maxRows = $state(100);
	let queryRunning = $state(false);
	let currentQueryId = $state<string | null>(null);

	// Query results state
	let resultColumns = $state<ColumnInfo[]>([]);
	let resultRows = $state<Row[]>([]);
	let resultStatus = $state<QueryStatus | null>(null);
	let nextToken = $state<string | null>(null);
	let prevToken = $state<string | null>(null);

	// Scheduled queries state
	let scheduledQueries = $state<ScheduledQuery[]>([]);
	let loadingQueries = $state(false);
	let showCreateForm = $state(false);

	// Account settings state
	let accountPricingModel = $state('COMPUTE_UNITS');
	let accountMaxTCU = $state<number | null>(null);
	let accountLastUpdated = $state<number | null>(null);
	let loadingSettings = $state(false);

	// Create scheduled query form
	let newSqName = $state('');
	let newSqQueryString = $state('');
	let newSqSchedule = $state('rate(1 hour)');
	let newSqRoleArn = $state('');
	let newSqTopicArn = $state('');
	let newSqS3Bucket = $state('');
	let newSqDatabase = $state('');
	let newSqTable = $state('');

	let activeTab = $state<'query' | 'scheduled' | 'settings'>('query');
	let errorMsg = $state<string | null>(null);

	// ---------------------------------------------------------------------------
	// SQL Query execution
	// ---------------------------------------------------------------------------

	async function runQuery() {
		if (!sqlText.trim()) return;
		queryRunning = true;
		errorMsg = null;
		resultRows = [];
		resultColumns = [];
		resultStatus = null;
		nextToken = null;
		prevToken = null;
		try {
			const resp = await tsQuery.send(
				new QueryCommand({
					QueryString: sqlText,
					MaxRows: maxRows,
					QueryInsights: { Mode: 'ENABLED_WITH_RATE_CONTROL' }
				})
			);
			currentQueryId = resp.QueryId ?? null;
			resultColumns = resp.ColumnInfo ?? [];
			resultRows = resp.Rows ?? [];
			resultStatus = resp.QueryStatus ?? null;
			nextToken = resp.NextToken ?? null;
			toast.success(`Query completed: ${resultRows.length} rows`);
		} catch (e) {
			errorMsg = String(e);
			toast.error('Query failed: ' + String(e));
		} finally {
			queryRunning = false;
		}
	}

	async function cancelQuery() {
		if (!currentQueryId) return;
		try {
			await tsQuery.send(new CancelQueryCommand({ QueryId: currentQueryId }));
			queryRunning = false;
			currentQueryId = null;
			toast.success('Query cancelled');
		} catch (e) {
			toast.error('Cancel failed: ' + String(e));
		}
	}

	async function loadNextPage() {
		if (!nextToken) return;
		queryRunning = true;
		try {
			const resp = await tsQuery.send(
				new QueryCommand({ QueryString: sqlText, NextToken: nextToken, MaxRows: maxRows })
			);
			prevToken = null; // simplified: no full back-stack
			resultRows = resp.Rows ?? [];
			resultColumns = resp.ColumnInfo ?? resultColumns;
			resultStatus = resp.QueryStatus ?? null;
			nextToken = resp.NextToken ?? null;
		} catch (e) {
			toast.error('Pagination failed: ' + String(e));
		} finally {
			queryRunning = false;
		}
	}

	async function prepareQuery() {
		if (!sqlText.trim()) return;
		try {
			const resp = await tsQuery.send(
				new PrepareQueryCommand({ QueryString: sqlText })
			);
			const cols = resp.Columns ?? [];
			const params = resp.Parameters ?? [];
			toast.success(
				`Prepared: ${cols.length} columns, ${params.length} parameters`
			);
		} catch (e) {
			toast.error('Prepare failed: ' + String(e));
		}
	}

	// ---------------------------------------------------------------------------
	// Scheduled queries
	// ---------------------------------------------------------------------------

	async function loadScheduledQueries() {
		loadingQueries = true;
		try {
			const resp = await tsQuery.send(new ListScheduledQueriesCommand({}));
			scheduledQueries = resp.ScheduledQueries ?? [];
		} catch (e) {
			toast.error('Failed to load scheduled queries: ' + String(e));
		} finally {
			loadingQueries = false;
		}
	}

	async function createScheduledQuery() {
		if (!newSqName || !newSqQueryString || !newSqSchedule) {
			toast.error('Name, query, and schedule are required');
			return;
		}
		try {
			await tsQuery.send(
				new CreateScheduledQueryCommand({
					Name: newSqName,
					QueryString: newSqQueryString,
					ScheduledQueryExecutionRoleArn: newSqRoleArn,
					ScheduleConfiguration: { ScheduleExpression: newSqSchedule },
					NotificationConfiguration: newSqTopicArn
						? { SnsConfiguration: { TopicArn: newSqTopicArn } }
						: undefined,
					ErrorReportConfiguration: newSqS3Bucket
						? { S3Configuration: { BucketName: newSqS3Bucket } }
						: undefined,
					TargetConfiguration:
						newSqDatabase && newSqTable
							? {
									TimestreamConfiguration: {
										DatabaseName: newSqDatabase,
										TableName: newSqTable,
										TimeColumn: 'time',
										DimensionMappings: []
									}
								}
							: undefined
				})
			);
			toast.success('Scheduled query created');
			showCreateForm = false;
			resetCreateForm();
			await loadScheduledQueries();
		} catch (e) {
			toast.error('Create failed: ' + String(e));
		}
	}

	async function deleteScheduledQuery(arn: string, name: string) {
		if (!confirm(`Delete scheduled query "${name}"?`)) return;
		try {
			await tsQuery.send(new DeleteScheduledQueryCommand({ ScheduledQueryArn: arn }));
			toast.success('Deleted');
			await loadScheduledQueries();
		} catch (e) {
			toast.error('Delete failed: ' + String(e));
		}
	}

	async function toggleScheduledQuery(sq: ScheduledQuery) {
		const newState = sq.State === 'ENABLED' ? 'DISABLED' : 'ENABLED';
		try {
			await tsQuery.send(
				new UpdateScheduledQueryCommand({
					ScheduledQueryArn: sq.Arn,
					State: newState as 'ENABLED' | 'DISABLED'
				})
			);
			toast.success(`Query ${newState.toLowerCase()}`);
			await loadScheduledQueries();
		} catch (e) {
			toast.error('Update failed: ' + String(e));
		}
	}

	async function runScheduledQueryNow(sq: ScheduledQuery) {
		try {
			await tsQuery.send(
				new ExecuteScheduledQueryCommand({
					ScheduledQueryArn: sq.Arn,
					InvocationTime: new Date()
				})
			);
			toast.success('Scheduled query triggered');
			await loadScheduledQueries();
		} catch (e) {
			toast.error('Execute failed: ' + String(e));
		}
	}

	function resetCreateForm() {
		newSqName = '';
		newSqQueryString = '';
		newSqSchedule = 'rate(1 hour)';
		newSqRoleArn = '';
		newSqTopicArn = '';
		newSqS3Bucket = '';
		newSqDatabase = '';
		newSqTable = '';
	}

	// ---------------------------------------------------------------------------
	// Account settings
	// ---------------------------------------------------------------------------

	async function loadAccountSettings() {
		loadingSettings = true;
		try {
			const resp = await tsQuery.send(new DescribeAccountSettingsCommand({}));
			accountPricingModel = resp.QueryPricingModel ?? 'COMPUTE_UNITS';
			accountMaxTCU = resp.MaxQueryTCU ?? null;
			accountLastUpdated = (resp as Record<string, unknown>)['LastUpdatedTime'] as number ?? null;
		} catch (e) {
			toast.error('Failed to load account settings: ' + String(e));
		} finally {
			loadingSettings = false;
		}
	}

	async function updateAccountSettings() {
		try {
			await tsQuery.send(
				new UpdateAccountSettingsCommand({
					QueryPricingModel: accountPricingModel as 'BYTES_SCANNED' | 'COMPUTE_UNITS',
					MaxQueryTCU: accountMaxTCU ?? undefined
				})
			);
			toast.success('Account settings updated');
			await loadAccountSettings();
		} catch (e) {
			toast.error('Update failed: ' + String(e));
		}
	}

	// ---------------------------------------------------------------------------
	// Datum cell rendering
	// ---------------------------------------------------------------------------

	function renderDatum(datum: Row['Data'][number] | undefined): string {
		if (!datum) return '';
		if (datum.ScalarValue !== undefined && datum.ScalarValue !== null) {
			return datum.ScalarValue;
		}
		if (datum.NullValue) return 'NULL';
		if (datum.ArrayValue) return '[Array]';
		if (datum.TimeSeriesValue) return '[TimeSeries]';
		if (datum.RowValue) return '[Row]';
		return '';
	}

	// ---------------------------------------------------------------------------
	// Lifecycle
	// ---------------------------------------------------------------------------

	onMount(() => {
		loadScheduledQueries();
		loadAccountSettings();
	});
</script>

<div class="space-y-4 p-4">
	<div class="flex items-center gap-2 border-b pb-2">
		<Activity class="size-5 text-blue-600" />
		<h1 class="text-xl font-semibold">Timestream Query</h1>
	</div>

	<!-- Tab navigation -->
	<div class="flex gap-1 rounded bg-gray-100 p-1">
		{#each [['query', 'SQL Query'], ['scheduled', 'Scheduled Queries'], ['settings', 'Account Settings']] as [id, label]}
			<button
				class="rounded px-3 py-1.5 text-sm font-medium transition-colors {activeTab === id
					? 'bg-white shadow'
					: 'hover:bg-gray-200'}"
				onclick={() => (activeTab = id as typeof activeTab)}
			>
				{label}
			</button>
		{/each}
	</div>

	<!-- SQL Query tab -->
	{#if activeTab === 'query'}
		<div class="space-y-3">
			<!-- SQL editor -->
			<div class="rounded border">
				<div class="flex items-center justify-between border-b bg-gray-50 px-3 py-2 text-sm text-gray-600">
					<span class="font-mono font-medium">SQL Editor</span>
					<span class="text-xs">Timestream SQL</span>
				</div>
				<textarea
					bind:value={sqlText}
					rows={6}
					class="w-full resize-y p-3 font-mono text-sm focus:outline-none"
					placeholder="SELECT * FROM &quot;database&quot;.&quot;table&quot; ORDER BY time DESC LIMIT 100"
				></textarea>
			</div>

			<!-- Controls -->
			<div class="flex items-center gap-2">
				<button
					onclick={runQuery}
					disabled={queryRunning}
					class="flex items-center gap-1.5 rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
				>
					<Play class="size-4" />
					{queryRunning ? 'Running…' : 'Run Query'}
				</button>

				{#if queryRunning && currentQueryId}
					<button
						onclick={cancelQuery}
						class="flex items-center gap-1.5 rounded border border-red-300 px-4 py-2 text-sm font-medium text-red-700 hover:bg-red-50"
					>
						<StopCircle class="size-4" />
						Cancel
					</button>
				{/if}

				<button
					onclick={prepareQuery}
					class="flex items-center gap-1.5 rounded border px-4 py-2 text-sm font-medium hover:bg-gray-50"
				>
					<Eye class="size-4" />
					Prepare
				</button>

				<label class="flex items-center gap-1.5 text-sm text-gray-600">
					Max rows
					<input
						type="number"
						bind:value={maxRows}
						min={1}
						max={1000}
						class="w-20 rounded border px-2 py-1 text-sm"
					/>
				</label>
			</div>

			<!-- Error -->
			{#if errorMsg}
				<div class="rounded bg-red-50 px-3 py-2 text-sm text-red-700">{errorMsg}</div>
			{/if}

			<!-- Query status -->
			{#if resultStatus}
				<div class="flex gap-4 rounded bg-blue-50 px-3 py-2 text-xs text-blue-800">
					<span>Progress: {resultStatus.ProgressPercentage ?? 0}%</span>
					<span>Scanned: {((resultStatus.CumulativeBytesScanned ?? 0) / 1024 / 1024).toFixed(2)} MB</span>
					<span>Metered: {((resultStatus.CumulativeBytesMetered ?? 0) / 1024 / 1024).toFixed(2)} MB</span>
					{#if currentQueryId}
						<span class="font-mono">ID: {currentQueryId.slice(0, 12)}…</span>
					{/if}
				</div>
			{/if}

			<!-- Results table -->
			{#if resultColumns.length > 0}
				<div class="overflow-auto rounded border">
					<table class="min-w-full text-sm">
						<thead class="bg-gray-50 text-xs uppercase text-gray-500">
							<tr>
								{#each resultColumns as col}
									<th class="whitespace-nowrap border-b px-3 py-2 text-left">
										{col.Name ?? ''}
										<span class="ml-1 font-normal text-gray-400">
											{col.Type?.ScalarType ?? ''}
										</span>
									</th>
								{/each}
							</tr>
						</thead>
						<tbody class="divide-y">
							{#if resultRows.length === 0}
								<tr>
									<td colspan={resultColumns.length} class="px-3 py-8 text-center text-gray-400">
										No rows returned
									</td>
								</tr>
							{:else}
								{#each resultRows as row}
									<tr class="hover:bg-gray-50">
										{#each row.Data ?? [] as cell}
											<td class="whitespace-nowrap border-b px-3 py-2 font-mono text-xs">
												{renderDatum(cell)}
											</td>
										{/each}
									</tr>
								{/each}
							{/if}
						</tbody>
					</table>
				</div>

				<!-- Pagination -->
				<div class="flex items-center justify-between text-sm text-gray-600">
					<span>{resultRows.length} rows on this page</span>
					<div class="flex gap-2">
						{#if nextToken}
							<button
								onclick={loadNextPage}
								class="flex items-center gap-1 rounded border px-3 py-1 hover:bg-gray-50"
							>
								Next <ChevronRight class="size-4" />
							</button>
						{/if}
					</div>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Scheduled Queries tab -->
	{#if activeTab === 'scheduled'}
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<h2 class="font-medium">Scheduled Queries ({scheduledQueries.length})</h2>
				<div class="flex gap-2">
					<button
						onclick={loadScheduledQueries}
						class="flex items-center gap-1 rounded border px-3 py-1.5 text-sm hover:bg-gray-50"
					>
						<RefreshCw class="size-4" />
						Refresh
					</button>
					<button
						onclick={() => (showCreateForm = !showCreateForm)}
						class="flex items-center gap-1 rounded bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700"
					>
						<Plus class="size-4" />
						New
					</button>
				</div>
			</div>

			<!-- Create form -->
			{#if showCreateForm}
				<div class="rounded border bg-gray-50 p-4">
					<h3 class="mb-3 font-medium">Create Scheduled Query</h3>
					<div class="grid grid-cols-2 gap-3 text-sm">
						<label class="flex flex-col gap-1">
							Name *
							<input bind:value={newSqName} class="rounded border px-2 py-1" placeholder="my-scheduled-query" />
						</label>
						<label class="flex flex-col gap-1">
							Schedule Expression *
							<input bind:value={newSqSchedule} class="rounded border px-2 py-1" placeholder="rate(1 hour)" />
						</label>
						<label class="col-span-2 flex flex-col gap-1">
							Query String *
							<textarea bind:value={newSqQueryString} rows={3} class="rounded border px-2 py-1 font-mono text-xs" placeholder="SELECT ..." />
						</label>
						<label class="flex flex-col gap-1">
							Execution Role ARN
							<input bind:value={newSqRoleArn} class="rounded border px-2 py-1 font-mono text-xs" placeholder="arn:aws:iam::..." />
						</label>
						<label class="flex flex-col gap-1">
							SNS Topic ARN
							<input bind:value={newSqTopicArn} class="rounded border px-2 py-1 font-mono text-xs" placeholder="arn:aws:sns:..." />
						</label>
						<label class="flex flex-col gap-1">
							Error Report S3 Bucket
							<input bind:value={newSqS3Bucket} class="rounded border px-2 py-1" placeholder="my-error-bucket" />
						</label>
						<div class="flex gap-2">
							<label class="flex flex-1 flex-col gap-1">
								Target Database
								<input bind:value={newSqDatabase} class="rounded border px-2 py-1" placeholder="mydb" />
							</label>
							<label class="flex flex-1 flex-col gap-1">
								Target Table
								<input bind:value={newSqTable} class="rounded border px-2 py-1" placeholder="mytable" />
							</label>
						</div>
					</div>
					<div class="mt-3 flex gap-2">
						<button
							onclick={createScheduledQuery}
							class="rounded bg-blue-600 px-4 py-1.5 text-sm font-medium text-white hover:bg-blue-700"
						>
							Create
						</button>
						<button
							onclick={() => { showCreateForm = false; resetCreateForm(); }}
							class="rounded border px-4 py-1.5 text-sm hover:bg-gray-100"
						>
							Cancel
						</button>
					</div>
				</div>
			{/if}

			<!-- Query list -->
			{#if loadingQueries}
				<div class="py-8 text-center text-gray-400">Loading…</div>
			{:else if scheduledQueries.length === 0}
				<div class="py-8 text-center text-gray-400">No scheduled queries</div>
			{:else}
				<div class="space-y-2">
					{#each scheduledQueries as sq}
						<div class="rounded border bg-white p-3">
							<div class="flex items-start justify-between">
								<div>
									<div class="flex items-center gap-2">
										<span class="font-medium">{sq.Name}</span>
										<span
											class="rounded px-1.5 py-0.5 text-xs {sq.State === 'ENABLED'
												? 'bg-green-100 text-green-700'
												: 'bg-gray-100 text-gray-600'}"
										>
											{sq.State}
										</span>
									</div>
									<div class="mt-1 font-mono text-xs text-gray-500 truncate max-w-md">
										{sq.Arn}
									</div>
									{#if (sq as Record<string, unknown>)['NextInvocationTime']}
										<div class="mt-1 flex items-center gap-1 text-xs text-gray-500">
											<Clock class="size-3" />
											Next: {new Date(((sq as Record<string, unknown>)['NextInvocationTime'] as number) * 1000).toLocaleString()}
										</div>
									{/if}
								</div>
								<div class="flex gap-1">
									<button
										onclick={() => runScheduledQueryNow(sq)}
										title="Run now"
										class="rounded border p-1.5 hover:bg-gray-50"
									>
										<Play class="size-4 text-blue-600" />
									</button>
									<button
										onclick={() => toggleScheduledQuery(sq)}
										title={sq.State === 'ENABLED' ? 'Disable' : 'Enable'}
										class="rounded border p-1.5 hover:bg-gray-50"
									>
										<StopCircle class="size-4 {sq.State === 'ENABLED' ? 'text-orange-500' : 'text-green-500'}" />
									</button>
									<button
										onclick={() => deleteScheduledQuery(sq.Arn ?? '', sq.Name ?? '')}
										title="Delete"
										class="rounded border p-1.5 hover:bg-red-50"
									>
										<Trash2 class="size-4 text-red-500" />
									</button>
								</div>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}

	<!-- Account Settings tab -->
	{#if activeTab === 'settings'}
		<div class="max-w-md space-y-4">
			<h2 class="font-medium">Account Settings</h2>

			{#if loadingSettings}
				<div class="text-gray-400">Loading…</div>
			{:else}
				<div class="space-y-3 rounded border p-4">
					<label class="flex flex-col gap-1 text-sm">
						Query Pricing Model
						<select bind:value={accountPricingModel} class="rounded border px-2 py-1.5">
							<option value="COMPUTE_UNITS">COMPUTE_UNITS (default)</option>
							<option value="BYTES_SCANNED">BYTES_SCANNED (legacy)</option>
						</select>
					</label>

					<label class="flex flex-col gap-1 text-sm">
						Max Query TCU (optional)
						<input
							type="number"
							bind:value={accountMaxTCU}
							min={1}
							class="rounded border px-2 py-1.5"
							placeholder="Leave empty for on-demand"
						/>
					</label>

					{#if accountLastUpdated}
						<div class="text-xs text-gray-500">
							Last updated: {new Date(accountLastUpdated * 1000).toLocaleString()}
						</div>
					{/if}

					<button
						onclick={updateAccountSettings}
						class="flex items-center gap-1.5 rounded bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700"
					>
						<Settings class="size-4" />
						Save Settings
					</button>
				</div>
			{/if}
		</div>
	{/if}
</div>
