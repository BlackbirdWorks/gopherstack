<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { newDynamoDBClient } from '$lib/aws/client';
	import {
		DescribeTableCommand,
		DescribeTimeToLiveCommand,
		ExecuteStatementCommand,
		type StreamViewType,
		type TableDescription,
		UpdateTableCommand,
		UpdateTimeToLiveCommand
	} from '@aws-sdk/client-dynamodb';
	import { toast } from 'svelte-sonner';

	const dynamodb = newDynamoDBClient();

	const tableName = $derived(page.params.tableName ?? '');

	let loading = $state(false);
	let ttlEnabled = $state(false);
	let ttlAttributeName = $state('');
	let streamsEnabled = $state(false);
	let streamViewType = $state<StreamViewType>('NEW_AND_OLD_IMAGES');
	let activeTab = $state<'overview' | 'streams' | 'partiql'>('overview');
	let statusMessage = $state('');
	let statement = $state('');
	let partiqlOutput = $state('');
	let tableDesc = $state<TableDescription | null>(null);

	function stringifyPartiQLOutput(result: unknown): string {
		try {
			return JSON.stringify(result, null, 2);
		} catch {
			return String(result);
		}
	}

	async function loadState(): Promise<void> {
		if (!tableName) {
			return;
		}

		loading = true;

		try {
			const [ttl, desc] = await Promise.all([
				dynamodb.send(new DescribeTimeToLiveCommand({ TableName: tableName })),
				dynamodb.send(new DescribeTableCommand({ TableName: tableName }))
			]);

			tableDesc = desc.Table ?? null;

			const ttlStatus = ttl.TimeToLiveDescription?.TimeToLiveStatus;
			ttlEnabled = ttlStatus === 'ENABLED';
			ttlAttributeName = ttl.TimeToLiveDescription?.AttributeName ?? '';

			const streamSpec = desc.Table?.StreamSpecification;
			streamsEnabled = streamSpec?.StreamEnabled ?? false;
			if (streamSpec?.StreamViewType) {
				streamViewType = streamSpec.StreamViewType;
			}

			statement = `SELECT * FROM "${tableName}"`;
		} catch (err) {
			statusMessage = err instanceof Error ? err.message : 'Failed to load table details';
		} finally {
			loading = false;
		}
	}

	async function updateTTL(): Promise<void> {
		if (!tableName) {
			return;
		}

		try {
			await dynamodb.send(
				new UpdateTimeToLiveCommand({
					TableName: tableName,
					TimeToLiveSpecification: {
						AttributeName: ttlAttributeName,
						Enabled: ttlEnabled
					}
				})
			);

			if (ttlEnabled) {
				toast.success('TTL enabled successfully');
			} else {
				toast.success('TTL disabled successfully');
			}
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'failed to update ttl');
		}

		await loadState();
	}

	async function updateStreams(): Promise<void> {
		if (!tableName) {
			return;
		}

		try {
			if (streamsEnabled) {
				await dynamodb.send(
					new UpdateTableCommand({
						TableName: tableName,
						StreamSpecification: {
							StreamEnabled: true,
							StreamViewType: streamViewType
						}
					})
				);
			} else {
				await dynamodb.send(
					new UpdateTableCommand({
						TableName: tableName,
						StreamSpecification: {
							StreamEnabled: false
						}
					})
				);
			}

			if (streamsEnabled) {
				toast.success('Streams enabled successfully');
			} else {
				toast.success('Streams disabled successfully');
			}
		} catch (err) {
			toast.error(err instanceof Error ? err.message : 'failed to update streams');
		}

		await loadState();
	}

	async function executePartiQL(): Promise<void> {
		if (!tableName || !statement.trim()) {
			return;
		}

		try {
			const result = await dynamodb.send(
				new ExecuteStatementCommand({
					Statement: statement
				})
			);
			partiqlOutput = stringifyPartiQLOutput(result.Items ?? []);
		} catch (err) {
			partiqlOutput = err instanceof Error ? err.message : 'query failed';
		}
	}

	onMount(() => {
		void loadState();
	});
</script>

<div class="space-y-4">
	<div class="flex items-center gap-4 mb-4">
		<a href="/dashboard/dynamodb" class="inline-flex items-center gap-1.5 text-sm font-medium text-slate-600 hover:text-blue-600 dark:text-slate-400 dark:hover:text-white">
			<svg class="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M15 19l-7-7 7-7" /></svg>
			DynamoDB Tables
		</a>
		<span class="text-slate-400">/</span>
		<h1 class="text-2xl font-bold text-slate-900 dark:text-white">{tableName}</h1>
	</div>

	<!-- Stat Cards -->
	<div class="grid grid-cols-2 md:grid-cols-4 gap-4">
		<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
			<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Item Count</div>
			<div class="text-2xl font-bold text-blue-600 dark:text-blue-400 mt-1">{tableDesc?.ItemCount ?? 0}</div>
		</div>
		<div id="ttl-status-card" class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
			<div class="text-xs font-medium text-slate-500 dark:text-slate-400">TTL</div>
			<div class="mt-1"><span class="inline-block rounded bg-slate-100 px-2 py-1 text-xs font-semibold dark:bg-slate-700 dark:text-slate-300">{ttlEnabled ? 'ENABLED' : 'DISABLED'}</span></div>
		</div>
		<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
			<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Streams</div>
			<div class="mt-1"><span class="inline-block rounded bg-slate-100 px-2 py-1 text-xs font-semibold dark:bg-slate-700 dark:text-slate-300">{streamsEnabled ? 'ENABLED' : 'DISABLED'}</span></div>
		</div>
		<div class="p-4 bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm">
			<div class="text-xs font-medium text-slate-500 dark:text-slate-400">Status</div>
			<div class="mt-1"><span class="inline-block rounded bg-green-100 text-green-800 px-2 py-1 text-xs font-semibold dark:bg-green-900 dark:text-green-300">{tableDesc?.TableStatus ?? 'LOADING'}</span></div>
		</div>
	</div>

	{#if statusMessage}
		<div class="rounded border border-red-300 bg-red-50 px-3 py-2 text-sm text-red-800 dark:bg-red-900/20 dark:text-red-400">
			{statusMessage}
		</div>
	{/if}

	<div class="flex gap-2 border-b border-slate-200 dark:border-slate-700 pb-0">
		{#each [['overview', 'Overview'], ['streams', 'Stream Events'], ['partiql', 'PartiQL']] as [id, label]}
			<button id="{id}-tab" type="button"
				class="px-4 py-2 text-sm font-medium border-b-2 -mb-px {activeTab === id ? 'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' : 'border-transparent text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-300'}"
				onclick={() => (activeTab = id as 'overview' | 'streams' | 'partiql')}>
				{label}
			</button>
		{/each}
	</div>

	{#if activeTab === 'overview'}
		<section class="space-y-4 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4">
			<div class="space-y-2">
				<label class="block text-sm font-medium text-slate-900 dark:text-white" for="ttl-attr">TTL Attribute</label>
				<input id="ttl-attr" name="attributeName" class="w-full rounded border border-slate-300 px-2 py-1 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={ttlAttributeName} />
				<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300" for="ttl-enabled">
					<input id="ttl-enabled" type="checkbox" bind:checked={ttlEnabled} />
					Enable TTL
				</label>
				<button type="button" class="rounded bg-blue-600 text-white px-3 py-1 text-sm hover:bg-blue-700" onclick={updateTTL}>Update TTL</button>
			</div>

			<div class="space-y-2">
				<label class="block text-sm font-medium text-slate-900 dark:text-white" for="viewType">Stream View Type</label>
				<select id="viewType" name="viewType" class="w-full rounded border border-slate-300 px-2 py-1 text-sm dark:bg-slate-700 dark:border-slate-600 dark:text-white" bind:value={streamViewType}>
					<option value="NEW_IMAGE">NEW_IMAGE</option>
					<option value="OLD_IMAGE">OLD_IMAGE</option>
					<option value="NEW_AND_OLD_IMAGES">NEW_AND_OLD_IMAGES</option>
					<option value="KEYS_ONLY">KEYS_ONLY</option>
				</select>
				<label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300" for="streams-enabled">
					<input id="streams-enabled" type="checkbox" bind:checked={streamsEnabled} />
					Enable Streams
				</label>
				<button type="button" class="rounded bg-blue-600 text-white px-3 py-1 text-sm hover:bg-blue-700" onclick={updateStreams}>Update Streams</button>
			</div>
		</section>
	{/if}

	{#if activeTab === 'streams'}
		<section class="rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4">
			<table class="w-full text-sm">
				<thead>
					<tr><th class="text-left text-slate-700 dark:text-slate-300">Event Name</th></tr>
				</thead>
				<tbody>
					{#if streamsEnabled}
						<tr><td class="text-slate-600 dark:text-slate-400">INSERT</td></tr>
					{:else}
						<tr><td class="text-slate-500 dark:text-slate-400">No stream events</td></tr>
					{/if}
				</tbody>
			</table>
		</section>
	{/if}

	{#if activeTab === 'partiql'}
		<section class="rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 space-y-2">
			<textarea
				name="statement"
				class="h-28 w-full rounded border border-slate-300 px-2 py-1 text-sm font-mono dark:bg-slate-700 dark:border-slate-600 dark:text-white"
				bind:value={statement}
				placeholder='SELECT * FROM "TableName"'
			></textarea>
			<button id="partiql-execute" type="button" class="rounded bg-blue-600 text-white px-3 py-1 text-sm hover:bg-blue-700" onclick={executePartiQL}>Execute</button>
			<div id="partiql-output" class="rounded border border-slate-200 dark:border-slate-700 bg-slate-50 dark:bg-slate-900 p-3 text-xs">
				{#if partiqlOutput}
					<pre class="text-slate-800 dark:text-slate-200">{partiqlOutput}</pre>
				{/if}
			</div>
		</section>
	{/if}

	{#if loading}
		<div class="text-xs text-slate-500 dark:text-slate-400">Loading table details...</div>
	{/if}
</div>
