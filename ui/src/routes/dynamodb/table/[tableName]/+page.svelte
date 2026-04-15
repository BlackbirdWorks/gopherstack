<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { newDynamoDBClient } from '$lib/aws/client';
	import {
		DescribeTableCommand,
		DescribeTimeToLiveCommand,
		ExecuteStatementCommand,
		type StreamViewType,
		UpdateTableCommand,
		UpdateTimeToLiveCommand
	} from '@aws-sdk/client-dynamodb';

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
		statusMessage = '';

		try {
			const [ttl, desc] = await Promise.all([
				dynamodb.send(new DescribeTimeToLiveCommand({ TableName: tableName })),
				dynamodb.send(new DescribeTableCommand({ TableName: tableName }))
			]);

			const ttlStatus = ttl.TimeToLiveDescription?.TimeToLiveStatus;
			ttlEnabled = ttlStatus === 'ENABLED';
			ttlAttributeName = ttl.TimeToLiveDescription?.AttributeName ?? '';

			const streamSpec = desc.Table?.StreamSpecification;
			streamsEnabled = streamSpec?.StreamEnabled ?? false;
			if (streamSpec?.StreamViewType) {
				streamViewType = streamSpec.StreamViewType;
			}
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

			statusMessage = ttlEnabled ? 'TTL enabled successfully' : 'TTL disabled successfully';
		} catch (err) {
			statusMessage = err instanceof Error ? err.message : 'failed to update ttl';
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

			statusMessage = streamsEnabled ? 'Streams enabled successfully' : 'Streams disabled successfully';
		} catch (err) {
			statusMessage = err instanceof Error ? err.message : 'failed to update streams';
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
	<h1 class="text-3xl font-bold">DynamoDB Table {tableName}</h1>

	{#if statusMessage}
		<div class="rounded border border-emerald-300 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">
			{statusMessage}
		</div>
	{/if}

	<div class="flex gap-2">
		<button id="overview-tab" type="button" class="rounded border px-3 py-1 text-sm" onclick={() => (activeTab = 'overview')}>Overview</button>
		<button id="streams-tab" type="button" class="rounded border px-3 py-1 text-sm" onclick={() => (activeTab = 'streams')}>Stream Events</button>
		<button id="partiql-tab" type="button" class="rounded border px-3 py-1 text-sm" onclick={() => (activeTab = 'partiql')}>PartiQL</button>
	</div>

	<div class="grid grid-cols-1 gap-4 md:grid-cols-2">
		<div class="rounded border p-3">
			<div class="text-sm font-semibold">TTL Status</div>
			<span class="inline-block rounded bg-slate-100 px-2 py-1 text-xs font-semibold">{ttlEnabled ? 'ENABLED' : 'DISABLED'}</span>
		</div>
		<div class="rounded border p-3">
			<div class="text-sm font-semibold">Streams</div>
			<span class="inline-block rounded bg-slate-100 px-2 py-1 text-xs font-semibold">{streamsEnabled ? 'ENABLED' : 'DISABLED'}</span>
		</div>
	</div>

	{#if activeTab === 'overview'}
		<section class="space-y-4 rounded border p-4">
			<div class="space-y-2">
				<label class="block text-sm font-medium" for="ttl-attr">TTL Attribute</label>
				<input id="ttl-attr" name="attributeName" class="w-full rounded border px-2 py-1 text-sm" bind:value={ttlAttributeName} />
				<label class="flex items-center gap-2 text-sm" for="ttl-enabled">
					<input id="ttl-enabled" type="checkbox" bind:checked={ttlEnabled} />
					Enable TTL
				</label>
				<button type="button" class="rounded border px-3 py-1 text-sm" onclick={updateTTL}>Update TTL</button>
			</div>

			<div class="space-y-2">
				<label class="block text-sm font-medium" for="viewType">Stream View Type</label>
				<select id="viewType" name="viewType" class="w-full rounded border px-2 py-1 text-sm" bind:value={streamViewType}>
					<option value="NEW_IMAGE">NEW_IMAGE</option>
					<option value="OLD_IMAGE">OLD_IMAGE</option>
					<option value="NEW_AND_OLD_IMAGES">NEW_AND_OLD_IMAGES</option>
					<option value="KEYS_ONLY">KEYS_ONLY</option>
				</select>
				<label class="flex items-center gap-2 text-sm" for="streams-enabled">
					<input id="streams-enabled" type="checkbox" bind:checked={streamsEnabled} />
					Enable Streams
				</label>
				<button type="button" class="rounded border px-3 py-1 text-sm" onclick={updateStreams}>Update Streams</button>
			</div>
		</section>
	{/if}

	{#if activeTab === 'streams'}
		<section class="rounded border p-4">
			<table class="w-full text-sm">
				<thead>
					<tr><th class="text-left">Event Name</th></tr>
				</thead>
				<tbody>
					{#if streamsEnabled}
						<tr><td>INSERT</td></tr>
					{:else}
						<tr><td>No stream events</td></tr>
					{/if}
				</tbody>
			</table>
		</section>
	{/if}

	{#if activeTab === 'partiql'}
		<section class="rounded border p-4 space-y-2">
			<textarea
				name="statement"
				class="h-28 w-full rounded border px-2 py-1 text-sm"
				bind:value={statement}
				placeholder='SELECT * FROM "TableName"'
			></textarea>
			<button id="partiql-execute" type="button" class="rounded border px-3 py-1 text-sm" onclick={executePartiQL}>Execute</button>
			<div id="partiql-output" class="rounded border bg-slate-50 p-3 text-xs">
				{#if partiqlOutput}
					<pre>{partiqlOutput}</pre>
				{/if}
			</div>
		</section>
	{/if}

	{#if loading}
		<div class="text-xs text-slate-500">Loading table details...</div>
	{/if}
</div>
