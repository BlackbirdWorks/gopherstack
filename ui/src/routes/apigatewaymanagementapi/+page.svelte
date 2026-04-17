<script lang="ts">
	import { toast } from 'svelte-sonner';

	type Connection = {
		connectionId: string;
		sourceIp: string;
		userAgent: string;
	};

	let connections = $state<Connection[]>([]);
	let showCreateModal = $state(false);
	let connectionId = $state('');

	const supportedOperations = ['PostToConnection', 'GetConnection', 'DeleteConnection'];

	function createConnection() {
		if (!connectionId.trim()) {
			return;
		}

		connections = [
			{ connectionId: connectionId.trim(), sourceIp: '127.0.0.1', userAgent: 'Gopherstack UI' },
			...connections
		];
		toast.success(`Connection ${connectionId.trim()} created`);
		connectionId = '';
		showCreateModal = false;
	}
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">API Gateway Management API</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Manage simulated WebSocket connections</p>
	</div>

	<div class="flex items-center justify-between rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div>
			<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Supported Operations</h2>
			<div class="mt-3 flex flex-wrap gap-2">
				{#each supportedOperations as operation}
					<span class="rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700 dark:bg-slate-700 dark:text-slate-200">{operation}</span>
				{/each}
			</div>
		</div>
		<button type="button" onclick={() => (showCreateModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Simulate Connection</button>
	</div>

	<div class="grid gap-6 lg:grid-cols-[2fr,1fr]">
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<table class="w-full text-left text-sm">
				<thead>
					<tr class="border-b border-slate-200 dark:border-slate-700">
						<th class="pb-3 font-semibold text-slate-900 dark:text-white">Connection ID</th>
						<th class="pb-3 font-semibold text-slate-900 dark:text-white">Source IP</th>
						<th class="pb-3 font-semibold text-slate-900 dark:text-white">User Agent</th>
					</tr>
				</thead>
				<tbody>
					{#if connections.length === 0}
						<tr><td colspan="3" class="pt-4 text-slate-500 dark:text-slate-400">No simulated connections</td></tr>
					{:else}
						{#each connections as connection}
							<tr class="border-b border-slate-100 dark:border-slate-800">
								<td class="py-3">{connection.connectionId}</td>
								<td class="py-3">{connection.sourceIp}</td>
								<td class="py-3">{connection.userAgent}</td>
							</tr>
						{/each}
					{/if}
				</tbody>
			</table>
		</div>

		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Snippet</h2>
			<pre class="mt-3 overflow-auto rounded-lg bg-slate-950 p-4 text-xs text-slate-100">const client = new AWS.ApiGatewayManagementApi(&#123; endpoint &#125;);
await client.postToConnection(&#123; ConnectionId, Data &#125;).promise();
// service: apigatewaymanagementapi</pre>
		</div>
	</div>
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white">Create Connection</h2>
			<form onsubmit={(event) => { event.preventDefault(); createConnection(); }} class="mt-4 space-y-4">
				<input name="connectionId" bind:value={connectionId} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="e.g. conn-001" />
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => (showCreateModal = false)} class="rounded-lg px-4 py-2 text-sm text-slate-600 dark:text-slate-300">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}
