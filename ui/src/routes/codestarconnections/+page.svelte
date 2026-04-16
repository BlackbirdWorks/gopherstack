<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';

	type Connection = {
		connectionArn?: string;
		connectionName?: string;
		connectionStatus?: string;
		providerType?: string;
		hostArn?: string;
	};

	type Host = {
		hostArn?: string;
		name?: string;
		providerType?: string;
		providerEndpoint?: string;
		status?: string;
	};

	let loading = $state(false);
	let connections = $state<Connection[]>([]);
	let hosts = $state<Host[]>([]);
	let showConnectionModal = $state(false);
	let showHostModal = $state(false);
	let connectionName = $state('');
	let hostName = $state('');
	let hostEndpoint = $state('');

	async function loadData() {
		loading = true;
		try {
			const [connectionsResponse, hostsResponse] = await Promise.all([
				fetch('/dashboard/api/codestarconnections/connections'),
				fetch('/dashboard/api/codestarconnections/hosts')
			]);

			connections = ((await connectionsResponse.json()) as { connections?: Connection[] }).connections ?? [];
			hosts = ((await hostsResponse.json()) as { hosts?: Host[] }).hosts ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load CodeStar Connections data: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createConnection() {
		try {
			const response = await fetch('/dashboard/api/codestarconnections/connections', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({ connectionName, providerType: 'GitHub' })
			});
			if (!response.ok) {
				throw new Error(((await response.json()) as { error?: string }).error ?? 'request failed');
			}
			showConnectionModal = false;
			connectionName = '';
			await loadData();
		} catch (err: unknown) {
			toast.error(`Failed to create connection: ${(err as Error).message}`);
		}
	}

	async function createHost() {
		try {
			const response = await fetch('/dashboard/api/codestarconnections/hosts', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					name: hostName,
					providerType: 'GitHubEnterpriseServer',
					providerEndpoint: hostEndpoint
				})
			});
			if (!response.ok) {
				throw new Error(((await response.json()) as { error?: string }).error ?? 'request failed');
			}
			showHostModal = false;
			hostName = '';
			hostEndpoint = '';
			await loadData();
		} catch (err: unknown) {
			toast.error(`Failed to create host: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		void loadData();
	});
</script>

<div class="space-y-6 p-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">AWS CodeStar Connections</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Connections and self-managed hosts</p>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="mb-4 flex gap-3">
			<button type="button" onclick={() => (showConnectionModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">+ Connection</button>
			<button type="button" onclick={() => (showHostModal = true)} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-900 hover:bg-slate-50 dark:border-slate-600 dark:text-white dark:hover:bg-slate-700">+ Host</button>
		</div>

		{#if loading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading resources...</p>
		{:else}
			<div class="space-y-6">
				<div>
					<h2 class="mb-2 text-lg font-semibold text-slate-900 dark:text-white">Connections</h2>
					{#if connections.length === 0}
						<p class="text-sm text-slate-500 dark:text-slate-400">No connections found</p>
					{:else}
						<div class="space-y-2">
							{#each connections as connection}
								<div class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
									<div class="font-medium text-slate-900 dark:text-white">{connection.connectionName}</div>
									<div class="text-xs text-slate-500 dark:text-slate-400">{connection.providerType} • {connection.connectionStatus}</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>

				<div>
					<h2 class="mb-2 text-lg font-semibold text-slate-900 dark:text-white">Hosts</h2>
					{#if hosts.length === 0}
						<p class="text-sm text-slate-500 dark:text-slate-400">No hosts found</p>
					{:else}
						<div class="space-y-2">
							{#each hosts as host}
								<div class="rounded-lg border border-slate-200 p-3 dark:border-slate-700">
									<div class="font-medium text-slate-900 dark:text-white">{host.name}</div>
									<div class="text-xs text-slate-500 dark:text-slate-400">{host.providerEndpoint} • {host.status}</div>
								</div>
							{/each}
						</div>
					{/if}
				</div>
			</div>
		{/if}
	</div>
</div>

{#if showConnectionModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div id="create-connection-modal" class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<form onsubmit={(event) => { event.preventDefault(); createConnection(); }} class="space-y-4">
				<input id="conn-name" bind:value={connectionName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Connection name" />
				<div class="flex justify-end gap-3"><button type="button" onclick={() => (showConnectionModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
			</form>
		</div>
	</div>
{/if}

{#if showHostModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div id="create-host-modal" class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<form onsubmit={(event) => { event.preventDefault(); createHost(); }} class="space-y-4">
				<input id="host-name" bind:value={hostName} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="Host name" />
				<input id="host-endpoint" bind:value={hostEndpoint} class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="https://example.com" />
				<div class="flex justify-end gap-3"><button type="button" onclick={() => (showHostModal = false)} class="px-4 py-2 text-sm">Cancel</button><button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white">Create</button></div>
			</form>
		</div>
	</div>
{/if}