<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getCodeConnectionsClient } from '$lib/aws-client';
	import {
		ListConnectionsCommand,
		ListHostsCommand,
		ListRepositoryLinksCommand,
		ListSyncConfigurationsCommand,
		GetSyncBlockerSummaryCommand,
		GetSyncConfigurationCommand,
		type Connection,
		type Host,
		type RepositoryLinkInfo,
		type SyncConfiguration
	} from '@aws-sdk/client-codeconnections';
	import { toast } from 'svelte-sonner';
	import { Link2, Server, RefreshCw, GitBranch, Settings, ShieldAlert } from 'lucide-svelte';

	const client = regionalClient(getCodeConnectionsClient);

	let loading = $state(false);
	let connections = $state<Connection[]>([]);
	let hosts = $state<Host[]>([]);
	let repositoryLinks = $state<RepositoryLinkInfo[]>([]);
	let selectedConnection = $state<Connection | null>(null);
	let syncConfigs = $state<SyncConfiguration[]>([]);
	let loadingSyncConfigs = $state(false);
	let blockerCount = $state(0);
	let activeTab = $state<'connections' | 'hosts' | 'repolinks'>('connections');

	async function loadData() {
		loading = true;
		try {
			const [connResp, hostResp, linkResp] = await Promise.all([
				client().send(new ListConnectionsCommand({})),
				client().send(new ListHostsCommand({})),
				client().send(new ListRepositoryLinksCommand({}))
			]);
			connections = connResp.Connections ?? [];
			hosts = hostResp.Hosts ?? [];
			repositoryLinks = linkResp.RepositoryLinks ?? [];
		} catch (e) {
			toast.error('Failed to load CodeConnections data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectConnection(conn: Connection) {
		selectedConnection = conn;
		syncConfigs = [];
		blockerCount = 0;
		if (!conn.ConnectionArn) return;
		loadingSyncConfigs = true;
		try {
			const connLinks = repositoryLinks.filter((l) => l.ConnectionArn === conn.ConnectionArn);
			const allConfigs: SyncConfiguration[] = [];
			let blockers = 0;
			for (const link of connLinks) {
				if (!link.RepositoryLinkId) continue;
				try {
					const resp = await client().send(
						new ListSyncConfigurationsCommand({
							RepositoryLinkId: link.RepositoryLinkId,
							SyncType: 'CFN_STACK_SYNC'
						})
					);
					allConfigs.push(...(resp.SyncConfigurations ?? []));
				} catch {
					// link may have no configs
				}
			}
			// Check blockers for each config
			for (const cfg of allConfigs) {
				if (!cfg.ResourceName) continue;
				try {
					const summary = await client().send(
						new GetSyncBlockerSummaryCommand({
							ResourceName: cfg.ResourceName,
							SyncType: 'CFN_STACK_SYNC'
						})
					);
					blockers += (summary.SyncBlockerSummary?.LatestBlockers ?? []).length;
				} catch {
					// ignore
				}
			}
			syncConfigs = allConfigs;
			blockerCount = blockers;
		} catch (e) {
			toast.error('Failed to load sync configs: ' + String(e));
		} finally {
			loadingSyncConfigs = false;
		}
	}

	async function viewSyncConfig(resourceName: string) {
		try {
			const resp = await client().send(
				new GetSyncConfigurationCommand({ ResourceName: resourceName, SyncType: 'CFN_STACK_SYNC' })
			);
			const cfg = resp.SyncConfiguration;
			toast.info(`Sync config: branch=${cfg?.Branch ?? 'N/A'}, file=${cfg?.ConfigFile ?? 'N/A'}`);
		} catch (e) {
			toast.error('Failed to get sync config: ' + String(e));
		}
	}

	function statusColor(status: string | undefined) {
		switch (status) {
			case 'AVAILABLE':
				return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
			case 'PENDING':
				return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400';
			case 'ERROR':
				return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
			default:
				return 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400';
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Link2 class="w-7 h-7 text-blue-500" />
			<div>
				<h1 id="codeconnections-title" class="text-2xl font-bold text-gray-900 dark:text-white">AWS CodeConnections</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">External source control connections and sync configurations</p>
			</div>
		</div>
		<button
			onclick={loadData}
			title="Refresh"
			class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
		>
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Stats -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Link2 class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{connections.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Connections</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Server class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{hosts.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Hosts</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<GitBranch class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{repositoryLinks.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Repo Links</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
				<ShieldAlert class="w-5 h-5 text-red-600 dark:text-red-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{blockerCount}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Sync Blockers</p>
			</div>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-2 border-b border-slate-200 dark:border-slate-700">
		{#each [['connections', 'Connections'], ['hosts', 'Hosts'], ['repolinks', 'Repo Links']] as [tab, label]}
			<button
				onclick={() => (activeTab = tab as typeof activeTab)}
				class="px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab ? 'border-blue-500 text-blue-600 dark:text-blue-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-200'}"
			>{label}</button>
		{/each}
	</div>

	{#if loading}
		<div class="text-center py-12 text-gray-500 dark:text-gray-400">Loading...</div>
	{:else}
		<!-- Connections Tab -->
		{#if activeTab === 'connections'}
			<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
					<div class="p-4 border-b border-slate-200 dark:border-slate-700">
						<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Connections ({connections.length})</h2>
					</div>
					<div class="p-2 max-h-96 overflow-y-auto">
						{#if connections.length === 0}
							<div class="text-center py-8 text-gray-500 dark:text-gray-400 text-sm">No connections found</div>
						{:else}
							{#each connections as conn}
								<button
									onclick={() => selectConnection(conn)}
									class="w-full text-left p-3 rounded-lg hover:bg-gray-50 dark:hover:bg-slate-700 {selectedConnection?.ConnectionArn === conn.ConnectionArn ? 'bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-800' : ''}"
								>
									<div class="flex items-center justify-between">
										<span class="text-sm font-medium text-gray-900 dark:text-white truncate">{conn.ConnectionName}</span>
										<span class="ml-2 text-xs px-2 py-0.5 rounded-full {statusColor(conn.ConnectionStatus)}">{conn.ConnectionStatus}</span>
									</div>
									<p class="text-xs text-gray-500 dark:text-gray-400 mt-1">{conn.ProviderType}</p>
								</button>
							{/each}
						{/if}
					</div>
				</div>

				<div class="lg:col-span-2 space-y-4">
					{#if selectedConnection}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
							<h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-3">{selectedConnection.ConnectionName}</h2>
							<div class="grid grid-cols-2 gap-3 text-sm">
								<div>
									<p class="text-xs text-gray-500 dark:text-gray-400">Status</p>
									<span class="px-2 py-0.5 rounded-full text-xs {statusColor(selectedConnection.ConnectionStatus)}">{selectedConnection.ConnectionStatus}</span>
								</div>
								<div>
									<p class="text-xs text-gray-500 dark:text-gray-400">Provider</p>
									<p class="font-medium text-gray-900 dark:text-white">{selectedConnection.ProviderType}</p>
								</div>
								<div class="col-span-2">
									<p class="text-xs text-gray-500 dark:text-gray-400">ARN</p>
									<p class="font-mono text-xs text-gray-900 dark:text-white truncate">{selectedConnection.ConnectionArn}</p>
								</div>
							</div>
						</div>

						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
							<div class="flex items-center gap-2 mb-3">
								<Settings class="w-4 h-4 text-gray-500" />
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Sync Configurations</h3>
							</div>
							{#if loadingSyncConfigs}
								<p class="text-center py-4 text-sm text-gray-500 dark:text-gray-400">Loading...</p>
							{:else if syncConfigs.length === 0}
								<p class="text-center py-4 text-sm text-gray-500 dark:text-gray-400">No sync configurations</p>
							{:else}
								<div class="space-y-2">
									{#each syncConfigs as cfg}
										<div class="flex items-center justify-between p-2 rounded bg-gray-50 dark:bg-slate-700">
											<span class="text-sm font-medium text-gray-900 dark:text-white">{cfg.ResourceName}</span>
											<div class="flex items-center gap-3">
												<span class="flex items-center gap-1 text-xs text-blue-600 dark:text-blue-400">
													<GitBranch class="w-3 h-3" />{cfg.Branch}
												</span>
												<button
													onclick={() => viewSyncConfig(cfg.ResourceName ?? '')}
													class="text-xs text-gray-500 dark:text-gray-400 hover:text-blue-600 dark:hover:text-blue-400"
												>Details</button>
											</div>
										</div>
									{/each}
								</div>
							{/if}
						</div>
					{:else}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center text-gray-500 dark:text-gray-400 text-sm">
							Select a connection to view sync configurations
						</div>
					{/if}
				</div>
			</div>
		{/if}

		<!-- Hosts Tab -->
		{#if activeTab === 'hosts'}
			{#if hosts.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center text-gray-500 dark:text-gray-400 text-sm">No hosts found</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-slate-700">
							<tr>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Name</th>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Provider</th>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Endpoint</th>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Status</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-slate-700">
							{#each hosts as host}
								<tr class="hover:bg-gray-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{host.Name}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{host.ProviderType}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300 font-mono text-xs">{host.ProviderEndpoint}</td>
									<td class="px-4 py-3">
										<span class="px-2 py-0.5 rounded-full text-xs {statusColor(host.Status)}">{host.Status}</span>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}

		<!-- Repo Links Tab -->
		{#if activeTab === 'repolinks'}
			{#if repositoryLinks.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center text-gray-500 dark:text-gray-400 text-sm">No repository links found</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-gray-50 dark:bg-slate-700">
							<tr>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Repository</th>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Owner</th>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Provider</th>
								<th class="px-4 py-3 text-left text-xs font-medium text-gray-500 dark:text-gray-400 uppercase tracking-wider">Link ID</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-gray-100 dark:divide-slate-700">
							{#each repositoryLinks as link}
								<tr class="hover:bg-gray-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-medium text-gray-900 dark:text-white">{link.RepositoryName}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{link.OwnerId}</td>
									<td class="px-4 py-3 text-gray-600 dark:text-gray-300">{link.ProviderType}</td>
									<td class="px-4 py-3 font-mono text-xs text-gray-500 dark:text-gray-400">{link.RepositoryLinkId}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
