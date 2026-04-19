<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getTransferClient } from '$lib/aws-client';
	import {
		ListServersCommand,
		DescribeServerCommand,
		CreateServerCommand,
		DeleteServerCommand,
		StartServerCommand,
		StopServerCommand,
		ListUsersCommand,
		CreateUserCommand,
		DeleteUserCommand,
		type DescribedServer,
		type ListedServer,
		type ListedUser
	} from '@aws-sdk/client-transfer';
	import { toast } from 'svelte-sonner';
	import {
		ArrowLeftRight,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Play,
		Square,
		Users,
		CheckCircle,
		XCircle,
		Server
	} from 'lucide-svelte';

	const transfer = getTransferClient();

	let loading = $state(false);
	let activeTab = $state<'servers' | 'users'>('servers');
	let searchQuery = $state('');

	// Servers
	let servers = $state<ListedServer[]>([]);
	let selectedServer = $state<ListedServer | null>(null);
	let serverDetail = $state<DescribedServer | null>(null);
	let serverUsers = $state<ListedUser[]>([]);
	let loadingUsers = $state(false);

	let showCreateServerModal = $state(false);
	let creatingServer = $state(false);
	let newServerProtocols = $state<string[]>(['SFTP']);
	let newServerEndpointType = $state<'PUBLIC' | 'VPC'>('PUBLIC');
	let newServerIdentityProviderType = $state<'SERVICE_MANAGED' | 'API_GATEWAY'>('SERVICE_MANAGED');

	// Users
	let selectedServerId = $state('');
	let showCreateUserModal = $state(false);
	let creatingUser = $state(false);
	let newUserName = $state('');
	let newUserRole = $state('');
	let newUserHomeDirectory = $state('/');

	const filteredServers = $derived(
		servers.filter(
			(s) =>
				(s.ServerId ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.Domain ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function serverStateBadge(state?: string) {
		if (state === 'ONLINE') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (state === 'OFFLINE') return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		if (state === 'STARTING' || state === 'STOPPING')
			return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadServers() {
		loading = true;
		try {
			const res = await transfer.send(new ListServersCommand({ MaxResults: 100 }));
			servers = res.Servers ?? [];
		} catch (e) {
			toast.error(`Failed to load servers: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewServer(server: ListedServer) {
		selectedServer = server;
		if (!server.ServerId) return;
		loadingUsers = true;
		try {
			const [detailRes, usersRes] = await Promise.all([
				transfer.send(new DescribeServerCommand({ ServerId: server.ServerId })),
				transfer.send(new ListUsersCommand({ ServerId: server.ServerId }))
			]);
			serverDetail = detailRes.Server ?? null;
			serverUsers = usersRes.Users ?? [];
			selectedServerId = server.ServerId;
		} catch (e) {
			toast.error(`Failed to load server details: ${e}`);
		} finally {
			loadingUsers = false;
		}
	}

	async function toggleServer(server: ListedServer) {
		if (!server.ServerId) return;
		try {
			if (server.State === 'ONLINE') {
				await transfer.send(new StopServerCommand({ ServerId: server.ServerId }));
				toast.success(`Stopping server ${server.ServerId}`);
			} else {
				await transfer.send(new StartServerCommand({ ServerId: server.ServerId }));
				toast.success(`Starting server ${server.ServerId}`);
			}
			await loadServers();
		} catch (e) {
			toast.error(`Failed to toggle server: ${e}`);
		}
	}

	async function deleteServer(server: ListedServer) {
		if (!server.ServerId || !await confirmDestructive(`Delete server ${server.ServerId}?`)) return;
		try {
			await transfer.send(new DeleteServerCommand({ ServerId: server.ServerId }));
			toast.success(`Server deleted`);
			selectedServer = null;
			await loadServers();
		} catch (e) {
			toast.error(`Failed to delete server: ${e}`);
		}
	}

	async function createServer() {
		creatingServer = true;
		try {
			await transfer.send(
				new CreateServerCommand({
					Protocols: newServerProtocols as ('SFTP' | 'FTP' | 'FTPS' | 'AS2')[],
					EndpointType: newServerEndpointType,
					IdentityProviderType: newServerIdentityProviderType
				})
			);
			toast.success('Server created');
			showCreateServerModal = false;
			newServerProtocols = ['SFTP'];
			await loadServers();
		} catch (e) {
			toast.error(`Failed to create server: ${e}`);
		} finally {
			creatingServer = false;
		}
	}

	async function createUser() {
		if (!newUserName.trim() || !newUserRole.trim() || !selectedServerId) return;
		creatingUser = true;
		try {
			await transfer.send(
				new CreateUserCommand({
					ServerId: selectedServerId,
					UserName: newUserName.trim(),
					Role: newUserRole.trim(),
					HomeDirectory: newUserHomeDirectory.trim()
				})
			);
			toast.success(`User "${newUserName}" created`);
			showCreateUserModal = false;
			newUserName = '';
			newUserRole = '';
			// Reload users for selected server
			if (selectedServer) await viewServer(selectedServer);
		} catch (e) {
			toast.error(`Failed to create user: ${e}`);
		} finally {
			creatingUser = false;
		}
	}

	async function deleteUser(userName: string) {
		if (!selectedServerId || !await confirmDestructive(`Delete user "${userName}"?`)) return;
		try {
			await transfer.send(new DeleteUserCommand({ ServerId: selectedServerId, UserName: userName }));
			toast.success(`User "${userName}" deleted`);
			if (selectedServer) await viewServer(selectedServer);
		} catch (e) {
			toast.error(`Failed to delete user: ${e}`);
		}
	}

	onMount(() => loadServers());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ArrowLeftRight class="h-8 w-8 text-blue-600" />
			<div>
				<h1 class="text-2xl font-bold">Transfer Family</h1>
				<p class="text-sm text-muted-foreground">Managed file transfer servers (SFTP, FTP, FTPS)</p>
			</div>
		</div>
		<button
			onclick={loadServers}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Servers list + create -->
	<div class="flex items-center justify-between gap-4">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search servers..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>
		<button
			onclick={() => (showCreateServerModal = true)}
			class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
		>
			<Plus class="h-4 w-4" />
			Create Server
		</button>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if filteredServers.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Server class="h-12 w-12 mb-3 opacity-30" />
			<p>No Transfer Family servers found</p>
			<p class="text-sm">Create a server to enable managed file transfers</p>
		</div>
	{:else}
		<div class="rounded-lg border overflow-hidden">
			<table class="w-full text-sm">
				<thead class="bg-muted/50">
					<tr>
						<th class="px-4 py-3 text-left font-medium">Server ID</th>
						<th class="px-4 py-3 text-left font-medium">Domain</th>
						<th class="px-4 py-3 text-left font-medium">Endpoint</th>
						<th class="px-4 py-3 text-left font-medium">State</th>
						<th class="px-4 py-3 text-left font-medium">Users</th>
						<th class="px-4 py-3 text-right font-medium">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y">
					{#each filteredServers as server}
						<tr
							class="hover:bg-muted/30 cursor-pointer"
							onclick={() => viewServer(server)}
						>
							<td class="px-4 py-3 font-mono text-xs">{server.ServerId}</td>
							<td class="px-4 py-3">{server.Domain ?? 'S3'}</td>
							<td class="px-4 py-3 text-muted-foreground">{server.EndpointType ?? '—'}</td>
							<td class="px-4 py-3">
								<span class="rounded-full px-2 py-0.5 text-xs font-medium {serverStateBadge(server.State)}">
									{server.State ?? '—'}
								</span>
							</td>
							<td class="px-4 py-3 text-muted-foreground">{server.UserCount ?? 0}</td>
							<td class="px-4 py-3 text-right flex justify-end gap-1">
								<button
									onclick={(e) => { e.stopPropagation(); toggleServer(server); }}
									class="rounded p-1 hover:bg-accent"
									title={server.State === 'ONLINE' ? 'Stop' : 'Start'}
								>
									{#if server.State === 'ONLINE'}
										<Square class="h-4 w-4 text-yellow-500" />
									{:else}
										<Play class="h-4 w-4 text-green-500" />
									{/if}
								</button>
								<button
									onclick={(e) => { e.stopPropagation(); deleteServer(server); }}
									class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
									title="Delete server"
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	<!-- Server Detail Panel -->
	{#if selectedServer}
		<div class="rounded-lg border p-4 space-y-4">
			<div class="flex items-center justify-between">
				<h3 class="font-semibold flex items-center gap-2">
					<Users class="h-5 w-5" />
					{selectedServer.ServerId} — Users
				</h3>
				<div class="flex gap-2">
					<button
						onclick={() => (showCreateUserModal = true)}
						class="flex items-center gap-1.5 rounded-md bg-primary px-3 py-1.5 text-xs text-primary-foreground hover:bg-primary/90"
					>
						<Plus class="h-3 w-3" />
						Add User
					</button>
					<button onclick={() => { selectedServer = null; serverUsers = []; }} class="text-xs text-muted-foreground hover:text-foreground">
						Close
					</button>
				</div>
			</div>

			{#if serverDetail}
				<div class="flex flex-wrap gap-4 text-sm">
					<div>
						<span class="text-muted-foreground">Endpoint Type:</span>
						<span class="ml-1">{serverDetail.EndpointType ?? '—'}</span>
					</div>
					<div>
						<span class="text-muted-foreground">Identity Provider:</span>
						<span class="ml-1">{serverDetail.IdentityProviderType ?? '—'}</span>
					</div>
					<div>
						<span class="text-muted-foreground">Protocols:</span>
						<span class="ml-1">{serverDetail.Protocols?.join(', ') ?? '—'}</span>
					</div>
				</div>
			{/if}

			{#if loadingUsers}
				<RefreshCw class="h-5 w-5 animate-spin text-muted-foreground" />
			{:else if serverUsers.length === 0}
				<p class="text-sm text-muted-foreground">No users configured.</p>
			{:else}
				<div class="rounded border overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-muted/50">
							<tr>
								<th class="px-3 py-2 text-left font-medium">Username</th>
								<th class="px-3 py-2 text-left font-medium">Home Directory</th>
								<th class="px-3 py-2 text-right font-medium">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y">
							{#each serverUsers as user}
								<tr>
									<td class="px-3 py-2 font-medium">{user.UserName}</td>
									<td class="px-3 py-2 text-muted-foreground font-mono text-xs">
										{user.HomeDirectory ?? '/'}
									</td>
									<td class="px-3 py-2 text-right">
										<button
											onclick={() => deleteUser(user.UserName ?? '')}
											class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
										>
											<Trash2 class="h-3.5 w-3.5" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Create Server Modal -->
{#if showCreateServerModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Transfer Server</h2>
			<div class="space-y-3">
				<div>
					<p class="block text-sm font-medium mb-2">Protocols</p>
					<div class="flex gap-3">
						{#each ['SFTP', 'FTP', 'FTPS', 'AS2'] as proto}
							<label class="flex items-center gap-1.5 text-sm">
								<input
									type="checkbox"
									checked={newServerProtocols.includes(proto)}
									onchange={() => {
										if (newServerProtocols.includes(proto)) {
											newServerProtocols = newServerProtocols.filter((p) => p !== proto);
										} else {
											newServerProtocols = [...newServerProtocols, proto];
										}
									}}
								/>
								{proto}
							</label>
						{/each}
					</div>
				</div>
				<div>
					<label for="server-endpoint" class="block text-sm font-medium mb-1">Endpoint Type</label>
					<select
						id="server-endpoint"
						bind:value={newServerEndpointType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="PUBLIC">Public</option>
						<option value="VPC">VPC</option>
					</select>
				</div>
				<div>
					<label for="server-identity" class="block text-sm font-medium mb-1"
						>Identity Provider</label
					>
					<select
						id="server-identity"
						bind:value={newServerIdentityProviderType}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="SERVICE_MANAGED">Service Managed</option>
						<option value="API_GATEWAY">API Gateway</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateServerModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createServer}
					disabled={creatingServer || newServerProtocols.length === 0}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingServer ? 'Creating...' : 'Create Server'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create User Modal -->
{#if showCreateUserModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Add Transfer User</h2>
			<div class="space-y-3">
				<div>
					<label for="user-name" class="block text-sm font-medium mb-1">Username *</label>
					<input
						id="user-name"
						type="text"
						bind:value={newUserName}
						placeholder="john.doe"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="user-role" class="block text-sm font-medium mb-1">IAM Role ARN *</label>
					<input
						id="user-role"
						type="text"
						bind:value={newUserRole}
						placeholder="arn:aws:iam::123456789012:role/transfer-role"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="user-home" class="block text-sm font-medium mb-1">Home Directory</label>
					<input
						id="user-home"
						type="text"
						bind:value={newUserHomeDirectory}
						placeholder="/my-bucket/uploads"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateUserModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createUser}
					disabled={creatingUser || !newUserName.trim() || !newUserRole.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creatingUser ? 'Creating...' : 'Add User'}
				</button>
			</div>
		</div>
	</div>
{/if}
