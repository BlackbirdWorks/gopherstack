<script lang="ts">
	import { onMount } from 'svelte';
	import { getCognitoIdentityClient } from '$lib/aws-client';
	import {
		ListIdentityPoolsCommand,
		DescribeIdentityPoolCommand,
		CreateIdentityPoolCommand,
		DeleteIdentityPoolCommand,
		GetIdentityPoolRolesCommand,
		type IdentityPoolShortDescription,
		type IdentityPool
	} from '@aws-sdk/client-cognito-identity';
	import { toast } from 'svelte-sonner';
	import {
		Users,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		CheckCircle,
		XCircle,
		KeyRound
	} from 'lucide-svelte';

	const cognitoId = getCognitoIdentityClient();

	let loading = $state(false);
	let identityPools = $state<IdentityPoolShortDescription[]>([]);
	let selectedPool = $state<IdentityPool | null>(null);
	let poolRoles = $state<Record<string, string>>({});
	let loadingDetail = $state(false);
	let searchQuery = $state('');

	let showCreateModal = $state(false);
	let creating = $state(false);
	let newPoolName = $state('');
	let newPoolUnauthenticated = $state(false);
	let newPoolCognitoProvider = $state('');

	const filteredPools = $derived(
		identityPools.filter(
			(p) =>
				(p.IdentityPoolName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(p.IdentityPoolId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadPools() {
		loading = true;
		try {
			const res = await cognitoId.send(new ListIdentityPoolsCommand({ MaxResults: 60 }));
			identityPools = res.IdentityPools ?? [];
		} catch (e) {
			toast.error(`Failed to load identity pools: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewPool(pool: IdentityPoolShortDescription) {
		if (!pool.IdentityPoolId) return;
		loadingDetail = true;
		selectedPool = null;
		poolRoles = {};
		try {
			const [detailRes, rolesRes] = await Promise.all([
				cognitoId.send(new DescribeIdentityPoolCommand({ IdentityPoolId: pool.IdentityPoolId })),
				cognitoId.send(new GetIdentityPoolRolesCommand({ IdentityPoolId: pool.IdentityPoolId }))
			]);
			selectedPool = detailRes ?? null;
			poolRoles = rolesRes.Roles ?? {};
		} catch (e) {
			toast.error(`Failed to load pool details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function deletePool(pool: IdentityPoolShortDescription) {
		if (!pool.IdentityPoolId || !confirm(`Delete identity pool "${pool.IdentityPoolName}"?`)) return;
		try {
			await cognitoId.send(
				new DeleteIdentityPoolCommand({ IdentityPoolId: pool.IdentityPoolId })
			);
			toast.success(`Identity pool deleted`);
			if (selectedPool?.IdentityPoolId === pool.IdentityPoolId) selectedPool = null;
			await loadPools();
		} catch (e) {
			toast.error(`Failed to delete pool: ${e}`);
		}
	}

	async function createPool() {
		if (!newPoolName.trim()) return;
		creating = true;
		try {
			const providers: Record<string, { ClientId?: string; ServerSideTokenCheck?: boolean }> = {};
			if (newPoolCognitoProvider.trim()) {
				providers[newPoolCognitoProvider.trim()] = {};
			}
			await cognitoId.send(
				new CreateIdentityPoolCommand({
					IdentityPoolName: newPoolName.trim(),
					AllowUnauthenticatedIdentities: newPoolUnauthenticated,
					CognitoIdentityProviders:
						newPoolCognitoProvider.trim()
							? [{ ProviderName: newPoolCognitoProvider.trim() }]
							: undefined
				})
			);
			toast.success(`Identity pool "${newPoolName}" created`);
			showCreateModal = false;
			newPoolName = '';
			newPoolCognitoProvider = '';
			newPoolUnauthenticated = false;
			await loadPools();
		} catch (e) {
			toast.error(`Failed to create pool: ${e}`);
		} finally {
			creating = false;
		}
	}

	onMount(() => loadPools());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Users class="h-8 w-8 text-pink-600" />
			<div>
				<h1 class="text-2xl font-bold">Cognito Identity</h1>
				<p class="text-sm text-muted-foreground">Federated identity pools for AWS service access</p>
			</div>
		</div>
		<button
			onclick={loadPools}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Filter + Create -->
	<div class="flex items-center gap-3">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search identity pools..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>
		<button
			onclick={() => (showCreateModal = true)}
			class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
		>
			<Plus class="h-4 w-4" />
			Create Pool
		</button>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if filteredPools.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Users class="h-12 w-12 mb-3 opacity-30" />
			<p>No identity pools found</p>
			<p class="text-sm">Create a pool to enable federated identities</p>
		</div>
	{:else}
		<div class="rounded-lg border overflow-hidden">
			<table class="w-full text-sm">
				<thead class="bg-muted/50">
					<tr>
						<th class="px-4 py-3 text-left font-medium">Pool Name</th>
						<th class="px-4 py-3 text-left font-medium">Pool ID</th>
						<th class="px-4 py-3 text-right font-medium">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y">
					{#each filteredPools as pool}
						<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewPool(pool)}>
							<td class="px-4 py-3 font-medium">{pool.IdentityPoolName}</td>
							<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{pool.IdentityPoolId}</td>
							<td class="px-4 py-3 text-right flex justify-end gap-1">
								<button
									onclick={(e) => { e.stopPropagation(); viewPool(pool); }}
									class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
									title="View details"
								>
									<Eye class="h-4 w-4" />
								</button>
								<button
									onclick={(e) => { e.stopPropagation(); deletePool(pool); }}
									class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
									title="Delete pool"
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

	<!-- Pool Detail -->
	{#if loadingDetail}
		<div class="flex justify-center py-4">
			<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
		</div>
	{:else if selectedPool}
		<div class="rounded-lg border p-5 space-y-4">
			<div class="flex items-center justify-between">
				<h3 class="font-semibold">{selectedPool.IdentityPoolName}</h3>
				<button onclick={() => (selectedPool = null)} class="text-xs text-muted-foreground hover:text-foreground">
					Close
				</button>
			</div>

			<div class="grid grid-cols-2 gap-3 text-sm">
				<div>
					<p class="text-muted-foreground">Pool ID</p>
					<p class="font-mono text-xs">{selectedPool.IdentityPoolId}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Unauthenticated Identities</p>
					<div class="flex items-center gap-1">
						{#if selectedPool.AllowUnauthenticatedIdentities}
							<CheckCircle class="h-4 w-4 text-green-500" />
							<span>Allowed</span>
						{:else}
							<XCircle class="h-4 w-4 text-muted-foreground" />
							<span>Not allowed</span>
						{/if}
					</div>
				</div>
			</div>

			{#if (selectedPool.CognitoIdentityProviders ?? []).length > 0}
				<div>
					<p class="text-sm font-medium mb-2">Cognito User Pool Providers</p>
					<div class="divide-y rounded border overflow-hidden">
						{#each selectedPool.CognitoIdentityProviders ?? [] as provider}
							<div class="px-3 py-2 text-xs">
								<span class="font-mono">{provider.ProviderName}</span>
								{#if provider.ClientId}
									<span class="ml-2 text-muted-foreground">Client: {provider.ClientId}</span>
								{/if}
							</div>
						{/each}
					</div>
				</div>
			{/if}

			{#if Object.keys(poolRoles).length > 0}
				<div>
					<p class="text-sm font-medium mb-2 flex items-center gap-1">
						<KeyRound class="h-4 w-4" />
						IAM Roles
					</p>
					<div class="space-y-1">
						{#each Object.entries(poolRoles) as [roleType, roleArn]}
							<div class="text-xs">
								<span class="font-medium capitalize">{roleType}:</span>
								<span class="ml-1 font-mono text-muted-foreground truncate block">{roleArn}</span>
							</div>
						{/each}
					</div>
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Create Pool Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Identity Pool</h2>
			<div class="space-y-3">
				<div>
					<label for="pool-name" class="block text-sm font-medium mb-1">Pool Name *</label>
					<input
						id="pool-name"
						type="text"
						bind:value={newPoolName}
						placeholder="my-identity-pool"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="pool-provider" class="block text-sm font-medium mb-1"
						>Cognito User Pool Provider (optional)</label
					>
					<input
						id="pool-provider"
						type="text"
						bind:value={newPoolCognitoProvider}
						placeholder="cognito-idp.us-east-1.amazonaws.com/us-east-1_xxxxx"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary font-mono"
					/>
				</div>
				<div class="flex items-center gap-2">
					<input
						id="pool-unauth"
						type="checkbox"
						bind:checked={newPoolUnauthenticated}
						class="rounded"
					/>
					<label for="pool-unauth" class="text-sm">Allow unauthenticated identities</label>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createPool}
					disabled={creating || !newPoolName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Pool'}
				</button>
			</div>
		</div>
	</div>
{/if}
