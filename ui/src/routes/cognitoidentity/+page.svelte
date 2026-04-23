<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getCognitoIdentityClient } from '$lib/aws-client';
	import {
		ListIdentityPoolsCommand,
		DescribeIdentityPoolCommand,
		CreateIdentityPoolCommand,
		DeleteIdentityPoolCommand,
		GetIdentityPoolRolesCommand,
		GetPrincipalTagAttributeMapCommand,
		LookupDeveloperIdentityCommand,
		MergeDeveloperIdentitiesCommand,
		SetPrincipalTagAttributeMapCommand,
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
	let principalProviderName = $state('');
	let principalUseDefaults = $state(true);
	let principalTagsText = $state('');
	let savingPrincipalTags = $state(false);
	let lookingUpDeveloperIdentity = $state(false);
	let mergingDeveloperIdentities = $state(false);
	let lookupIdentityID = $state('');
	let lookupDeveloperUserIdentifier = $state('');
	let developerProviderName = $state('');
	let lookupResultIdentityID = $state('');
	let lookupResultDeveloperUsers = $state<string[]>([]);
	let mergeSourceUserIdentifier = $state('');
	let mergeDestinationUserIdentifier = $state('');

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
		principalProviderName = '';
		principalUseDefaults = true;
		principalTagsText = '';
		lookupIdentityID = '';
		lookupDeveloperUserIdentifier = '';
		developerProviderName = '';
		lookupResultIdentityID = '';
		lookupResultDeveloperUsers = [];
		mergeSourceUserIdentifier = '';
		mergeDestinationUserIdentifier = '';
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
		if (!pool.IdentityPoolId || !await confirmDestructive({ title: 'Delete Identity Pool', message: `Delete identity pool "${pool.IdentityPoolName}"? All identities associated with this pool will be removed.` })) return;
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

	function parsePrincipalTags(text: string): Record<string, string> {
		const trimmed = text.trim();
		if (!trimmed) return {};

		try {
			const parsed = JSON.parse(trimmed);
			if (parsed && typeof parsed === 'object') {
				return Object.fromEntries(
					Object.entries(parsed).map(([k, v]) => [k, String(v ?? '')])
				);
			}
		} catch {
			// Fallback to line-based "key=value" parsing.
		}

		return Object.fromEntries(
			trimmed
				.split('\n')
				.map((line) => line.trim())
				.filter(Boolean)
				.map((line) => {
					const index = line.indexOf('=');
					if (index < 0) return [line, ''];
					return [line.slice(0, index).trim(), line.slice(index + 1).trim()];
				})
				.filter(([k]) => k.length > 0)
		);
	}

	async function loadPrincipalTagMap() {
		if (!selectedPool?.IdentityPoolId || !principalProviderName.trim()) return;
		try {
			const out = await cognitoId.send(
				new GetPrincipalTagAttributeMapCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					IdentityProviderName: principalProviderName.trim()
				})
			);
			principalUseDefaults = out.UseDefaults ?? true;
			principalTagsText = JSON.stringify(out.PrincipalTags ?? {}, null, 2);
			toast.success('Principal tag map loaded');
		} catch (e) {
			toast.error(`Failed to load principal tag map: ${e}`);
		}
	}

	async function savePrincipalTagMap() {
		if (!selectedPool?.IdentityPoolId || !principalProviderName.trim()) return;
		savingPrincipalTags = true;
		try {
			await cognitoId.send(
				new SetPrincipalTagAttributeMapCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					IdentityProviderName: principalProviderName.trim(),
					UseDefaults: principalUseDefaults,
					PrincipalTags: parsePrincipalTags(principalTagsText)
				})
			);
			toast.success('Principal tag map saved');
		} catch (e) {
			toast.error(`Failed to save principal tag map: ${e}`);
		} finally {
			savingPrincipalTags = false;
		}
	}

	async function runDeveloperIdentityLookup() {
		if (!selectedPool?.IdentityPoolId || !developerProviderName.trim()) return;
		if (!lookupIdentityID.trim() && !lookupDeveloperUserIdentifier.trim()) return;

		lookingUpDeveloperIdentity = true;
		lookupResultIdentityID = '';
		lookupResultDeveloperUsers = [];
		try {
			const out = await cognitoId.send(
				new LookupDeveloperIdentityCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					IdentityId: lookupIdentityID.trim() || undefined,
					DeveloperUserIdentifier: lookupDeveloperUserIdentifier.trim() || undefined
				})
			);
			lookupResultIdentityID = out.IdentityId ?? '';
			lookupResultDeveloperUsers = out.DeveloperUserIdentifierList ?? [];
			toast.success('Developer identity lookup complete');
		} catch (e) {
			toast.error(`Failed to lookup developer identity: ${e}`);
		} finally {
			lookingUpDeveloperIdentity = false;
		}
	}

	async function mergeDeveloperIdentityUsers() {
		if (!selectedPool?.IdentityPoolId || !developerProviderName.trim()) return;
		if (!mergeSourceUserIdentifier.trim() || !mergeDestinationUserIdentifier.trim()) return;

		mergingDeveloperIdentities = true;
		try {
			await cognitoId.send(
				new MergeDeveloperIdentitiesCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					SourceUserIdentifier: mergeSourceUserIdentifier.trim(),
					DestinationUserIdentifier: mergeDestinationUserIdentifier.trim(),
					DeveloperProviderName: developerProviderName.trim()
				})
			);
			toast.success('Developer identities merged');
		} catch (e) {
			toast.error(`Failed to merge developer identities: ${e}`);
		} finally {
			mergingDeveloperIdentities = false;
		}
	}

	onMount(() => loadPools());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg">
				<Users class="h-6 w-6 text-pink-600 dark:text-pink-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito Identity</h1>
				<p class="text-slate-600 dark:text-slate-300">Federated identity pools for AWS service access</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button
				onclick={loadPools}
				class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300"
				title="Refresh"
			>
				<RefreshCw class="h-4 w-4" />
			</button>
			<button
				onclick={() => (showCreateModal = true)}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"
			>
				<Plus class="h-4 w-4" />
				Create Pool
			</button>
		</div>
	</div>

	<!-- Stats cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg">
				<Users class="w-5 h-5 text-pink-600 dark:text-pink-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{identityPools.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Identity Pools</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{identityPools.filter(p => selectedPool?.IdentityPoolId === p.IdentityPoolId ? (selectedPool?.AllowUnauthenticatedIdentities ?? false) : false).length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Unauth Enabled</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<KeyRound class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{filteredPools.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Filtered Results</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Eye class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{selectedPool ? 1 : 0}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Selected</p>
			</div>
		</div>
	</div>

	<!-- Filter -->
	<div class="flex items-center gap-3">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
			<input
				type="text"
				placeholder="Search identity pools..."
				bind:value={searchQuery}
				class="w-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 pl-9 pr-4 py-2 text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
			/>
		</div>
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

			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium">Principal Tag Attribute Map</p>
				<input
					type="text"
					bind:value={principalProviderName}
					placeholder="Identity provider name"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<label class="flex items-center gap-2 text-xs">
					<input type="checkbox" bind:checked={principalUseDefaults} class="rounded" />
					Use defaults
				</label>
				<textarea
					bind:value={principalTagsText}
					rows="4"
					placeholder="JSON map or key=value lines"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				></textarea>
				<div class="flex gap-2">
					<button
						onclick={loadPrincipalTagMap}
						disabled={!principalProviderName.trim()}
						class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
					>
						Load
					</button>
					<button
						onclick={savePrincipalTagMap}
						disabled={savingPrincipalTags || !principalProviderName.trim()}
						class="rounded-md bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
					>
						{savingPrincipalTags ? 'Saving...' : 'Save'}
					</button>
				</div>
			</div>

			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium">Developer Identity Tools</p>
				<input
					type="text"
					bind:value={developerProviderName}
					placeholder="Developer provider name"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
					<input
						type="text"
						bind:value={lookupIdentityID}
						placeholder="Lookup by identity ID"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<input
						type="text"
						bind:value={lookupDeveloperUserIdentifier}
						placeholder="Lookup by developer user"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<button
					onclick={runDeveloperIdentityLookup}
					disabled={!developerProviderName.trim() || (!lookupIdentityID.trim() && !lookupDeveloperUserIdentifier.trim()) || lookingUpDeveloperIdentity}
					class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
				>
					{lookingUpDeveloperIdentity ? 'Looking up...' : 'Lookup'}
				</button>

				{#if lookupResultIdentityID}
					<div class="rounded border bg-muted/30 p-2 text-xs">
						<p><span class="font-medium">Identity:</span> <span class="font-mono">{lookupResultIdentityID}</span></p>
						<p><span class="font-medium">Developer users:</span> {lookupResultDeveloperUsers.join(', ') || 'none'}</p>
					</div>
				{/if}

				<div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
					<input
						type="text"
						bind:value={mergeSourceUserIdentifier}
						placeholder="Source user ID"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<input
						type="text"
						bind:value={mergeDestinationUserIdentifier}
						placeholder="Destination user ID"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<button
					onclick={mergeDeveloperIdentityUsers}
					disabled={!developerProviderName.trim() || !mergeSourceUserIdentifier.trim() || !mergeDestinationUserIdentifier.trim() || mergingDeveloperIdentities}
					class="rounded-md bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{mergingDeveloperIdentities ? 'Merging...' : 'Merge Developer Users'}
				</button>
			</div>
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
