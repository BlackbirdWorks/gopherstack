<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getRAMClient } from '$lib/aws-client';
	import {
		GetResourceSharesCommand,
		ListResourcesCommand,
		ListPrincipalsCommand,
		ListPermissionsCommand,
		ListPermissionVersionsCommand,
		GetResourceShareInvitationsCommand,
		RejectResourceShareInvitationCommand,
		AcceptResourceShareInvitationCommand,
		SetDefaultPermissionVersionCommand,
		type ResourceShare,
		type Resource,
		type Principal,
		type ResourceSharePermissionSummary,
		type ResourceSharePermissionDetail,
		type ResourceShareInvitation
	} from '@aws-sdk/client-ram';
	import { toast } from 'svelte-sonner';
	import { Share2, RefreshCw, Search, Users, Box, CheckCircle, Key, Bell } from 'lucide-svelte';

	const ram = regionalClient(getRAMClient);

	let loading = $state(false);
	let activeTab = $state<'shares' | 'resources' | 'principals' | 'permissions' | 'invitations'>('shares');
	let searchQuery = $state('');
	let shares = $state<ResourceShare[]>([]);
	let resources = $state<Resource[]>([]);
	let principals = $state<Principal[]>([]);
	let permissions = $state<ResourceSharePermissionSummary[]>([]);
	let invitations = $state<ResourceShareInvitation[]>([]);

	// Permission version management
	let selectedPermission = $state<ResourceSharePermissionSummary | null>(null);
	let permissionVersions = $state<ResourceSharePermissionDetail[]>([]);
	let loadingVersions = $state(false);

	// Pagination state
	let sharesNextToken = $state<string | null>(null);
	let resourcesNextToken = $state<string | null>(null);
	let principalsNextToken = $state<string | null>(null);
	let permissionsNextToken = $state<string | null>(null);

	const filteredShares = $derived(shares.filter((s) => (s.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredResources = $derived(resources.filter((r) => (r.arn ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredPrincipals = $derived(principals.filter((p) => (p.id ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredPermissions = $derived(permissions.filter((p) => (p.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredInvitations = $derived(invitations.filter((i) => (i.resourceShareName ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const activeShares = $derived(shares.filter((s) => s.status === 'ACTIVE').length);

	async function loadData() {
		loading = true;
		try {
			const [sharesResp, resourcesResp, principalsResp, permsResp, invsResp] = await Promise.all([
				ram().send(new GetResourceSharesCommand({ resourceOwner: 'SELF' })),
				ram().send(new ListResourcesCommand({ resourceOwner: 'SELF' })),
				ram().send(new ListPrincipalsCommand({ resourceOwner: 'SELF' })),
				ram().send(new ListPermissionsCommand({})),
				ram().send(new GetResourceShareInvitationsCommand({}))
			]);
			shares = sharesResp.resourceShares ?? [];
			sharesNextToken = sharesResp.nextToken ?? null;
			resources = resourcesResp.resources ?? [];
			resourcesNextToken = resourcesResp.nextToken ?? null;
			principals = principalsResp.principals ?? [];
			principalsNextToken = principalsResp.nextToken ?? null;
			permissions = permsResp.permissions ?? [];
			permissionsNextToken = permsResp.nextToken ?? null;
			invitations = invsResp.resourceShareInvitations ?? [];
		} catch (e) {
			toast.error('Failed to load RAM data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadMoreShares() {
		if (!sharesNextToken) return;
		try {
			const resp = await ram().send(new GetResourceSharesCommand({ resourceOwner: 'SELF', nextToken: sharesNextToken }));
			shares = [...shares, ...(resp.resourceShares ?? [])];
			sharesNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more shares: ' + String(e));
		}
	}

	async function loadMoreResources() {
		if (!resourcesNextToken) return;
		try {
			const resp = await ram().send(new ListResourcesCommand({ resourceOwner: 'SELF', nextToken: resourcesNextToken }));
			resources = [...resources, ...(resp.resources ?? [])];
			resourcesNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more resources: ' + String(e));
		}
	}

	async function loadMorePrincipals() {
		if (!principalsNextToken) return;
		try {
			const resp = await ram().send(new ListPrincipalsCommand({ resourceOwner: 'SELF', nextToken: principalsNextToken }));
			principals = [...principals, ...(resp.principals ?? [])];
			principalsNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more principals: ' + String(e));
		}
	}

	async function loadMorePermissions() {
		if (!permissionsNextToken) return;
		try {
			const resp = await ram().send(new ListPermissionsCommand({ nextToken: permissionsNextToken }));
			permissions = [...permissions, ...(resp.permissions ?? [])];
			permissionsNextToken = resp.nextToken ?? null;
		} catch (e) {
			toast.error('Failed to load more permissions: ' + String(e));
		}
	}

	async function selectPermission(perm: ResourceSharePermissionSummary) {
		selectedPermission = perm;
		permissionVersions = [];
		if (!perm.arn) return;
		loadingVersions = true;
		try {
			const resp = await ram().send(new ListPermissionVersionsCommand({ permissionArn: perm.arn }));
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			permissionVersions = (resp.permissions ?? []) as any;
		} catch (e) {
			toast.error('Failed to load permission versions: ' + String(e));
		} finally {
			loadingVersions = false;
		}
	}

	async function setDefaultVersion(permArn: string, version: number) {
		try {
			await ram().send(new SetDefaultPermissionVersionCommand({ permissionArn: permArn, permissionVersion: version }));
			toast.success('Default permission version updated');
			if (selectedPermission) await selectPermission(selectedPermission);
			await loadData();
		} catch (e) {
			toast.error('Failed to set default version: ' + String(e));
		}
	}

	async function acceptInvitation(invArn: string) {
		try {
			await ram().send(new AcceptResourceShareInvitationCommand({ resourceShareInvitationArn: invArn }));
			toast.success('Invitation accepted');
			await loadData();
		} catch (e) {
			toast.error('Failed to accept invitation: ' + String(e));
		}
	}

	async function rejectInvitation(invArn: string) {
		try {
			await ram().send(new RejectResourceShareInvitationCommand({ resourceShareInvitationArn: invArn }));
			toast.success('Invitation rejected');
			await loadData();
		} catch (e) {
			toast.error('Failed to reject invitation: ' + String(e));
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Share2 class="w-7 h-7 text-cyan-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Resource Access Manager</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Share AWS resources across accounts and organizations</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-5 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-cyan-100 dark:bg-cyan-900/30 rounded-lg"><Share2 class="w-5 h-5 text-cyan-600 dark:text-cyan-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{shares.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Resource Shares</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{activeShares}</p><p class="text-sm text-gray-500 dark:text-gray-400">Active Shares</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Box class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{resources.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Shared Resources</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Users class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{principals.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Principals</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Bell class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{invitations.filter((i) => i.status === 'PENDING').length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Pending Invites</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['shares', 'Resource Shares'], ['resources', 'Resources'], ['principals', 'Principals'], ['permissions', 'Permissions'], ['invitations', 'Invitations']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; selectedPermission = null; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-cyan-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'shares'}
				{#if filteredShares.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No resource shares found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredShares as share}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Share2 class="w-5 h-5 text-cyan-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{share.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{share.resourceShareArn}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {share.status === 'ACTIVE' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{share.status}</span>
							</div>
						{/each}
					</div>
					{#if sharesNextToken}
						<button onclick={loadMoreShares} class="mt-4 w-full py-2 text-sm text-cyan-600 dark:text-cyan-400 hover:underline">Load more</button>
					{/if}
				{/if}
			{:else if activeTab === 'resources'}
				{#if filteredResources.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No shared resources found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredResources as res}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Box class="w-5 h-5 text-blue-500" />
								<div>
									<p class="font-medium text-sm text-gray-900 dark:text-white truncate max-w-lg">{res.arn}</p>
									<p class="text-xs text-gray-500 dark:text-gray-400">{res.type} · {res.status}</p>
								</div>
							</div>
						{/each}
					</div>
					{#if resourcesNextToken}
						<button onclick={loadMoreResources} class="mt-4 w-full py-2 text-sm text-cyan-600 dark:text-cyan-400 hover:underline">Load more</button>
					{/if}
				{/if}
			{:else if activeTab === 'principals'}
				{#if filteredPrincipals.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No principals found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPrincipals as principal}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Users class="w-5 h-5 text-purple-500" />
								<p class="text-sm text-gray-900 dark:text-white">{principal.id}</p>
							</div>
						{/each}
					</div>
					{#if principalsNextToken}
						<button onclick={loadMorePrincipals} class="mt-4 w-full py-2 text-sm text-cyan-600 dark:text-cyan-400 hover:underline">Load more</button>
					{/if}
				{/if}
			{:else if activeTab === 'permissions'}
				{#if selectedPermission}
					<div class="mb-4">
						<button onclick={() => { selectedPermission = null; permissionVersions = []; }} class="text-sm text-cyan-600 dark:text-cyan-400 hover:underline">← Back to permissions</button>
						<h2 class="mt-2 text-lg font-semibold text-gray-900 dark:text-white">{selectedPermission.name}</h2>
						<p class="text-xs text-gray-500 dark:text-gray-400 mb-4">{selectedPermission.arn}</p>
						{#if loadingVersions}
							<div class="text-center py-4 text-gray-500 dark:text-gray-400">Loading versions...</div>
						{:else if permissionVersions.length === 0}
							<div class="text-center py-4 text-gray-500 dark:text-gray-400">No versions found</div>
						{:else}
							<div class="space-y-2">
								{#each permissionVersions as ver}
									<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
										<div class="flex items-center gap-3">
											<Key class="w-4 h-4 text-cyan-500" />
											<div>
												<!-- eslint-disable-next-line @typescript-eslint/no-explicit-any -->
												<p class="text-sm font-medium text-gray-900 dark:text-white">Version {(ver as any).version ?? ver.version}</p>
												<!-- eslint-disable-next-line @typescript-eslint/no-explicit-any -->
												{#if (ver as any).defaultVersion}
													<span class="text-xs text-green-600 dark:text-green-400">Default</span>
												{/if}
											</div>
										</div>
										<!-- eslint-disable-next-line @typescript-eslint/no-explicit-any -->
										{#if !(ver as any).defaultVersion && selectedPermission?.arn}
											<button
												onclick={() => setDefaultVersion(selectedPermission!.arn!, Number((ver as any).version ?? ver.version))}
												class="text-xs px-3 py-1 rounded-lg bg-cyan-600 text-white hover:bg-cyan-700">
												Set as Default
											</button>
										{/if}
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{:else if filteredPermissions.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No permissions found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPermissions as perm}
							<button onclick={() => selectPermission(perm)} class="w-full text-left flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700">
								<div class="flex items-center gap-3">
									<Key class="w-5 h-5 text-cyan-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{perm.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{perm.resourceType} · v{perm.version}</p>
									</div>
								</div>
								<span class="text-xs text-gray-400">Manage versions →</span>
							</button>
						{/each}
					</div>
					{#if permissionsNextToken}
						<button onclick={loadMorePermissions} class="mt-4 w-full py-2 text-sm text-cyan-600 dark:text-cyan-400 hover:underline">Load more</button>
					{/if}
				{/if}
			{:else if activeTab === 'invitations'}
				{#if filteredInvitations.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No invitations found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredInvitations as inv}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Bell class="w-5 h-5 text-orange-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{inv.resourceShareName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">From: {inv.senderAccountId} · {inv.status}</p>
									</div>
								</div>
								{#if inv.status === 'PENDING' && inv.resourceShareInvitationArn}
									<div class="flex gap-2">
										<button
											onclick={() => acceptInvitation(inv.resourceShareInvitationArn!)}
											class="text-xs px-3 py-1 rounded-lg bg-green-600 text-white hover:bg-green-700">
											Accept
										</button>
										<button
											onclick={() => rejectInvitation(inv.resourceShareInvitationArn!)}
											class="text-xs px-3 py-1 rounded-lg bg-red-600 text-white hover:bg-red-700">
											Reject
										</button>
									</div>
								{:else}
									<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{inv.status}</span>
								{/if}
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
