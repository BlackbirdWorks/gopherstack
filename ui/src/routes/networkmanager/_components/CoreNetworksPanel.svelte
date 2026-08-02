<script lang="ts">
	// Core Networks -- the Cloud WAN root resource (services/networkmanager/
	// PARITY.md family L, 5 ops) plus its versioned-policy + change-set
	// lifecycle (family M, 8 ops) and prefix-list associations (family N, 3
	// ops), all surfaced from one detail view since they only make sense
	// scoped to a single core network. The policy lifecycle is real and
	// fully working in this emulator: PutCoreNetworkPolicy creates a new
	// immutable version, ExecuteCoreNetworkChangeSet promotes it to LIVE,
	// and "you can't delete the current LIVE policy" is an enforced
	// invariant -- see services/networkmanager/PARITY.md. GetCoreNetworkChangeSet's
	// diff is honestly empty (no policy-JSON differ in this pass).
	import {
		ListCoreNetworksCommand,
		CreateCoreNetworkCommand,
		UpdateCoreNetworkCommand,
		DeleteCoreNetworkCommand,
		GetCoreNetworkCommand,
		GetCoreNetworkPolicyCommand,
		PutCoreNetworkPolicyCommand,
		ListCoreNetworkPolicyVersionsCommand,
		DeleteCoreNetworkPolicyVersionCommand,
		RestoreCoreNetworkPolicyVersionCommand,
		GetCoreNetworkChangeSetCommand,
		ExecuteCoreNetworkChangeSetCommand,
		ListCoreNetworkPrefixListAssociationsCommand,
		CreateCoreNetworkPrefixListAssociationCommand,
		DeleteCoreNetworkPrefixListAssociationCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type CoreNetworkSummary,
		type CoreNetwork,
		type CoreNetworkPolicyVersion,
		type PrefixListAssociation,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let networks = $state<CoreNetworkSummary[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchNetworks(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListCoreNetworksCommand({ MaxResults: 50, NextToken: reset ? undefined : nextToken })
		);
		networks = reset ? (resp.CoreNetworks ?? []) : [...networks, ...(resp.CoreNetworks ?? [])];
		nextToken = resp.NextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchNetworks(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchNetworks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		networks.filter((n) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(n.CoreNetworkId ?? '').toLowerCase().includes(q) || (n.Description ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createGlobalNetworkId = $state('');
	let createDescription = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createGlobalNetworkId = '';
		createDescription = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		if (!createGlobalNetworkId.trim()) {
			createError = 'Global network ID is required.';
			return;
		}
		createBusy = true;
		createError = null;
		try {
			await client().send(
				new CreateCoreNetworkCommand({
					GlobalNetworkId: createGlobalNetworkId.trim(),
					Description: createDescription || undefined
				})
			);
			toast.success('Core network created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteNetwork(n: CoreNetworkSummary): Promise<void> {
		if (!n.CoreNetworkId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete core network',
			message: `Delete core network ${n.CoreNetworkId}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCoreNetworkCommand({ CoreNetworkId: n.CoreNetworkId }));
			toast.success('Core network deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<CoreNetwork | null>(null);
	let editDescription = $state('');
	let detailBusy = $state(false);
	let detailError = $state<string | null>(null);

	let policyVersions = $state<CoreNetworkPolicyVersion[]>([]);
	let liveVersionId = $state<number | undefined>();
	let latestVersionId = $state<number | undefined>();
	let policyDocument = $state('');
	let policyError = $state<string | null>(null);
	let policyBusy = $state(false);

	let changeSetSummary = $state<string | null>(null);

	let prefixListAssociations = $state<PrefixListAssociation[]>([]);
	let newPrefixListArn = $state('');
	let newPrefixListAlias = $state('');

	async function openDetail(n: CoreNetworkSummary): Promise<void> {
		if (!n.CoreNetworkId) return;
		detailError = null;
		policyError = null;
		changeSetSummary = null;
		detailModal?.open();
		try {
			const resp = await client().send(new GetCoreNetworkCommand({ CoreNetworkId: n.CoreNetworkId }));
			viewed = resp.CoreNetwork ?? { CoreNetworkId: n.CoreNetworkId };
			editDescription = viewed.Description ?? '';
		} catch (e) {
			detailError = describeError(e);
			viewed = { CoreNetworkId: n.CoreNetworkId };
		}
		await Promise.all([loadPolicyVersions(), loadPrefixListAssociations()]);
	}

	async function loadPolicyVersions(): Promise<void> {
		if (!viewed?.CoreNetworkId) return;
		try {
			const resp = await client().send(
				new ListCoreNetworkPolicyVersionsCommand({ CoreNetworkId: viewed.CoreNetworkId, MaxResults: 50 })
			);
			policyVersions = (resp.CoreNetworkPolicyVersions ?? []).toSorted(
				(a, b) => (b.PolicyVersionId ?? 0) - (a.PolicyVersionId ?? 0)
			);
			liveVersionId = policyVersions.find((v) => v.Alias === 'LIVE')?.PolicyVersionId;
			latestVersionId = policyVersions.find((v) => v.Alias === 'LATEST')?.PolicyVersionId;
		} catch (e) {
			policyError = describeError(e);
		}
	}

	async function loadPrefixListAssociations(): Promise<void> {
		if (!viewed?.CoreNetworkId) return;
		try {
			const resp = await client().send(
				new ListCoreNetworkPrefixListAssociationsCommand({ CoreNetworkId: viewed.CoreNetworkId, MaxResults: 50 })
			);
			prefixListAssociations = resp.PrefixListAssociations ?? [];
		} catch {
			// Non-fatal for the main detail view -- surfaced only if the user
			// tries to add/remove an association.
		}
	}

	async function saveNetwork(): Promise<void> {
		if (!viewed?.CoreNetworkId) return;
		detailBusy = true;
		detailError = null;
		try {
			const resp = await client().send(
				new UpdateCoreNetworkCommand({ CoreNetworkId: viewed.CoreNetworkId, Description: editDescription || undefined })
			);
			viewed = resp.CoreNetwork ?? viewed;
			toast.success('Core network updated');
			await refresh();
		} catch (e) {
			detailError = describeError(e);
		} finally {
			detailBusy = false;
		}
	}

	async function putPolicy(): Promise<void> {
		if (!viewed?.CoreNetworkId || !policyDocument.trim()) {
			policyError = 'Policy document (JSON) is required.';
			return;
		}
		policyBusy = true;
		policyError = null;
		try {
			await client().send(
				new PutCoreNetworkPolicyCommand({ CoreNetworkId: viewed.CoreNetworkId, PolicyDocument: policyDocument })
			);
			toast.success('Policy version submitted');
			policyDocument = '';
			await loadPolicyVersions();
		} catch (e) {
			policyError = describeError(e);
		} finally {
			policyBusy = false;
		}
	}

	async function executeVersion(versionId: number | undefined): Promise<void> {
		if (!viewed?.CoreNetworkId || versionId === undefined) return;
		try {
			await client().send(new ExecuteCoreNetworkChangeSetCommand({ CoreNetworkId: viewed.CoreNetworkId, PolicyVersionId: versionId }));
			toast.success(`Executed change set for version ${versionId}`);
			await loadPolicyVersions();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function restoreVersion(versionId: number | undefined): Promise<void> {
		if (!viewed?.CoreNetworkId || versionId === undefined) return;
		try {
			await client().send(new RestoreCoreNetworkPolicyVersionCommand({ CoreNetworkId: viewed.CoreNetworkId, PolicyVersionId: versionId }));
			toast.success(`Restored version ${versionId} as a new version`);
			await loadPolicyVersions();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteVersion(versionId: number | undefined): Promise<void> {
		if (!viewed?.CoreNetworkId || versionId === undefined) return;
		const confirmed = await confirmDestructive(`Delete policy version ${versionId}? The current LIVE version cannot be deleted.`);
		if (!confirmed) return;
		try {
			await client().send(new DeleteCoreNetworkPolicyVersionCommand({ CoreNetworkId: viewed.CoreNetworkId, PolicyVersionId: versionId }));
			toast.success(`Deleted version ${versionId}`);
			await loadPolicyVersions();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function viewChangeSet(versionId: number | undefined): Promise<void> {
		if (!viewed?.CoreNetworkId || versionId === undefined) return;
		try {
			const resp = await client().send(new GetCoreNetworkChangeSetCommand({ CoreNetworkId: viewed.CoreNetworkId, PolicyVersionId: versionId }));
			const changes = resp.CoreNetworkChanges ?? [];
			changeSetSummary =
				changes.length === 0
					? `No structural diff for version ${versionId} -- this emulator does not run a policy-JSON differ, so the change set is honestly empty rather than a fabricated diff.`
					: `${changes.length} change(s) for version ${versionId}.`;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addPrefixListAssociation(): Promise<void> {
		if (!viewed?.CoreNetworkId || !newPrefixListArn.trim()) return;
		try {
			await client().send(
				new CreateCoreNetworkPrefixListAssociationCommand({
					CoreNetworkId: viewed.CoreNetworkId,
					PrefixListArn: newPrefixListArn.trim(),
					PrefixListAlias: newPrefixListAlias.trim() || newPrefixListArn.trim()
				})
			);
			toast.success('Prefix list associated');
			newPrefixListArn = '';
			newPrefixListAlias = '';
			await loadPrefixListAssociations();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removePrefixListAssociation(p: PrefixListAssociation): Promise<void> {
		if (!viewed?.CoreNetworkId || !p.PrefixListArn) return;
		try {
			await client().send(
				new DeleteCoreNetworkPrefixListAssociationCommand({ CoreNetworkId: viewed.CoreNetworkId, PrefixListArn: p.PrefixListArn })
			);
			toast.success('Prefix list association removed');
			await loadPrefixListAssociations();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.CoreNetworkId) return;
		const arn = taggableArn('core-network', viewed.CoreNetworkId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.CoreNetworkId) return;
		const arn = taggableArn('core-network', viewed.CoreNetworkId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create core network
	</button>
</div>

{#snippet stateCell(n: CoreNetworkSummary)}
	<span class="text-xs px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400">{n.State ?? '—'}</span>
{/snippet}
{#snippet rowActions(n: CoreNetworkSummary)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(n)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteNetwork(n)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(n) => n.CoreNetworkId ?? ''}
	columns={defineColumns<CoreNetworkSummary>([
		{ key: 'CoreNetworkId', label: 'ID' },
		{ key: 'GlobalNetworkId', label: 'Global network' },
		{ key: 'Description', label: 'Description' },
		{ key: 'State', label: 'State', render: stateCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No core networks found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create core network">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="nm-cn-gn">
				Global network ID *
				<input id="nm-cn-gn" bind:value={createGlobalNetworkId} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="nm-cn-desc">
				Description
				<input id="nm-cn-desc" bind:value={createDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Core network {viewed?.CoreNetworkId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				{#if detailError}<p class="text-sm text-red-600 dark:text-red-400">{detailError}</p>{/if}
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.CoreNetworkArn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Global network</dt><dd>{viewed.GlobalNetworkId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Segments</dt><dd>{(viewed.Segments ?? []).map((s) => s.Name).join(', ') || '—'}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<label class="flex flex-col gap-1 text-sm" for="nm-cn-edit-desc">
						Description
						<input id="nm-cn-edit-desc" bind:value={editDescription} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</label>
					<button onclick={saveNetwork} disabled={detailBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Core network policy</p>
					<label class="flex flex-col gap-1 text-sm" for="nm-cn-policy-doc">
						New policy document (JSON)
						<textarea id="nm-cn-policy-doc" bind:value={policyDocument} rows="4" class="px-2 py-1 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
					</label>
					<button onclick={putPolicy} disabled={policyBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Submit new version</button>
					{#if policyError}<p class="text-sm text-red-600 dark:text-red-400">{policyError}</p>{/if}
					{#if changeSetSummary}<p class="text-xs text-amber-600 dark:text-amber-400">{changeSetSummary}</p>{/if}

					{#if policyVersions.length === 0}
						<p class="text-sm text-slate-500 dark:text-slate-400">No policy versions yet.</p>
					{:else}
						<table class="w-full text-sm">
							<thead>
								<tr class="border-b border-gray-200 dark:border-gray-700 text-left text-gray-500 dark:text-gray-400">
									<th class="px-2 py-1 font-medium">Version</th>
									<th class="px-2 py-1 font-medium">Alias</th>
									<th class="px-2 py-1 font-medium">Change set state</th>
									<th class="px-2 py-1 font-medium"></th>
								</tr>
							</thead>
							<tbody>
								{#each policyVersions as v (v.PolicyVersionId)}
									<tr class="border-b border-gray-100 dark:border-gray-800">
										<td class="px-2 py-1">{v.PolicyVersionId}</td>
										<td class="px-2 py-1">
											{#if v.PolicyVersionId === liveVersionId}
												<span class="text-xs px-2 py-0.5 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">LIVE</span>
											{/if}
											{#if v.PolicyVersionId === latestVersionId}
												<span class="text-xs px-2 py-0.5 rounded-full bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400">LATEST</span>
											{/if}
										</td>
										<td class="px-2 py-1">{v.ChangeSetState ?? '—'}</td>
										<td class="px-2 py-1 text-right space-x-2 whitespace-nowrap">
											<button onclick={() => viewChangeSet(v.PolicyVersionId)} class="text-blue-600 hover:underline text-xs">Diff</button>
											<button onclick={() => executeVersion(v.PolicyVersionId)} class="text-emerald-600 hover:underline text-xs">Execute</button>
											<button onclick={() => restoreVersion(v.PolicyVersionId)} class="text-slate-600 dark:text-slate-300 hover:underline text-xs">Restore</button>
											<button onclick={() => deleteVersion(v.PolicyVersionId)} class="text-red-600 hover:underline text-xs">Delete</button>
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Prefix list associations</p>
					{#if prefixListAssociations.length === 0}
						<p class="text-sm text-slate-500 dark:text-slate-400">No prefix list associations.</p>
					{:else}
						<ul class="space-y-1 text-sm">
							{#each prefixListAssociations as p (p.PrefixListArn)}
								<li class="flex items-center justify-between gap-2">
									<span class="break-all">{p.PrefixListAlias ?? p.PrefixListArn}</span>
									<button onclick={() => removePrefixListAssociation(p)} class="text-red-600 hover:underline text-xs">Remove</button>
								</li>
							{/each}
						</ul>
					{/if}
					<div class="flex gap-2">
						<input bind:value={newPrefixListArn} placeholder="Prefix list ARN" class="flex-1 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input bind:value={newPrefixListAlias} placeholder="Alias (optional)" class="flex-1 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<button onclick={addPrefixListAssociation} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Add</button>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={viewed.Tags ?? []} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
