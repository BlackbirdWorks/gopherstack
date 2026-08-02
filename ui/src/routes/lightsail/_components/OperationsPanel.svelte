<script lang="ts">
	// Operations -- family Z (3 ops), the polling surface for the Operation
	// model nearly every mutating op returns (services/lightsail/PARITY.md
	// section 2). This is the central "recent operations" view for the whole
	// service: rather than discard each mutating call's returned Operation
	// records, every panel's own actions surface a toast, but the durable
	// record of what happened and its real NotStarted/Started ->
	// Succeeded/Failed status progression lives here and in
	// GetOperationsForResource (searchable by resource name below).
	import {
		GetOperationsCommand,
		GetOperationCommand,
		GetOperationsForResourceCommand,
		type Operation,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { describeError, operationStatusClass } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let operations = $state<Operation[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);
	let resourceFilter = $state('');

	async function fetchOperations(reset: boolean): Promise<void> {
		if (resourceFilter.trim()) {
			const resp = await client().send(
				new GetOperationsForResourceCommand({ resourceName: resourceFilter.trim(), pageToken: reset ? undefined : nextToken })
			);
			operations = reset ? (resp.operations ?? []) : [...operations, ...(resp.operations ?? [])];
			nextToken = resp.nextPageToken;
			return;
		}
		const resp = await client().send(new GetOperationsCommand({ pageToken: reset ? undefined : nextToken }));
		operations = reset ? (resp.operations ?? []) : [...operations, ...(resp.operations ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchOperations(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchOperations(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	async function applyResourceFilter(): Promise<void> {
		await refresh();
	}

	const filtered = $derived(
		operations.filter((o) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(o.id ?? '').toLowerCase().includes(q) ||
				(o.resourceName ?? '').toLowerCase().includes(q) ||
				(o.operationType ?? '').toLowerCase().includes(q) ||
				(o.status ?? '').toLowerCase().includes(q)
			);
		})
	);

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Operation | null>(null);

	async function openDetail(o: Operation): Promise<void> {
		viewed = o;
		detailModal?.open();
		if (o.id) {
			try {
				const resp = await client().send(new GetOperationCommand({ operationId: o.id }));
				viewed = resp.operation ?? o;
			} catch (e) {
				toast.error(describeError(e));
			}
		}
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="flex items-center gap-2">
	<input
		bind:value={resourceFilter}
		onchange={applyResourceFilter}
		placeholder="Filter by resource name (blank = all operations)"
		aria-label="Filter by resource name"
		class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 flex-1"
	/>
	<button onclick={applyResourceFilter} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600">
		Apply
	</button>
</div>

{#snippet statusCell(o: Operation)}
	<span class="text-xs px-2 py-1 rounded-full {operationStatusClass(o.status)}">{o.status ?? '—'}</span>
{/snippet}
{#snippet createdCell(o: Operation)}
	{formatDate(o.createdAt)}
{/snippet}
{#snippet rowActions(o: Operation)}
	<button onclick={() => openDetail(o)} class="text-blue-600 hover:underline text-sm">View</button>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(o) => o.id ?? ''}
	columns={defineColumns<Operation>([
		{ key: 'operationType', label: 'Type' },
		{ key: 'resourceName', label: 'Resource' },
		{ key: 'resourceType', label: 'Resource type' },
		{ key: 'status', label: 'Status', render: statusCell },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No operations found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={detailModal} title="Operation {viewed?.id ?? ''}">
	{#snippet children()}
		{#if viewed}
			<dl class="grid grid-cols-2 gap-2 text-sm">
				<div><dt class="text-slate-500">Type</dt><dd>{viewed.operationType ?? '—'}</dd></div>
				<div><dt class="text-slate-500">Status</dt><dd>{viewed.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500">Resource</dt><dd>{viewed.resourceName ?? '—'}</dd></div>
				<div><dt class="text-slate-500">Resource type</dt><dd>{viewed.resourceType ?? '—'}</dd></div>
				<div><dt class="text-slate-500">Terminal?</dt><dd>{viewed.isTerminal ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				<div><dt class="text-slate-500">Status changed</dt><dd>{formatDate(viewed.statusChangedAt)}</dd></div>
				{#if viewed.errorCode}
					<div><dt class="text-slate-500">Error</dt><dd>{viewed.errorCode}: {viewed.errorDetails ?? ''}</dd></div>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
