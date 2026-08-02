<script lang="ts">
	// Static IPs -- family H (6 ops). StaticIp is one of the 4 ResourceType
	// kinds with NO Tags field at all (shared.ts's TAGS_NOT_SUPPORTED) -- this
	// panel deliberately never renders tag UI for it.
	import {
		GetStaticIpsCommand,
		AllocateStaticIpCommand,
		AttachStaticIpCommand,
		DetachStaticIpCommand,
		ReleaseStaticIpCommand,
		type StaticIp,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { describeError } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let staticIps = $state<StaticIp[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchStaticIps(reset: boolean): Promise<void> {
		const resp = await client().send(new GetStaticIpsCommand({ pageToken: reset ? undefined : nextToken }));
		staticIps = reset ? (resp.staticIps ?? []) : [...staticIps, ...(resp.staticIps ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchStaticIps(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchStaticIps(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		staticIps.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (s.name ?? '').toLowerCase().includes(q) || (s.ipAddress ?? '').toLowerCase().includes(q);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(new AllocateStaticIpCommand({ staticIpName: createName }));
			toast.success('Static IP allocated');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Attach/detach ---------------------------

	let attachModal = $state<Modal | null>(null);
	let attachTarget = $state<StaticIp | null>(null);
	let attachInstanceName = $state('');
	let attachBusy = $state(false);
	let attachError = $state<string | null>(null);

	function openAttach(s: StaticIp): void {
		attachTarget = s;
		attachInstanceName = '';
		attachError = null;
		attachModal?.open();
	}

	async function submitAttach(): Promise<void> {
		if (!attachTarget?.name) return;
		attachBusy = true;
		attachError = null;
		try {
			await client().send(
				new AttachStaticIpCommand({ staticIpName: attachTarget.name, instanceName: attachInstanceName })
			);
			toast.success('Static IP attached');
			attachModal?.close();
			await refresh();
		} catch (e) {
			attachError = describeError(e);
		} finally {
			attachBusy = false;
		}
	}

	async function detach(s: StaticIp): Promise<void> {
		if (!s.name) return;
		try {
			await client().send(new DetachStaticIpCommand({ staticIpName: s.name }));
			toast.success('Static IP detached');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Release ---------------------------------

	async function release(s: StaticIp): Promise<void> {
		if (!s.name) return;
		const confirmed = await confirmDestructive({
			title: 'Release static IP',
			message: `Release static IP ${s.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new ReleaseStaticIpCommand({ staticIpName: s.name }));
			toast.success('Static IP released');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<StaticIp | null>(null);

	function openDetail(s: StaticIp): void {
		viewed = s;
		detailModal?.open();
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
		Allocate static IP
	</button>
</div>

{#snippet createdCell(s: StaticIp)}
	{formatDate(s.createdAt)}
{/snippet}
{#snippet attachedCell(s: StaticIp)}
	{s.isAttached ? (s.attachedTo ?? 'attached') : 'unattached'}
{/snippet}
{#snippet rowActions(s: StaticIp)}
	<div class="flex items-center gap-3 justify-end">
		{#if s.isAttached}
			<button onclick={() => detach(s)} class="text-amber-600 hover:underline text-sm">Detach</button>
		{:else}
			<button onclick={() => openAttach(s)} class="text-emerald-600 hover:underline text-sm">Attach</button>
		{/if}
		<button onclick={() => openDetail(s)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => release(s)} class="text-red-600 hover:underline text-sm">Release</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(s) => s.name ?? ''}
	columns={defineColumns<StaticIp>([
		{ key: 'name', label: 'Name' },
		{ key: 'ipAddress', label: 'IP address' },
		{ key: 'attachedTo', label: 'Attached to', render: attachedCell },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No static IPs found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Allocate static IP">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-sip-name">
				Name
				<input id="ls-sip-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Allocate</button>
	{/snippet}
</Modal>

<Modal bind:this={attachModal} title="Attach {attachTarget?.name ?? ''} to an instance">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-sip-attach-instance">
				Instance name
				<input id="ls-sip-attach-instance" bind:value={attachInstanceName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if attachError}<p class="text-sm text-red-600 dark:text-red-400">{attachError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => attachModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitAttach} disabled={attachBusy || !attachInstanceName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Attach</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Static IP {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Support code</dt><dd>{viewed.supportCode ?? '—'}</dd></div>
					<div><dt class="text-slate-500">IP address</dt><dd>{viewed.ipAddress ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Attached to</dt><dd>{viewed.attachedTo || '(none)'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>
				<p class="text-xs text-slate-500">
					StaticIp is one of four Lightsail resource kinds with no Tags field on the wire -- tagging is
					not offered for this resource.
				</p>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
