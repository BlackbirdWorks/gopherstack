<script lang="ts">
	// Certificates -- family V (3 ops). CDN/distribution-facing certificates,
	// distinct from the load-balancer TLS certificate family (M, folded into
	// LoadBalancersPanel) and from real ACM. CertificateProvider has exactly
	// one SDK value (LetsEncrypt).
	import {
		GetCertificatesCommand,
		CreateCertificateCommand,
		DeleteCertificateCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type CertificateSummary,
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
	import TagEditor from './TagEditor.svelte';
	import { describeError, tagsToRecord } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let certificates = $state<CertificateSummary[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchCertificates(reset: boolean): Promise<void> {
		const resp = await client().send(new GetCertificatesCommand({ pageToken: reset ? undefined : nextToken }));
		certificates = reset ? (resp.certificates ?? []) : [...certificates, ...(resp.certificates ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchCertificates(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchCertificates(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		certificates.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (c.certificateName ?? '').toLowerCase().includes(q) || (c.domainName ?? '').toLowerCase().includes(q);
		})
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let createDomain = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createName = '';
		createDomain = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(new CreateCertificateCommand({ certificateName: createName, domainName: createDomain }));
			toast.success('Certificate requested');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteCertificate(c: CertificateSummary): Promise<void> {
		if (!c.certificateName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete certificate',
			message: `Delete certificate ${c.certificateName}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCertificateCommand({ certificateName: c.certificateName }));
			toast.success('Certificate deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<CertificateSummary | null>(null);

	function openDetail(c: CertificateSummary): void {
		viewed = c;
		detailModal?.open();
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.certificateName) return;
		await client().send(new TagResourceCommand({ resourceName: viewed.certificateName, tags: [{ key, value }] }));
		viewed = { ...viewed, tags: [...(viewed.tags ?? []).filter((t) => t.key !== key), { key, value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.certificateName) return;
		await client().send(new UntagResourceCommand({ resourceName: viewed.certificateName, tagKeys: [key] }));
		viewed = { ...viewed, tags: (viewed.tags ?? []).filter((t) => t.key !== key) };
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
		Request certificate
	</button>
</div>

{#snippet statusCell(c: CertificateSummary)}
	{c.certificateDetail?.status ?? '—'}
{/snippet}
{#snippet rowActions(c: CertificateSummary)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(c)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteCertificate(c)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(c) => c.certificateName ?? ''}
	columns={defineColumns<CertificateSummary>([
		{ key: 'certificateName', label: 'Name' },
		{ key: 'domainName', label: 'Domain' },
		{ key: 'status', label: 'Status', render: statusCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No certificates found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Request certificate">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-cert-name">
				Name
				<input id="ls-cert-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-cert-domain">
				Domain name
				<input id="ls-cert-domain" bind:value={createDomain} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName || !createDomain} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Request</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Certificate {viewed?.certificateName ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.certificateArn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Domain</dt><dd>{viewed.domainName ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Status</dt><dd>{viewed.certificateDetail?.status ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Issued</dt><dd>{formatDate(viewed.certificateDetail?.issuedAt)}</dd></div>
					<div><dt class="text-slate-500">Not after</dt><dd>{formatDate(viewed.certificateDetail?.notAfter)}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.certificateDetail?.createdAt)}</dd></div>
				</dl>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={tagsToRecord(viewed.tags)} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => detailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
