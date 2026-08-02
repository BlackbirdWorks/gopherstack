<script lang="ts">
	// Key pairs -- family G (6 ops). Private key material
	// (privateKeyBase64/publicKeyBase64) is returned exactly once by
	// CreateKeyPair/ImportKeyPair/DownloadDefaultKeyPair and never stored by
	// the backend -- this panel shows it inline in the create-result modal
	// and never tries to fetch it again afterward.
	import {
		GetKeyPairsCommand,
		CreateKeyPairCommand,
		ImportKeyPairCommand,
		DeleteKeyPairCommand,
		DownloadDefaultKeyPairCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type KeyPair,
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

	let keyPairs = $state<KeyPair[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchKeyPairs(reset: boolean): Promise<void> {
		const resp = await client().send(new GetKeyPairsCommand({ pageToken: reset ? undefined : nextToken }));
		keyPairs = reset ? (resp.keyPairs ?? []) : [...keyPairs, ...(resp.keyPairs ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchKeyPairs(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchKeyPairs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		keyPairs.filter((k) => (k.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ------------------------------ Create/import ---------------------------

	let createModal = $state<Modal | null>(null);
	let createName = $state('');
	let importPublicKey = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);
	let createdKeyMaterial = $state<{ publicKeyBase64?: string; privateKeyBase64?: string } | null>(null);

	function openCreate(): void {
		createName = '';
		importPublicKey = '';
		createError = null;
		createdKeyMaterial = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			if (importPublicKey.trim()) {
				await client().send(new ImportKeyPairCommand({ keyPairName: createName, publicKeyBase64: importPublicKey }));
				toast.success('Key pair imported');
				createModal?.close();
			} else {
				const resp = await client().send(new CreateKeyPairCommand({ keyPairName: createName }));
				createdKeyMaterial = { publicKeyBase64: resp.publicKeyBase64, privateKeyBase64: resp.privateKeyBase64 };
				toast.success('Key pair created -- save the private key shown below, it will not be shown again');
			}
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	let defaultKeyMaterial = $state<{ publicKeyBase64?: string; privateKeyBase64?: string } | null>(null);

	async function downloadDefault(): Promise<void> {
		try {
			const resp = await client().send(new DownloadDefaultKeyPairCommand({}));
			defaultKeyMaterial = { publicKeyBase64: resp.publicKeyBase64, privateKeyBase64: resp.privateKeyBase64 };
			createModal?.open();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteKeyPair(k: KeyPair): Promise<void> {
		if (!k.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete key pair',
			message: `Delete key pair ${k.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteKeyPairCommand({ keyPairName: k.name }));
			toast.success('Key pair deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<KeyPair | null>(null);

	function openDetail(k: KeyPair): void {
		viewed = k;
		detailModal?.open();
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new TagResourceCommand({ resourceName: viewed.name, tags: [{ key, value }] }));
		viewed = { ...viewed, tags: [...(viewed.tags ?? []).filter((t) => t.key !== key), { key, value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.name) return;
		await client().send(new UntagResourceCommand({ resourceName: viewed.name, tagKeys: [key] }));
		viewed = { ...viewed, tags: (viewed.tags ?? []).filter((t) => t.key !== key) };
	}
</script>

{#if error}
	<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
		<p class="font-medium">Failed to load data</p>
		<p>{error}</p>
	</div>
{/if}

<div class="flex justify-end gap-2">
	<button onclick={downloadDefault} class="px-3 py-1.5 text-sm rounded-lg border border-slate-300 dark:border-slate-600">
		Download default key pair
	</button>
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create / import key pair
	</button>
</div>

{#snippet createdCell(k: KeyPair)}
	{formatDate(k.createdAt)}
{/snippet}
{#snippet rowActions(k: KeyPair)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(k)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteKeyPair(k)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(k) => k.name ?? ''}
	columns={defineColumns<KeyPair>([
		{ key: 'name', label: 'Name' },
		{ key: 'fingerprint', label: 'Fingerprint' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No key pairs found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create or import key pair">
	{#snippet children()}
		<div class="space-y-3">
			{#if defaultKeyMaterial}
				<p class="text-sm font-medium">Default key pair material (shown once):</p>
				<p class="text-xs break-all">Public: {defaultKeyMaterial.publicKeyBase64}</p>
				<p class="text-xs break-all">Private: {defaultKeyMaterial.privateKeyBase64}</p>
			{:else if createdKeyMaterial}
				<p class="text-sm font-medium">Key material (shown once):</p>
				<p class="text-xs break-all">Public: {createdKeyMaterial.publicKeyBase64}</p>
				<p class="text-xs break-all">Private: {createdKeyMaterial.privateKeyBase64}</p>
			{:else}
				<label class="flex flex-col gap-1 text-sm" for="ls-kp-name">
					Name
					<input id="ls-kp-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</label>
				<label class="flex flex-col gap-1 text-sm" for="ls-kp-import">
					Public key (base64, optional -- leave empty to generate a new key pair)
					<textarea id="ls-kp-import" bind:value={importPublicKey} rows="3" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
				</label>
				{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if !defaultKeyMaterial && !createdKeyMaterial}
			<button type="button" onclick={submitCreate} disabled={createBusy || !createName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Save</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Key pair {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Fingerprint</dt><dd>{viewed.fingerprint ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
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
