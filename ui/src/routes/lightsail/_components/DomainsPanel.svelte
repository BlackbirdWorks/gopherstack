<script lang="ts">
	// Domains -- family U (7 ops). Domain is CONFIRMED a GLOBAL resource
	// (its own SDK doc-comment example ARN uses the literal region segment
	// "global" -- services/lightsail/PARITY.md 4.7/5.1), unlike Instance/
	// Disk/StaticIp/KeyPair (regional) and Distribution (modeled global but
	// reports us-east-1).
	import {
		GetDomainsCommand,
		CreateDomainCommand,
		DeleteDomainCommand,
		CreateDomainEntryCommand,
		UpdateDomainEntryCommand,
		DeleteDomainEntryCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Domain,
		type DomainEntry,
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

	let domains = $state<Domain[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);

	async function fetchDomains(reset: boolean): Promise<void> {
		const resp = await client().send(new GetDomainsCommand({ pageToken: reset ? undefined : nextToken }));
		domains = reset ? (resp.domains ?? []) : [...domains, ...(resp.domains ?? [])];
		nextToken = resp.nextPageToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchDomains(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchDomains(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		domains.filter((d) => (d.name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
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
			await client().send(new CreateDomainCommand({ domainName: createName }));
			toast.success('Domain created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteDomain(d: Domain): Promise<void> {
		if (!d.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete domain',
			message: `Delete domain ${d.name}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDomainCommand({ domainName: d.name }));
			toast.success('Domain deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<Domain | null>(null);
	let newEntryName = $state('');
	let newEntryType = $state('A');
	let newEntryTarget = $state('');

	function openDetail(d: Domain): void {
		viewed = d;
		newEntryName = '';
		newEntryType = 'A';
		newEntryTarget = '';
		detailModal?.open();
	}

	async function refreshDomain(): Promise<void> {
		if (!viewed?.name) return;
		const resp = await client().send(new GetDomainsCommand({}));
		const found = (resp.domains ?? []).find((d) => d.name === viewed?.name);
		if (found) viewed = found;
	}

	async function addEntry(): Promise<void> {
		if (!viewed?.name || !newEntryName || !newEntryTarget) return;
		try {
			await client().send(
				new CreateDomainEntryCommand({
					domainName: viewed.name,
					domainEntry: { name: newEntryName, type: newEntryType, target: newEntryTarget }
				})
			);
			toast.success('DNS entry created');
			newEntryName = '';
			newEntryTarget = '';
			await refreshDomain();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function updateEntry(entry: DomainEntry, target: string): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new UpdateDomainEntryCommand({ domainName: viewed.name, domainEntry: { ...entry, target } }));
			toast.success('DNS entry updated');
			await refreshDomain();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function deleteEntry(entry: DomainEntry): Promise<void> {
		if (!viewed?.name) return;
		try {
			await client().send(new DeleteDomainEntryCommand({ domainName: viewed.name, domainEntry: entry }));
			toast.success('DNS entry deleted');
			await refreshDomain();
		} catch (e) {
			toast.error(describeError(e));
		}
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

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Create domain
	</button>
</div>

{#snippet createdCell(d: Domain)}
	{formatDate(d.createdAt)}
{/snippet}
{#snippet entriesCell(d: Domain)}
	{(d.domainEntries ?? []).length}
{/snippet}
{#snippet rowActions(d: Domain)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => openDetail(d)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteDomain(d)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(d) => d.name ?? ''}
	columns={defineColumns<Domain>([
		{ key: 'name', label: 'Name' },
		{ key: 'domainEntries', label: 'DNS entries', render: entriesCell },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No domains found"
/>
<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />

<Modal bind:this={createModal} title="Create domain">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-domain-name">
				Domain name
				<input id="ls-domain-name" bind:value={createName} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createName} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Create</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Domain {viewed?.name ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.createdAt)}</dd></div>
				</dl>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">DNS entries</p>
					<ul class="text-sm space-y-1">
						{#each viewed.domainEntries ?? [] as entry (entry.id)}
							<li class="flex items-center justify-between gap-2">
								<span class="truncate">{entry.type} {entry.name} -&gt; {entry.target}</span>
								<span class="flex gap-2 shrink-0">
									<button
										onclick={() => updateEntry(entry, prompt('New target', entry.target ?? '') ?? entry.target ?? '')}
										class="text-blue-600 hover:underline text-xs"
									>
										Edit
									</button>
									<button onclick={() => deleteEntry(entry)} class="text-red-600 hover:underline text-xs">Delete</button>
								</span>
							</li>
						{:else}
							<li class="text-slate-500">No DNS entries</li>
						{/each}
					</ul>
					<div class="flex items-center gap-2">
						<input bind:value={newEntryName} placeholder="Name" aria-label="New entry name" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700" />
						<select bind:value={newEntryType} aria-label="New entry type" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700">
							<option value="A">A</option>
							<option value="AAAA">AAAA</option>
							<option value="CNAME">CNAME</option>
							<option value="MX">MX</option>
							<option value="TXT">TXT</option>
							<option value="SRV">SRV</option>
							<option value="NS">NS</option>
							<option value="SOA">SOA</option>
						</select>
						<input bind:value={newEntryTarget} placeholder="Target" aria-label="New entry target" class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 flex-1" />
						<button onclick={addEntry} class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700">Add</button>
					</div>
				</div>

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
