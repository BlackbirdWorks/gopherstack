<script lang="ts">
	// Contact methods -- family W's other half (CreateContactMethod,
	// DeleteContactMethod, GetContactMethods, SendContactMethodVerification).
	// Keyed by Protocol -- there is no separate Name concept, and
	// ContactProtocol has exactly 2 values (Email/SMS). Verification is never
	// real email/SMS delivery in this emulator.
	import {
		GetContactMethodsCommand,
		CreateContactMethodCommand,
		DeleteContactMethodCommand,
		SendContactMethodVerificationCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type ContactMethod,
		type LightsailClient
	} from '@aws-sdk/client-lightsail';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, tagsToRecord } from './shared';

	type Props = {
		client: () => LightsailClient;
		searchQuery: string;
	};

	let { client, searchQuery }: Props = $props();

	let contactMethods = $state<ContactMethod[]>([]);
	let loading = $state(false);
	let error = $state<string | null>(null);

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			const resp = await client().send(new GetContactMethodsCommand({}));
			contactMethods = resp.contactMethods ?? [];
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		contactMethods.filter((c) => (c.contactEndpoint ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// ------------------------------ Create ---------------------------------

	let createModal = $state<Modal | null>(null);
	let createProtocol = $state<'Email' | 'SMS'>('Email');
	let createEndpoint = $state('');
	let createBusy = $state(false);
	let createError = $state<string | null>(null);

	function openCreate(): void {
		createProtocol = 'Email';
		createEndpoint = '';
		createError = null;
		createModal?.open();
	}

	async function submitCreate(): Promise<void> {
		createBusy = true;
		createError = null;
		try {
			await client().send(new CreateContactMethodCommand({ protocol: createProtocol, contactEndpoint: createEndpoint }));
			toast.success('Contact method created');
			createModal?.close();
			await refresh();
		} catch (e) {
			createError = describeError(e);
		} finally {
			createBusy = false;
		}
	}

	// ------------------------------ Delete ----------------------------------

	async function deleteContactMethod(c: ContactMethod): Promise<void> {
		if (!c.protocol) return;
		const confirmed = await confirmDestructive({
			title: 'Delete contact method',
			message: `Delete the ${c.protocol} contact method? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteContactMethodCommand({ protocol: c.protocol }));
			toast.success('Contact method deleted');
			await refresh();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// SendContactMethodVerificationRequest.protocol is typed as accepting only
	// "Email" in the installed SDK (ContactMethodVerificationProtocol has
	// exactly one value) -- verified against the real TypeScript types, not
	// assumed. SMS contact methods have no verification-send op to call.
	async function sendVerification(c: ContactMethod): Promise<void> {
		if (c.protocol !== 'Email') {
			toast.error('SendContactMethodVerification only supports the Email protocol in this SDK');
			return;
		}
		try {
			await client().send(new SendContactMethodVerificationCommand({ protocol: 'Email' }));
			toast.success('Verification sent (simulated -- no real email is delivered by this emulator)');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ------------------------------ Detail ----------------------------------

	let detailModal = $state<Modal | null>(null);
	let viewed = $state<ContactMethod | null>(null);

	function openDetail(c: ContactMethod): void {
		viewed = c;
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

<p class="text-xs text-slate-500 dark:text-slate-400">
	Verification never sends a real email or SMS in this emulator -- SendContactMethodVerification is
	simulated bookkeeping only.
</p>

<div class="flex justify-end">
	<button onclick={openCreate} class="px-3 py-1.5 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700">
		Add contact method
	</button>
</div>

{#snippet createdCell(c: ContactMethod)}
	{formatDate(c.createdAt)}
{/snippet}
{#snippet rowActions(c: ContactMethod)}
	<div class="flex items-center gap-3 justify-end">
		<button onclick={() => sendVerification(c)} class="text-emerald-600 hover:underline text-sm">Verify</button>
		<button onclick={() => openDetail(c)} class="text-blue-600 hover:underline text-sm">View</button>
		<button onclick={() => deleteContactMethod(c)} class="text-red-600 hover:underline text-sm">Delete</button>
	</div>
{/snippet}
<DataTable
	rows={filtered}
	rowKey={(c) => c.protocol ?? ''}
	columns={defineColumns<ContactMethod>([
		{ key: 'protocol', label: 'Protocol' },
		{ key: 'contactEndpoint', label: 'Endpoint' },
		{ key: 'status', label: 'Status' },
		{ key: 'createdAt', label: 'Created', render: createdCell },
		{ key: 'actions', label: '', render: rowActions }
	])}
	{loading}
	emptyMessage="No contact methods found"
/>

<Modal bind:this={createModal} title="Add contact method">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm" for="ls-cm-protocol">
				Protocol
				<select id="ls-cm-protocol" bind:value={createProtocol} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700">
					<option value="Email">Email</option>
					<option value="SMS">SMS</option>
				</select>
			</label>
			<label class="flex flex-col gap-1 text-sm" for="ls-cm-endpoint">
				Endpoint (email address or phone number)
				<input id="ls-cm-endpoint" bind:value={createEndpoint} class="px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if createError}<p class="text-sm text-red-600 dark:text-red-400">{createError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreate} disabled={createBusy || !createEndpoint} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50">Add</button>
	{/snippet}
</Modal>

<Modal bind:this={detailModal} title="Contact method {viewed?.protocol ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">ARN</dt><dd class="break-all">{viewed.arn ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Endpoint</dt><dd>{viewed.contactEndpoint ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Status</dt><dd>{viewed.status ?? '—'}</dd></div>
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
