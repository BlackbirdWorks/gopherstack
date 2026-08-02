<script lang="ts">
	// Attachment detail modal -- view metadata, manage the (at most one)
	// routing policy label (family P, 3 ops), and edit tags. Split out of
	// AttachmentsPanel to keep that file's size down; owns its own `viewed`
	// copy of the Attachment exactly as the parent did before the split --
	// the list table has no Tags column, so edits here not reflecting back
	// into the list row is pre-existing behavior, not a regression.
	import {
		PutAttachmentRoutingPolicyLabelCommand,
		RemoveAttachmentRoutingPolicyLabelCommand,
		ListAttachmentRoutingPolicyAssociationsCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Attachment,
		type NetworkManagerClient
	} from '@aws-sdk/client-networkmanager';
	import { toast } from 'svelte-sonner';
	import { formatDate } from '$lib/format';
	import Modal from '$lib/components/Modal.svelte';
	import TagEditor from './TagEditor.svelte';
	import { describeError, taggableArn } from './shared';

	type Props = {
		client: () => NetworkManagerClient;
	};

	let { client }: Props = $props();

	let modal = $state<Modal | null>(null);
	let viewed = $state<Attachment | null>(null);
	let routingLabel = $state('');
	let routingLabelBusy = $state(false);
	let routingLabelError = $state<string | null>(null);
	let currentLabels = $state<string[]>([]);

	export async function open(a: Attachment): Promise<void> {
		viewed = a;
		routingLabel = '';
		routingLabelError = null;
		currentLabels = [];
		modal?.open();
		if (a.CoreNetworkId && a.AttachmentId) {
			try {
				const resp = await client().send(
					new ListAttachmentRoutingPolicyAssociationsCommand({
						CoreNetworkId: a.CoreNetworkId,
						AttachmentId: a.AttachmentId
					})
				);
				currentLabels = (resp.AttachmentRoutingPolicyAssociations ?? [])
					.map((s) => s.RoutingPolicyLabel)
					.filter((label): label is string => !!label);
			} catch {
				// Non-fatal: routing policy labels are a secondary feature of
				// the detail view.
			}
		}
	}

	async function putRoutingLabel(): Promise<void> {
		if (!viewed?.AttachmentId || !viewed.CoreNetworkId || !routingLabel.trim()) return;
		routingLabelBusy = true;
		routingLabelError = null;
		try {
			await client().send(
				new PutAttachmentRoutingPolicyLabelCommand({
					AttachmentId: viewed.AttachmentId,
					CoreNetworkId: viewed.CoreNetworkId,
					RoutingPolicyLabel: routingLabel.trim()
				})
			);
			toast.success('Routing policy label applied');
			currentLabels = [...currentLabels, routingLabel.trim()];
			routingLabel = '';
		} catch (e) {
			routingLabelError = describeError(e);
		} finally {
			routingLabelBusy = false;
		}
	}

	async function removeRoutingLabel(): Promise<void> {
		if (!viewed?.AttachmentId || !viewed.CoreNetworkId) return;
		try {
			await client().send(
				new RemoveAttachmentRoutingPolicyLabelCommand({
					AttachmentId: viewed.AttachmentId,
					CoreNetworkId: viewed.CoreNetworkId
				})
			);
			toast.success('Routing policy label removed');
			currentLabels = [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function addTag(key: string, value: string): Promise<void> {
		if (!viewed?.AttachmentId) return;
		const arn = taggableArn('attachment', viewed.AttachmentId);
		await client().send(new TagResourceCommand({ ResourceArn: arn, Tags: [{ Key: key, Value: value }] }));
		viewed = { ...viewed, Tags: [...(viewed.Tags ?? []).filter((t) => t.Key !== key), { Key: key, Value: value }] };
	}

	async function removeTag(key: string): Promise<void> {
		if (!viewed?.AttachmentId) return;
		const arn = taggableArn('attachment', viewed.AttachmentId);
		await client().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [key] }));
		viewed = { ...viewed, Tags: (viewed.Tags ?? []).filter((t) => t.Key !== key) };
	}
</script>

<Modal bind:this={modal} title="Attachment {viewed?.AttachmentId ?? ''}">
	{#snippet children()}
		{#if viewed}
			<div class="space-y-4 max-h-[70vh] overflow-y-auto pr-1">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Type</dt><dd>{viewed.AttachmentType ?? '—'}</dd></div>
					<div><dt class="text-slate-500">State</dt><dd>{viewed.State ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Core network</dt><dd>{viewed.CoreNetworkId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Segment</dt><dd>{viewed.SegmentName ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Owner account</dt><dd>{viewed.OwnerAccountId ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Created</dt><dd>{formatDate(viewed.CreatedAt)}</dd></div>
				</dl>
				{#if (viewed.LastModificationErrors ?? []).length > 0}
					<div class="rounded-lg border border-red-200 dark:border-red-800 bg-red-50 dark:bg-red-900/20 p-2 text-xs text-red-700 dark:text-red-300">
						{#each viewed.LastModificationErrors ?? [] as e (e.RequestId ?? e.Code)}
							<p>{e.Code}: {e.Message}</p>
						{/each}
					</div>
				{/if}
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Routing policy label</p>
					{#if currentLabels.length > 0}
						<p class="text-sm text-slate-600 dark:text-slate-300">Current: {currentLabels.join(', ')}</p>
						<button onclick={removeRoutingLabel} class="text-red-600 hover:underline text-xs">Remove label</button>
					{:else}
						<div class="flex gap-2">
							<input bind:value={routingLabel} placeholder="Label" class="flex-1 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
							<button onclick={putRoutingLabel} disabled={routingLabelBusy} class="px-3 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">Apply</button>
						</div>
					{/if}
					{#if routingLabelError}<p class="text-sm text-red-600 dark:text-red-400">{routingLabelError}</p>{/if}
				</div>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<TagEditor tags={viewed.Tags ?? []} onAdd={addTag} onRemove={removeTag} />
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => modal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
