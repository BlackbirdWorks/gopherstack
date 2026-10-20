<script lang="ts">
	import { toast } from 'svelte-sonner';
	import { Plus, Trash2, Power, Download, Copy, TriangleAlert } from 'lucide-svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY, attempt, copyText, downloadText } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		createKey,
		deleteKey,
		keyAction,
		keyStatus,
		listKeys,
		type AccountInfo,
		type CreatedKey,
		type KeyInfo
	} from '#lib/gcpiam.ts';

	type Props = { project: string; accounts: AccountInfo[]; selected: string };

	let { project, accounts, selected = $bindable() }: Props = $props();

	const cell = 'px-4 py-3';
	let keys = $state<KeyInfo[]>([]);
	let loading = $state(false);
	let busy = $state('');
	let created = $state<CreatedKey | null>(null);
	let secretModal = $state<Modal | null>(null);

	const when = (iso: string) => (iso ? new Date(iso).toLocaleString() : '—');
	const statusClass = (s: string) =>
		s === 'active' ? 'text-green-700 dark:text-green-400' : s === 'disabled' ? 'text-amber-700 dark:text-amber-400' : 'text-red-700 dark:text-red-400';

	async function load() {
		if (!selected) {
			keys = [];
			return;
		}
		loading = true;
		try {
			keys = await listKeys(project, selected);
		} catch (e) {
			toast.error(`Failed to load keys: ${describeGcpError(e)}`);
		} finally {
			loading = false;
		}
	}

	async function create() {
		busy = 'create';
		let result: CreatedKey | null = null;
		const ok = await attempt(async () => (result = await createKey(project, selected)), 'Created key', 'Failed to create key');
		busy = '';
		if (!ok || !result) return;
		created = result;
		secretModal?.open();
		await load();
	}

	function dismissSecret() {
		created = null;
	}

	async function toggle(k: KeyInfo) {
		busy = `toggle-${k.id}`;
		const action = k.disabled ? 'enable' : 'disable';
		const ok = await attempt(() => keyAction(project, selected, k.id, action), `Key ${action}d`, `Failed to ${action} key`);
		busy = '';
		if (ok) await load();
	}

	async function remove(k: KeyInfo) {
		const message = `Delete key "${k.id}"? Anything using it will stop authenticating.`;
		if (!(await confirmDestructive({ title: 'Delete key', message }))) return;
		busy = `del-${k.id}`;
		const ok = await attempt(() => deleteKey(project, selected, k.id), 'Deleted key', 'Failed to delete key');
		busy = '';
		if (ok) await load();
	}

	$effect(() => {
		void selected;
		void load();
	});
</script>

<div class="space-y-4" data-testid="iam-keys">
	{#if accounts.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400">Create a service account first to manage its keys.</p>
	{:else}
		<div class="flex flex-wrap items-end justify-between gap-3">
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Service account</span>
				<select class="{GCP_INPUT} min-w-[22rem] font-mono text-xs" bind:value={selected} aria-label="Service account" data-testid="iam-key-account">
					{#each accounts as a (a.uniqueId)}<option value={a.email}>{a.email}</option>{/each}
				</select>
			</label>
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'create' || !selected} onclick={create} data-testid="iam-create-key">
				<Plus class="h-4 w-4" /> {busy === 'create' ? 'Creating…' : 'Create key'}
			</button>
		</div>

		{#if loading && keys.length === 0}
			<p class="py-8 text-center text-gray-500 dark:text-gray-400">Loading keys…</p>
		{:else if keys.length === 0}
			<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="iam-no-keys">No keys for this account</p>
		{:else}
			<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="iam-key-list">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Key ID</th><th class={cell}>Type</th><th class={cell}>Algorithm</th><th class={cell}>Status</th><th class={cell}>Valid from</th><th class={cell}>Valid until</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each keys as k (k.name)}
							{@const st = keyStatus(k)}
							<tr data-testid="iam-key-row">
								<td class="{cell} font-mono text-xs break-all">{k.id}</td>
								<td class="{cell} text-xs" data-testid="iam-key-type">{k.keyType === 'USER_MANAGED' ? 'User managed' : 'System managed'}</td>
								<td class="{cell} text-xs">{k.algorithm || '—'}</td>
								<td class="{cell} text-xs font-medium {statusClass(st)}" data-testid="iam-key-status">{st}</td>
								<td class="{cell} text-xs whitespace-nowrap">{when(k.validAfter)}</td>
								<td class="{cell} text-xs whitespace-nowrap">{when(k.validBefore)}</td>
								<td class="{cell} text-right whitespace-nowrap space-x-1">
									{#if k.keyType === 'USER_MANAGED'}
										<button class={GCP_MINI} disabled={busy === `toggle-${k.id}`} onclick={() => toggle(k)} data-testid="iam-key-toggle"><Power class="inline h-3.5 w-3.5" /> {k.disabled ? 'Enable' : 'Disable'}</button>
										<button class="{GCP_MINI} text-red-600" disabled={busy === `del-${k.id}`} onclick={() => remove(k)} data-testid="iam-key-delete" aria-label="Delete key {k.id}"><Trash2 class="inline h-3.5 w-3.5" /></button>
									{:else}
										<span class="text-xs text-gray-400">managed by IAM</span>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<Modal bind:this={secretModal} title="Service account key created" titleId="iam-secret-title" onClose={dismissSecret}>
	{#snippet children()}
		{#if created}
			<div class="space-y-3" data-testid="iam-key-secret">
				<div class="flex gap-2 rounded-md border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900 dark:border-amber-700 dark:bg-amber-950/40 dark:text-amber-200" role="alert" data-testid="iam-key-warning">
					<TriangleAlert class="mt-0.5 h-4 w-4 shrink-0" />
					<p><strong>This is the only time the private key is shown.</strong> It cannot be retrieved again. Download or copy it now and store it securely.</p>
				</div>
				<p class="font-mono text-xs break-all text-gray-500 dark:text-gray-400">Key ID {created.key.id}</p>
				<pre class="max-h-64 overflow-auto whitespace-pre-wrap break-all rounded-md bg-gray-50 dark:bg-slate-900 p-2 font-mono text-xs" data-testid="iam-key-json">{created.file}</pre>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		{#if created}
			{@const file = created}
			<button type="button" class={GCP_GHOST} onclick={() => copyText(file.file)} data-testid="iam-key-copy"><Copy class="inline h-4 w-4" /> Copy</button>
			<button type="button" class={GCP_PRIMARY} onclick={() => downloadText(file.filename, file.file)} data-testid="iam-key-download"><Download class="inline h-4 w-4" /> Download JSON</button>
		{/if}
		<button type="button" class={GCP_GHOST} onclick={() => secretModal?.close()} data-testid="iam-key-done">Done</button>
	{/snippet}
</Modal>
