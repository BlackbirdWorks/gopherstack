<script lang="ts">
	import { Plus, Trash2, KeyRound, Power, Undo2 } from 'lucide-svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY, attempt } from '#lib/gcp-action.ts';
	import {
		createAccount,
		deleteAccount,
		setAccountDisabled,
		undeleteAccount,
		validateAccountId,
		type AccountInfo
	} from '#lib/gcpiam.ts';

	type Props = {
		project: string;
		accounts: AccountInfo[];
		onChanged: () => Promise<void>;
		onOpenKeys: (email: string) => void;
	};

	let { project, accounts, onChanged, onOpenKeys }: Props = $props();

	const cell = 'px-4 py-3';
	let busy = $state('');
	let recentlyDeleted = $state<AccountInfo[]>([]);
	let undeleteId = $state('');

	let modal = $state<Modal | null>(null);
	let fId = $state('');
	let fName = $state('');
	let fDesc = $state('');
	let touched = $state(false);
	const idError = $derived(validateAccountId(fId.trim()));

	function openNew() {
		fId = '';
		fName = '';
		fDesc = '';
		touched = false;
		modal?.open();
	}

	async function submitNew(event: Event) {
		event.preventDefault();
		touched = true;
		if (idError) return;
		const id = fId.trim();
		busy = 'create';
		const ok = await attempt(
			() => createAccount(project, { id, displayName: fName, description: fDesc }),
			`Created service account ${id}`,
			`Failed to create ${id}`
		);
		busy = '';
		if (ok) {
			modal?.close('created');
			await onChanged();
		}
	}

	async function toggle(a: AccountInfo) {
		busy = `toggle-${a.email}`;
		const verb = a.disabled ? 'Enabled' : 'Disabled';
		const ok = await attempt(
			() => setAccountDisabled(project, a.email, !a.disabled),
			`${verb} ${a.email}`,
			`Failed to update ${a.email}`
		);
		busy = '';
		if (ok) await onChanged();
	}

	async function remove(a: AccountInfo) {
		const message = `Delete service account "${a.email}"? Its keys are removed; it can be undeleted for 30 days.`;
		if (!(await confirmDestructive({ title: 'Delete service account', message }))) return;
		busy = `del-${a.email}`;
		const ok = await attempt(() => deleteAccount(project, a.email), `Deleted ${a.email}`, `Failed to delete ${a.email}`);
		busy = '';
		if (!ok) return;
		recentlyDeleted = [a, ...recentlyDeleted.filter((d) => d.uniqueId !== a.uniqueId)];
		await onChanged();
	}

	async function restore(uniqueId: string) {
		busy = `undel-${uniqueId}`;
		const ok = await attempt(() => undeleteAccount(uniqueId), 'Restored service account', `Failed to undelete ${uniqueId}`);
		busy = '';
		if (!ok) return;
		recentlyDeleted = recentlyDeleted.filter((d) => d.uniqueId !== uniqueId);
		await onChanged();
	}

	async function submitUndelete(event: Event) {
		event.preventDefault();
		const id = undeleteId.trim();
		if (!id) return;
		await restore(id);
		undeleteId = '';
	}
</script>

<div class="space-y-6" data-testid="iam-accounts">
	<div class="flex justify-end">
		<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700" onclick={openNew} data-testid="iam-new-account">
			<Plus class="h-4 w-4" /> New service account
		</button>
	</div>

	{#if accounts.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="iam-no-accounts">No service accounts in {project}</p>
	{:else}
		<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="iam-account-list">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Email</th><th class={cell}>Display name</th><th class={cell}>Unique ID</th><th class={cell}>Status</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each accounts as a (a.uniqueId)}
						<tr data-testid="iam-account-row">
							<td class="{cell} font-mono text-xs break-all">{a.email}</td>
							<td class="{cell} text-xs">{a.displayName || '—'}</td>
							<td class="{cell} font-mono text-xs" data-testid="iam-account-uid">{a.uniqueId}</td>
							<td class="{cell} text-xs font-medium {a.disabled ? 'text-amber-700 dark:text-amber-400' : 'text-green-700 dark:text-green-400'}" data-testid="iam-account-status">{a.disabled ? 'Disabled' : 'Enabled'}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								<button class={GCP_MINI} onclick={() => onOpenKeys(a.email)} data-testid="iam-account-keys"><KeyRound class="inline h-3.5 w-3.5" /> Keys</button>
								<button class={GCP_MINI} disabled={busy === `toggle-${a.email}`} onclick={() => toggle(a)} data-testid="iam-account-toggle"><Power class="inline h-3.5 w-3.5" /> {a.disabled ? 'Enable' : 'Disable'}</button>
								<button class="{GCP_MINI} text-red-600" disabled={busy === `del-${a.email}`} onclick={() => remove(a)} data-testid="iam-account-delete" aria-label="Delete {a.email}"><Trash2 class="inline h-3.5 w-3.5" /></button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	<section class="space-y-2" data-testid="iam-undelete">
		<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Undelete</h3>
		<p class="text-xs text-gray-500 dark:text-gray-400">Deleted accounts are not listed by the API; restore one by its unique ID within 30 days.</p>
		{#each recentlyDeleted as d (d.uniqueId)}
			<div class="flex flex-wrap items-center gap-3 rounded-md border border-gray-200 dark:border-gray-700 px-3 py-2 text-xs" data-testid="iam-deleted-row">
				<span class="font-mono break-all">{d.email}</span>
				<span class="font-mono text-gray-500 dark:text-gray-400">{d.uniqueId}</span>
				<button class={GCP_MINI} disabled={busy === `undel-${d.uniqueId}`} onclick={() => restore(d.uniqueId)} data-testid="iam-undelete-recent"><Undo2 class="inline h-3.5 w-3.5" /> Undelete</button>
			</div>
		{/each}
		<form class="flex flex-wrap items-center gap-2" onsubmit={submitUndelete}>
			<input class="{GCP_INPUT} max-w-xs font-mono" bind:value={undeleteId} placeholder="Unique ID" autocomplete="off" aria-label="Unique ID to undelete" data-testid="iam-undelete-id" />
			<button type="submit" class={GCP_MINI} disabled={!undeleteId.trim()} data-testid="iam-undelete-submit">Undelete by ID</button>
		</form>
	</section>
</div>

<Modal bind:this={modal} title="New service account" titleId="iam-account-title">
	{#snippet children()}
		<form id="iam-account-form" class="space-y-3" onsubmit={submitNew} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Account ID</span>
				<input class={GCP_INPUT} bind:value={fId} placeholder="my-service" autocomplete="off" aria-label="Account ID" />
				{#if touched && idError}<span class="block text-xs text-red-600 dark:text-red-400">{idError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Display name (optional)</span>
				<input class={GCP_INPUT} bind:value={fName} aria-label="Display name" />
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Description (optional)</span>
				<input class={GCP_INPUT} bind:value={fDesc} aria-label="Description" />
			</label>
			<p class="text-xs text-gray-500 dark:text-gray-400">Created in {project}.</p>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="iam-account-form" class={GCP_PRIMARY} disabled={busy === 'create'} data-testid="iam-account-submit">{busy === 'create' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>
