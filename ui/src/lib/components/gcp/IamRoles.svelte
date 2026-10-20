<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Plus, Trash2, Pencil, Undo2, ChevronRight, ChevronDown } from 'lucide-svelte';
	import Modal from '#lib/components/Modal.svelte';
	import SearchInput from '#lib/components/SearchInput.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY, attempt } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		ROLE_STAGES,
		createRole,
		deleteRole,
		listCustomRoles,
		listPredefinedRoles,
		matchesRole,
		parsePermissions,
		undeleteRole,
		updateRole,
		validatePermissions,
		validateRoleId,
		type RoleInfo,
		type RoleStage
	} from '#lib/gcpiam.ts';

	type Props = { project: string };

	let { project }: Props = $props();

	const cell = 'px-4 py-3';
	let view = $state<'predefined' | 'custom'>('predefined');
	let predefined = $state<RoleInfo[]>([]);
	let custom = $state<RoleInfo[]>([]);
	let query = $state('');
	let expanded = $state<string | null>(null);
	let busy = $state('');
	let shown = $state(100);

	const filtered = $derived(predefined.filter((r) => matchesRole(r, query)));

	let modal = $state<Modal | null>(null);
	let editingId = $state<string | null>(null);
	let fId = $state('');
	let fTitle = $state('');
	let fDesc = $state('');
	let fStage = $state<RoleStage>('GA');
	let fPerms = $state('');
	let touched = $state(false);
	const parsedPerms = $derived(parsePermissions(fPerms));
	const idError = $derived(editingId === null ? validateRoleId(fId.trim()) : null);
	const permError = $derived(validatePermissions(parsedPerms));

	async function load() {
		try {
			[predefined, custom] = await Promise.all([listPredefinedRoles(), listCustomRoles(project)]);
			custom = custom.toSorted((a, b) => a.id.localeCompare(b.id));
		} catch (e) {
			toast.error(`Failed to load roles: ${describeGcpError(e)}`);
		}
	}

	function openNew() {
		editingId = null;
		fId = '';
		fTitle = '';
		fDesc = '';
		fStage = 'GA';
		fPerms = '';
		touched = false;
		modal?.open();
	}

	function openEdit(r: RoleInfo) {
		editingId = r.id;
		fTitle = r.title;
		fDesc = r.description;
		fStage = (ROLE_STAGES as readonly string[]).includes(r.stage) ? (r.stage as RoleStage) : 'GA';
		fPerms = r.permissions.join('\n');
		touched = false;
		modal?.open();
	}

	function save() {
		const body = { title: fTitle, description: fDesc, stage: fStage, permissions: parsedPerms };
		if (editingId !== null) {
			const id = editingId;
			return attempt(() => updateRole(project, id, body), `Updated role ${id}`, `Failed to update ${id}`);
		}
		const id = fId.trim();
		return attempt(() => createRole(project, { id, ...body }), `Created role ${id}`, `Failed to create ${id}`);
	}

	async function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (idError || permError) return;
		busy = 'save';
		const ok = await save();
		busy = '';
		if (ok) {
			modal?.close('saved');
			await load();
		}
	}

	async function remove(r: RoleInfo) {
		const message = `Delete custom role "${r.id}"? It can be undeleted for 7 days.`;
		if (!(await confirmDestructive({ title: 'Delete role', message }))) return;
		busy = `del-${r.id}`;
		const ok = await attempt(() => deleteRole(project, r.id), `Deleted role ${r.id}`, `Failed to delete ${r.id}`);
		busy = '';
		if (ok) await load();
	}

	async function restore(r: RoleInfo) {
		busy = `undel-${r.id}`;
		const ok = await attempt(() => undeleteRole(project, r.id), `Restored role ${r.id}`, `Failed to undelete ${r.id}`);
		busy = '';
		if (ok) await load();
	}

	const toggle = (name: string) => (expanded = expanded === name ? null : name);
	const tabBtn = (active: boolean) =>
		`rounded-md px-3 py-1.5 text-xs font-medium ${active ? 'bg-blue-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300'}`;

	onMount(load);
</script>

{#snippet permList(r: RoleInfo)}
	<tr class="bg-gray-50/60 dark:bg-slate-800/30" data-testid="iam-role-detail">
		<td colspan="5" class="px-4 py-3 text-xs space-y-1">
			<p class="font-mono break-all">{r.name}</p>
			{#if r.description}<p>{r.description}</p>{/if}
			<p class="font-medium text-gray-500 dark:text-gray-400">{r.permissions.length} permissions</p>
			<ul class="max-h-56 overflow-auto columns-1 md:columns-2 font-mono" data-testid="iam-role-perms">
				{#each r.permissions as p (p)}<li>{p}</li>{/each}
			</ul>
		</td>
	</tr>
{/snippet}

<div class="space-y-4" data-testid="iam-roles">
	<div class="flex flex-wrap items-center justify-between gap-3">
		<div class="flex gap-2" role="group" aria-label="Role type">
			<button class={tabBtn(view === 'predefined')} onclick={() => (view = 'predefined')} data-testid="iam-roles-predefined">Predefined ({predefined.length})</button>
			<button class={tabBtn(view === 'custom')} onclick={() => (view = 'custom')} data-testid="iam-roles-custom">Custom ({custom.length})</button>
		</div>
		{#if view === 'predefined'}
			<SearchInput bind:value={query} placeholder="Search roles or permissions…" onInput={() => (shown = 100)} />
		{:else}
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700" onclick={openNew} data-testid="iam-new-role"><Plus class="h-4 w-4" /> New custom role</button>
		{/if}
	</div>

	{#if view === 'predefined'}
		{#if filtered.length === 0}
			<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="iam-no-roles">No matching roles</p>
		{:else}
			<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="iam-predefined-list">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Role</th><th class={cell}>Title</th><th class={cell}>Permissions</th><th class={cell}>Stage</th><th class={cell}></th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filtered.slice(0, shown) as r (r.name)}
							<tr data-testid="iam-role-row">
								<td class="{cell} font-mono text-xs break-all">{r.name}</td>
								<td class="{cell} text-xs">{r.title}</td>
								<td class="{cell} text-xs">{r.permissions.length}</td>
								<td class="{cell} text-xs">{r.stage}</td>
								<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => toggle(r.name)} aria-expanded={expanded === r.name} data-testid="iam-role-toggle">{#if expanded === r.name}<ChevronDown class="inline h-3.5 w-3.5" />{:else}<ChevronRight class="inline h-3.5 w-3.5" />{/if} Permissions</button></td>
							</tr>
							{#if expanded === r.name}{@render permList(r)}{/if}
						{/each}
					</tbody>
				</table>
			</div>
			{#if filtered.length > shown}
				<button class={GCP_MINI} onclick={() => (shown += 100)} data-testid="iam-roles-more">Show more ({filtered.length - shown} remaining)</button>
			{/if}
		{/if}
	{:else if custom.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="iam-no-custom">No custom roles in {project}</p>
	{:else}
		<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="iam-custom-list">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Role</th><th class={cell}>Title</th><th class={cell}>Permissions</th><th class={cell}>Stage</th><th class="{cell} text-right">Actions</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each custom as r (r.name)}
						<tr data-testid="iam-custom-row" class={r.deleted ? 'opacity-60' : ''}>
							<td class="{cell} font-mono text-xs break-all"><button class="underline decoration-dotted text-left" onclick={() => toggle(r.name)} aria-expanded={expanded === r.name} data-testid="iam-role-toggle">{r.id}</button>{#if r.deleted}<span class="ml-2 text-red-600 dark:text-red-400" data-testid="iam-role-deleted">deleted</span>{/if}</td>
							<td class="{cell} text-xs">{r.title || '—'}</td>
							<td class="{cell} text-xs">{r.permissions.length}</td>
							<td class="{cell} text-xs">{r.stage}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								{#if r.deleted}
									<button class={GCP_MINI} disabled={busy === `undel-${r.id}`} onclick={() => restore(r)} data-testid="iam-role-undelete"><Undo2 class="inline h-3.5 w-3.5" /> Undelete</button>
								{:else}
									<button class={GCP_MINI} onclick={() => openEdit(r)} data-testid="iam-role-edit" aria-label="Edit role {r.id}"><Pencil class="inline h-3.5 w-3.5" /> Edit</button>
									<button class="{GCP_MINI} text-red-600" disabled={busy === `del-${r.id}`} onclick={() => remove(r)} data-testid="iam-role-delete" aria-label="Delete role {r.id}"><Trash2 class="inline h-3.5 w-3.5" /></button>
								{/if}
							</td>
						</tr>
						{#if expanded === r.name}{@render permList(r)}{/if}
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
</div>

<Modal bind:this={modal} title={editingId === null ? 'New custom role' : 'Edit custom role'} titleId="iam-role-title">
	{#snippet children()}
		<form id="iam-role-form" class="space-y-3" onsubmit={submit} novalidate>
			{#if editingId === null}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Role ID</span>
					<input class={GCP_INPUT} bind:value={fId} placeholder="myCustomRole" autocomplete="off" aria-label="Role ID" />
					{#if touched && idError}<span class="block text-xs text-red-600 dark:text-red-400">{idError}</span>{/if}
				</label>
			{:else}
				<p class="font-mono text-xs text-gray-500 dark:text-gray-400">{editingId}</p>
			{/if}
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Title</span>
				<input class={GCP_INPUT} bind:value={fTitle} aria-label="Title" />
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Description</span>
				<input class={GCP_INPUT} bind:value={fDesc} aria-label="Description" />
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Launch stage</span>
				<select class={GCP_INPUT} bind:value={fStage} aria-label="Launch stage">
					{#each ROLE_STAGES as s (s)}<option value={s}>{s}</option>{/each}
				</select>
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Permissions (one per line or comma separated)</span>
				<textarea class="{GCP_INPUT} font-mono" rows="6" bind:value={fPerms} placeholder="storage.objects.get" aria-label="Permissions"></textarea>
				<span class="block text-xs text-gray-500 dark:text-gray-400">{parsedPerms.length} permissions</span>
				{#if touched && permError}<span class="block text-xs text-red-600 dark:text-red-400">{permError}</span>{/if}
			</label>
			<p class="text-xs text-gray-500 dark:text-gray-400">Role lives in {project}.</p>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="iam-role-form" class={GCP_PRIMARY} disabled={busy === 'save'} data-testid="iam-role-submit">{busy === 'save' ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
