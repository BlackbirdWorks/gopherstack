<script lang="ts">
	import Tabs from '#lib/components/Tabs.svelte';
	import RmApis from '#lib/components/gcp/RmApis.svelte';
	import RmIam from '#lib/components/gcp/RmIam.svelte';
	import RmTags from '#lib/components/gcp/RmTags.svelte';
	import { confirmedAttempt, attempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY, GCP_GHOST } from '#lib/gcp-action.ts';
	import { deleteNode, isDeleted, moveNode, parentOptions, statePill, undeleteNode, type RmNode } from '#lib/gcpresourcemanager.ts';

	type Props = { node: RmNode; roots: RmNode[]; parentLabel: string; onChanged: () => Promise<void> };
	let { node, roots, parentLabel, onChanged }: Props = $props();

	let tab = $state('overview');
	let moving = $state(false);
	let dest = $state('');

	const orgName = $derived(roots.find((r) => r.name === node.parent || r.name === node.name)?.name ?? roots[0]?.name ?? '');
	const tabs = $derived([
		{ id: 'overview', label: 'Overview' },
		...(node.kind === 'project' ? [{ id: 'apis', label: 'APIs' }] : []),
		{ id: 'iam', label: 'IAM' },
		{ id: 'tags', label: 'Tags' }
	]);
	const options = $derived(roots.flatMap((r) => parentOptions(r, node.kind === 'folder' ? node.name : '')).filter((o) => o.name !== node.parent));

	$effect(() => {
		void node.name;
		if (!tabs.some((t) => t.id === tab)) tab = 'overview';
		moving = false;
	});

	async function remove() {
		const what = node.kind === 'project' ? 'project' : 'folder';
		const ok = await confirmedAttempt(
			{ title: `Delete ${what}`, message: `Delete ${what} "${node.label}"? A project can be restored with undelete until its retention passes; a folder must be empty.`, confirmLabel: 'Delete' },
			() => deleteNode(node),
			`Deleted ${node.label}`,
			`Failed to delete ${node.label}`
		);
		if (ok) await onChanged();
	}

	async function restore() {
		if (await attempt(() => undeleteNode(node), `Restored ${node.label}`, `Failed to restore ${node.label}`)) await onChanged();
	}

	async function move() {
		if (!dest) return;
		if (await attempt(() => moveNode(node, dest), `Moved ${node.label}`, `Failed to move ${node.label}`)) {
			moving = false;
			await onChanged();
		}
	}

	const row = 'grid grid-cols-[10rem_1fr] gap-2 py-1';
</script>

<section class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="rm-detail">
	<div class="flex flex-wrap items-center gap-3">
		<h2 class="text-lg font-semibold">{node.label}</h2>
		<span class="rounded-full px-2 py-0.5 text-xs {statePill(node.state)}" data-testid="rm-detail-state">{node.state}</span>
		{#if node.kind !== 'organization'}
			<span class="ml-auto space-x-1">
				{#if isDeleted(node)}
					<button class={GCP_MINI} onclick={restore} data-testid="rm-undelete">Undelete</button>
				{:else}
					<button class={GCP_MINI} onclick={() => { moving = !moving; dest = options[0]?.name ?? ''; }} data-testid="rm-move-toggle">Move</button>
					<button class={GCP_MINI} onclick={remove} data-testid="rm-delete">Delete</button>
				{/if}
			</span>
		{/if}
	</div>

	{#if moving}
		<div class="flex flex-wrap items-center gap-2 text-sm" data-testid="rm-move-form">
			<label for="rm-move-dest">Move to</label>
			<select id="rm-move-dest" class="{GCP_INPUT} !w-80" bind:value={dest} data-testid="rm-move-dest">
				{#each options as o (o.name)}<option value={o.name}>{o.label}</option>{/each}
			</select>
			<button class={GCP_PRIMARY} disabled={!dest} onclick={move} data-testid="rm-move-confirm">Move</button>
			<button class={GCP_GHOST} onclick={() => (moving = false)}>Cancel</button>
		</div>
	{/if}

	<Tabs {tabs} active={tab} onSelect={(id) => (tab = id)} color="blue" />

	{#if tab === 'overview'}
		<dl class="text-sm" data-testid="rm-overview">
			{#if node.kind === 'project'}
				<div class={row}><dt class="text-gray-500">Project ID</dt><dd class="font-mono" data-testid="rm-project-id">{node.id}</dd></div>
				<div class={row}><dt class="text-gray-500">Project number</dt><dd class="font-mono" data-testid="rm-project-number">{node.number}</dd></div>
			{:else}
				<div class={row}><dt class="text-gray-500">Name</dt><dd class="font-mono">{node.name}</dd></div>
			{/if}
			<div class={row}><dt class="text-gray-500">Display name</dt><dd>{node.label}</dd></div>
			<div class={row}><dt class="text-gray-500">Parent</dt><dd data-testid="rm-parent">{parentLabel || '—'}</dd></div>
			<div class={row}><dt class="text-gray-500">Created</dt><dd>{node.createTime || '—'}</dd></div>
			{#if isDeleted(node)}
				<div class={row}><dt class="text-gray-500">Delete requested</dt><dd data-testid="rm-delete-time">{node.deleteTime || '—'} <span class="text-xs text-gray-500">(restorable until the retention window passes)</span></dd></div>
			{/if}
			{#if node.kind === 'project'}
				<div class={row}><dt class="text-gray-500">Labels</dt><dd class="font-mono text-xs" data-testid="rm-labels">{Object.entries(node.labels).map(([k, v]) => `${k}=${v}`).join(', ') || '—'}</dd></div>
			{/if}
		</dl>
	{:else if tab === 'apis' && node.kind === 'project'}
		<RmApis project={node.id} />
	{:else if tab === 'iam'}
		<RmIam {node} />
	{:else if tab === 'tags'}
		<RmTags {node} org={orgName} />
	{/if}
</section>
