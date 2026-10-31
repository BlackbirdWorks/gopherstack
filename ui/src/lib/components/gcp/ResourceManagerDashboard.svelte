<script lang="ts">
	import { onMount } from 'svelte';
	import { FolderPlus, Landmark, Plus, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import RmTree from '#lib/components/gcp/RmTree.svelte';
	import RmDetail from '#lib/components/gcp/RmDetail.svelte';
	import { attempt, GCP_GHOST, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		createFolder,
		createProject,
		flatten,
		getEnforcement,
		listOrganizations,
		listOrphanProjects,
		loadTree,
		orphanRoot,
		parentOptions,
		validFolderName,
		validProjectId,
		validProjectName,
		type RmEnforcement,
		type RmNode
	} from '#lib/gcpresourcemanager.ts';

	let roots = $state<RmNode[]>([]);
	let enforcement = $state<RmEnforcement>({ projects: false, serviceEnablement: false });
	let selected = $state('');
	let loading = $state(true);
	let error = $state('');
	let form = $state<'' | 'folder' | 'project'>('');
	let folderName = $state('');
	let folderParent = $state('');
	let projectId = $state('');
	let projectName = $state('');
	let projectParent = $state('');
	let labels = $state<{ key: string; value: string }[]>([]);

	const orgRoots = $derived(roots.filter((r) => r.name));
	const all = $derived(roots.flatMap((r) => flatten(r)).filter((n) => n.name));
	const current = $derived(all.find((n) => n.name === selected));
	const byName = $derived(new Map(all.map((n) => [n.name, n])));
	const folderOptions = $derived(orgRoots.flatMap((r) => parentOptions(r)));
	const folderOk = $derived(validFolderName(folderName.trim()) && Boolean(folderParent));
	const projectOk = $derived(validProjectId(projectId) && validProjectName(projectName.trim()) && (!projectParent || folderOptions.some((o) => o.name === projectParent)));

	async function load() {
		try {
			const [orgs, orphans, enf] = await Promise.all([listOrganizations(), listOrphanProjects(), getEnforcement()]);
			const trees = await Promise.all(orgs.map((o) => loadTree(o)));
			roots = orphans.length > 0 ? [...trees, orphanRoot(orphans)] : trees;
			enforcement = enf;
			error = '';
			if (!selected) selected = trees[0]?.name ?? '';
			folderParent ||= folderOptions[0]?.name ?? '';
			projectParent ||= folderOptions[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function submitFolder() {
		if (!folderOk) return;
		if (!(await attempt(() => createFolder(folderParent, folderName.trim()), `Created folder ${folderName.trim()}`, 'Failed to create folder'))) return;
		folderName = '';
		form = '';
		await load();
	}

	async function submitProject() {
		if (!projectOk) return;
		const id = projectId;
		const ok = await attempt(() => createProject({ projectId: id, displayName: projectName, parent: projectParent, labels }), `Created project ${id}`, 'Failed to create project');
		if (!ok) return;
		projectId = '';
		projectName = '';
		labels = [];
		form = '';
		await load();
		selected = all.find((n) => n.kind === 'project' && n.id === id)?.name ?? selected;
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="rm-dashboard">
	<PageHeader icon={Landmark} title="Resource Manager" description="Organization, folders, projects, IAM and API enablement" color="blue" onRefresh={load} />

	<div class="rounded-lg border border-gray-200 p-3 text-sm dark:border-gray-700" data-testid="rm-enforcement">
		<span class="font-medium">Enforcement</span>
		<span class="ml-3" data-testid="rm-enforce-projects">--gcp-enforce-projects: <b>{enforcement.projects || enforcement.serviceEnablement ? 'on' : 'off'}</b></span>
		<span class="ml-3" data-testid="rm-enforce-services">--gcp-enforce-service-enablement: <b>{enforcement.serviceEnablement ? 'on' : 'off'}</b></span>
		<span class="ml-3 text-xs text-gray-500 dark:text-gray-400">{enforcement.projects || enforcement.serviceEnablement ? 'Other GCP services reject unknown or deleted projects' : 'Other GCP services accept any project id'}{enforcement.serviceEnablement ? ' and APIs that are not enabled' : ''}.</span>
	</div>

	{#if error && !roots.length}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="rm-error">
			<p class="font-medium">Could not load Resource Manager</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap gap-2">
			<button class="{GCP_PRIMARY} flex items-center gap-1 !py-1" onclick={() => (form = form === 'folder' ? '' : 'folder')} data-testid="rm-folder-toggle"><FolderPlus class="h-4 w-4" /> Create folder</button>
			<button class="{GCP_PRIMARY} flex items-center gap-1 !py-1" onclick={() => (form = form === 'project' ? '' : 'project')} data-testid="rm-project-toggle"><Plus class="h-4 w-4" /> Create project</button>
		</div>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="rm-scope-error">{error}</p>{/if}

		{#if form === 'folder'}
			<form class="flex flex-wrap items-end gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700" onsubmit={(e) => { e.preventDefault(); void submitFolder(); }} data-testid="rm-folder-form">
				<label class="block">Display name<input class="{GCP_INPUT} !w-64" bind:value={folderName} data-testid="rm-folder-name" />
					{#if folderName && !validFolderName(folderName.trim())}<span class="text-xs text-red-600">3 to 30 letters, digits, spaces, hyphens or underscores; no space at either end.</span>{/if}
				</label>
				<label class="block">Parent
					<select class="{GCP_INPUT} !w-72" bind:value={folderParent} data-testid="rm-folder-parent">
						{#each folderOptions as o (o.name)}<option value={o.name}>{o.label}</option>{/each}
					</select>
				</label>
				<button type="submit" class={GCP_PRIMARY} disabled={!folderOk} data-testid="rm-folder-submit">Create</button>
				<button type="button" class={GCP_GHOST} onclick={() => (form = '')}>Close</button>
			</form>
		{:else if form === 'project'}
			<form class="space-y-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700" onsubmit={(e) => { e.preventDefault(); void submitProject(); }} data-testid="rm-project-form">
				<div class="grid gap-3 sm:grid-cols-3">
					<label class="block">Project ID<input class="{GCP_INPUT} font-mono" bind:value={projectId} placeholder="my-project-1" data-testid="rm-project-new-id" />
						{#if projectId && !validProjectId(projectId)}<span class="text-xs text-red-600">6 to 30 lowercase letters, digits or hyphens; start with a letter, no trailing hyphen.</span>{/if}
					</label>
					<label class="block">Display name<input class={GCP_INPUT} bind:value={projectName} data-testid="rm-project-new-name" />
						{#if projectName && !validProjectName(projectName.trim())}<span class="text-xs text-red-600">4 to 30 letters, digits, hyphens, quotes, spaces or !.</span>{/if}
					</label>
					<label class="block">Parent
						<select class={GCP_INPUT} bind:value={projectParent} data-testid="rm-project-new-parent">
							<option value="">(none)</option>
							{#each folderOptions as o (o.name)}<option value={o.name}>{o.label}</option>{/each}
						</select>
					</label>
				</div>
				<div class="space-y-2">
					<p>Labels</p>
					{#each labels as l, i (i)}
						<div class="flex gap-2">
							<input class="{GCP_INPUT} font-mono" placeholder="key" bind:value={l.key} aria-label="Label key" />
							<input class="{GCP_INPUT} font-mono" placeholder="value" bind:value={l.value} aria-label="Label value" />
							<button type="button" class={GCP_GHOST} onclick={() => (labels = labels.filter((_, k) => k !== i))}>Remove</button>
						</div>
					{/each}
					<button type="button" class={GCP_GHOST} onclick={() => (labels = [...labels, { key: '', value: '' }])} data-testid="rm-label-add">Add label</button>
				</div>
				<div class="flex gap-2">
					<button type="submit" class={GCP_PRIMARY} disabled={!projectOk} data-testid="rm-project-submit">Create</button>
					<button type="button" class={GCP_GHOST} onclick={() => (form = '')}>Close</button>
				</div>
			</form>
		{/if}

		<div class="grid gap-4 lg:grid-cols-[22rem_1fr]">
			<nav class="rounded-lg border border-gray-200 p-2 dark:border-gray-700" aria-label="Resource hierarchy" data-testid="rm-tree">
				{#if roots.length === 0}
					<p class="p-3 text-sm text-gray-500" data-testid="rm-empty">No organization or projects yet.</p>
				{:else}
					<ul>
						{#each roots as r (r.name || 'none')}
							<RmTree node={r} {selected} onSelect={(n) => (selected = n.name)} />
						{/each}
					</ul>
				{/if}
			</nav>
			{#if current}
				<RmDetail node={current} roots={orgRoots} parentLabel={byName.get(current.parent)?.label ?? current.parent} onChanged={load} />
			{:else}
				<p class="py-12 text-center text-gray-500" data-testid="rm-select-hint">Select an organization, folder or project.</p>
			{/if}
		</div>
	{/if}
</div>
