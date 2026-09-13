<script lang="ts" module>
	export type PermissionRow = { principal: string; actions: string[] };
	export type ActionPreset = { label: string; actions: string[] };
</script>

<script lang="ts">
	// Shared Describe*Permissions/Update*Permissions editor for every QuickSight
	// sub-resource that carries the family (dashboards, analyses, data sets,
	// data sources, templates, themes, folders). The parent owns the SDK calls
	// (describe on open, grant/revoke via onUpdate) and hands this component
	// plain {principal, actions[]} rows -- same split as TagEditor.svelte in
	// lightsail's panels. Dashboards' link-sharing permissions (GrantLink/
	// RevokeLinkPermissions) reuse this same component with its own props
	// rather than a special-cased mode; see +page.svelte.
	let {
		sectionTitle = 'Permissions',
		permissions,
		loading = false,
		presets,
		onUpdate
	}: {
		sectionTitle?: string;
		permissions: PermissionRow[];
		loading?: boolean;
		presets: ActionPreset[];
		onUpdate: (grant: PermissionRow[], revoke: PermissionRow[]) => Promise<void>;
	} = $props();

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	function presetActions(label: string): string[] {
		return presets.find((p) => p.label === label)?.actions ?? [];
	}

	function errMessage(e: unknown): string {
		return e instanceof Error ? e.message : String(e);
	}

	let addPrincipal = $state('');
	let addPreset = $state('');
	let addExtraActions = $state('');
	let addBusy = $state(false);
	let addError = $state<string | null>(null);

	$effect(() => {
		if (!addPreset && presets.length > 0) addPreset = presets[0].label;
	});

	async function submitAdd(): Promise<void> {
		const principal = addPrincipal.trim();
		if (!principal) return;
		const actions = Array.from(new Set([...presetActions(addPreset), ...parseCommaList(addExtraActions)]));
		if (actions.length === 0) {
			addError = 'Select a preset or enter at least one action.';
			return;
		}
		addBusy = true;
		addError = null;
		try {
			await onUpdate([{ principal, actions }], []);
			addPrincipal = '';
			addExtraActions = '';
		} catch (e) {
			addError = errMessage(e);
		} finally {
			addBusy = false;
		}
	}

	let editingPrincipal = $state<string | null>(null);
	let editActions = $state('');
	let editBusy = $state(false);
	let editError = $state<string | null>(null);

	function startEdit(row: PermissionRow): void {
		editingPrincipal = row.principal;
		editActions = row.actions.join(', ');
		editError = null;
	}

	function cancelEdit(): void {
		editingPrincipal = null;
	}

	async function saveEdit(row: PermissionRow): Promise<void> {
		const newActions = parseCommaList(editActions);
		const grantActions = newActions.filter((a) => !row.actions.includes(a));
		const revokeActions = row.actions.filter((a) => !newActions.includes(a));
		if (grantActions.length === 0 && revokeActions.length === 0) {
			editingPrincipal = null;
			return;
		}
		editBusy = true;
		editError = null;
		try {
			await onUpdate(
				grantActions.length > 0 ? [{ principal: row.principal, actions: grantActions }] : [],
				revokeActions.length > 0 ? [{ principal: row.principal, actions: revokeActions }] : []
			);
			editingPrincipal = null;
		} catch (e) {
			editError = errMessage(e);
		} finally {
			editBusy = false;
		}
	}

	let removingPrincipal = $state<string | null>(null);
	let removeError = $state<string | null>(null);

	async function removeRow(row: PermissionRow): Promise<void> {
		removingPrincipal = row.principal;
		removeError = null;
		try {
			await onUpdate([], [{ principal: row.principal, actions: row.actions }]);
		} catch (e) {
			removeError = errMessage(e);
		} finally {
			removingPrincipal = null;
		}
	}
</script>

<div class="space-y-2">
	<p class="text-sm font-medium text-slate-700 dark:text-slate-300">{sectionTitle}</p>
	{#if loading}
		<p class="text-xs text-slate-500 dark:text-slate-400">Loading permissions…</p>
	{:else}
		<table class="w-full text-xs">
			<thead>
				<tr class="text-left text-slate-500">
					<th class="pr-2 font-medium">Principal</th>
					<th class="pr-2 font-medium">Actions</th>
					<th class="font-medium"></th>
				</tr>
			</thead>
			<tbody>
				{#each permissions as row (row.principal)}
					<tr>
						<td class="pr-2 py-1 break-all">{row.principal}</td>
						<td class="pr-2 py-1">
							{#if editingPrincipal === row.principal}
								<input
									bind:value={editActions}
									aria-label="Edit actions for {row.principal}"
									class="w-full px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
								/>
							{:else}
								{row.actions.join(', ')}
							{/if}
						</td>
						<td class="py-1 whitespace-nowrap">
							{#if editingPrincipal === row.principal}
								<button
									onclick={() => saveEdit(row)}
									disabled={editBusy}
									class="text-blue-600 hover:underline mr-2 disabled:opacity-50">Save</button
								>
								<button onclick={cancelEdit} class="text-slate-500 hover:underline mr-2">Cancel</button>
							{:else}
								<button onclick={() => startEdit(row)} class="text-blue-600 hover:underline mr-2">Edit</button>
							{/if}
							<button
								onclick={() => removeRow(row)}
								disabled={removingPrincipal === row.principal}
								class="text-red-600 hover:underline disabled:opacity-50">Remove</button
							>
						</td>
					</tr>
				{:else}
					<tr><td colspan="3" class="text-slate-500 py-1">No principals granted access</td></tr>
				{/each}
			</tbody>
		</table>
		{#if editError}<p class="text-xs text-red-600 dark:text-red-400">{editError}</p>{/if}
		{#if removeError}<p class="text-xs text-red-600 dark:text-red-400">{removeError}</p>{/if}

		<div class="flex flex-col sm:flex-row gap-2 items-start sm:items-center pt-1">
			<input
				bind:value={addPrincipal}
				placeholder="Principal ARN"
				aria-label="{sectionTitle}: new principal ARN"
				class="flex-1 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
			/>
			<select
				bind:value={addPreset}
				aria-label="{sectionTitle}: action preset"
				class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
			>
				{#each presets as p (p.label)}
					<option value={p.label}>{p.label}</option>
				{/each}
			</select>
			<input
				bind:value={addExtraActions}
				placeholder="Extra actions, comma-separated (optional)"
				aria-label="{sectionTitle}: extra actions"
				class="flex-1 px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
			/>
			<button
				onclick={submitAdd}
				disabled={addBusy || !addPrincipal.trim()}
				class="px-2 py-1 text-xs rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50"
				>Grant</button
			>
		</div>
		{#if addError}<p class="text-xs text-red-600 dark:text-red-400">{addError}</p>{/if}
	{/if}
</div>
