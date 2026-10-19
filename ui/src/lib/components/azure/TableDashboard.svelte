<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Table2, Plus, Trash2, Pencil, RefreshCw, X, Inbox, Copy, Search, Filter } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		DEFAULT_ACCOUNT,
		DEFAULT_PAGE_SIZE,
		EDM_TYPES,
		PAGE_SIZES,
		TableError,
		buildFilter,
		createTable,
		deleteEntity,
		deleteTable,
		entityToDrafts,
		insertEntity,
		listTables,
		propertyColumns,
		queryEntities,
		updateEntity,
		validateKey,
		validatePropertyName,
		validateTableName,
		validateTypedValue,
		type Continuation,
		type EdmType,
		type Entity,
		type PropertyDraft,
		type TableInfo,
		type UpdateMode
	} from '#lib/azuretable.ts';

	const account = DEFAULT_ACCOUNT;

	let tables = $state<TableInfo[]>([]);
	let current = $state<string | null>(null);
	let entities = $state<Entity[]>([]);
	let next = $state<Continuation | null>(null);
	let selected = $state<Entity | null>(null);
	let loading = $state(true);
	let loadingEntities = $state(false);
	let loadError = $state('');
	let queryError = $state('');

	let filterText = $state('');
	let appliedFilter = $state('');
	let pageSize = $state<number>(DEFAULT_PAGE_SIZE);
	let top = $state<number>(DEFAULT_PAGE_SIZE);

	let tableModal = $state<Modal | null>(null);
	let tableName = $state('');
	let tableTouched = $state(false);
	let creating = $state(false);

	let entityModal = $state<Modal | null>(null);
	let editing = $state<Entity | null>(null);
	let formPk = $state('');
	let formRk = $state('');
	let formProps = $state<PropertyDraft[]>([]);
	let formMode = $state<UpdateMode>('merge');
	let formTouched = $state(false);
	let saving = $state(false);

	const columns = $derived(propertyColumns(entities));
	const nameError = $derived(validateTableName(tableName.trim()));
	const pkError = $derived(validateKey('PartitionKey', formPk));
	const rkError = $derived(validateKey('RowKey', formRk));
	const propErrors = $derived(
		formProps.map((p, i) => {
			const nameErr = validatePropertyName(p.name);
			const dup = formProps.findIndex((o) => o.name === p.name) !== i;
			return {
				name: nameErr ?? (dup ? 'Duplicate property name' : null),
				value: validateTypedValue(p.type, p.value)
			};
		})
	);
	const formInvalid = $derived(
		Boolean(pkError || rkError) || propErrors.some((e) => e.name || e.value)
	);
	const hasMore = $derived(next !== null || (entities.length > 0 && entities.length >= top));

	function describe(e: unknown): string {
		if (e instanceof TableError) return e.code ? `${e.code}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	async function loadTables() {
		loading = true;
		loadError = '';
		try {
			tables = (await listTables(account)).toSorted((a, b) => a.name.localeCompare(b.name));
			if (current && !tables.some((t) => t.name === current)) selectTable(null);
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load tables: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadEntities(append = false) {
		if (!current) return;
		loadingEntities = true;
		queryError = '';
		try {
			const page = await queryEntities(account, current, {
				filter: appliedFilter,
				top: append && next ? pageSize : top,
				next: append ? next : null
			});
			entities = append && next ? [...entities, ...page.entities] : page.entities;
			next = page.next;
			if (selected) {
				const keep = entities.find((x) => x.partitionKey === selected?.partitionKey && x.rowKey === selected?.rowKey);
				selected = keep ?? null;
			}
		} catch (e) {
			queryError = describe(e);
			entities = [];
			next = null;
		} finally {
			loadingEntities = false;
		}
	}

	async function refresh() {
		await loadTables();
		await loadEntities();
	}

	function selectTable(name: string | null) {
		current = name;
		entities = [];
		next = null;
		selected = null;
		filterText = '';
		appliedFilter = '';
		top = pageSize;
		queryError = '';
		if (name) void loadEntities();
	}

	function runQuery(event?: Event) {
		event?.preventDefault();
		appliedFilter = filterText.trim();
		top = pageSize;
		selected = null;
		void loadEntities();
	}

	function clearFilter() {
		filterText = '';
		runQuery();
	}

	function changePageSize() {
		top = pageSize;
		void loadEntities();
	}

	function loadMore() {
		if (next) {
			void loadEntities(true);
		} else {
			top += pageSize;
			void loadEntities();
		}
	}

	function filterToPartition(e: Entity) {
		filterText = buildFilter([{ property: 'PartitionKey', op: 'eq', type: 'Edm.String', value: e.partitionKey }]);
		runQuery();
	}

	function openNewTable() {
		tableName = '';
		tableTouched = false;
		tableModal?.open();
	}

	async function submitTable(event: Event) {
		event.preventDefault();
		tableTouched = true;
		if (nameError) return;
		creating = true;
		const name = tableName.trim();
		try {
			const created = await createTable(account, name);
			toast.success(created ? `Created table ${name}` : `Table ${name} already exists`);
			tableModal?.close('created');
			await loadTables();
			selectTable(name);
		} catch (e) {
			toast.error(`Failed to create ${name}: ${describe(e)}`);
		} finally {
			creating = false;
		}
	}

	async function removeTable(t: TableInfo) {
		if (!(await confirmDestructive({ title: 'Delete table', message: `Delete table "${t.name}" and all of its entities? This cannot be undone.` }))) return;
		try {
			await deleteTable(account, t.name);
			toast.success(`Deleted table ${t.name}`);
			if (current === t.name) selectTable(null);
			await loadTables();
		} catch (e) {
			toast.error(`Failed to delete ${t.name}: ${describe(e)}`);
		}
	}

	function openEntityForm(entity: Entity | null) {
		editing = entity;
		formPk = entity?.partitionKey ?? '';
		formRk = entity?.rowKey ?? '';
		formProps = entity ? entityToDrafts(entity) : [];
		formMode = 'merge';
		formTouched = false;
		entityModal?.open();
	}

	function addProp() {
		formProps = [...formProps, { name: '', type: 'Edm.String', value: '' }];
	}

	function removeProp(i: number) {
		formProps = formProps.filter((_, idx) => idx !== i);
	}

	function changeType(i: number, type: EdmType) {
		formProps[i] = { ...formProps[i], type, value: type === 'Edm.Boolean' ? 'true' : formProps[i].value };
	}

	async function submitEntity(event: Event) {
		event.preventDefault();
		formTouched = true;
		if (!current || formInvalid || saving) return;
		saving = true;
		const table = current;
		try {
			if (editing) {
				await updateEntity(account, table, editing.partitionKey, editing.rowKey, formProps, formMode, editing.etag);
				toast.success(`${formMode === 'merge' ? 'Merged' : 'Replaced'} entity ${editing.partitionKey} / ${editing.rowKey}`);
			} else {
				await insertEntity(account, table, formPk, formRk, formProps);
				toast.success(`Inserted entity ${formPk} / ${formRk}`);
			}
			entityModal?.close('saved');
			await loadEntities();
		} catch (e) {
			toast.error(`Failed to save entity: ${describe(e)}`);
		} finally {
			saving = false;
		}
	}

	async function removeEntity(e: Entity) {
		if (!current) return;
		if (!(await confirmDestructive({ title: 'Delete entity', message: `Delete entity "${e.partitionKey}" / "${e.rowKey}"? This cannot be undone.` }))) return;
		try {
			await deleteEntity(account, current, e.partitionKey, e.rowKey, e.etag);
			toast.success('Deleted entity');
			if (selected?.partitionKey === e.partitionKey && selected?.rowKey === e.rowKey) selected = null;
			await loadEntities();
		} catch (err) {
			toast.error(`Failed to delete entity: ${describe(err)}`);
		}
	}

	async function copy(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	const isSelected = (e: Entity) => selected?.partitionKey === e.partitionKey && selected?.rowKey === e.rowKey;
	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const th = 'px-4 py-3 font-medium whitespace-nowrap';
	const td = 'px-4 py-3 text-gray-600 dark:text-gray-400 max-w-xs truncate';
	const errText = 'block text-xs text-red-600 dark:text-red-400';

	onMount(loadTables);
</script>

<div class="space-y-6 pb-20" data-testid="table-dashboard">
	<PageHeader
		icon={Table2}
		title="Table Storage"
		description="Azure Table Storage: tables and entities"
		color="blue"
		onRefresh={refresh}
	>
		{#snippet actions()}
			<button
				class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
				onclick={openNewTable}
				disabled={loading || Boolean(loadError)}
			>
				<Plus class="h-4 w-4" /> New table
			</button>
		{/snippet}
	</PageHeader>

	<p class="text-xs text-gray-500 dark:text-gray-400">
		Storage account <span class="font-mono" data-testid="table-account">{account}</span>
	</p>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="table-error">
			<p class="font-medium">Could not reach Azure Table Storage</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadTables}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<section aria-label="Tables" class="space-y-2">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Tables</h2>
				{#if loading}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if tables.length === 0}
					<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="table-none">
						<Inbox class="h-10 w-10 mb-2 opacity-30" />
						<p class="text-sm">No tables</p>
						<p class="text-xs">Create a table to get started</p>
					</div>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
						{#each tables as t (t.name)}
							<li class="flex items-center {current === t.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="table-row">
								<button class="flex-1 truncate px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectTable(t.name)}>
									{t.name}
								</button>
								<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {t.name}" aria-label="Delete table {t.name}" onclick={() => removeTable(t)}>
									<Trash2 class="h-4 w-4" />
								</button>
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<section aria-label="Entities" class="space-y-4 min-w-0">
				{#if !current}
					<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="table-select-hint">
						<Table2 class="h-12 w-12 mb-3 opacity-30" />
						<p>Select a table to browse its entities</p>
					</div>
				{:else}
					<div class="flex flex-wrap items-center justify-between gap-2">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white break-all" data-testid="table-current">{current}</h2>
						<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700" onclick={() => openEntityForm(null)}>
							<Plus class="h-4 w-4" /> Insert entity
						</button>
					</div>

					<form class="flex flex-wrap items-end gap-3" onsubmit={runQuery} data-testid="table-query-form">
						<label class="min-w-64 flex-1 space-y-1 text-sm">
							<span class="block font-medium text-gray-700 dark:text-gray-300">$filter</span>
							<input class="{input} font-mono" bind:value={filterText} placeholder="PartitionKey eq 'p1' and Age gt 20" autocomplete="off" aria-label="OData filter" />
						</label>
						<label class="space-y-1 text-sm">
							<span class="block font-medium text-gray-700 dark:text-gray-300">Page size ($top)</span>
							<select class="{input} w-28" bind:value={pageSize} onchange={changePageSize} aria-label="Page size">
								{#each PAGE_SIZES as n (n)}<option value={n}>{n}</option>{/each}
							</select>
						</label>
						<button type="submit" class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={loadingEntities}>
							<Search class="h-4 w-4" /> Query
						</button>
						{#if appliedFilter || filterText}
							<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={clearFilter}>Clear</button>
						{/if}
					</form>

					{#if queryError}
						<div class="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="table-query-error">{queryError}</div>
					{:else if loadingEntities && entities.length === 0}
						<div class="flex justify-center py-6"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
					{:else if entities.length === 0}
						<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="table-empty">
							<Inbox class="h-10 w-10 mb-2 opacity-30" />
							<p class="text-sm">{appliedFilter ? 'No entities match the filter' : 'No entities'}</p>
							{#if !appliedFilter}<p class="text-xs">Insert an entity to get started</p>{/if}
						</div>
					{:else}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
							<table class="w-full text-sm" data-testid="table-entities">
								<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
									<tr>
										<th class={th}>PartitionKey</th>
										<th class={th}>RowKey</th>
										<th class={th}>Timestamp</th>
										{#each columns as c (c)}<th class={th}>{c}</th>{/each}
									</tr>
								</thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each entities as e (e.partitionKey + '\u0000' + e.rowKey)}
										<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {isSelected(e) ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="table-entity-row" onclick={() => (selected = e)}>
											<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-gray-100">{e.partitionKey}</td>
											<td class="px-4 py-3 font-mono text-xs text-gray-900 dark:text-gray-100">{e.rowKey}</td>
											<td class={td}>{e.timestamp}</td>
											{#each columns as c (c)}<td class={td}>{e.props[c]?.value ?? ''}</td>{/each}
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
						<div class="flex flex-wrap items-center justify-between gap-2 text-xs text-gray-500 dark:text-gray-400">
							<span data-testid="table-count">{entities.length} entit{entities.length === 1 ? 'y' : 'ies'} shown{appliedFilter ? ' (filtered)' : ''}</span>
							{#if hasMore}
								<button class="rounded-md border border-gray-300 dark:border-gray-600 px-3 py-1.5 text-sm text-gray-700 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" onclick={loadMore} disabled={loadingEntities} data-testid="table-load-more">
									Load more
								</button>
							{/if}
						</div>
					{/if}

					{#if selected}
						<section class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-4" data-testid="table-entity-detail" aria-label="Entity {selected.partitionKey} / {selected.rowKey}">
							<div class="flex items-start justify-between gap-4">
								<div class="min-w-0">
									<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{selected.partitionKey} / {selected.rowKey}</h3>
									<p class="text-xs text-gray-500 dark:text-gray-400">Entity in {current}</p>
								</div>
								<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (selected = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
							</div>
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
								<dt class={dt}>ETag</dt>
								<dd class="{dd} flex items-start gap-2">
									<span class="font-mono text-xs" data-testid="table-entity-etag">{selected.etag || '—'}</span>
									{#if selected.etag}<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy ETag" onclick={() => selected && copy(selected.etag)}><Copy class="h-3.5 w-3.5" /></button>{/if}
								</dd>
								<dt class={dt}>Timestamp</dt><dd class={dd}>{selected.timestamp || '—'}</dd>
							</dl>
							{#if Object.keys(selected.props).length === 0}
								<p class="text-sm text-gray-500 dark:text-gray-400">No custom properties.</p>
							{:else}
								<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
									<table class="w-full text-sm" data-testid="table-entity-props">
										<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
											<tr><th class={th}>Property</th><th class={th}>Type</th><th class={th}>Value</th></tr>
										</thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each entityToDrafts(selected) as p (p.name)}
												<tr data-testid="table-entity-prop">
													<td class="px-4 py-2 font-medium text-gray-900 dark:text-gray-100">{p.name}</td>
													<td class="px-4 py-2 font-mono text-xs text-gray-500 dark:text-gray-400">{p.type}</td>
													<td class="px-4 py-2 font-mono text-xs text-gray-800 dark:text-gray-200 break-all">{p.value}</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
							<div class="flex flex-wrap gap-2">
								<button class="flex items-center gap-2 rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700" onclick={() => selected && openEntityForm(selected)}><Pencil class="h-4 w-4" /> Edit</button>
								<button class="flex items-center gap-2 rounded-md border border-gray-300 dark:border-gray-600 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => selected && filterToPartition(selected)}><Filter class="h-4 w-4" /> Filter to partition</button>
								<button class="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950" onclick={() => selected && removeEntity(selected)}><Trash2 class="h-4 w-4" /> Delete</button>
							</div>
						</section>
					{/if}

					<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="table-unsupported-note">
						Batch transactions, access policies and $select are not exposed by this page. The emulator does not issue continuation tokens, so "Load more" re-queries with a larger $top. It never annotates Edm.Double, so a whole-number Double reads back as Edm.Int32.
					</p>
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={tableModal} title="New table" titleId="table-create-title">
	{#snippet children()}
		<form id="table-create-form" class="space-y-3" onsubmit={submitTable} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Name</span>
				<input class={input} bind:value={tableName} placeholder="MyTable" autocomplete="off" aria-label="Name" />
				{#if tableTouched && nameError}<span class={errText}>{nameError}</span>{/if}
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => tableModal?.close()}>Cancel</button>
		<button type="submit" form="table-create-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={creating}>
			{creating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>

<Modal bind:this={entityModal} title={editing ? 'Edit entity' : 'Insert entity'} titleId="table-entity-title">
	{#snippet children()}
		<form id="table-entity-form" class="space-y-3" onsubmit={submitEntity} novalidate>
			<div class="grid grid-cols-2 gap-3">
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">PartitionKey</span>
					<input class={input} bind:value={formPk} disabled={Boolean(editing)} autocomplete="off" aria-label="PartitionKey" />
					{#if formTouched && pkError}<span class={errText}>{pkError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">RowKey</span>
					<input class={input} bind:value={formRk} disabled={Boolean(editing)} autocomplete="off" aria-label="RowKey" />
					{#if formTouched && rkError}<span class={errText}>{rkError}</span>{/if}
				</label>
			</div>

			{#if editing}
				<fieldset class="space-y-1 text-sm">
					<legend class="font-medium text-gray-700 dark:text-gray-300">Update mode</legend>
					<label class="flex items-start gap-2 text-gray-700 dark:text-gray-300">
						<input type="radio" bind:group={formMode} value="merge" /> <span><strong>Merge</strong>: change listed properties, keep the others</span>
					</label>
					<label class="flex items-start gap-2 text-gray-700 dark:text-gray-300">
						<input type="radio" bind:group={formMode} value="replace" /> <span><strong>Replace</strong>: the entity becomes exactly the listed properties</span>
					</label>
				</fieldset>
			{/if}

			<div class="space-y-2">
				<div class="flex items-center justify-between">
					<span class="text-sm font-medium text-gray-700 dark:text-gray-300">Properties</span>
					<button type="button" class="flex items-center gap-1 rounded-md border border-gray-300 dark:border-gray-600 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800" onclick={addProp}><Plus class="h-3 w-3" /> Add property</button>
				</div>
				{#each formProps as p, i (i)}
					<div class="grid grid-cols-[1fr_9rem_1fr_auto] items-start gap-2" data-testid="table-form-prop">
						<div>
							<input class={input} bind:value={p.name} placeholder="Name" autocomplete="off" aria-label="Property name {i + 1}" />
							{#if formTouched && propErrors[i]?.name}<span class={errText}>{propErrors[i].name}</span>{/if}
						</div>
						<select class={input} value={p.type} onchange={(ev) => changeType(i, ev.currentTarget.value as EdmType)} aria-label="Property type {i + 1}">
							{#each EDM_TYPES as t (t)}<option value={t}>{t.replace('Edm.', '')}</option>{/each}
						</select>
						<div>
							{#if p.type === 'Edm.Boolean'}
								<select class={input} bind:value={p.value} aria-label="Property value {i + 1}">
									<option value="true">true</option><option value="false">false</option>
								</select>
							{:else}
								<input class="{input} font-mono" bind:value={p.value} placeholder="Value" autocomplete="off" aria-label="Property value {i + 1}" />
							{/if}
							{#if formTouched && propErrors[i]?.value}<span class={errText}>{propErrors[i].value}</span>{/if}
						</div>
						<button type="button" class="rounded p-2 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove property {i + 1}" onclick={() => removeProp(i)}><Trash2 class="h-4 w-4" /></button>
					</div>
				{/each}
				{#if formProps.length === 0}<p class="text-xs text-gray-500 dark:text-gray-400">No properties. Binary values are base64 text; DateTime accepts ISO 8601.</p>{/if}
			</div>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => entityModal?.close()}>Cancel</button>
		<button type="submit" form="table-entity-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={saving}>
			{saving ? 'Saving…' : editing ? 'Save' : 'Insert'}
		</button>
	{/snippet}
</Modal>
