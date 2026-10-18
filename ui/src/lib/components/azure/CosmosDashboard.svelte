<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Database, Plus, Trash2, Pencil, RefreshCw, X, Inbox, Copy, Play, Layers } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		DEFAULT_PAGE_SIZE,
		DEFAULT_QUERY,
		PAGE_SIZES,
		CosmosError,
		cellText,
		createContainer,
		createDatabase,
		deleteContainer,
		deleteDatabase,
		deleteDocument,
		documentColumns,
		etagOf,
		tsOf,
		editableText,
		listContainers,
		listDatabases,
		listDocuments,
		newDocumentText,
		pageCount,
		paginate,
		parseDocumentText,
		partitionKeyValue,
		queryDocuments,
		referencedParams,
		replaceDocument,
		saveDocument,
		validateContainerId,
		validateDatabaseId,
		validateParam,
		validatePartitionKeyPath,
		type ContainerInfo,
		type CosmosDocument,
		type DatabaseInfo,
		type QueryParam
	} from '#lib/azurecosmos.ts';

	type Tab = 'items' | 'query' | 'settings';

	let databases = $state<DatabaseInfo[]>([]);
	let containers = $state<ContainerInfo[]>([]);
	let currentDb = $state<string | null>(null);
	let currentColl = $state<string | null>(null);
	let container = $state<ContainerInfo | null>(null);
	let tab = $state<Tab>('items');
	let loading = $state(true);
	let loadError = $state('');
	let loadingDocs = $state(false);

	let docs = $state<CosmosDocument[]>([]);
	let docsError = $state('');
	let selected = $state<CosmosDocument | null>(null);
	let page = $state(0);
	let pageSize = $state<number>(DEFAULT_PAGE_SIZE);
	let viewRaw = $state(false);
	let charge = $state('');
	let ranQuery = $state(false);

	let queryText = $state(DEFAULT_QUERY);
	let params = $state<QueryParam[]>([]);

	let dbModal = $state<Modal | null>(null);
	let dbName = $state('');
	let dbTouched = $state(false);
	let dbBusy = $state(false);

	let collModal = $state<Modal | null>(null);
	let collName = $state('');
	let collPk = $state('/id');
	let collTouched = $state(false);
	let collBusy = $state(false);

	let itemModal = $state<Modal | null>(null);
	let editing = $state<CosmosDocument | null>(null);
	let itemText = $state('');
	let upsert = $state(false);
	let itemTouched = $state(false);
	let itemBusy = $state(false);

	const dbError = $derived(validateDatabaseId(dbName.trim()));
	const collError = $derived(validateContainerId(collName.trim()));
	const pkError = $derived(validatePartitionKeyPath(collPk.trim()));
	const itemCheck = $derived(parseDocumentText(itemText));
	const paramErrors = $derived(params.map((p) => validateParam(p)));
	const missingParams = $derived(
		referencedParams(queryText).filter((n) => !params.some((p) => p.name === n))
	);
	const columns = $derived(documentColumns(docs));
	const pages = $derived(pageCount(docs.length, pageSize));
	const visible = $derived(paginate(docs, page, pageSize));
	const pkPath = $derived(container?.partitionKeyPath ?? '');

	function describe(e: unknown): string {
		if (e instanceof CosmosError) return e.code ? `${e.code}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	async function loadDatabases() {
		loading = true;
		loadError = '';
		try {
			databases = (await listDatabases()).toSorted((a, b) => a.id.localeCompare(b.id));
			if (currentDb && !databases.some((d) => d.id === currentDb)) selectDatabase(null);
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load databases: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadContainers() {
		if (!currentDb) return;
		try {
			containers = (await listContainers(currentDb)).toSorted((a, b) => a.id.localeCompare(b.id));
			if (currentColl && !containers.some((c) => c.id === currentColl)) selectContainer(null);
			else if (currentColl) container = containers.find((c) => c.id === currentColl) ?? null;
		} catch (e) {
			toast.error(`Failed to load containers: ${describe(e)}`);
		}
	}

	async function loadItems() {
		if (!currentDb || !currentColl) return;
		loadingDocs = true;
		docsError = '';
		try {
			docs = await listDocuments(currentDb, currentColl);
			charge = '';
			ranQuery = false;
			keepSelection();
		} catch (e) {
			docsError = describe(e);
			docs = [];
		} finally {
			loadingDocs = false;
		}
	}

	async function runQuery(event?: Event) {
		event?.preventDefault();
		if (!currentDb || !currentColl || paramErrors.some((e) => e !== null)) return;
		loadingDocs = true;
		docsError = '';
		page = 0;
		try {
			const res = await queryDocuments(currentDb, currentColl, queryText, params);
			docs = res.documents;
			charge = res.charge;
			ranQuery = true;
			keepSelection();
		} catch (e) {
			docsError = describe(e);
			docs = [];
			ranQuery = true;
		} finally {
			loadingDocs = false;
		}
	}

	function keepSelection() {
		page = Math.min(page, pageCount(docs.length, pageSize) - 1);
		const cur = selected;
		if (cur) selected = docs.find((d) => d.id === cur.id && sameKey(d, cur)) ?? null;
	}

	const sameKey = (a: CosmosDocument, b: CosmosDocument) =>
		cellText(partitionKeyValue(a, pkPath)) === cellText(partitionKeyValue(b, pkPath));

	async function refresh() {
		await loadDatabases();
		await loadContainers();
		await reloadDocs();
	}

	const reloadDocs = () => (tab === 'query' && ranQuery ? runQuery() : loadItems());

	function selectDatabase(id: string | null) {
		currentDb = id;
		containers = [];
		selectContainer(null);
		if (id) void loadContainers();
	}

	function selectContainer(id: string | null) {
		currentColl = id;
		container = containers.find((c) => c.id === id) ?? null;
		docs = [];
		selected = null;
		docsError = '';
		page = 0;
		charge = '';
		ranQuery = false;
		tab = 'items';
		if (id) void loadItems();
	}

	function setTab(t: Tab) {
		tab = t;
		selected = null;
		docsError = '';
		if (t === 'items') void loadItems();
		else if (t === 'query') {
			docs = [];
			ranQuery = false;
			charge = '';
		}
	}

	function openNewDb() {
		dbName = '';
		dbTouched = false;
		dbModal?.open();
	}

	async function submitDb(event: Event) {
		event.preventDefault();
		dbTouched = true;
		if (dbError || dbBusy) return;
		dbBusy = true;
		const id = dbName.trim();
		try {
			await createDatabase(id);
			toast.success(`Created database ${id}`);
			dbModal?.close('created');
			await loadDatabases();
			selectDatabase(id);
		} catch (e) {
			toast.error(`Failed to create ${id}: ${describe(e)}`);
		} finally {
			dbBusy = false;
		}
	}

	async function removeDb(d: DatabaseInfo) {
		if (!(await confirmDestructive({ title: 'Delete database', message: `Delete database "${d.id}" with all of its containers and items? This cannot be undone.` }))) return;
		try {
			await deleteDatabase(d.id);
			toast.success(`Deleted database ${d.id}`);
			if (currentDb === d.id) selectDatabase(null);
			await loadDatabases();
		} catch (e) {
			toast.error(`Failed to delete ${d.id}: ${describe(e)}`);
		}
	}

	function openNewColl() {
		collName = '';
		collPk = '/id';
		collTouched = false;
		collModal?.open();
	}

	async function submitColl(event: Event) {
		event.preventDefault();
		collTouched = true;
		if (!currentDb || collError || pkError || collBusy) return;
		collBusy = true;
		const id = collName.trim();
		try {
			await createContainer(currentDb, id, collPk.trim());
			toast.success(`Created container ${id}`);
			collModal?.close('created');
			await loadContainers();
			selectContainer(id);
		} catch (e) {
			toast.error(`Failed to create ${id}: ${describe(e)}`);
		} finally {
			collBusy = false;
		}
	}

	async function removeColl(c: ContainerInfo) {
		if (!currentDb) return;
		if (!(await confirmDestructive({ title: 'Delete container', message: `Delete container "${c.id}" and all of its items? This cannot be undone.` }))) return;
		try {
			await deleteContainer(currentDb, c.id);
			toast.success(`Deleted container ${c.id}`);
			if (currentColl === c.id) selectContainer(null);
			await loadContainers();
		} catch (e) {
			toast.error(`Failed to delete ${c.id}: ${describe(e)}`);
		}
	}

	function openItemForm(doc: CosmosDocument | null) {
		editing = doc;
		itemText = doc ? editableText(doc) : newDocumentText(pkPath);
		upsert = false;
		itemTouched = false;
		itemModal?.open();
	}

	async function submitItem(event: Event) {
		event.preventDefault();
		itemTouched = true;
		const doc = itemCheck.doc;
		if (!currentDb || !currentColl || !doc || itemBusy) return;
		itemBusy = true;
		try {
			if (editing) {
				const id = String(editing.id);
				await replaceDocument(currentDb, currentColl, id, partitionKeyValue(editing, pkPath), doc, etagOf(editing));
				toast.success(`Replaced item ${id}`);
			} else {
				await saveDocument(currentDb, currentColl, doc, upsert);
				toast.success(`${upsert ? 'Upserted' : 'Created'} item ${String(doc.id)}`);
			}
			itemModal?.close('saved');
			await reloadDocs();
		} catch (e) {
			toast.error(`Failed to save item: ${describe(e)}`);
		} finally {
			itemBusy = false;
		}
	}

	async function removeItem(doc: CosmosDocument) {
		if (!currentDb || !currentColl) return;
		const id = String(doc.id);
		if (!(await confirmDestructive({ title: 'Delete item', message: `Delete item "${id}"? This cannot be undone.` }))) return;
		try {
			await deleteDocument(currentDb, currentColl, id, partitionKeyValue(doc, pkPath), etagOf(doc));
			toast.success(`Deleted item ${id}`);
			selected = null;
			await reloadDocs();
		} catch (e) {
			toast.error(`Failed to delete item: ${describe(e)}`);
		}
	}

	function addParam(name = '') {
		params = [...params, { name, value: '' }];
	}

	async function copy(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	const isSelected = (d: CosmosDocument) => selected !== null && selected.id === d.id && sameKey(selected, d);
	const pretty = (v: unknown) => JSON.stringify(v, null, 2);
	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const btn = 'rounded-md border border-gray-300 dark:border-gray-600 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const primary = 'flex items-center gap-2 rounded-md bg-blue-600 px-3 py-1.5 text-sm text-white hover:bg-blue-700 disabled:opacity-50';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const th = 'px-4 py-3 font-medium whitespace-nowrap';
	const td = 'px-4 py-3 text-gray-600 dark:text-gray-400 max-w-xs truncate';
	const errText = 'block text-xs text-red-600 dark:text-red-400';
	const tabCls = (t: Tab) =>
		`px-4 py-2 text-sm font-medium border-b-2 ${tab === t ? 'border-blue-600 text-blue-700 dark:text-blue-400' : 'border-transparent text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}`;

	onMount(loadDatabases);
</script>

<div class="space-y-6 pb-20" data-testid="cosmos-dashboard">
	<PageHeader
		icon={Database}
		title="Cosmos DB"
		description="Azure Cosmos DB (SQL API): databases, containers and items"
		color="blue"
		onRefresh={refresh}
	>
		{#snippet actions()}
			<button class={primary} onclick={openNewDb} disabled={loading || Boolean(loadError)}>
				<Plus class="h-4 w-4" /> New database
			</button>
		{/snippet}
	</PageHeader>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="cosmos-error">
			<p class="font-medium">Could not reach Cosmos DB</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadDatabases}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<div class="space-y-6">
				<section aria-label="Databases" class="space-y-2">
					<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Databases</h2>
					{#if loading}
						<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
					{:else if databases.length === 0}
						<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="cosmos-no-databases">
							<Inbox class="h-10 w-10 mb-2 opacity-30" />
							<p class="text-sm">No databases</p>
							<p class="text-xs">Create a database to get started</p>
						</div>
					{:else}
						<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
							{#each databases as d (d.id)}
								<li class="flex items-center {currentDb === d.id ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="cosmos-db-row">
									<button class="flex-1 truncate px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectDatabase(d.id)}>{d.id}</button>
									<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {d.id}" aria-label="Delete database {d.id}" onclick={() => removeDb(d)}><Trash2 class="h-4 w-4" /></button>
								</li>
							{/each}
						</ul>
					{/if}
				</section>

				{#if currentDb}
					<section aria-label="Containers" class="space-y-2">
						<div class="flex items-center justify-between">
							<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Containers in {currentDb}</h2>
							<button class={btn} onclick={openNewColl}><Plus class="inline h-3 w-3" /> New</button>
						</div>
						{#if containers.length === 0}
							<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="cosmos-no-containers">No containers</p>
						{:else}
							<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
								{#each containers as c (c.id)}
									<li class="flex items-center {currentColl === c.id ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="cosmos-coll-row">
										<button class="flex-1 truncate px-3 py-2 text-left text-sm hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectContainer(c.id)}>
											<span class="font-medium text-gray-900 dark:text-gray-100">{c.id}</span>
											<span class="ml-2 font-mono text-xs text-gray-500 dark:text-gray-400">{c.partitionKeyPath}</span>
										</button>
										<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {c.id}" aria-label="Delete container {c.id}" onclick={() => removeColl(c)}><Trash2 class="h-4 w-4" /></button>
									</li>
								{/each}
							</ul>
						{/if}
					</section>
				{/if}
			</div>

			<section aria-label="Container" class="space-y-4 min-w-0">
				{#if !currentColl || !container}
					<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="cosmos-select-hint">
						<Layers class="h-12 w-12 mb-3 opacity-30" />
						<p>{currentDb ? 'Select a container to browse its items' : 'Select a database, then a container'}</p>
					</div>
				{:else}
					<div class="flex flex-wrap items-center justify-between gap-2">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white break-all" data-testid="cosmos-current">{currentDb} / {currentColl}</h2>
						<button class={primary} onclick={() => openItemForm(null)}><Plus class="h-4 w-4" /> New item</button>
					</div>

					<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700" role="tablist">
						<button role="tab" aria-selected={tab === 'items'} class={tabCls('items')} onclick={() => setTab('items')} data-testid="cosmos-tab-items">Items</button>
						<button role="tab" aria-selected={tab === 'query'} class={tabCls('query')} onclick={() => setTab('query')} data-testid="cosmos-tab-query">SQL query</button>
						<button role="tab" aria-selected={tab === 'settings'} class={tabCls('settings')} onclick={() => setTab('settings')} data-testid="cosmos-tab-settings">Properties</button>
					</div>

					{#if tab === 'query'}
						<form class="space-y-3" onsubmit={runQuery} data-testid="cosmos-query-form">
							<label class="block space-y-1 text-sm">
								<span class="font-medium text-gray-700 dark:text-gray-300">Query</span>
								<textarea class="{input} font-mono" rows="3" bind:value={queryText} spellcheck="false" aria-label="SQL query" data-testid="cosmos-query-text"></textarea>
							</label>
							<div class="space-y-2">
								<div class="flex items-center gap-2">
									<span class="text-sm font-medium text-gray-700 dark:text-gray-300">Parameters</span>
									<button type="button" class={btn} onclick={() => addParam()}><Plus class="inline h-3 w-3" /> Add</button>
									{#each missingParams as n (n)}
										<button type="button" class="rounded-md border border-amber-300 bg-amber-50 px-2 py-1 text-xs text-amber-800 dark:border-amber-800 dark:bg-amber-950/40 dark:text-amber-300" onclick={() => addParam(n)}>Add {n}</button>
									{/each}
								</div>
								{#each params as p, i (i)}
									<div class="grid grid-cols-[12rem_1fr_auto] items-start gap-2" data-testid="cosmos-param">
										<div>
											<input class="{input} font-mono" bind:value={p.name} placeholder="@name" autocomplete="off" aria-label="Parameter name {i + 1}" />
											{#if paramErrors[i]}<span class={errText}>{paramErrors[i]}</span>{/if}
										</div>
										<input class="{input} font-mono" bind:value={p.value} placeholder="value (JSON, or plain text)" autocomplete="off" aria-label="Parameter value {i + 1}" />
										<button type="button" class="rounded p-2 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove parameter {i + 1}" onclick={() => (params = params.filter((_, k) => k !== i))}><Trash2 class="h-4 w-4" /></button>
									</div>
								{/each}
							</div>
							<button type="submit" class={primary} disabled={loadingDocs || paramErrors.some((e) => e !== null)} data-testid="cosmos-run-query"><Play class="h-4 w-4" /> Run query</button>
						</form>
					{/if}

					{#if tab !== 'settings'}
						{#if docsError}
							<div class="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="cosmos-docs-error">{docsError}</div>
						{:else if loadingDocs && docs.length === 0}
							<div class="flex justify-center py-6"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
						{:else if tab === 'query' && !ranQuery}
							<p class="text-sm text-gray-500 dark:text-gray-400">Run a query to see results.</p>
						{:else if docs.length === 0}
							<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="cosmos-empty">
								<Inbox class="h-10 w-10 mb-2 opacity-30" />
								<p class="text-sm">{tab === 'query' ? 'The query returned no results' : 'No items'}</p>
							</div>
						{:else}
							<div class="flex items-center justify-between gap-2 text-xs text-gray-500 dark:text-gray-400">
								<span data-testid="cosmos-count">{docs.length} item{docs.length === 1 ? '' : 's'}{charge ? ` · ${charge} RU` : ''}</span>
								<div class="flex gap-1">
									<button class="{btn} {viewRaw ? '' : 'bg-gray-100 dark:bg-slate-800'}" onclick={() => (viewRaw = false)}>Grid</button>
									<button class="{btn} {viewRaw ? 'bg-gray-100 dark:bg-slate-800' : ''}" onclick={() => (viewRaw = true)} data-testid="cosmos-raw-toggle">Raw JSON</button>
								</div>
							</div>
							{#if viewRaw}
								<pre class="max-h-96 overflow-auto rounded-lg border border-gray-200 dark:border-gray-700 p-3 font-mono text-xs text-gray-800 dark:text-gray-200" data-testid="cosmos-raw">{pretty(visible)}</pre>
							{:else}
								<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
									<table class="w-full text-sm" data-testid="cosmos-items">
										<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
											<tr>{#each columns as c (c)}<th class={th}>{c}</th>{/each}</tr>
										</thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each visible as d, i (i)}
												<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {isSelected(d) ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="cosmos-item-row" onclick={() => (selected = d)}>
													{#each columns as c (c)}<td class={c === 'id' ? 'px-4 py-3 font-mono text-xs text-gray-900 dark:text-gray-100' : td}>{cellText(d[c])}</td>{/each}
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
							<div class="flex flex-wrap items-center justify-between gap-2 text-sm">
								<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">Per page
									<select class="{input} w-20" bind:value={pageSize} onchange={() => (page = 0)} aria-label="Page size">
										{#each PAGE_SIZES as n (n)}<option value={n}>{n}</option>{/each}
									</select>
								</label>
								<div class="flex items-center gap-2">
									<button class={btn} onclick={() => (page -= 1)} disabled={page === 0} aria-label="Previous page">Prev</button>
									<span class="text-xs text-gray-500 dark:text-gray-400" data-testid="cosmos-page">Page {page + 1} of {pages}</span>
									<button class={btn} onclick={() => (page += 1)} disabled={page + 1 >= pages} aria-label="Next page">Next</button>
								</div>
							</div>
						{/if}

						{#if selected}
							<section class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-4" data-testid="cosmos-item-detail" aria-label="Item {String(selected.id)}">
								<div class="flex items-start justify-between gap-4">
									<div class="min-w-0">
										<h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{String(selected.id)}</h3>
										<p class="text-xs text-gray-500 dark:text-gray-400">Item in {currentColl}</p>
									</div>
									<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (selected = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
								</div>
								<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
									<dt class={dt}>Partition key</dt><dd class="{dd} font-mono text-xs">{pkPath} = {cellText(partitionKeyValue(selected, pkPath)) || 'null'}</dd>
									<dt class={dt}>_etag</dt>
									<dd class="{dd} flex items-start gap-2">
										<span class="font-mono text-xs" data-testid="cosmos-item-etag">{etagOf(selected) || '—'}</span>
										{#if etagOf(selected)}<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy etag" onclick={() => selected && copy(etagOf(selected))}><Copy class="h-3.5 w-3.5" /></button>{/if}
									</dd>
									<dt class={dt}>_ts</dt><dd class={dd}>{tsOf(selected) ? `${tsOf(selected)} (${new Date((tsOf(selected) ?? 0) * 1000).toISOString()})` : '—'}</dd>
								</dl>
								<pre class="max-h-80 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-800/60 p-3 font-mono text-xs text-gray-800 dark:text-gray-200" data-testid="cosmos-item-json">{pretty(selected)}</pre>
								<div class="flex flex-wrap gap-2">
									<button class={primary} onclick={() => selected && openItemForm(selected)}><Pencil class="h-4 w-4" /> Edit</button>
									<button class={btn} onclick={() => selected && copy(pretty(selected))}><Copy class="inline h-4 w-4" /> Copy JSON</button>
									<button class="flex items-center gap-2 rounded-md border border-red-200 dark:border-red-900 px-3 py-1.5 text-sm text-red-600 hover:bg-red-50 dark:hover:bg-red-950" onclick={() => selected && removeItem(selected)}><Trash2 class="h-4 w-4" /> Delete</button>
								</div>
							</section>
						{/if}
					{:else}
						<section class="space-y-4" data-testid="cosmos-properties">
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
								<dt class={dt}>id</dt><dd class={dd}>{container.id}</dd>
								<dt class={dt}>partitionKey</dt><dd class="{dd} font-mono text-xs">{container.partitionKeyPath} ({container.partitionKeyKind}{container.partitionKeyVersion ? `, v${container.partitionKeyVersion}` : ''})</dd>
								<dt class={dt}>_rid</dt><dd class="{dd} font-mono text-xs">{container.rid}</dd>
								<dt class={dt}>_etag</dt><dd class="{dd} font-mono text-xs">{container.etag || '—'}</dd>
								<dt class={dt}>_ts</dt><dd class={dd}>{container.ts ?? '—'}</dd>
								<dt class={dt}>indexingPolicy</dt>
								<dd class={dd}>{container.indexingPolicy ? '' : 'Not modeled by the emulator (every property is queryable)'}</dd>
							</dl>
							{#if container.indexingPolicy}
								<pre class="overflow-auto rounded-lg bg-gray-50 dark:bg-slate-800/60 p-3 font-mono text-xs">{pretty(container.indexingPolicy)}</pre>
							{/if}
							<div>
								<h3 class="mb-1 text-sm font-semibold text-gray-700 dark:text-gray-300">Raw container resource</h3>
								<pre class="max-h-80 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-800/60 p-3 font-mono text-xs text-gray-800 dark:text-gray-200" data-testid="cosmos-container-json">{pretty(container.raw)}</pre>
							</div>
						</section>
					{/if}

					<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="cosmos-note">
						The emulator returns every result in one response (no continuation tokens), so this page pages them client-side. It does not model throughput, indexing policies, TTL or multi-region accounts. Numbers beyond 2^53 lose precision in the browser.
					</p>
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={dbModal} title="New database" titleId="cosmos-db-title">
	{#snippet children()}
		<form id="cosmos-db-form" class="space-y-3" onsubmit={submitDb} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Database id</span>
				<input class={input} bind:value={dbName} autocomplete="off" aria-label="Database id" />
				{#if dbTouched && dbError}<span class={errText}>{dbError}</span>{/if}
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={btn} onclick={() => dbModal?.close()}>Cancel</button>
		<button type="submit" form="cosmos-db-form" class={primary} disabled={dbBusy}>{dbBusy ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={collModal} title="New container" titleId="cosmos-coll-title">
	{#snippet children()}
		<form id="cosmos-coll-form" class="space-y-3" onsubmit={submitColl} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Container id</span>
				<input class={input} bind:value={collName} autocomplete="off" aria-label="Container id" />
				{#if collTouched && collError}<span class={errText}>{collError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Partition key path</span>
				<input class="{input} font-mono" bind:value={collPk} autocomplete="off" aria-label="Partition key path" />
				{#if collTouched && pkError}<span class={errText}>{pkError}</span>{/if}
				<span class="block text-xs text-gray-500 dark:text-gray-400">Exactly one path; it cannot be changed later. Throughput is not modeled.</span>
			</label>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={btn} onclick={() => collModal?.close()}>Cancel</button>
		<button type="submit" form="cosmos-coll-form" class={primary} disabled={collBusy}>{collBusy ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={itemModal} title={editing ? 'Edit item' : 'New item'} titleId="cosmos-item-title">
	{#snippet children()}
		<form id="cosmos-item-form" class="space-y-3" onsubmit={submitItem} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">JSON document</span>
				<textarea class="{input} font-mono" rows="14" bind:value={itemText} spellcheck="false" aria-label="JSON document" data-testid="cosmos-item-editor"></textarea>
				{#if itemTouched && itemCheck.error}<span class={errText} data-testid="cosmos-item-error">{itemCheck.error}</span>{/if}
			</label>
			{#if editing}
				<p class="text-xs text-gray-500 dark:text-gray-400">Saving replaces the whole item. The id and partition key value must stay the same; the stored _etag is sent as If-Match.</p>
			{:else}
				<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300">
					<input type="checkbox" bind:checked={upsert} /> Upsert (replace an existing item with the same id and partition key)
				</label>
			{/if}
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={btn} onclick={() => itemModal?.close()}>Cancel</button>
		<button type="submit" form="cosmos-item-form" class={primary} disabled={itemBusy}>{itemBusy ? 'Saving…' : editing ? 'Save' : upsert ? 'Upsert' : 'Create'}</button>
	{/snippet}
</Modal>
