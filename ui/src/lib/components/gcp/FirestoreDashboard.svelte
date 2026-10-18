<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { ChevronRight, FileText, Flame, Folder, Plus, Play, Radio, RefreshCw, Save, Sigma, Trash2, Undo2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import FirestoreDocEditor from '#lib/components/gcp/FirestoreDocEditor.svelte';
	import FirestoreFilterGroup from '#lib/components/gcp/FirestoreFilterGroup.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		buildStructuredQuery,
		createDocument,
		deleteDocument,
		docsSignature,
		fieldsToNodes,
		getDocument,
		gridColumns,
		listCollectionIds,
		listDocuments,
		newQuerySpec,
		nodesToFields,
		readStoredFirestoreDatabase,
		readStoredFirestoreProject,
		resolveFields,
		runAggregation,
		runQuery,
		saveDocument,
		valueSummary,
		valueType,
		writeStoredFirestore,
		type AggOp,
		type FsDoc,
		type FsNode
	} from '#lib/gcpfirestore.ts';

	const POLL_MS = 2000;
	const describe = describeGcpError;
	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const small =
		'rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const btn = 'rounded-md bg-orange-600 px-3 py-1 text-sm text-white hover:bg-orange-700 disabled:opacity-50';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const cell = 'px-3 py-2';

	let project = $state(readStoredFirestoreProject());
	let database = $state(readStoredFirestoreDatabase());
	let projectInput = $state(readStoredFirestoreProject());
	let databaseInput = $state(readStoredFirestoreDatabase());
	const ctx = $derived({ project, database });

	let segments = $state<string[]>([]);
	const collPath = $derived(segments.length === 0 ? null : segments.length % 2 === 1 ? segments : segments.slice(0, -1));
	const docPath = $derived(segments.length > 0 && segments.length % 2 === 0 ? segments : null);
	const parentDoc = $derived(collPath ? collPath.slice(0, -1) : []);
	const key = (p: string[] | null) => (p ? p.join('/') : '');

	let collections = $state<string[]>([]);
	let docs = $state<FsDoc[]>([]);
	let nextToken = $state('');
	let loadError = $state('');
	let loadingCollections = $state(true);
	let loadingDocs = $state(false);
	let navSeq = 0;

	let doc = $state<FsDoc | null>(null);
	let subcollections = $state<string[]>([]);
	let editNodes = $state<FsNode[]>([]);
	let editMode = $state<'typed' | 'raw'>('typed');
	let editRaw = $state('');
	let saveMode = $state<'merge' | 'replace'>('merge');
	let baseline = $state('');
	let remoteNote = $state('');
	let busy = $state('');

	let tab = $state<'documents' | 'query'>('documents');
	let spec = $state(newQuerySpec());
	let results = $state<FsDoc[] | null>(null);
	let aggOp = $state<AggOp>('count');
	let aggField = $state('');
	let aggResult = $state('');

	let listen = $state(false);
	let lastChange = $state('');
	let polling = false;

	let modal = $state<Modal | null>(null);
	let createKind = $state<'doc' | 'collection'>('doc');
	let fCollection = $state('');
	let fDocId = $state('');
	let fNodes = $state<FsNode[]>([]);
	let fMode = $state<'typed' | 'raw'>('typed');
	let fRaw = $state('{}');

	const dirty = $derived.by(() => {
		if (!doc) return false;
		const r = resolveFields(editMode, editNodes, editRaw, ctx);
		return 'error' in r ? true : JSON.stringify(r.fields) !== baseline;
	});

	async function run(label: string, action: () => Promise<void>, success: string, failure: string): Promise<boolean> {
		busy = label;
		try {
			await action();
			if (success) toast.success(success);
			return true;
		} catch (e) {
			toast.error(`${failure}: ${describe(e)}`);
			return false;
		} finally {
			busy = '';
		}
	}

	async function loadCollections() {
		loadingCollections = true;
		try {
			collections = await listCollectionIds(ctx, parentDoc);
			loadError = '';
		} catch (e) {
			loadError = describe(e);
		} finally {
			loadingCollections = false;
		}
	}

	async function loadDocs() {
		if (!collPath) {
			docs = [];
			nextToken = '';
			return;
		}
		loadingDocs = true;
		const seq = navSeq;
		try {
			const page = await listDocuments(ctx, collPath);
			if (seq !== navSeq) return;
			docs = page.docs;
			nextToken = page.next;
		} catch (e) {
			toast.error(`Failed to list documents: ${describe(e)}`);
		} finally {
			loadingDocs = false;
		}
	}

	async function loadMore() {
		if (!collPath || !nextToken) return;
		loadingDocs = true;
		try {
			const page = await listDocuments(ctx, collPath, nextToken);
			docs = [...docs, ...page.docs];
			nextToken = page.next;
		} catch (e) {
			toast.error(`Failed to list documents: ${describe(e)}`);
		} finally {
			loadingDocs = false;
		}
	}

	function adopt(d: FsDoc) {
		doc = d;
		editNodes = fieldsToNodes(d.fields);
		editMode = 'typed';
		editRaw = '';
		baseline = JSON.stringify(nodesToFields(fieldsToNodes(d.fields), ctx));
		remoteNote = '';
	}

	async function openDoc() {
		const path = docPath;
		if (!path) {
			doc = null;
			subcollections = [];
			return;
		}
		const seq = navSeq;
		try {
			const [d, subs] = await Promise.all([getDocument(ctx, path), listCollectionIds(ctx, path)]);
			if (seq !== navSeq) return;
			adopt(d);
			subcollections = subs;
		} catch (e) {
			if (seq !== navSeq) return;
			doc = null;
			subcollections = await listCollectionIds(ctx, path).catch(() => []);
			toast.error(`Document ${path.join('/')} is not available: ${describe(e)}`);
		}
	}

	async function navigate(next: string[]) {
		const prevColl = key(collPath);
		const prevParent = key(parentDoc);
		navSeq++;
		segments = next;
		doc = null;
		subcollections = [];
		const jobs: Promise<void>[] = [];
		if (key(collPath) !== prevColl) {
			docs = [];
			nextToken = '';
			results = null;
			aggResult = '';
			spec = newQuerySpec();
			tab = 'documents';
			jobs.push(loadDocs());
		}
		if (key(parentDoc) !== prevParent) jobs.push(loadCollections());
		jobs.push(openDoc());
		await Promise.all(jobs);
	}

	async function refresh() {
		navSeq++;
		await Promise.all([loadCollections(), loadDocs(), openDoc()]);
		if (tab === 'query' && results) await execQuery(false);
	}

	function applyConnection(event: Event) {
		event.preventDefault();
		const p = projectInput.trim();
		const d = databaseInput.trim();
		if (!p || !d) return;
		project = p;
		database = d;
		writeStoredFirestore(p, d);
		listen = false;
		segments = [];
		doc = null;
		docs = [];
		results = null;
		navSeq++;
		void loadCollections();
	}

	// ---- save / create / delete ----

	async function save() {
		const path = docPath;
		if (!path || !doc) return;
		const r = resolveFields(editMode, editNodes, editRaw, ctx);
		if ('error' in r) return void toast.error(r.error);
		const ok = await run('save', async () => {
			adopt(await saveDocument(ctx, path, r.fields, saveMode, true));
		}, `Saved ${path.join('/')} (${saveMode})`, 'Failed to save');
		if (ok) await loadDocs();
	}

	async function removeDoc() {
		const path = docPath;
		if (!path) return;
		const where = path.join('/');
		if (!(await confirmDestructive({ title: 'Delete document', message: `Delete document "${where}"? Its subcollections are not deleted. This cannot be undone.` }))) return;
		const ok = await run('delete', () => deleteDocument(ctx, path), `Deleted ${where}`, `Failed to delete ${where}`);
		if (ok) {
			await navigate(path.slice(0, -1));
			await loadCollections();
		}
	}

	function openCreate(kind: 'doc' | 'collection') {
		createKind = kind;
		fCollection = '';
		fDocId = '';
		fNodes = [];
		fMode = 'typed';
		fRaw = '{}';
		modal?.open();
	}

	async function submitCreate(event: Event) {
		event.preventDefault();
		let target: string[];
		if (createKind === 'collection') {
			const id = fCollection.trim();
			if (!id) return void toast.error('Collection ID is required');
			target = [...parentDoc, id];
		} else if (collPath) {
			target = collPath;
		} else {
			return;
		}
		const r = resolveFields(fMode, fNodes, fRaw, ctx);
		if ('error' in r) return void toast.error(r.error);
		let created: FsDoc | null = null;
		const ok = await run('create', async () => {
			created = await createDocument(ctx, target, fDocId.trim(), r.fields);
		}, 'Document created', 'Failed to create document');
		if (ok && created) {
			modal?.close('created');
			await navigate((created as FsDoc).segs);
			await Promise.all([loadCollections(), loadDocs()]);
		}
	}

	// ---- query / aggregation ----

	async function execQuery(announce = true) {
		if (!collPath) return;
		const built = buildStructuredQuery(collPath.at(-1) ?? '', spec, ctx);
		if ('error' in built) return void (announce && toast.error(built.error));
		busy = 'query';
		try {
			results = await runQuery(ctx, collPath.slice(0, -1), built.ok);
		} catch (e) {
			if (announce) toast.error(`Query failed: ${describe(e)}`);
		} finally {
			busy = '';
		}
	}

	async function execAggregation(op: AggOp) {
		if (!collPath) return;
		aggOp = op;
		if (op !== 'count' && !aggField.trim()) return void toast.error(`${op} needs a field`);
		const built = tab === 'query' ? buildStructuredQuery(collPath.at(-1) ?? '', spec, ctx) : { ok: { from: [{ collectionId: collPath.at(-1) }] } };
		if ('error' in built) return void toast.error(built.error);
		await run('agg', async () => {
			const v = await runAggregation(ctx, collPath.slice(0, -1), op, aggField, built.ok);
			aggResult = `${op}${op === 'count' ? '' : `(${aggField.trim()})`} = ${v}`;
		}, '', 'Aggregation failed');
	}

	// ---- live updates ----

	async function poll() {
		const coll = collPath;
		if (polling || !coll || document.hidden) return;
		polling = true;
		const seq = navSeq;
		try {
			const size = Math.min(Math.max(docs.length, 50), 300);
			const [page, ids] = await Promise.all([listDocuments(ctx, coll, '', size), listCollectionIds(ctx, parentDoc)]);
			if (seq !== navSeq) return;
			let changed = false;
			if (docsSignature(page.docs) !== docsSignature(docs.slice(0, size))) {
				docs = page.docs;
				nextToken = page.next;
				changed = true;
			}
			if (ids.join('|') !== collections.join('|')) {
				collections = ids;
				changed = true;
			}
			if (tab === 'query' && results) {
				const before = docsSignature(results);
				await execQuery(false);
				changed ||= before !== docsSignature(results ?? []);
			}
			if (docPath) changed ||= await pollDoc(docPath);
			if (changed) lastChange = new Date().toLocaleTimeString();
		} catch {
			// transient errors are retried on the next tick
		} finally {
			polling = false;
		}
	}

	async function pollDoc(path: string[]): Promise<boolean> {
		try {
			const d = await getDocument(ctx, path);
			if (!doc || d.updateTime === doc.updateTime) return false;
			if (dirty) {
				remoteNote = 'This document changed elsewhere. Saving will overwrite those changes.';
				doc = { ...doc, updateTime: d.updateTime };
			} else {
				adopt(d);
			}
			subcollections = await listCollectionIds(ctx, path);
			return true;
		} catch {
			if (doc) {
				remoteNote = 'This document was deleted elsewhere.';
				doc = null;
				return true;
			}
			return false;
		}
	}

	$effect(() => {
		if (!listen) return;
		const t = setInterval(() => void poll(), POLL_MS);
		return () => clearInterval(t);
	});

	const typeColor = (v: unknown) => {
		const t = valueType(v);
		return t === 'string' ? '' : t === 'null' ? 'italic text-gray-400' : 'text-orange-700 dark:text-orange-300';
	};

	onMount(loadCollections);
</script>

{#snippet grid(rows: FsDoc[], showPath: boolean)}
	{@const cols = gridColumns(rows)}
	<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
		<table class="w-full text-sm" data-testid="fs-grid">
			<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
				<tr>
					<th class={cell}>{showPath ? 'Path' : 'Document ID'}</th>
					{#each cols as c (c)}<th class="{cell} font-mono">{c}</th>{/each}
				</tr>
			</thead>
			<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
				{#each rows as d (d.name)}
					<tr class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {key(docPath) === d.segs.join('/') ? 'bg-orange-50 dark:bg-orange-950/30' : ''}" onclick={() => navigate(d.segs)} data-testid="fs-doc-row">
						<td class="{cell} font-mono"><button type="button" class="text-left text-orange-700 dark:text-orange-300 hover:underline" onclick={(e) => { e.stopPropagation(); void navigate(d.segs); }}>{showPath ? d.segs.join('/') : d.id}</button></td>
						{#each cols as c (c)}
							<td class="{cell} max-w-[14rem] truncate {typeColor(d.fields[c])}" title={d.fields[c] ? valueSummary(d.fields[c]) : ''}>{d.fields[c] ? valueSummary(d.fields[c]) : ''}</td>
						{/each}
					</tr>
				{/each}
			</tbody>
		</table>
	</div>
{/snippet}

<div class="space-y-6 pb-20" data-testid="fs-dashboard">
	<PageHeader icon={Flame} title="Firestore" description="Google Cloud Firestore: collections, documents and queries" color="orange" onRefresh={refresh}>
		{#snippet actions()}
			<button class="flex items-center gap-2 rounded-lg bg-orange-600 px-3 py-2 text-sm text-white hover:bg-orange-700 disabled:opacity-50" onclick={() => openCreate('collection')} disabled={Boolean(loadError)} data-testid="fs-new-collection">
				<Plus class="h-4 w-4" /> New collection
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyConnection}>
		<label for="fs-project">Project</label>
		<input id="fs-project" class="w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="fs-project" />
		<label for="fs-database">Database</label>
		<input id="fs-database" class="w-32 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={databaseInput} data-testid="fs-database" />
		<button type="submit" class={small} disabled={!projectInput.trim() || !databaseInput.trim() || (projectInput.trim() === project && databaseInput.trim() === database)}>Switch</button>
	</form>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="fs-error">
			<p class="font-medium">Could not reach Firestore</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadCollections}>Retry</button>
		</div>
	{:else}
		<nav aria-label="Path" class="flex flex-wrap items-center gap-1 text-sm" data-testid="fs-breadcrumbs">
			<button class="rounded px-2 py-1 font-medium hover:bg-gray-100 dark:hover:bg-slate-800 {segments.length === 0 ? 'text-orange-700 dark:text-orange-300' : ''}" onclick={() => navigate([])} data-testid="fs-crumb-root">{database}</button>
			{#each segments as seg, i (i)}
				<ChevronRight class="h-4 w-4 text-gray-400" />
				<button class="flex items-center gap-1 rounded px-2 py-1 font-mono hover:bg-gray-100 dark:hover:bg-slate-800 {i === segments.length - 1 ? 'text-orange-700 dark:text-orange-300' : ''}" onclick={() => navigate(segments.slice(0, i + 1))} data-testid="fs-crumb">
					{#if i % 2 === 0}<Folder class="h-3.5 w-3.5" />{:else}<FileText class="h-3.5 w-3.5" />{/if}{seg}
				</button>
			{/each}
		</nav>

		<div class="grid gap-6 lg:grid-cols-[15rem_minmax(0,1fr)]">
			<section aria-label="Collections" class="space-y-2">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Collections{parentDoc.length ? ` in ${parentDoc.join('/')}` : ''}</h2>
				{#if loadingCollections}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if collections.length === 0}
					<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="fs-no-collections">No collections here</p>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
						{#each collections as c (c)}
							<li class={collPath && collPath.at(-1) === c ? 'bg-orange-50 dark:bg-orange-950/30' : ''}>
								<button class="flex w-full items-center gap-2 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => navigate([...parentDoc, c])} data-testid="fs-collection-row"><Folder class="h-4 w-4 text-orange-600" /><span class="truncate">{c}</span></button>
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<div class="grid min-w-0 gap-6 xl:grid-cols-[minmax(0,1.15fr)_minmax(0,1fr)]">
				<section aria-label="Documents" class="min-w-0 space-y-3">
					{#if !collPath}
						<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="fs-select-hint">Select a collection to browse its documents</p>
					{:else}
						<div class="flex flex-wrap items-center gap-2">
							<h2 class="mr-auto text-sm font-semibold text-gray-700 dark:text-gray-300 font-mono break-all">{collPath.join('/')}</h2>
							<button class="flex items-center gap-1 rounded-md border px-2 py-1 text-xs {listen ? 'border-green-500 bg-green-50 text-green-700 dark:bg-green-950/40 dark:text-green-300' : 'border-gray-200 dark:border-gray-700 hover:bg-gray-50 dark:hover:bg-slate-800'}" aria-pressed={listen} onclick={() => (listen = !listen)} data-testid="fs-listen">
								<Radio class="h-3.5 w-3.5 {listen ? 'animate-pulse' : ''}" /> {listen ? 'Listening' : 'Listen'}
							</button>
							<button class="flex items-center gap-1 {btn}" onclick={() => openCreate('doc')} data-testid="fs-new-doc"><Plus class="h-3.5 w-3.5" /> Add document</button>
						</div>
						{#if listen}
							<p class="text-[11px] text-green-700 dark:text-green-400" data-testid="fs-listen-status">Polling every {POLL_MS / 1000}s{lastChange ? ` · last change ${lastChange}` : ''}</p>
						{/if}

						<div class="flex flex-wrap items-center gap-2 rounded-lg border border-gray-200 dark:border-gray-700 px-3 py-2" data-testid="fs-agg">
							<Sigma class="h-4 w-4 text-gray-500" />
							<button class={small} onclick={() => execAggregation('count')} disabled={busy === 'agg'} data-testid="fs-count">Count</button>
							<select class="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 text-xs" bind:value={aggOp} aria-label="Aggregation" data-testid="fs-agg-op"><option value="count">count</option><option value="sum">sum</option><option value="avg">avg</option></select>
							{#if aggOp !== 'count'}
								<input class="w-28 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={aggField} placeholder="field" aria-label="Aggregation field" data-testid="fs-agg-field" />
							{/if}
							<button class={small} onclick={() => execAggregation(aggOp)} disabled={busy === 'agg'} data-testid="fs-agg-run">Run</button>
							{#if aggResult}<span class="rounded bg-orange-100 px-2 py-0.5 font-mono text-xs text-orange-800 dark:bg-orange-950/50 dark:text-orange-200" data-testid="fs-agg-result">{aggResult}</span>{/if}
							<span class="text-[11px] text-gray-500 dark:text-gray-400">{tab === 'query' ? 'uses the query filters' : 'whole collection'}</span>
						</div>

						<div class="flex gap-2 border-b border-gray-200 dark:border-gray-700 text-sm">
							<button class="px-3 py-1.5 {tab === 'documents' ? 'border-b-2 border-orange-600 font-medium' : 'text-gray-500'}" onclick={() => (tab = 'documents')} data-testid="fs-tab-documents">Documents</button>
							<button class="px-3 py-1.5 {tab === 'query' ? 'border-b-2 border-orange-600 font-medium' : 'text-gray-500'}" onclick={() => (tab = 'query')} data-testid="fs-tab-query">Query</button>
						</div>

						{#if tab === 'documents'}
							{#if loadingDocs && docs.length === 0}
								<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
							{:else if docs.length === 0}
								<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="fs-no-docs">No documents in this collection</p>
							{:else}
								{@render grid(docs, false)}
								{#if nextToken}<button class={small} onclick={loadMore} disabled={loadingDocs} data-testid="fs-load-more">Load more</button>{/if}
							{/if}
						{:else}
							<div class="space-y-3" data-testid="fs-query">
								<FirestoreFilterGroup bind:tree={spec.where} />
								<div class="space-y-2">
									<div class="flex items-center justify-between"><h3 class="text-xs font-semibold text-gray-700 dark:text-gray-300">Order by</h3>
										<button class={small} onclick={() => spec.orderBy.push({ id: spec.orderBy.length + Date.now(), field: '', dir: 'ASCENDING' })} data-testid="fs-add-order">Add</button></div>
									{#each spec.orderBy as o, i (o.id)}
										<div class="flex gap-2">
											<input class="{input} w-40 font-mono" bind:value={o.field} placeholder="field" aria-label="Order field" data-testid="fs-order-field" />
											<select class="{input} w-36" bind:value={o.dir} aria-label="Direction"><option value="ASCENDING">ascending</option><option value="DESCENDING">descending</option></select>
											<button type="button" class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Remove order" onclick={() => spec.orderBy.splice(i, 1)}><Trash2 class="h-4 w-4" /></button>
										</div>
									{/each}
								</div>
								<div class="grid grid-cols-2 gap-3 md:grid-cols-4">
									<label class="space-y-1 text-xs"><span class={dt}>Limit</span><input class={input} bind:value={spec.limit} placeholder="none" data-testid="fs-limit" /></label>
									<label class="space-y-1 text-xs"><span class={dt}>Offset</span><input class={input} bind:value={spec.offset} placeholder="0" /></label>
									<label class="col-span-2 space-y-1 text-xs"><span class={dt}>Select fields (comma separated)</span><input class="{input} font-mono" bind:value={spec.select} placeholder="all fields" /></label>
								</div>
								<div class="grid gap-3 md:grid-cols-2">
									<div class="space-y-1 text-xs"><span class={dt}>Start cursor</span>
										<div class="flex gap-2"><select class="{input} w-32" bind:value={spec.startKind} aria-label="Start cursor kind" data-testid="fs-start-kind"><option value="none">none</option><option value="startAt">startAt</option><option value="startAfter">startAfter</option></select>
											<input class="{input} font-mono" bind:value={spec.startValues} placeholder={'["Alice", 30]'} disabled={spec.startKind === 'none'} aria-label="Start cursor values" data-testid="fs-start-values" /></div></div>
									<div class="space-y-1 text-xs"><span class={dt}>End cursor</span>
										<div class="flex gap-2"><select class="{input} w-32" bind:value={spec.endKind} aria-label="End cursor kind" data-testid="fs-end-kind"><option value="none">none</option><option value="endAt">endAt</option><option value="endBefore">endBefore</option></select>
											<input class="{input} font-mono" bind:value={spec.endValues} placeholder={'["Zed", 99]'} disabled={spec.endKind === 'none'} aria-label="End cursor values" data-testid="fs-end-values" /></div></div>
								</div>
								<p class="text-[11px] text-gray-500 dark:text-gray-400">Cursor values map to the order-by fields in order.</p>
								<label class="flex items-center gap-2 text-xs"><input type="checkbox" bind:checked={spec.allDescendants} /> Include subcollections with this name (collection group)</label>
								<div class="flex gap-2">
									<button class="flex items-center gap-1 {btn}" onclick={() => execQuery()} disabled={busy === 'query'} data-testid="fs-run-query"><Play class="h-3.5 w-3.5" /> Run query</button>
									<button class={small} onclick={() => { spec = newQuerySpec(); results = null; }}>Reset</button>
								</div>
								{#if results}
									<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="fs-query-count">{results.length} result{results.length === 1 ? '' : 's'}</p>
									{#if results.length > 0}{@render grid(results, true)}{/if}
								{/if}
							</div>
						{/if}
					{/if}
				</section>

				<section aria-label="Document" class="min-w-0">
					{#if remoteNote && !doc}
						<p class="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/30 dark:text-amber-300" data-testid="fs-doc-gone">{remoteNote}</p>
					{:else if docPath && !doc}
						<div class="space-y-3" data-testid="fs-missing">
							<p class="text-sm text-gray-500 dark:text-gray-400">No document at <span class="font-mono">{docPath.join('/')}</span>.</p>
							{#if subcollections.length > 0}
								<div class="flex flex-wrap gap-2">{#each subcollections as s (s)}<button class={small} onclick={() => navigate([...docPath, s])}><Folder class="inline h-3 w-3" /> {s}</button>{/each}</div>
							{/if}
						</div>
					{:else if !doc}
						<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="fs-doc-hint">Select a document to view and edit it</p>
					{:else}
						<div class="space-y-4 rounded-lg border border-gray-200 dark:border-gray-700 p-5" data-testid="fs-detail">
							<div class="flex items-start justify-between gap-4">
								<div class="min-w-0"><h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all" data-testid="fs-doc-title">{doc.id}</h3><p class="font-mono text-xs text-gray-500 dark:text-gray-400 break-all">{doc.segs.join('/')}</p></div>
								<button class="flex items-center gap-1 rounded-md border border-red-200 px-2 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-900 dark:hover:bg-red-950 disabled:opacity-50" onclick={removeDoc} disabled={busy === 'delete'} data-testid="fs-delete"><Trash2 class="h-3.5 w-3.5" /> Delete</button>
							</div>
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
								<dt class={dt}>Created</dt><dd class="font-mono text-gray-700 dark:text-gray-300">{doc.createTime || '—'}</dd>
								<dt class={dt}>Updated</dt><dd class="font-mono text-gray-700 dark:text-gray-300" data-testid="fs-doc-updated">{doc.updateTime || '—'}</dd>
							</dl>
							{#if subcollections.length > 0}
								<div class="space-y-1" data-testid="fs-subcollections">
									<h4 class="text-xs font-semibold text-gray-700 dark:text-gray-300">Subcollections</h4>
									<div class="flex flex-wrap gap-2">{#each subcollections as s (s)}<button class={small} onclick={() => navigate([...doc!.segs, s])} data-testid="fs-subcollection"><Folder class="inline h-3 w-3" /> {s}</button>{/each}</div>
								</div>
							{/if}
							{#if remoteNote}<p class="rounded-md bg-amber-50 p-2 text-xs text-amber-800 dark:bg-amber-950/30 dark:text-amber-300" role="status" data-testid="fs-remote-note">{remoteNote}</p>{/if}
							<FirestoreDocEditor bind:nodes={editNodes} bind:mode={editMode} bind:rawText={editRaw} {ctx} />
							<div class="flex flex-wrap items-center gap-4 border-t border-gray-100 dark:border-gray-800 pt-3 text-sm">
								<label class="flex items-center gap-1"><input type="radio" bind:group={saveMode} value="merge" data-testid="fs-save-merge" /> Merge</label>
								<label class="flex items-center gap-1"><input type="radio" bind:group={saveMode} value="replace" data-testid="fs-save-replace" /> Replace</label>
								<span class="text-[11px] text-gray-500 dark:text-gray-400">{saveMode === 'merge' ? 'only the listed fields are written' : 'the document is overwritten'}</span>
								<span class="ml-auto flex gap-2">
									<button class="flex items-center gap-1 {small}" onclick={() => doc && adopt(doc)} disabled={!dirty} data-testid="fs-revert"><Undo2 class="h-3.5 w-3.5" /> Revert</button>
									<button class="flex items-center gap-1 {btn}" onclick={save} disabled={!dirty || busy === 'save'} data-testid="fs-save"><Save class="h-3.5 w-3.5" /> Save</button>
								</span>
							</div>
						</div>
					{/if}
				</section>
			</div>
		</div>
	{/if}
</div>

<Modal bind:this={modal} title={createKind === 'collection' ? 'New collection' : 'Add document'} titleId="fs-create-title">
	{#snippet children()}
		<form id="fs-create-form" class="space-y-3" onsubmit={submitCreate} novalidate>
			<p class="font-mono text-xs text-gray-500 dark:text-gray-400">{createKind === 'collection' ? parentDoc.join('/') || '(root)' : collPath?.join('/')}</p>
			{#if createKind === 'collection'}
				<label class="block space-y-1 text-sm"><span class="font-medium text-gray-700 dark:text-gray-300">Collection ID</span>
					<input class={input} bind:value={fCollection} placeholder="users" autocomplete="off" aria-label="Collection ID" data-testid="fs-new-collection-id" /></label>
			{/if}
			<label class="block space-y-1 text-sm"><span class="font-medium text-gray-700 dark:text-gray-300">Document ID <span class="font-normal text-gray-500">(blank for auto-ID)</span></span>
				<input class={input} bind:value={fDocId} placeholder="auto" autocomplete="off" aria-label="Document ID" data-testid="fs-new-doc-id" /></label>
			<FirestoreDocEditor bind:nodes={fNodes} bind:mode={fMode} bind:rawText={fRaw} {ctx} />
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="fs-create-form" class="rounded-md bg-orange-600 px-4 py-2 text-sm text-white hover:bg-orange-700 disabled:opacity-50" disabled={busy === 'create'} data-testid="fs-create-submit">{busy === 'create' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>
