<script lang="ts">
	import { onMount } from 'svelte';
	import { Table2, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		NOSQL_PAGE_SIZE,
		canDrop,
		cellText,
		createTable,
		defaultQuery,
		deleteRow,
		dropTable,
		getRow,
		getTable,
		listCompartments,
		listIndexes,
		listTables,
		listUsage,
		putRow,
		readStoredNosqlCompartment,
		rowsUsable,
		runQuery,
		writeStoredNosqlCompartment,
		type NosqlIndex,
		type NosqlQueryPage,
		type NosqlTable,
		type NosqlUsage
	} from '#lib/ocinosql.ts';

	const TABS = [
		{ id: 'schema', label: 'Schema' },
		{ id: 'indexes', label: 'Indexes' },
		{ id: 'query', label: 'Query' },
		{ id: 'rows', label: 'Rows' },
		{ id: 'usage', label: 'Usage' }
	];
	const DDL_SAMPLE = 'CREATE TABLE IF NOT EXISTS users (id INTEGER, name STRING, PRIMARY KEY (id))';

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredNosqlCompartment());
	let tables = $state<NosqlTable[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let detail = $state<NosqlTable | null>(null);
	let detailError = $state('');
	let tab = $state('schema');
	let indexes = $state<NosqlIndex[]>([]);
	let usage = $state<NosqlUsage[]>([]);

	let showCreate = $state(false);
	let ddl = $state(DDL_SAMPLE);
	let mode = $state<'PROVISIONED' | 'ON_DEMAND'>('PROVISIONED');
	let readUnits = $state(10);
	let writeUnits = $state(10);
	let storageGb = $state(1);

	let statement = $state('');
	let queryPages = $state<string[]>(['']);
	let queryIdx = $state(0);
	let queryResult = $state<NosqlQueryPage | null>(null);
	let queryError = $state('');
	let querying = $state(false);

	let keyValues = $state<Record<string, string>>({});
	let rowJson = $state('{\n  \n}');
	let rowMessage = $state('');
	let rowError = $state('');

	async function loadTables() {
		if (!compartment) {
			tables = [];
			return;
		}
		try {
			tables = await listTables(compartment);
			error = '';
			if (!tables.some((t) => t.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadDetail();
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadTables();
		loading = false;
	}

	async function loadDetail() {
		detailError = '';
		if (!selected) {
			detail = null;
			indexes = [];
			usage = [];
			return;
		}
		try {
			[detail, indexes, usage] = await Promise.all([
				getTable(compartment, selected),
				listIndexes(compartment, selected),
				listUsage(compartment, selected)
			]);
			keyValues = Object.fromEntries(detail.primaryKey.map((k) => [k, keyValues[k] ?? '']));
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredNosqlCompartment(compartment);
		selected = '';
		queryResult = null;
		loading = true;
		void loadTables();
	}

	function select(t: NosqlTable) {
		selected = selected === t.name ? '' : t.name;
		detail = null;
		if (selected) statement = defaultQuery(selected);
		queryResult = null;
		rowMessage = rowError = '';
		void loadDetail();
	}

	async function create() {
		const ok = await attempt(
			() => createTable(compartment, ddl, { mode, readUnits, writeUnits, storageGb }),
			'Table creation started',
			'Failed to create table'
		);
		if (ok) {
			showCreate = false;
			await loadTables();
		}
	}

	async function drop(t: NosqlTable) {
		const ok = await confirmedAttempt(
			{ title: 'Drop table', message: `Drop table "${t.name}" and all of its rows?`, confirmLabel: 'Drop' },
			() => dropTable(compartment, t.name),
			`Dropping ${t.name}`,
			`Failed to drop ${t.name}`
		);
		if (ok) await loadTables();
	}

	async function execute(idx: number) {
		querying = true;
		queryError = '';
		try {
			const res = await runQuery(compartment, statement, queryPages[idx] ?? '', NOSQL_PAGE_SIZE);
			queryResult = res;
			queryIdx = idx;
			queryPages[idx + 1] = res.next;
		} catch (e) {
			queryError = describeGcpError(e);
			queryResult = null;
		} finally {
			querying = false;
		}
	}

	function startQuery() {
		queryPages = [''];
		void execute(0);
	}

	async function fetchRow() {
		rowMessage = rowError = '';
		try {
			const r = await getRow(compartment, selected, keyValues);
			if (!r) {
				rowMessage = 'No row with that key';
				return;
			}
			rowJson = JSON.stringify(r.value, null, 2);
			rowMessage = `Row loaded (${r.readUnits} read unit${r.readUnits === 1 ? '' : 's'})`;
		} catch (e) {
			rowError = describeGcpError(e);
		}
	}

	async function saveRow() {
		rowMessage = rowError = '';
		const ok = await attempt(() => putRow(compartment, selected, rowJson), 'Row saved', 'Failed to save row');
		if (ok) {
			rowMessage = 'Row saved';
			await loadDetail();
		}
	}

	async function removeRow() {
		rowMessage = rowError = '';
		const ok = await confirmedAttempt(
			{ title: 'Delete row', message: 'Delete the row with this key?', confirmLabel: 'Delete' },
			async () => {
				if (!(await deleteRow(compartment, selected, keyValues))) throw new Error('No row with that key');
			},
			'Row deleted',
			'Failed to delete row'
		);
		if (ok) rowMessage = 'Row deleted';
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';

	$effect(() => {
		if (!tables.some((t) => t.state !== 'ACTIVE' && t.state !== 'FAILED')) return;
		const timer = setTimeout(() => void loadTables(), 1500);
		return () => clearTimeout(timer);
	});

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="nosql-dashboard">
	<PageHeader icon={Table2} title="NoSQL Database" description="Oracle NoSQL tables, indexes, rows and SQL queries" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="nosql-error">
			<p class="font-medium">Could not load tables</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="nosql-empty">No tables yet. Create one with the OCI SDK or CLI.</p>
	{:else}
		<div class="flex flex-wrap items-center justify-between gap-2">
			<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				Compartment
				<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="nosql-compartment">
					{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
				</select>
			</label>
			<button class={GCP_MINI} onclick={() => (showCreate = !showCreate)} aria-expanded={showCreate} data-testid="nosql-create-toggle">Create table</button>
		</div>

		{#if showCreate}
			<form class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" onsubmit={(e) => { e.preventDefault(); void create(); }} data-testid="nosql-create">
				<label class="block text-sm"><span class="mb-1 block font-medium">DDL</span>
					<textarea class="{GCP_INPUT} font-mono text-xs" rows="3" spellcheck="false" bind:value={ddl} data-testid="nosql-ddl"></textarea>
				</label>
				<div class="flex flex-wrap items-end gap-3 text-sm">
					<label>Capacity
						<select class="{GCP_INPUT} !w-40 !py-1" bind:value={mode} data-testid="nosql-mode">
							<option value="PROVISIONED">Provisioned</option>
							<option value="ON_DEMAND">On demand</option>
						</select>
					</label>
					{#if mode === 'PROVISIONED'}
						<label>Read units <input class="{GCP_INPUT} !w-24 !py-1" type="number" min="1" bind:value={readUnits} data-testid="nosql-read-units" /></label>
						<label>Write units <input class="{GCP_INPUT} !w-24 !py-1" type="number" min="1" bind:value={writeUnits} data-testid="nosql-write-units" /></label>
					{/if}
					<label>Storage (GB) <input class="{GCP_INPUT} !w-24 !py-1" type="number" min="1" bind:value={storageGb} data-testid="nosql-storage-gb" /></label>
					<button type="submit" class={GCP_PRIMARY} data-testid="nosql-create-submit">Create</button>
				</div>
			</form>
		{/if}

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="nosql-list-error">{error}</p>{/if}

		<div class={box}>
			<table class="w-full text-sm" data-testid="nosql-tables">
				<thead class={thead}>
					<tr><th class={cell}>Table</th><th class={cell}>State</th><th class={cell}>Capacity</th><th class={cell}>Limits</th><th class={cell}>Storage</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each tables as t (t.id)}
						<tr class={selected === t.name ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="nosql-table">
							<td class="{cell} font-mono text-xs" title={t.id}><button class="underline decoration-dotted" onclick={() => select(t)} aria-expanded={selected === t.name} data-testid="nosql-select">{t.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(t.state)}" data-testid="nosql-state">{t.state || '—'}</span></td>
							<td class="{cell} text-xs" data-testid="nosql-capacity">{t.mode || '—'}</td>
							<td class="{cell} font-mono text-xs" data-testid="nosql-limits">{t.mode === 'ON_DEMAND' ? '—' : `${t.readUnits} RU / ${t.writeUnits} WU`}</td>
							<td class="{cell} text-xs">{t.storageGb} GB</td>
							<td class="{cell} text-xs">{t.created}</td>
							<td class="{cell} text-right whitespace-nowrap"><button class={GCP_MINI} onclick={() => drop(t)} disabled={!canDrop(t)} data-testid="nosql-drop">Drop</button></td>
						</tr>
					{:else}
						<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="nosql-none">No tables in this compartment</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="nosql-detail-error">{detailError}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

		{#if tab === 'query'}
			<div class="space-y-3" data-testid="nosql-query-console">
				<label class="block text-sm"><span class="mb-1 block font-medium">SELECT statement</span>
					<textarea class="{GCP_INPUT} font-mono text-xs" rows="3" spellcheck="false" bind:value={statement} data-testid="nosql-query"></textarea>
				</label>
				<button class={GCP_PRIMARY} onclick={startQuery} disabled={querying || !statement.trim()} data-testid="nosql-run">Query</button>
				{#if queryError}<p class="text-sm text-red-600" role="alert" data-testid="nosql-query-error">{queryError}</p>{/if}
				{#if queryResult}
					<div class="flex items-center gap-3 text-xs text-gray-500">
						<span data-testid="nosql-result-count">{queryResult.rows.length} row{queryResult.rows.length === 1 ? '' : 's'} on page {queryIdx + 1}, {queryResult.readUnits} read unit{queryResult.readUnits === 1 ? '' : 's'}</span>
						<button class={GCP_MINI} onclick={() => execute(queryIdx - 1)} disabled={queryIdx === 0 || querying} data-testid="nosql-prev">Previous</button>
						<button class={GCP_MINI} onclick={() => execute(queryIdx + 1)} disabled={!queryResult.next || querying} data-testid="nosql-next">Next</button>
					</div>
					{#if queryResult.rows.length === 0}
						<p class="text-sm text-gray-500" data-testid="nosql-result-empty">No rows</p>
					{:else}
						<div class={box}>
							<table class="w-full text-sm" data-testid="nosql-results">
								<thead class={thead}><tr>{#each queryResult.columns as c (c)}<th class={cell}>{c}</th>{/each}</tr></thead>
								<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
									{#each queryResult.rows as r, i (i)}
										<tr data-testid="nosql-result-row">{#each queryResult.columns as c (c)}<td class="{cell} font-mono text-xs">{cellText(r[c])}</td>{/each}</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{/if}
				{/if}
			</div>
		{:else if !detail}
			<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="nosql-pick">Select a table to see its {tab}.</p>
		{:else if tab === 'schema'}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="nosql-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.name}</h2>
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
					<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{detail.id}</dd>
					<dt class="text-gray-500">Capacity mode</dt><dd data-testid="nosql-detail-mode">{detail.mode}</dd>
					<dt class="text-gray-500">Limits</dt><dd data-testid="nosql-detail-limits">{detail.mode === 'ON_DEMAND' ? 'On demand' : `${detail.readUnits} read units, ${detail.writeUnits} write units`}, {detail.storageGb} GB</dd>
					<dt class="text-gray-500">Time to live</dt><dd data-testid="nosql-detail-ttl">{detail.ttlDays ? `${detail.ttlDays} days` : 'None'}</dd>
					<dt class="text-gray-500">Primary key</dt><dd class="font-mono" data-testid="nosql-detail-pk">{detail.primaryKey.join(', ') || '—'}</dd>
					<dt class="text-gray-500">Shard key</dt><dd class="font-mono">{detail.shardKey.join(', ') || '—'}</dd>
				</dl>
				<div>
					<h3 class="mb-1 text-sm font-medium">DDL</h3>
					<pre class="overflow-x-auto rounded-md bg-gray-50 p-3 text-xs dark:bg-slate-800" data-testid="nosql-detail-ddl">{detail.ddl}</pre>
				</div>
				<div class={box}>
					<table class="w-full text-sm" data-testid="nosql-columns">
						<thead class={thead}><tr><th class={cell}>Column</th><th class={cell}>Type</th><th class={cell}>Nullable</th><th class={cell}>Key</th><th class={cell}>Default</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each detail.columns as c (c.name)}
								<tr data-testid="nosql-column">
									<td class="{cell} font-mono text-xs">{c.name}</td>
									<td class="{cell} font-mono text-xs">{c.type}{c.generated ? ' (generated)' : ''}</td>
									<td class="{cell} text-xs">{c.nullable ? 'Yes' : 'No'}</td>
									<td class="{cell} text-xs">{detail.primaryKey.includes(c.name) ? 'Primary key' : ''}</td>
									<td class="{cell} font-mono text-xs">{c.defaultValue || '—'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			</div>
		{:else if tab === 'indexes'}
			{#if indexes.length === 0}
				<p class="text-sm text-gray-500" data-testid="nosql-indexes-empty">No indexes</p>
			{:else}
				<div class={box}>
					<table class="w-full text-sm" data-testid="nosql-indexes">
						<thead class={thead}><tr><th class={cell}>Index</th><th class={cell}>State</th><th class={cell}>Keys</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each indexes as i (i.name)}
								<tr data-testid="nosql-index"><td class="{cell} font-mono text-xs">{i.name}</td><td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(i.state)}">{i.state}</span></td><td class="{cell} font-mono text-xs">{i.keys.join(', ')}</td></tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if tab === 'rows'}
			<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="nosql-row-editor">
				{#if !rowsUsable(detail)}<p class="text-xs text-amber-600">Table is {detail.state}; rows are available once it is ACTIVE.</p>{/if}
				<div class="flex flex-wrap items-end gap-3 text-sm">
					{#each detail.primaryKey as k (k)}
						<label>{k} <input class="{GCP_INPUT} !w-40 !py-1 font-mono text-xs" type="text" bind:value={keyValues[k]} data-testid="nosql-key-{k}" /></label>
					{/each}
					<button class={GCP_MINI} onclick={fetchRow} disabled={!rowsUsable(detail)} data-testid="nosql-row-get">Get by key</button>
					<button class={GCP_MINI} onclick={removeRow} disabled={!rowsUsable(detail)} data-testid="nosql-row-delete">Delete by key</button>
				</div>
				<label class="block text-sm"><span class="mb-1 block font-medium">Row (JSON)</span>
					<textarea class="{GCP_INPUT} font-mono text-xs" rows="8" spellcheck="false" bind:value={rowJson} data-testid="nosql-row-json"></textarea>
				</label>
				<button class={GCP_PRIMARY} onclick={saveRow} disabled={!rowsUsable(detail)} data-testid="nosql-row-put">Put row</button>
				{#if rowMessage}<p class="text-sm text-green-700 dark:text-green-400" data-testid="nosql-row-message">{rowMessage}</p>{/if}
				{#if rowError}<p class="text-sm text-red-600" role="alert" data-testid="nosql-row-error">{rowError}</p>{/if}
			</div>
		{:else}
			{#if usage.length === 0}
				<p class="text-sm text-gray-500" data-testid="nosql-usage-empty">No usage recorded in the last hour</p>
			{:else}
				<div class={box}>
					<table class="w-full text-sm" data-testid="nosql-usage">
						<thead class={thead}><tr><th class={cell}>Period start</th><th class={cell}>Seconds</th><th class={cell}>Read units</th><th class={cell}>Write units</th><th class={cell}>Storage (GB)</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each usage as u (u.started)}
								<tr data-testid="nosql-usage-row"><td class="{cell} text-xs">{u.started}</td><td class="{cell} text-xs">{u.seconds}</td><td class="{cell} text-xs">{u.readUnits}</td><td class="{cell} text-xs">{u.writeUnits}</td><td class="{cell} text-xs">{u.storageGb}</td></tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
