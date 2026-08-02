<script lang="ts">
	// Export/Import (services/mgn/PARITY.md family I, 8 ops): StartExport/
	// ListExports/ListExportErrors, StartImport/ListImports/ListImportErrors,
	// plus StartImportFileEnrichment/ListImportFileEnrichments (wire-routed
	// under /network-migration/ despite being conceptually part of this
	// family -- PARITY.md's wire-shape trap #4).
	//
	// StartImport is the ONLY public-API path that creates SourceServer/
	// Application/Wave records (see the Source Servers tab's own banner). It
	// reads a CSV object from a caller-supplied S3 bucket/key using a schema
	// this backend documents as an ASSUMPTION, not something AWS publishes
	// (services/mgn/s3import.go) -- shown verbatim below so users know what
	// to upload. As of this pass, cli.go has not yet wired the MGN backend to
	// the S3 backend (PARITY.md's "Pending cli.go wiring" section), so every
	// StartImport against a real running server FAILS honestly with no S3
	// backend configured -- this panel surfaces that failure inline rather
	// than hiding it.
	import {
		ListExportsCommand,
		StartExportCommand,
		ListExportErrorsCommand,
		ListImportsCommand,
		StartImportCommand,
		ListImportErrorsCommand,
		ListImportFileEnrichmentsCommand,
		StartImportFileEnrichmentCommand,
		type ExportTask,
		type ExportTaskError,
		type ImportTask,
		type ImportTaskError,
		type ImportFileEnrichment,
		type MgnClient
	} from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { formatDate } from '$lib/format';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { describeError, PLACEHOLDER_ACCOUNT_ID } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client, searchQuery }: Props = $props();

	// ------------------------------- Exports --------------------------------

	let exports = $state<ExportTask[]>([]);
	let exportsNextToken = $state<string | undefined>();
	let exportsLoading = $state(false);
	let exportsLoadingMore = $state(false);
	let exportsError = $state<string | null>(null);

	// ------------------------------- Imports --------------------------------

	let imports = $state<ImportTask[]>([]);
	let importsNextToken = $state<string | undefined>();
	let importsLoading = $state(false);
	let importsLoadingMore = $state(false);
	let importsError = $state<string | null>(null);

	// ----------------------------- Enrichments -------------------------------

	let enrichments = $state<ImportFileEnrichment[]>([]);
	let enrichmentsNextToken = $state<string | undefined>();
	let enrichmentsLoading = $state(false);
	let enrichmentsError = $state<string | null>(null);

	async function fetchExports(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListExportsCommand({ maxResults: 50, nextToken: reset ? undefined : exportsNextToken })
		);
		exports = reset ? (resp.items ?? []) : [...exports, ...(resp.items ?? [])];
		exportsNextToken = resp.nextToken;
	}

	async function fetchImports(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListImportsCommand({ maxResults: 50, nextToken: reset ? undefined : importsNextToken })
		);
		imports = reset ? (resp.items ?? []) : [...imports, ...(resp.items ?? [])];
		importsNextToken = resp.nextToken;
	}

	async function fetchEnrichments(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListImportFileEnrichmentsCommand({ maxResults: 50, nextToken: reset ? undefined : enrichmentsNextToken })
		);
		enrichments = reset ? (resp.items ?? []) : [...enrichments, ...(resp.items ?? [])];
		enrichmentsNextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		exportsLoading = true;
		importsLoading = true;
		enrichmentsLoading = true;
		exportsError = null;
		importsError = null;
		enrichmentsError = null;
		try {
			await Promise.all([
				fetchExports(true).catch((e) => {
					exportsError = describeError(e);
				}),
				fetchImports(true).catch((e) => {
					importsError = describeError(e);
				}),
				fetchEnrichments(true).catch((e) => {
					enrichmentsError = describeError(e);
				})
			]);
		} finally {
			exportsLoading = false;
			importsLoading = false;
			enrichmentsLoading = false;
		}
	}

	async function loadMoreExports(): Promise<void> {
		exportsLoadingMore = true;
		try {
			await fetchExports(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			exportsLoadingMore = false;
		}
	}

	async function loadMoreImports(): Promise<void> {
		importsLoadingMore = true;
		try {
			await fetchImports(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			importsLoadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filteredExports = $derived(
		exports.filter((e) => (e.exportID ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredImports = $derived(
		imports.filter((i) => (i.importID ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function statusClass(status: string | undefined): string {
		if (status === 'SUCCEEDED') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status === 'FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-400';
	}

	// -------------------------------- Start Export ---------------------------

	let exportModal = $state<Modal | null>(null);
	let exporting = $state(false);
	let exportError = $state<string | null>(null);
	let exportBucket = $state('');
	let exportKey = $state('');

	function openExportModal(): void {
		exportError = null;
		exportBucket = '';
		exportKey = '';
		exportModal?.open();
	}

	async function submitExport(): Promise<void> {
		if (!exportBucket.trim() || !exportKey.trim()) {
			exportError = 'S3 bucket and key are required.';
			return;
		}
		exporting = true;
		exportError = null;
		try {
			await client().send(new StartExportCommand({ s3Bucket: exportBucket.trim(), s3Key: exportKey.trim() }));
			toast.success('Export started');
			exportModal?.close();
			await refresh();
		} catch (e) {
			exportError = describeError(e);
			toast.error(exportError);
		} finally {
			exporting = false;
		}
	}

	let exportDetailModal = $state<Modal | null>(null);
	let viewedExport = $state<ExportTask | null>(null);
	let exportErrors = $state<ExportTaskError[]>([]);
	let exportErrorsLoading = $state(false);
	let exportErrorsErr = $state<string | null>(null);

	async function openExportDetail(e: ExportTask): Promise<void> {
		viewedExport = e;
		exportErrors = [];
		exportErrorsErr = null;
		exportDetailModal?.open();
		if (!e.exportID) return;
		exportErrorsLoading = true;
		try {
			const resp = await client().send(new ListExportErrorsCommand({ exportID: e.exportID }));
			exportErrors = resp.items ?? [];
		} catch (err) {
			exportErrorsErr = describeError(err);
		} finally {
			exportErrorsLoading = false;
		}
	}

	// -------------------------------- Start Import ---------------------------

	let importModal = $state<Modal | null>(null);
	let importing = $state(false);
	let importError = $state<string | null>(null);
	let importBucket = $state('');
	let importKey = $state('');

	function openImportModal(): void {
		importError = null;
		importBucket = '';
		importKey = '';
		importModal?.open();
	}

	async function submitImport(): Promise<void> {
		if (!importBucket.trim() || !importKey.trim()) {
			importError = 'S3 bucket and key are required.';
			return;
		}
		importing = true;
		importError = null;
		try {
			await client().send(
				new StartImportCommand({ s3BucketSource: { s3Bucket: importBucket.trim(), s3Key: importKey.trim() } })
			);
			toast.success('Import started');
			importModal?.close();
			await refresh();
		} catch (e) {
			importError = describeError(e);
			toast.error(importError);
		} finally {
			importing = false;
		}
	}

	let importDetailModal = $state<Modal | null>(null);
	let viewedImport = $state<ImportTask | null>(null);
	let importErrors = $state<ImportTaskError[]>([]);
	let importErrorsLoading = $state(false);
	let importErrorsErr = $state<string | null>(null);

	async function openImportDetail(i: ImportTask): Promise<void> {
		viewedImport = i;
		importErrors = [];
		importErrorsErr = null;
		importDetailModal?.open();
		if (!i.importID) return;
		importErrorsLoading = true;
		try {
			const resp = await client().send(new ListImportErrorsCommand({ importID: i.importID }));
			importErrors = resp.items ?? [];
		} catch (err) {
			importErrorsErr = describeError(err);
		} finally {
			importErrorsLoading = false;
		}
	}

	// ---------------------------- Import file enrichment ----------------------

	let enrichModal = $state<Modal | null>(null);
	let enriching = $state(false);
	let enrichError = $state<string | null>(null);
	let enrichSourceBucket = $state('');
	let enrichSourceBucketOwner = $state(PLACEHOLDER_ACCOUNT_ID);
	let enrichSourceKey = $state('');
	let enrichTargetBucket = $state('');
	let enrichTargetBucketOwner = $state(PLACEHOLDER_ACCOUNT_ID);
	let enrichTargetKey = $state('');

	function openEnrichModal(): void {
		enrichError = null;
		enrichSourceBucket = '';
		enrichSourceBucketOwner = PLACEHOLDER_ACCOUNT_ID;
		enrichSourceKey = '';
		enrichTargetBucket = '';
		enrichTargetBucketOwner = PLACEHOLDER_ACCOUNT_ID;
		enrichTargetKey = '';
		enrichModal?.open();
	}

	async function submitEnrich(): Promise<void> {
		if (
			!enrichSourceBucket.trim() ||
			!enrichSourceBucketOwner.trim() ||
			!enrichSourceKey.trim() ||
			!enrichTargetBucket.trim() ||
			!enrichTargetBucketOwner.trim() ||
			!enrichTargetKey.trim()
		) {
			enrichError = 'Source and target S3 bucket/owner/key are all required.';
			return;
		}
		enriching = true;
		enrichError = null;
		try {
			await client().send(
				new StartImportFileEnrichmentCommand({
					s3BucketSource: {
						s3Bucket: enrichSourceBucket.trim(),
						s3BucketOwner: enrichSourceBucketOwner.trim(),
						s3Key: enrichSourceKey.trim()
					},
					s3BucketTarget: {
						s3Bucket: enrichTargetBucket.trim(),
						s3BucketOwner: enrichTargetBucketOwner.trim(),
						s3Key: enrichTargetKey.trim()
					}
				})
			);
			toast.success('Import file enrichment started');
			enrichModal?.close();
			await refresh();
		} catch (e) {
			enrichError = describeError(e);
			toast.error(enrichError);
		} finally {
			enriching = false;
		}
	}

	const exportColumns = defineColumns<ExportTask>([
		{ key: 'exportID', label: 'Export ID' },
		{
			key: 'status',
			label: 'Status'
		},
		{ key: 'progressPercentage', label: 'Progress %' },
		{ key: 'creationDateTime', label: 'Created' }
	]);
	const importColumns = defineColumns<ImportTask>([
		{ key: 'importID', label: 'Import ID' },
		{ key: 'status', label: 'Status' },
		{ key: 'progressPercentage', label: 'Progress %' },
		{ key: 'creationDateTime', label: 'Created' }
	]);
	const enrichmentColumns = defineColumns<ImportFileEnrichment>([
		{ key: 'jobID', label: 'Job ID' },
		{ key: 'status', label: 'Status' }
	]);
	const exportErrorColumns = defineColumns<ExportTaskError>([
		{ key: 'errorDateTime', label: 'Time' },
		{
			key: 'errorData',
			label: 'Error'
		}
	]);
	const importErrorColumns = defineColumns<ImportTaskError>([
		{ key: 'errorType', label: 'Type' },
		{
			key: 'errorData',
			label: 'Error'
		}
	]);
</script>

<div class="rounded-lg border border-blue-200 dark:border-blue-900 bg-blue-50 dark:bg-blue-950/30 px-4 py-3 text-sm text-blue-800 dark:text-blue-300 space-y-1">
	<p>
		<strong>StartImport</strong> is the only AWS operation that creates
		SourceServer/Application/Wave records. It reads a CSV object from S3
		using a schema this emulator documents as an assumption (AWS does not
		publish one): header row required; only <code>hostname</code> is
		required; optional columns <code>fqdn</code>, <code>userProvidedID</code>,
		<code>operatingSystem</code>, <code>recommendedInstanceType</code>,
		<code>cpuCores</code>, <code>cpuModelName</code>, <code>ramBytes</code>,
		<code>diskDeviceName</code>, <code>diskBytes</code>,
		<code>networkInterfaceMac</code>, <code>networkInterfaceIPs</code>
		(semicolon-separated).
	</p>
	<p class="text-amber-700 dark:text-amber-400">
		The S3 accessor is not yet wired into the running server (cli.go), so
		today a real StartImport call fails honestly with no S3 backend
		configured -- check the errors below rather than assuming the import
		silently worked.
	</p>
</div>

<div class="space-y-6">
	<div>
		<div class="flex items-center justify-between mb-2">
			<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Exports</h3>
			<button onclick={openExportModal} class="px-3 py-1.5 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Start export</button>
		</div>
		{#if exportsError}<p class="text-sm text-red-600 dark:text-red-400 mb-2">{exportsError}</p>{/if}
		{#snippet exportStatusCell(e: ExportTask)}
			<span class="text-xs px-2 py-1 rounded-full {statusClass(e.status)}">{e.status ?? '—'}</span>
		{/snippet}
		{#snippet exportRowActions(e: ExportTask)}
			<button onclick={() => openExportDetail(e)} class="text-blue-600 hover:underline text-sm">View</button>
		{/snippet}
		<DataTable
			rows={filteredExports}
			rowKey={(e) => e.exportID ?? ''}
			columns={[exportColumns[0], { ...exportColumns[1], render: exportStatusCell }, exportColumns[2], exportColumns[3], { key: 'actions', label: '', render: exportRowActions }]}
			loading={exportsLoading}
			emptyMessage="No exports found"
		/>
		<LoadMore hasMore={!!exportsNextToken} loading={exportsLoadingMore} onLoadMore={loadMoreExports} />
	</div>

	<div>
		<div class="flex items-center justify-between mb-2">
			<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Imports</h3>
			<button onclick={openImportModal} class="px-3 py-1.5 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Start import</button>
		</div>
		{#if importsError}<p class="text-sm text-red-600 dark:text-red-400 mb-2">{importsError}</p>{/if}
		{#snippet importStatusCell(i: ImportTask)}
			<span class="text-xs px-2 py-1 rounded-full {statusClass(i.status)}">{i.status ?? '—'}</span>
		{/snippet}
		{#snippet importRowActions(i: ImportTask)}
			<button onclick={() => openImportDetail(i)} class="text-blue-600 hover:underline text-sm">View</button>
		{/snippet}
		<DataTable
			rows={filteredImports}
			rowKey={(i) => i.importID ?? ''}
			columns={[importColumns[0], { ...importColumns[1], render: importStatusCell }, importColumns[2], importColumns[3], { key: 'actions', label: '', render: importRowActions }]}
			loading={importsLoading}
			emptyMessage="No imports found"
		/>
		<LoadMore hasMore={!!importsNextToken} loading={importsLoadingMore} onLoadMore={loadMoreImports} />
	</div>

	<div>
		<div class="flex items-center justify-between mb-2">
			<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Import file enrichment</h3>
			<button onclick={openEnrichModal} class="px-3 py-1.5 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">Start enrichment</button>
		</div>
		{#if enrichmentsError}<p class="text-sm text-red-600 dark:text-red-400 mb-2">{enrichmentsError}</p>{/if}
		<DataTable rows={enrichments} rowKey={(e) => e.jobID ?? ''} columns={enrichmentColumns} loading={enrichmentsLoading} emptyMessage="No import file enrichment jobs found" />
	</div>
</div>

<Modal bind:this={exportModal} title="Start Export">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">S3 bucket
				<input bind:value={exportBucket} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">S3 key
				<input bind:value={exportKey} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if exportError}<p class="text-sm text-red-600 dark:text-red-400">{exportError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => exportModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitExport} disabled={exporting} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{exporting ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={importModal} title="Start Import">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">S3 bucket
				<input bind:value={importBucket} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">S3 key (CSV object)
				<input bind:value={importKey} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if importError}<p class="text-sm text-red-600 dark:text-red-400">{importError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => importModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitImport} disabled={importing} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{importing ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={enrichModal} title="Start Import File Enrichment">
	{#snippet children()}
		<div class="space-y-3">
			<label class="flex flex-col gap-1 text-sm">Source S3 bucket
				<input bind:value={enrichSourceBucket} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Source S3 bucket owner (account ID)
				<input bind:value={enrichSourceBucketOwner} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Source S3 key
				<input bind:value={enrichSourceKey} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Target S3 bucket
				<input bind:value={enrichTargetBucket} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Target S3 bucket owner (account ID)
				<input bind:value={enrichTargetBucketOwner} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			<label class="flex flex-col gap-1 text-sm">Target S3 key
				<input bind:value={enrichTargetKey} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</label>
			{#if enrichError}<p class="text-sm text-red-600 dark:text-red-400">{enrichError}</p>{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => enrichModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEnrich} disabled={enriching} class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50">{enriching ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={exportDetailModal} title="Export {viewedExport?.exportID ?? ''}">
	{#snippet children()}
		{#if viewedExport}
			<div class="space-y-3">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Status</dt><dd>{viewedExport.status ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Progress</dt><dd>{viewedExport.progressPercentage ?? 0}%</dd></div>
					<div><dt class="text-slate-500">S3 location</dt><dd>{viewedExport.s3Bucket}/{viewedExport.s3Key}</dd></div>
					<div><dt class="text-slate-500">Ended</dt><dd>{formatDate(viewedExport.endDateTime)}</dd></div>
				</dl>
				<p class="text-sm text-slate-600 dark:text-slate-300">
					Summary -- Applications: {viewedExport.summary?.applicationsCount ?? 0}, Servers: {viewedExport.summary
						?.serversCount ?? 0}, Waves: {viewedExport.summary?.wavesCount ?? 0} (real, live counts of this
					account's resources -- not a fabricated file content round-trip)
				</p>
				<div>
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Errors</p>
					{#if exportErrorsErr}<p class="text-sm text-red-600 dark:text-red-400">{exportErrorsErr}</p>{/if}
					{#snippet exportErrorCell(e: ExportTaskError)}
						{e.errorData?.rawError ?? '—'}
					{/snippet}
					<DataTable
						rows={exportErrors}
						rowKey={(e) => e.errorDateTime ?? ''}
						columns={[exportErrorColumns[0], { ...exportErrorColumns[1], render: exportErrorCell }]}
						loading={exportErrorsLoading}
						emptyMessage="No errors"
					/>
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => exportDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<Modal bind:this={importDetailModal} title="Import {viewedImport?.importID ?? ''}">
	{#snippet children()}
		{#if viewedImport}
			<div class="space-y-3">
				<dl class="grid grid-cols-2 gap-2 text-sm">
					<div><dt class="text-slate-500">Status</dt><dd>{viewedImport.status ?? '—'}</dd></div>
					<div><dt class="text-slate-500">Progress</dt><dd>{viewedImport.progressPercentage ?? 0}%</dd></div>
					<div>
						<dt class="text-slate-500">S3 location</dt>
						<dd>{viewedImport.s3BucketSource?.s3Bucket}/{viewedImport.s3BucketSource?.s3Key}</dd>
					</div>
					<div><dt class="text-slate-500">Ended</dt><dd>{formatDate(viewedImport.endDateTime)}</dd></div>
				</dl>
				<p class="text-sm text-slate-600 dark:text-slate-300">
					Servers created: {viewedImport.summary?.servers?.createdCount ?? 0}, modified: {viewedImport.summary
						?.servers?.modifiedCount ?? 0} (real per-row parse results, never fabricated)
				</p>
				<div>
					<p class="text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Errors (per-row or whole-object failures)</p>
					{#if importErrorsErr}<p class="text-sm text-red-600 dark:text-red-400">{importErrorsErr}</p>{/if}
					{#snippet importErrorCell(e: ImportTaskError)}
						{#if e.errorData?.rowNumber}Row {e.errorData.rowNumber}: {/if}{e.errorData?.rawError ?? '—'}
					{/snippet}
					<DataTable
						rows={importErrors}
						rowKey={(e) => `${e.errorDateTime}-${e.errorData?.rowNumber}`}
						columns={[importErrorColumns[0], { ...importErrorColumns[1], render: importErrorCell }]}
						loading={importErrorsLoading}
						emptyMessage="No errors"
					/>
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => importDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>
