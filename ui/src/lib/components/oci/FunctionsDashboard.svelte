<script lang="ts">
	import { onMount } from 'svelte';
	import { Play, RefreshCw, Zap } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		getApplication,
		getFunction,
		invokeFunction,
		listApplications,
		listFunctions,
		listWorkRequestLogs,
		listWorkRequests,
		shortOcid,
		type AppInfo,
		type ConfigEntry,
		type FunctionInfo,
		type InvokeResult,
		type WorkRequestInfo,
		type WorkRequestLog
	} from '#lib/ocifunctions.ts';

	const TABS = [
		{ id: 'functions', label: 'Functions' },
		{ id: 'invoke', label: 'Invoke' },
		{ id: 'workrequests', label: 'Work requests' }
	];

	let apps = $state<AppInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let appId = $state('');
	let appDetail = $state<AppInfo | null>(null);
	let tab = $state('functions');
	let fns = $state<FunctionInfo[]>([]);
	let childLoading = $state(false);
	let childError = $state('');
	let fnId = $state('');
	let fnDetail = $state<FunctionInfo | null>(null);

	let body = $state('{"name": "world"}');
	let contentType = $state('application/json');
	let dryRun = $state(false);
	let invoking = $state(false);
	let invokeError = $state('');
	let invokeResult = $state<InvokeResult | null>(null);

	let workRequests = $state<WorkRequestInfo[]>([]);
	let wrLoading = $state(false);
	let wrError = $state('');
	let wrId = $state('');
	let wrLogs = $state<WorkRequestLog[]>([]);

	async function load() {
		try {
			apps = await listApplications();
			error = '';
			if (!apps.some((a) => a.id === appId)) {
				appId = '';
				appDetail = null;
			}
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChildren() {
		if (!appId) return;
		childLoading = true;
		childError = '';
		fnId = '';
		fnDetail = null;
		invokeResult = null;
		invokeError = '';
		try {
			[appDetail, fns] = await Promise.all([getApplication(appId), listFunctions(appId)]);
		} catch (e) {
			childError = describeGcpError(e);
			appDetail = null;
			fns = [];
		} finally {
			childLoading = false;
		}
	}

	function selectApp(row: ResourceRow) {
		appId = appId === row.id ? '' : row.id;
		tab = 'functions';
		appDetail = null;
		fns = [];
		fnId = '';
		fnDetail = null;
		if (appId) void loadChildren();
	}

	async function selectFn(row: ResourceRow) {
		fnId = fnId === row.id ? '' : row.id;
		fnDetail = null;
		invokeResult = null;
		invokeError = '';
		if (!fnId) return;
		try {
			fnDetail = await getFunction(fnId);
			childError = '';
		} catch (e) {
			childError = describeGcpError(e);
		}
	}

	async function invoke() {
		if (!fnId || invoking) return;
		invoking = true;
		invokeError = '';
		invokeResult = null;
		try {
			invokeResult = await invokeFunction(fnId, { body, contentType, dryRun });
		} catch (e) {
			invokeError = describeGcpError(e);
		} finally {
			invoking = false;
		}
	}

	async function loadWorkRequests() {
		wrLoading = true;
		wrError = '';
		wrId = '';
		wrLogs = [];
		try {
			workRequests = await listWorkRequests();
		} catch (e) {
			wrError = describeGcpError(e);
			workRequests = [];
		} finally {
			wrLoading = false;
		}
	}

	async function selectWr(row: ResourceRow) {
		wrId = wrId === row.id ? '' : row.id;
		wrLogs = [];
		if (!wrId) return;
		try {
			wrLogs = await listWorkRequestLogs(wrId);
		} catch (e) {
			wrError = describeGcpError(e);
		}
	}

	function openTab(id: string) {
		tab = id;
		if (id === 'workrequests') void loadWorkRequests();
	}

	const appRows = $derived(apps.map((a) => ({ id: a.id, cells: [a.name, a.state, a.shape, shortOcid(a.compartmentId), a.timeCreated], raw: a.raw })));
	const fnRows = $derived(fns.map((f) => ({ id: f.id, cells: [f.name, f.state, f.image, `${f.memoryMb} MB`, `${f.timeoutSec} s`], raw: f.raw })));
	const wrRows = $derived(
		workRequests.map((w) => ({
			id: w.id,
			cells: [w.operation, w.status, `${w.percentComplete}%`, w.resources.map((r) => shortOcid(r.identifier)).join(', '), w.timeAccepted],
			raw: w
		}))
	);
	const cell = 'px-4 py-2';

	onMount(load);
</script>

{#snippet configTable(entries: ConfigEntry[], testId: string)}
	{#if entries.length === 0}
		<p class="text-xs text-gray-500" data-testid="{testId}-empty">No configuration</p>
	{:else}
		<table class="w-full text-xs" data-testid={testId}>
			<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Key</th><th class={cell}>Value</th></tr></thead>
			<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
				{#each entries as e (e.key)}
					<tr data-testid="{testId}-row"><td class="{cell} font-mono">{e.key}</td><td class="{cell} font-mono break-all" data-masked={e.masked}>{e.value}{#if e.masked} <span class="text-gray-400">(hidden)</span>{/if}</td></tr>
				{/each}
			</tbody>
		</table>
	{/if}
{/snippet}

<div class="space-y-6 pb-20" data-testid="oci-functions-dashboard">
	<PageHeader icon={Zap} title="Functions" description="Oracle Cloud Functions: applications, functions, synchronous invocation and work requests" color="red" onRefresh={load} />
	<GcpResourceTable testId="oci-fn-app" noun="applications" columns={['Application', 'State', 'Shape', 'Compartment', 'Created']} rows={appRows} {loading} {error} selected={appId} onRetry={load} onSelect={selectApp} />

	{#if appId}
		<div class="space-y-3" data-testid="oci-fn-detail">
			{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="oci-fn-error">{childError}</p>{/if}
			{#if childLoading}
				<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
			{:else}
				{#if appDetail}
					<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-3 space-y-2" data-testid="oci-fn-app-config">
						<p class="text-sm font-semibold">Application configuration</p>
						{#if appDetail.syslogUrl}<p class="text-xs font-mono">syslog: {appDetail.syslogUrl}</p>{/if}
						{@render configTable(appDetail.config, 'oci-fn-app-cfg')}
					</div>
				{/if}
				<Tabs tabs={TABS} active={tab} onSelect={openTab} color="red" />
				{#if tab === 'functions'}
					<GcpResourceTable testId="oci-fn" noun="functions" columns={['Function', 'State', 'Image', 'Memory', 'Timeout']} rows={fnRows} loading={false} error="" selected={fnId} onRetry={loadChildren} onSelect={selectFn} />
					{#if fnDetail}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-3 space-y-2" data-testid="oci-fn-fn-detail">
							<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
								<dt class="font-semibold">Image</dt><dd class="font-mono break-all" data-testid="oci-fn-image">{fnDetail.image}{fnDetail.imageDigest ? `@${fnDetail.imageDigest}` : ''}</dd>
								<dt class="font-semibold">Memory</dt><dd data-testid="oci-fn-memory">{fnDetail.memoryMb} MB</dd>
								<dt class="font-semibold">Timeout</dt><dd data-testid="oci-fn-timeout">{fnDetail.timeoutSec} seconds</dd>
								<dt class="font-semibold">Invoke endpoint</dt><dd class="font-mono break-all" data-testid="oci-fn-endpoint">{fnDetail.invokeEndpoint}</dd>
							</dl>
							<p class="text-sm font-semibold">Function configuration</p>
							{@render configTable(fnDetail.config, 'oci-fn-cfg')}
						</div>
					{/if}
				{:else if tab === 'invoke'}
					{#if !fnId}
						<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="oci-invoke-select">Select a function on the Functions tab to invoke it.</p>
					{:else}
						<div class="space-y-3" data-testid="oci-invoke">
							<p class="text-sm">Invoking <span class="font-mono">{fnDetail?.name ?? shortOcid(fnId)}</span> synchronously. A cold start pulls the image and can take a while.</p>
							<label class="block text-sm"><span class="mb-1 block font-medium">Content type</span><input class={GCP_INPUT} bind:value={contentType} data-testid="oci-invoke-content-type" /></label>
							<label class="block text-sm"><span class="mb-1 block font-medium">Body</span><textarea class="{GCP_INPUT} h-32 font-mono" spellcheck="false" bind:value={body} data-testid="oci-invoke-body"></textarea></label>
							<label class="flex items-center gap-2 text-sm"><input type="checkbox" bind:checked={dryRun} data-testid="oci-invoke-dry" /> Dry run (validate without running the function)</label>
							<button class={GCP_PRIMARY} onclick={invoke} disabled={invoking} data-testid="oci-invoke-run"><Play class="inline h-4 w-4" /> {invoking ? 'Invoking…' : 'Invoke'}</button>
							{#if invokeError}<p class="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="oci-invoke-error">{invokeError}</p>{/if}
							{#if invokeResult}
								<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-3 space-y-2" data-testid="oci-invoke-result">
									<p class="text-sm">Status <span class="font-semibold" data-testid="oci-invoke-status">{invokeResult.status}</span>{#if invokeResult.callId} · call <span class="font-mono text-xs">{invokeResult.callId}</span>{/if}</p>
									<table class="w-full text-xs" data-testid="oci-invoke-headers">
										<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Response header</th><th class={cell}>Value</th></tr></thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each invokeResult.headers as [k, v] (k)}<tr data-testid="oci-invoke-header"><td class="{cell} font-mono">{k}</td><td class="{cell} font-mono break-all">{v}</td></tr>{:else}<tr><td class={cell} colspan="2">No response headers</td></tr>{/each}
										</tbody>
									</table>
									<pre class="max-h-80 overflow-auto whitespace-pre-wrap break-all rounded-md bg-gray-50 dark:bg-slate-900 p-2 font-mono text-xs" data-testid="oci-invoke-output">{invokeResult.text ?? `base64:${invokeResult.base64}`}</pre>
								</div>
							{/if}
						</div>
					{/if}
				{:else}
					<GcpResourceTable testId="oci-wr" noun="work requests" columns={['Operation', 'Status', 'Progress', 'Resources', 'Accepted']} rows={wrRows} loading={wrLoading} error={wrError} selected={wrId} onRetry={loadWorkRequests} onSelect={selectWr} />
					{#if wrId}
						<ul class="space-y-1 text-xs font-mono" data-testid="oci-wr-logs">
							{#each wrLogs as l, i (i)}<li data-testid="oci-wr-log">{l.timestamp} {l.message}</li>{/each}
						</ul>
					{/if}
				{/if}
			{/if}
		</div>
	{/if}
</div>
