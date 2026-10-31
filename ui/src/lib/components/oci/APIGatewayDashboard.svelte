<script lang="ts">
	import { onMount } from 'svelte';
	import { Waypoints, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		TRY_METHODS,
		backendLabel,
		getGateway,
		isServing,
		joinPath,
		listCertificates,
		listCompartments,
		listDeployments,
		listGateways,
		prettyBody,
		readStoredApigwCompartment,
		tryIt,
		writeStoredApigwCompartment,
		type ApigwCertificate,
		type ApigwDeployment,
		type ApigwGateway,
		type ApigwRoute,
		type TryResponse
	} from '#lib/ociapigateway.ts';

	const TABS = [
		{ id: 'deployments', label: 'Deployments' },
		{ id: 'try', label: 'Try it' },
		{ id: 'certificates', label: 'Certificates' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredApigwCompartment());
	let gateways = $state<ApigwGateway[]>([]);
	let certificates = $state<ApigwCertificate[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let detail = $state<ApigwGateway | null>(null);
	let deployments = $state<ApigwDeployment[]>([]);
	let detailError = $state('');
	let tab = $state('deployments');

	let method = $state('GET');
	let path = $state('/');
	let query = $state('');
	let headerText = $state('');
	let reqBody = $state('');
	let sending = $state(false);
	let tryError = $state('');
	let result = $state<TryResponse | null>(null);

	async function loadGateways() {
		if (!compartment) {
			gateways = [];
			return;
		}
		try {
			[gateways, certificates] = await Promise.all([listGateways(compartment), listCertificates(compartment)]);
			error = '';
			if (!gateways.some((g) => g.id === selected)) selected = '';
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
		await loadGateways();
		loading = false;
	}

	async function loadDetail() {
		detailError = '';
		if (!selected) {
			detail = null;
			deployments = [];
			return;
		}
		try {
			[detail, deployments] = await Promise.all([getGateway(selected), listDeployments(compartment, selected)]);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredApigwCompartment(compartment);
		selected = '';
		result = null;
		loading = true;
		void loadGateways();
	}

	function select(g: ApigwGateway) {
		selected = selected === g.id ? '' : g.id;
		detail = null;
		result = null;
		tryError = '';
		void loadDetail();
	}

	function tryRoute(d: ApigwDeployment, r: ApigwRoute) {
		method = r.methods.find((m) => TRY_METHODS.includes(m)) ?? 'GET';
		path = joinPath(d.pathPrefix, r.path.replaceAll(/\{(\w+)\*?\}/g, '$1'));
		query = '';
		tab = 'try';
	}

	function parseHeaders(text: string): { name: string; value: string }[] {
		return text
			.split('\n')
			.map((l) => l.trim())
			.filter(Boolean)
			.map((l) => {
				const i = l.indexOf(':');
				return i < 0 ? { name: l, value: '' } : { name: l.slice(0, i).trim(), value: l.slice(i + 1).trim() };
			});
	}

	async function send() {
		sending = true;
		tryError = '';
		try {
			result = await tryIt(selected, { method, path, query, body: reqBody, headers: parseHeaders(headerText) });
		} catch (e) {
			tryError = describeGcpError(e);
			result = null;
		} finally {
			sending = false;
		}
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';

	const settled = (st: string) => st === 'ACTIVE' || st === 'FAILED';

	$effect(() => {
		if (gateways.every((g) => settled(g.state)) && deployments.every((d) => settled(d.state))) return;
		const timer = setTimeout(() => void loadGateways(), 1500);
		return () => clearTimeout(timer);
	});

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="apigw-dashboard">
	<PageHeader icon={Waypoints} title="API Gateway" description="Oracle Cloud API gateways, deployments and certificates" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="apigw-error">
			<p class="font-medium">Could not load gateways</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="apigw-empty">No gateways yet. Create one with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="apigw-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="apigw-list-error">{error}</p>{/if}

		<div class={box}>
			<table class="w-full text-sm" data-testid="apigw-gateways">
				<thead class={thead}><tr><th class={cell}>Gateway</th><th class={cell}>State</th><th class={cell}>Hostname</th><th class={cell}>Endpoint</th><th class={cell}>Created</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each gateways as g (g.id)}
						<tr class={selected === g.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="apigw-gateway">
							<td class="{cell} font-mono text-xs" title={g.id}><button class="underline decoration-dotted" onclick={() => select(g)} aria-expanded={selected === g.id} data-testid="apigw-select">{g.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(g.state)}" data-testid="apigw-state">{g.state || '—'}</span></td>
							<td class="{cell} font-mono text-xs" data-testid="apigw-hostname">{g.hostname || '—'}</td>
							<td class="{cell} text-xs" data-testid="apigw-endpoint-type">{g.endpointType || '—'}</td>
							<td class="{cell} text-xs">{g.created}</td>
						</tr>
					{:else}
						<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="apigw-none">No gateways in this compartment</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="apigw-detail-error">{detailError}</p>{/if}

		{#if detail}
			<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="apigw-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.name}</h2>
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
					<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{detail.id}</dd>
					<dt class="text-gray-500">Hostname</dt><dd class="font-mono">{detail.hostname}</dd>
					<dt class="text-gray-500">Endpoint type</dt><dd>{detail.endpointType}</dd>
					<dt class="text-gray-500">IP mode</dt><dd>{detail.ipMode || '—'}</dd>
					<dt class="text-gray-500">Subnet</dt><dd class="font-mono break-all">{detail.subnetId || '—'}</dd>
					<dt class="text-gray-500">Certificate</dt><dd class="font-mono break-all">{detail.certificateId || '—'}</dd>
				</dl>
			</div>
		{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

		{#if tab === 'certificates'}
			{#if certificates.length === 0}
				<p class="text-sm text-gray-500" data-testid="apigw-certs-empty">No certificates</p>
			{:else}
				<div class={box}>
					<table class="w-full text-sm" data-testid="apigw-certs">
						<thead class={thead}><tr><th class={cell}>Certificate</th><th class={cell}>State</th><th class={cell}>Subjects</th><th class={cell}>Expires</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each certificates as c (c.id)}
								<tr data-testid="apigw-cert">
									<td class="{cell} font-mono text-xs" title={c.id}>{c.name}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(c.state)}">{c.state}</span></td>
									<td class="{cell} font-mono text-xs" data-testid="apigw-cert-subjects">{c.subjects.join(', ') || '—'}</td>
									<td class="{cell} text-xs" data-testid="apigw-cert-expiry">{c.notAfter || '—'}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
				<p class="text-xs text-gray-500">Private keys are discarded by the emulator and are never shown.</p>
			{/if}
		{:else if !detail}
			<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="apigw-pick">Select a gateway to see its {tab === 'try' ? 'data plane' : 'deployments'}.</p>
		{:else if tab === 'deployments'}
			{#if deployments.length === 0}
				<p class="text-sm text-gray-500" data-testid="apigw-deployments-empty">No deployments on this gateway</p>
			{/if}
			{#each deployments as d (d.id)}
				<div class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="apigw-deployment">
					<div class="flex flex-wrap items-center gap-3">
						<h3 class="font-medium text-gray-900 dark:text-white">{d.name}</h3>
						<span class="rounded-full px-2 py-0.5 text-xs {pill(d.state)}" data-testid="apigw-deployment-state">{d.state}</span>
						<span class="font-mono text-xs text-gray-500" data-testid="apigw-prefix">{d.pathPrefix}</span>
					</div>
					<p class="font-mono text-xs text-gray-500 break-all">{d.endpoint}</p>
					{#if d.policies.length}<p class="text-xs text-gray-500" data-testid="apigw-deployment-policies">Policies: {d.policies.join(', ')}</p>{/if}
					<div class={box}>
						<table class="w-full text-sm" data-testid="apigw-routes">
							<thead class={thead}><tr><th class={cell}>Path</th><th class={cell}>Methods</th><th class={cell}>Backend</th><th class={cell}>Policies</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each d.routes as r, i (i)}
									<tr data-testid="apigw-route">
										<td class="{cell} font-mono text-xs">{r.path}</td>
										<td class="{cell} text-xs" data-testid="apigw-route-methods">{r.methods.join(', ')}</td>
										<td class="{cell} text-xs" data-testid="apigw-route-backend"><span class="font-medium">{backendLabel(r.backendType)}</span> <span class="font-mono text-gray-500">{r.backend}</span></td>
										<td class="{cell} text-xs" data-testid="apigw-route-policies">{r.policies.join(', ') || '—'}</td>
										<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => tryRoute(d, r)} disabled={!isServing(detail)} data-testid="apigw-route-try">Try</button></td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/each}
		{:else}
			<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="apigw-try">
				{#if !isServing(detail)}<p class="text-xs text-amber-600">Gateway is {detail.state}; requests are served once it is ACTIVE.</p>{/if}
				<p class="text-xs text-gray-500">Sent in-process to the data plane of {detail.name}; the destination cannot be changed. Include the deployment path prefix in the path.</p>
				<div class="flex flex-wrap items-end gap-3 text-sm">
					<label>Method
						<select class="{GCP_INPUT} !w-28 !py-1" bind:value={method} data-testid="apigw-try-method">{#each TRY_METHODS as m (m)}<option value={m}>{m}</option>{/each}</select>
					</label>
					<label class="min-w-64 flex-1">Path <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={path} data-testid="apigw-try-path" /></label>
					<label class="w-56">Query <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="a=1&b=2" bind:value={query} data-testid="apigw-try-query" /></label>
				</div>
				<label class="block text-sm"><span class="mb-1 block font-medium">Headers (Name: value per line)</span>
					<textarea class="{GCP_INPUT} font-mono text-xs" rows="2" spellcheck="false" bind:value={headerText} data-testid="apigw-try-headers"></textarea>
				</label>
				<label class="block text-sm"><span class="mb-1 block font-medium">Body</span>
					<textarea class="{GCP_INPUT} font-mono text-xs" rows="4" spellcheck="false" bind:value={reqBody} data-testid="apigw-try-body"></textarea>
				</label>
				<button class={GCP_PRIMARY} onclick={send} disabled={sending || !isServing(detail)} data-testid="apigw-try-send">Send</button>
				{#if tryError}<p class="text-sm text-red-600" role="alert" data-testid="apigw-try-error">{tryError}</p>{/if}
				{#if result}
					<div class="space-y-2" data-testid="apigw-try-result">
						<p class="text-sm">Status <span class="rounded-full px-2 py-0.5 text-xs {result.status < 400 ? pill('ACTIVE') : pill('FAILED')}" data-testid="apigw-try-status">{result.status}</span>{#if result.truncated} <span class="text-xs text-amber-600">(body truncated)</span>{/if}</p>
						<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs" data-testid="apigw-try-response-headers">
							{#each Object.entries(result.headers) as [k, v] (k)}<dt class="text-gray-500">{k}</dt><dd class="font-mono break-all">{v}</dd>{/each}
						</dl>
						<pre class="max-h-96 overflow-auto rounded-md bg-gray-50 p-3 text-xs dark:bg-slate-800" data-testid="apigw-try-response-body">{prettyBody(result)}</pre>
					</div>
				{/if}
			</div>
		{/if}
	{/if}
</div>
