<script lang="ts">
	import { onMount } from 'svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import TryItPanel from '#lib/components/TryItPanel.svelte';
	import { GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		getPolicy,
		isActiveSubscription,
		listApis,
		listNamedValues,
		listOperations,
		listProducts,
		listSubscriptions,
		operationPath,
		tryGateway,
		type ApimApi,
		type ApimNamedValue,
		type ApimOperation,
		type ApimProduct,
		type ApimService,
		type ApimSubscription
	} from '#lib/azureapim.ts';

	let { subscription, service }: { subscription: string; service: ApimService } = $props();

	const TABS = [
		{ id: 'apis', label: 'APIs' },
		{ id: 'products', label: 'Products' },
		{ id: 'subscriptions', label: 'Subscriptions' },
		{ id: 'named', label: 'Named values' },
		{ id: 'policies', label: 'Policies' },
		{ id: 'try', label: 'Try it' }
	];

	let tab = $state('apis');
	let apis = $state<ApimApi[]>([]);
	let products = $state<ApimProduct[]>([]);
	let subs = $state<ApimSubscription[]>([]);
	let named = $state<ApimNamedValue[]>([]);
	let errors = $state<string[]>([]);
	let selectedApi = $state('');
	let operations = $state<ApimOperation[]>([]);
	let opsError = $state('');
	let policyApi = $state('');
	let policyOp = $state('');
	let policyOps = $state<ApimOperation[]>([]);
	let policy = $state('');
	let policyError = $state('');
	let trySub = $state('');
	let tryApi = $state('');
	let method = $state('GET');
	let path = $state('/');

	const target = $derived({ resourceGroup: service.resourceGroup, name: service.name });
	const activeSubs = $derived(subs.filter((s) => isActiveSubscription(s)));

	async function load() {
		const errs: string[] = [];
		const run = async <T,>(label: string, p: Promise<T>, fallback: T): Promise<T> => {
			try {
				return await p;
			} catch (e) {
				errs.push(`${label}: ${describeGcpError(e)}`);
				return fallback;
			}
		};
		[apis, products, subs, named] = await Promise.all([
			run('APIs', listApis(subscription, target), [] as ApimApi[]),
			run('Products', listProducts(subscription, target), [] as ApimProduct[]),
			run('Subscriptions', listSubscriptions(subscription, target), [] as ApimSubscription[]),
			run('Named values', listNamedValues(subscription, target), [] as ApimNamedValue[])
		]);
		errors = errs;
		await loadPolicy();
	}

	async function pickApi(a: ApimApi) {
		selectedApi = selectedApi === a.id ? '' : a.id;
		operations = [];
		opsError = '';
		if (!selectedApi) return;
		try {
			operations = await listOperations(subscription, target, a.id);
		} catch (e) {
			opsError = describeGcpError(e);
		}
	}

	function tryOperation(a: ApimApi, o: ApimOperation) {
		method = o.method || 'GET';
		path = operationPath(a, o);
		tryApi = a.id;
		tab = 'try';
	}

	async function loadPolicy() {
		policyError = '';
		try {
			policy = await getPolicy(subscription, target, { api: policyApi || undefined, operation: policyApi ? policyOp || undefined : undefined });
		} catch (e) {
			policy = '';
			policyError = describeGcpError(e);
		}
	}

	async function pickPolicyApi(id: string) {
		policyApi = id;
		policyOp = '';
		policyOps = [];
		if (id) {
			try {
				policyOps = await listOperations(subscription, target, id);
			} catch (e) {
				policyError = describeGcpError(e);
			}
		}
		await loadPolicy();
	}

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const pill = (s: string) =>
		['active', 'published', 'succeeded'].includes(s.toLowerCase()) ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : ['cancelled', 'rejected', 'failed'].includes(s.toLowerCase()) ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300';

	onMount(load);
</script>

<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="apim-detail">
	<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{service.name}</h2>
	{#each errors as e (e)}<p class="text-sm text-red-600" role="alert" data-testid="apim-detail-error">{e}</p>{/each}
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="sky" />

	{#if tab === 'apis'}
		{#if apis.length === 0}
			<p class="text-sm text-gray-500" data-testid="apim-apis-empty">No APIs.</p>
		{:else}
			<table class="w-full text-sm" data-testid="apim-apis">
				<thead class={thead}><tr><th class={cell}>API</th><th class={cell}>Path</th><th class={cell}>Protocols</th><th class={cell}>Backend</th><th class={cell}>Subscription</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each apis as a (a.id)}
						<tr class={selectedApi === a.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="apim-api">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => pickApi(a)} aria-expanded={selectedApi === a.id} data-testid="apim-api-select">{a.id}</button>{a.displayName && a.displayName !== a.id ? ` (${a.displayName})` : ''}</td>
							<td class="{cell} font-mono text-xs">/{a.path}</td>
							<td class="{cell} text-xs">{a.protocols.join(', ') || '—'}</td>
							<td class="{cell} font-mono text-xs break-all">{a.serviceUrl || '—'}</td>
							<td class="{cell} text-xs">{a.subscriptionRequired ? 'Required' : 'Open'}</td>
						</tr>
						{#if selectedApi === a.id}
							<tr><td colspan="5" class={cell}>
								{#if opsError}<p class="text-sm text-red-600" role="alert">{opsError}</p>{/if}
								{#if operations.length === 0 && !opsError}
									<p class="text-xs text-gray-500" data-testid="apim-ops-empty">No operations.</p>
								{:else}
									<table class="w-full text-sm" data-testid="apim-ops">
										<thead class="text-left text-gray-500"><tr><th class={cell}>Operation</th><th class={cell}>Method</th><th class={cell}>URL template</th><th class="{cell} text-right">Actions</th></tr></thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each operations as o (o.id)}
												<tr data-testid="apim-op">
													<td class="{cell} font-mono text-xs">{o.id}</td>
													<td class="{cell} text-xs" data-testid="apim-op-method">{o.method}</td>
													<td class="{cell} font-mono text-xs" data-testid="apim-op-template">{o.urlTemplate}</td>
													<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => tryOperation(a, o)} data-testid="apim-op-try">Try it</button></td>
												</tr>
											{/each}
										</tbody>
									</table>
								{/if}
							</td></tr>
						{/if}
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if tab === 'products'}
		{#if products.length === 0}
			<p class="text-sm text-gray-500" data-testid="apim-products-empty">No products.</p>
		{:else}
			<table class="w-full text-sm" data-testid="apim-products">
				<thead class={thead}><tr><th class={cell}>Product</th><th class={cell}>State</th><th class={cell}>Subscription</th><th class={cell}>Approval</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each products as p (p.id)}
						<tr data-testid="apim-product">
							<td class="{cell} text-xs">{p.displayName || p.id} <span class="font-mono text-gray-500">({p.id})</span></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(p.state)}">{p.state || '—'}</span></td>
							<td class="{cell} text-xs">{p.subscriptionRequired ? 'Required' : 'Open'}</td>
							<td class="{cell} text-xs">{p.approvalRequired ? 'Required' : 'Automatic'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if tab === 'subscriptions'}
		<p class="text-xs text-gray-500">Subscription keys never leave the server. Use "Try it" to send a request with a subscription's key.</p>
		{#if subs.length === 0}
			<p class="text-sm text-gray-500" data-testid="apim-subs-empty">No subscriptions.</p>
		{:else}
			<table class="w-full text-sm" data-testid="apim-subs">
				<thead class={thead}><tr><th class={cell}>Subscription</th><th class={cell}>Scope</th><th class={cell}>State</th><th class={cell}>Created</th><th class={cell}>Keys</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each subs as s (s.id)}
						<tr data-testid="apim-sub">
							<td class="{cell} text-xs">{s.displayName || s.id} <span class="font-mono text-gray-500">({s.id})</span></td>
							<td class="{cell} font-mono text-xs">{s.scope || '—'}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(s.state)}" data-testid="apim-sub-state">{s.state || '—'}</span></td>
							<td class="{cell} text-xs">{s.createdDate || '—'}</td>
							<td class="{cell} text-xs text-gray-500" data-testid="apim-sub-keys">hidden (server-side)</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if tab === 'named'}
		{#if named.length === 0}
			<p class="text-sm text-gray-500" data-testid="apim-named-empty">No named values.</p>
		{:else}
			<table class="w-full text-sm" data-testid="apim-named">
				<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Value</th><th class={cell}>Tags</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each named as n (n.id)}
						<tr data-testid="apim-named-row">
							<td class="{cell} font-mono text-xs">{n.displayName || n.id}</td>
							<td class="{cell} font-mono text-xs" data-testid="apim-named-value">{#if n.secret}<span class="text-gray-500">•••••• (secret)</span>{:else if n.keyVault}<span class="text-gray-500">(Key Vault reference)</span>{:else}{n.value}{/if}</td>
							<td class="{cell} text-xs">{n.tags.join(', ') || '—'}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		{/if}
	{:else if tab === 'policies'}
		<div class="flex flex-wrap items-end gap-3 text-sm">
			<label>Scope
				<select class="{GCP_INPUT} !py-1 text-xs" value={policyApi} onchange={(e) => pickPolicyApi(e.currentTarget.value)} data-testid="apim-policy-api">
					<option value="">All APIs (service level)</option>
					{#each apis as a (a.id)}<option value={a.id}>API: {a.id}</option>{/each}
				</select>
			</label>
			{#if policyApi}
				<label>Operation
					<select class="{GCP_INPUT} !py-1 text-xs" bind:value={policyOp} onchange={loadPolicy} data-testid="apim-policy-op">
						<option value="">(whole API)</option>
						{#each policyOps as o (o.id)}<option value={o.id}>{o.id}</option>{/each}
					</select>
				</label>
			{/if}
		</div>
		{#if policyError}<p class="text-sm text-red-600" role="alert" data-testid="apim-policy-error">{policyError}</p>{/if}
		{#if policy}
			<pre class="max-h-96 overflow-auto rounded-md bg-gray-50 p-3 text-xs dark:bg-slate-800" data-testid="apim-policy-xml">{policy}</pre>
		{:else if !policyError}
			<p class="text-sm text-gray-500" data-testid="apim-policy-empty">No policy at this scope.</p>
		{/if}
	{:else}
		<TryItPanel
			prefix="apim"
			bind:method
			bind:path
			resetKey={service.id}
			note="Sent in-process to this service's gateway. When a subscription is chosen, its key is added on the server and never shown here."
			send={(r) => tryGateway(subscription, target, { ...r, subscription: trySub, api: tryApi })}
		>
			{#snippet fields()}
				<div class="flex flex-wrap items-end gap-3 text-sm">
					<label>Subscription
						<select class="{GCP_INPUT} !py-1 text-xs" bind:value={trySub} data-testid="apim-try-subscription">
							<option value="">(none: no key)</option>
							{#each activeSubs as s (s.id)}<option value={s.id}>{s.displayName || s.id}</option>{/each}
						</select>
					</label>
					<label>API (for key and protocol settings)
						<select class="{GCP_INPUT} !py-1 text-xs" bind:value={tryApi} data-testid="apim-try-api">
							<option value="">(service defaults)</option>
							{#each apis as a (a.id)}<option value={a.id}>{a.id}</option>{/each}
						</select>
					</label>
				</div>
			{/snippet}
		</TryItPanel>
	{/if}
</div>
