<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Globe, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { shortOcid } from '#lib/ocirest.ts';
	import {
		DEFAULT_TTL,
		QUERY_TYPES,
		RECORD_TYPES,
		addOp,
		createZone,
		deleteZone,
		editOps,
		getDataPlane,
		getZoneRecords,
		groupRecords,
		isConflict,
		listCompartments,
		listEndpoints,
		listResolvers,
		listSteeringPolicies,
		listTsigKeys,
		listViews,
		listZones,
		patchRecords,
		readStoredDnsCompartment,
		removeOp,
		tryQuery,
		validateQuery,
		validateRecord,
		validateZone,
		writeStoredDnsCompartment,
		type DnsAnswer,
		type DnsRecord,
		type Endpoint,
		type RecordOp,
		type Resolver,
		type Scope,
		type SteeringPolicy,
		type TsigKey,
		type View,
		type Zone
	} from '#lib/ocidns.ts';

	const TABS = [
		{ id: 'zones', label: 'Zones' },
		{ id: 'views', label: 'Views' },
		{ id: 'resolvers', label: 'Resolvers' },
		{ id: 'tsig', label: 'TSIG keys' },
		{ id: 'steering', label: 'Steering policies' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredDnsCompartment());
	let tab = $state('zones');
	let loading = $state(true);
	let error = $state('');
	let zones = $state<Zone[]>([]);
	let views = $state<View[]>([]);
	let resolvers = $state<Resolver[]>([]);
	let endpoints = $state<Record<string, Endpoint[]>>({});
	let tsigKeys = $state<TsigKey[]>([]);
	let policies = $state<SteeringPolicy[]>([]);
	let dataPlane = $state({ enabled: false, port: 0 });

	let selected = $state('');
	let records = $state<DnsRecord[]>([]);
	let etag = $state('');
	let detailError = $state('');

	let showCreate = $state(false);
	let zName = $state('');
	let zScope = $state<Scope>('GLOBAL');
	let zView = $state('');

	let editing = $state<DnsRecord>();
	let rDomain = $state('');
	let rType = $state('A');
	let rTtl = $state(DEFAULT_TTL);
	let rData = $state('');

	let qName = $state('');
	let qType = $state('A');
	let answer = $state<DnsAnswer>();
	let queryError = $state('');
	let querying = $state(false);

	const zone = $derived(zones.find((z) => z.id === selected));
	const rrsets = $derived(groupRecords(records));
	const zoneForm = $derived({ name: zName, compartmentId: compartment, scope: zScope, viewId: zView });
	const zoneInvalid = $derived(validateZone(zoneForm));
	const recordDraft = $derived({ domain: rDomain, rtype: rType, ttl: rTtl, rdata: rData });
	const recordInvalid = $derived(zone ? validateRecord(zone.name, recordDraft) : 'Select a zone');
	const queryInvalid = $derived(validateQuery(qName, qType));

	async function loadAll() {
		if (!compartment) {
			zones = views = resolvers = tsigKeys = policies = [];
			return;
		}
		try {
			[zones, views, resolvers, tsigKeys, policies, dataPlane] = await Promise.all([
				listZones(compartment),
				listViews(compartment),
				listResolvers(compartment),
				listTsigKeys(compartment),
				listSteeringPolicies(compartment),
				getDataPlane()
			]);
			error = '';
			if (!zones.some((z) => z.id === selected)) selected = '';
			if (!views.some((v) => v.id === zView)) zView = views[0]?.id ?? '';
		} catch (e) {
			error = describeGcpError(e);
		}
		await loadRecords();
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
		await loadAll();
		loading = false;
	}

	async function loadRecords() {
		detailError = '';
		if (!zone) {
			records = [];
			etag = '';
			return;
		}
		try {
			const res = await getZoneRecords(zone);
			records = res.records;
			etag = res.etag;
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredDnsCompartment(compartment);
		selected = '';
		loading = true;
		void loadAll().finally(() => (loading = false));
	}

	function select(z: Zone) {
		selected = selected === z.id ? '' : z.id;
		resetRecordForm();
		answer = undefined;
		queryError = '';
		qName = selected ? z.name : '';
		void loadRecords();
	}

	function resetRecordForm() {
		editing = undefined;
		rDomain = zone?.name ?? '';
		rType = 'A';
		rTtl = DEFAULT_TTL;
		rData = '';
	}

	function startEdit(r: DnsRecord) {
		editing = r;
		rDomain = r.domain;
		rType = r.rtype;
		rTtl = r.ttl;
		rData = r.rdata;
	}

	async function applyOps(ops: RecordOp[], success: string) {
		if (!zone) return false;
		try {
			await patchRecords(zone, ops, etag);
			toast.success(success);
			return true;
		} catch (e) {
			if (isConflict(e)) {
				toast.error('The zone changed elsewhere. Records were reloaded; try again.');
			} else {
				toast.error(`Failed to update records: ${describeGcpError(e)}`);
			}
			return false;
		} finally {
			await loadAll();
		}
	}

	async function saveRecord() {
		if (recordInvalid || !zone) return;
		const old = editing;
		const ops = old ? editOps(old, recordDraft) : [addOp(recordDraft)];
		const ok = await applyOps(ops, old ? `${rType} record updated` : `${rType} record added`);
		if (ok) resetRecordForm();
	}

	async function removeRecord(r: DnsRecord) {
		const confirmed = await confirmDestructive({
			title: 'Delete record',
			message: `Delete ${r.rtype} record ${r.domain} -> ${r.rdata}?`,
			confirmLabel: 'Delete'
		});
		if (confirmed && (await applyOps([removeOp(r)], 'Record deleted'))) resetRecordForm();
	}

	async function create() {
		if (zoneInvalid) return;
		const ok = await attempt(() => createZone(zoneForm), `Zone ${zName.trim()} created`, 'Failed to create zone');
		if (ok) {
			zName = '';
			showCreate = false;
			await loadAll();
		}
	}

	async function removeZone(z: Zone) {
		const ok = await confirmedAttempt(
			{ title: 'Delete zone', message: `Delete zone "${z.name}" and all of its records?`, confirmLabel: 'Delete' },
			() => deleteZone(z),
			`Deleting ${z.name}`,
			`Failed to delete ${z.name}`
		);
		if (ok) {
			selected = '';
			await loadAll();
		}
	}

	async function loadEndpoints(r: Resolver) {
		if (endpoints[r.id]) {
			const { [r.id]: _gone, ...rest } = endpoints;
			endpoints = rest;
			return;
		}
		try {
			endpoints = { ...endpoints, [r.id]: await listEndpoints(r.id) };
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function runQuery() {
		if (queryInvalid) return;
		querying = true;
		answer = undefined;
		queryError = '';
		try {
			answer = await tryQuery(qName, qType);
		} catch (e) {
			queryError = describeGcpError(e);
		} finally {
			querying = false;
		}
	}

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';
	const body = 'divide-y divide-gray-100 dark:divide-gray-800';
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');
	const viewName = (id: string) => views.find((v) => v.id === id)?.name ?? shortOcid(id);

	onMount(() => {
		void load();
	});
</script>

<div class="space-y-6 pb-20" data-testid="dns-dashboard">
	<PageHeader icon={Globe} title="DNS" description="Oracle Cloud DNS zones, records, views, resolvers and a live query panel" color="orange" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="dns-error">
			<p class="font-medium">Could not load DNS</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3">
			<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				Compartment
				<input class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" list="dns-compartments" bind:value={compartment} onchange={pickCompartment} data-testid="dns-compartment" />
				<datalist id="dns-compartments">{#each compartments as c (c)}<option value={c}></option>{/each}</datalist>
			</label>
			<span class="rounded-full px-2 py-0.5 text-xs {dataPlane.enabled ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300'}" data-testid="dns-dataplane">
				{dataPlane.enabled ? `Data plane: port ${dataPlane.port}` : 'Data plane: off'}
			</span>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="dns-list-error">{error}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="orange" />

		{#if tab === 'zones'}
			<div class="flex items-center gap-3">
				<button class={GCP_MINI} onclick={() => (showCreate = !showCreate)} aria-expanded={showCreate} data-testid="dns-create-toggle">Create zone</button>
			</div>

			{#if showCreate}
				<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-3" onsubmit={(e) => { e.preventDefault(); void create(); }} data-testid="dns-create-form">
					<label>Zone name <input class="{GCP_INPUT} !py-1" type="text" placeholder="example.com" bind:value={zName} data-testid="dns-create-name" /></label>
					<label>Scope
						<select class="{GCP_INPUT} !py-1" bind:value={zScope} data-testid="dns-create-scope">
							<option value="GLOBAL">GLOBAL</option>
							<option value="PRIVATE">PRIVATE</option>
						</select>
					</label>
					{#if zScope === 'PRIVATE'}
						<label>View
							<select class="{GCP_INPUT} !py-1" bind:value={zView} data-testid="dns-create-view">
								{#each views as v (v.id)}<option value={v.id}>{v.name}</option>{/each}
							</select>
						</label>
					{/if}
					<div class="flex items-center gap-3 md:col-span-3">
						<button type="submit" class={GCP_PRIMARY} disabled={zoneInvalid !== ''} data-testid="dns-create-submit">Create</button>
						{#if zoneInvalid && zName}<span class="text-xs text-gray-500" data-testid="dns-create-hint">{zoneInvalid}</span>{/if}
					</div>
				</form>
			{/if}

			<div class={box}>
				<table class="w-full text-sm" data-testid="dns-zones">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>Scope</th><th class={cell}>Serial</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class={body}>
						{#each zones as z (z.id)}
							<tr class={selected === z.id ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="dns-zone-row">
								<td class="{cell} text-xs" title={z.id}><button class="underline decoration-dotted" onclick={() => select(z)} aria-expanded={selected === z.id} data-testid="dns-zone-select">{z.name}</button></td>
								<td class="{cell} text-xs" data-testid="dns-zone-type">{z.type}</td>
								<td class="{cell} text-xs" data-testid="dns-zone-scope">{z.scope}{z.viewId ? ` · ${viewName(z.viewId)}` : ''}</td>
								<td class="{cell} text-xs" data-testid="dns-zone-serial">{z.serial}</td>
								<td class="{cell} text-xs">{when(z.created)}</td>
								<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeZone(z)} data-testid="dns-zone-delete">Delete</button></td>
							</tr>
						{:else}
							<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="dns-zones-empty">No zones in this compartment. Create one above.</td></tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if zone}
				<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="dns-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{zone.name}</h2>
					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
						<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{zone.id}</dd>
						<dt class="text-gray-500">Name servers</dt><dd class="font-mono" data-testid="dns-detail-ns">{zone.nameservers.join(', ') || '—'}</dd>
					</dl>
					{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="dns-detail-error">{detailError}</p>{/if}

					<div class={box}>
						<table class="w-full text-sm" data-testid="dns-records">
							<thead class={thead}><tr><th class={cell}>Domain</th><th class={cell}>Type</th><th class={cell}>TTL</th><th class={cell}>Data</th><th class="{cell} text-right">Actions</th></tr></thead>
							{#each rrsets as set (set.domain + '|' + set.rtype)}
								<tbody class={body} data-testid="dns-rrset">
									{#each set.records as r, i (r.rdata)}
										<tr data-testid="dns-record-row">
											<td class="{cell} font-mono text-xs" data-testid="dns-record-domain">{i === 0 ? r.domain : ''}</td>
											<td class="{cell} text-xs" data-testid="dns-record-type">{i === 0 ? r.rtype : ''}</td>
											<td class="{cell} text-xs" data-testid="dns-record-ttl">{i === 0 ? r.ttl : ''}</td>
											<td class="{cell} font-mono text-xs break-all" data-testid="dns-record-data">{r.rdata}</td>
											<td class="{cell} text-right whitespace-nowrap">
												<button class={GCP_MINI} disabled={r.protected} onclick={() => startEdit(r)} data-testid="dns-record-edit">Edit</button>
												<button class={GCP_MINI} disabled={r.protected} onclick={() => removeRecord(r)} data-testid="dns-record-delete">Delete</button>
											</td>
										</tr>
									{/each}
								</tbody>
							{:else}
								<tbody><tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="dns-records-empty">No records</td></tr></tbody>
							{/each}
						</table>
					</div>

					<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-4" onsubmit={(e) => { e.preventDefault(); void saveRecord(); }} data-testid="dns-record-form">
						<h3 class="font-medium text-gray-900 dark:text-white md:col-span-4">{editing ? `Edit ${editing.rtype} record` : 'Add record'}</h3>
						<label class="md:col-span-2">Domain <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={rDomain} data-testid="dns-record-domain-input" /></label>
						<label>Type
							<select class="{GCP_INPUT} !py-1" bind:value={rType} disabled={editing !== undefined} data-testid="dns-record-type-input">
								{#each RECORD_TYPES as t (t)}<option value={t}>{t}</option>{/each}
							</select>
						</label>
						<label>TTL (seconds) <input class="{GCP_INPUT} !py-1" type="number" min="0" step="1" bind:value={rTtl} data-testid="dns-record-ttl-input" /></label>
						<label class="md:col-span-4">Data <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="192.0.2.10" bind:value={rData} data-testid="dns-record-data-input" /></label>
						<div class="flex items-center gap-3 md:col-span-4">
							<button type="submit" class={GCP_PRIMARY} disabled={recordInvalid !== ''} data-testid="dns-record-submit">{editing ? 'Save record' : 'Add record'}</button>
							{#if editing}<button type="button" class={GCP_MINI} onclick={resetRecordForm} data-testid="dns-record-cancel">Cancel</button>{/if}
							{#if recordInvalid && (rData || editing)}<span class="text-xs text-gray-500" data-testid="dns-record-hint">{recordInvalid}</span>{/if}
						</div>
					</form>
				</div>
			{/if}

			<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="dns-tryit">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Try it</h2>
				{#if !dataPlane.enabled}
					<p class="text-sm text-gray-500" data-testid="dns-tryit-off">The DNS data plane is off (OCI_DNS_PORT_DNS is 0), so queries are unavailable.</p>
				{:else}
					<p class="text-xs text-gray-500">Runs a query against the emulator's DNS server on 127.0.0.1:{dataPlane.port}. Only GLOBAL zones answer.</p>
					<form class="flex flex-wrap items-end gap-3 text-sm" onsubmit={(e) => { e.preventDefault(); void runQuery(); }}>
						<label>Name <input class="{GCP_INPUT} !w-72 !py-1 font-mono text-xs" type="text" placeholder="www.example.com" bind:value={qName} data-testid="dns-query-name" /></label>
						<label>Type
							<select class="{GCP_INPUT} !py-1" bind:value={qType} data-testid="dns-query-type">
								{#each QUERY_TYPES as t (t)}<option value={t}>{t}</option>{/each}
							</select>
						</label>
						<button type="submit" class={GCP_PRIMARY} disabled={queryInvalid !== '' || querying} data-testid="dns-query-submit">Query</button>
					</form>
					{#if queryError}<p class="text-sm text-red-600" role="alert" data-testid="dns-query-error">{queryError}</p>{/if}
					{#if answer}
						<div class="space-y-2 text-sm" data-testid="dns-answer">
							<p class="text-xs text-gray-500">Status <span class="font-mono" data-testid="dns-answer-rcode">{answer.rcode}</span>{answer.authoritative ? ' · authoritative' : ''} · {answer.name} {answer.type} from {answer.server}</p>
							<div class={box}>
								<table class="w-full text-sm" data-testid="dns-answer-section">
									<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>TTL</th><th class={cell}>Data</th></tr></thead>
									<tbody class={body}>
										{#each answer.answer as rr, i (i)}
											<tr data-testid="dns-answer-row">
												<td class="{cell} font-mono text-xs">{rr.name}</td>
												<td class="{cell} text-xs" data-testid="dns-answer-type">{rr.type}</td>
												<td class="{cell} text-xs">{rr.ttl}</td>
												<td class="{cell} font-mono text-xs break-all" data-testid="dns-answer-data">{rr.data}</td>
											</tr>
										{:else}
											<tr><td colspan="4" class="{cell} text-center text-gray-500" data-testid="dns-answer-empty">Empty answer section</td></tr>
										{/each}
									</tbody>
								</table>
							</div>
						</div>
					{/if}
				{/if}
			</div>
		{:else if tab === 'views'}
			<div class={box}>
				<table class="w-full text-sm" data-testid="dns-views">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>Protected</th><th class={cell}>Created</th><th class={cell}>OCID</th></tr></thead>
					<tbody class={body}>
						{#each views as v (v.id)}
							<tr data-testid="dns-view-row"><td class="{cell} text-xs">{v.name}</td><td class="{cell} text-xs">{v.state}</td><td class="{cell} text-xs">{v.protected ? 'yes' : 'no'}</td><td class="{cell} text-xs">{when(v.created)}</td><td class="{cell} font-mono text-xs" title={v.id}>{shortOcid(v.id)}</td></tr>
						{:else}
							<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="dns-views-empty">No views</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{:else if tab === 'resolvers'}
			<div class={box}>
				<table class="w-full text-sm" data-testid="dns-resolvers">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>VCN</th><th class={cell}>Default view</th><th class="{cell} text-right">Endpoints</th></tr></thead>
					<tbody class={body}>
						{#each resolvers as r (r.id)}
							<tr data-testid="dns-resolver-row">
								<td class="{cell} text-xs">{r.name}</td><td class="{cell} text-xs">{r.state}</td>
								<td class="{cell} font-mono text-xs" title={r.vcnId}>{shortOcid(r.vcnId)}</td>
								<td class="{cell} font-mono text-xs" title={r.defaultViewId}>{r.defaultViewId ? viewName(r.defaultViewId) : '—'}</td>
								<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => loadEndpoints(r)} data-testid="dns-resolver-endpoints">{endpoints[r.id] ? 'Hide' : 'Show'}</button></td>
							</tr>
							{#each endpoints[r.id] ?? [] as e (e.name)}
								<tr data-testid="dns-endpoint-row"><td colspan="5" class="{cell} text-xs">{e.name} · {e.kind} · {e.address || 'no address'} · {e.state} · subnet {shortOcid(e.subnetId)}</td></tr>
							{:else}
								{#if endpoints[r.id]}<tr><td colspan="5" class="{cell} text-xs text-gray-500">No endpoints</td></tr>{/if}
							{/each}
						{:else}
							<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="dns-resolvers-empty">No resolvers</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{:else if tab === 'tsig'}
			<p class="text-xs text-gray-500">Secrets are never shown in the dashboard.</p>
			<div class={box}>
				<table class="w-full text-sm" data-testid="dns-tsig">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Algorithm</th><th class={cell}>State</th><th class={cell}>Created</th></tr></thead>
					<tbody class={body}>
						{#each tsigKeys as k (k.id)}
							<tr data-testid="dns-tsig-row"><td class="{cell} text-xs">{k.name}</td><td class="{cell} font-mono text-xs">{k.algorithm}</td><td class="{cell} text-xs">{k.state}</td><td class="{cell} text-xs">{when(k.created)}</td></tr>
						{:else}
							<tr><td colspan="4" class="{cell} text-center text-gray-500" data-testid="dns-tsig-empty">No TSIG keys</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{:else}
			<div class={box}>
				<table class="w-full text-sm" data-testid="dns-policies">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Template</th><th class={cell}>TTL</th><th class={cell}>State</th><th class={cell}>Created</th></tr></thead>
					<tbody class={body}>
						{#each policies as p (p.id)}
							<tr data-testid="dns-policy-row"><td class="{cell} text-xs">{p.name}</td><td class="{cell} text-xs">{p.template}</td><td class="{cell} text-xs">{p.ttl}</td><td class="{cell} text-xs">{p.state}</td><td class="{cell} text-xs">{when(p.created)}</td></tr>
						{:else}
							<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="dns-policies-empty">No steering policies</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
