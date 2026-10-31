<script lang="ts">
	import { onMount } from 'svelte';
	import { Network, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		TRY_METHODS,
		addBackend,
		backendKey,
		canTry,
		createLoadBalancer,
		deleteBackend,
		deleteLoadBalancer,
		getLoadBalancerHealth,
		healthTone,
		isServing,
		listCompartments,
		listLoadBalancers,
		listWorkRequests,
		loadHealth,
		prettyBody,
		readStoredLbCompartment,
		tryIt,
		updateBackend,
		validBandwidth,
		validIp,
		validPort,
		validWeight,
		writeStoredLbCompartment,
		type HealthTone,
		type LbBackend,
		type LbBackendSet,
		type LbHealth,
		type LbWorkRequest,
		type LoadBalancer,
		type TryResponse
	} from '#lib/ociloadbalancer.ts';

	const TABS = [
		{ id: 'listeners', label: 'Listeners' },
		{ id: 'backendsets', label: 'Backend sets' },
		{ id: 'routing', label: 'Routing' },
		{ id: 'rules', label: 'Rule sets' },
		{ id: 'certificates', label: 'Certificates' },
		{ id: 'requests', label: 'Work requests' },
		{ id: 'try', label: 'Try it' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredLbCompartment());
	let lbs = $state<LoadBalancer[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let tab = $state('listeners');
	let health = $state<Record<string, LbHealth>>({});
	let lbHealth = $state<LbHealth>();
	let requests = $state<LbWorkRequest[]>([]);
	let detailError = $state('');
	let weights = $state<Record<string, string>>({});
	let newBackend = $state<Record<string, { ip: string; port: string; weight: string }>>({});

	let showCreate = $state(false);
	let formComp = $state('');
	let formName = $state('');
	let formSubnet = $state('');
	let formPrivate = $state(false);
	let formMin = $state('10');
	let formMax = $state('10');
	let formPort = $state('');
	let creating = $state(false);

	let listener = $state('');
	let method = $state('GET');
	let path = $state('/');
	let query = $state('');
	let virtualHost = $state('');
	let headerText = $state('');
	let reqBody = $state('');
	let sending = $state(false);
	let tryError = $state('');
	let result = $state<TryResponse | null>(null);

	const current = $derived(lbs.find((l) => l.id === selected));
	const busy = $derived(requests.some((r) => r.state === 'IN_PROGRESS'));
	const tryable = $derived(current ? current.listeners.filter(canTry) : []);
	const createValid = $derived(
		formComp.trim() !== '' &&
			formName.trim() !== '' &&
			formSubnet.trim() !== '' &&
			validBandwidth(formMin) &&
			validBandwidth(formMax) &&
			Number(formMin) <= Number(formMax) &&
			(formPort.trim() === '' || validPort(formPort))
	);

	async function loadLbs() {
		if (!compartment) {
			lbs = [];
			return;
		}
		try {
			lbs = await listLoadBalancers(compartment);
			for (const l of lbs) for (const set of l.backendSets) newBackend[set.name] ??= { ip: '127.0.0.1', port: '', weight: '1' };
			error = '';
			if (!lbs.some((l) => l.id === selected)) selected = '';
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
			if (!formComp) formComp = compartment;
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadLbs();
		loading = false;
	}

	async function loadDetail() {
		const lb = current;
		detailError = '';
		if (!lb) {
			health = {};
			lbHealth = void 0;
			requests = [];
			return;
		}
		try {
			[health, requests, lbHealth] = await Promise.all([loadHealth(lb), listWorkRequests(lb.id), getLoadBalancerHealth(lb.id)]);
			if (!tryable.some((l) => l.name === listener)) listener = tryable[0]?.name ?? '';
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredLbCompartment(compartment);
		selected = '';
		result = null;
		loading = true;
		void loadLbs();
	}

	function select(lb: LoadBalancer) {
		selected = selected === lb.id ? '' : lb.id;
		result = null;
		tryError = '';
		listener = '';
		void loadDetail();
	}

	const tone = (s: string): HealthTone => healthTone(s);

	async function add(lb: LoadBalancer, set: LbBackendSet) {
		const d = newBackend[set.name];
		if (!d || !validIp(d.ip) || !validPort(d.port) || !validWeight(d.weight)) return;
		const ok = await attempt(
			() => addBackend(lb.id, set.name, { ipAddress: d.ip.trim(), port: Number(d.port), weight: Number(d.weight) }),
			`Backend ${d.ip.trim()}:${d.port} added`,
			'Failed to add backend'
		);
		if (ok) {
			newBackend[set.name] = { ip: d.ip, port: '', weight: '1' };
			await loadLbs();
		}
	}

	async function change(lb: LoadBalancer, set: LbBackendSet, b: LbBackend, patch: Partial<LbBackend>, what: string) {
		const ok = await attempt(() => updateBackend(lb.id, set.name, b, patch), `${b.name}: ${what}`, `Failed to update ${b.name}`);
		if (ok) await loadLbs();
	}

	async function setWeight(lb: LoadBalancer, set: LbBackendSet, b: LbBackend) {
		const v = weights[backendKey(set.name, b.name)] ?? String(b.weight);
		if (!validWeight(v)) return;
		await change(lb, set, b, { weight: Number(v) }, `weight set to ${v}`);
	}

	async function removeBackend(lb: LoadBalancer, set: LbBackendSet, b: LbBackend) {
		const ok = await confirmedAttempt(
			{ title: 'Remove backend', message: `Remove backend "${b.name}" from "${set.name}"?`, confirmLabel: 'Remove' },
			() => deleteBackend(lb.id, set.name, b.name),
			`Removed ${b.name}`,
			`Failed to remove ${b.name}`
		);
		if (ok) await loadLbs();
	}

	async function removeLb(lb: LoadBalancer) {
		const ok = await confirmedAttempt(
			{ title: 'Delete load balancer', message: `Delete load balancer "${lb.name}"?`, confirmLabel: 'Delete' },
			() => deleteLoadBalancer(lb.id),
			`Deletion started for ${lb.name}`,
			`Failed to delete ${lb.name}`
		);
		if (ok) await load();
	}

	async function create() {
		if (!createValid) return;
		creating = true;
		const ok = await attempt(
			() =>
				createLoadBalancer({
					compartmentId: formComp.trim(),
					displayName: formName.trim(),
					subnetId: formSubnet.trim(),
					isPrivate: formPrivate,
					minimumBandwidthInMbps: Number(formMin),
					maximumBandwidthInMbps: Number(formMax),
					listenerPort: formPort.trim() === '' ? null : Number(formPort)
				}),
			`Load balancer ${formName.trim()} requested`,
			'Failed to create load balancer'
		);
		creating = false;
		if (ok) {
			compartment = formComp.trim();
			writeStoredLbCompartment(compartment);
			formName = '';
			showCreate = false;
			await load();
		}
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
		if (!current || !listener) return;
		sending = true;
		tryError = '';
		try {
			result = await tryIt(current.id, listener, { method, path, query, body: reqBody, virtualHost, headers: parseHeaders(headerText) });
		} catch (e) {
			tryError = describeGcpError(e);
			result = null;
		} finally {
			sending = false;
		}
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' || s === 'SUCCEEDED' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';
	const hpill = (s: string) => {
		const t = tone(s);
		return t === 'ok'
			? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300'
			: t === 'warn'
				? 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300'
				: t === 'bad'
					? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'
					: 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300';
	};
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';
	const body = 'divide-y divide-gray-100 dark:divide-gray-800';

	const settled = (st: string) => st === 'ACTIVE' || st === 'FAILED';

	$effect(() => {
		if (lbs.every((l) => settled(l.state))) return;
		const timer = setTimeout(() => void loadLbs(), 1500);
		return () => clearTimeout(timer);
	});

	$effect(() => {
		if (!selected) return;
		const timer = setInterval(() => void loadDetail(), 2000);
		return () => clearInterval(timer);
	});

	onMount(load);
</script>

{#snippet healthBadge(h: LbHealth | undefined, testid: string)}
	<span class="rounded-full px-2 py-0.5 text-xs {hpill(h?.status ?? 'UNKNOWN')}" title={h?.detail ?? ''} data-testid={testid} data-tone={tone(h?.status ?? 'UNKNOWN')}>{h?.status ?? 'UNKNOWN'}</span>
{/snippet}

<div class="space-y-6 pb-20" data-testid="lb-dashboard">
	<PageHeader icon={Network} title="Load Balancer" description="Oracle Cloud load balancers, listeners, backend sets and backend health" color="red" onRefresh={load} />

	{#if error && compartments.length === 0 && !showCreate}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="lb-error">
			<p class="font-medium">Could not load load balancers</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3">
			{#if compartments.length > 0}
				<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
					Compartment
					<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="lb-compartment">
						{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
					</select>
				</label>
			{/if}
			<button class={GCP_MINI} onclick={() => (showCreate = !showCreate)} aria-expanded={showCreate} data-testid="lb-create-toggle">Create load balancer</button>
		</div>

		{#if showCreate}
			<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-2" onsubmit={(e) => { e.preventDefault(); void create(); }} data-testid="lb-create-form">
				<label class="md:col-span-2">Compartment OCID <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={formComp} data-testid="lb-create-compartment" /></label>
				<label>Name <input class="{GCP_INPUT} !py-1" type="text" bind:value={formName} data-testid="lb-create-name" /></label>
				<label>Subnet OCID <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={formSubnet} data-testid="lb-create-subnet" /></label>
				<label>Min bandwidth (Mbps) <input class="{GCP_INPUT} !py-1" type="text" inputmode="numeric" bind:value={formMin} data-testid="lb-create-min" /></label>
				<label>Max bandwidth (Mbps) <input class="{GCP_INPUT} !py-1" type="text" inputmode="numeric" bind:value={formMax} data-testid="lb-create-max" /></label>
				<label>First HTTP listener port (optional) <input class="{GCP_INPUT} !py-1" type="text" inputmode="numeric" placeholder="80" bind:value={formPort} data-testid="lb-create-port" /></label>
				<label class="flex items-end gap-2"><input type="checkbox" bind:checked={formPrivate} data-testid="lb-create-private" /> Private</label>
				<div class="md:col-span-2"><button type="submit" class={GCP_PRIMARY} disabled={!createValid || creating} data-testid="lb-create-submit">Create</button></div>
			</form>
		{/if}

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="lb-list-error">{error}</p>{/if}

		{#if compartments.length === 0}
			<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="lb-empty">No load balancers yet. Create one above or with the OCI SDK or CLI.</p>
		{:else}
			<div class={box}>
				<table class="w-full text-sm" data-testid="lb-list">
					<thead class={thead}><tr><th class={cell}>Load balancer</th><th class={cell}>State</th><th class={cell}>Shape</th><th class={cell}>IP addresses</th><th class={cell}>Listeners (gopherstack address)</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class={body}>
						{#each lbs as lb (lb.id)}
							<tr class={selected === lb.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="lb-row">
								<td class="{cell} font-mono text-xs" title={lb.id}><button class="underline decoration-dotted" onclick={() => select(lb)} aria-expanded={selected === lb.id} data-testid="lb-select">{lb.name}</button></td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(lb.state)}" data-testid="lb-state">{lb.state || '—'}</span></td>
								<td class="{cell} text-xs" data-testid="lb-shape">{lb.shape || '—'}</td>
								<td class="{cell} font-mono text-xs" data-testid="lb-ips">{lb.ips.map((i) => `${i.address}${i.isPublic ? '' : ' (private)'}`).join(', ') || '—'}</td>
								<td class="{cell} font-mono text-xs" data-testid="lb-addresses">
									{#each lb.listeners as l (l.name)}<div>{l.name} :{l.port} → {l.address || 'not serving'}</div>{:else}—{/each}
								</td>
								<td class="{cell} text-right whitespace-nowrap"><button class={GCP_MINI} onclick={() => removeLb(lb)} disabled={lb.state === 'DELETING' || lb.state === 'DELETED'} data-testid="lb-delete">Delete</button></td>
							</tr>
						{:else}
							<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="lb-none">No load balancers in this compartment</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}

		{#if current}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="lb-detail">
				<div class="flex flex-wrap items-center gap-3">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.name}</h2>
					{@render healthBadge(lbHealth, 'lb-health')}
				</div>
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="lb-detail-error">{detailError}</p>{/if}
				{#if busy}<p class="text-xs text-amber-600" data-testid="lb-busy">A work request is in progress; changes are accepted once it finishes.</p>{/if}
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
					<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{current.id}</dd>
					<dt class="text-gray-500">Visibility</dt><dd>{current.isPrivate ? 'Private' : 'Public'}</dd>
					<dt class="text-gray-500">Subnets</dt><dd class="font-mono break-all">{current.subnetIds.join(', ') || '—'}</dd>
				</dl>

				<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

				{#if tab === 'listeners'}
					<div class={box}>
						<table class="w-full text-sm" data-testid="lb-listeners">
							<thead class={thead}><tr><th class={cell}>Listener</th><th class={cell}>Protocol</th><th class={cell}>Port</th><th class={cell}>Default backend set</th><th class={cell}>Hostnames</th><th class={cell}>Routing</th><th class={cell}>Address</th></tr></thead>
							<tbody class={body}>
								{#each current.listeners as l (l.name)}
									<tr data-testid="lb-listener">
										<td class="{cell} font-mono text-xs">{l.name}</td>
										<td class="{cell} text-xs">{l.protocol}{l.tls ? ' (TLS)' : ''}</td>
										<td class="{cell} text-xs">{l.port}</td>
										<td class="{cell} font-mono text-xs">{l.defaultBackendSet}</td>
										<td class="{cell} font-mono text-xs">{l.hostnames.join(', ') || '—'}</td>
										<td class="{cell} text-xs">{l.routingPolicy ? `policy ${l.routingPolicy}` : l.pathRouteSet ? `paths ${l.pathRouteSet}` : '—'}{l.ruleSets.length ? `; rules ${l.ruleSets.join(', ')}` : ''}</td>
										<td class="{cell} font-mono text-xs" data-testid="lb-listener-address">{l.address || 'not serving'}</td>
									</tr>
								{:else}
									<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="lb-listeners-empty">No listeners</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
				{:else if tab === 'backendsets'}
					{#each current.backendSets as set (set.name)}
						<div class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="lb-set">
							<div class="flex flex-wrap items-center gap-3">
								<h3 class="font-medium text-gray-900 dark:text-white" data-testid="lb-set-name">{set.name}</h3>
								<span class="text-xs text-gray-500" data-testid="lb-set-policy">{set.policy}</span>
								{@render healthBadge(health[set.name], 'lb-set-health')}
							</div>
							<p class="font-mono text-xs text-gray-500" data-testid="lb-set-hc">Health check: {set.healthChecker.protocol}{set.healthChecker.urlPath ? ` ${set.healthChecker.urlPath}` : ''}{set.healthChecker.port ? ` port ${set.healthChecker.port}` : ' backend port'}, every {set.healthChecker.intervalMillis} ms, timeout {set.healthChecker.timeoutMillis} ms, {set.healthChecker.retries} retries, expect {set.healthChecker.returnCode}</p>
							<div class={box}>
								<table class="w-full text-sm" data-testid="lb-backends">
									<thead class={thead}><tr><th class={cell}>Backend</th><th class={cell}>Health</th><th class={cell}>Weight</th><th class={cell}>State</th><th class="{cell} text-right">Actions</th></tr></thead>
									<tbody class={body}>
										{#each set.backends as b (b.name)}
											<tr data-testid="lb-backend">
												<td class="{cell} font-mono text-xs" data-testid="lb-backend-name">{b.name}</td>
												<td class={cell}>{@render healthBadge(health[backendKey(set.name, b.name)], 'lb-backend-health')}</td>
												<td class={cell}>
													<span class="inline-flex items-center gap-1">
														<input class="{GCP_INPUT} !w-16 !py-0.5 text-xs" type="text" inputmode="numeric" value={weights[backendKey(set.name, b.name)] ?? String(b.weight)} oninput={(e) => (weights[backendKey(set.name, b.name)] = e.currentTarget.value)} data-testid="lb-backend-weight" aria-label="Weight of {b.name}" />
														<button class={GCP_MINI} onclick={() => setWeight(current, set, b)} disabled={busy || !validWeight(weights[backendKey(set.name, b.name)] ?? String(b.weight))} data-testid="lb-backend-set-weight">Set</button>
													</span>
												</td>
												<td class="{cell} text-xs" data-testid="lb-backend-flags">{[b.offline ? 'offline' : '', b.drain ? 'draining' : '', b.backup ? 'backup' : ''].filter(Boolean).join(', ') || 'in rotation'}</td>
												<td class="{cell} text-right whitespace-nowrap">
													<button class={GCP_MINI} onclick={() => change(current, set, b, { offline: !b.offline }, b.offline ? 'brought online' : 'set offline')} disabled={busy} data-testid="lb-backend-offline">{b.offline ? 'Online' : 'Offline'}</button>
													<button class={GCP_MINI} onclick={() => change(current, set, b, { drain: !b.drain }, b.drain ? 'drain cleared' : 'draining')} disabled={busy} data-testid="lb-backend-drain">{b.drain ? 'Undrain' : 'Drain'}</button>
													<button class={GCP_MINI} onclick={() => removeBackend(current, set, b)} disabled={busy} data-testid="lb-backend-remove">Remove</button>
												</td>
											</tr>
										{:else}
											<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="lb-backends-empty">No backends</td></tr>
										{/each}
									</tbody>
								</table>
							</div>
							{#if newBackend[set.name]}
								{@const d = newBackend[set.name]}
								<div class="flex flex-wrap items-end gap-2 text-xs" data-testid="lb-add-backend">
									<label>IP <input class="{GCP_INPUT} !w-36 !py-1 font-mono" type="text" bind:value={d.ip} data-testid="lb-add-ip" /></label>
									<label>Port <input class="{GCP_INPUT} !w-24 !py-1" type="text" inputmode="numeric" bind:value={d.port} data-testid="lb-add-port" /></label>
									<label>Weight <input class="{GCP_INPUT} !w-20 !py-1" type="text" inputmode="numeric" bind:value={d.weight} data-testid="lb-add-weight" /></label>
									<button class={GCP_MINI} onclick={() => add(current, set)} disabled={busy || !validIp(d.ip) || !validPort(d.port) || !validWeight(d.weight)} data-testid="lb-add-submit">Add backend</button>
								</div>
							{/if}
						</div>
					{:else}
						<p class="text-sm text-gray-500" data-testid="lb-sets-empty">No backend sets</p>
					{/each}
				{:else if tab === 'routing'}
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Path route sets</h3>
					{#each current.pathRouteSets as s (s.name)}
						<div class={box}>
							<table class="w-full text-sm" data-testid="lb-pathroutes">
								<thead class={thead}><tr><th class={cell}>{s.name}: path</th><th class={cell}>Match</th><th class={cell}>Backend set</th></tr></thead>
								<tbody class={body}>
									{#each s.routes as r, i (i)}<tr data-testid="lb-pathroute"><td class="{cell} font-mono text-xs">{r.path}</td><td class="{cell} text-xs">{r.matchType}</td><td class="{cell} font-mono text-xs">{r.backendSet}</td></tr>{/each}
								</tbody>
							</table>
						</div>
					{:else}
						<p class="text-sm text-gray-500" data-testid="lb-pathroutes-empty">No path route sets</p>
					{/each}
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Routing policies</h3>
					{#each current.routingPolicies as p (p.name)}
						<div class={box}>
							<table class="w-full text-sm" data-testid="lb-policies">
								<thead class={thead}><tr><th class={cell}>{p.name}: rule</th><th class={cell}>Condition</th><th class={cell}>Backend sets</th></tr></thead>
								<tbody class={body}>
									{#each p.rules as r (r.name)}<tr data-testid="lb-policy-rule"><td class="{cell} text-xs">{r.name}</td><td class="{cell} font-mono text-xs">{r.condition}</td><td class="{cell} font-mono text-xs">{r.backendSets.join(', ')}</td></tr>{/each}
								</tbody>
							</table>
						</div>
					{:else}
						<p class="text-sm text-gray-500" data-testid="lb-policies-empty">No routing policies</p>
					{/each}
				{:else if tab === 'rules'}
					{#each current.ruleSets as s (s.name)}
						<div class="rounded-lg border border-gray-200 p-3 text-sm dark:border-gray-700" data-testid="lb-ruleset">
							<h3 class="font-medium">{s.name}</h3>
							<ul class="list-disc pl-5 text-xs text-gray-600 dark:text-gray-400">{#each s.items as i, n (n)}<li>{i}</li>{/each}</ul>
						</div>
					{:else}
						<p class="text-sm text-gray-500" data-testid="lb-rulesets-empty">No rule sets</p>
					{/each}
				{:else if tab === 'certificates'}
					{#if current.certificates.length === 0}
						<p class="text-sm text-gray-500" data-testid="lb-certs-empty">No certificates</p>
					{:else}
						<div class={box}>
							<table class="w-full text-sm" data-testid="lb-certs">
								<thead class={thead}><tr><th class={cell}>Certificate</th><th class={cell}>Subjects</th><th class={cell}>Issuer</th><th class={cell}>Expires</th></tr></thead>
								<tbody class={body}>
									{#each current.certificates as c (c.name)}
										<tr data-testid="lb-cert"><td class="{cell} font-mono text-xs">{c.name}</td><td class="{cell} font-mono text-xs" data-testid="lb-cert-subjects">{c.subjects.join(', ') || '—'}</td><td class="{cell} text-xs">{c.issuer || '—'}</td><td class="{cell} text-xs" data-testid="lb-cert-expiry">{c.notAfter || '—'}</td></tr>
									{/each}
								</tbody>
							</table>
						</div>
						<p class="text-xs text-gray-500">Only subjects and expiry are shown; certificate bodies and private keys never leave the emulator.</p>
					{/if}
				{:else if tab === 'requests'}
					<div class={box}>
						<table class="w-full text-sm" data-testid="lb-requests">
							<thead class={thead}><tr><th class={cell}>Type</th><th class={cell}>State</th><th class={cell}>Accepted</th><th class={cell}>Message</th></tr></thead>
							<tbody class={body}>
								{#each requests as r (r.id)}
									<tr data-testid="lb-request"><td class="{cell} text-xs" title={r.id}>{r.type}</td><td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(r.state)}">{r.state}</span></td><td class="{cell} text-xs">{r.accepted}</td><td class="{cell} text-xs">{r.message || '—'}</td></tr>
								{:else}
									<tr><td colspan="4" class="{cell} text-center text-gray-500" data-testid="lb-requests-empty">No work requests</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
				{:else}
					<div class="space-y-3" data-testid="lb-try">
						{#if !isServing(current)}<p class="text-xs text-amber-600">Load balancer is {current.state}; requests are served once it is ACTIVE.</p>{/if}
						{#if tryable.length === 0}
							<p class="text-sm text-gray-500" data-testid="lb-try-none">No serving plain-text listener to send to.</p>
						{:else}
							<p class="text-xs text-gray-500">Sent in-process to the loopback address of the chosen listener; the destination cannot be changed.</p>
							<div class="flex flex-wrap items-end gap-3 text-sm">
								<label>Listener
									<select class="{GCP_INPUT} !w-56 !py-1" bind:value={listener} data-testid="lb-try-listener">{#each tryable as l (l.name)}<option value={l.name}>{l.name} :{l.port} ({l.address})</option>{/each}</select>
								</label>
								<label>Method
									<select class="{GCP_INPUT} !w-28 !py-1" bind:value={method} data-testid="lb-try-method">{#each TRY_METHODS as m (m)}<option value={m}>{m}</option>{/each}</select>
								</label>
								<label class="min-w-64 flex-1">Path <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={path} data-testid="lb-try-path" /></label>
								<label class="w-48">Query <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="a=1&b=2" bind:value={query} data-testid="lb-try-query" /></label>
								<label class="w-56">Virtual host <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="optional" bind:value={virtualHost} data-testid="lb-try-vhost" /></label>
							</div>
							<label class="block text-sm"><span class="mb-1 block font-medium">Headers (Name: value per line)</span>
								<textarea class="{GCP_INPUT} font-mono text-xs" rows="2" spellcheck="false" bind:value={headerText} data-testid="lb-try-headers"></textarea>
							</label>
							<label class="block text-sm"><span class="mb-1 block font-medium">Body</span>
								<textarea class="{GCP_INPUT} font-mono text-xs" rows="3" spellcheck="false" bind:value={reqBody} data-testid="lb-try-body"></textarea>
							</label>
							<button class={GCP_PRIMARY} onclick={send} disabled={sending || !isServing(current) || !listener} data-testid="lb-try-send">Send</button>
						{/if}
						{#if tryError}<p class="text-sm text-red-600" role="alert" data-testid="lb-try-error">{tryError}</p>{/if}
						{#if result}
							<div class="space-y-2" data-testid="lb-try-result">
								<p class="text-sm">Status <span class="rounded-full px-2 py-0.5 text-xs {result.status < 400 ? pill('ACTIVE') : pill('FAILED')}" data-testid="lb-try-status">{result.status}</span>{#if result.truncated} <span class="text-xs text-amber-600">(body truncated)</span>{/if}</p>
								<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs" data-testid="lb-try-response-headers">
									{#each Object.entries(result.headers) as [k, v] (k)}<dt class="text-gray-500">{k}</dt><dd class="font-mono break-all">{v}</dd>{/each}
								</dl>
								<pre class="max-h-96 overflow-auto rounded-md bg-gray-50 p-3 text-xs dark:bg-slate-800" data-testid="lb-try-response-body">{prettyBody(result)}</pre>
							</div>
						{/if}
					</div>
				{/if}
			</div>
		{/if}
	{/if}
</div>
