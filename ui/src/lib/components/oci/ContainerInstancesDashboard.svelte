<script lang="ts">
	import { onMount } from 'svelte';
	import { Container, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { shortOcid } from '#lib/ocirest.ts';
	import {
		actionEnabled,
		createInstance,
		deleteInstance,
		getEngine,
		getInstance,
		inProgress,
		instanceAction,
		isBusy,
		listAvailabilityDomains,
		listCompartments,
		listContainers,
		listInstances,
		listShapes,
		listSubnets,
		listWorkRequestErrors,
		listWorkRequests,
		memoryRange,
		readStoredCiCompartment,
		renameInstance,
		retrieveLogs,
		validateNew,
		writeStoredCiCompartment,
		type ContainerInfo,
		type EnvPair,
		type InstanceAction,
		type InstanceDetail,
		type InstanceSummary,
		type LogsResult,
		type Shape,
		type Subnet,
		type WorkRequest,
		type WorkRequestError
	} from '#lib/ocicontainerinstances.ts';

	const TABS = [
		{ id: 'containers', label: 'Containers' },
		{ id: 'config', label: 'Network and volumes' },
		{ id: 'workrequests', label: 'Work requests' }
	];
	const RESTART_POLICIES = ['ALWAYS', 'NEVER', 'ON_FAILURE'];
	const POLL_MS = 3000;

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredCiCompartment());
	let engine = $state('');
	let loading = $state(true);
	let error = $state('');
	let instances = $state<InstanceSummary[]>([]);
	let selected = $state('');
	let detail = $state<InstanceDetail>();
	let containers = $state<ContainerInfo[]>([]);
	let workRequests = $state<WorkRequest[]>([]);
	let wrErrors = $state<Record<string, WorkRequestError[]>>({});
	let detailError = $state('');
	let tab = $state('containers');
	let nameDraft = $state('');

	let logsFor = $state('');
	let logsPrevious = $state(false);
	let logs = $state<LogsResult>();
	let logsError = $state('');

	let showCreate = $state(false);
	let shapes = $state<Shape[]>([]);
	let subnets = $state<Subnet[]>([]);
	let ads = $state<string[]>([]);
	let fName = $state('');
	let fShape = $state('');
	let fOcpus = $state(1);
	let fMemory = $state(16);
	let fAd = $state('');
	let fSubnet = $state('');
	let fRestart = $state('ALWAYS');
	let fImage = $state('');
	let fContainer = $state('');
	let fCommand = $state('');
	let fArgs = $state('');
	let fEnv = $state<EnvPair[]>([]);

	const shape = $derived(shapes.find((s) => s.name === fShape));
	const range = $derived(shape ? memoryRange(shape, fOcpus) : { min: 1, max: 1 });
	const form = $derived({
		compartmentId: compartment,
		availabilityDomain: fAd,
		displayName: fName,
		shape: fShape,
		ocpus: fOcpus,
		memoryInGBs: fMemory,
		restartPolicy: fRestart,
		subnetId: fSubnet,
		image: fImage,
		containerName: fContainer,
		command: fCommand,
		args: fArgs,
		env: fEnv
	});
	const invalid = $derived(validateNew(form, shape));
	const busyWr = $derived(inProgress(workRequests));
	const inst = $derived(instances.find((i) => i.id === selected));

	async function loadInstances() {
		if (!compartment) {
			instances = [];
			return;
		}
		try {
			instances = await listInstances(compartment);
			error = '';
			if (!instances.some((i) => i.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function loadDetail() {
		if (!selected) {
			detail = undefined;
			containers = [];
			workRequests = [];
			return;
		}
		try {
			[detail, containers, workRequests] = await Promise.all([
				getInstance(selected),
				listContainers(compartment, selected),
				listWorkRequests(compartment, selected)
			]);
			detailError = '';
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	async function refresh() {
		await loadInstances();
		await loadDetail();
	}

	async function load() {
		try {
			[compartments, engine] = await Promise.all([listCompartments(), getEngine()]);
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await refresh();
		loading = false;
	}

	function pickCompartment() {
		writeStoredCiCompartment(compartment);
		selected = '';
		showCreate = false;
		void refresh();
	}

	function select(i: InstanceSummary) {
		selected = selected === i.id ? '' : i.id;
		logsFor = '';
		logs = undefined;
		wrErrors = {};
		nameDraft = i.name;
		tab = 'containers';
		void loadDetail();
	}

	async function act(a: InstanceAction, i: InstanceSummary) {
		const ok = await attempt(() => instanceAction(i.id, a), `${i.name}: ${a} requested`, `Failed to ${a} ${i.name}`);
		if (ok) await refresh();
	}

	async function remove(i: InstanceSummary) {
		const ok = await confirmedAttempt(
			{ title: 'Delete container instance', message: `Delete container instance "${i.name}" and its containers?`, confirmLabel: 'Delete' },
			() => deleteInstance(i.id),
			`Deleting ${i.name}`,
			`Failed to delete ${i.name}`
		);
		if (ok) await refresh();
	}

	async function rename(i: InstanceSummary) {
		const ok = await attempt(() => renameInstance(i.id, nameDraft.trim()), `Renamed to ${nameDraft.trim()}`, 'Failed to rename');
		if (ok) await refresh();
	}

	async function showLogs(c: ContainerInfo, previous = false) {
		logsFor = c.id;
		logsPrevious = previous;
		logs = undefined;
		logsError = '';
		try {
			logs = await retrieveLogs(c.id, previous);
		} catch (e) {
			logsError = describeGcpError(e);
		}
	}

	async function loadWrErrors(w: WorkRequest) {
		if (wrErrors[w.id]) {
			const { [w.id]: _gone, ...rest } = wrErrors;
			wrErrors = rest;
			return;
		}
		try {
			wrErrors = { ...wrErrors, [w.id]: await listWorkRequestErrors(w.id) };
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	async function openCreate() {
		showCreate = !showCreate;
		if (!showCreate) return;
		try {
			[shapes, subnets, ads] = await Promise.all([listShapes(compartment), listSubnets(compartment), listAvailabilityDomains()]);
			if (!shapes.some((s) => s.name === fShape)) pickShape(shapes[0]?.name ?? '');
			if (!fAd) fAd = ads[0] ?? '';
			if (!fSubnet) fSubnet = subnets[0]?.id ?? '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	function pickShape(name: string) {
		fShape = name;
		const s = shapes.find((x) => x.name === name);
		if (!s) return;
		fOcpus = s.ocpuMin;
		fMemory = Math.min(range.max, Math.max(range.min, fOcpus * s.defaultPerOcpu));
	}

	function ocpusChanged() {
		if (shape) fMemory = Math.min(range.max, Math.max(range.min, fOcpus * shape.defaultPerOcpu));
	}

	async function create() {
		if (invalid) return;
		const ok = await attempt(() => createInstance(form), `Container instance ${fName.trim() || 'created'}`, 'Failed to create container instance');
		if (ok) {
			fName = fImage = fContainer = fCommand = fArgs = '';
			fEnv = [];
			showCreate = false;
			await refresh();
		}
	}

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';
	const body = 'divide-y divide-gray-100 dark:divide-gray-800';
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');
	const tone = (s: string) =>
		s === 'ACTIVE' || s === 'RUNNING' || s === 'HEALTHY' || s === 'SUCCEEDED'
			? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300'
			: s === 'FAILED' || s === 'UNHEALTHY'
				? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'
				: isBusy(s) || s === 'IN_PROGRESS' || s === 'ACCEPTED'
					? 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300'
					: 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300';

	onMount(() => {
		void load();
		const timer = setInterval(() => {
			if (!loading && (instances.some((i) => isBusy(i.state)) || busyWr)) void refresh();
		}, POLL_MS);
		return () => clearInterval(timer);
	});
</script>

<div class="space-y-6 pb-20" data-testid="ci-dashboard">
	<PageHeader icon={Container} title="Container Instances" description="Oracle Cloud Container Instances, their containers, logs and work requests" color="orange" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="ci-error">
			<p class="font-medium">Could not load container instances</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3">
			<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				Compartment
				<input class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" list="ci-compartments" bind:value={compartment} onchange={pickCompartment} data-testid="ci-compartment" />
				<datalist id="ci-compartments">{#each compartments as c (c)}<option value={c}></option>{/each}</datalist>
			</label>
			<span class="rounded-full px-2 py-0.5 text-xs {engine === 'docker' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300'}" title={engine === 'docker' ? 'Containers run in the container runtime' : 'Lifecycle only: no container runs and logs are unavailable'} data-testid="ci-engine">Engine: {engine || 'unknown'}</span>
			<button class={GCP_MINI} onclick={openCreate} aria-expanded={showCreate} data-testid="ci-create-toggle">Create container instance</button>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="ci-list-error">{error}</p>{/if}

		{#if showCreate}
			<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-2" onsubmit={(e) => { e.preventDefault(); void create(); }} data-testid="ci-create-form">
				<label>Name (optional) <input class="{GCP_INPUT} !py-1" type="text" bind:value={fName} data-testid="ci-create-name" /></label>
				<label>Shape
					<select class="{GCP_INPUT} !py-1" value={fShape} onchange={(e) => pickShape(e.currentTarget.value)} data-testid="ci-create-shape">
						{#each shapes as s (s.name)}<option value={s.name}>{s.name}</option>{/each}
					</select>
				</label>
				<label>OCPUs{shape ? ` (${shape.ocpuMin}-${shape.ocpuMax})` : ''}
					<input class="{GCP_INPUT} !py-1" type="number" step="1" min={shape?.ocpuMin} max={shape?.ocpuMax} bind:value={fOcpus} oninput={ocpusChanged} data-testid="ci-create-ocpus" />
				</label>
				<label>Memory GB ({range.min}-{range.max})
					<input class="{GCP_INPUT} !py-1" type="number" step="1" min={range.min} max={range.max} bind:value={fMemory} data-testid="ci-create-memory" />
				</label>
				<label>Availability domain
					<input class="{GCP_INPUT} !py-1 font-mono text-xs" list="ci-ads" bind:value={fAd} data-testid="ci-create-ad" />
					<datalist id="ci-ads">{#each ads as a (a)}<option value={a}></option>{/each}</datalist>
				</label>
				<label>Subnet
					{#if subnets.length > 0}
						<select class="{GCP_INPUT} !py-1 font-mono text-xs" bind:value={fSubnet} data-testid="ci-create-subnet">
							{#each subnets as s (s.id)}<option value={s.id}>{s.name} ({s.cidr})</option>{/each}
						</select>
					{:else}
						<input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="ocid1.subnet..." bind:value={fSubnet} data-testid="ci-create-subnet" />
					{/if}
				</label>
				<label>Restart policy
					<select class="{GCP_INPUT} !py-1" bind:value={fRestart} data-testid="ci-create-restart">
						{#each RESTART_POLICIES as p (p)}<option value={p}>{p}</option>{/each}
					</select>
				</label>
				<label>Container name (optional) <input class="{GCP_INPUT} !py-1" type="text" bind:value={fContainer} data-testid="ci-create-container-name" /></label>
				<label class="md:col-span-2">Image <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="alpine:3.20" bind:value={fImage} data-testid="ci-create-image" /></label>
				<label>Command <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="sh" bind:value={fCommand} data-testid="ci-create-command" /></label>
				<label>Arguments <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder={"-c 'echo hello; sleep 3600'"} bind:value={fArgs} data-testid="ci-create-args" /></label>
				<div class="space-y-2 md:col-span-2" data-testid="ci-create-env">
					<p>Environment variables <span class="text-xs text-gray-500">(values are sent to the container and never shown again)</span></p>
					{#each fEnv as pair, idx (idx)}
						<div class="flex gap-2">
							<input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="NAME" bind:value={pair.name} data-testid="ci-env-name" />
							<input class="{GCP_INPUT} !py-1 font-mono text-xs" type="password" autocomplete="off" placeholder="value" bind:value={pair.value} data-testid="ci-env-value" />
							<button type="button" class={GCP_MINI} onclick={() => (fEnv = fEnv.filter((_, k) => k !== idx))}>Remove</button>
						</div>
					{/each}
					<button type="button" class={GCP_MINI} onclick={() => (fEnv = [...fEnv, { name: '', value: '' }])} data-testid="ci-env-add">Add variable</button>
				</div>
				<div class="flex items-center gap-3 md:col-span-2">
					<button type="submit" class={GCP_PRIMARY} disabled={invalid !== ''} data-testid="ci-create-submit">Create</button>
					{#if invalid}<span class="text-xs text-gray-500" data-testid="ci-create-hint">{invalid}</span>{/if}
				</div>
			</form>
		{/if}

		<div class={box}>
			<table class="w-full text-sm" data-testid="ci-instances">
				<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>Shape</th><th class={cell}>Containers</th><th class={cell}>Availability domain</th><th class={cell}>Created</th></tr></thead>
				<tbody class={body}>
					{#each instances as i (i.id)}
						<tr class={selected === i.id ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="ci-instance-row">
							<td class="{cell} text-xs" title={i.id}><button class="underline decoration-dotted" onclick={() => select(i)} aria-expanded={selected === i.id} data-testid="ci-instance-select">{i.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {tone(i.state)}" data-testid="ci-instance-state">{i.state}</span></td>
							<td class="{cell} text-xs" data-testid="ci-instance-shape">{i.shape} · {i.ocpus} OCPU · {i.memoryGBs} GB</td>
							<td class="{cell} text-xs" data-testid="ci-instance-containers">{i.containerCount}</td>
							<td class="{cell} font-mono text-xs">{i.availabilityDomain}</td>
							<td class="{cell} text-xs">{when(i.created)}</td>
						</tr>
					{:else}
						<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="ci-instances-empty">No container instances in this compartment. Create one above.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if inst && detail}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="ci-detail">
				<div class="flex flex-wrap items-center justify-between gap-3">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.name} <span class="rounded-full px-2 py-0.5 text-xs {tone(detail.state)}" data-testid="ci-detail-state">{detail.state}</span></h2>
					<div class="flex flex-wrap gap-2">
						{#each ['start', 'stop', 'restart'] as const as a (a)}
							<button class={GCP_MINI} disabled={!actionEnabled(a, detail.state, busyWr)} onclick={() => act(a, inst)} data-testid="ci-action-{a}">{a[0].toUpperCase() + a.slice(1)}</button>
						{/each}
						<button class={GCP_MINI} disabled={isBusy(detail.state) || busyWr} onclick={() => remove(inst)} data-testid="ci-action-delete">Delete</button>
					</div>
				</div>
				{#if detail.details}<p class="text-xs text-amber-700 dark:text-amber-300" data-testid="ci-detail-details">{detail.details}</p>{/if}
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="ci-detail-error">{detailError}</p>{/if}
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
					<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{detail.id}</dd>
					<dt class="text-gray-500">Shape</dt><dd data-testid="ci-detail-shape">{detail.shape} · {detail.ocpus} OCPU · {detail.memoryGBs} GB</dd>
					<dt class="text-gray-500">Availability domain</dt><dd class="font-mono">{detail.availabilityDomain}</dd>
					<dt class="text-gray-500">Restart policy</dt><dd data-testid="ci-detail-restart">{detail.restartPolicy}</dd>
					<dt class="text-gray-500">Graceful shutdown</dt><dd data-testid="ci-detail-graceful">{detail.gracefulShutdownSeconds === undefined ? 'default' : `${detail.gracefulShutdownSeconds}s`}</dd>
					<dt class="text-gray-500">Created</dt><dd>{when(detail.created)}</dd>
				</dl>
				<form class="flex items-center gap-2 text-sm" onsubmit={(e) => { e.preventDefault(); void rename(inst); }}>
					<input class="{GCP_INPUT} !w-72 !py-1" type="text" bind:value={nameDraft} aria-label="Display name" data-testid="ci-rename-input" />
					<button type="submit" class={GCP_MINI} disabled={nameDraft.trim() === '' || nameDraft.trim() === detail.name} data-testid="ci-rename-submit">Update name</button>
				</form>

				<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="orange" />

				{#if tab === 'containers'}
					<div class={box}>
						<table class="w-full text-sm" data-testid="ci-containers">
							<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Image</th><th class={cell}>State</th><th class={cell}>Exit code</th><th class={cell}>Health</th><th class={cell}>Restarts</th><th class={cell}>Limits</th><th class={cell}>Env names</th><th class="{cell} text-right">Logs</th></tr></thead>
							<tbody class={body}>
								{#each containers as c (c.id)}
									<tr data-testid="ci-container-row">
										<td class="{cell} text-xs" data-testid="ci-container-name">{c.name}</td>
										<td class="{cell} font-mono text-xs break-all" data-testid="ci-container-image">{c.image}</td>
										<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {tone(c.state)}" title={c.details} data-testid="ci-container-state">{c.state}</span></td>
										<td class="{cell} text-xs" data-testid="ci-container-exit">{c.exitCode ?? '—'}</td>
										<td class="{cell} text-xs" data-testid="ci-container-health">{c.health || '—'}</td>
										<td class="{cell} text-xs" data-testid="ci-container-restarts">{c.restartCount}</td>
										<td class="{cell} text-xs" data-testid="ci-container-limits">{c.vcpusLimit ?? '—'} vCPU · {c.memoryLimitGBs ?? '—'} GB</td>
										<td class="{cell} font-mono text-xs break-all" data-testid="ci-container-env">{c.envNames.join(', ') || '—'}</td>
										<td class="{cell} text-right whitespace-nowrap">
											<button class={GCP_MINI} onclick={() => showLogs(c)} data-testid="ci-logs-open">Logs</button>
											<button class={GCP_MINI} onclick={() => showLogs(c, true)} data-testid="ci-logs-previous">Previous run</button>
										</td>
									</tr>
								{:else}
									<tr><td colspan="9" class="{cell} text-center text-gray-500" data-testid="ci-containers-empty">No containers yet</td></tr>
								{/each}
							</tbody>
						</table>
					</div>

					{#if logsFor}
						<div class="space-y-2" data-testid="ci-logs">
							<div class="flex items-center gap-3">
								<h3 class="font-medium text-gray-900 dark:text-white">{containers.find((c) => c.id === logsFor)?.name ?? 'Container'} logs{logsPrevious ? ' (previous run)' : ''}</h3>
								<button class={GCP_MINI} onclick={() => { const c = containers.find((x) => x.id === logsFor); if (c) void showLogs(c, logsPrevious); }} data-testid="ci-logs-refresh">Refresh</button>
								<button class={GCP_MINI} onclick={() => (logsFor = '')}>Close</button>
							</div>
							{#if logsError}
								<p class="rounded-lg border border-amber-200 bg-amber-50 p-3 text-sm text-amber-800 dark:border-amber-900 dark:bg-amber-950/40 dark:text-amber-300" role="alert" data-testid="ci-logs-error">{logsError}</p>
							{:else if logs}
								{#if logs.truncated}<p class="text-xs text-amber-700 dark:text-amber-300" data-testid="ci-logs-truncated">Showing the last part of {logs.totalBytes} bytes.</p>{/if}
								<pre class="max-h-96 overflow-auto rounded-lg bg-gray-900 p-3 font-mono text-xs whitespace-pre-wrap text-gray-100" data-testid="ci-logs-text">{logs.text || '(no output)'}</pre>
							{:else}
								<p class="text-sm text-gray-500">Loading logs...</p>
							{/if}
						</div>
					{/if}
				{:else if tab === 'config'}
					<div class="space-y-4">
						<div>
							<h3 class="mb-1 font-medium text-gray-900 dark:text-white">VNICs</h3>
							<ul class="text-xs" data-testid="ci-vnics">
								{#each detail.vnicIds as v (v)}<li class="font-mono" title={v}>{shortOcid(v)}</li>{:else}<li class="text-gray-500">None</li>{/each}
							</ul>
						</div>
						<div>
							<h3 class="mb-1 font-medium text-gray-900 dark:text-white">Volumes</h3>
							<div class={box}>
								<table class="w-full text-sm" data-testid="ci-volumes">
									<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>Details</th></tr></thead>
									<tbody class={body}>
										{#each detail.volumes as v (v.name)}
											<tr data-testid="ci-volume-row">
												<td class="{cell} text-xs">{v.name}</td>
												<td class="{cell} text-xs">{v.type}</td>
												<td class="{cell} font-mono text-xs">{#if v.type === 'CONFIGFILE'}{v.files.map((f) => `${f.path ? `${f.path}/` : ''}${f.fileName} (${f.sizeBytes} B)`).join(', ')}{:else}{v.backingStore || '—'}{/if}</td>
											</tr>
										{:else}
											<tr><td colspan="3" class="{cell} text-center text-gray-500" data-testid="ci-volumes-empty">No volumes</td></tr>
										{/each}
									</tbody>
								</table>
							</div>
						</div>
						<div>
							<h3 class="mb-1 font-medium text-gray-900 dark:text-white">Image pull secrets</h3>
							<ul class="text-xs" data-testid="ci-secrets">
								{#each detail.pullSecrets as s, k (k)}<li>{s.type} · {s.registry}</li>{:else}<li class="text-gray-500">None</li>{/each}
							</ul>
						</div>
					</div>
				{:else}
					<div class={box}>
						<table class="w-full text-sm" data-testid="ci-workrequests">
							<thead class={thead}><tr><th class={cell}>Operation</th><th class={cell}>Status</th><th class={cell}>Progress</th><th class={cell}>Resources</th><th class={cell}>Accepted</th><th class={cell}>Finished</th><th class="{cell} text-right">Errors</th></tr></thead>
							<tbody class={body}>
								{#each workRequests as w (w.id)}
									<tr data-testid="ci-wr-row">
										<td class="{cell} text-xs">{w.type}</td>
										<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {tone(w.status)}" data-testid="ci-wr-status">{w.status}</span></td>
										<td class="{cell} text-xs" data-testid="ci-wr-percent">{w.percent}%</td>
										<td class="{cell} text-xs">{w.resources}</td>
										<td class="{cell} text-xs">{when(w.accepted)}</td>
										<td class="{cell} text-xs">{when(w.finished)}</td>
										<td class="{cell} text-right"><button class={GCP_MINI} disabled={w.status !== 'FAILED'} onclick={() => loadWrErrors(w)} data-testid="ci-wr-errors">Errors</button></td>
									</tr>
									{#each wrErrors[w.id] ?? [] as e (e.timestamp + e.code)}
										<tr data-testid="ci-wr-error-row"><td colspan="7" class="{cell} text-xs text-red-700 dark:text-red-300">{e.code}: {e.message}</td></tr>
									{/each}
								{:else}
									<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="ci-wr-empty">No work requests</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		{/if}
	{/if}
</div>
