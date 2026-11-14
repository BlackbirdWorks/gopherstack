<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { HardDrive, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { shortOcid } from '#lib/ocirest.ts';
	import {
		ACCESS_MODES,
		MAX_EXPORT_OPTIONS,
		SQUASH_MODES,
		cloneFromSnapshot,
		createExport,
		createFileSystem,
		createMountTarget,
		createSnapshot,
		deleteExport,
		deleteFileSystem,
		deleteMountTarget,
		deleteSnapshot,
		emptyOption,
		formatBytes,
		getExport,
		getNfs,
		getPrivateIp,
		listAvailabilityDomains,
		listCompartments,
		listExports,
		listFileSystems,
		listMountTargets,
		listPolicies,
		listReplications,
		listSnapshots,
		listSubnets,
		readStoredAd,
		readStoredCompartment,
		validateExport,
		validateFileSystem,
		validateMountTarget,
		validateOption,
		validateSnapshotName,
		writeStoredAd,
		writeStoredCompartment,
		type ExportDetail,
		type ExportOption,
		type ExportSummary,
		type FileSystem,
		type MountTarget,
		type NfsInfo,
		type Policy,
		type Replication,
		type Snapshot,
		type Subnet
	} from '#lib/ocifilestorage.ts';

	const TABS = [
		{ id: 'filesystems', label: 'File systems' },
		{ id: 'targets', label: 'Mount targets' },
		{ id: 'exports', label: 'Exports' },
		{ id: 'policies', label: 'Snapshot policies' },
		{ id: 'replications', label: 'Replications' }
	];
	const POLL_MS = 3000;
	const SETTLED = ['ACTIVE', 'FAILED', 'DELETED', 'INACTIVE'];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredCompartment());
	let ads = $state<string[]>([]);
	let ad = $state(readStoredAd());
	let tab = $state('filesystems');
	let loading = $state(true);
	let error = $state('');
	let fileSystems = $state<FileSystem[]>([]);
	let targets = $state<MountTarget[]>([]);
	let exports = $state<ExportSummary[]>([]);
	let policies = $state<Policy[]>([]);
	let replications = $state<Replication[]>([]);
	let subnets = $state<Subnet[]>([]);
	let nfs = $state<NfsInfo>({ enabled: false, endpoints: {} });
	let addresses = $state<Record<string, string>>({});

	let selected = $state('');
	let snapshots = $state<Snapshot[]>([]);
	let snapshotName = $state('');
	let cloneName = $state('');
	let detailError = $state('');

	let fsName = $state('');
	let showFsForm = $state(false);
	let mtName = $state('');
	let mtSubnet = $state('');
	let showMtForm = $state(false);

	let showExportForm = $state(false);
	let exSet = $state('');
	let exFs = $state('');
	let exPath = $state('/');
	let exOptions = $state<ExportOption[]>([emptyOption()]);
	let openExport = $state('');
	let exportDetail = $state<ExportDetail>();

	const fs = $derived(fileSystems.find((f) => f.id === selected));
	const busy = $derived(
		[...fileSystems, ...targets, ...exports, ...snapshots].some((r) => !SETTLED.includes(r.state))
	);
	const fsForm = $derived({ name: fsName, compartmentId: compartment, ad, snapshotId: '' });
	const fsInvalid = $derived(validateFileSystem(fsForm));
	const mtForm = $derived({ name: mtName, compartmentId: compartment, ad, subnetId: mtSubnet });
	const mtInvalid = $derived(validateMountTarget(mtForm));
	const exForm = $derived({ exportSetId: exSet, fileSystemId: exFs, path: exPath, options: exOptions });
	const exInvalid = $derived(validateExport(exForm));
	const snapInvalid = $derived(validateSnapshotName(snapshotName));

	const nameOfFs = (id: string) => fileSystems.find((f) => f.id === id)?.name ?? shortOcid(id);
	const targetOf = (setId: string) => targets.find((m) => m.exportSetId === setId);

	async function loadScope() {
		if (!compartment || !ad) {
			fileSystems = targets = exports = policies = replications = [];
			return;
		}
		try {
			[fileSystems, targets, exports, policies, replications, nfs, subnets] = await Promise.all([
				listFileSystems(compartment, ad),
				listMountTargets(compartment, ad),
				listExports(compartment),
				listPolicies(compartment, ad),
				listReplications(compartment, ad),
				getNfs(),
				listSubnets(compartment)
			]);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
			return;
		}
		if (!fileSystems.some((f) => f.id === selected)) selected = '';
		if (!subnets.some((s) => s.id === mtSubnet)) mtSubnet = subnets[0]?.id ?? '';
		if (!targets.some((m) => m.exportSetId === exSet)) exSet = targets[0]?.exportSetId ?? '';
		if (!fileSystems.some((f) => f.id === exFs)) exFs = fileSystems[0]?.id ?? '';
		await Promise.all([loadSnapshots(), loadAddresses()]);
	}

	async function loadAddresses() {
		const next: Record<string, string> = {};
		await Promise.all(
			targets.flatMap((m) =>
				m.privateIpIds.slice(0, 1).map(async (id) => {
					try {
						next[m.id] = (await getPrivateIp(id)).address;
					} catch {
						next[m.id] = '';
					}
				})
			)
		);
		addresses = next;
	}

	async function loadSnapshots() {
		detailError = '';
		if (!fs) {
			snapshots = [];
			return;
		}
		try {
			snapshots = await listSnapshots(fs.id);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	async function load() {
		try {
			[compartments, ads] = await Promise.all([listCompartments(), listAvailabilityDomains()]);
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
			if (!ads.includes(ad)) ad = ads[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadScope();
		loading = false;
	}

	function pickScope() {
		writeStoredCompartment(compartment);
		writeStoredAd(ad);
		selected = '';
		openExport = '';
		loading = true;
		void loadScope().finally(() => (loading = false));
	}

	function select(f: FileSystem) {
		selected = selected === f.id ? '' : f.id;
		snapshotName = '';
		cloneName = '';
		void loadSnapshots();
	}

	async function addFileSystem() {
		if (fsInvalid) return;
		const ok = await attempt(() => createFileSystem(fsForm), 'File system created', 'Failed to create file system');
		if (ok) {
			fsName = '';
			showFsForm = false;
			await loadScope();
		}
	}

	async function removeFileSystem(f: FileSystem) {
		const ok = await confirmedAttempt(
			{ title: 'Delete file system', message: `Delete "${f.name}" and its snapshots?`, confirmLabel: 'Delete' },
			() => deleteFileSystem(f.id),
			`Deleting ${f.name}`,
			`Failed to delete ${f.name}`
		);
		if (ok) await loadScope();
	}

	async function addSnapshot() {
		if (!fs || snapInvalid) return;
		const ok = await attempt(() => createSnapshot(fs.id, snapshotName), `Snapshot ${snapshotName.trim()} created`, 'Failed to create snapshot');
		if (ok) {
			snapshotName = '';
			await loadScope();
		}
	}

	async function removeSnapshot(s: Snapshot) {
		const ok = await confirmedAttempt(
			{ title: 'Delete snapshot', message: `Delete snapshot "${s.name}"?`, confirmLabel: 'Delete' },
			() => deleteSnapshot(s.id),
			`Deleting ${s.name}`,
			`Failed to delete ${s.name}`
		);
		if (ok) await loadScope();
	}

	async function clone(s: Snapshot) {
		const name = cloneName.trim() || `${fs?.name ?? 'fs'}-clone`;
		const ok = await attempt(() => cloneFromSnapshot(s, name, compartment, ad), `Cloned ${s.name} to ${name}`, 'Failed to clone');
		if (ok) {
			cloneName = '';
			await loadScope();
		}
	}

	async function addTarget() {
		if (mtInvalid) return;
		const ok = await attempt(() => createMountTarget(mtForm), 'Mount target created', 'Failed to create mount target');
		if (ok) {
			mtName = '';
			showMtForm = false;
			await loadScope();
		}
	}

	async function removeTarget(m: MountTarget) {
		const ok = await confirmedAttempt(
			{ title: 'Delete mount target', message: `Delete "${m.name}" and its exports?`, confirmLabel: 'Delete' },
			() => deleteMountTarget(m.id),
			`Deleting ${m.name}`,
			`Failed to delete ${m.name}`
		);
		if (ok) await loadScope();
	}

	async function addExport() {
		if (exInvalid) return;
		const ok = await attempt(() => createExport(exForm), `Export ${exPath.trim()} created`, 'Failed to create export');
		if (ok) {
			exPath = '/';
			exOptions = [emptyOption()];
			showExportForm = false;
			await loadScope();
		}
	}

	async function removeExport(e: ExportSummary) {
		const ok = await confirmedAttempt(
			{ title: 'Delete export', message: `Delete export "${e.path}"?`, confirmLabel: 'Delete' },
			() => deleteExport(e.id),
			`Deleting ${e.path}`,
			`Failed to delete ${e.path}`
		);
		if (ok) {
			if (openExport === e.id) openExport = '';
			await loadScope();
		}
	}

	async function toggleExport(e: ExportSummary) {
		if (openExport === e.id) {
			openExport = '';
			return;
		}
		try {
			exportDetail = await getExport(e.id);
			openExport = e.id;
		} catch (err) {
			toast.error(`Failed to load export: ${describeGcpError(err)}`);
		}
	}

	function addOption() {
		if (exOptions.length < MAX_EXPORT_OPTIONS) exOptions = [...exOptions, emptyOption()];
	}

	function removeOption(i: number) {
		exOptions = exOptions.filter((_, n) => n !== i);
	}

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';
	const body = 'divide-y divide-gray-100 dark:divide-gray-800';
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');
	const subnetName = (id: string) => subnets.find((s) => s.id === id)?.name ?? shortOcid(id);

	onMount(() => {
		void load();
		const timer = setInterval(() => {
			if (!loading && busy) void loadScope();
		}, POLL_MS);
		return () => clearInterval(timer);
	});
</script>

<div class="space-y-6 pb-20" data-testid="fs-dashboard">
	<PageHeader icon={HardDrive} title="File Storage" description="Oracle Cloud file systems, snapshots, mount targets and NFS exports" color="orange" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="fs-error">
			<p class="font-medium">Could not load File Storage</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3">
			<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				Compartment
				<input class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" list="fs-compartments" bind:value={compartment} onchange={pickScope} data-testid="fs-compartment" />
				<datalist id="fs-compartments">{#each compartments as c (c)}<option value={c}></option>{/each}</datalist>
			</label>
			<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				Availability domain
				<select class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={ad} onchange={pickScope} data-testid="fs-ad">
					{#each ads as a (a)}<option value={a}>{a}</option>{/each}
				</select>
			</label>
			<span class="rounded-full px-2 py-0.5 text-xs {nfs.enabled ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300'}" data-testid="fs-nfs-status">
				{nfs.enabled ? 'NFS engine: on' : 'NFS engine: off'}
			</span>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="fs-list-error">{error}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="orange" />

		{#if tab === 'filesystems'}
			<div class="flex items-center gap-3">
				<button class={GCP_MINI} onclick={() => (showFsForm = !showFsForm)} aria-expanded={showFsForm} data-testid="fs-create-toggle">Create file system</button>
			</div>

			{#if showFsForm}
				<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-3" onsubmit={(e) => { e.preventDefault(); void addFileSystem(); }} data-testid="fs-create-form">
					<label class="md:col-span-2">Name <input class="{GCP_INPUT} !py-1" type="text" placeholder="optional" bind:value={fsName} data-testid="fs-create-name" /></label>
					<div class="flex items-end gap-3">
						<button type="submit" class={GCP_PRIMARY} disabled={fsInvalid !== ''} data-testid="fs-create-submit">Create</button>
						{#if fsInvalid}<span class="text-xs text-gray-500" data-testid="fs-create-hint">{fsInvalid}</span>{/if}
					</div>
				</form>
			{/if}

			<div class={box}>
				<table class="w-full text-sm" data-testid="fs-table">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>Metered</th><th class={cell}>Source</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class={body}>
						{#each fileSystems as f (f.id)}
							<tr class={selected === f.id ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="fs-row">
								<td class="{cell} text-xs" title={f.id}><button class="underline decoration-dotted" onclick={() => select(f)} aria-expanded={selected === f.id} data-testid="fs-select">{f.name || shortOcid(f.id)}</button></td>
								<td class="{cell} text-xs" data-testid="fs-state">{f.state}</td>
								<td class="{cell} text-xs" data-testid="fs-metered">{formatBytes(f.meteredBytes)}</td>
								<td class="{cell} text-xs" data-testid="fs-source">{f.parentId ? `clone of ${nameOfFs(f.parentId)}` : '—'}</td>
								<td class="{cell} text-xs">{when(f.created)}</td>
								<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeFileSystem(f)} data-testid="fs-delete">Delete</button></td>
							</tr>
						{:else}
							<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="fs-empty">No file systems in this compartment and availability domain.</td></tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if fs}
				<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="fs-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{fs.name || shortOcid(fs.id)}</h2>
					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
						<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{fs.id}</dd>
						<dt class="text-gray-500">Metered bytes</dt><dd data-testid="fs-detail-metered">{fs.meteredBytes.toLocaleString()} ({formatBytes(fs.meteredBytes)})</dd>
						<dt class="text-gray-500">Clone</dt><dd data-testid="fs-detail-clone">{fs.parentId ? `${fs.cloneStatus || 'ATTACHED'} · parent ${nameOfFs(fs.parentId)}${fs.hydrated === false ? ' · hydrating' : ''}` : fs.isCloneParent ? 'parent of clones' : '—'}</dd>
					</dl>
					{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="fs-detail-error">{detailError}</p>{/if}

					<h3 class="font-medium text-gray-900 dark:text-white">Snapshots</h3>
					<div class={box}>
						<table class="w-full text-sm" data-testid="fs-snapshots">
							<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>State</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class={body}>
								{#each snapshots as s (s.id)}
									<tr data-testid="fs-snapshot-row">
										<td class="{cell} text-xs" data-testid="fs-snapshot-name">{s.name}</td>
										<td class="{cell} text-xs">{s.type}</td>
										<td class="{cell} text-xs">{s.state}</td>
										<td class="{cell} text-xs">{when(s.created)}</td>
										<td class="{cell} text-right whitespace-nowrap">
											<button class={GCP_MINI} onclick={() => clone(s)} disabled={s.state !== 'ACTIVE'} data-testid="fs-snapshot-clone">Clone</button>
											<button class={GCP_MINI} onclick={() => removeSnapshot(s)} data-testid="fs-snapshot-delete">Delete</button>
										</td>
									</tr>
								{:else}
									<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="fs-snapshots-empty">No snapshots</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
					<form class="flex flex-wrap items-end gap-3 text-sm" onsubmit={(e) => { e.preventDefault(); void addSnapshot(); }} data-testid="fs-snapshot-form">
						<label>Snapshot name <input class="{GCP_INPUT} !w-56 !py-1" type="text" bind:value={snapshotName} data-testid="fs-snapshot-input" /></label>
						<button type="submit" class={GCP_PRIMARY} disabled={snapInvalid !== ''} data-testid="fs-snapshot-submit">Create snapshot</button>
						<label>Clone name <input class="{GCP_INPUT} !w-56 !py-1" type="text" placeholder="{fs.name}-clone" bind:value={cloneName} data-testid="fs-clone-input" /></label>
						{#if snapInvalid && snapshotName}<span class="text-xs text-gray-500" data-testid="fs-snapshot-hint">{snapInvalid}</span>{/if}
					</form>
				</div>
			{/if}
		{:else if tab === 'targets'}
			<div class="flex items-center gap-3">
				<button class={GCP_MINI} onclick={() => (showMtForm = !showMtForm)} aria-expanded={showMtForm} data-testid="mt-create-toggle">Create mount target</button>
			</div>

			{#if showMtForm}
				<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-3" onsubmit={(e) => { e.preventDefault(); void addTarget(); }} data-testid="mt-create-form">
					<label>Name <input class="{GCP_INPUT} !py-1" type="text" placeholder="optional" bind:value={mtName} data-testid="mt-create-name" /></label>
					<label>Subnet
						<select class="{GCP_INPUT} !py-1" bind:value={mtSubnet} data-testid="mt-create-subnet">
							{#each subnets as s (s.id)}<option value={s.id}>{s.name} ({s.cidr})</option>{/each}
						</select>
					</label>
					<div class="flex items-end gap-3">
						<button type="submit" class={GCP_PRIMARY} disabled={mtInvalid !== ''} data-testid="mt-create-submit">Create</button>
						{#if mtInvalid}<span class="text-xs text-gray-500" data-testid="mt-create-hint">{mtInvalid}</span>{/if}
					</div>
				</form>
			{/if}

			<div class={box}>
				<table class="w-full text-sm" data-testid="mt-table">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>IP address</th><th class={cell}>Subnet</th><th class={cell}>Export set</th><th class={cell}>NFS endpoint</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class={body}>
						{#each targets as m (m.id)}
							<tr data-testid="mt-row">
								<td class="{cell} text-xs" title={m.id}>{m.name || shortOcid(m.id)}</td>
								<td class="{cell} text-xs" data-testid="mt-state">{m.state}</td>
								<td class="{cell} font-mono text-xs" data-testid="mt-ip">{addresses[m.id] || '—'}</td>
								<td class="{cell} text-xs">{subnetName(m.subnetId)}</td>
								<td class="{cell} font-mono text-xs" title={m.exportSetId} data-testid="mt-set">{shortOcid(m.exportSetId)}</td>
								<td class="{cell} font-mono text-xs" data-testid="mt-nfs">{nfs.enabled ? (nfs.endpoints[m.id] ?? 'starts with its first export') : 'NFS engine off'}</td>
								<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => removeTarget(m)} data-testid="mt-delete">Delete</button></td>
							</tr>
						{:else}
							<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="mt-empty">No mount targets in this compartment and availability domain.</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{:else if tab === 'exports'}
			<div class="flex items-center gap-3">
				<button class={GCP_MINI} onclick={() => (showExportForm = !showExportForm)} aria-expanded={showExportForm} data-testid="ex-create-toggle">Create export</button>
			</div>

			{#if showExportForm}
				<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-3" onsubmit={(e) => { e.preventDefault(); void addExport(); }} data-testid="ex-create-form">
					<label>Mount target
						<select class="{GCP_INPUT} !py-1" bind:value={exSet} data-testid="ex-create-target">
							{#each targets as m (m.id)}<option value={m.exportSetId}>{m.name || shortOcid(m.id)}</option>{/each}
						</select>
					</label>
					<label>File system
						<select class="{GCP_INPUT} !py-1" bind:value={exFs} data-testid="ex-create-fs">
							{#each fileSystems as f (f.id)}<option value={f.id}>{f.name || shortOcid(f.id)}</option>{/each}
						</select>
					</label>
					<label>Path <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={exPath} data-testid="ex-create-path" /></label>

					<div class="space-y-2 md:col-span-3" data-testid="ex-options">
						<p class="font-medium text-gray-900 dark:text-white">Client options</p>
						{#each exOptions as o, i (i)}
							<div class="grid items-end gap-2 md:grid-cols-[2fr_1fr_1fr_auto_auto]" data-testid="ex-option">
								<label>Source <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" bind:value={o.source} data-testid="ex-option-source" /></label>
								<label>Access
									<select class="{GCP_INPUT} !py-1" bind:value={o.access} data-testid="ex-option-access">
										{#each ACCESS_MODES as a (a)}<option value={a}>{a}</option>{/each}
									</select>
								</label>
								<label>Identity squash
									<select class="{GCP_INPUT} !py-1" bind:value={o.squash} data-testid="ex-option-squash">
										{#each SQUASH_MODES as s (s)}<option value={s}>{s}</option>{/each}
									</select>
								</label>
								<label class="flex items-center gap-1 pb-1 text-xs"><input type="checkbox" bind:checked={o.privileged} data-testid="ex-option-privileged" /> Privileged port</label>
								<button type="button" class={GCP_MINI} onclick={() => removeOption(i)} data-testid="ex-option-remove">Remove</button>
								{#if validateOption(o)}<p class="text-xs text-red-600 md:col-span-5" data-testid="ex-option-error">{validateOption(o)}</p>{/if}
							</div>
						{/each}
						<button type="button" class={GCP_MINI} onclick={addOption} disabled={exOptions.length >= MAX_EXPORT_OPTIONS} data-testid="ex-option-add">Add option</button>
					</div>

					<div class="flex items-center gap-3 md:col-span-3">
						<button type="submit" class={GCP_PRIMARY} disabled={exInvalid !== ''} data-testid="ex-create-submit">Create</button>
						{#if exInvalid}<span class="text-xs text-gray-500" data-testid="ex-create-hint">{exInvalid}</span>{/if}
					</div>
				</form>
			{/if}

			<div class={box}>
				<table class="w-full text-sm" data-testid="ex-table">
					<thead class={thead}><tr><th class={cell}>Path</th><th class={cell}>File system</th><th class={cell}>Mount target</th><th class={cell}>State</th><th class="{cell} text-right">Actions</th></tr></thead>
					{#each exports as e (e.id)}
						<tbody class={body}>
							<tr data-testid="ex-row">
								<td class="{cell} font-mono text-xs" data-testid="ex-path">{e.path}</td>
								<td class="{cell} text-xs">{nameOfFs(e.fileSystemId)}</td>
								<td class="{cell} text-xs">{targetOf(e.exportSetId)?.name || shortOcid(e.exportSetId)}</td>
								<td class="{cell} text-xs">{e.state}</td>
								<td class="{cell} text-right whitespace-nowrap">
									<button class={GCP_MINI} onclick={() => toggleExport(e)} aria-expanded={openExport === e.id} data-testid="ex-options-toggle">{openExport === e.id ? 'Hide options' : 'Options'}</button>
									<button class={GCP_MINI} onclick={() => removeExport(e)} data-testid="ex-delete">Delete</button>
								</td>
							</tr>
							{#if openExport === e.id && exportDetail}
								<tr data-testid="ex-detail">
									<td colspan="5" class="{cell}">
										<table class="w-full text-xs" data-testid="ex-options-table">
											<thead class={thead}><tr><th class="px-2 py-1">Source</th><th class="px-2 py-1">Access</th><th class="px-2 py-1">Identity squash</th><th class="px-2 py-1">Privileged port</th></tr></thead>
											<tbody>
												{#each exportDetail.options as o, i (i)}
													<tr data-testid="ex-options-row"><td class="px-2 py-1 font-mono">{o.source}</td><td class="px-2 py-1">{o.access}</td><td class="px-2 py-1">{o.squash}</td><td class="px-2 py-1">{o.privileged ? 'required' : 'not required'}</td></tr>
												{:else}
													<tr><td colspan="4" class="px-2 py-1 text-gray-500">No client options</td></tr>
												{/each}
											</tbody>
										</table>
									</td>
								</tr>
							{/if}
						</tbody>
					{:else}
						<tbody><tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="ex-empty">No exports in this compartment.</td></tr></tbody>
					{/each}
				</table>
			</div>
		{:else if tab === 'policies'}
			<div class={box}>
				<table class="w-full text-sm" data-testid="pol-table">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>Prefix</th><th class={cell}>Schedules</th><th class={cell}>Created</th></tr></thead>
					<tbody class={body}>
						{#each policies as p (p.id)}
							<tr data-testid="pol-row"><td class="{cell} text-xs" title={p.id}>{p.name}</td><td class="{cell} text-xs">{p.state}</td><td class="{cell} font-mono text-xs">{p.prefix || '—'}</td><td class="{cell} text-xs">{p.schedules.join('; ') || '—'}</td><td class="{cell} text-xs">{when(p.created)}</td></tr>
						{:else}
							<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="pol-empty">No snapshot policies</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{:else}
			<div class={box}>
				<table class="w-full text-sm" data-testid="rep-table">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>State</th><th class={cell}>Source</th><th class={cell}>Target</th><th class={cell}>Interval</th><th class={cell}>Delta</th></tr></thead>
					<tbody class={body}>
						{#each replications as r (r.id)}
							<tr data-testid="rep-row"><td class="{cell} text-xs" title={r.id}>{r.name}</td><td class="{cell} text-xs">{r.state}</td><td class="{cell} text-xs">{nameOfFs(r.sourceId)}</td><td class="{cell} text-xs">{nameOfFs(r.targetId)}</td><td class="{cell} text-xs">{r.intervalMinutes} min</td><td class="{cell} text-xs">{r.deltaStatus || '—'}</td></tr>
						{:else}
							<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="rep-empty">No replications</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
