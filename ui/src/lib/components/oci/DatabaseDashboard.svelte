<script lang="ts">
	import { onMount } from 'svelte';
	import { Database, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, copyText, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		WORKLOADS,
		actionEnabled,
		createBackup,
		createDatabase,
		databaseAction,
		deleteDatabase,
		downloadWallet,
		getDatabase,
		getEngine,
		isBusy,
		listBackups,
		listCompartments,
		listDatabases,
		parseAcl,
		readStoredDbCompartment,
		saveBlob,
		updateDatabase,
		validAclEntry,
		validateNew,
		writeStoredDbCompartment,
		type DbAction,
		type DbBackup,
		type DbDetail,
		type DbSummary
	} from '#lib/ocidatabase.ts';

	const TABS = [
		{ id: 'connections', label: 'Connections' },
		{ id: 'access', label: 'Access and compute' },
		{ id: 'backups', label: 'Backups' },
		{ id: 'wallet', label: 'Wallet' }
	];
	const POLL_MS = 3000;

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredDbCompartment());
	let engine = $state('');
	let loading = $state(true);
	let error = $state('');
	let databases = $state<DbSummary[]>([]);
	let selected = $state('');
	let detail = $state<DbDetail>();
	let backups = $state<DbBackup[]>([]);
	let detailError = $state('');
	let tab = $state('connections');
	let nameDraft = $state('');
	let aclDraft = $state('');
	let aclOn = $state(false);
	let computeDraft = $state(2);
	let backupName = $state('');
	let walletPassword = $state('');

	let showCreate = $state(false);
	let fName = $state('');
	let fDbName = $state('');
	let fWorkload = $state('OLTP');
	let fFree = $state(false);
	let fCompute = $state(2);
	let fStorage = $state(1);
	let fMtls = $state(true);
	let fPassword = $state('');

	const db = $derived(databases.find((d) => d.id === selected));
	const form = $derived({
		compartmentId: compartment,
		displayName: fName,
		dbName: fDbName,
		workload: fWorkload,
		computeCount: fCompute,
		storageTBs: fStorage,
		freeTier: fFree,
		mtlsRequired: fMtls,
		adminPassword: fPassword
	});
	const invalid = $derived(validateNew(form));
	const aclEntries = $derived(parseAcl(aclDraft));
	const aclInvalid = $derived(aclEntries.find((e) => !validAclEntry(e)));
	const busy = $derived(databases.some((d) => isBusy(d.state)));

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

	async function refresh() {
		if (!compartment) {
			databases = [];
			return;
		}
		try {
			databases = await listDatabases(compartment);
			error = '';
			if (!databases.some((d) => d.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		}
		await loadDetail();
	}

	async function loadDetail() {
		detailError = '';
		if (!db) {
			detail = undefined;
			backups = [];
			return;
		}
		try {
			[detail, backups] = await Promise.all([getDatabase(db.id), listBackups(compartment, db.id)]);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredDbCompartment(compartment);
		selected = '';
		loading = true;
		void refresh().finally(() => (loading = false));
	}

	function select(d: DbSummary) {
		selected = selected === d.id ? '' : d.id;
		nameDraft = selected ? d.name : '';
		walletPassword = '';
		detail = undefined;
		void loadDetail().then(() => {
			if (detail) {
				aclDraft = detail.acl.join('\n');
				aclOn = detail.aclEnabled;
				computeDraft = detail.computeCount;
			}
		});
	}

	async function act(action: DbAction, d: DbSummary) {
		const ok = await attempt(() => databaseAction(d.id, action), `${d.name}: ${action} requested`, `Failed to ${action} ${d.name}`);
		if (ok) await refresh();
	}

	async function remove(d: DbSummary) {
		const ok = await confirmedAttempt(
			{ title: 'Delete Autonomous Database', message: `Delete database "${d.name}" and its backups?`, confirmLabel: 'Delete' },
			() => deleteDatabase(d.id),
			`Deleting ${d.name}`,
			`Failed to delete ${d.name}`
		);
		if (ok) await refresh();
	}

	async function rename(d: DbSummary) {
		const ok = await attempt(() => updateDatabase(d.id, { displayName: nameDraft }), `Renamed to ${nameDraft.trim()}`, 'Failed to rename');
		if (ok) await refresh();
	}

	async function saveAccess(d: DbSummary) {
		if (aclInvalid !== undefined) return;
		const ok = await attempt(() => updateDatabase(d.id, { acl: aclEntries, aclEnabled: aclOn }), 'Access control updated', 'Failed to update access control');
		if (ok) await refresh();
	}

	async function saveCompute(d: DbSummary) {
		const ok = await attempt(() => updateDatabase(d.id, { computeCount: computeDraft }), `Compute set to ${computeDraft} ECPUs`, 'Failed to update compute');
		if (ok) await refresh();
	}

	async function backupNow(d: DbSummary) {
		const ok = await attempt(() => createBackup(d.id, backupName), 'Backup started', 'Failed to create backup');
		if (ok) {
			backupName = '';
			await refresh();
		}
	}

	async function wallet(d: DbSummary) {
		const password = walletPassword;
		const ok = await attempt(async () => saveBlob(await downloadWallet(d.id, password)), 'Wallet downloaded', 'Failed to download wallet');
		if (ok) walletPassword = '';
	}

	async function create() {
		if (invalid) return;
		const ok = await attempt(() => createDatabase(form), `Database ${fDbName.trim()} is provisioning`, 'Failed to create database');
		fPassword = '';
		if (ok) {
			fName = fDbName = '';
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
		s === 'AVAILABLE' || s === 'ACTIVE'
			? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300'
			: s === 'FAILED' || s === 'UNAVAILABLE'
				? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300'
				: isBusy(s) || s === 'CREATING'
					? 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300'
					: 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300';
	const compute = (d: DbSummary) => (d.freeTier ? 'Always Free' : `${d.computeCount} ${d.computeModel || 'ECPU'}`);

	onMount(() => {
		void load();
		const timer = setInterval(() => {
			if (!loading && busy) void refresh();
		}, POLL_MS);
		return () => clearInterval(timer);
	});
</script>

<div class="space-y-6 pb-20" data-testid="db-dashboard">
	<PageHeader icon={Database} title="Autonomous Database" description="Oracle Cloud Autonomous Databases, connection strings, backups and wallets" color="orange" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="db-error">
			<p class="font-medium">Could not load Autonomous Databases</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3">
			<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				Compartment
				<input class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" list="db-compartments" bind:value={compartment} onchange={pickCompartment} data-testid="db-compartment" />
				<datalist id="db-compartments">{#each compartments as c (c)}<option value={c}></option>{/each}</datalist>
			</label>
			<span class="rounded-full px-2 py-0.5 text-xs {engine === 'docker' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300'}" title={engine === 'docker' ? 'Databases run in a container' : 'State only: the database lifecycle is modelled but nothing accepts SQL connections'} data-testid="db-engine">Engine: {engine || 'unknown'}</span>
			<button class={GCP_MINI} onclick={() => (showCreate = !showCreate)} aria-expanded={showCreate} data-testid="db-create-toggle">Create Autonomous Database</button>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="db-list-error">{error}</p>{/if}

		{#if showCreate}
			<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-3" autocomplete="off" onsubmit={(e) => { e.preventDefault(); void create(); }} data-testid="db-create-form">
				<label>Display name (optional) <input class="{GCP_INPUT} !py-1" type="text" bind:value={fName} data-testid="db-create-display" /></label>
				<label>Database name <input class="{GCP_INPUT} !py-1 font-mono text-xs" type="text" placeholder="MYADB" bind:value={fDbName} data-testid="db-create-dbname" /></label>
				<label>Workload
					<select class="{GCP_INPUT} !py-1" bind:value={fWorkload} data-testid="db-create-workload">
						{#each WORKLOADS as w (w)}<option value={w}>{w}</option>{/each}
					</select>
				</label>
				<label class="flex items-center gap-2"><input type="checkbox" bind:checked={fFree} data-testid="db-create-free" /> Always Free</label>
				<label>ECPUs <input class="{GCP_INPUT} !py-1" type="number" min="2" step="1" bind:value={fCompute} disabled={fFree} data-testid="db-create-compute" /></label>
				<label>Storage (TB) <input class="{GCP_INPUT} !py-1" type="number" min="1" step="1" bind:value={fStorage} disabled={fFree} data-testid="db-create-storage" /></label>
				<label class="flex items-center gap-2"><input type="checkbox" bind:checked={fMtls} data-testid="db-create-mtls" /> Require mutual TLS</label>
				<label class="md:col-span-2">ADMIN password (12 to 30 characters; not shown again)
					<input class="{GCP_INPUT} !py-1" type="password" autocomplete="new-password" bind:value={fPassword} data-testid="db-create-password" />
				</label>
				<div class="flex items-center gap-3 md:col-span-3">
					<button type="submit" class={GCP_PRIMARY} disabled={invalid !== ''} data-testid="db-create-submit">Create</button>
					{#if invalid && (fDbName || fPassword)}<span class="text-xs text-gray-500" data-testid="db-create-hint">{invalid}</span>{/if}
				</div>
			</form>
		{/if}

		<div class={box}>
			<table class="w-full text-sm" data-testid="db-databases">
				<thead class={thead}><tr><th class={cell}>Display name</th><th class={cell}>DB name</th><th class={cell}>Workload</th><th class={cell}>State</th><th class={cell}>Compute</th><th class={cell}>Storage</th><th class={cell}>Free tier</th><th class={cell}>mTLS required</th></tr></thead>
				<tbody class={body}>
					{#each databases as d (d.id)}
						<tr class={selected === d.id ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="db-row">
							<td class="{cell} text-xs" title={d.id}><button class="underline decoration-dotted" onclick={() => select(d)} aria-expanded={selected === d.id} data-testid="db-select">{d.name}</button></td>
							<td class="{cell} font-mono text-xs" data-testid="db-dbname">{d.dbName}</td>
							<td class="{cell} text-xs" data-testid="db-workload">{d.workload}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {tone(d.state)}" data-testid="db-state">{d.state}</span></td>
							<td class="{cell} text-xs" data-testid="db-compute">{compute(d)}</td>
							<td class="{cell} text-xs" data-testid="db-storage">{d.storageTBs} TB</td>
							<td class="{cell} text-xs" data-testid="db-free">{d.freeTier ? 'yes' : 'no'}</td>
							<td class="{cell} text-xs" data-testid="db-mtls">{d.mtlsRequired ? 'yes' : 'no'}</td>
						</tr>
					{:else}
						<tr><td colspan="8" class="{cell} text-center text-gray-500" data-testid="db-empty">No Autonomous Databases in this compartment. Create one above.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if db && detail}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="db-detail">
				<div class="flex flex-wrap items-center justify-between gap-3">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.name} <span class="rounded-full px-2 py-0.5 text-xs {tone(detail.state)}" data-testid="db-detail-state">{detail.state}</span></h2>
					<div class="flex flex-wrap gap-2">
						{#each ['start', 'stop', 'restart'] as const as a (a)}
							<button class={GCP_MINI} disabled={!actionEnabled(a, detail.state)} onclick={() => act(a, db)} data-testid="db-action-{a}">{a[0].toUpperCase() + a.slice(1)}</button>
						{/each}
						<button class={GCP_MINI} disabled={isBusy(detail.state)} onclick={() => remove(db)} data-testid="db-action-delete">Delete</button>
					</div>
				</div>
				{#if detail.details}<p class="text-xs text-amber-700 dark:text-amber-300" data-testid="db-detail-details">{detail.details}</p>{/if}
				{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="db-detail-error">{detailError}</p>{/if}
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
					<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{detail.id}</dd>
					<dt class="text-gray-500">Version</dt><dd>{detail.version || '—'}</dd>
					<dt class="text-gray-500">Backup retention</dt><dd>{detail.backupRetentionDays} days</dd>
					<dt class="text-gray-500">Private endpoint</dt><dd class="font-mono">{detail.privateEndpoint || 'none (public)'}</dd>
					<dt class="text-gray-500">Created</dt><dd>{when(detail.created)}</dd>
				</dl>
				<form class="flex items-center gap-2 text-sm" onsubmit={(e) => { e.preventDefault(); void rename(db); }}>
					<input class="{GCP_INPUT} !w-72 !py-1" type="text" bind:value={nameDraft} aria-label="Display name" data-testid="db-rename-input" />
					<button type="submit" class={GCP_MINI} disabled={nameDraft.trim() === '' || nameDraft.trim() === detail.name || isBusy(detail.state)} data-testid="db-rename-submit">Update name</button>
				</form>

				<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="orange" />

				{#if tab === 'connections'}
					<div class="space-y-4">
						{#if detail.profiles.length === 0}
							<p class="text-sm text-gray-500" data-testid="db-profiles-empty">No connection strings yet. They appear once the database is available.</p>
						{:else}
							<div class={box}>
								<table class="w-full text-sm" data-testid="db-profiles">
									<thead class={thead}><tr><th class={cell}>Profile</th><th class={cell}>Group</th><th class={cell}>Protocol</th><th class={cell}>TLS</th><th class={cell}>Connection string</th><th class={cell}></th></tr></thead>
									<tbody class={body}>
										{#each detail.profiles as p, i (i)}
											<tr data-testid="db-profile-row">
												<td class="{cell} text-xs">{p.name}</td>
												<td class="{cell} text-xs">{p.consumerGroup}</td>
												<td class="{cell} text-xs">{p.protocol}</td>
												<td class="{cell} text-xs">{p.tlsAuthentication}</td>
												<td class="{cell} font-mono text-xs break-all" data-testid="db-profile-value">{p.value}</td>
												<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => copyText(p.value)}>Copy</button></td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
						<div>
							<h3 class="mb-1 font-medium text-gray-900 dark:text-white">Connection URLs</h3>
							<ul class="text-xs" data-testid="db-urls">
								{#each Object.entries(detail.connectionUrls) as [k, v] (k)}<li><span class="text-gray-500">{k}</span> <span class="font-mono break-all">{v}</span></li>{:else}<li class="text-gray-500">None</li>{/each}
							</ul>
						</div>
					</div>
				{:else if tab === 'access'}
					<div class="grid gap-6 md:grid-cols-2">
						<form class="space-y-2 text-sm" onsubmit={(e) => { e.preventDefault(); void saveAccess(db); }} data-testid="db-acl-form">
							<h3 class="font-medium text-gray-900 dark:text-white">Access control list</h3>
							<label class="flex items-center gap-2"><input type="checkbox" bind:checked={aclOn} data-testid="db-acl-enabled" /> Restrict access to the entries below</label>
							<textarea class="{GCP_INPUT} font-mono text-xs" rows="4" placeholder="203.0.113.0/24" bind:value={aclDraft} data-testid="db-acl-input"></textarea>
							{#if aclInvalid !== undefined}<p class="text-xs text-red-600" data-testid="db-acl-hint">Invalid entry: {aclInvalid}</p>{/if}
							<button type="submit" class={GCP_MINI} disabled={aclInvalid !== undefined || isBusy(detail.state)} data-testid="db-acl-save">Save access control</button>
						</form>
						<form class="space-y-2 text-sm" onsubmit={(e) => { e.preventDefault(); void saveCompute(db); }} data-testid="db-compute-form">
							<h3 class="font-medium text-gray-900 dark:text-white">Compute</h3>
							<label>ECPUs <input class="{GCP_INPUT} !w-32 !py-1" type="number" min="2" step="1" bind:value={computeDraft} disabled={detail.freeTier} data-testid="db-compute-input" /></label>
							{#if detail.freeTier}<p class="text-xs text-gray-500">Always Free databases have a fixed shape.</p>{/if}
							<button type="submit" class={GCP_MINI} disabled={detail.freeTier || computeDraft < 2 || computeDraft === detail.computeCount || isBusy(detail.state)} data-testid="db-compute-save">Scale</button>
						</form>
					</div>
				{:else if tab === 'backups'}
					<div class="space-y-3">
						<form class="flex flex-wrap items-center gap-2 text-sm" onsubmit={(e) => { e.preventDefault(); void backupNow(db); }}>
							<input class="{GCP_INPUT} !w-72 !py-1" type="text" placeholder="Backup name (optional)" bind:value={backupName} aria-label="Backup name" data-testid="db-backup-name" />
							<button type="submit" class={GCP_MINI} disabled={detail.state !== 'AVAILABLE'} data-testid="db-backup-create">Create manual backup</button>
						</form>
						<div class={box}>
							<table class="w-full text-sm" data-testid="db-backups">
								<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Type</th><th class={cell}>State</th><th class={cell}>Started</th><th class={cell}>Available until</th></tr></thead>
								<tbody class={body}>
									{#each backups as b (b.id)}
										<tr data-testid="db-backup-row">
											<td class="{cell} text-xs">{b.name}{b.automatic ? ' (automatic)' : ''}</td>
											<td class="{cell} text-xs">{b.type}</td>
											<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {tone(b.state)}" data-testid="db-backup-state">{b.state}</span></td>
											<td class="{cell} text-xs">{when(b.started)}</td>
											<td class="{cell} text-xs">{when(b.availableTill)}</td>
										</tr>
									{:else}
										<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="db-backups-empty">No backups</td></tr>
									{/each}
								</tbody>
							</table>
						</div>
					</div>
				{:else}
					<form class="space-y-2 text-sm" autocomplete="off" onsubmit={(e) => { e.preventDefault(); void wallet(db); }} data-testid="db-wallet-form">
						<p class="text-xs text-gray-500">The wallet zip holds the client certificate for mutual TLS, encrypted with the password you choose here. The password is not stored or shown again.</p>
						<label class="block">Wallet password (8+ characters with a letter and a digit or symbol)
							<input class="{GCP_INPUT} !w-72 !py-1" type="password" autocomplete="new-password" bind:value={walletPassword} data-testid="db-wallet-password" />
						</label>
						<button type="submit" class={GCP_PRIMARY} disabled={walletPassword.length < 8 || !['AVAILABLE', 'STOPPED'].includes(detail.state)} data-testid="db-wallet-download">Download wallet</button>
					</form>
				{/if}
			</div>
		{/if}
	{/if}
</div>
