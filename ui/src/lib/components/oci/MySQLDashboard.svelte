<script lang="ts">
	import { onMount } from 'svelte';
	import { Database, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		allowedActions,
		createBackup,
		deleteSystem,
		getSystem,
		listBackups,
		listCompartments,
		listConfigurations,
		listSystems,
		mysqlEndpoint,
		readStoredMysqlCompartment,
		systemAction,
		writeStoredMysqlCompartment,
		type MysqlBackup,
		type MysqlConfiguration,
		type MysqlSystem,
		type SystemAction
	} from '#lib/ocimysql.ts';
	import { shortOcid } from '#lib/ocirest.ts';

	const TABS = [
		{ id: 'backups', label: 'Backups' },
		{ id: 'configurations', label: 'Configurations' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredMysqlCompartment());
	let systems = $state<MysqlSystem[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let detail = $state<MysqlSystem | null>(null);
	let tab = $state('backups');
	let backups = $state<MysqlBackup[]>([]);
	let configurations = $state<MysqlConfiguration[]>([]);
	let backupName = $state('');
	let detailError = $state('');

	async function loadSystems() {
		if (!compartment) {
			systems = [];
			return;
		}
		try {
			systems = await listSystems(compartment);
			error = '';
			if (!systems.some((s) => s.id === selected)) selected = '';
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
		await loadSystems();
		loading = false;
	}

	async function loadDetail() {
		detailError = '';
		try {
			[backups, configurations] = await Promise.all([listBackups(compartment, selected), listConfigurations(compartment)]);
			detail = selected ? await getSystem(selected) : null;
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredMysqlCompartment(compartment);
		selected = '';
		loading = true;
		void loadSystems();
	}

	function select(s: MysqlSystem) {
		selected = selected === s.id ? '' : s.id;
		detail = null;
		void loadDetail();
	}

	async function act(s: MysqlSystem, action: SystemAction) {
		const ok = await attempt(() => systemAction(s.id, action), `${action} requested for ${s.name}`, `Failed to ${action} ${s.name}`);
		if (ok) await loadSystems();
	}

	async function remove(s: MysqlSystem) {
		const ok = await confirmedAttempt({ title: 'Delete DB system', message: `Delete DB system "${s.name}"?`, confirmLabel: 'Delete' }, () => deleteSystem(s.id), `Deletion started for ${s.name}`, `Failed to delete ${s.name}`);
		if (ok) await loadSystems();
	}

	async function backup() {
		const ok = await attempt(() => createBackup(selected, backupName), 'Backup requested', 'Failed to create backup');
		if (ok) {
			backupName = '';
			await loadDetail();
		}
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'ACTIVE' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'FAILED' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : s === 'INACTIVE' ? 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="mysql-dashboard">
	<PageHeader icon={Database} title="MySQL HeatWave" description="Oracle Cloud MySQL DB systems, backups and configurations" color="red" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="mysql-error">
			<p class="font-medium">Could not load DB systems</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if compartments.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="mysql-empty">No DB systems yet. Create one with the OCI SDK or CLI.</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Compartment
			<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="mysql-compartment">
				{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
			</select>
		</label>
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="mysql-list-error">{error}</p>{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="mysql-systems">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>DB system</th><th class={cell}>State</th><th class={cell}>Version</th><th class={cell}>Shape</th><th class={cell}>Endpoint</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each systems as s (s.id)}
						{@const a = allowedActions(s)}
						<tr class={selected === s.id ? 'bg-red-50/60 dark:bg-red-950/20' : ''} data-testid="mysql-system">
							<td class="{cell} font-mono text-xs" title={s.id}><button class="underline decoration-dotted" onclick={() => select(s)} aria-expanded={selected === s.id} data-testid="mysql-select">{s.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(s.state)}" data-testid="mysql-state" title={s.details}>{s.state || '—'}</span></td>
							<td class="{cell} font-mono text-xs" data-testid="mysql-version">{s.version || '—'}</td>
							<td class="{cell} font-mono text-xs" data-testid="mysql-shape">{s.shape || '—'}</td>
							<td class="{cell} font-mono text-xs" data-testid="mysql-endpoint">{mysqlEndpoint(s) || '—'}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								<button class={GCP_MINI} onclick={() => act(s, 'start')} disabled={!a.start} data-testid="mysql-start">Start</button>
								<button class={GCP_MINI} onclick={() => act(s, 'stop')} disabled={!a.stop} data-testid="mysql-stop">Stop</button>
								<button class={GCP_MINI} onclick={() => act(s, 'restart')} disabled={!a.restart} data-testid="mysql-restart">Restart</button>
								<button class={GCP_MINI} onclick={() => remove(s)} disabled={!a.remove} title={s.deleteProtected ? 'Delete protection is on' : ''} data-testid="mysql-delete">Delete</button>
							</td>
						</tr>
					{:else}
						<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="mysql-none">No DB systems in this compartment</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="mysql-detail-error">{detailError}</p>{/if}

		{#if detail}
			<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="mysql-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.name}</h2>
				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs" data-testid="mysql-config">
					<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{detail.id}</dd>
					<dt class="text-gray-500">MySQL version</dt><dd>{detail.version}</dd>
					<dt class="text-gray-500">Shape</dt><dd>{detail.shape}</dd>
					<dt class="text-gray-500">Endpoint</dt><dd class="font-mono" data-testid="mysql-detail-endpoint">{mysqlEndpoint(detail) || '—'}{detail.portX ? ` (X protocol ${detail.portX})` : ''}</dd>
					<dt class="text-gray-500">Storage</dt><dd data-testid="mysql-storage">{detail.storageGb} GB{detail.autoExpand ? `, auto-expand up to ${detail.maxStorageGb} GB` : ''}</dd>
					<dt class="text-gray-500">High availability</dt><dd data-testid="mysql-ha">{detail.highlyAvailable ? 'Yes' : 'No'}</dd>
					<dt class="text-gray-500">Backup policy</dt><dd data-testid="mysql-backup-policy">{detail.backupEnabled ? `Enabled, ${detail.backupRetentionDays} days retention, window ${detail.backupWindow}${detail.pitr ? ', point-in-time recovery' : ''}` : 'Disabled'}</dd>
					<dt class="text-gray-500">Delete protection</dt><dd data-testid="mysql-protection">{detail.deleteProtected ? 'On' : 'Off'}{detail.finalBackup ? ` (final backup: ${detail.finalBackup})` : ''}</dd>
					<dt class="text-gray-500">Placement</dt><dd>{detail.availabilityDomain || '—'} / {detail.faultDomain || '—'}</dd>
					<dt class="text-gray-500">Mode</dt><dd>{detail.databaseMode} / {detail.accessMode}</dd>
					<dt class="text-gray-500">Configuration</dt><dd class="font-mono" title={detail.configurationId}>{shortOcid(detail.configurationId)}</dd>
					<dt class="text-gray-500">Admin password</dt><dd class="text-gray-500">Never shown. It is set when the DB system is created.</dd>
				</dl>
			</div>
		{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

		{#if tab === 'backups'}
			{#if selected}
				<div class="flex items-center gap-2">
					<input class="{GCP_INPUT} !w-64 !py-1 text-xs" type="text" placeholder="Backup name (optional)" bind:value={backupName} aria-label="Backup name" data-testid="mysql-backup-name" />
					<button class={GCP_MINI} onclick={backup} data-testid="mysql-backup-create">Create backup</button>
				</div>
			{:else}
				<p class="text-xs text-gray-500">Select a DB system to create a backup. Showing all backups in the compartment.</p>
			{/if}
			{#if backups.length === 0}
				<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="mysql-backups-empty">No backups</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="mysql-backups">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Backup</th><th class={cell}>State</th><th class={cell}>Type</th><th class={cell}>Creation</th><th class={cell}>Size</th><th class={cell}>Retention</th><th class={cell}>Created</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each backups as b (b.id)}
								<tr data-testid="mysql-backup">
									<td class="{cell} font-mono text-xs" title={b.id}>{b.name}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(b.state)}" data-testid="mysql-backup-state">{b.state}</span></td>
									<td class="{cell} text-xs">{b.type}</td>
									<td class="{cell} text-xs">{b.creation}</td>
									<td class="{cell} text-xs">{b.sizeGb} GB</td>
									<td class="{cell} text-xs">{b.retentionDays} days</td>
									<td class="{cell} text-xs">{b.created}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if configurations.length === 0}
			<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="mysql-configs-empty">No configurations</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="mysql-configs">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Configuration</th><th class={cell}>Type</th><th class={cell}>Shape</th><th class={cell}>State</th><th class={cell}>Description</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each configurations as c (c.id)}
							<tr data-testid="mysql-config-row">
								<td class="{cell} font-mono text-xs" title={c.id}>{c.name}</td>
								<td class="{cell} text-xs">{c.type}</td>
								<td class="{cell} font-mono text-xs">{c.shape}</td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(c.state)}">{c.state}</span></td>
								<td class="{cell} text-xs">{c.description || '—'}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
