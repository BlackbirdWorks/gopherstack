<script lang="ts">
	import { onMount } from 'svelte';
	import { Container, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import {
		appAction,
		appUrl,
		lastSegment,
		listApps,
		listEnvironments,
		listExecutions,
		listJobs,
		listRevisions,
		revisionAction,
		revisionActions,
		startJob,
		type AppAction,
		type ContainerApp,
		type ContainerEnv,
		type ContainerJob,
		type JobExecution,
		type Revision,
		type RevisionAction
	} from '#lib/azurecontainerapps.ts';
	import { attempt, confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'apps', label: 'Container apps' },
		{ id: 'environments', label: 'Environments' },
		{ id: 'jobs', label: 'Jobs' }
	];

	let subscription = $state('');
	let tab = $state('apps');
	let apps = $state<ContainerApp[]>([]);
	let envs = $state<ContainerEnv[]>([]);
	let jobs = $state<ContainerJob[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selectedApp = $state('');
	let revisions = $state<Revision[]>([]);
	let selectedJob = $state('');
	let executions = $state<JobExecution[]>([]);
	let childError = $state('');

	const currentApp = $derived(apps.find((a) => a.id === selectedApp));
	const currentJob = $derived(jobs.find((j) => j.id === selectedJob));

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			[apps, envs, jobs] = await Promise.all([listApps(subscription), listEnvironments(subscription), listJobs(subscription)]);
			error = '';
			if (!apps.some((a) => a.id === selectedApp)) selectedApp = '';
			if (!jobs.some((j) => j.id === selectedJob)) selectedJob = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadChildren();
	}

	async function loadChildren() {
		childError = '';
		try {
			if (currentApp) revisions = await listRevisions(subscription, currentApp);
			if (currentJob) executions = await listExecutions(subscription, currentJob);
		} catch (e) {
			childError = describeGcpError(e);
		}
	}

	function selectApp(a: ContainerApp) {
		selectedApp = selectedApp === a.id ? '' : a.id;
		revisions = [];
		if (selectedApp) void loadChildren();
	}

	function selectJob(j: ContainerJob) {
		selectedJob = selectedJob === j.id ? '' : j.id;
		executions = [];
		if (selectedJob) void loadChildren();
	}

	async function toggleApp(a: ContainerApp, action: AppAction) {
		const run = () => appAction(subscription, a, action);
		const ok =
			action === 'stop'
				? await confirmedAttempt({ title: 'Stop container app', message: `Stop "${a.name}"? Its replicas are removed until it is started again.`, confirmLabel: 'Stop' }, run, `Stop requested for ${a.name}`, `Failed to stop ${a.name}`)
				: await attempt(run, `Start requested for ${a.name}`, `Failed to start ${a.name}`);
		if (ok) await load();
	}

	async function actRevision(a: ContainerApp, r: Revision, action: RevisionAction) {
		const run = () => revisionAction(subscription, a, r.name, action);
		const ok =
			action === 'deactivate'
				? await confirmedAttempt({ title: 'Deactivate revision', message: `Deactivate revision "${r.name}"? Its replicas are removed and it stops receiving traffic.`, confirmLabel: 'Deactivate' }, run, `Deactivate requested for ${r.name}`, `Failed to deactivate ${r.name}`)
				: await attempt(run, `${action[0].toUpperCase()}${action.slice(1)} requested for ${r.name}`, `Failed to ${action} ${r.name}`);
		if (ok) await load();
	}

	async function runJob(j: ContainerJob) {
		const ok = await attempt(() => startJob(subscription, j), `Execution started for ${j.name}`, `Failed to start ${j.name}`);
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'Running' || s === 'Succeeded' || s === 'Provisioned' || s === 'Healthy' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'Failed' || s === 'Unhealthy' ? 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300';
	const actionLabel: Record<RevisionAction, string> = { restart: 'Restart', deactivate: 'Deactivate', activate: 'Activate' };

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="containerapps-dashboard">
	<PageHeader icon={Container} title="Container Apps" description="Azure Container Apps: environments, apps with revisions and traffic, and jobs" color="sky" onRefresh={load} />

	{#if error && apps.length === 0 && envs.length === 0 && jobs.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="containerapps-error">
			<p class="font-medium">Could not load Container Apps</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="sky" />
		{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="containerapps-child-error">{childError}</p>{/if}

		{#if tab === 'apps'}
			{#if apps.length === 0}
				<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="containerapps-empty">No container apps. Create one through Resource Manager (Microsoft.App/containerApps).</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="containerapps-apps">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>App</th><th class={cell}>Resource group</th><th class={cell}>State</th><th class={cell}>Environment</th><th class={cell}>Latest revision</th><th class={cell}>Replicas</th><th class={cell}>FQDN</th><th class="{cell} text-right">Actions</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each apps as a (a.id)}
								<tr class={selectedApp === a.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="containerapps-app">
									<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => selectApp(a)} aria-expanded={selectedApp === a.id} data-testid="containerapps-select">{a.name}</button></td>
									<td class="{cell} font-mono text-xs">{a.resourceGroup}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(a.runningStatus)}" data-testid="containerapps-state">{a.runningStatus || a.state || '—'}</span></td>
									<td class="{cell} font-mono text-xs">{lastSegment(a.environment) || '—'}</td>
									<td class="{cell} font-mono text-xs">{a.latestRevision || '—'}</td>
									<td class="{cell} text-xs">{a.minReplicas}–{a.maxReplicas}</td>
									<td class="{cell} font-mono text-xs break-all">
										{#if appUrl(a)}
											<a class="underline decoration-dotted" href={appUrl(a)} target="_blank" rel="noopener noreferrer" data-testid="containerapps-fqdn">{a.fqdn}</a>
										{:else}<span data-testid="containerapps-fqdn-internal">{a.fqdn || '—'}</span>{/if}
									</td>
									<td class="{cell} text-right whitespace-nowrap space-x-1">
										<button class={GCP_MINI} onclick={() => toggleApp(a, 'start')} data-testid="containerapps-start">Start</button>
										<button class={GCP_MINI} onclick={() => toggleApp(a, 'stop')} data-testid="containerapps-stop">Stop</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>

				{#if currentApp}
					<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="containerapps-detail">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{currentApp.name} revisions</h2>
						<p class="text-xs text-gray-500">Revision mode: {currentApp.revisionMode || 'Single'}{currentApp.images.length ? ` · image ${currentApp.images.join(', ')}` : ''}</p>
						{#if revisions.length === 0}
							<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="containerapps-revisions-empty">No revisions</p>
						{:else}
							<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
								<table class="w-full text-sm" data-testid="containerapps-revisions">
									<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Revision</th><th class={cell}>Active</th><th class={cell}>Running</th><th class={cell}>Health</th><th class={cell}>Replicas</th><th class={cell}>Traffic</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
									<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
										{#each revisions as r (r.name)}
											<tr data-testid="containerapps-revision">
												<td class="{cell} font-mono text-xs" title={r.error}>{r.name}</td>
												<td class="{cell} text-xs" data-testid="containerapps-revision-active">{r.active ? 'yes' : 'no'}</td>
												<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(r.running)}" data-testid="containerapps-revision-running">{r.running || '—'}</span></td>
												<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(r.health)}">{r.health || '—'}</span></td>
												<td class="{cell} text-xs" data-testid="containerapps-revision-replicas">{r.replicas}</td>
												<td class="{cell} text-xs" data-testid="containerapps-revision-traffic">{r.traffic}%</td>
												<td class="{cell} text-xs">{r.created}</td>
												<td class="{cell} text-right whitespace-nowrap space-x-1">
													{#each revisionActions(r) as act (act)}
														<button class={GCP_MINI} onclick={() => actRevision(currentApp, r, act)} data-testid="containerapps-revision-{act}">{actionLabel[act]}</button>
													{/each}
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</div>
				{/if}
			{/if}
		{:else if tab === 'environments'}
			{#if envs.length === 0}
				<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="containerapps-envs-empty">No managed environments</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="containerapps-envs">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Environment</th><th class={cell}>Resource group</th><th class={cell}>Location</th><th class={cell}>State</th><th class={cell}>Default domain</th><th class={cell}>Static IP</th><th class={cell}>Apps</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each envs as e (e.id)}
								<tr data-testid="containerapps-env">
									<td class="{cell} font-mono text-xs">{e.name}</td>
									<td class="{cell} font-mono text-xs">{e.resourceGroup}</td>
									<td class="{cell} text-xs">{e.location}</td>
									<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(e.state)}">{e.state || '—'}</span></td>
									<td class="{cell} font-mono text-xs break-all">{e.defaultDomain || '—'}</td>
									<td class="{cell} font-mono text-xs">{e.staticIp || '—'}</td>
									<td class="{cell} text-xs" data-testid="containerapps-env-apps">{apps.filter((a) => a.environment.toLowerCase() === e.id.toLowerCase()).length}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if jobs.length === 0}
			<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="containerapps-jobs-empty">No jobs</p>
		{:else}
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="containerapps-jobs">
					<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Job</th><th class={cell}>Resource group</th><th class={cell}>State</th><th class={cell}>Trigger</th><th class={cell}>Environment</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each jobs as j (j.id)}
							<tr class={selectedJob === j.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="containerapps-job">
								<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => selectJob(j)} aria-expanded={selectedJob === j.id} data-testid="containerapps-job-select">{j.name}</button></td>
								<td class="{cell} font-mono text-xs">{j.resourceGroup}</td>
								<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(j.state)}">{j.state || '—'}</span></td>
								<td class="{cell} text-xs">{j.trigger || '—'}</td>
								<td class="{cell} font-mono text-xs">{lastSegment(j.environment) || '—'}</td>
								<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => runJob(j)} data-testid="containerapps-job-start">Start</button></td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
			{#if currentJob}
				<div class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="containerapps-executions">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{currentJob.name} executions</h2>
					{#if executions.length === 0}
						<p class="text-sm text-gray-500" data-testid="containerapps-executions-empty">No executions</p>
					{:else}
						<table class="w-full text-xs">
							<thead class="text-left text-gray-500"><tr><th class="py-1 pr-4">Execution</th><th class="py-1 pr-4">Status</th><th class="py-1 pr-4">Started</th><th class="py-1">Finished</th></tr></thead>
							<tbody>
								{#each executions as x (x.name)}
									<tr data-testid="containerapps-execution"><td class="py-1 pr-4 font-mono">{x.name}</td><td class="py-1 pr-4"><span class="rounded-full px-2 py-0.5 {pill(x.status)}">{x.status}</span></td><td class="py-1 pr-4">{x.start}</td><td class="py-1">{x.end || '—'}</td></tr>
								{/each}
							</tbody>
						</table>
					{/if}
				</div>
			{/if}
		{/if}
	{/if}
</div>
