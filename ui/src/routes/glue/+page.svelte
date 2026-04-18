<script lang="ts">
	import { onMount } from 'svelte';
	import { getGlueClient } from '$lib/aws-client';
	import {
		GetDatabasesCommand,
		GetTablesCommand,
		GetJobsCommand,
		GetJobRunsCommand,
		StartJobRunCommand,
		BatchStopJobRunCommand,
		GetCrawlersCommand,
		StartCrawlerCommand,
		StopCrawlerCommand,
		GetConnectionsCommand,
		type Database,
		type Table,
		type Job,
		type JobRun,
		type Crawler,
		type Connection
	} from '@aws-sdk/client-glue';
	import { toast } from 'svelte-sonner';
	import { Database as DBIcon, Search, RefreshCw, Play, XCircle, ChevronRight, Table as TableIcon, Settings, Globe } from 'lucide-svelte';

	const glue = getGlueClient();

	let activeTab = $state<'catalog' | 'jobs' | 'crawlers' | 'connections'>('catalog');
	let searchQuery = $state('');
	let loading = $state(false);

	// Catalog
	let databases = $state<Database[]>([]);
	let selectedDatabase = $state<Database | null>(null);
	let tables = $state<Table[]>([]);
	let loadingTables = $state(false);

	// Jobs
	let jobs = $state<Job[]>([]);
	let loadingJobs = $state(false);
	let selectedJob = $state<Job | null>(null);
	let jobRuns = $state<JobRun[]>([]);
	let loadingRuns = $state(false);

	// Crawlers
	let crawlers = $state<Crawler[]>([]);
	let loadingCrawlers = $state(false);

	// Connections
	let connections = $state<Connection[]>([]);
	let loadingConnections = $state(false);

	const filteredDatabases = $derived(
		databases.filter((d) => !searchQuery || (d.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredTables = $derived(
		tables.filter((t) => !searchQuery || (t.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredJobs = $derived(
		jobs.filter((j) => !searchQuery || (j.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredCrawlers = $derived(
		crawlers.filter((c) => !searchQuery || (c.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const jobRunStatus = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status === 'SUCCEEDED') return 'green';
		if (status === 'FAILED' || status === 'ERROR') return 'red';
		if (status === 'RUNNING') return 'blue';
		if (status === 'STARTING') return 'yellow';
		return 'gray';
	};

	const crawlerStatus = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status === 'READY') return 'green';
		if (status === 'RUNNING') return 'blue';
		if (status === 'STOPPING') return 'yellow';
		return 'gray';
	};

	async function loadDatabases() {
		loading = true;
		try {
			const resp = await glue.send(new GetDatabasesCommand({ MaxResults: 50 }));
			databases = resp.DatabaseList ?? [];
		} catch (e) {
			toast.error('Failed to load databases: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectDatabase(db: Database) {
		selectedDatabase = db;
		searchQuery = '';
		loadingTables = true;
		try {
			const resp = await glue.send(new GetTablesCommand({ DatabaseName: db.Name ?? '', MaxResults: 100 }));
			tables = resp.TableList ?? [];
		} catch (e) {
			toast.error('Failed to load tables: ' + String(e));
		} finally {
			loadingTables = false;
		}
	}

	async function loadJobs() {
		loadingJobs = true;
		try {
			const resp = await glue.send(new GetJobsCommand({ MaxResults: 50 }));
			jobs = resp.Jobs ?? [];
		} catch (e) {
			toast.error('Failed to load jobs: ' + String(e));
		} finally {
			loadingJobs = false;
		}
	}

	async function selectJob(job: Job) {
		selectedJob = job;
		loadingRuns = true;
		try {
			const resp = await glue.send(new GetJobRunsCommand({ JobName: job.Name ?? '', MaxResults: 20 }));
			jobRuns = resp.JobRuns ?? [];
		} catch (e) {
			toast.error('Failed to load job runs: ' + String(e));
		} finally {
			loadingRuns = false;
		}
	}

	async function startJob(jobName: string) {
		try {
			const resp = await glue.send(new StartJobRunCommand({ JobName: jobName }));
			toast.success(`Job run started: ${resp.JobRunId}`);
			if (selectedJob?.Name === jobName) await selectJob(selectedJob);
		} catch (e) {
			toast.error('Failed to start job: ' + String(e));
		}
	}

	async function stopJobRun(jobName: string, runId: string) {
		try {
			await glue.send(new BatchStopJobRunCommand({ JobName: jobName, JobRunIds: [runId] }));
			toast.success('Job run stopping');
			if (selectedJob?.Name === jobName) await selectJob(selectedJob);
		} catch (e) {
			toast.error('Failed to stop job run: ' + String(e));
		}
	}

	async function loadCrawlers() {
		loadingCrawlers = true;
		try {
			const resp = await glue.send(new GetCrawlersCommand({ MaxResults: 50 }));
			crawlers = resp.Crawlers ?? [];
		} catch (e) {
			toast.error('Failed to load crawlers: ' + String(e));
		} finally {
			loadingCrawlers = false;
		}
	}

	async function startCrawler(name: string) {
		try {
			await glue.send(new StartCrawlerCommand({ Name: name }));
			toast.success(`Crawler "${name}" started`);
			await loadCrawlers();
		} catch (e) {
			toast.error('Failed to start crawler: ' + String(e));
		}
	}

	async function stopCrawler(name: string) {
		try {
			await glue.send(new StopCrawlerCommand({ Name: name }));
			toast.success(`Crawler "${name}" stopping`);
			await loadCrawlers();
		} catch (e) {
			toast.error('Failed to stop crawler: ' + String(e));
		}
	}

	async function loadConnections() {
		loadingConnections = true;
		try {
			const resp = await glue.send(new GetConnectionsCommand({ MaxResults: 50 }));
			connections = resp.ConnectionList ?? [];
		} catch (e) {
			toast.error('Failed to load connections: ' + String(e));
		} finally {
			loadingConnections = false;
		}
	}

	async function handleTabChange(tab: 'catalog' | 'jobs' | 'crawlers' | 'connections') {
		activeTab = tab;
		searchQuery = '';
		selectedDatabase = null;
		selectedJob = null;
		if (tab === 'jobs' && jobs.length === 0) await loadJobs();
		if (tab === 'crawlers' && crawlers.length === 0) await loadCrawlers();
		if (tab === 'connections' && connections.length === 0) await loadConnections();
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	function formatDuration(seconds: number | undefined): string {
		if (!seconds) return '-';
		const m = Math.floor(seconds / 60);
		const s = seconds % 60;
		return m > 0 ? `${m}m ${s}s` : `${s}s`;
	}

	onMount(loadDatabases);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Settings class="w-7 h-7 text-sky-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Glue</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">ETL, data catalog, and crawlers</p>
			</div>
		</div>
		<button
			onclick={() => {
				if (activeTab === 'catalog') loadDatabases();
				else if (activeTab === 'jobs') loadJobs();
				else if (activeTab === 'crawlers') loadCrawlers();
				else loadConnections();
			}}
			class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
		>
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Tabs -->
	<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
		{#each [['catalog', 'Data Catalog'], ['jobs', 'ETL Jobs'], ['crawlers', 'Crawlers'], ['connections', 'Connections']] as [tab, label]}
			<button
				onclick={() => handleTabChange(tab as 'catalog' | 'jobs' | 'crawlers' | 'connections')}
				class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-sky-500 text-sky-600 dark:text-sky-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
			>
				{label}
			</button>
		{/each}
	</div>

	<!-- Search -->
	{#if !selectedDatabase && !selectedJob}
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>
	{/if}

	<!-- CATALOG TAB -->
	{#if activeTab === 'catalog'}
		{#if selectedDatabase}
			<div class="flex items-center gap-2 text-sm mb-4">
				<button onclick={() => { selectedDatabase = null; tables = []; }} class="text-sky-600 hover:underline">Databases</button>
				<ChevronRight class="w-4 h-4 text-gray-400" />
				<span class="font-medium">{selectedDatabase.Name}</span>
			</div>

			{#if loadingTables}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-sky-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<div class="relative mb-4">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} type="text" placeholder="Search tables..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
				</div>
				{#if filteredTables.length === 0}
					<div class="text-center py-12 text-gray-500"><TableIcon class="w-10 h-10 mx-auto mb-2 opacity-40" /><p>No tables found</p></div>
				{:else}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Table Name</th>
									<th class="px-4 py-3 text-left">Type</th>
									<th class="px-4 py-3 text-left">Columns</th>
									<th class="px-4 py-3 text-left">Location</th>
									<th class="px-4 py-3 text-left">Created</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each filteredTables as tbl}
									<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
										<td class="px-4 py-3 font-medium text-sky-600 dark:text-sky-400">{tbl.Name}</td>
										<td class="px-4 py-3 text-xs text-gray-500">{tbl.TableType ?? '-'}</td>
										<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{(tbl.StorageDescriptor?.Columns ?? []).length}</td>
										<td class="px-4 py-3 font-mono text-xs text-gray-500 truncate max-w-xs">{tbl.StorageDescriptor?.Location ?? '-'}</td>
										<td class="px-4 py-3 text-xs text-gray-500">{formatDate(tbl.CreateTime)}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{/if}
		{:else}
			{#if loading}
				<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-sky-600 border-t-transparent rounded-full"></div></div>
			{:else if filteredDatabases.length === 0}
				<div class="text-center py-16 text-gray-500 dark:text-gray-400">
					<DBIcon class="w-12 h-12 mx-auto mb-3 opacity-40" />
					<p class="font-medium">No databases found</p>
					<p class="text-sm mt-1">Use the Glue Data Catalog to register databases and tables</p>
				</div>
			{:else}
				<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
					{#each filteredDatabases as db}
						<button
							onclick={() => selectDatabase(db)}
							class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5 text-left hover:border-sky-400 hover:shadow-md transition-all"
						>
							<div class="flex items-start gap-3">
								<DBIcon class="w-5 h-5 text-sky-500 mt-0.5" />
								<div class="min-w-0">
									<div class="font-semibold text-sm text-gray-900 dark:text-white truncate">{db.Name}</div>
									<div class="text-xs text-gray-500 mt-1 truncate">{db.Description ?? 'No description'}</div>
									<div class="text-xs text-gray-400 mt-2">{formatDate(db.CreateTime)}</div>
								</div>
							</div>
						</button>
					{/each}
				</div>
			{/if}
		{/if}
	{/if}

	<!-- JOBS TAB -->
	{#if activeTab === 'jobs'}
		{#if selectedJob}
			<div class="flex items-center justify-between mb-4">
				<div class="flex items-center gap-2 text-sm">
					<button onclick={() => { selectedJob = null; jobRuns = []; }} class="text-sky-600 hover:underline">Jobs</button>
					<ChevronRight class="w-4 h-4 text-gray-400" />
					<span class="font-medium">{selectedJob.Name}</span>
				</div>
				<button onclick={() => startJob(selectedJob?.Name ?? '')} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm font-medium">
					<Play class="w-4 h-4" /> Run Job
				</button>
			</div>

			<div class="grid grid-cols-1 md:grid-cols-3 gap-4 mb-4">
				{#each [
					{ label: 'Role', value: selectedJob.Role },
					{ label: 'Glue Version', value: selectedJob.GlueVersion ?? '-' },
					{ label: 'Max Capacity', value: String(selectedJob.MaxCapacity ?? '-') },
					{ label: 'Timeout', value: selectedJob.Timeout ? `${selectedJob.Timeout}min` : '-' },
					{ label: 'Max Retries', value: String(selectedJob.MaxRetries ?? 0) },
					{ label: 'Created', value: formatDate(selectedJob.CreatedOn) }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-3">
						<div class="text-xs text-gray-500">{row.label}</div>
						<div class="text-sm font-medium text-gray-900 dark:text-white mt-1 truncate">{row.value}</div>
					</div>
				{/each}
			</div>

			{#if loadingRuns}
				<div class="flex justify-center py-8"><div class="animate-spin w-8 h-8 border-4 border-sky-600 border-t-transparent rounded-full"></div></div>
			{:else}
				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Recent Runs</h3>
				{#if jobRuns.length === 0}
					<div class="text-center py-8 text-gray-500">No recent runs</div>
				{:else}
					<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
								<tr>
									<th class="px-4 py-3 text-left">Run ID</th>
									<th class="px-4 py-3 text-left">Status</th>
									<th class="px-4 py-3 text-left">Started</th>
									<th class="px-4 py-3 text-left">Duration</th>
									<th class="px-4 py-3 text-left">Actions</th>
								</tr>
							</thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each jobRuns as run}
									<tr>
										<td class="px-4 py-3 font-mono text-xs truncate max-w-xs">{run.Id}</td>
										<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium bg-${jobRunStatus(run.JobRunState)}-100 text-${jobRunStatus(run.JobRunState)}-700`}>{run.JobRunState}</span></td>
										<td class="px-4 py-3 text-xs text-gray-500">{formatDate(run.StartedOn)}</td>
										<td class="px-4 py-3 text-xs text-gray-500">{formatDuration(run.ExecutionTime)}</td>
										<td class="px-4 py-3">
											{#if run.JobRunState === 'RUNNING' || run.JobRunState === 'STARTING'}
												<button onclick={() => stopJobRun(selectedJob?.Name ?? '', run.Id ?? '')} class="text-red-500 hover:text-red-700 p-1">
													<XCircle class="w-4 h-4" />
												</button>
											{/if}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			{/if}
		{:else if loadingJobs}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-sky-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredJobs.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Play class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No ETL jobs found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Job Name</th>
							<th class="px-4 py-3 text-left">Type</th>
							<th class="px-4 py-3 text-left">Role</th>
							<th class="px-4 py-3 text-left">Created</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredJobs as job}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectJob(job)} class="text-sky-600 dark:text-sky-400 hover:underline font-medium">{job.Name}</button>
								</td>
								<td class="px-4 py-3 text-xs text-gray-500">{job.Command?.Name ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500 truncate max-w-xs">{job.Role ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(job.CreatedOn)}</td>
								<td class="px-4 py-3">
									<button onclick={() => startJob(job.Name ?? '')} class="text-sky-600 hover:text-sky-800 p-1" title="Run">
										<Play class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- CRAWLERS TAB -->
	{#if activeTab === 'crawlers'}
		{#if loadingCrawlers}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-sky-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredCrawlers.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Globe class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No crawlers found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Crawler Name</th>
							<th class="px-4 py-3 text-left">State</th>
							<th class="px-4 py-3 text-left">Database</th>
							<th class="px-4 py-3 text-left">Last Run</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredCrawlers as crawler}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3 font-medium">{crawler.Name}</td>
								<td class="px-4 py-3"><span class={`px-2 py-0.5 rounded text-xs font-medium bg-${crawlerStatus(crawler.State)}-100 text-${crawlerStatus(crawler.State)}-700`}>{crawler.State}</span></td>
								<td class="px-4 py-3 text-xs text-gray-500">{crawler.DatabaseName ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(crawler.LastUpdated)}</td>
								<td class="px-4 py-3 flex gap-1">
									{#if crawler.State === 'READY'}
										<button onclick={() => startCrawler(crawler.Name ?? '')} class="text-sky-600 hover:text-sky-800 p-1" title="Start"><Play class="w-4 h-4" /></button>
									{:else if crawler.State === 'RUNNING'}
										<button onclick={() => stopCrawler(crawler.Name ?? '')} class="text-red-500 hover:text-red-700 p-1" title="Stop"><XCircle class="w-4 h-4" /></button>
									{/if}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}

	<!-- CONNECTIONS TAB -->
	{#if activeTab === 'connections'}
		{#if loadingConnections}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-sky-600 border-t-transparent rounded-full"></div></div>
		{:else if connections.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Settings class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No connections found</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-xs text-gray-500 uppercase">
						<tr>
							<th class="px-4 py-3 text-left">Connection Name</th>
							<th class="px-4 py-3 text-left">Type</th>
							<th class="px-4 py-3 text-left">Creation Time</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each connections as conn}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50">
								<td class="px-4 py-3 font-medium">{conn.Name}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{conn.ConnectionType ?? '-'}</td>
								<td class="px-4 py-3 text-xs text-gray-500">{formatDate(conn.CreationTime)}</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
