<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMWAAClient } from '$lib/aws-client';
	import {
		ListEnvironmentsCommand,
		GetEnvironmentCommand,
		CreateEnvironmentCommand,
		DeleteEnvironmentCommand,
		CreateWebLoginTokenCommand,
		CreateCliTokenCommand,
		type Environment
	} from '@aws-sdk/client-mwaa';
	import { toast } from 'svelte-sonner';
	import {
		Wind,
		Search,
		RefreshCw,
		CheckCircle,
		Clock,
		XCircle,
		Globe,
		ChevronRight,
		Activity,
		Plus,
		Trash2,
		BarChart2,
		ExternalLink,
		Terminal
	} from 'lucide-svelte';

	const mwaa = regionalClient(getMWAAClient);

	type Tab = 'environments' | 'metrics';

	let activeTab = $state<Tab>('environments');
	let loading = $state(false);
	let searchQuery = $state('');
	let environmentNames = $state<string[]>([]);
	let environments = $state<Environment[]>([]);
	let selectedEnv = $state<Environment | null>(null);
	let loadingDetail = $state(false);
	let loadedNames = $state(new Set<string>());

	// Metrics state
	type MetricDatum = {
		MetricName: string;
		Unit?: string;
		Value?: number;
		Timestamp?: number;
		Dimensions?: { Name: string; Value: string }[];
	};
	let metricsEnvName = $state('');
	let metricData = $state<MetricDatum[]>([]);
	let loadingMetrics = $state(false);

	// Create environment modal
	let showCreate = $state(false);
	let creating = $state(false);
	let newName = $state('');
	let newDagS3Path = $state('');
	let newExecutionRoleArn = $state('');
	let newSourceBucketArn = $state('');
	let newAirflowVersion = $state('2.10.3');
	let newEnvClass = $state('mw1.small');
	let newAccessMode = $state('PUBLIC_ONLY');
	let newMinWorkers = $state(1);
	let newMaxWorkers = $state(10);

	// Delete confirmation
	let deleteTarget = $state('');
	let deleting = $state(false);

	const filteredEnvironments = $derived(
		environments.filter(
			(e) =>
				(e.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(e.Status ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const availableCount = $derived(environments.filter((e) => e.Status === 'AVAILABLE').length);
	const creatingCount = $derived(environments.filter((e) => e.Status === 'CREATING').length);
	const webserverPublicCount = $derived(
		environments.filter((e) => e.WebserverAccessMode === 'PUBLIC_ONLY').length
	);

	// Group metrics by MetricName for display
	const groupedMetrics = $derived(() => {
		const groups: Record<string, MetricDatum[]> = {};
		for (const m of metricData) {
			if (!groups[m.MetricName]) groups[m.MetricName] = [];
			groups[m.MetricName].push(m);
		}
		return groups;
	});

	function statusColor(status: string | undefined): string {
		if (status === 'AVAILABLE') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'CREATING' || status === 'UPDATING')
			return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'DELETING' || status === 'UNAVAILABLE' || status === 'CREATE_FAILED')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	async function loadEnvironmentNames() {
		loading = true;
		try {
			const res = await mwaa().send(new ListEnvironmentsCommand({}));
			environmentNames = res.Environments ?? [];
			await loadEnvironmentDetails(environmentNames);
		} catch (err: unknown) {
			toast.error(`Failed to list environments: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadEnvironmentDetails(names: string[]) {
		const toLoad = names.filter((n) => !loadedNames.has(n));
		const results = await Promise.allSettled(
			toLoad.map((name) => mwaa().send(new GetEnvironmentCommand({ Name: name })))
		);
		const loaded: Environment[] = [];
		results.forEach((r, i) => {
			if (r.status === 'fulfilled' && r.value.Environment) {
				loaded.push(r.value.Environment);
				loadedNames.add(toLoad[i]);
			}
		});
		environments = [...environments.filter((e) => !toLoad.includes(e.Name ?? '')), ...loaded];
	}

	async function selectEnvironment(name: string) {
		loadingDetail = true;
		selectedEnv = null;
		try {
			const res = await mwaa().send(new GetEnvironmentCommand({ Name: name }));
			selectedEnv = res.Environment ?? null;
		} catch (err: unknown) {
			toast.error(`Failed to get environment: ${(err as Error).message}`);
		} finally {
			loadingDetail = false;
		}
	}

	// Airflow Web UI access + CLI token (CreateWebLoginToken / CreateCliToken).
	let tokenLoading = $state(false);
	let cliToken = $state<{ token?: string; hostname?: string } | null>(null);

	async function openAirflowUi(name: string) {
		tokenLoading = true;
		try {
			const res = await mwaa().send(new CreateWebLoginTokenCommand({ Name: name }));
			if (res.WebServerHostname && res.WebToken) {
				const url = `https://${res.WebServerHostname}/aws_mwaa/aws-console-sso?login=true#${res.WebToken}`;
				window.open(url, '_blank', 'noopener');
				toast.success('Opening Airflow web UI…');
			} else {
				toast.error('No web login token returned');
			}
		} catch (err: unknown) {
			toast.error(`Failed to create web login token: ${(err as Error).message}`);
		} finally {
			tokenLoading = false;
		}
	}

	async function getCliToken(name: string) {
		tokenLoading = true;
		cliToken = null;
		try {
			const res = await mwaa().send(new CreateCliTokenCommand({ Name: name }));
			cliToken = { token: res.CliToken, hostname: res.WebServerHostname };
			toast.success('CLI token generated');
		} catch (err: unknown) {
			toast.error(`Failed to create CLI token: ${(err as Error).message}`);
		} finally {
			tokenLoading = false;
		}
	}

	async function refresh() {
		environments = [];
		loadedNames = new Set();
		await loadEnvironmentNames();
	}

	function openCreateModal() {
		newName = '';
		newDagS3Path = '';
		newExecutionRoleArn = '';
		newSourceBucketArn = '';
		newAirflowVersion = '2.10.3';
		newEnvClass = 'mw1.small';
		newAccessMode = 'PUBLIC_ONLY';
		newMinWorkers = 1;
		newMaxWorkers = 10;
		showCreate = true;
	}

	async function createEnvironment() {
		if (!newName.trim()) { toast.error('Name is required'); return; }
		if (!newDagS3Path.trim()) { toast.error('DAGs S3 Path is required'); return; }
		if (!newExecutionRoleArn.trim()) { toast.error('Execution Role ARN is required'); return; }
		if (!newSourceBucketArn.trim()) { toast.error('Source Bucket ARN is required'); return; }
		creating = true;
		try {
			await mwaa().send(new CreateEnvironmentCommand({
				Name: newName.trim(),
				DagS3Path: newDagS3Path.trim(),
				ExecutionRoleArn: newExecutionRoleArn.trim(),
				SourceBucketArn: newSourceBucketArn.trim(),
				AirflowVersion: newAirflowVersion || undefined,
				EnvironmentClass: newEnvClass || undefined,
				WebserverAccessMode: newAccessMode as 'PUBLIC_ONLY' | 'PRIVATE_ONLY',
				MinWorkers: newMinWorkers,
				MaxWorkers: newMaxWorkers,
				NetworkConfiguration: {}
			}));
			toast.success(`Environment "${newName.trim()}" created`);
			showCreate = false;
			await refresh();
		} catch (err: unknown) {
			toast.error(`Failed to create environment: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	function confirmDelete(name: string) {
		deleteTarget = name;
	}

	async function deleteEnvironment() {
		if (!deleteTarget) return;
		deleting = true;
		try {
			await mwaa().send(new DeleteEnvironmentCommand({ Name: deleteTarget }));
			toast.success(`Environment "${deleteTarget}" deleted`);
			if (selectedEnv?.Name === deleteTarget) selectedEnv = null;
			deleteTarget = '';
			await refresh();
		} catch (err: unknown) {
			toast.error(`Failed to delete environment: ${(err as Error).message}`);
		} finally {
			deleting = false;
		}
	}

	async function loadMetrics(name: string) {
		metricsEnvName = name;
		loadingMetrics = true;
		metricData = [];
		activeTab = 'metrics';
		try {
			const res = await fetch(`/metrics/environments/${encodeURIComponent(name)}`, {
				headers: {
					'x-amz-target': 'GetMetrics',
					Authorization: 'AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/airflow/aws4_request'
				}
			});
			if (!res.ok) throw new Error(await res.text());
			const body = await res.json();
			metricData = body.MetricData ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load metrics: ${(err as Error).message}`);
		} finally {
			loadingMetrics = false;
		}
	}

	// Environments are cached by name in loadedNames; on a region change the
	// same env name in the new region must not be treated as already
	// loaded, so use refresh() (which clears environments/loadedNames)
	// rather than calling loadEnvironmentNames() directly.
	onRegionChange(() => {
		void refresh();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<Wind class="w-6 h-6 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Amazon MWAA</h1>
				<p class="text-slate-600 dark:text-slate-300">Managed Workflows for Apache Airflow</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => openCreateModal()}
				class="flex items-center gap-1.5 px-3 py-2 bg-teal-600 hover:bg-teal-700 text-white text-sm font-medium rounded-lg transition-colors"
			>
				<Plus class="w-4 h-4" />Create Environment
			</button>
			<button
				onclick={() => refresh()}
				class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<Wind class="w-5 h-5 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{environmentNames.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Total</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{availableCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Available</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-yellow-100 dark:bg-yellow-900/30 rounded-lg">
				<Clock class="w-5 h-5 text-yellow-600 dark:text-yellow-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{creatingCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Creating</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-sky-100 dark:bg-sky-900/30 rounded-lg">
				<Globe class="w-5 h-5 text-sky-600 dark:text-sky-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{webserverPublicCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Public Access</p>
			</div>
		</div>
	</div>

	<!-- Tab bar -->
	<div class="flex items-center gap-1 border-b border-slate-200 dark:border-slate-700">
		<button
			onclick={() => activeTab = 'environments'}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'environments' ? 'border-teal-500 text-teal-600 dark:text-teal-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
		>
			<span class="flex items-center gap-1.5">
				<Wind class="w-4 h-4" />Environments
			</span>
		</button>
		<button
			onclick={() => activeTab = 'metrics'}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'metrics' ? 'border-teal-500 text-teal-600 dark:text-teal-400' : 'border-transparent text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white'}"
		>
			<span class="flex items-center gap-1.5">
				<BarChart2 class="w-4 h-4" />Metrics
			</span>
		</button>
	</div>

	{#if activeTab === 'environments'}
		<!-- Search -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input
				type="text"
				bind:value={searchQuery}
				placeholder="Search environments..."
				class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-teal-500"
			/>
		</div>

		<!-- Content -->
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<!-- List -->
			<div class="lg:col-span-1 space-y-2">
				{#if loading && environments.length === 0}
					<div class="text-center py-12">
						<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-teal-500 mb-2"></div>
						<p class="text-slate-500 dark:text-slate-400">Loading environments...</p>
					</div>
				{:else if filteredEnvironments.length === 0 && environmentNames.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Wind class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No environments found</p>
						<button
							onclick={() => openCreateModal()}
							class="mt-3 text-sm text-teal-600 dark:text-teal-400 hover:underline"
						>Create your first environment</button>
					</div>
				{:else if filteredEnvironments.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Wind class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No matching environments</p>
					</div>
				{:else}
					{#each filteredEnvironments as env}
						<div
							role="button"
							tabindex="0"
							onclick={() => { if (env.Name) selectEnvironment(env.Name); }}
							onkeypress={(e) => { if (e.key === 'Enter' && env.Name) selectEnvironment(env.Name); }}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-teal-400 transition-colors cursor-pointer {selectedEnv?.Name === env.Name ? 'border-teal-500 ring-1 ring-teal-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{env.Name}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">Airflow {env.AirflowVersion ?? 'N/A'} · {env.EnvironmentClass ?? 'N/A'}</p>
								</div>
								<div class="flex items-center gap-1.5 ml-2 flex-shrink-0">
									<span class="px-2 py-0.5 text-xs rounded-full {statusColor(env.Status)}">{env.Status}</span>
									<button
										onclick={(e) => { e.stopPropagation(); if (env.Name) loadMetrics(env.Name); }}
										class="p-1 text-slate-400 hover:text-teal-600 dark:hover:text-teal-400 transition-colors"
										title="View metrics"
									>
										<BarChart2 class="w-3.5 h-3.5" />
									</button>
									<button
										onclick={(e) => { e.stopPropagation(); if (env.Name) confirmDelete(env.Name); }}
										class="p-1 text-slate-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
										title="Delete environment"
									>
										<Trash2 class="w-3.5 h-3.5" />
									</button>
									<ChevronRight class="w-4 h-4 text-slate-400" />
								</div>
							</div>
						</div>
					{/each}
				{/if}
			</div>

			<!-- Detail panel -->
			<div class="lg:col-span-2">
				{#if loadingDetail}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
						<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-teal-500 mb-2"></div>
						<p class="text-slate-500 dark:text-slate-400">Loading environment details...</p>
					</div>
				{:else if selectedEnv}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
						<div class="flex items-start justify-between">
							<div>
								<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedEnv.Name}</h2>
								<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {statusColor(selectedEnv.Status)}">
									{selectedEnv.Status}
								</span>
							</div>
							<div class="flex items-center gap-2">
								<button
									onclick={() => { if (selectedEnv?.Name) openAirflowUi(selectedEnv.Name); }}
									disabled={tokenLoading}
									class="flex items-center gap-1.5 px-3 py-1.5 text-sm text-teal-600 dark:text-teal-400 border border-teal-300 dark:border-teal-700 rounded-lg hover:bg-teal-50 dark:hover:bg-teal-900/20 transition-colors disabled:opacity-50"
								>
									<ExternalLink class="w-4 h-4" />Open Airflow UI
								</button>
								<button
									onclick={() => { if (selectedEnv?.Name) getCliToken(selectedEnv.Name); }}
									disabled={tokenLoading}
									class="flex items-center gap-1.5 px-3 py-1.5 text-sm text-slate-600 dark:text-slate-300 border border-slate-300 dark:border-slate-700 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors disabled:opacity-50"
								>
									<Terminal class="w-4 h-4" />CLI Token
								</button>
								<button
									onclick={() => { if (selectedEnv?.Name) loadMetrics(selectedEnv.Name); }}
									class="flex items-center gap-1.5 px-3 py-1.5 text-sm text-teal-600 dark:text-teal-400 border border-teal-300 dark:border-teal-700 rounded-lg hover:bg-teal-50 dark:hover:bg-teal-900/20 transition-colors"
								>
									<BarChart2 class="w-4 h-4" />Metrics
								</button>
								<button
									onclick={() => { if (selectedEnv?.Name) confirmDelete(selectedEnv.Name); }}
									class="flex items-center gap-1.5 px-3 py-1.5 text-sm text-red-600 dark:text-red-400 border border-red-200 dark:border-red-800 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/20 transition-colors"
								>
									<Trash2 class="w-4 h-4" />Delete
								</button>
							</div>
						</div>

						<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
							{#each [
								['Environment ARN', selectedEnv.Arn ?? 'N/A'],
								['Airflow Version', selectedEnv.AirflowVersion ?? 'N/A'],
								['Environment Class', selectedEnv.EnvironmentClass ?? 'N/A'],
								['Schedulers', String(selectedEnv.Schedulers ?? 'N/A')],
								['Webserver Access', selectedEnv.WebserverAccessMode ?? 'N/A'],
								['Min Workers', String(selectedEnv.MinWorkers ?? 'N/A')],
								['Max Workers', String(selectedEnv.MaxWorkers ?? 'N/A')],
								['DAGs S3 Path', selectedEnv.DagS3Path ?? 'N/A'],
								['Created At', selectedEnv.CreatedAt ? new Date(selectedEnv.CreatedAt).toLocaleDateString() : 'N/A']
							] as [label, value]}
								<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
									<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
									<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
								</div>
							{/each}
						</div>

						{#if selectedEnv.WebserverUrl}
							<div class="bg-teal-50 dark:bg-teal-900/20 rounded-lg p-3 flex items-center gap-3">
								<Globe class="w-4 h-4 text-teal-600 dark:text-teal-400 flex-shrink-0" />
								<div class="min-w-0">
									<p class="text-xs text-slate-500 dark:text-slate-400">Webserver URL</p>
									<a
										href={selectedEnv.WebserverUrl}
										target="_blank"
										rel="noopener noreferrer"
										class="text-sm font-medium text-teal-600 dark:text-teal-400 hover:underline truncate block"
									>{selectedEnv.WebserverUrl}</a>
								</div>
							</div>
						{/if}

						{#if cliToken}
							<div class="bg-slate-900 rounded-lg p-3 space-y-2">
								<div class="flex items-center justify-between">
									<p class="text-xs font-semibold text-slate-300 flex items-center gap-2"><Terminal class="w-3.5 h-3.5" /> Airflow CLI Token</p>
									<button onclick={() => (cliToken = null)} class="text-xs text-slate-400 hover:text-slate-200">Clear</button>
								</div>
								<p class="text-[10px] text-slate-400">Endpoint: <span class="font-mono">{cliToken.hostname}</span></p>
								<pre class="overflow-x-auto rounded bg-black/40 p-2 text-[10px] font-mono text-teal-300 break-all whitespace-pre-wrap">{cliToken.token}</pre>
							</div>
						{/if}

						{#if selectedEnv.LoggingConfiguration}
							<div>
								<h3 class="font-semibold text-slate-900 dark:text-white mb-2 flex items-center gap-2">
									<Activity class="w-4 h-4" />Logging
								</h3>
								<div class="grid grid-cols-2 gap-2">
									{#each Object.entries(selectedEnv.LoggingConfiguration) as [logType, logConfig]}
										{#if logConfig && typeof logConfig === 'object' && 'Enabled' in logConfig}
											<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2 flex items-center justify-between">
												<span class="text-xs text-slate-600 dark:text-slate-400 capitalize">{logType.replace(/([A-Z])/g, ' $1').trim()}</span>
												<span class="text-xs px-1.5 py-0.5 rounded-full {logConfig.Enabled ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-500'}">{logConfig.Enabled ? 'On' : 'Off'}</span>
											</div>
										{/if}
									{/each}
								</div>
							</div>
						{/if}
					</div>
				{:else}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
						<Wind class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
						<p class="text-slate-500 dark:text-slate-400">Select an environment to view details</p>
					</div>
				{/if}
			</div>
		</div>
	{:else}
		<!-- Metrics tab -->
		<div class="space-y-4">
			<!-- Environment selector -->
			<div class="flex items-center gap-3">
				<select
					bind:value={metricsEnvName}
					class="flex-1 px-3 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-teal-500 text-sm"
				>
					<option value="">Select an environment...</option>
					{#each environmentNames as name}
						<option value={name}>{name}</option>
					{/each}
				</select>
				<button
					onclick={() => metricsEnvName && loadMetrics(metricsEnvName)}
					disabled={!metricsEnvName || loadingMetrics}
					class="flex items-center gap-1.5 px-4 py-2 bg-teal-600 hover:bg-teal-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
				>
					<BarChart2 class="w-4 h-4" />Load Metrics
				</button>
			</div>

			{#if loadingMetrics}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-teal-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading metrics...</p>
				</div>
			{:else if metricsEnvName && metricData.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<BarChart2 class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No metrics published for <span class="font-medium">{metricsEnvName}</span></p>
					<p class="text-xs text-slate-400 dark:text-slate-500 mt-1">Metrics are published via PublishMetrics API calls from your environment</p>
				</div>
			{:else if metricData.length > 0}
				<div class="space-y-4">
					<div class="flex items-center justify-between">
						<h3 class="text-sm font-medium text-slate-700 dark:text-slate-300">
							Metrics for <span class="font-semibold text-slate-900 dark:text-white">{metricsEnvName}</span>
							<span class="ml-2 text-xs text-slate-400">({metricData.length} data points)</span>
						</h3>
					</div>
					{#each Object.entries(groupedMetrics()) as [metricName, dataPoints]}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
							<div class="flex items-center justify-between mb-3">
								<h4 class="font-semibold text-slate-900 dark:text-white text-sm">{metricName}</h4>
								<span class="text-xs text-slate-500 dark:text-slate-400">{dataPoints.length} point{dataPoints.length !== 1 ? 's' : ''} · {dataPoints[0]?.Unit ?? 'None'}</span>
							</div>
							<!-- Simple bar visualization -->
							{#if dataPoints.some(d => d.Value != null)}
								{@const values = dataPoints.filter(d => d.Value != null).map(d => d.Value as number)}
								{@const maxVal = Math.max(...values, 1)}
								<div class="flex items-end gap-1 h-16">
									{#each values.slice(-20) as val}
										<div
											class="flex-1 bg-teal-400 dark:bg-teal-600 rounded-sm min-h-[2px] transition-all"
											style="height: {Math.max((val / maxVal) * 100, 2)}%"
											title={String(val)}
										></div>
									{/each}
								</div>
								<div class="flex justify-between text-xs text-slate-400 mt-1">
									<span>min: {Math.min(...values).toFixed(2)}</span>
									<span>max: {maxVal.toFixed(2)}</span>
									<span>latest: {values[values.length - 1]?.toFixed(2)}</span>
								</div>
							{/if}
							<!-- Dimensions summary -->
							{#if dataPoints[0]?.Dimensions?.length}
								<div class="mt-2 flex flex-wrap gap-1">
									{#each dataPoints[0].Dimensions as dim}
										<span class="text-xs px-2 py-0.5 bg-slate-100 dark:bg-slate-700 rounded text-slate-600 dark:text-slate-400">{dim.Name}={dim.Value}</span>
									{/each}
								</div>
							{/if}
						</div>
					{/each}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<BarChart2 class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">Select an environment and click Load Metrics</p>
				</div>
			{/if}
		</div>
	{/if}
</div>

<!-- Create Environment Modal -->
{#if showCreate}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-lg max-h-[90vh] overflow-y-auto">
			<div class="p-6 border-b border-slate-200 dark:border-slate-700">
				<h2 class="text-lg font-bold text-slate-900 dark:text-white">Create Environment</h2>
				<p class="text-sm text-slate-500 dark:text-slate-400 mt-0.5">Create a new MWAA Airflow environment</p>
			</div>
			<div class="p-6 space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Name <span class="text-red-500">*</span></label>
					<input bind:value={newName} type="text" placeholder="my-airflow-env" class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">DAGs S3 Path <span class="text-red-500">*</span></label>
					<input bind:value={newDagS3Path} type="text" placeholder="dags/" class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Execution Role ARN <span class="text-red-500">*</span></label>
					<input bind:value={newExecutionRoleArn} type="text" placeholder="arn:aws:iam::123456789012:role/MyRole" class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Source Bucket ARN <span class="text-red-500">*</span></label>
					<input bind:value={newSourceBucketArn} type="text" placeholder="arn:aws:s3:::my-airflow-bucket" class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Airflow Version</label>
						<input bind:value={newAirflowVersion} type="text" placeholder="2.10.3" class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Environment Class</label>
						<select bind:value={newEnvClass} class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500">
							<option value="mw1.small">mw1.small</option>
							<option value="mw1.medium">mw1.medium</option>
							<option value="mw1.large">mw1.large</option>
							<option value="mw1.xlarge">mw1.xlarge</option>
							<option value="mw1.2xlarge">mw1.2xlarge</option>
						</select>
					</div>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Webserver Access Mode</label>
					<select bind:value={newAccessMode} class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500">
						<option value="PUBLIC_ONLY">PUBLIC_ONLY</option>
						<option value="PRIVATE_ONLY">PRIVATE_ONLY</option>
					</select>
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Min Workers</label>
						<input bind:value={newMinWorkers} type="number" min="1" class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Max Workers</label>
						<input bind:value={newMaxWorkers} type="number" min="1" class="w-full px-3 py-2 bg-white dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-2 focus:ring-teal-500" />
					</div>
				</div>
			</div>
			<div class="p-6 border-t border-slate-200 dark:border-slate-700 flex items-center justify-end gap-3">
				<button
					onclick={() => showCreate = false}
					class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition-colors"
				>Cancel</button>
				<button
					onclick={() => createEnvironment()}
					disabled={creating}
					class="flex items-center gap-1.5 px-4 py-2 bg-teal-600 hover:bg-teal-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
				>
					{#if creating}
						<div class="w-4 h-4 animate-spin rounded-full border-2 border-white border-t-transparent"></div>Creating...
					{:else}
						<Plus class="w-4 h-4" />Create
					{/if}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Delete Confirmation Modal -->
{#if deleteTarget}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md">
			<div class="p-6">
				<div class="flex items-center gap-3 mb-4">
					<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
						<XCircle class="w-5 h-5 text-red-600 dark:text-red-400" />
					</div>
					<h2 class="text-lg font-bold text-slate-900 dark:text-white">Delete Environment</h2>
				</div>
				<p class="text-slate-600 dark:text-slate-400 text-sm">
					Are you sure you want to delete <span class="font-semibold text-slate-900 dark:text-white">{deleteTarget}</span>? This action cannot be undone.
				</p>
			</div>
			<div class="p-6 border-t border-slate-200 dark:border-slate-700 flex items-center justify-end gap-3">
				<button
					onclick={() => deleteTarget = ''}
					class="px-4 py-2 text-sm text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white transition-colors"
				>Cancel</button>
				<button
					onclick={() => deleteEnvironment()}
					disabled={deleting}
					class="flex items-center gap-1.5 px-4 py-2 bg-red-600 hover:bg-red-700 disabled:opacity-50 text-white text-sm font-medium rounded-lg transition-colors"
				>
					{#if deleting}
						<div class="w-4 h-4 animate-spin rounded-full border-2 border-white border-t-transparent"></div>Deleting...
					{:else}
						<Trash2 class="w-4 h-4" />Delete
					{/if}
				</button>
			</div>
		</div>
	</div>
{/if}
