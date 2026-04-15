<script lang="ts">
	import { onMount } from 'svelte';
	import { getMWAAClient } from '$lib/aws-client';
	import {
		ListEnvironmentsCommand,
		GetEnvironmentCommand,
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
		Server,
		ChevronRight,
		Activity
	} from 'lucide-svelte';

	const mwaa = getMWAAClient();

	let loading = $state(false);
	let searchQuery = $state('');
	let environmentNames = $state<string[]>([]);
	let environments = $state<Environment[]>([]);
	let selectedEnv = $state<Environment | null>(null);
	let loadingDetail = $state(false);
	let loadedNames = $state(new Set<string>());

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
			const res = await mwaa.send(new ListEnvironmentsCommand({}));
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
			toLoad.map((name) => mwaa.send(new GetEnvironmentCommand({ Name: name })))
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
			const res = await mwaa.send(new GetEnvironmentCommand({ Name: name }));
			selectedEnv = res.Environment ?? null;
		} catch (err: unknown) {
			toast.error(`Failed to get environment: ${(err as Error).message}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function refresh() {
		environments = [];
		loadedNames = new Set();
		await loadEnvironmentNames();
	}

	onMount(() => {
		loadEnvironmentNames();
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

	<!-- Tab bar (single tab) -->
	<div class="flex items-center gap-1 border-b border-slate-200 dark:border-slate-700">
		<button
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px border-teal-500 text-teal-600 dark:text-teal-400"
		>
			<span class="flex items-center gap-1.5">
				<Wind class="w-4 h-4" />Environments
			</span>
		</button>
	</div>

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
						<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
							<Wind class="w-5 h-5 text-teal-600 dark:text-teal-400" />
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
</div>
