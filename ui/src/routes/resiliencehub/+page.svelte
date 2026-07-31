<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getResiliencehubClient } from '$lib/aws-client';
	import {
		ListAppsCommand,
		ListAppAssessmentsCommand,
		type AppSummary,
		type AppAssessmentSummary
	} from '@aws-sdk/client-resiliencehub';
	import { toast } from 'svelte-sonner';
	import { ShieldCheck, RefreshCw, Search, BarChart3, Activity, CheckCircle } from 'lucide-svelte';

	const rh = regionalClient(getResiliencehubClient);

	let loading = $state(false);
	let activeTab = $state<'apps' | 'assessments'>('apps');
	let searchQuery = $state('');
	let apps = $state<AppSummary[]>([]);
	let assessments = $state<AppAssessmentSummary[]>([]);

	const filteredApps = $derived(apps.filter((a) => (a.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredAssessments = $derived(assessments.filter((a) => (a.appArn ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	const passedAssessments = $derived(assessments.filter((a) => a.complianceStatus === 'PolicyMet').length);

	async function loadData() {
		loading = true;
		try {
			const [appsResp, assResp] = await Promise.all([
				rh().send(new ListAppsCommand({})),
				rh().send(new ListAppAssessmentsCommand({}))
			]);
			apps = appsResp.appSummaries ?? [];
			assessments = assResp.assessmentSummaries ?? [];
		} catch (e) {
			toast.error('Failed to load Resilience Hub data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<ShieldCheck class="w-7 h-7 text-emerald-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Resilience Hub</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Assess, monitor, and optimize application resilience</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-emerald-100 dark:bg-emerald-900/30 rounded-lg"><ShieldCheck class="w-5 h-5 text-emerald-600 dark:text-emerald-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{apps.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Applications</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Activity class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{assessments.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Assessments</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><CheckCircle class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{passedAssessments}</p><p class="text-sm text-gray-500 dark:text-gray-400">Policy Met</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2">
				{#each [['apps', 'Applications'], ['assessments', 'Assessments']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-emerald-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'apps'}
				{#if filteredApps.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No applications found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredApps as app}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<ShieldCheck class="w-5 h-5 text-emerald-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{app.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{app.appArn}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {app.complianceStatus === 'PolicyMet' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{app.complianceStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'assessments'}
				{#if filteredAssessments.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No assessments found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAssessments as assessment}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Activity class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white truncate max-w-sm">{assessment.assessmentName}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{assessment.assessmentStatus}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full {assessment.complianceStatus === 'PolicyMet' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400'}">{assessment.complianceStatus}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
