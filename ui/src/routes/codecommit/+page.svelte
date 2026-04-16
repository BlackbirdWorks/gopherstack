<script lang="ts">
	import { onMount } from 'svelte';
	import { getCodeCommitClient } from '$lib/aws-client';
	import {
		ListRepositoriesCommand,
		GetRepositoryCommand,
		ListBranchesCommand,
		type RepositoryNameIdPair,
		type RepositoryMetadata
	} from '@aws-sdk/client-codecommit';
	import { toast } from 'svelte-sonner';
	import { GitBranch, RefreshCw, Search, GitCommit, ChevronRight, Folder } from 'lucide-svelte';

	const cc = getCodeCommitClient();

	let loading = $state(false);
	let repositories = $state<RepositoryNameIdPair[]>([]);
	let selectedRepo = $state<RepositoryMetadata | null>(null);
	let branches = $state<string[]>([]);
	let searchQuery = $state('');
	let loadingDetail = $state(false);

	const filteredRepos = $derived(
		repositories.filter((r) => (r.repositoryName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function loadData() {
		loading = true;
		try {
			const resp = await cc.send(new ListRepositoriesCommand({}));
			repositories = resp.repositories ?? [];
		} catch (e) {
			toast.error('Failed to load CodeCommit data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectRepo(repoName: string) {
		loadingDetail = true;
		try {
			const [detailResp, branchResp] = await Promise.all([
				cc.send(new GetRepositoryCommand({ repositoryName: repoName })),
				cc.send(new ListBranchesCommand({ repositoryName: repoName }))
			]);
			selectedRepo = detailResp.repositoryMetadata ?? null;
			branches = branchResp.branches ?? [];
		} catch (e) {
			toast.error('Failed to load repository details: ' + String(e));
		} finally {
			loadingDetail = false;
		}
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<GitBranch class="w-7 h-7 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS CodeCommit</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fully managed source control service hosting Git repositories</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Folder class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{repositories.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Repositories</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><GitBranch class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{branches.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Branches (selected)</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><GitCommit class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{selectedRepo?.defaultBranch ?? '—'}</p><p class="text-sm text-gray-500 dark:text-gray-400">Default Branch</p></div>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 lg:col-span-1">
			<div class="p-4 border-b border-slate-200 dark:border-slate-700">
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search repositories..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full" />
				</div>
			</div>
			<div class="p-2 max-h-96 overflow-y-auto">
				{#if loading}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
				{:else if filteredRepos.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No repositories found</div>
				{:else}
					{#each filteredRepos as repo}
						<button onclick={() => selectRepo(repo.repositoryName ?? '')}
							class="w-full text-left flex items-center justify-between p-3 rounded-lg hover:bg-gray-50 dark:hover:bg-slate-700 {selectedRepo?.repositoryName === repo.repositoryName ? 'bg-orange-50 dark:bg-orange-900/20 border border-orange-200 dark:border-orange-800' : ''}">
							<div class="flex items-center gap-2">
								<Folder class="w-4 h-4 text-orange-500" />
								<span class="text-sm font-medium text-gray-900 dark:text-white">{repo.repositoryName}</span>
							</div>
							<ChevronRight class="w-4 h-4 text-gray-400" />
						</button>
					{/each}
				{/if}
			</div>
		</div>

		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 lg:col-span-2 p-4">
			{#if loadingDetail}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading details...</div>
			{:else if selectedRepo}
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">{selectedRepo.repositoryName}</h2>
				<div class="grid grid-cols-2 gap-4 mb-4">
					<div><p class="text-xs text-gray-500 dark:text-gray-400">Repository ID</p><p class="text-sm font-medium text-gray-900 dark:text-white">{selectedRepo.repositoryId}</p></div>
					<div><p class="text-xs text-gray-500 dark:text-gray-400">Default Branch</p><p class="text-sm font-medium text-gray-900 dark:text-white">{selectedRepo.defaultBranch ?? 'N/A'}</p></div>
					<div class="col-span-2"><p class="text-xs text-gray-500 dark:text-gray-400">Clone URL (HTTPS)</p><p class="text-sm font-mono text-gray-900 dark:text-white truncate">{selectedRepo.cloneUrlHttp}</p></div>
					<div class="col-span-2"><p class="text-xs text-gray-500 dark:text-gray-400">Description</p><p class="text-sm text-gray-900 dark:text-white">{selectedRepo.repositoryDescription ?? 'No description'}</p></div>
				</div>
				{#if branches.length > 0}
					<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-2">Branches ({branches.length})</h3>
					<div class="flex flex-wrap gap-2">
						{#each branches as branch}
							<span class="flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400">
								<GitBranch class="w-3 h-3" /> {branch}
							</span>
						{/each}
					</div>
				{/if}
			{:else}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a repository to view details</div>
			{/if}
		</div>
	</div>
</div>
