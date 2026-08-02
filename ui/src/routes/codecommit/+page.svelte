<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getCodeCommitClient } from '$lib/aws-client';
	import {
		ListRepositoriesCommand,
		GetRepositoryCommand,
		ListBranchesCommand,
		ListPullRequestsCommand,
		GetPullRequestCommand,
		CreatePullRequestCommand,
		GetBranchCommand,
		GetCommitCommand,
		GetFolderCommand,
		type RepositoryNameIdPair,
		type RepositoryMetadata,
		type PullRequest,
		type Commit,
		type GetFolderCommandOutput,
		type File as CCFile
	} from '@aws-sdk/client-codecommit';
	import { toast } from 'svelte-sonner';
	import { GitBranch, RefreshCw, Search, GitCommit, ChevronRight, Folder, GitMerge, Plus, X, FileText, FolderOpen, ArrowUp } from 'lucide-svelte';

	const cc = regionalClient(getCodeCommitClient);

	type DetailTab = 'overview' | 'branches' | 'pullrequests' | 'files' | 'commits';

	let loading = $state(false);
	let repositories = $state<RepositoryNameIdPair[]>([]);
	let selectedRepo = $state<RepositoryMetadata | null>(null);
	let branches = $state<string[]>([]);
	let pullRequests = $state<PullRequest[]>([]);
	let searchQuery = $state('');
	let loadingDetail = $state(false);
	let loadingPRs = $state(false);
	let detailTab = $state<DetailTab>('overview');

	// Create PR form
	let showCreatePR = $state(false);
	let prTitle = $state('');
	let prDescription = $state('');
	let prSourceBranch = $state('');
	let prDestBranch = $state('main');
	let creatingPR = $state(false);

	const filteredRepos = $derived(
		repositories.filter((r) => (r.repositoryName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const prCount = $derived(pullRequests.filter((pr) => pr.pullRequestStatus === 'OPEN').length);

	async function loadData() {
		loading = true;
		try {
			const resp = await cc().send(new ListRepositoriesCommand({}));
			repositories = resp.repositories ?? [];
		} catch (e) {
			toast.error('Failed to load CodeCommit data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectRepo(repoName: string) {
		loadingDetail = true;
		detailTab = 'overview';
		pullRequests = [];
		try {
			const [detailResp, branchResp] = await Promise.all([
				cc().send(new GetRepositoryCommand({ repositoryName: repoName })),
				cc().send(new ListBranchesCommand({ repositoryName: repoName }))
			]);
			selectedRepo = detailResp.repositoryMetadata ?? null;
			branches = branchResp.branches ?? [];
		} catch (e) {
			toast.error('Failed to load repository details: ' + String(e));
		} finally {
			loadingDetail = false;
		}
	}

	async function loadPullRequests() {
		if (!selectedRepo?.repositoryName) return;
		loadingPRs = true;
		try {
			const listResp = await cc().send(new ListPullRequestsCommand({
				repositoryName: selectedRepo.repositoryName
			}));
			const ids = listResp.pullRequestIds ?? [];
			if (ids.length > 0) {
				const details = await Promise.allSettled(
					ids.slice(0, 20).map((id) =>
						cc().send(new GetPullRequestCommand({ pullRequestId: id })).then((r) => r.pullRequest)
					)
				);
				pullRequests = details
					.filter((r) => r.status === 'fulfilled')
					.map((r) => (r as PromiseFulfilledResult<PullRequest | undefined>).value!)
					.filter(Boolean);
			} else {
				pullRequests = [];
			}
		} catch (e) {
			toast.error('Failed to load pull requests: ' + String(e));
		} finally {
			loadingPRs = false;
		}
	}

	// File browser
	let browseBranch = $state('');
	// currentFolder is the relative path inside the repo.
	let currentFolder = $state('');
	let folderContent = $state<GetFolderCommandOutput | null>(null);
	let folderFiles = $state<CCFile[]>([]);
	let loadingFiles = $state(false);

	async function loadFolder(path: string) {
		if (!selectedRepo?.repositoryName || !browseBranch) return;
		loadingFiles = true;
		try {
			const res = await cc().send(
				new GetFolderCommand({
					repositoryName: selectedRepo.repositoryName,
					commitSpecifier: browseBranch,
					folderPath: path || '/'
				})
			);
			folderContent = res;
			folderFiles = res.files ?? [];
			currentFolder = path;
		} catch (e) {
			toast.error('Failed to load folder: ' + String(e));
		} finally {
			loadingFiles = false;
		}
	}

	async function openFiles() {
		browseBranch = selectedRepo?.defaultBranch ?? branches[0] ?? '';
		if (browseBranch) await loadFolder('');
	}

	function parentFolder(path: string): string {
		const trimmed = path.replace(/\/$/, '');
		const idx = trimmed.lastIndexOf('/');
		return idx <= 0 ? '' : trimmed.slice(0, idx);
	}

	// Commit log: resolve the branch tip, then walk parent commits.
	let commits = $state<Commit[]>([]);
	let loadingCommits = $state(false);

	async function loadCommits() {
		if (!selectedRepo?.repositoryName) return;
		const branch = selectedRepo.defaultBranch ?? branches[0];
		if (!branch) return;
		loadingCommits = true;
		commits = [];
		try {
			const branchRes = await cc().send(
				new GetBranchCommand({ repositoryName: selectedRepo.repositoryName, branchName: branch })
			);
			let commitId = branchRes.branch?.commitId;
			const collected: Commit[] = [];
			let guard = 0;
			while (commitId && guard < 30) {
				const commitRes = await cc().send(
					new GetCommitCommand({ repositoryName: selectedRepo.repositoryName, commitId })
				);
				const commit = commitRes.commit;
				if (!commit) break;
				collected.push(commit);
				commitId = commit.parents?.[0];
				guard++;
			}
			commits = collected;
		} catch (e) {
			toast.error('Failed to load commit log: ' + String(e));
		} finally {
			loadingCommits = false;
		}
	}

	async function switchDetailTab(tab: DetailTab) {
		detailTab = tab;
		if (tab === 'pullrequests' && pullRequests.length === 0) await loadPullRequests();
		else if (tab === 'files') await openFiles();
		else if (tab === 'commits') await loadCommits();
	}

	async function createPR() {
		if (!selectedRepo?.repositoryName || !prTitle.trim() || !prSourceBranch.trim()) return;
		creatingPR = true;
		try {
			await cc().send(new CreatePullRequestCommand({
				title: prTitle.trim(),
				description: prDescription.trim() || undefined,
				targets: [{
					repositoryName: selectedRepo.repositoryName,
					sourceReference: prSourceBranch.trim(),
					destinationReference: prDestBranch.trim() || 'main'
				}]
			}));
			toast.success(`Pull request "${prTitle}" created`);
			showCreatePR = false;
			prTitle = '';
			prDescription = '';
			prSourceBranch = '';
			prDestBranch = 'main';
			await loadPullRequests();
		} catch (e) {
			toast.error('Failed to create pull request: ' + String(e));
		} finally {
			creatingPR = false;
		}
	}

	function prStatusBadge(status?: string): string {
		if (status === 'OPEN') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		if (status === 'CLOSED') return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
		if (status === 'MERGED') return 'bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-400';
		return 'bg-gray-100 text-gray-600';
	}

	onRegionChange(loadData);
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

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg"><Folder class="w-5 h-5 text-orange-600 dark:text-orange-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{repositories.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Repositories</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><GitBranch class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{branches.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Branches</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><GitMerge class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{prCount}</p><p class="text-sm text-gray-500 dark:text-gray-400">Open PRs</p></div>
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

		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 lg:col-span-2">
			{#if loadingDetail}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading details...</div>
			{:else if selectedRepo}
				<!-- Detail tabs -->
				<div class="flex gap-1 p-4 border-b border-gray-200 dark:border-gray-700">
					{#each ([['overview', 'Overview', Folder], ['files', 'Files', FolderOpen], ['commits', 'Commits', GitCommit], ['branches', 'Branches', GitBranch], ['pullrequests', 'Pull Requests', GitMerge]] as const) as [tab, label, Icon]}
						<button
							onclick={() => switchDetailTab(tab)}
							class="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium transition-colors {detailTab === tab ? 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400' : 'text-gray-500 hover:text-gray-700 dark:hover:text-gray-300'}"
						>
							<Icon class="w-3.5 h-3.5" />{label}
						</button>
					{/each}
					{#if detailTab === 'pullrequests'}
						<button
							onclick={() => (showCreatePR = true)}
							class="ml-auto flex items-center gap-1.5 px-3 py-1.5 rounded-lg bg-orange-500 text-white text-xs font-medium hover:bg-orange-600"
						>
							<Plus class="w-3.5 h-3.5" /> New PR
						</button>
					{/if}
				</div>

				{#if detailTab === 'overview'}
					<div class="p-4">
						<div class="grid grid-cols-2 gap-4 mb-4">
							<div><p class="text-xs text-gray-500 dark:text-gray-400">Repository ID</p><p class="text-sm font-medium text-gray-900 dark:text-white">{selectedRepo.repositoryId}</p></div>
							<div><p class="text-xs text-gray-500 dark:text-gray-400">Default Branch</p><p class="text-sm font-medium text-gray-900 dark:text-white">{selectedRepo.defaultBranch ?? 'N/A'}</p></div>
							<div class="col-span-2"><p class="text-xs text-gray-500 dark:text-gray-400">Clone URL (HTTPS)</p><p class="text-sm font-mono text-gray-900 dark:text-white truncate">{selectedRepo.cloneUrlHttp}</p></div>
							<div class="col-span-2"><p class="text-xs text-gray-500 dark:text-gray-400">Description</p><p class="text-sm text-gray-900 dark:text-white">{selectedRepo.repositoryDescription ?? 'No description'}</p></div>
						</div>
					</div>
				{:else if detailTab === 'branches'}
					<div class="p-4">
						{#if branches.length === 0}
							<div class="text-center py-8 text-gray-500">No branches found</div>
						{:else}
							<div class="flex flex-wrap gap-2">
								{#each branches as branch}
									<span class="flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400">
										<GitBranch class="w-3 h-3" /> {branch}
									</span>
								{/each}
							</div>
						{/if}
					</div>
				{:else if detailTab === 'files'}
					<div class="p-4 space-y-3">
						<div class="flex items-center gap-3">
							<select bind:value={browseBranch} onchange={() => loadFolder('')} class="px-3 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800">
								{#each branches as b}<option value={b}>{b}</option>{/each}
							</select>
							<div class="flex items-center gap-1 text-xs text-gray-500 font-mono truncate">
								<button onclick={() => loadFolder('')} class="hover:underline">root</button>
								{#if currentFolder}<span>/ {currentFolder}</span>{/if}
							</div>
						</div>
						{#if loadingFiles}
							<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-3 border-orange-500 border-t-transparent rounded-full"></div></div>
						{:else if !folderContent}
							<div class="text-center py-8 text-gray-500">No content</div>
						{:else}
							<div class="rounded-lg border border-gray-200 dark:border-gray-700 divide-y divide-gray-100 dark:divide-gray-700">
								{#if currentFolder}
									<button onclick={() => loadFolder(parentFolder(currentFolder))} class="flex w-full items-center gap-2 px-3 py-2 text-left text-sm hover:bg-gray-50 dark:hover:bg-gray-800">
										<ArrowUp class="w-4 h-4 text-gray-400" /> ..
									</button>
								{/if}
								{#each folderContent.subFolders ?? [] as sf}
									<button onclick={() => loadFolder(sf.absolutePath ?? '')} class="flex w-full items-center gap-2 px-3 py-2 text-left text-sm hover:bg-gray-50 dark:hover:bg-gray-800">
										<FolderOpen class="w-4 h-4 text-orange-500" /> {sf.relativePath}
									</button>
								{/each}
								{#each folderFiles as f}
									<div class="flex items-center gap-2 px-3 py-2 text-sm">
										<FileText class="w-4 h-4 text-gray-400" /> {f.relativePath}
										<span class="ml-auto text-xs text-gray-400 font-mono">{f.fileMode ?? ''}</span>
									</div>
								{/each}
								{#if (folderContent.subFolders ?? []).length === 0 && folderFiles.length === 0}
									<div class="px-3 py-6 text-center text-sm text-gray-400">Empty folder</div>
								{/if}
							</div>
						{/if}
					</div>
				{:else if detailTab === 'commits'}
					<div class="p-4">
						{#if loadingCommits}
							<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-3 border-orange-500 border-t-transparent rounded-full"></div></div>
						{:else if commits.length === 0}
							<div class="text-center py-8 text-gray-500">No commits found</div>
						{:else}
							<div class="relative space-y-0">
								{#each commits as commit, i}
									<div class="flex gap-3">
										<div class="flex flex-col items-center">
											<div class="w-3 h-3 rounded-full bg-orange-500 mt-1.5"></div>
											{#if i < commits.length - 1}<div class="w-0.5 flex-1 bg-gray-200 dark:bg-gray-700"></div>{/if}
										</div>
										<div class="pb-4 min-w-0">
											<div class="text-sm text-gray-900 dark:text-white">{(commit.message ?? '').split('\n')[0] || '(no message)'}</div>
											<div class="text-xs text-gray-500 font-mono">
												{commit.commitId?.slice(0, 8)} · {commit.author?.name ?? 'unknown'}
												{#if commit.author?.date}· {commit.author.date}{/if}
											</div>
										</div>
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{:else if detailTab === 'pullrequests'}
					<div class="p-4">
						{#if loadingPRs}
							<div class="flex justify-center py-8"><div class="animate-spin w-6 h-6 border-3 border-orange-500 border-t-transparent rounded-full"></div></div>
						{:else if pullRequests.length === 0}
							<div class="text-center py-8 text-gray-500">
								<GitMerge class="w-8 h-8 mx-auto mb-2 opacity-40" />
								<p class="text-sm">No pull requests found</p>
							</div>
						{:else}
							<div class="space-y-2">
								{#each pullRequests as pr}
									<div class="p-3 bg-gray-50 dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700">
										<div class="flex items-start justify-between gap-2">
											<div>
												<div class="text-sm font-medium text-gray-900 dark:text-white">{pr.title}</div>
												<div class="text-xs text-gray-500 mt-0.5">#{pr.pullRequestId} · {pr.pullRequestTargets?.[0]?.sourceReference ?? ''} → {pr.pullRequestTargets?.[0]?.destinationReference ?? ''}</div>
											</div>
											<span class="shrink-0 px-2 py-0.5 rounded text-xs font-medium {prStatusBadge(pr.pullRequestStatus)}">{pr.pullRequestStatus}</span>
										</div>
										{#if pr.description}
											<p class="text-xs text-gray-500 mt-1 line-clamp-2">{pr.description}</p>
										{/if}
									</div>
								{/each}
							</div>
						{/if}
					</div>
				{/if}
			{:else}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400 p-4">Select a repository to view details</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create PR Modal -->
{#if showCreatePR && selectedRepo}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-md p-6 space-y-4">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Pull Request</h2>
				<button onclick={() => (showCreatePR = false)} class="text-gray-400 hover:text-gray-600"><X class="w-5 h-5" /></button>
			</div>
			<p class="text-sm text-gray-500">Repository: {selectedRepo.repositoryName}</p>
			<div>
				<label for="pr-title" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Title</label>
				<input id="pr-title" bind:value={prTitle} type="text" placeholder="Feature: add new functionality" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="pr-desc" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Description (optional)</label>
				<textarea id="pr-desc" bind:value={prDescription} rows={3} placeholder="Describe your changes..." class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm"></textarea>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="pr-src" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Source Branch</label>
					{#if branches.length > 0}
						<select id="pr-src" bind:value={prSourceBranch} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
							<option value="">Select...</option>
							{#each branches as b}<option value={b}>{b}</option>{/each}
						</select>
					{:else}
						<input id="pr-src" bind:value={prSourceBranch} type="text" placeholder="feature-branch" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					{/if}
				</div>
				<div>
					<label for="pr-dest" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Target Branch</label>
					{#if branches.length > 0}
						<select id="pr-dest" bind:value={prDestBranch} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
							{#each branches as b}<option value={b}>{b}</option>{/each}
						</select>
					{:else}
						<input id="pr-dest" bind:value={prDestBranch} type="text" placeholder="main" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					{/if}
				</div>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => (showCreatePR = false)} class="flex-1 px-4 py-2 rounded-lg border text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createPR} disabled={creatingPR || !prTitle.trim() || !prSourceBranch.trim()} class="flex-1 px-4 py-2 rounded-lg bg-orange-500 text-white text-sm font-medium hover:bg-orange-600 disabled:opacity-50">
					{creatingPR ? 'Creating...' : 'Create PR'}
				</button>
			</div>
		</div>
	</div>
{/if}
