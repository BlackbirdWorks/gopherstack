<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { getCodeArtifactClient } from '$lib/aws-client';
	import {
		ListDomainsCommand,
		ListRepositoriesCommand,
		ListPackagesCommand,
		ListPackageVersionsCommand,
		ListPackageVersionDependenciesCommand,
		ListPackageGroupsCommand,
		UpdatePackageVersionsStatusCommand,
		type DomainSummary,
		type RepositorySummary,
		type PackageSummary,
		type PackageVersionSummary,
		type PackageDependency,
		type PackageGroupSummary,
		type PackageVersionStatus
	} from '@aws-sdk/client-codeartifact';
	import { toast } from 'svelte-sonner';
	import { Package, RefreshCw, Search, Database, Archive, ChevronRight, GitBranch, Layers, CheckCircle2, Trash2 } from 'lucide-svelte';

	const ca = regionalClient(getCodeArtifactClient);

	let loading = $state(false);
	let activeTab = $state<'domains' | 'repositories' | 'packages' | 'versions' | 'groups'>('domains');
	let searchQuery = $state('');
	let domains = $state<DomainSummary[]>([]);
	let repositories = $state<RepositorySummary[]>([]);
	let packages = $state<PackageSummary[]>([]);
	let versions = $state<PackageVersionSummary[]>([]);
	let dependencies = $state<PackageDependency[]>([]);
	let packageGroups = $state<PackageGroupSummary[]>([]);
	let selectedPackage = $state<PackageSummary | null>(null);
	let selectedVersion = $state<PackageVersionSummary | null>(null);
	let currentDomain = $state('');
	let currentRepo = $state('');
	let loadingVersions = $state(false);
	let loadingDeps = $state(false);

	const filteredDomains = $derived(domains.filter((d) => (d.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredRepos = $derived(repositories.filter((r) => (r.name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredPackages = $derived(packages.filter((p) => (p.package ?? '').toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			const [domainsResp, reposResp] = await Promise.all([
				ca().send(new ListDomainsCommand({})),
				ca().send(new ListRepositoriesCommand({}))
			]);
			domains = domainsResp.domains ?? [];
			repositories = reposResp.repositories ?? [];

			// Load package groups for the first domain
			if (domains.length > 0 && domains[0].name) {
				try {
					const groupResp = await ca().send(new ListPackageGroupsCommand({ domain: domains[0].name }));
					packageGroups = groupResp.packageGroups ?? [];
				} catch {
					packageGroups = [];
				}
			}
		} catch (e) {
			toast.error('Failed to load CodeArtifact data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadPackages(domain: string, repository: string) {
		currentDomain = domain;
		currentRepo = repository;
		try {
			const resp = await ca().send(new ListPackagesCommand({ domain, repository }));
			packages = resp.packages ?? [];
			activeTab = 'packages';
			selectedPackage = null;
			versions = [];
		} catch (e) {
			toast.error('Failed to load packages: ' + String(e));
		}
	}

	async function loadVersions(pkg: PackageSummary) {
		selectedPackage = pkg;
		versions = [];
		dependencies = [];
		selectedVersion = null;
		loadingVersions = true;
		activeTab = 'versions';
		try {
			const resp = await ca().send(
				new ListPackageVersionsCommand({
					domain: currentDomain,
					repository: currentRepo,
					format: pkg.format,
					namespace: pkg.namespace,
					package: pkg.package
				})
			);
			versions = resp.versions ?? [];
		} catch (e) {
			toast.error('Failed to load versions: ' + String(e));
		} finally {
			loadingVersions = false;
		}
	}

	async function loadDependencies(version: PackageVersionSummary) {
		if (!selectedPackage) return;
		selectedVersion = version;
		loadingDeps = true;
		dependencies = [];
		try {
			const resp = await ca().send(
				new ListPackageVersionDependenciesCommand({
					domain: currentDomain,
					repository: currentRepo,
					format: selectedPackage.format,
					namespace: selectedPackage.namespace,
					package: selectedPackage.package,
					packageVersion: version.version
				})
			);
			dependencies = resp.dependencies ?? [];
		} catch (e) {
			toast.error('Failed to load dependencies: ' + String(e));
		} finally {
			loadingDeps = false;
		}
	}

	let updatingVersion = $state<string | null>(null);

	// Promote (Published) or dispose (Disposed) a specific package version via
	// UpdatePackageVersionsStatus.
	async function updateVersionStatus(version: PackageVersionSummary, targetStatus: PackageVersionStatus) {
		if (!selectedPackage || !version.version) return;
		if (
			targetStatus === 'Disposed' &&
			!(await confirmDestructive({
				title: 'Dispose Version',
				message: `Dispose version ${version.version}? Its assets will be permanently deleted.`,
				confirmLabel: 'Dispose'
			}))
		)
			return;
		updatingVersion = version.version;
		try {
			const resp = await ca().send(
				new UpdatePackageVersionsStatusCommand({
					domain: currentDomain,
					repository: currentRepo,
					format: selectedPackage.format,
					namespace: selectedPackage.namespace,
					package: selectedPackage.package,
					versions: [version.version],
					targetStatus
				})
			);
			const failed = resp.failedVersions?.[version.version];
			if (failed) {
				toast.error(`Failed: ${failed.errorMessage ?? failed.errorCode}`);
			} else {
				toast.success(`Version ${version.version} → ${targetStatus}`);
			}
			await loadVersions(selectedPackage);
		} catch (e) {
			toast.error('Failed to update version status: ' + String(e));
		} finally {
			updatingVersion = null;
		}
	}

	function statusBadge(status: string | undefined) {
		switch (status) {
			case 'Published':
				return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
			case 'Unfinished':
				return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-400';
			case 'Unlisted':
				return 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400';
			case 'Archived':
				return 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-400';
			case 'Disposed':
				return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
			default:
				return 'bg-gray-100 dark:bg-gray-800 text-gray-600 dark:text-gray-400';
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Package class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS CodeArtifact</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Fully managed artifact repository service</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg"><Database class="w-5 h-5 text-indigo-600 dark:text-indigo-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{domains.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Domains</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Archive class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{repositories.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Repositories</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Package class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{packages.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Packages</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Layers class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{packageGroups.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Pkg Groups</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['domains', 'Domains'], ['repositories', 'Repositories'], ['packages', 'Packages'], ['versions', 'Versions'], ['groups', 'Pkg Groups']] as [tab, label]}
					<button onclick={() => { activeTab = tab as typeof activeTab; searchQuery = ''; }}
						class="px-3 py-1.5 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-indigo-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'domains'}
				{#if filteredDomains.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No domains found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredDomains as domain}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Database class="w-5 h-5 text-indigo-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{domain.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{domain.owner}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">{domain.status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'repositories'}
				{#if filteredRepos.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No repositories found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredRepos as repo}
							<button onclick={() => loadPackages(repo.domainName ?? '', repo.name ?? '')}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<Archive class="w-5 h-5 text-blue-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{repo.name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{repo.domainName}</p>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-gray-400" />
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'packages'}
				{#if filteredPackages.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No packages found. Select a repository to browse packages.</div>
				{:else}
					<div class="space-y-2">
						{#each filteredPackages as pkg}
							<button onclick={() => loadVersions(pkg)}
								class="w-full flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 hover:bg-gray-100 dark:hover:bg-slate-700 text-left">
								<div class="flex items-center gap-3">
									<Package class="w-5 h-5 text-purple-500" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{pkg.package}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{pkg.format}{pkg.namespace ? ' · ' + pkg.namespace : ''}</p>
									</div>
								</div>
								<ChevronRight class="w-4 h-4 text-gray-400" />
							</button>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'versions'}
				{#if !selectedPackage}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a package to view versions</div>
				{:else}
					<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
						<div>
							<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
								Versions of {selectedPackage.package}
							</h3>
							{#if loadingVersions}
								<p class="text-sm text-gray-500 dark:text-gray-400">Loading...</p>
							{:else if versions.length === 0}
								<p class="text-sm text-gray-500 dark:text-gray-400">No versions found</p>
							{:else}
								<div class="space-y-2 max-h-80 overflow-y-auto">
									{#each versions as ver}
										<div class="flex items-center gap-2 p-2 rounded bg-gray-50 dark:bg-slate-700/50 {selectedVersion?.version === ver.version ? 'ring-1 ring-indigo-400' : ''}">
											<button onclick={() => loadDependencies(ver)} class="flex flex-1 items-center justify-between text-left">
												<span class="text-sm font-mono text-gray-900 dark:text-white">{ver.version}</span>
												<span class="text-xs px-2 py-0.5 rounded-full {statusBadge(ver.status)}">{ver.status}</span>
											</button>
											<button
												onclick={() => updateVersionStatus(ver, 'Published')}
												disabled={updatingVersion === ver.version || ver.status === 'Published'}
												title="Promote to Published"
												class="rounded p-1 text-green-600 hover:bg-green-100 disabled:opacity-30 dark:hover:bg-green-900/30"
											>
												<CheckCircle2 class="w-4 h-4" />
											</button>
											<button
												onclick={() => updateVersionStatus(ver, 'Disposed')}
												disabled={updatingVersion === ver.version || ver.status === 'Disposed'}
												title="Dispose (delete assets)"
												class="rounded p-1 text-red-600 hover:bg-red-100 disabled:opacity-30 dark:hover:bg-red-900/30"
											>
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						</div>
						<div>
							<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">
								{selectedVersion ? `Dependencies of ${selectedVersion.version}` : 'Select a version to view dependencies'}
							</h3>
							{#if loadingDeps}
								<p class="text-sm text-gray-500 dark:text-gray-400">Loading...</p>
							{:else if dependencies.length === 0}
								<p class="text-sm text-gray-500 dark:text-gray-400">{selectedVersion ? 'No dependencies' : 'Select a version above'}</p>
							{:else}
								<div class="space-y-1 max-h-80 overflow-y-auto">
									{#each dependencies as dep}
										<div class="flex items-center gap-2 p-2 rounded bg-gray-50 dark:bg-slate-700/50">
											<GitBranch class="w-3 h-3 text-gray-400 shrink-0" />
											<span class="text-sm text-gray-900 dark:text-white">{dep.package}</span>
											<span class="text-xs text-gray-500 dark:text-gray-400 ml-auto">{dep.versionRequirement}</span>
										</div>
									{/each}
								</div>
							{/if}
						</div>
					</div>
				{/if}
			{:else if activeTab === 'groups'}
				{#if packageGroups.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No package groups found</div>
				{:else}
					<div class="space-y-2">
						{#each packageGroups as group}
							<div class="flex items-center gap-3 p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<Layers class="w-5 h-5 text-green-500 shrink-0" />
								<div class="min-w-0">
									<p class="font-medium text-gray-900 dark:text-white font-mono text-sm">{group.pattern}</p>
									{#if group.description}
										<p class="text-xs text-gray-500 dark:text-gray-400">{group.description}</p>
									{/if}
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
