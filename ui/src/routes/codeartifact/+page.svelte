<script lang="ts">
	// CodeArtifact is a hierarchy: domains contain repositories, repositories
	// contain packages, and packages contain versions; package groups are a
	// separate domain-scoped resource used for origin-configuration policy.
	// Repositories are domain-scoped (like App Mesh's mesh scoping); packages
	// and versions are reached by drilling down from a repository / package
	// row rather than their own independent scope selector, since a package
	// only makes sense in the context of the repository (and package) that
	// was clicked.
	//
	// Packages have no Create or Update operation in the real API at all --
	// they come into existence via PublishPackageVersion and are described/
	// deleted directly, never created or updated as a standalone resource
	// (confirmed against the SDK's command list: no CreatePackage/
	// UpdatePackage). Domains have no Update operation either. Both are
	// modeled here without those affordances rather than inventing them.
	import { untrack } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getCodeArtifactClient } from '$lib/aws-client';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate, formatBytes } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import {
		ListDomainsCommand,
		CreateDomainCommand,
		DescribeDomainCommand,
		DeleteDomainCommand,
		ListRepositoriesInDomainCommand,
		CreateRepositoryCommand,
		DescribeRepositoryCommand,
		UpdateRepositoryCommand,
		DeleteRepositoryCommand,
		ListPackagesCommand,
		DescribePackageCommand,
		DeletePackageCommand,
		ListPackageVersionsCommand,
		DescribePackageVersionCommand,
		DeletePackageVersionsCommand,
		PublishPackageVersionCommand,
		UpdatePackageVersionsStatusCommand,
		ListPackageVersionDependenciesCommand,
		ListPackageGroupsCommand,
		CreatePackageGroupCommand,
		DescribePackageGroupCommand,
		UpdatePackageGroupCommand,
		DeletePackageGroupCommand,
		PackageFormat,
		type DomainSummary,
		type DomainDescription,
		type RepositorySummary,
		type RepositoryDescription,
		type PackageSummary,
		type PackageDescription,
		type PackageVersionSummary,
		type PackageVersionDescription,
		type PackageDependency,
		type PackageGroupSummary,
		type PackageGroupDescription,
		type PackageVersionStatus
	} from '@aws-sdk/client-codeartifact';
	import { toast } from 'svelte-sonner';
	import { Package, Plus, Trash2, Eye, Pencil, Database, Archive, ChevronRight, GitBranch, Layers, CheckCircle2 } from 'lucide-svelte';

	const ca = regionalClient(getCodeArtifactClient);

	type TabId = 'domains' | 'repositories' | 'packages' | 'versions' | 'groups';

	const tabs: TabDef[] = [
		{ id: 'domains', label: 'Domains' },
		{ id: 'repositories', label: 'Repositories' },
		{ id: 'packages', label: 'Packages' },
		{ id: 'versions', label: 'Versions' },
		{ id: 'groups', label: 'Pkg Groups' }
	];

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}
	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	async function sha256Hex(text: string): Promise<string> {
		const data = new TextEncoder().encode(text);
		const digest = await crypto.subtle.digest('SHA-256', data);
		return Array.from(new Uint8Array(digest))
			.map((b) => b.toString(16).padStart(2, '0'))
			.join('');
	}

	let activeTab = $state<TabId>('domains');
	let searchQuery = $state('');
	let selectedDomain = $state('');

	// --- Domains ---
	let domains = $state<DomainSummary[]>([]);
	async function fetchDomains(): Promise<void> {
		const resp = await ca().send(new ListDomainsCommand({}));
		domains = resp.domains ?? [];
		if (!untrack(() => selectedDomain) && domains.length > 0) {
			selectedDomain = domains[0].name ?? '';
		}
	}

	// --- Repositories (domain-scoped) ---
	let repositories = $state<RepositorySummary[]>([]);
	async function fetchRepositories(): Promise<void> {
		const domain = untrack(() => selectedDomain);
		repositories = domain
			? (await ca().send(new ListRepositoriesInDomainCommand({ domain }))).repositories ?? []
			: [];
	}

	// --- Packages (repository-scoped, reached via drill-down) ---
	let packages = $state<PackageSummary[]>([]);
	let currentDomain = $state('');
	let currentRepo = $state('');
	async function fetchPackages(): Promise<void> {
		if (!currentDomain || !currentRepo) {
			packages = [];
			return;
		}
		const resp = await ca().send(new ListPackagesCommand({ domain: currentDomain, repository: currentRepo }));
		packages = resp.packages ?? [];
	}

	function browseRepository(repo: RepositorySummary): void {
		if (!repo.domainName || !repo.name) return;
		currentDomain = repo.domainName;
		currentRepo = repo.name;
		selectedPackage = null;
		versions = [];
		switchTab('packages');
	}

	// --- Versions (package-scoped, reached via drill-down) ---
	let versions = $state<PackageVersionSummary[]>([]);
	let dependencies = $state<PackageDependency[]>([]);
	let selectedPackage = $state<PackageSummary | null>(null);
	let selectedVersion = $state<PackageVersionSummary | null>(null);
	let loadingDeps = $state(false);

	async function fetchVersions(): Promise<void> {
		if (!selectedPackage || !currentDomain || !currentRepo) {
			versions = [];
			return;
		}
		const resp = await ca().send(
			new ListPackageVersionsCommand({
				domain: currentDomain,
				repository: currentRepo,
				format: selectedPackage.format,
				namespace: selectedPackage.namespace,
				package: selectedPackage.package
			})
		);
		versions = resp.versions ?? [];
	}

	function browsePackage(pkg: PackageSummary): void {
		selectedPackage = pkg;
		selectedVersion = null;
		dependencies = [];
		switchTab('versions');
	}

	async function loadDependencies(version: PackageVersionSummary): Promise<void> {
		if (!selectedPackage) return;
		selectedVersion = version;
		dependencies = [];
		loadingDeps = true;
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
			toast.error('Failed to load dependencies: ' + describeError(e));
		} finally {
			loadingDeps = false;
		}
	}

	// --- Package Groups (domain-scoped) ---
	let packageGroups = $state<PackageGroupSummary[]>([]);
	async function fetchPackageGroups(): Promise<void> {
		const domain = untrack(() => selectedDomain);
		packageGroups = domain ? (await ca().send(new ListPackageGroupsCommand({ domain }))).packageGroups ?? [] : [];
	}

	const tabLoader = createTabLoader<TabId>({
		domains: () => fetchDomains().catch(rethrowDescribed),
		repositories: () => fetchRepositories().catch(rethrowDescribed),
		packages: () => fetchPackages().catch(rethrowDescribed),
		versions: () => fetchVersions().catch(rethrowDescribed),
		groups: () => fetchPackageGroups().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}
	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}
	function changeScopeDomain(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		selectedDomain = '';
		currentDomain = '';
		currentRepo = '';
		selectedPackage = null;
		selectedVersion = null;
		viewedDomain = null;
		viewedRepository = null;
		viewedPackage = null;
		viewedVersion = null;
		viewedGroup = null;
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	function matches(q: string, ...parts: (string | undefined)[]): boolean {
		if (!q) return true;
		return parts.some((p) => (p ?? '').toLowerCase().includes(q.toLowerCase()));
	}

	const filteredDomains = $derived(domains.filter((d) => matches(searchQuery, d.name)));
	const filteredRepos = $derived(repositories.filter((r) => matches(searchQuery, r.name)));
	const filteredPackages = $derived(packages.filter((p) => matches(searchQuery, p.package)));
	const filteredGroups = $derived(packageGroups.filter((g) => matches(searchQuery, g.pattern)));

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

	// ── Domains: create / detail / delete (no Update op exists) ─────────────
	let createDomainModal = $state<Modal | null>(null);
	let creatingDomain = $state(false);
	let createDomainError = $state<string | null>(null);
	let newDomainName = $state('');

	function openCreateDomainModal(): void {
		createDomainError = null;
		newDomainName = '';
		createDomainModal?.open();
	}

	async function submitCreateDomain(): Promise<void> {
		if (!newDomainName) {
			createDomainError = 'Domain name is required.';
			return;
		}
		creatingDomain = true;
		createDomainError = null;
		try {
			await ca().send(new CreateDomainCommand({ domain: newDomainName }));
			toast.success('Domain created');
			createDomainModal?.close();
			await tabLoader.refresh('domains');
		} catch (e) {
			const msg = describeError(e);
			createDomainError = msg;
			toast.error(msg);
		} finally {
			creatingDomain = false;
		}
	}

	let domainDetailModal = $state<Modal | null>(null);
	let viewedDomain = $state<DomainDescription | null>(null);
	let domainDetailLoading = $state(false);
	let domainDetailError = $state<string | null>(null);

	async function openDomainDetail(d: DomainSummary): Promise<void> {
		viewedDomain = null;
		domainDetailError = null;
		domainDetailModal?.open();
		if (!d.name) return;
		domainDetailLoading = true;
		try {
			const resp = await ca().send(new DescribeDomainCommand({ domain: d.name }));
			viewedDomain = resp.domain ?? null;
		} catch (e) {
			domainDetailError = describeError(e);
		} finally {
			domainDetailLoading = false;
		}
	}

	async function deleteDomain(d: DomainSummary | DomainDescription): Promise<void> {
		if (!d.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete domain',
			message: `Delete domain "${d.name}"? It must have no repositories.`
		});
		if (!confirmed) return;
		try {
			await ca().send(new DeleteDomainCommand({ domain: d.name }));
			toast.success('Domain deleted');
			domainDetailModal?.close();
			await tabLoader.refresh('domains');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Repositories: create / detail / edit / delete ───────────────────────
	let createRepoModal = $state<Modal | null>(null);
	let creatingRepo = $state(false);
	let createRepoError = $state<string | null>(null);
	let newRepoName = $state('');
	let newRepoDescription = $state('');

	function openCreateRepoModal(): void {
		createRepoError = null;
		newRepoName = '';
		newRepoDescription = '';
		createRepoModal?.open();
	}

	async function submitCreateRepo(): Promise<void> {
		if (!newRepoName || !selectedDomain) {
			createRepoError = 'Repository name is required, and a domain must be selected.';
			return;
		}
		creatingRepo = true;
		createRepoError = null;
		try {
			await ca().send(
				new CreateRepositoryCommand({ domain: selectedDomain, repository: newRepoName, description: newRepoDescription || undefined })
			);
			toast.success('Repository created');
			createRepoModal?.close();
			await tabLoader.refresh('repositories');
		} catch (e) {
			const msg = describeError(e);
			createRepoError = msg;
			toast.error(msg);
		} finally {
			creatingRepo = false;
		}
	}

	let repoDetailModal = $state<Modal | null>(null);
	let viewedRepository = $state<RepositoryDescription | null>(null);
	let repoDetailLoading = $state(false);
	let repoDetailError = $state<string | null>(null);

	async function openRepoDetail(r: RepositorySummary): Promise<void> {
		viewedRepository = null;
		repoDetailError = null;
		repoDetailModal?.open();
		if (!r.domainName || !r.name) return;
		repoDetailLoading = true;
		try {
			const resp = await ca().send(new DescribeRepositoryCommand({ domain: r.domainName, repository: r.name }));
			viewedRepository = resp.repository ?? null;
		} catch (e) {
			repoDetailError = describeError(e);
		} finally {
			repoDetailLoading = false;
		}
	}

	let editRepoModal = $state<Modal | null>(null);
	let editingRepo = $state(false);
	let editRepoError = $state<string | null>(null);
	let editRepoName = $state('');
	let editRepoDescription = $state('');

	function openEditRepoModal(r: RepositoryDescription): void {
		editRepoError = null;
		editRepoName = r.name ?? '';
		editRepoDescription = r.description ?? '';
		editRepoModal?.open();
	}

	async function submitEditRepo(): Promise<void> {
		if (!editRepoName || !selectedDomain) return;
		editingRepo = true;
		editRepoError = null;
		try {
			const resp = await ca().send(
				new UpdateRepositoryCommand({ domain: selectedDomain, repository: editRepoName, description: editRepoDescription || undefined })
			);
			toast.success('Repository updated');
			editRepoModal?.close();
			await tabLoader.refresh('repositories');
			viewedRepository = resp.repository ?? viewedRepository;
		} catch (e) {
			const msg = describeError(e);
			editRepoError = msg;
			toast.error(msg);
		} finally {
			editingRepo = false;
		}
	}

	async function deleteRepo(r: RepositorySummary | RepositoryDescription): Promise<void> {
		const domainName = r.domainName;
		if (!domainName || !r.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete repository',
			message: `Delete repository "${r.name}"?`
		});
		if (!confirmed) return;
		try {
			await ca().send(new DeleteRepositoryCommand({ domain: domainName, repository: r.name }));
			toast.success('Repository deleted');
			repoDetailModal?.close();
			await tabLoader.refresh('repositories');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Packages: detail / delete (no Create/Update op exists) ──────────────
	let packageDetailModal = $state<Modal | null>(null);
	let viewedPackage = $state<PackageDescription | null>(null);
	let packageDetailLoading = $state(false);
	let packageDetailError = $state<string | null>(null);

	async function openPackageDetail(p: PackageSummary): Promise<void> {
		viewedPackage = null;
		packageDetailError = null;
		packageDetailModal?.open();
		if (!p.format || !p.package) return;
		packageDetailLoading = true;
		try {
			const resp = await ca().send(
				new DescribePackageCommand({
					domain: currentDomain,
					repository: currentRepo,
					format: p.format,
					namespace: p.namespace,
					package: p.package
				})
			);
			viewedPackage = resp.package ?? null;
		} catch (e) {
			packageDetailError = describeError(e);
		} finally {
			packageDetailLoading = false;
		}
	}

	async function deletePackage(format: string | undefined, namespace: string | undefined, packageName: string | undefined): Promise<void> {
		if (!format || !packageName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete package',
			message: `Delete package "${packageName}"? All of its versions will be deleted.`
		});
		if (!confirmed) return;
		try {
			await ca().send(
				new DeletePackageCommand({ domain: currentDomain, repository: currentRepo, format: format as PackageFormat, namespace, package: packageName })
			);
			toast.success('Package deleted');
			packageDetailModal?.close();
			await tabLoader.refresh('packages');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Versions: publish (create) / detail / status update / delete ───────
	let publishVersionModal = $state<Modal | null>(null);
	let publishing = $state(false);
	let publishError = $state<string | null>(null);
	let newVersionFormat = $state<string>(PackageFormat.GENERIC);
	let newVersionPackage = $state('');
	let newVersionNamespace = $state('');
	let newVersionVersion = $state('');
	let newVersionAssetName = $state('');
	let newVersionAssetContent = $state('');

	function openPublishVersionModal(): void {
		publishError = null;
		newVersionFormat = selectedPackage?.format ?? PackageFormat.GENERIC;
		newVersionPackage = selectedPackage?.package ?? '';
		newVersionNamespace = selectedPackage?.namespace ?? '';
		newVersionVersion = '';
		newVersionAssetName = '';
		newVersionAssetContent = '';
		publishVersionModal?.open();
	}

	async function submitPublishVersion(): Promise<void> {
		if (!currentDomain || !currentRepo || !newVersionPackage || !newVersionVersion || !newVersionAssetName) {
			publishError = 'Package, version and asset name are required.';
			return;
		}
		publishing = true;
		publishError = null;
		try {
			const assetSHA256 = await sha256Hex(newVersionAssetContent);
			await ca().send(
				new PublishPackageVersionCommand({
					domain: currentDomain,
					repository: currentRepo,
					format: newVersionFormat as PackageFormat,
					namespace: newVersionNamespace || undefined,
					package: newVersionPackage,
					packageVersion: newVersionVersion,
					assetName: newVersionAssetName,
					assetContent: new TextEncoder().encode(newVersionAssetContent),
					assetSHA256
				})
			);
			toast.success('Package version published');
			publishVersionModal?.close();
			await tabLoader.refresh('versions');
		} catch (e) {
			const msg = describeError(e);
			publishError = msg;
			toast.error(msg);
		} finally {
			publishing = false;
		}
	}

	let versionDetailModal = $state<Modal | null>(null);
	let viewedVersion = $state<PackageVersionDescription | null>(null);
	let versionDetailLoading = $state(false);
	let versionDetailError = $state<string | null>(null);

	async function openVersionDetail(v: PackageVersionSummary): Promise<void> {
		if (!selectedPackage) return;
		viewedVersion = null;
		versionDetailError = null;
		versionDetailModal?.open();
		if (!v.version) return;
		versionDetailLoading = true;
		try {
			const resp = await ca().send(
				new DescribePackageVersionCommand({
					domain: currentDomain,
					repository: currentRepo,
					format: selectedPackage.format,
					namespace: selectedPackage.namespace,
					package: selectedPackage.package,
					packageVersion: v.version
				})
			);
			viewedVersion = resp.packageVersion ?? null;
		} catch (e) {
			versionDetailError = describeError(e);
		} finally {
			versionDetailLoading = false;
		}
	}

	let updatingVersion = $state<string | null>(null);

	async function updateVersionStatus(version: PackageVersionSummary, targetStatus: PackageVersionStatus): Promise<void> {
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
			await tabLoader.refresh('versions');
		} catch (e) {
			toast.error('Failed to update version status: ' + describeError(e));
		} finally {
			updatingVersion = null;
		}
	}

	async function deleteVersion(version: PackageVersionSummary): Promise<void> {
		if (!selectedPackage || !version.version) return;
		const confirmed = await confirmDestructive({
			title: 'Delete version',
			message: `Delete version ${version.version}? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await ca().send(
				new DeletePackageVersionsCommand({
					domain: currentDomain,
					repository: currentRepo,
					format: selectedPackage.format,
					namespace: selectedPackage.namespace,
					package: selectedPackage.package,
					versions: [version.version]
				})
			);
			toast.success('Version deleted');
			versionDetailModal?.close();
			if (selectedVersion?.version === version.version) {
				selectedVersion = null;
				dependencies = [];
			}
			await tabLoader.refresh('versions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Package Groups: create / detail / edit / delete ─────────────────────
	let createGroupModal = $state<Modal | null>(null);
	let creatingGroup = $state(false);
	let createGroupError = $state<string | null>(null);
	let newGroupPattern = $state('');
	let newGroupDescription = $state('');

	function openCreateGroupModal(): void {
		createGroupError = null;
		newGroupPattern = '';
		newGroupDescription = '';
		createGroupModal?.open();
	}

	async function submitCreateGroup(): Promise<void> {
		if (!newGroupPattern || !selectedDomain) {
			createGroupError = 'Pattern is required, and a domain must be selected.';
			return;
		}
		creatingGroup = true;
		createGroupError = null;
		try {
			await ca().send(
				new CreatePackageGroupCommand({ domain: selectedDomain, packageGroup: newGroupPattern, description: newGroupDescription || undefined })
			);
			toast.success('Package group created');
			createGroupModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			const msg = describeError(e);
			createGroupError = msg;
			toast.error(msg);
		} finally {
			creatingGroup = false;
		}
	}

	let groupDetailModal = $state<Modal | null>(null);
	let viewedGroup = $state<PackageGroupDescription | null>(null);
	let groupDetailLoading = $state(false);
	let groupDetailError = $state<string | null>(null);

	async function openGroupDetail(g: PackageGroupSummary): Promise<void> {
		viewedGroup = null;
		groupDetailError = null;
		groupDetailModal?.open();
		if (!g.pattern || !selectedDomain) return;
		groupDetailLoading = true;
		try {
			const resp = await ca().send(new DescribePackageGroupCommand({ domain: selectedDomain, packageGroup: g.pattern }));
			viewedGroup = resp.packageGroup ?? null;
		} catch (e) {
			groupDetailError = describeError(e);
		} finally {
			groupDetailLoading = false;
		}
	}

	let editGroupModal = $state<Modal | null>(null);
	let editingGroup = $state(false);
	let editGroupError = $state<string | null>(null);
	let editGroupPattern = $state('');
	let editGroupDescription = $state('');

	function openEditGroupModal(g: PackageGroupDescription): void {
		editGroupError = null;
		editGroupPattern = g.pattern ?? '';
		editGroupDescription = g.description ?? '';
		editGroupModal?.open();
	}

	async function submitEditGroup(): Promise<void> {
		if (!editGroupPattern || !selectedDomain) return;
		editingGroup = true;
		editGroupError = null;
		try {
			const resp = await ca().send(
				new UpdatePackageGroupCommand({ domain: selectedDomain, packageGroup: editGroupPattern, description: editGroupDescription || undefined })
			);
			toast.success('Package group updated');
			editGroupModal?.close();
			await tabLoader.refresh('groups');
			viewedGroup = resp.packageGroup ?? viewedGroup;
		} catch (e) {
			const msg = describeError(e);
			editGroupError = msg;
			toast.error(msg);
		} finally {
			editingGroup = false;
		}
	}

	async function deleteGroup(g: PackageGroupSummary | PackageGroupDescription): Promise<void> {
		if (!g.pattern || !selectedDomain) return;
		const confirmed = await confirmDestructive({
			title: 'Delete package group',
			message: `Delete package group "${g.pattern}"?`
		});
		if (!confirmed) return;
		try {
			await ca().send(new DeletePackageGroupCommand({ domain: selectedDomain, packageGroup: g.pattern }));
			toast.success('Package group deleted');
			groupDetailModal?.close();
			await tabLoader.refresh('groups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Package}
		title="AWS CodeArtifact"
		description="Fully managed artifact repository service"
		onRefresh={handleRefresh}
		color="indigo"
	>
		{#snippet actions()}
			{#if activeTab === 'domains'}
				<button onclick={openCreateDomainModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm">
					<Plus class="w-4 h-4" /> Create domain
				</button>
			{:else if activeTab === 'repositories'}
				<button onclick={openCreateRepoModal} disabled={!selectedDomain} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50 text-sm">
					<Plus class="w-4 h-4" /> Create repository
				</button>
			{:else if activeTab === 'versions'}
				<button onclick={openPublishVersionModal} disabled={!selectedPackage} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50 text-sm">
					<Plus class="w-4 h-4" /> Publish version
				</button>
			{:else if activeTab === 'groups'}
				<button onclick={openCreateGroupModal} disabled={!selectedDomain} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50 text-sm">
					<Plus class="w-4 h-4" /> Create package group
				</button>
			{/if}
		{/snippet}
	</PageHeader>

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
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="indigo" />
			<div class="flex items-center gap-3 flex-wrap">
				{#if activeTab === 'repositories' || activeTab === 'groups'}
					<label class="text-xs text-gray-500 dark:text-gray-400 flex items-center gap-2">
						Domain:
						<select
							bind:value={selectedDomain}
							onchange={changeScopeDomain}
							class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-2 py-1.5"
						>
							<option value="">Select a domain…</option>
							{#each domains as d (d.name)}
								<option value={d.name}>{d.name}</option>
							{/each}
						</select>
					</label>
				{/if}
				{#if activeTab !== 'versions'}
					<SearchInput bind:value={searchQuery} />
				{/if}
			</div>
		</div>
		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'domains'}
				{#snippet domainActionsCell(d: DomainSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDomainDetail(d)} title="View" aria-label="View domain {d.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteDomain(d)} title="Delete" aria-label="Delete domain {d.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const domainColumns = defineColumns<DomainSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'owner', label: 'Owner' },
					{ key: 'status', label: 'Status' },
					{ key: 'actions', label: '', render: domainActionsCell }
				])}
				<DataTable rows={filteredDomains} rowKey={(d) => d.name ?? ''} columns={domainColumns} loading={tabLoader.isLoading('domains')} emptyMessage="No domains found" />
			{:else if activeTab === 'repositories'}
				{#if !selectedDomain}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a domain above to view its repositories</div>
				{:else}
					{#snippet repoNameCell(r: RepositorySummary)}
						<button onclick={() => browseRepository(r)} class="flex items-center gap-1 text-left text-indigo-600 dark:text-indigo-400 hover:underline">
							{r.name} <ChevronRight class="w-3 h-3" />
						</button>
					{/snippet}
					{#snippet repoActionsCell(r: RepositorySummary)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openRepoDetail(r)} title="View" aria-label="View repository {r.name}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => deleteRepo(r)} title="Delete" aria-label="Delete repository {r.name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const repoColumns = defineColumns<RepositorySummary>([
						{ key: 'name', label: 'Name', render: repoNameCell },
						{ key: 'domainName', label: 'Domain' },
						{ key: 'description', label: 'Description' },
						{ key: 'actions', label: '', render: repoActionsCell }
					])}
					<DataTable rows={filteredRepos} rowKey={(r) => r.name ?? ''} columns={repoColumns} loading={tabLoader.isLoading('repositories')} emptyMessage="No repositories found" />
				{/if}
			{:else if activeTab === 'packages'}
				{#if !currentRepo}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a repository to browse its packages</div>
				{:else}
					{#snippet packageNameCell(p: PackageSummary)}
						<button onclick={() => browsePackage(p)} class="flex items-center gap-1 text-left text-indigo-600 dark:text-indigo-400 hover:underline">
							{p.package} <ChevronRight class="w-3 h-3" />
						</button>
					{/snippet}
					{#snippet packageActionsCell(p: PackageSummary)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openPackageDetail(p)} title="View" aria-label="View package {p.package}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => deletePackage(p.format, p.namespace, p.package)} title="Delete" aria-label="Delete package {p.package}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const packageColumns = defineColumns<PackageSummary>([
						{ key: 'package', label: 'Name', render: packageNameCell },
						{ key: 'format', label: 'Format' },
						{ key: 'namespace', label: 'Namespace' },
						{ key: 'actions', label: '', render: packageActionsCell }
					])}
					<DataTable rows={filteredPackages} rowKey={(p) => `${p.format}/${p.namespace ?? ''}/${p.package}`} columns={packageColumns} loading={tabLoader.isLoading('packages')} emptyMessage="No packages found. Select a repository to browse packages." />
				{/if}
			{:else if activeTab === 'versions'}
				{#if !selectedPackage}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a package to view versions</div>
				{:else}
					<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
						<div>
							<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">Versions of {selectedPackage.package}</h3>
							{#if tabLoader.isLoading('versions')}
								<p class="text-sm text-gray-500 dark:text-gray-400">Loading...</p>
							{:else if versions.length === 0}
								<p class="text-sm text-gray-500 dark:text-gray-400">No versions found</p>
							{:else}
								<div class="space-y-2 max-h-96 overflow-y-auto">
									{#each versions as ver (ver.version)}
										<div class="flex items-center gap-2 p-2 rounded bg-gray-50 dark:bg-slate-700/50 {selectedVersion?.version === ver.version ? 'ring-1 ring-indigo-400' : ''}">
											<button onclick={() => loadDependencies(ver)} class="flex flex-1 items-center justify-between text-left">
												<span class="text-sm font-mono text-gray-900 dark:text-white">{ver.version}</span>
												<span class="text-xs px-2 py-0.5 rounded-full {statusBadge(ver.status)}">{ver.status}</span>
											</button>
											<button onclick={() => openVersionDetail(ver)} title="View" aria-label="View version {ver.version}" class="rounded p-1 text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
											<button
												onclick={() => updateVersionStatus(ver, 'Published')}
												disabled={updatingVersion === ver.version || ver.status === 'Published'}
												title="Promote to Published"
												class="rounded p-1 text-green-600 hover:bg-green-100 disabled:opacity-30 dark:hover:bg-green-900/30"
											>
												<CheckCircle2 class="w-4 h-4" />
											</button>
											<button
												onclick={() => deleteVersion(ver)}
												title="Delete"
												aria-label="Delete version {ver.version}"
												class="rounded p-1 text-red-600 hover:bg-red-100 dark:hover:bg-red-900/30"
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
								<div class="space-y-1 max-h-96 overflow-y-auto">
									{#each dependencies as dep, i (i)}
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
				{#if !selectedDomain}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a domain above to view its package groups</div>
				{:else}
					{#snippet groupActionsCell(g: PackageGroupSummary)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openGroupDetail(g)} title="View" aria-label="View package group {g.pattern}" class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => deleteGroup(g)} title="Delete" aria-label="Delete package group {g.pattern}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const groupColumns = defineColumns<PackageGroupSummary>([
						{ key: 'pattern', label: 'Pattern' },
						{ key: 'description', label: 'Description' },
						{ key: 'actions', label: '', render: groupActionsCell }
					])}
					<DataTable rows={filteredGroups} rowKey={(g) => g.pattern ?? ''} columns={groupColumns} loading={tabLoader.isLoading('groups')} emptyMessage="No package groups found" />
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Create Domain -->
<Modal bind:this={createDomainModal} title="Create Domain">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="domain-name" class="text-sm text-slate-600 dark:text-slate-300">Domain name</label>
				<input id="domain-name" bind:value={newDomainName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createDomainError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDomainError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createDomainModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateDomain} disabled={creatingDomain} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingDomain ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Domain detail -->
<Modal bind:this={domainDetailModal} title="Domain">
	{#snippet children()}
		{#if domainDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if domainDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{domainDetailError}</p>
		{:else if viewedDomain}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedDomain.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedDomain.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Owner</dt><dd class="text-slate-900 dark:text-white">{viewedDomain.owner ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedDomain.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Repository count</dt><dd class="text-slate-900 dark:text-white">{viewedDomain.repositoryCount ?? 0}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Asset size</dt><dd class="text-slate-900 dark:text-white">{formatBytes(viewedDomain.assetSizeBytes)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedDomain.createdTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => domainDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedDomain}
			<button type="button" onclick={() => viewedDomain && deleteDomain(viewedDomain)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Create Repository -->
<Modal bind:this={createRepoModal} title="Create Repository">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="repo-name" class="text-sm text-slate-600 dark:text-slate-300">Repository name</label>
				<input id="repo-name" bind:value={newRepoName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="repo-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="repo-description" bind:value={newRepoDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createRepoError}
				<p class="text-sm text-red-600 dark:text-red-400">{createRepoError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createRepoModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateRepo} disabled={creatingRepo} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingRepo ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Repository detail -->
<Modal bind:this={repoDetailModal} title="Repository">
	{#snippet children()}
		{#if repoDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if repoDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{repoDetailError}</p>
		{:else if viewedRepository}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedRepository.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Domain</dt><dd class="text-slate-900 dark:text-white">{viewedRepository.domainName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedRepository.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedRepository.description ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => repoDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedRepository}
			<button type="button" onclick={() => viewedRepository && openEditRepoModal(viewedRepository)} class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedRepository && deleteRepo(viewedRepository)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Repository -->
<Modal bind:this={editRepoModal} title="Edit Repository">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="repo-edit-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="repo-edit-description" bind:value={editRepoDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editRepoError}
				<p class="text-sm text-red-600 dark:text-red-400">{editRepoError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editRepoModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditRepo} disabled={editingRepo} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{editingRepo ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Package detail -->
<Modal bind:this={packageDetailModal} title="Package">
	{#snippet children()}
		{#if packageDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if packageDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{packageDetailError}</p>
		{:else if viewedPackage}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedPackage.name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Format</dt><dd class="text-slate-900 dark:text-white">{viewedPackage.format ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Namespace</dt><dd class="text-slate-900 dark:text-white">{viewedPackage.namespace ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => packageDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedPackage}
			<button type="button" onclick={() => viewedPackage && deletePackage(viewedPackage.format, viewedPackage.namespace, viewedPackage.name)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Publish Package Version -->
<Modal bind:this={publishVersionModal} title="Publish Package Version">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="pv-format" class="text-sm text-slate-600 dark:text-slate-300">Format</label>
				<select id="pv-format" bind:value={newVersionFormat} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each Object.values(PackageFormat) as f (f)}
						<option value={f}>{f}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="pv-package" class="text-sm text-slate-600 dark:text-slate-300">Package</label>
				<input id="pv-package" bind:value={newVersionPackage} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pv-namespace" class="text-sm text-slate-600 dark:text-slate-300">Namespace (optional)</label>
				<input id="pv-namespace" bind:value={newVersionNamespace} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pv-version" class="text-sm text-slate-600 dark:text-slate-300">Version</label>
				<input id="pv-version" bind:value={newVersionVersion} placeholder="1.0.0" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pv-asset-name" class="text-sm text-slate-600 dark:text-slate-300">Asset name</label>
				<input id="pv-asset-name" bind:value={newVersionAssetName} placeholder="package.json" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="pv-asset-content" class="text-sm text-slate-600 dark:text-slate-300">Asset content</label>
				<textarea id="pv-asset-content" bind:value={newVersionAssetContent} rows={4} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if publishError}
				<p class="text-sm text-red-600 dark:text-red-400">{publishError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => publishVersionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitPublishVersion} disabled={publishing} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{publishing ? 'Publishing…' : 'Publish'}</button>
	{/snippet}
</Modal>

<!-- Version detail -->
<Modal bind:this={versionDetailModal} title="Package Version">
	{#snippet children()}
		{#if versionDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if versionDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{versionDetailError}</p>
		{:else if viewedVersion}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Version</dt><dd class="text-slate-900 dark:text-white">{viewedVersion.version ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedVersion.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Revision</dt><dd class="text-slate-900 dark:text-white">{viewedVersion.revision ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Published</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedVersion.publishedTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => versionDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- Create Package Group -->
<Modal bind:this={createGroupModal} title="Create Package Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="group-pattern" class="text-sm text-slate-600 dark:text-slate-300">Pattern</label>
				<input id="group-pattern" bind:value={newGroupPattern} placeholder="npm/*" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="group-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="group-description" bind:value={newGroupDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{createGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createGroupModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateGroup} disabled={creatingGroup} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{creatingGroup ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Package Group detail -->
<Modal bind:this={groupDetailModal} title="Package Group">
	{#snippet children()}
		{#if groupDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if groupDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{groupDetailError}</p>
		{:else if viewedGroup}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Pattern</dt><dd class="text-slate-900 dark:text-white font-mono">{viewedGroup.pattern ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedGroup.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedGroup.description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedGroup.createdTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => groupDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedGroup}
			<button type="button" onclick={() => viewedGroup && openEditGroupModal(viewedGroup)} class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedGroup && deleteGroup(viewedGroup)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Package Group -->
<Modal bind:this={editGroupModal} title="Edit Package Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="group-edit-description" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="group-edit-description" bind:value={editGroupDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editGroupError}
				<p class="text-sm text-red-600 dark:text-red-400">{editGroupError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editGroupModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditGroup} disabled={editingGroup} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-semibold text-white hover:bg-indigo-700 disabled:opacity-50">{editingGroup ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
