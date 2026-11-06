<script lang="ts">
	import { onMount } from 'svelte';
	import { Package } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import ArtifactRegistryDetail from '#lib/components/gcp/ArtifactRegistryDetail.svelte';
	import { confirmedAttempt, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		deleteResource,
		fileDownloadUrl,
		formatBytes,
		listDockerImages,
		listFiles,
		listPackages,
		listRepositories,
		listTags,
		listVersions,
		readStoredArLocation,
		readStoredArProject,
		writeStoredArScope,
		modeLabel,
		summarizeCleanup,
		type DockerImageInfo,
		type FileInfo,
		type PackageInfo,
		type RepoInfo,
		type TagInfo,
		type VersionInfo
	} from '#lib/gcpartifactregistry.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const TABS = [
		{ id: 'packages', label: 'Packages' },
		{ id: 'images', label: 'Docker images' },
		{ id: 'files', label: 'Files' },
		{ id: 'details', label: 'Details' }
	];

	let project = $state(readStoredArProject());
	let location = $state(readStoredArLocation());
	let repos = $state<RepoInfo[]>([]);
	let repo = $state('');
	let tab = $state('packages');
	let packages = $state<PackageInfo[]>([]);
	let files = $state<FileInfo[]>([]);
	let images = $state<DockerImageInfo[]>([]);
	let pkg = $state('');
	let versions = $state<VersionInfo[]>([]);
	let tags = $state<TagInfo[]>([]);
	let loading = $state(true);
	let childLoading = $state(false);
	let leafLoading = $state(false);
	let error = $state('');
	let childError = $state('');
	let leafError = $state('');
	let tagsNote = $state('');

	async function loadRepos() {
		try {
			repos = (await listRepositories({ project, location })).toSorted((a, b) => a.id.localeCompare(b.id));
			error = '';
			if (!repos.some((r) => r.name === repo)) repo = repos[0]?.name ?? '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChildren() {
		if (!repo) {
			packages = [];
			files = [];
			images = [];
			return;
		}
		childLoading = true;
		try {
			const docker = repos.find((r) => r.name === repo)?.format === 'DOCKER';
			[packages, files, images] = await Promise.all([listPackages(repo), listFiles(repo), docker ? listDockerImages(repo) : Promise.resolve([])]);
			childError = '';
			if (!packages.some((p) => p.name === pkg)) pkg = '';
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	async function loadLeaves() {
		if (!pkg) {
			versions = [];
			tags = [];
			return;
		}
		leafLoading = true;
		const [v, t] = await Promise.allSettled([listVersions(pkg), listTags(pkg)]);
		versions = v.status === 'fulfilled' ? v.value : [];
		leafError = v.status === 'rejected' ? describeGcpError(v.reason) : '';
		tags = t.status === 'fulfilled' ? t.value : [];
		tagsNote = t.status === 'rejected' ? 'Tags are not available for this repository format.' : '';
		leafLoading = false;
	}

	$effect(() => {
		void repo;
		void loadChildren();
	});
	$effect(() => {
		void pkg;
		void loadLeaves();
	});

	function applyScope(p: string, l: string) {
		project = p;
		location = l;
		writeStoredArScope(p, l);
		repo = '';
		pkg = '';
		loading = true;
		void loadRepos();
	}

	async function remove(kind: string, row: ResourceRow, after: () => Promise<void>) {
		const ok = await confirmedAttempt({ title: `Delete ${kind}`, message: `Delete ${kind} "${row.cells[0]}"?` }, () => deleteResource(row.id), `Deleted ${kind} ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`);
		if (ok) await after();
	}

	const repoRows = $derived(repos.map((r) => ({ id: r.name, cells: [r.id, r.format, modeLabel(r.mode), r.description, summarizeCleanup(r.raw), formatBytes(r.sizeBytes)], raw: r.raw })));
	const pkgRows = $derived(packages.map((p) => ({ id: p.name, cells: [p.id, p.displayName, p.updateTime ? new Date(p.updateTime).toLocaleString() : ''], raw: p.raw })));
	const fileRows = $derived(files.map((f) => ({ id: f.name, cells: [f.id, formatBytes(f.sizeBytes), f.owner, f.hashes.join(', ')], raw: f.raw })));
	const versionRows = $derived(versions.map((v) => ({ id: v.name, cells: [v.id, v.relatedTags.join(', '), v.createTime ? new Date(v.createTime).toLocaleString() : ''], raw: v.raw })));
	const imageRows = $derived(images.map((i) => ({ id: i.name, cells: [i.image, i.digest, i.tags.join(', '), formatBytes(i.sizeBytes), i.uploadTime ? new Date(i.uploadTime).toLocaleString() : ''], raw: i.raw })));
	const selectedRepo = $derived(repos.find((r) => r.name === repo));
	const tagRows = $derived(tags.map((t) => ({ id: t.name, cells: [t.id, t.version], raw: t.raw })));
	const pkgId = $derived(packages.find((p) => p.name === pkg)?.id ?? '');

	onMount(loadRepos);
</script>

<div class="space-y-6 pb-20" data-testid="artifactregistry-dashboard">
	<PageHeader icon={Package} title="Artifact Registry" description="Google Artifact Registry: repositories, packages, versions, tags and files" color="blue" onRefresh={() => { void loadRepos(); void loadChildren(); void loadLeaves(); }} />
	<GcpScopeBar prefix="artifactregistry" {project} {location} hint="Repositories are listed per project and location." onApply={applyScope} />

	<GcpResourceTable testId="artifactregistry-repo" noun="repositories" columns={['Repository', 'Format', 'Mode', 'Description', 'Cleanup', 'Size']} rows={repoRows} {loading} {error} selected={repo} onRetry={loadRepos} onSelect={(r) => { repo = r.id; pkg = ''; }} onDelete={(r) => remove('repository', r, loadRepos)} />

	{#if repo}
		<section class="space-y-3" data-testid="artifactregistry-repo-panel">
			<h2 class="font-mono text-sm text-gray-700 dark:text-gray-300">{repo}</h2>
			<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />
			{#if tab === 'packages'}
				<GcpResourceTable testId="artifactregistry-package" noun="packages" columns={['Package', 'Display name', 'Updated']} rows={pkgRows} loading={childLoading} error={childError} selected={pkg} onRetry={loadChildren} onSelect={(r) => (pkg = r.id)} onDelete={(r) => remove('package', r, loadChildren)} />
				{#if pkg}
					<h3 class="text-sm font-medium text-gray-700 dark:text-gray-300">Versions of <span class="font-mono">{pkgId}</span></h3>
					<GcpResourceTable testId="artifactregistry-version" noun="versions" columns={['Version', 'Tags', 'Created']} rows={versionRows} loading={leafLoading} error={leafError} onRetry={loadLeaves} onDelete={(r) => remove('version', r, loadLeaves)} />
					<h3 class="text-sm font-medium text-gray-700 dark:text-gray-300">Tags</h3>
					{#if tagsNote}
						<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="artifactregistry-tags-note">{tagsNote}</p>
					{:else}
						<GcpResourceTable testId="artifactregistry-tag" noun="tags" columns={['Tag', 'Version']} rows={tagRows} loading={leafLoading} error="" onRetry={loadLeaves} onDelete={(r) => remove('tag', r, loadLeaves)} />
					{/if}
				{/if}
			{:else if tab === 'images'}
				{#if selectedRepo?.format === 'DOCKER'}
					<GcpResourceTable testId="artifactregistry-image" noun="images" columns={['Image', 'Digest', 'Tags', 'Size', 'Uploaded']} rows={imageRows} loading={childLoading} error={childError} onRetry={loadChildren} />
				{:else}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="artifactregistry-image-na">Docker images are only listed for DOCKER repositories.</p>
				{/if}
			{:else if tab === 'files'}
				<GcpResourceTable testId="artifactregistry-file" noun="files" columns={['File', 'Size', 'Owner', 'Hashes']} rows={fileRows} loading={childLoading} error={childError} onRetry={loadChildren}>
					{#snippet actions(row)}
						<a class={GCP_MINI} href={fileDownloadUrl(row.id)} download data-testid="artifactregistry-file-download">Download</a>
					{/snippet}
				</GcpResourceTable>
			{:else if selectedRepo}
				<ArtifactRegistryDetail repo={selectedRepo} />
			{/if}
		</section>
	{/if}
</div>
