<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getECRClient } from '$lib/aws-client';
	import {
		DescribeRepositoriesCommand,
		CreateRepositoryCommand,
		DeleteRepositoryCommand,
		DescribeImagesCommand,
		BatchDeleteImageCommand,
		GetRepositoryPolicyCommand,
		GetLifecyclePolicyCommand,
		PutImageScanningConfigurationCommand,
		PutLifecyclePolicyCommand,
		StartImageScanCommand,
		DescribeRegistryCommand,
		GetRegistryScanningConfigurationCommand,
		GetSigningConfigurationCommand,
		DescribePullThroughCacheRulesCommand,
		DescribeRepositoryCreationTemplatesCommand,
		ListPullTimeUpdateExclusionsCommand,
		type Repository,
		type ImageDetail,
		type ReplicationConfiguration,
		type RegistryScanningConfiguration,
		type SigningConfiguration,
		type PullThroughCacheRule,
		type RepositoryCreationTemplate
	} from '@aws-sdk/client-ecr';
	import { toast } from 'svelte-sonner';
	import { Archive, Search, RefreshCw, Plus, Trash2, Image, Lock, Copy, ShieldCheck, ScanLine } from 'lucide-svelte';

	const ecr = getECRClient();

	let loading = $state(false);
	let repositories = $state<Repository[]>([]);
	let searchQuery = $state('');
	let selectedRepo = $state<Repository | null>(null);
	let images = $state<ImageDetail[]>([]);
	let loadingImages = $state(false);
	let detailTab = $state<'images' | 'policy' | 'lifecycle'>('images');
	let repoPolicy = $state<string | null>(null);
	let lifecyclePolicy = $state<string | null>(null);
	let scanningImages = $state<string[]>([]);
	let loadingRegistry = $state(false);
	let registryReplication = $state<ReplicationConfiguration | null>(null);
	let registryScanning = $state<RegistryScanningConfiguration | null>(null);
	let registrySigning = $state<SigningConfiguration | null>(null);
	let pullThroughRules = $state<PullThroughCacheRule[]>([]);
	let repositoryTemplates = $state<RepositoryCreationTemplate[]>([]);
	let pullTimeExclusions = $state<string[]>([]);

	// Create repo modal
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newRepoName = $state('');
	let newMutability = $state<'MUTABLE' | 'IMMUTABLE'>('MUTABLE');
	let newScanOnPush = $state(true);
	let newEncryption = $state<'AES256' | 'KMS'>('AES256');

	// Delete image confirmation
	let deletingImages = $state<string[]>([]);

	const filteredRepos = $derived(
		repositories.filter((r) => (r.repositoryName ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	function formatBytes(bytes?: number): string {
		if (!bytes) return 'N/A';
		if (bytes > 1_000_000) return `${(bytes / 1_000_000).toFixed(1)} MB`;
		if (bytes > 1_000) return `${(bytes / 1_000).toFixed(1)} KB`;
		return `${bytes} B`;
	}

	function formatDate(d?: Date): string {
		return d ? new Date(d).toLocaleDateString() : 'N/A';
	}

	function imageTags(img: ImageDetail): string {
		return img.imageTags?.join(', ') ?? '<untagged>';
	}

	function countLabel(count: number, singular: string): string {
		return `${count} ${singular}${count === 1 ? '' : 's'}`;
	}

	async function loadRepositories() {
		loading = true;
		try {
			const res = await ecr.send(new DescribeRepositoriesCommand({ maxResults: 100 }));
			repositories = res.repositories ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load repositories: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadRegistryFeatures() {
		loadingRegistry = true;
		try {
			const [registry, scanning, signing, cacheRules, templates, exclusions] = await Promise.all([
				ecr.send(new DescribeRegistryCommand({})),
				ecr.send(new GetRegistryScanningConfigurationCommand({})),
				ecr.send(new GetSigningConfigurationCommand({})),
				ecr.send(new DescribePullThroughCacheRulesCommand({ maxResults: 100 })),
				ecr.send(new DescribeRepositoryCreationTemplatesCommand({ maxResults: 100 })),
				ecr.send(new ListPullTimeUpdateExclusionsCommand({ maxResults: 100 }))
			]);
			registryReplication = registry?.replicationConfiguration ?? null;
			registryScanning = scanning?.scanningConfiguration ?? null;
			registrySigning = signing?.signingConfiguration ?? null;
			pullThroughRules = cacheRules?.pullThroughCacheRules ?? [];
			repositoryTemplates = templates?.repositoryCreationTemplates ?? [];
			pullTimeExclusions = exclusions?.pullTimeUpdateExclusions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load registry features: ${(err as Error).message}`);
		} finally {
			loadingRegistry = false;
		}
	}

	async function selectRepo(repo: Repository) {
		selectedRepo = repo;
		images = [];
		repoPolicy = null;
		lifecyclePolicy = null;
		detailTab = 'images';
		await loadImages(repo.repositoryName ?? '');
	}

	async function loadImages(repoName: string) {
		loadingImages = true;
		try {
			const res = await ecr.send(new DescribeImagesCommand({ repositoryName: repoName }));
			images = res.imageDetails ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load images: ${(err as Error).message}`);
		} finally {
			loadingImages = false;
		}
	}

	async function loadPolicy(repoName: string) {
		try {
			const res = await ecr.send(new GetRepositoryPolicyCommand({ repositoryName: repoName }));
			repoPolicy = res.policyText ?? null;
		} catch {
			repoPolicy = null;
		}
	}

	async function loadLifecyclePolicy(repoName: string) {
		try {
			const res = await ecr.send(new GetLifecyclePolicyCommand({ repositoryName: repoName }));
			lifecyclePolicy = res.lifecyclePolicyText ?? null;
		} catch {
			lifecyclePolicy = null;
		}
	}

	async function createRepository() {
		if (!newRepoName.trim()) return;
		creating = true;
		try {
			await ecr.send(new CreateRepositoryCommand({
				repositoryName: newRepoName.trim(),
				imageTagMutability: newMutability,
				imageScanningConfiguration: { scanOnPush: newScanOnPush },
				encryptionConfiguration: { encryptionType: newEncryption }
			}));
			toast.success(`Repository "${newRepoName.trim()}" created`);
			showCreateModal = false;
			newRepoName = '';
			await loadRepositories();
		} catch (err: unknown) {
			toast.error(`Create failed: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteRepository(name: string) {
		if (!await confirmDestructive({ title: 'Delete Repository', message: `Delete repository "${name}"? All container images will be permanently removed.` })) return;
		try {
			await ecr.send(new DeleteRepositoryCommand({ repositoryName: name, force: true }));
			toast.success(`Repository "${name}" deleted`);
			if (selectedRepo?.repositoryName === name) selectedRepo = null;
			await loadRepositories();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function deleteImage(img: ImageDetail) {
		const tag = img.imageTags?.[0] ?? img.imageDigest?.slice(0, 20) ?? '';
		if (!selectedRepo || !await confirmDestructive({ title: 'Delete Image', message: `Delete image "${tag}"? This cannot be undone.` })) return;
		const digest = img.imageDigest ?? '';
		deletingImages = [...deletingImages, digest];
		try {
			await ecr.send(new BatchDeleteImageCommand({
				repositoryName: selectedRepo.repositoryName,
				imageIds: [{ imageDigest: img.imageDigest }]
			}));
			toast.success('Image deleted');
			await loadImages(selectedRepo.repositoryName ?? '');
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		} finally {
			deletingImages = deletingImages.filter((d) => d !== digest);
		}
	}

	async function startImageScan(img: ImageDetail) {
		if (!selectedRepo) return;
		const digest = img.imageDigest ?? '';
		scanningImages = [...scanningImages, digest];
		try {
			await ecr.send(new StartImageScanCommand({
				repositoryName: selectedRepo.repositoryName,
				imageId: { imageDigest: img.imageDigest }
			}));
			toast.success('Image scan completed');
			await loadImages(selectedRepo.repositoryName ?? '');
		} catch (err: unknown) {
			toast.error(`Scan failed: ${(err as Error).message}`);
		} finally {
			scanningImages = scanningImages.filter((d) => d !== digest);
		}
	}

	async function toggleScanOnPush() {
		if (!selectedRepo?.repositoryName) return;
		const next = !selectedRepo.imageScanningConfiguration?.scanOnPush;
		try {
			await ecr.send(new PutImageScanningConfigurationCommand({
				repositoryName: selectedRepo.repositoryName,
				imageScanningConfiguration: { scanOnPush: next }
			}));
			selectedRepo = {
				...selectedRepo,
				imageScanningConfiguration: { scanOnPush: next }
			};
			toast.success(`Scan on push ${next ? 'enabled' : 'disabled'}`);
			await loadRepositories();
		} catch (err: unknown) {
			toast.error(`Update failed: ${(err as Error).message}`);
		}
	}

	async function copyUri(uri: string | undefined) {
		if (!uri) return;
		await navigator.clipboard.writeText(uri);
		toast.success('URI copied');
	}

	$effect(() => {
		if (detailTab === 'policy' && selectedRepo) {
			void loadPolicy(selectedRepo.repositoryName ?? '');
		}
		if (detailTab === 'lifecycle' && selectedRepo) {
			void loadLifecyclePolicy(selectedRepo.repositoryName ?? '');
		}
	});

	onMount(() => {
		void loadRepositories();
		void loadRegistryFeatures();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Archive class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">ECR Repositories</h1>
				<p class="text-slate-600 dark:text-slate-300">Elastic Container Registry</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadRepositories()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button id="create-repo-btn" onclick={() => { showCreateModal = true; }} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />Create Repository
			</button>
		</div>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input type="text" bind:value={searchQuery} placeholder="Search repositories..." class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
	</div>

	<!-- Registry Features -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-5">
		<div class="flex items-center justify-between mb-4">
			<div>
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Registry Features</h2>
				<p class="text-sm text-slate-500 dark:text-slate-400">Registry-wide ECR configuration and SDK-backed resources</p>
			</div>
			<button onclick={loadRegistryFeatures} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh registry features">
				<RefreshCw class="w-4 h-4 {loadingRegistry ? 'animate-spin' : ''}" />
			</button>
		</div>

		<div class="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-3 mb-4">
			{#each [
				['Scan type', registryScanning?.scanType ?? 'BASIC'],
				['Scan rules', countLabel(registryScanning?.rules?.length ?? 0, 'rule')],
				['Signing rules', countLabel(registrySigning?.rules?.length ?? 0, 'rule')],
				['Replication rules', countLabel(registryReplication?.rules?.length ?? 0, 'rule')],
				['Pull-through cache', countLabel(pullThroughRules.length, 'rule')],
				['Templates', countLabel(repositoryTemplates.length, 'template')]
			] as [label, value]}
				<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
					<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
					<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5">{value}</p>
				</div>
			{/each}
		</div>

		<div class="grid grid-cols-1 lg:grid-cols-3 gap-3">
			<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-2">Scanning & Signing</h3>
				{#if registryScanning?.rules?.length}
					<div class="space-y-2 mb-3">
						{#each registryScanning.rules as rule}
							<div class="text-xs text-slate-600 dark:text-slate-300">
								<span class="font-medium">{rule.scanFrequency}</span>
								<span class="text-slate-400"> · </span>
								{rule.repositoryFilters?.map((f) => f.filter).join(', ') || 'all repositories'}
							</div>
						{/each}
					</div>
				{:else}
					<p class="text-xs text-slate-500 dark:text-slate-400 mb-3">No registry scanning rules configured</p>
				{/if}
				{#if registrySigning?.rules?.length}
					<div class="space-y-2">
						{#each registrySigning.rules as rule}
							<p class="text-xs text-slate-600 dark:text-slate-300 font-mono truncate">{rule.signingProfileArn}</p>
						{/each}
					</div>
				{:else}
					<p class="text-xs text-slate-500 dark:text-slate-400">No signing configuration</p>
				{/if}
			</div>

			<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-2">Pull-through Cache</h3>
				{#if pullThroughRules.length}
					<div class="space-y-2">
						{#each pullThroughRules as rule}
							<div>
								<p class="text-xs font-medium text-slate-700 dark:text-slate-200">{rule.ecrRepositoryPrefix}</p>
								<p class="text-xs text-slate-500 dark:text-slate-400 truncate">{rule.upstreamRegistryUrl}</p>
							</div>
						{/each}
					</div>
				{:else}
					<p class="text-xs text-slate-500 dark:text-slate-400">No pull-through cache rules</p>
				{/if}
			</div>

			<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
				<h3 class="text-sm font-semibold text-slate-900 dark:text-white mb-2">Creation Templates & Exclusions</h3>
				{#if repositoryTemplates.length}
					<div class="space-y-2 mb-3">
						{#each repositoryTemplates as template}
							<div>
								<p class="text-xs font-medium text-slate-700 dark:text-slate-200">{template.prefix}</p>
								<p class="text-xs text-slate-500 dark:text-slate-400">{template.appliedFor?.join(', ') || 'No scenarios set'}</p>
							</div>
						{/each}
					</div>
				{:else}
					<p class="text-xs text-slate-500 dark:text-slate-400 mb-3">No repository creation templates</p>
				{/if}
				<p class="text-xs text-slate-500 dark:text-slate-400">
					{countLabel(pullTimeExclusions.length, 'pull-time exclusion')}
				</p>
			</div>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Repo List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading repositories...</p>
				</div>
			{:else if filteredRepos.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<Archive class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No repositories found</p>
				</div>
			{:else}
				{#each filteredRepos as repo}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectRepo(repo)}
						onkeypress={(e) => { if (e.key === 'Enter') selectRepo(repo); }}
						class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-indigo-400 transition-colors cursor-pointer {selectedRepo?.repositoryName === repo.repositoryName ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
					>
						<div class="flex items-center justify-between">
							<div class="min-w-0 flex-1">
								<p class="font-medium text-slate-900 dark:text-white truncate">{repo.repositoryName}</p>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{formatDate(repo.createdAt)}</p>
							</div>
							<div class="flex items-center gap-1 ml-2 flex-shrink-0">
								{#if repo.imageTagMutability === 'IMMUTABLE'}
									<span class="px-1.5 py-0.5 text-xs rounded bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">Immutable</span>
								{/if}
								<button id="delete-repo-{repo.repositoryName}" onclick={(e) => { e.stopPropagation(); deleteRepository(repo.repositoryName ?? ''); }} class="p-1 text-slate-400 hover:text-red-500">
									<Trash2 class="w-4 h-4" />
								</button>
							</div>
						</div>
					</div>
				{/each}
			{/if}
		</div>

		<!-- Repo Detail -->
		<div class="lg:col-span-2">
			{#if selectedRepo}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
					<div class="mb-4">
						<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedRepo.repositoryName}</h2>
						<button onclick={() => copyUri(selectedRepo?.repositoryUri)} class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500 mt-1 font-mono">
							<Copy class="w-3 h-3" />
							{selectedRepo.repositoryUri}
						</button>
					</div>

					<!-- Repo metadata -->
					<div class="grid grid-cols-3 gap-3 mb-4">
						{#each [
							['Mutability', selectedRepo.imageTagMutability ?? 'MUTABLE'],
							['Encryption', selectedRepo.encryptionConfiguration?.encryptionType ?? 'AES256'],
							['Scan on Push', selectedRepo.imageScanningConfiguration?.scanOnPush ? 'Enabled' : 'Disabled']
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5">{value}</p>
							</div>
						{/each}
					</div>
					<div class="flex gap-2 mb-4">
						<button onclick={toggleScanOnPush} class="px-3 py-1.5 text-sm rounded-lg border border-slate-200 dark:border-slate-700 text-slate-700 dark:text-slate-200 hover:bg-slate-50 dark:hover:bg-slate-700 flex items-center gap-2">
							<ShieldCheck class="w-4 h-4" />
							{selectedRepo.imageScanningConfiguration?.scanOnPush ? 'Disable' : 'Enable'} scan on push
						</button>
					</div>

					<!-- Tabs -->
					<div class="flex border-b border-slate-200 dark:border-slate-700 mb-4">
						{#each [
							{ id: 'images', label: 'Images', icon: Image },
							{ id: 'policy', label: 'Repository Policy', icon: Lock },
							{ id: 'lifecycle', label: 'Lifecycle Policy', icon: ShieldCheck }
						] as tab}
							<button
								onclick={() => { detailTab = tab.id as 'images' | 'policy' | 'lifecycle'; }}
								class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors {detailTab === tab.id ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-600 dark:text-slate-400'}"
							>
								<tab.icon class="w-4 h-4" />{tab.label}
							</button>
						{/each}
					</div>

					{#if detailTab === 'images'}
						{#if loadingImages}
							<div class="text-center py-6"><div class="inline-block animate-spin rounded-full h-5 w-5 border-b-2 border-indigo-500"></div></div>
						{:else if images.length === 0}
							<div class="text-center py-6">
								<Image class="w-10 h-10 mx-auto text-slate-300 dark:text-slate-600 mb-2" />
								<p class="text-slate-500 dark:text-slate-400 text-sm">No images pushed yet</p>
								<p class="text-xs text-slate-400 dark:text-slate-500 mt-1 font-mono">docker push {selectedRepo.repositoryUri}:tag</p>
							</div>
						{:else}
							<div class="space-y-2">
								{#each images as img}
									<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/30 rounded-lg p-3">
										<div class="min-w-0 flex-1">
											<p class="text-sm font-medium text-slate-900 dark:text-white font-mono">{imageTags(img)}</p>
											<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
												{formatBytes(img.imageSizeInBytes)} · pushed {formatDate(img.imagePushedAt)}
											</p>
											<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
												Scan: {img.imageScanStatus?.status ?? 'Not scanned'} · Findings: {Object.values(img.imageScanFindingsSummary?.findingSeverityCounts ?? {}).reduce((sum, count) => sum + count, 0)}
											</p>
											<p class="text-xs text-slate-400 font-mono truncate">{img.imageDigest}</p>
										</div>
										<div class="ml-3 flex items-center gap-1">
											<button
												onclick={() => startImageScan(img)}
												disabled={scanningImages.includes(img.imageDigest ?? '')}
												class="p-1.5 text-slate-400 hover:text-indigo-500 disabled:opacity-50"
												title="Start image scan"
											>
												<ScanLine class="w-4 h-4" />
											</button>
											<button
												onclick={() => deleteImage(img)}
												disabled={deletingImages.includes(img.imageDigest ?? '')}
												class="p-1.5 text-slate-400 hover:text-red-500 disabled:opacity-50"
											>
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									</div>
								{/each}
							</div>
						{/if}
					{:else if detailTab === 'policy'}
						<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4">
							{#if repoPolicy}
								<pre class="text-xs text-slate-700 dark:text-slate-300 whitespace-pre-wrap font-mono">{repoPolicy}</pre>
							{:else}
								<p class="text-slate-500 dark:text-slate-400 text-sm">No repository policy set</p>
							{/if}
						</div>
					{:else}
						<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4">
							{#if lifecyclePolicy}
								<pre class="text-xs text-slate-700 dark:text-slate-300 whitespace-pre-wrap font-mono">{lifecyclePolicy}</pre>
							{:else}
								<p class="text-slate-500 dark:text-slate-400 text-sm">No lifecycle policy set</p>
							{/if}
						</div>
					{/if}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Archive class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a repository to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Repository Modal -->
{#if showCreateModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Repository</h2>
			<form onsubmit={(e) => { e.preventDefault(); createRepository(); }} class="space-y-4">
				<div>
					<label for="new-repo-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Repository Name</label>
					<input id="new-repo-name" type="text" bind:value={newRepoName} placeholder="e.g. my-service/api" class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" required />
				</div>
				<div>
					<label for="ecr-mutability" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Image Tag Mutability</label>
					<select id="ecr-mutability" bind:value={newMutability} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						<option value="MUTABLE">MUTABLE</option>
						<option value="IMMUTABLE">IMMUTABLE</option>
					</select>
				</div>
				<div>
					<label for="ecr-encryption" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Encryption</label>
					<select id="ecr-encryption" bind:value={newEncryption} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
						<option value="AES256">AES256</option>
						<option value="KMS">KMS</option>
					</select>
				</div>
				<label class="flex items-center gap-2 cursor-pointer">
					<input type="checkbox" bind:checked={newScanOnPush} class="rounded" />
					<span class="text-sm text-slate-700 dark:text-slate-300">Scan images on push</span>
				</label>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showCreateModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400">Cancel</button>
					<button id="confirm-create-repo-btn" type="submit" disabled={creating} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50">
						{creating ? 'Creating...' : 'Create Repository'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
