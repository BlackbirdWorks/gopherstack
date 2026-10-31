<script lang="ts">
	import { onMount } from 'svelte';
	import { Package, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { backendInfo, loadBackendInfo } from '#lib/cloud.svelte.ts';
	import { attempt, confirmedAttempt, copyText, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		ART_DOWNLOAD_LIMIT_MIB,
		artifactDownloadUrl,
		createGenericRepo,
		createRepo,
		deleteArtifact,
		deleteGenericRepo,
		deleteImage,
		deleteRepo,
		formatBytes,
		getConfig,
		imageTags,
		listArtifacts,
		listCompartments,
		listGenericRepos,
		listImages,
		listRepos,
		listSignatures,
		readStoredArtCompartment,
		registryHints,
		restoreImage,
		setCreatedOnFirstPush,
		shortDigest,
		updateRepo,
		validName,
		writeStoredArtCompartment,
		type ContainerConfig,
		type ContainerImage,
		type ContainerRepo,
		type GenericArtifact,
		type GenericRepo,
		type ImageSignature
	} from '#lib/ociartifacts.ts';

	const TABS = [
		{ id: 'containers', label: 'Container registry' },
		{ id: 'generic', label: 'Generic artifacts' }
	];

	let compartments = $state<string[]>([]);
	let compartment = $state(readStoredArtCompartment());
	let tab = $state('containers');
	let loading = $state(true);
	let error = $state('');

	let repos = $state<ContainerRepo[]>([]);
	let config = $state<ContainerConfig>();
	let selectedRepo = $state('');
	let images = $state<ContainerImage[]>([]);
	let showDeleted = $state(false);
	let selectedImage = $state('');
	let signatures = $state<ImageSignature[]>([]);
	let detailError = $state('');
	let readmeDraft = $state('');
	let tagFor = $state('');

	let showCreate = $state(false);
	let formName = $state('');
	let formPublic = $state(false);
	let formImmutable = $state(false);
	let formReadme = $state('');

	let genRepos = $state<GenericRepo[]>([]);
	let selectedGen = $state('');
	let artifacts = $state<GenericArtifact[]>([]);
	let showGenCreate = $state(false);
	let genName = $state('');
	let genDesc = $state('');
	let genImmutable = $state(false);

	const repo = $derived(repos.find((r) => r.id === selectedRepo));
	const image = $derived(images.find((i) => i.id === selectedImage));
	const gen = $derived(genRepos.find((r) => r.id === selectedGen));
	const info = $derived(backendInfo());
	const hints = $derived(
		repo && config
			? registryHints(window.location.hostname, info.ports.ociartifacts, config.namespace, repo.name, tagFor || (image ? (imageTags(image)[0] ?? '') : ''))
			: undefined
	);
	const createValid = $derived(validName(formName));
	const genValid = $derived(genName.trim() !== '');

	async function loadRepos() {
		if (!compartment) {
			repos = [];
			genRepos = [];
			return;
		}
		try {
			[repos, config, genRepos] = await Promise.all([listRepos(compartment), getConfig(compartment), listGenericRepos(compartment)]);
			error = '';
			if (!repos.some((r) => r.id === selectedRepo)) selectedRepo = '';
			if (!genRepos.some((r) => r.id === selectedGen)) selectedGen = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await Promise.all([loadImages(), loadArtifacts()]);
	}

	async function load() {
		try {
			compartments = await listCompartments();
			if (!compartments.includes(compartment)) compartment = compartments[0] ?? '';
		} catch (e) {
			error = describeGcpError(e);
			loading = false;
			return;
		}
		await loadRepos();
		loading = false;
	}

	async function loadImages() {
		detailError = '';
		if (!repo) {
			images = [];
			selectedImage = '';
			return;
		}
		readmeDraft = readmeDraft || repo.readme;
		try {
			images = await listImages(compartment, repo.id, showDeleted);
			if (!images.some((i) => i.id === selectedImage)) selectedImage = '';
		} catch (e) {
			detailError = describeGcpError(e);
		}
		await loadSignatures();
	}

	async function loadSignatures() {
		signatures = [];
		if (!image || image.state === 'DELETED') return;
		try {
			signatures = await listSignatures(compartment, image.id);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	async function loadArtifacts() {
		if (!gen) {
			artifacts = [];
			return;
		}
		try {
			artifacts = await listArtifacts(compartment, gen.id);
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function pickCompartment() {
		writeStoredArtCompartment(compartment);
		selectedRepo = selectedImage = selectedGen = '';
		loading = true;
		void loadRepos();
	}

	function selectRepo(r: ContainerRepo) {
		selectedRepo = selectedRepo === r.id ? '' : r.id;
		selectedImage = '';
		readmeDraft = selectedRepo ? r.readme : '';
		tagFor = '';
		showDeleted = false;
		void loadImages();
	}

	function selectImage(i: ContainerImage) {
		selectedImage = selectedImage === i.id ? '' : i.id;
		tagFor = '';
		void loadSignatures();
	}

	function selectGen(r: GenericRepo) {
		selectedGen = selectedGen === r.id ? '' : r.id;
		void loadArtifacts();
	}

	async function toggle(r: ContainerRepo, key: 'isPublic' | 'isImmutable') {
		const next = !(key === 'isPublic' ? r.isPublic : r.isImmutable);
		const ok = await attempt(() => updateRepo(r.id, { [key]: next }), `${r.name}: ${key === 'isPublic' ? (next ? 'now public' : 'now private') : next ? 'now immutable' : 'now mutable'}`, `Failed to update ${r.name}`);
		if (ok) await loadRepos();
	}

	async function saveReadme(r: ContainerRepo) {
		const ok = await attempt(() => updateRepo(r.id, { readme: readmeDraft }), `${r.name}: README saved`, `Failed to save README`);
		if (ok) await loadRepos();
	}

	async function removeRepo(r: ContainerRepo) {
		const ok = await confirmedAttempt(
			{ title: 'Delete repository', message: `Delete repository "${r.name}" and its ${r.imageCount} image(s)?`, confirmLabel: 'Delete' },
			() => deleteRepo(r.id),
			`Deleted ${r.name}`,
			`Failed to delete ${r.name}`
		);
		if (ok) {
			if (selectedRepo === r.id) selectedRepo = '';
			await loadRepos();
		}
	}

	async function createContainerRepo() {
		if (!createValid) return;
		const ok = await attempt(
			() => createRepo({ compartmentId: compartment, displayName: formName.trim(), isPublic: formPublic, isImmutable: formImmutable, readme: formReadme }),
			`Repository ${formName.trim()} created`,
			'Failed to create repository'
		);
		if (ok) {
			formName = formReadme = '';
			showCreate = false;
			await loadRepos();
		}
	}

	async function removeImage(i: ContainerImage) {
		const ok = await confirmedAttempt(
			{ title: 'Delete image', message: `Delete image ${shortDigest(i.digest)}${imageTags(i).length > 0 ? ` (${imageTags(i).join(', ')})` : ''}? It can be restored afterwards.`, confirmLabel: 'Delete' },
			() => deleteImage(i.id),
			`Deleted ${shortDigest(i.digest)}`,
			'Failed to delete image'
		);
		if (ok) await loadRepos();
	}

	async function restore(i: ContainerImage) {
		const ok = await confirmedAttempt(
			{ title: 'Restore image', message: `Restore image ${shortDigest(i.digest)}?`, confirmLabel: 'Restore' },
			() => restoreImage(i.id),
			`Restored ${shortDigest(i.digest)}`,
			'Failed to restore image'
		);
		if (ok) await loadRepos();
	}

	async function toggleConfig() {
		if (!config) return;
		const next = !config.createdOnFirstPush;
		const ok = await attempt(() => setCreatedOnFirstPush(compartment, next), `Repository creation on first push ${next ? 'enabled' : 'disabled'}`, 'Failed to update configuration');
		if (ok) await loadRepos();
	}

	async function createGeneric() {
		if (!genValid) return;
		const ok = await attempt(
			() => createGenericRepo({ compartmentId: compartment, displayName: genName.trim(), description: genDesc.trim(), isImmutable: genImmutable }),
			`Repository ${genName.trim()} created`,
			'Failed to create repository'
		);
		if (ok) {
			genName = genDesc = '';
			showGenCreate = false;
			await loadRepos();
		}
	}

	async function removeGeneric(r: GenericRepo) {
		const ok = await confirmedAttempt(
			{ title: 'Delete repository', message: `Delete generic repository "${r.name}" and all its artifacts?`, confirmLabel: 'Delete' },
			() => deleteGenericRepo(r.id),
			`Deleted ${r.name}`,
			`Failed to delete ${r.name}`
		);
		if (ok) {
			if (selectedGen === r.id) selectedGen = '';
			await loadRepos();
		}
	}

	async function removeArtifact(a: GenericArtifact) {
		const ok = await confirmedAttempt(
			{ title: 'Delete artifact', message: `Delete artifact ${a.path}:${a.version}?`, confirmLabel: 'Delete' },
			() => deleteArtifact(a.id),
			`Deleted ${a.path}:${a.version}`,
			'Failed to delete artifact'
		);
		if (ok) await loadArtifacts();
	}

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
	const box = 'overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700';
	const body = 'divide-y divide-gray-100 dark:divide-gray-800';
	const badge = (on: boolean) => (on ? 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-700 dark:text-gray-300');
	const when = (t: string) => (t ? new Date(t).toLocaleString() : '—');

	onMount(() => {
		void loadBackendInfo();
		void load();
	});
</script>

<div class="space-y-6 pb-20" data-testid="art-dashboard">
	<PageHeader icon={Package} title="Artifacts" description="Oracle Cloud container registry repositories, images and generic artifacts" color="orange" onRefresh={load} />

	{#if error && compartments.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="art-error">
			<p class="font-medium">Could not load artifacts</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3">
			<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				Compartment
				<select class="{GCP_INPUT} !w-[28rem] !py-1 font-mono text-xs" bind:value={compartment} onchange={pickCompartment} data-testid="art-compartment">
					{#each compartments as c (c)}<option value={c}>{c}</option>{/each}
				</select>
			</label>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="art-list-error">{error}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="orange" />

		{#if tab === 'containers'}
			{#if config}
				<div class="flex flex-wrap items-center gap-4 rounded-lg border border-gray-200 p-3 text-sm dark:border-gray-700" data-testid="art-config">
					<span>Namespace <span class="font-mono text-xs" data-testid="art-namespace">{config.namespace}</span></span>
					<label class="flex items-center gap-2"><input type="checkbox" checked={config.createdOnFirstPush} onchange={toggleConfig} data-testid="art-first-push" /> Create repository on first push</label>
				</div>
			{/if}

			<div class="flex flex-wrap items-center gap-3">
				<button class={GCP_MINI} onclick={() => (showCreate = !showCreate)} aria-expanded={showCreate} data-testid="art-create-toggle">Create repository</button>
			</div>

			{#if showCreate}
				<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-2" onsubmit={(e) => { e.preventDefault(); void createContainerRepo(); }} data-testid="art-create-form">
					<label>Name <input class="{GCP_INPUT} !py-1 font-mono" type="text" placeholder="team/service" bind:value={formName} data-testid="art-create-name" /></label>
					<div class="flex items-end gap-4">
						<label class="flex items-center gap-2"><input type="checkbox" bind:checked={formPublic} data-testid="art-create-public" /> Public</label>
						<label class="flex items-center gap-2"><input type="checkbox" bind:checked={formImmutable} data-testid="art-create-immutable" /> Immutable tags</label>
					</div>
					<label class="md:col-span-2">README (markdown, optional) <textarea class="{GCP_INPUT} !py-1 font-mono text-xs" rows="3" bind:value={formReadme} data-testid="art-create-readme"></textarea></label>
					<div class="md:col-span-2"><button type="submit" class={GCP_PRIMARY} disabled={!createValid} data-testid="art-create-submit">Create</button></div>
				</form>
			{/if}

			<div class={box}>
				<table class="w-full text-sm" data-testid="art-repos">
					<thead class={thead}><tr><th class={cell}>Repository</th><th class={cell}>Visibility</th><th class={cell}>Images</th><th class={cell}>Layers</th><th class={cell}>Size</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class={body}>
						{#each repos as r (r.id)}
							<tr class={selectedRepo === r.id ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="art-repo-row">
								<td class="{cell} font-mono text-xs" title={r.id}><button class="underline decoration-dotted" onclick={() => selectRepo(r)} aria-expanded={selectedRepo === r.id} data-testid="art-repo-select">{r.name}</button></td>
								<td class={cell}>
									<span class="rounded-full px-2 py-0.5 text-xs {badge(r.isPublic)}" data-testid="art-repo-public">{r.isPublic ? 'Public' : 'Private'}</span>
									<span class="ml-1 rounded-full px-2 py-0.5 text-xs {badge(r.isImmutable)}" data-testid="art-repo-immutable">{r.isImmutable ? 'Immutable' : 'Mutable'}</span>
								</td>
								<td class="{cell} text-xs" data-testid="art-repo-images">{r.imageCount}</td>
								<td class="{cell} text-xs" data-testid="art-repo-layers">{r.layerCount}</td>
								<td class="{cell} text-xs" data-testid="art-repo-size">{formatBytes(r.sizeBytes)}</td>
								<td class="{cell} text-xs">{when(r.created)}</td>
								<td class="{cell} text-right whitespace-nowrap">
									<button class={GCP_MINI} onclick={() => toggle(r, 'isPublic')} data-testid="art-repo-toggle-public">{r.isPublic ? 'Make private' : 'Make public'}</button>
									<button class={GCP_MINI} onclick={() => toggle(r, 'isImmutable')} data-testid="art-repo-toggle-immutable">{r.isImmutable ? 'Make mutable' : 'Make immutable'}</button>
									<button class={GCP_MINI} onclick={() => removeRepo(r)} data-testid="art-repo-delete">Delete</button>
								</td>
							</tr>
						{:else}
							<tr><td colspan="7" class="{cell} text-center text-gray-500" data-testid="art-repos-empty">No container repositories in this compartment. Create one above or push an image.</td></tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if repo}
				<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="art-repo-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{repo.name}</h2>
					{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="art-detail-error">{detailError}</p>{/if}
					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
						<dt class="text-gray-500">OCID</dt><dd class="font-mono break-all">{repo.id}</dd>
						<dt class="text-gray-500">Last pushed</dt><dd>{when(repo.lastPushed)}</dd>
					</dl>

					{#if hints}
						<div class="space-y-1 text-xs" data-testid="art-commands">
							<p class="font-medium text-gray-700 dark:text-gray-300">Docker commands</p>
							{#each [['login', hints.login], ['push', hints.push], ['pull', hints.pull]] as [k, cmd] (k)}
								<div class="flex items-center gap-2"><code class="flex-1 rounded bg-gray-100 px-2 py-1 font-mono break-all dark:bg-slate-800" data-testid="art-cmd-{k}">{cmd}</code><button class={GCP_MINI} onclick={() => copyText(cmd)}>Copy</button></div>
							{/each}
						</div>
					{/if}

					<label class="block text-sm">README
						<textarea class="{GCP_INPUT} !py-1 font-mono text-xs" rows="3" bind:value={readmeDraft} data-testid="art-readme"></textarea>
					</label>
					<button class={GCP_MINI} onclick={() => saveReadme(repo)} disabled={readmeDraft === repo.readme} data-testid="art-readme-save">Save README</button>

					<div class="flex items-center gap-4">
						<h3 class="font-medium text-gray-900 dark:text-white">Images</h3>
						<label class="flex items-center gap-2 text-xs"><input type="checkbox" bind:checked={showDeleted} onchange={() => { selectedImage = ''; void loadImages(); }} data-testid="art-show-deleted" /> Show deleted</label>
					</div>
					<div class={box}>
						<table class="w-full text-sm" data-testid="art-images">
							<thead class={thead}><tr><th class={cell}>Version</th><th class={cell}>Digest</th><th class={cell}>Size</th><th class={cell}>Layers</th><th class={cell}>Created</th><th class={cell}>Pulls</th><th class={cell}>Last pulled</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class={body}>
								{#each images as i (i.id)}
									<tr class={selectedImage === i.id ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="art-image-row">
										<td class="{cell} font-mono text-xs" data-testid="art-image-versions">{imageTags(i).join(', ') || 'untagged'}</td>
										<td class="{cell} font-mono text-xs" title={i.digest}><button class="underline decoration-dotted" onclick={() => selectImage(i)} aria-expanded={selectedImage === i.id} data-testid="art-image-select">{shortDigest(i.digest)}</button></td>
										<td class="{cell} text-xs" data-testid="art-image-size">{formatBytes(i.sizeBytes)}</td>
										<td class="{cell} text-xs" data-testid="art-image-layers">{i.layers.length}</td>
										<td class="{cell} text-xs">{when(i.created)}</td>
										<td class="{cell} text-xs" data-testid="art-image-pulls">{i.pullCount}</td>
										<td class="{cell} text-xs">{when(i.lastPulled)}</td>
										<td class="{cell} text-right whitespace-nowrap">
											{#if i.state === 'DELETED'}
												<button class={GCP_MINI} onclick={() => restore(i)} data-testid="art-image-restore">Restore</button>
											{:else}
												<button class={GCP_MINI} onclick={() => removeImage(i)} data-testid="art-image-delete">Delete</button>
											{/if}
										</td>
									</tr>
								{:else}
									<tr><td colspan="8" class="{cell} text-center text-gray-500" data-testid="art-images-empty">{showDeleted ? 'No deleted images' : 'No images pushed yet'}</td></tr>
								{/each}
							</tbody>
						</table>
					</div>

					{#if image}
						<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="art-image-detail">
							<h3 class="font-medium text-gray-900 dark:text-white">Image <span class="font-mono text-xs break-all" data-testid="art-image-digest">{image.digest}</span></h3>
							<div class="flex flex-wrap gap-2 text-xs">
								{#each imageTags(image) as t (t)}
									<button class={GCP_MINI} onclick={() => (tagFor = t)} title="Use in the docker commands" data-testid="art-image-tag">{t}</button>
								{/each}
							</div>
							<div class={box}>
								<table class="w-full text-sm" data-testid="art-layers">
									<thead class={thead}><tr><th class={cell}>Layer digest</th><th class={cell}>Size</th></tr></thead>
									<tbody class={body}>
										{#each image.layers as l (l.digest)}
											<tr data-testid="art-layer-row"><td class="{cell} font-mono text-xs break-all">{l.digest}</td><td class="{cell} text-xs">{formatBytes(l.sizeBytes)}</td></tr>
										{:else}
											<tr><td colspan="2" class="{cell} text-center text-gray-500">No layers</td></tr>
										{/each}
									</tbody>
								</table>
							</div>
							<h4 class="text-sm font-medium text-gray-900 dark:text-white">Signatures</h4>
							<div class={box}>
								<table class="w-full text-sm" data-testid="art-signatures">
									<thead class={thead}><tr><th class={cell}>Key</th><th class={cell}>Algorithm</th><th class={cell}>Description</th><th class={cell}>Created</th></tr></thead>
									<tbody class={body}>
										{#each signatures as s (s.id)}
											<tr data-testid="art-signature-row">
												<td class="{cell} font-mono text-xs break-all" data-testid="art-signature-key">{s.keyId}</td>
												<td class="{cell} text-xs">{s.algorithm}</td>
												<td class="{cell} font-mono text-xs break-all">{s.description}</td>
												<td class="{cell} text-xs">{when(s.created)}</td>
											</tr>
										{:else}
											<tr><td colspan="4" class="{cell} text-center text-gray-500" data-testid="art-signatures-empty">No signatures</td></tr>
										{/each}
									</tbody>
								</table>
							</div>
						</div>
					{/if}
				</div>
			{/if}
		{:else}
			<div class="flex flex-wrap items-center gap-3">
				<button class={GCP_MINI} onclick={() => (showGenCreate = !showGenCreate)} aria-expanded={showGenCreate} data-testid="art-gen-create-toggle">Create generic repository</button>
			</div>

			{#if showGenCreate}
				<form class="grid gap-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700 md:grid-cols-2" onsubmit={(e) => { e.preventDefault(); void createGeneric(); }} data-testid="art-gen-create-form">
					<label>Name <input class="{GCP_INPUT} !py-1" type="text" bind:value={genName} data-testid="art-gen-create-name" /></label>
					<label>Description <input class="{GCP_INPUT} !py-1" type="text" bind:value={genDesc} data-testid="art-gen-create-description" /></label>
					<label class="flex items-center gap-2"><input type="checkbox" bind:checked={genImmutable} data-testid="art-gen-create-immutable" /> Immutable (artifacts cannot be overwritten)</label>
					<div class="md:col-span-2"><button type="submit" class={GCP_PRIMARY} disabled={!genValid} data-testid="art-gen-create-submit">Create</button></div>
				</form>
			{/if}

			<div class={box}>
				<table class="w-full text-sm" data-testid="art-gen-repos">
					<thead class={thead}><tr><th class={cell}>Repository</th><th class={cell}>Description</th><th class={cell}>Immutable</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class={body}>
						{#each genRepos as r (r.id)}
							<tr class={selectedGen === r.id ? 'bg-orange-50/60 dark:bg-orange-950/20' : ''} data-testid="art-gen-row">
								<td class="{cell} font-mono text-xs" title={r.id}><button class="underline decoration-dotted" onclick={() => selectGen(r)} aria-expanded={selectedGen === r.id} data-testid="art-gen-select">{r.name}</button></td>
								<td class="{cell} text-xs">{r.description || '—'}</td>
								<td class="{cell} text-xs">{r.isImmutable ? 'Yes' : 'No'}</td>
								<td class="{cell} text-xs">{when(r.created)}</td>
								<td class="{cell} text-right whitespace-nowrap"><button class={GCP_MINI} onclick={() => removeGeneric(r)} data-testid="art-gen-delete">Delete</button></td>
							</tr>
						{:else}
							<tr><td colspan="5" class="{cell} text-center text-gray-500" data-testid="art-gen-empty">No generic repositories in this compartment</td></tr>
						{/each}
					</tbody>
				</table>
			</div>

			{#if gen}
				<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="art-gen-detail">
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{gen.name}</h2>
					{#if detailError}<p class="text-sm text-red-600" role="alert">{detailError}</p>{/if}
					<p class="text-xs text-gray-500">Upload with the OCI SDK or CLI. Downloads here are attachments up to {ART_DOWNLOAD_LIMIT_MIB} MiB.</p>
					<div class={box}>
						<table class="w-full text-sm" data-testid="art-artifacts">
							<thead class={thead}><tr><th class={cell}>Path</th><th class={cell}>Version</th><th class={cell}>Size</th><th class={cell}>SHA-256</th><th class={cell}>State</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class={body}>
								{#each artifacts as a (a.id)}
									<tr data-testid="art-artifact-row">
										<td class="{cell} font-mono text-xs break-all" data-testid="art-artifact-path">{a.path}</td>
										<td class="{cell} text-xs" data-testid="art-artifact-version">{a.version}</td>
										<td class="{cell} text-xs" data-testid="art-artifact-size">{formatBytes(a.sizeBytes)}</td>
										<td class="{cell} font-mono text-xs break-all" data-testid="art-artifact-sha">{a.sha256}</td>
										<td class="{cell} text-xs">{a.state}</td>
										<td class="{cell} text-right whitespace-nowrap">
											<a class={GCP_MINI} href={artifactDownloadUrl(a.id)} download data-testid="art-artifact-download">Download</a>
											<button class={GCP_MINI} onclick={() => removeArtifact(a)} data-testid="art-artifact-delete">Delete</button>
										</td>
									</tr>
								{:else}
									<tr><td colspan="6" class="{cell} text-center text-gray-500" data-testid="art-artifacts-empty">No artifacts in this repository</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
				</div>
			{/if}
		{/if}
	{/if}
</div>
