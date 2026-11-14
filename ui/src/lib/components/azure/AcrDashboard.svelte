<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Package, Trash2, Inbox } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import RevealKeys from '#lib/components/azure/RevealKeys.svelte';
	import { confirmedAttempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		deleteTag,
		formatSize,
		listManifests,
		listRegistries,
		listRepositories,
		listTags,
		platform,
		revealCredentials,
		shortDigest,
		type AcrManifest,
		type AcrRegistry,
		type AcrRepository,
		type AcrTag
	} from '#lib/azureacr.ts';

	let registries = $state<AcrRegistry[]>([]);
	let registry = $state('');
	let repos = $state<AcrRepository[]>([]);
	let repo = $state('');
	let tags = $state<AcrTag[]>([]);
	let manifests = $state<AcrManifest[]>([]);
	let view = $state<'tags' | 'manifests'>('tags');
	let loading = $state(true);
	let loadError = $state('');

	const reg = $derived(registries.find((r) => r.name === registry));

	async function loadRepo() {
		if (!registry || !repo) {
			tags = [];
			manifests = [];
			return;
		}
		try {
			[tags, manifests] = await Promise.all([listTags(registry, repo), listManifests(registry, repo)]);
		} catch (e) {
			tags = [];
			manifests = [];
			toast.error(`Failed to load ${repo}: ${describeGcpError(e)}`);
		}
	}

	async function loadRepos() {
		if (!registry) {
			repos = [];
			return;
		}
		repos = await listRepositories(registry);
		if (!repos.some((r) => r.name === repo)) repo = repos[0]?.name ?? '';
		await loadRepo();
	}

	async function load() {
		loading = true;
		loadError = '';
		try {
			registries = await listRegistries();
			if (!registries.some((r) => r.name === registry)) registry = registries[0]?.name ?? '';
			await loadRepos();
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Container Registry: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function pickRegistry(name: string) {
		registry = name;
		repo = '';
		await loadRepos();
	}

	async function pickRepo(name: string) {
		repo = name;
		await loadRepo();
	}

	async function remove(t: AcrTag) {
		const ok = await confirmedAttempt(
			{ title: 'Delete tag', message: `Delete tag "${t.name}" from ${repo}? The image is removed once no tag references it.` },
			() => deleteTag(registry, repo, t.name),
			`Deleted ${repo}:${t.name}`,
			`Failed to delete ${t.name}`
		);
		if (ok) await loadRepos();
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="acr-dashboard">
	<PageHeader icon={Package} title="Container Registry" description="Azure Container Registry: repositories, tags, manifests and admin credentials" color="sky" onRefresh={load} />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="acr-error">
			<p class="font-medium">Could not reach Azure Container Registry</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if registries.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="acr-empty">
			<Inbox class="mb-3 h-10 w-10 opacity-30" />
			<p>No registries. Create one with the Azure CLI, Terraform or ARM.</p>
		</div>
	{:else}
		<div class="flex flex-wrap items-end gap-4">
			<label class="text-sm">Registry
				<select class="{GCP_INPUT} !w-64" value={registry} onchange={(e) => pickRegistry(e.currentTarget.value)} data-testid="acr-registry">
					{#each registries as r (r.name)}<option value={r.name}>{r.name}</option>{/each}
				</select>
			</label>
			{#if reg}<p class="font-mono text-xs text-gray-500" data-testid="acr-login-server">{reg.loginServer} - {reg.repositoryCount} repositories</p>{/if}
		</div>

		{#if reg}
			<section class="space-y-2" data-testid="acr-credentials">
				<h2 class="text-lg font-semibold">Admin credentials</h2>
				{#if reg.adminEnabled}
					{#key reg.name}
						<RevealKeys testid="acr-creds" fetchKeys={async () => {
							const c = await revealCredentials(reg.name);
							return [{ label: 'password', value: c.password }, { label: 'password2', value: c.password2 }, { label: 'Username', value: c.username }];
						}} />
					{/key}
				{:else}
					<p class="text-sm text-gray-500" data-testid="acr-admin-disabled">The admin user is not enabled on this registry.</p>
				{/if}
			</section>
		{/if}

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="acr-repos">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Repository</th><th class="px-4 py-2">Tags</th><th class="px-4 py-2">Manifests</th><th class="px-4 py-2">Updated</th></tr></thead>
				<tbody>
					{#each repos as r (r.name)}
						<tr class="border-t border-gray-100 dark:border-gray-800 {r.name === repo ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''}" data-testid="acr-repo">
							<td class="px-4 py-2 font-mono"><button class="underline decoration-dotted" onclick={() => pickRepo(r.name)} data-testid="acr-repo-select">{r.name}</button></td>
							<td class="px-4 py-2">{r.tagCount}</td>
							<td class="px-4 py-2">{r.manifestCount}</td>
							<td class="px-4 py-2 text-xs">{r.updated}</td>
						</tr>
					{:else}
						<tr><td class="px-4 py-6 text-center text-gray-500" colspan="4" data-testid="acr-repos-empty">No repositories. Push an image to create one.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if repo}
			<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700" role="tablist">
				<button role="tab" aria-selected={view === 'tags'} class="px-3 py-2 text-sm {view === 'tags' ? 'border-b-2 border-blue-600 font-medium text-blue-600' : 'text-gray-500'}" onclick={() => (view = 'tags')} data-testid="acr-tab-tags">Tags</button>
				<button role="tab" aria-selected={view === 'manifests'} class="px-3 py-2 text-sm {view === 'manifests' ? 'border-b-2 border-blue-600 font-medium text-blue-600' : 'text-gray-500'}" onclick={() => (view = 'manifests')} data-testid="acr-tab-manifests">Manifests</button>
			</div>

			{#if view === 'tags'}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="acr-tags">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Tag</th><th class="px-4 py-2">Digest</th><th class="px-4 py-2">Platform</th><th class="px-4 py-2">Size</th><th class="px-4 py-2">Created</th><th class="px-4 py-2"></th></tr></thead>
						<tbody>
							{#each tags as t (t.name)}
								<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="acr-tag">
									<td class="px-4 py-2 font-mono">{t.name}</td>
									<td class="px-4 py-2 font-mono text-xs" title={t.digest}>{shortDigest(t.digest)}</td>
									<td class="px-4 py-2">{platform(t)}</td>
									<td class="px-4 py-2">{formatSize(t.sizeBytes)}</td>
									<td class="px-4 py-2 text-xs">{t.created}</td>
									<td class="px-4 py-2 text-right"><button class={GCP_MINI} onclick={() => remove(t)} aria-label="Delete tag {t.name}" data-testid="acr-tag-delete"><Trash2 class="inline h-3 w-3" /></button></td>
								</tr>
							{:else}
								<tr><td class="px-4 py-6 text-center text-gray-500" colspan="6">No tags.</td></tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="acr-manifests">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Digest</th><th class="px-4 py-2">Tags</th><th class="px-4 py-2">Platform</th><th class="px-4 py-2">Size</th><th class="px-4 py-2">Media type</th><th class="px-4 py-2">Created</th></tr></thead>
						<tbody>
							{#each manifests as m (m.digest)}
								<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="acr-manifest">
									<td class="px-4 py-2 font-mono text-xs" title={m.digest}>{shortDigest(m.digest)}</td>
									<td class="px-4 py-2 font-mono">{m.tags.join(', ') || '(untagged)'}</td>
									<td class="px-4 py-2">{platform(m)}</td>
									<td class="px-4 py-2">{formatSize(m.sizeBytes)}</td>
									<td class="px-4 py-2 text-xs">{m.mediaType}</td>
									<td class="px-4 py-2 text-xs">{m.created}</td>
								</tr>
							{:else}
								<tr><td class="px-4 py-6 text-center text-gray-500" colspan="6">No manifests.</td></tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{/if}
	{/if}
</div>
