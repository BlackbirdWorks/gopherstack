<script lang="ts">
	import { GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { parseCleanupPolicies, parseUpstream, previewCleanup, type CleanupPreview, type RepoInfo } from '#lib/gcpartifactregistry.ts';

	type Props = { repo: RepoInfo };
	let { repo }: Props = $props();

	let preview = $state<CleanupPreview | null>(null);
	let previewError = $state('');
	let running = $state(false);

	const upstream = $derived(parseUpstream(repo.raw));
	const policies = $derived(parseCleanupPolicies(repo.raw));
	const labels = $derived(Object.entries(repo.labels));

	$effect(() => {
		void repo.name;
		preview = null;
		previewError = '';
	});

	async function runPreview() {
		running = true;
		try {
			preview = await previewCleanup(repo.name);
			previewError = '';
		} catch (e) {
			previewError = describeGcpError(e);
		} finally {
			running = false;
		}
	}
</script>

<div class="space-y-4 text-sm" data-testid="artifactregistry-detail">
	<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 text-xs">
		<dt class="text-gray-500">Name</dt><dd class="font-mono break-all">{repo.name}</dd>
		<dt class="text-gray-500">Format</dt><dd>{repo.format}</dd>
		<dt class="text-gray-500">Mode</dt><dd>{repo.mode}</dd>
		<dt class="text-gray-500">Created</dt><dd>{repo.createTime ? new Date(repo.createTime).toLocaleString() : '—'}</dd>
		{#if labels.length}<dt class="text-gray-500">Labels</dt><dd class="font-mono">{labels.map(([k, v]) => `${k}=${v}`).join(', ')}</dd>{/if}
	</dl>

	{#if upstream.kind !== 'none'}
		<section data-testid="artifactregistry-upstream">
			<h3 class="font-medium text-gray-700 dark:text-gray-300">{upstream.kind === 'remote' ? 'Remote upstream' : 'Virtual upstream policies'}</h3>
			{#if upstream.lines.length === 0}
				<p class="text-xs text-gray-500">No upstream configured.</p>
			{:else}
				<ul class="list-disc pl-5 text-xs font-mono">{#each upstream.lines as line (line)}<li>{line}</li>{/each}</ul>
			{/if}
		</section>
	{/if}

	<section data-testid="artifactregistry-cleanup">
		<h3 class="font-medium text-gray-700 dark:text-gray-300">Cleanup policies{repo.raw.cleanupPolicyDryRun === true ? ' (dry run)' : ''}</h3>
		{#if policies.length === 0}
			<p class="text-xs text-gray-500" data-testid="artifactregistry-cleanup-none">No cleanup policies.</p>
		{:else}
			<ul class="space-y-1 text-xs" data-testid="artifactregistry-cleanup-list">
				{#each policies as p (p.id)}<li><span class="mr-2 rounded bg-gray-100 px-1.5 py-0.5 font-mono dark:bg-slate-800">{p.action}</span><span class="font-mono">{p.id}</span>: {p.summary}</li>{/each}
			</ul>
			<button class="{GCP_MINI} mt-2" onclick={runPreview} disabled={running} data-testid="artifactregistry-cleanup-preview">Preview deletions</button>
			{#if previewError}
				<p class="mt-1 text-xs text-red-600" role="alert">{previewError}</p>
			{:else if preview}
				<div class="mt-2 text-xs" data-testid="artifactregistry-cleanup-result">
					<p>{preview.versions.length} {preview.versions.length === 1 ? 'version' : 'versions'} would be deleted. Nothing is deleted by this preview.</p>
					<ul class="list-disc pl-5 font-mono break-all">{#each preview.versions as v (v)}<li>{v}</li>{/each}</ul>
				</div>
			{/if}
		{/if}
	</section>
</div>
