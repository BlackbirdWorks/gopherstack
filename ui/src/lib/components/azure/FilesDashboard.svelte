<script lang="ts">
	import { onMount } from 'svelte';
	import { Download, File as FileIcon, Folder, FolderOpen, RefreshCw, Trash2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { formatBytes } from '#lib/format.ts';
	import { attempt, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		deleteFile,
		describeFileError,
		fileDownloadUrl,
		listAccounts,
		listDirectory,
		listShares,
		readStoredAccount,
		writeStoredAccount,
		type DirEntry,
		type ShareInfo
	} from '#lib/azurefile.ts';

	const TABS = [
		{ id: 'browse', label: 'Browse' },
		{ id: 'snapshots', label: 'Snapshots' }
	];

	let accounts = $state<string[]>([]);
	let account = $state(readStoredAccount());
	let shares = $state<ShareInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let share = $state('');
	let snapshot = $state('');
	let path = $state('');
	let tab = $state('browse');
	let entries = $state<DirEntry[]>([]);
	let childLoading = $state(false);
	let childError = $state('');

	const liveShares = $derived(shares.filter((s) => !s.snapshot));
	const snapshots = $derived(shares.filter((s) => s.snapshot));
	const crumbs = $derived(path.split('/').filter(Boolean).map((label, i, all) => ({ label, path: all.slice(0, i + 1).join('/') })));

	async function load() {
		try {
			accounts = await listAccounts();
			if (!accounts.includes(account)) account = accounts[0] ?? '';
			shares = account ? await listShares(account, true) : [];
			error = '';
			if (!liveShares.some((s) => s.name === share)) share = '';
		} catch (e) {
			error = describeFileError(e);
		} finally {
			loading = false;
		}
	}

	async function loadDir() {
		if (!share) return;
		childLoading = true;
		childError = '';
		try {
			entries = (await listDirectory(account, share, path, { snapshot })).entries.toSorted((a, b) =>
				a.kind === b.kind ? a.name.localeCompare(b.name) : a.kind === 'directory' ? -1 : 1
			);
		} catch (e) {
			childError = describeFileError(e);
			entries = [];
		} finally {
			childLoading = false;
		}
	}

	function switchAccount(name: string) {
		account = name;
		writeStoredAccount(name);
		share = '';
		loading = true;
		void load();
	}

	function openShare(name: string, snap = '') {
		share = name;
		snapshot = snap;
		path = '';
		tab = 'browse';
		void loadDir();
	}

	function openDir(p: string) {
		path = p;
		void loadDir();
	}

	async function removeFile(name: string) {
		const full = path ? `${path}/${name}` : name;
		if (!(await confirmDestructive({ title: 'Delete file', message: `Delete file "${full}"?` }))) return;
		if (await attempt(() => deleteFile(account, share, full), `Deleted ${name}`, `Failed to delete ${name}`)) await loadDir();
	}

	const cell = 'px-4 py-2';
	const child = (name: string) => (path ? `${path}/${name}` : name);

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="files-dashboard">
	<PageHeader icon={FolderOpen} title="Azure Files" description="Azure Files: accounts, shares, directories, file downloads and share snapshots" color="blue" onRefresh={load} />

	{#if error && accounts.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="files-error">
			<p class="font-medium">Could not load Azure Files</p><p>{error}</p><button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if accounts.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="files-empty">No file shares yet. Create one with the Azure Files SDK or REST API.</p>
	{:else}
		<div class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			<label for="files-account">Account</label>
			<select id="files-account" class="{GCP_INPUT} !w-56 font-mono text-xs" value={account} onchange={(e) => switchAccount(e.currentTarget.value)} data-testid="files-account">
				{#each accounts as a (a)}<option value={a}>{a}</option>{/each}
			</select>
		</div>
		<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="files-shares">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Share</th><th class={cell}>Quota</th><th class={cell}>Tier</th><th class={cell}>Modified</th><th class={cell}>Snapshots</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each liveShares as s (s.name)}
						<tr class={share === s.name && !snapshot ? 'bg-blue-50/60 dark:bg-blue-950/20' : ''} data-testid="files-share">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => openShare(s.name)} data-testid="files-share-open">{s.name}</button></td>
							<td class="{cell} text-xs">{s.quotaGb} GiB</td><td class="{cell} text-xs">{s.accessTier || '—'}</td><td class="{cell} text-xs">{s.lastModified}</td>
							<td class="{cell} text-xs">{snapshots.filter((x) => x.name === s.name).length}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if share}
			<div class="space-y-3" data-testid="files-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{share}{snapshot ? ` @ ${snapshot}` : ''}</h2>
				<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />
				{#if tab === 'browse'}
					<nav class="flex flex-wrap items-center gap-1 text-sm" aria-label="Directory" data-testid="files-crumbs">
						<button class="underline decoration-dotted" onclick={() => openDir('')}>{share}</button>
						{#each crumbs as c (c.path)}<span>/</span><button class="underline decoration-dotted" onclick={() => openDir(c.path)}>{c.label}</button>{/each}
					</nav>
					{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="files-detail-error">{childError}</p>{/if}
					{#if childLoading}
						<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
					{:else if entries.length === 0}
						<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="files-dir-empty">This directory is empty</p>
					{:else}
						<table class="w-full text-sm" data-testid="files-entries">
							<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Name</th><th class={cell}>Size</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each entries as e (`${e.kind}:${e.name}`)}
									<tr data-testid="files-entry">
										<td class="{cell} font-mono text-xs">
											{#if e.kind === 'directory'}
												<button class="inline-flex items-center gap-1 underline decoration-dotted" onclick={() => openDir(child(e.name))} data-testid="files-open-dir"><Folder class="h-3.5 w-3.5" />{e.name}</button>
											{:else}<span class="inline-flex items-center gap-1"><FileIcon class="h-3.5 w-3.5" />{e.name}</span>{/if}
										</td>
										<td class="{cell} text-xs">{e.kind === 'file' ? formatBytes(e.size) : '—'}</td>
										<td class="{cell} text-right whitespace-nowrap space-x-1">
											{#if e.kind === 'file'}
												<a class="{GCP_MINI} inline-block" href={fileDownloadUrl(account, share, child(e.name), snapshot)} download={e.name} data-testid="files-download"><Download class="inline h-3.5 w-3.5" /> Download</a>
												{#if !snapshot}<button class="{GCP_MINI} text-red-600" onclick={() => removeFile(e.name)} aria-label="Delete {e.name}" data-testid="files-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>{/if}
											{/if}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					{/if}
				{:else if snapshots.filter((s) => s.name === share).length === 0}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="files-snapshots-empty">No snapshots of this share</p>
				{:else}
					<table class="w-full text-sm" data-testid="files-snapshots">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Snapshot</th><th class={cell}>Modified</th><th class="{cell} text-right">Browse</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each snapshots.filter((s) => s.name === share) as s (s.snapshot)}
								<tr data-testid="files-snapshot"><td class="{cell} font-mono text-xs">{s.snapshot}</td><td class="{cell} text-xs">{s.lastModified}</td><td class="{cell} text-right"><button class={GCP_MINI} onclick={() => openShare(s.name, s.snapshot)} data-testid="files-snapshot-open">Browse snapshot</button></td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
			</div>
		{/if}
	{/if}
</div>
