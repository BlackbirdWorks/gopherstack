<script lang="ts">
	import { onMount } from 'svelte';
	import { ShieldCheck, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import IamAccounts from '#lib/components/gcp/IamAccounts.svelte';
	import IamKeys from '#lib/components/gcp/IamKeys.svelte';
	import IamRoles from '#lib/components/gcp/IamRoles.svelte';
	import IamSign from '#lib/components/gcp/IamSign.svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		listAccounts,
		readStoredIamProject,
		writeStoredIamProject,
		type AccountInfo
	} from '#lib/gcpiam.ts';

	const TABS = [
		{ id: 'accounts', label: 'Service accounts' },
		{ id: 'keys', label: 'Keys' },
		{ id: 'roles', label: 'Roles' },
		{ id: 'sign', label: 'Sign blob' }
	];

	let project = $state(readStoredIamProject());
	let projectInput = $state(readStoredIamProject());
	let tab = $state('accounts');
	let accounts = $state<AccountInfo[]>([]);
	let selected = $state('');
	let loading = $state(true);
	let loadError = $state('');

	async function load() {
		try {
			accounts = (await listAccounts(project)).toSorted((a, b) => a.email.localeCompare(b.email));
			if (!accounts.some((a) => a.email === selected)) selected = accounts[0]?.email ?? '';
			loadError = '';
		} catch (e) {
			loadError = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function applyProject(event: Event) {
		event.preventDefault();
		const p = projectInput.trim();
		if (!p) return;
		project = p;
		writeStoredIamProject(p);
		accounts = [];
		selected = '';
		loading = true;
		void load();
	}

	function openKeys(email: string) {
		selected = email;
		tab = 'keys';
	}

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="iam-dashboard">
	<PageHeader icon={ShieldCheck} title="Cloud IAM" description="Google Cloud IAM: service accounts, keys, and roles" color="blue" onRefresh={load} />

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyProject}>
		<label for="iam-project">Project</label>
		<input id="iam-project" class="w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="iam-project" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || projectInput.trim() === project}>Switch</button>
		<span>Service accounts and custom roles are scoped to a project.</span>
	</form>

	{#if loadError && accounts.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="iam-error">
			<p class="font-medium">Could not reach Cloud IAM</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />
		<div role="tabpanel" id="tabpanel-{tab}" aria-labelledby="tab-{tab}">
			{#if tab === 'accounts'}
				<IamAccounts {project} {accounts} onChanged={load} onOpenKeys={openKeys} />
			{:else if tab === 'keys'}
				<IamKeys {project} {accounts} bind:selected />
			{:else if tab === 'roles'}
				<IamRoles {project} />
			{:else}
				<IamSign {project} {accounts} bind:selected />
			{/if}
		</div>
	{/if}
</div>
