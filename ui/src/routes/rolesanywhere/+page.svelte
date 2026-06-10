<script lang="ts">
	import { onMount } from 'svelte';
	import { getRolesAnywhereClient } from '$lib/aws-client';
	import {
		ListCrlsCommand,
		ListProfilesCommand,
		ListSubjectsCommand,
		ListTrustAnchorsCommand,
		type CrlDetail,
		type ProfileDetail,
		type SubjectSummary,
		type TrustAnchorDetail
	} from '@aws-sdk/client-rolesanywhere';
	import { toast } from 'svelte-sonner';
	import { KeyRound, RefreshCw, Search } from 'lucide-svelte';

	const client = getRolesAnywhereClient();

	let loading = $state(false);
	let activeTab = $state<'profiles' | 'anchors' | 'subjects' | 'crls'>('profiles');
	let searchQuery = $state('');
	let profilesData = $state<ProfileDetail[]>([]);
	let anchorsData = $state<TrustAnchorDetail[]>([]);
	let subjectsData = $state<SubjectSummary[]>([]);
	let crlsData = $state<CrlDetail[]>([]);

	const filteredProfiles = $derived(profilesData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredAnchors = $derived(anchorsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredSubjects = $derived(subjectsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredCrls = $derived(crlsData.filter((x) => JSON.stringify(x).toLowerCase().includes(searchQuery.toLowerCase())));

	async function loadData() {
		loading = true;
		try {
			if (activeTab === 'profiles') {
				const resp = await client.send(new ListProfilesCommand({}));
				profilesData = resp.profiles ?? [];
			}
			if (activeTab === 'anchors') {
				const resp = await client.send(new ListTrustAnchorsCommand({}));
				anchorsData = resp.trustAnchors ?? [];
			}
			if (activeTab === 'subjects') {
				const resp = await client.send(new ListSubjectsCommand({}));
				subjectsData = resp.subjects ?? [];
			}
			if (activeTab === 'crls') {
				const resp = await client.send(new ListCrlsCommand({}));
				crlsData = resp.crls ?? [];
			}
		} catch (e) {
			toast.error('Failed to load IAM Roles Anywhere data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		loadData();
	}

	onMount(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between flex-wrap gap-3">
		<div class="flex items-center gap-3">
			<KeyRound class="w-7 h-7 text-emerald-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">IAM Roles Anywhere</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">X.509-based access for non-AWS workloads</p>
			</div>
		</div>
		<div class="flex items-center gap-2 flex-wrap">
			<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['profiles', 'Profiles'], ['anchors', 'Trust Anchors'], ['subjects', 'Subjects'], ['crls', 'CRLs']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-emerald-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
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
			{:else if activeTab === 'profiles'}
				{#if filteredProfiles.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No profiles found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredProfiles as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<KeyRound class="w-5 h-5 text-emerald-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.profileArn ?? a.profileId ?? ''}`}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full shrink-0 {a.enabled ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{a.enabled ? 'Enabled' : 'Disabled'}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'anchors'}
				{#if filteredAnchors.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No trust anchors found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredAnchors as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<KeyRound class="w-5 h-5 text-emerald-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.trustAnchorArn ?? ''}`}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full shrink-0 {a.enabled ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{a.enabled ? 'Enabled' : 'Disabled'}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'subjects'}
				{#if filteredSubjects.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No subjects found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredSubjects as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<KeyRound class="w-5 h-5 text-emerald-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.x509Subject ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.subjectArn ?? a.subjectId ?? ''}`}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full shrink-0 {a.enabled ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{a.enabled ? 'Enabled' : 'Disabled'}</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'crls'}
				{#if filteredCrls.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No crls found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredCrls as a}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3 min-w-0">
									<KeyRound class="w-5 h-5 text-emerald-500 shrink-0" />
									<div class="min-w-0">
										<p class="font-medium text-gray-900 dark:text-white truncate">{a.name ?? '(unnamed)'}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{`${a.crlArn ?? a.crlId ?? ''}`}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full shrink-0 {a.enabled ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400' : 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{a.enabled ? 'Enabled' : 'Disabled'}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>
