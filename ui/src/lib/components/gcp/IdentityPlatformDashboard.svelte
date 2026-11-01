<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw, UserRound } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import IdentityPlatformUsers from '#lib/components/gcp/IdentityPlatformUsers.svelte';
	import { attempt, copyText, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		getConfig,
		listOobCodes,
		listProjects,
		listTenants,
		readStoredProject,
		setSignIn,
		writeStoredProject,
		type IdpConfig,
		type IdpOobCode,
		type IdpTenant
	} from '#lib/gcpidentitytoolkit.ts';

	const TABS = [
		{ id: 'users', label: 'Users' },
		{ id: 'codes', label: 'Action codes' },
		{ id: 'tenants', label: 'Tenants' },
		{ id: 'settings', label: 'Sign-in methods' }
	];

	let projects = $state<string[]>([]);
	let project = $state('');
	let tenant = $state('');
	let tab = $state('users');
	let tenants = $state<IdpTenant[]>([]);
	let codes = $state<IdpOobCode[]>([]);
	let config = $state<IdpConfig | null>(null);
	let loading = $state(true);
	let error = $state('');
	let refreshKey = $state(0);

	async function boot() {
		try {
			projects = await listProjects();
			const stored = readStoredProject();
			project = projects.includes(stored) ? stored : (projects[0] ?? '');
			error = '';
			await loadScope();
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadScope() {
		if (!project) return;
		try {
			[tenants, config, codes] = await Promise.all([listTenants(project), getConfig(project), listOobCodes(project, tenant)]);
			error = '';
			if (tenant && !tenants.some((t) => t.id === tenant)) tenant = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	async function refresh() {
		refreshKey += 1;
		await loadScope();
	}

	async function pickProject(p: string) {
		project = p;
		tenant = '';
		writeStoredProject(p);
		await refresh();
	}

	async function pickTenant(t: string) {
		tenant = t;
		await refresh();
	}

	async function toggle(which: 'emailEnabled' | 'anonymousEnabled', value: boolean) {
		const ok = await attempt(async () => { config = await setSignIn(project, { [which]: value }); }, 'Sign-in settings saved', 'Failed to save sign-in settings');
		if (!ok) await loadScope();
	}

	const cell = 'px-4 py-3';
	onMount(boot);
</script>

<div class="space-y-6 pb-20" data-testid="idp-dashboard">
	<PageHeader icon={UserRound} title="Identity Platform" description="Users, tenants, action codes and sign-in methods" color="blue" onRefresh={refresh} />

	{#if error && !project}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="idp-error">
			<p class="font-medium">Could not load Identity Platform</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={boot}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else}
		<div class="flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
			<label for="idp-project">Project</label>
			<select id="idp-project" class="{GCP_INPUT} !w-56 !py-1 font-mono text-xs" value={project} onchange={(e) => pickProject(e.currentTarget.value)} data-testid="idp-project">
				{#each projects as p (p)}<option value={p}>{p}</option>{/each}
			</select>
			<label for="idp-tenant">Tenant</label>
			<select id="idp-tenant" class="{GCP_INPUT} !w-56 !py-1 font-mono text-xs" value={tenant} onchange={(e) => pickTenant(e.currentTarget.value)} data-testid="idp-tenant">
				<option value="">(project level)</option>
				{#each tenants as t (t.id)}<option value={t.id}>{t.displayName || t.id} ({t.id})</option>{/each}
			</select>
		</div>

		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="idp-scope-error">{error}</p>{/if}

		<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="blue" />

		{#if tab === 'users'}
			<IdentityPlatformUsers {project} {tenant} {refreshKey} />
		{:else if tab === 'codes'}
			<div class="space-y-2" data-testid="idp-codes">
				<p class="text-xs text-gray-500">No email is sent: password-reset, verification and sign-in links are kept here for you to open.</p>
				{#if codes.length === 0}
					<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="idp-codes-empty">No stored action codes.</p>
				{:else}
					<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
						<table class="w-full text-sm">
							<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Type</th><th class={cell}>Email</th><th class={cell}>Link</th><th class="{cell} text-right">Actions</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each codes as c (c.oobCode)}
									<tr data-testid="idp-code">
										<td class="{cell} text-xs" data-testid="idp-code-type">{c.requestType}</td>
										<td class="{cell} text-xs">{c.email}</td>
										<td class="{cell} font-mono text-xs break-all" data-testid="idp-code-link">{c.oobLink}</td>
										<td class="{cell} text-right whitespace-nowrap"><button class={GCP_MINI} onclick={() => copyText(c.oobLink)} data-testid="idp-code-copy">Copy link</button></td>
									</tr>
								{/each}
							</tbody>
						</table>
					</div>
				{/if}
			</div>
		{:else if tab === 'tenants'}
			{#if tenants.length === 0}
				<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="idp-tenants-empty">No tenants in this project.</p>
			{:else}
				<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
					<table class="w-full text-sm" data-testid="idp-tenant-table">
						<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Tenant</th><th class={cell}>ID</th><th class={cell}>Password sign-up</th><th class={cell}>Email link</th><th class={cell}>Anonymous</th><th class="{cell} text-right">Scope</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each tenants as t (t.id)}
								<tr data-testid="idp-tenant-row">
									<td class={cell}>{t.displayName || '—'}</td>
									<td class="{cell} font-mono text-xs">{t.id}</td>
									<td class="{cell} text-xs">{t.allowPasswordSignup ? 'Yes' : 'No'}</td>
									<td class="{cell} text-xs">{t.enableEmailLinkSignin ? 'Yes' : 'No'}</td>
									<td class="{cell} text-xs">{t.enableAnonymousUser ? 'Yes' : 'No'}</td>
									<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => { tab = 'users'; void pickTenant(t.id); }} disabled={tenant === t.id} data-testid="idp-tenant-switch">{tenant === t.id ? 'Active' : 'Switch'}</button></td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		{:else if config}
			<div class="space-y-3 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="idp-settings">
				<label class="flex items-center gap-2 text-sm"><input type="checkbox" checked={config.emailEnabled} onchange={(e) => toggle('emailEnabled', e.currentTarget.checked)} data-testid="idp-email-toggle" />Email/Password sign-in</label>
				<label class="flex items-center gap-2 text-sm"><input type="checkbox" checked={config.anonymousEnabled} onchange={(e) => toggle('anonymousEnabled', e.currentTarget.checked)} data-testid="idp-anonymous-toggle" />Anonymous sign-in</label>
				<p class="text-xs text-gray-500" data-testid="idp-domains">Authorized domains: {config.authorizedDomains.length ? config.authorizedDomains.join(', ') : '—'}</p>
			</div>
		{/if}
	{/if}
</div>
