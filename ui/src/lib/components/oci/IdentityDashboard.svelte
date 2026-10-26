<script lang="ts">
	import { onMount } from 'svelte';
	import { RefreshCw, Users } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		buildCompartmentTree,
		flattenTree,
		listApiKeys,
		listCompartments,
		listGroups,
		listMemberships,
		listPolicies,
		listUsers,
		type ApiKeyInfo,
		type Compartment,
		type GroupInfo,
		type Membership,
		type PolicyInfo,
		type UserInfo
	} from '#lib/ociidentity.ts';

	const TABS = [
		{ id: 'compartments', label: 'Compartments' },
		{ id: 'users', label: 'Users' },
		{ id: 'groups', label: 'Groups' },
		{ id: 'policies', label: 'Policies' }
	];

	let tab = $state('compartments');
	let compartments = $state<Compartment[]>([]);
	let users = $state<UserInfo[]>([]);
	let groups = $state<GroupInfo[]>([]);
	let memberships = $state<Membership[]>([]);
	let policies = $state<PolicyInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let userId = $state('');
	let keys = $state<ApiKeyInfo[]>([]);
	let keyError = $state('');

	async function load() {
		try {
			[compartments, users, groups, memberships, policies] = await Promise.all([listCompartments(), listUsers(), listGroups(), listMemberships(), listPolicies()]);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function selectUser(row: ResourceRow) {
		userId = userId === row.id ? '' : row.id;
		keys = [];
		keyError = '';
		if (!userId) return;
		try {
			keys = await listApiKeys(userId);
		} catch (e) {
			keyError = describeGcpError(e);
		}
	}

	const tree = $derived(flattenTree(buildCompartmentTree(compartments)));
	const compName = (id: string) => compartments.find((c) => c.id === id)?.name ?? id;
	const userName = (id: string) => users.find((u) => u.id === id)?.name ?? id;
	const membersOf = (groupId: string) => memberships.filter((m) => m.groupId === groupId).map((m) => userName(m.userId));
	const userRows = $derived(users.map((u) => ({ id: u.id, cells: [u.name, u.state, u.email, u.description, u.mfa ? 'MFA' : 'no MFA'], raw: u })));
	const groupRows = $derived(groups.map((g) => ({ id: g.id, cells: [g.name, g.state, g.description, membersOf(g.id).join(', ')], raw: g })));
	const policyRows = $derived(policies.map((p) => ({ id: p.id, cells: [p.name, compName(p.compartmentId), p.description, `${p.statements.length} statement${p.statements.length === 1 ? '' : 's'}`], raw: p })));
	const cell = 'px-4 py-2';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="oci-identity-dashboard">
	<PageHeader icon={Users} title="Identity" description="Oracle Cloud Identity and Access Management: compartments, users, groups and policies. API keys are shown by fingerprint only." color="red" onRefresh={load} />
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />

	{#if error}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="oci-identity-error">
			<p class="font-medium">Could not load identity data</p><p>{error}</p><button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if tab === 'compartments'}
		<table class="w-full text-sm" data-testid="oci-compartment-tree">
			<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Compartment</th><th class={cell}>State</th><th class={cell}>Description</th><th class={cell}>Created</th></tr></thead>
			<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
				{#each tree as c (c.id)}
					<tr data-testid="oci-compartment">
						<td class="{cell} text-xs" style="padding-left: {1 + c.depth * 1.5}rem" data-testid="oci-compartment-name">{c.depth ? '└ ' : ''}{c.name}{c.depth === 0 ? ' (root)' : ''}</td>
						<td class="{cell} text-xs">{c.state}</td><td class="{cell} text-xs">{c.description || '—'}</td><td class="{cell} text-xs">{c.timeCreated}</td>
					</tr>
				{/each}
			</tbody>
		</table>
	{:else if tab === 'users'}
		<GcpResourceTable testId="oci-user" noun="users" columns={['User', 'State', 'Email', 'Description', 'MFA']} rows={userRows} loading={false} error="" selected={userId} onRetry={load} onSelect={selectUser} />
		{#if userId}
			<div class="space-y-2" data-testid="oci-user-keys">
				<h3 class="font-medium text-gray-900 dark:text-white">API key fingerprints for {userName(userId)}</h3>
				{#if keyError}<p class="text-sm text-red-600" role="alert">{keyError}</p>{/if}
				{#if keys.length === 0}
					<p class="py-2 text-gray-500 dark:text-gray-400" data-testid="oci-user-keys-empty">No API keys</p>
				{:else}
					<ul class="space-y-1">
						{#each keys as k (k.fingerprint)}
							<li class="font-mono text-xs" data-testid="oci-api-key">{k.fingerprint} <span class="text-gray-500">({k.state}, added {k.timeCreated})</span></li>
						{/each}
					</ul>
				{/if}
			</div>
		{/if}
	{:else if tab === 'groups'}
		<GcpResourceTable testId="oci-group" noun="groups" columns={['Group', 'State', 'Description', 'Members']} rows={groupRows} loading={false} error="" onRetry={load} />
	{:else}
		<GcpResourceTable testId="oci-policy" noun="policies" columns={['Policy', 'Compartment', 'Description', 'Statements']} rows={policyRows} loading={false} error="" onRetry={load}>
			{#snippet detail(row)}
				{@const p = policies.find((x) => x.id === row.id)}
				{#if p}
					<ul class="list-disc space-y-1 pl-5 font-mono" data-testid="oci-policy-statements">
						{#each p.statements as s (s)}<li>{s}</li>{/each}
					</ul>
				{/if}
			{/snippet}
		</GcpResourceTable>
	{/if}
</div>
