<script lang="ts">
	import { RefreshCw } from 'lucide-svelte';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		createUser,
		deleteUser,
		filterUsers,
		formatTime,
		listUsers,
		providerLabel,
		setClaims,
		userAction,
		userProviders,
		validEmail,
		validPassword,
		validateClaims,
		type IdpUser,
		type UserAction
	} from '#lib/gcpidentitytoolkit.ts';

	type Props = { project: string; tenant: string; refreshKey: number };
	let { project, tenant, refreshKey }: Props = $props();

	let users = $state<IdpUser[]>([]);
	let total = $state(0);
	let loading = $state(true);
	let error = $state('');
	let query = $state('');
	let selected = $state('');
	let claimsText = $state('{}');
	let email = $state('');
	let password = $state('');
	let displayName = $state('');

	const visible = $derived(filterUsers(users, query));
	const current = $derived(users.find((u) => u.uid === selected));
	const claimsCheck = $derived(validateClaims(claimsText));
	const canCreate = $derived(validEmail(email) && validPassword(password));

	async function load() {
		try {
			const res = await listUsers(project, tenant);
			users = res.users;
			total = res.total;
			error = '';
			if (!users.some((u) => u.uid === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function select(u: IdpUser) {
		selected = selected === u.uid ? '' : u.uid;
		claimsText = JSON.stringify(u.claims, null, 2);
	}

	async function create(event: Event) {
		event.preventDefault();
		if (!canCreate) return;
		const input = { email: email.trim(), password, displayName: displayName.trim() };
		password = '';
		const ok = await attempt(() => createUser(project, tenant, input), `Created ${input.email}`, `Failed to create ${input.email}`);
		if (ok) {
			email = '';
			displayName = '';
			await load();
		}
	}

	async function act(u: IdpUser, action: UserAction) {
		const labels = { disable: 'Disabled', enable: 'Enabled', revoke: 'Revoked tokens for' } as const;
		const ok = await attempt(() => userAction(project, tenant, u.uid, action), `${labels[action]} ${u.email || u.uid}`, `Failed to ${action} ${u.email || u.uid}`);
		if (ok) await load();
	}

	async function saveClaims(u: IdpUser) {
		const check = claimsCheck;
		if (!check.ok) return;
		const ok = await attempt(() => setClaims(project, tenant, u.uid, check.claims), 'Custom claims saved', 'Failed to save custom claims');
		if (ok) await load();
	}

	async function remove(u: IdpUser) {
		const ok = await confirmedAttempt({ title: 'Delete user', message: `Delete ${u.email || u.uid}? Their sessions and refresh tokens stop working.`, confirmLabel: 'Delete' }, () => deleteUser(project, tenant, u.uid), `Deleted ${u.email || u.uid}`, `Failed to delete ${u.email || u.uid}`);
		if (ok) {
			selected = '';
			await load();
		}
	}

	const cell = 'px-4 py-3';
	const pill = (on: boolean, good: string, bad: string) =>
		on ? good : bad;

	$effect(() => {
		void refreshKey;
		void project;
		void tenant;
		loading = true;
		void load();
	});
</script>

<div class="space-y-4" data-testid="idp-users">
	<form class="flex flex-wrap items-end gap-2 rounded-lg border border-gray-200 p-3 dark:border-gray-700" onsubmit={create} autocomplete="off" data-testid="idp-create-form">
		<label class="text-xs text-gray-500">Email<input class="{GCP_INPUT} mt-1" type="email" bind:value={email} placeholder="user@example.com" autocomplete="off" data-testid="idp-create-email" /></label>
		<label class="text-xs text-gray-500">Password (6+ characters)<input class="{GCP_INPUT} mt-1" type="password" bind:value={password} autocomplete="new-password" data-testid="idp-create-password" /></label>
		<label class="text-xs text-gray-500">Display name<input class="{GCP_INPUT} mt-1" bind:value={displayName} autocomplete="off" data-testid="idp-create-name" /></label>
		<button class={GCP_PRIMARY} type="submit" disabled={!canCreate} data-testid="idp-create-submit">Create user</button>
	</form>

	<div class="flex flex-wrap items-center gap-3">
		<input class="{GCP_INPUT} !w-72" type="search" placeholder="Search by email, UID or name" bind:value={query} aria-label="Search users" data-testid="idp-search" />
		<span class="text-xs text-gray-500" data-testid="idp-count">{visible.length} of {total} users</span>
	</div>

	{#if error && users.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="idp-users-error">
			<p class="font-medium">Could not load users</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if visible.length === 0}
		<p class="py-8 text-center text-gray-500 dark:text-gray-400" data-testid="idp-users-empty">{users.length === 0 ? 'No users in this scope.' : 'No users match the search.'}</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="idp-user-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Identifier</th><th class={cell}>Providers</th><th class={cell}>Verified</th><th class={cell}>Status</th><th class={cell}>Created</th><th class={cell}>Last sign-in</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each visible as u (u.uid)}
						<tr class={selected === u.uid ? 'bg-blue-50/60 dark:bg-blue-950/20' : ''} data-testid="idp-user">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(u)} aria-expanded={selected === u.uid} data-testid="idp-select">{u.email || u.uid}</button></td>
							<td class="{cell} text-xs">{userProviders(u).map(providerLabel).join(', ') || '—'}</td>
							<td class="{cell} text-xs">{u.emailVerified ? 'Yes' : 'No'}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(u.disabled, 'bg-red-100 text-red-800 dark:bg-red-900/40 dark:text-red-300', 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300')}" data-testid="idp-status">{u.disabled ? 'Disabled' : 'Enabled'}</span></td>
							<td class="{cell} font-mono text-xs">{formatTime(u.createdAt)}</td>
							<td class="{cell} font-mono text-xs">{formatTime(u.lastLoginAt)}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	{#if current}
		<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="idp-detail">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.email || current.uid}</h2>
			<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
				<dt class="text-gray-500">UID</dt><dd class="font-mono" data-testid="idp-detail-uid">{current.uid}</dd>
				<dt class="text-gray-500">Email</dt><dd data-testid="idp-detail-email">{current.email || '—'}</dd>
				<dt class="text-gray-500">Email verified</dt><dd data-testid="idp-detail-verified">{current.emailVerified ? 'Yes' : 'No'}</dd>
				<dt class="text-gray-500">Disabled</dt><dd data-testid="idp-detail-disabled">{current.disabled ? 'Yes' : 'No'}</dd>
				<dt class="text-gray-500">Display name</dt><dd>{current.displayName || '—'}</dd>
				<dt class="text-gray-500">Providers</dt><dd data-testid="idp-detail-providers">{userProviders(current).map(providerLabel).join(', ') || '—'}</dd>
				<dt class="text-gray-500">Created</dt><dd class="font-mono">{formatTime(current.createdAt)}</dd>
				<dt class="text-gray-500">Last sign-in</dt><dd class="font-mono">{formatTime(current.lastLoginAt)}</dd>
				<dt class="text-gray-500">Tokens valid since</dt><dd class="font-mono" data-testid="idp-detail-valid-since">{current.validSince ? formatTime(current.validSince * 1000) : '—'}</dd>
			</dl>

			<div class="flex flex-wrap gap-2">
				<button class={GCP_MINI} onclick={() => act(current, 'disable')} disabled={current.disabled} data-testid="idp-disable">Disable</button>
				<button class={GCP_MINI} onclick={() => act(current, 'enable')} disabled={!current.disabled} data-testid="idp-enable">Enable</button>
				<button class={GCP_MINI} onclick={() => act(current, 'revoke')} data-testid="idp-revoke">Revoke refresh tokens</button>
				<button class={GCP_MINI} onclick={() => remove(current)} data-testid="idp-delete">Delete</button>
			</div>

			<div class="space-y-1">
				<label class="text-sm font-semibold text-gray-700 dark:text-gray-300" for="idp-claims">Custom claims (JSON object)</label>
				<textarea id="idp-claims" class="{GCP_INPUT} h-28 font-mono text-xs" bind:value={claimsText} spellcheck="false" data-testid="idp-claims"></textarea>
				{#if !claimsCheck.ok}<p class="text-xs text-red-600" role="alert" data-testid="idp-claims-error">{claimsCheck.error}</p>{/if}
				<button class={GCP_MINI} onclick={() => saveClaims(current)} disabled={!claimsCheck.ok} data-testid="idp-claims-save">Save claims</button>
			</div>
		</div>
	{/if}
</div>
