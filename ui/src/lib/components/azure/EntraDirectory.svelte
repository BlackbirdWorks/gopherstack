<script lang="ts">
	import { onMount } from 'svelte';
	import {
		addMember,
		createGroup,
		createUser,
		listGroups,
		listPrincipals,
		listUsers,
		mailNicknameFrom,
		removeMember,
		validNickname,
		validPassword,
		validUpn,
		type DirGroup,
		type DirObject,
		type DirUser
	} from '#lib/azureentra.ts';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let users = $state<DirUser[]>([]);
	let groups = $state<DirGroup[]>([]);
	let candidates = $state<DirObject[]>([]);
	let loading = $state(true);
	let error = $state('');
	let userName = $state('');
	let userUpn = $state('');
	let userPassword = $state('');
	let userForce = $state(true);
	let groupName = $state('');
	let pick = $state<Record<string, string>>({});

	const canUser = $derived(userName.trim() !== '' && validUpn(userUpn) && validNickname(mailNicknameFrom(userName)) && validPassword(userPassword));
	const canGroup = $derived(groupName.trim() !== '' && validNickname(mailNicknameFrom(groupName)));

	async function load() {
		try {
			[users, groups, candidates] = await Promise.all([listUsers(), listGroups(), listPrincipals()]);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function makeUser(event: Event) {
		event.preventDefault();
		if (!canUser) return;
		const ok = await attempt(
			() => createUser({ displayName: userName, mailNickname: mailNicknameFrom(userName), userPrincipalName: userUpn, password: userPassword, forceChangePasswordNextSignIn: userForce }),
			`User ${userUpn} created`,
			`Failed to create ${userUpn}`
		);
		userPassword = '';
		if (ok) {
			userName = userUpn = '';
			await load();
		}
	}

	async function makeGroup(event: Event) {
		event.preventDefault();
		if (!canGroup) return;
		const ok = await attempt(() => createGroup(groupName, mailNicknameFrom(groupName)), `Group ${groupName} created`, `Failed to create ${groupName}`);
		if (ok) {
			groupName = '';
			await load();
		}
	}

	async function add(g: DirGroup) {
		const id = pick[g.id];
		if (!id) return;
		const ok = await attempt(() => addMember(g.id, id), 'Member added', 'Failed to add member');
		if (ok) {
			pick[g.id] = '';
			await load();
		}
	}

	async function drop(g: DirGroup, m: DirObject) {
		const ok = await confirmedAttempt({ title: 'Remove member', message: `Remove "${m.name || m.id}" from ${g.displayName}?`, confirmLabel: 'Remove' }, () => removeMember(g.id, m.id), 'Member removed', 'Failed to remove member');
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const addable = (g: DirGroup) => candidates.filter((c) => c.type !== 'Application' && !g.members.some((m) => m.id === c.id));

	onMount(load);
</script>

<div class="space-y-8" data-testid="entra-directory">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="entra-dir-error">{error}</p>{/if}

	<section class="space-y-3">
		<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Users</h2>
		<form class="flex flex-wrap items-end gap-2" onsubmit={makeUser} autocomplete="off" data-testid="entra-user-form">
			<label class="text-xs text-gray-500">Display name<input class="{GCP_INPUT} mt-1" bind:value={userName} data-testid="entra-user-name" /></label>
			<label class="text-xs text-gray-500">User principal name<input class="{GCP_INPUT} mt-1 !w-72" autocomplete="username" bind:value={userUpn} placeholder="ann@gopherstack.onmicrosoft.com" data-testid="entra-user-upn" /></label>
			<label class="text-xs text-gray-500">Initial password<input class="{GCP_INPUT} mt-1" type="password" autocomplete="new-password" bind:value={userPassword} data-testid="entra-user-password" /></label>
			<label class="flex items-center gap-1 text-xs text-gray-500"><input type="checkbox" bind:checked={userForce} data-testid="entra-user-force" />Change at next sign-in</label>
			<button class={GCP_PRIMARY} type="submit" disabled={!canUser} data-testid="entra-user-create">Create user</button>
		</form>
		<p class="text-xs text-gray-500">The password is sent once and never shown or returned.</p>
		{#if loading}
			<p class="text-sm text-gray-500">Loading…</p>
		{:else if users.length === 0}
			<p class="text-sm text-gray-500" data-testid="entra-users-empty">No users.</p>
		{:else}
			<table class="w-full text-sm" data-testid="entra-users">
				<thead class="text-left text-gray-500"><tr><th class={cell}>User principal name</th><th class={cell}>Display name</th><th class={cell}>Object ID</th></tr></thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each users as u (u.id)}
						<tr data-testid="entra-user"><td class="{cell} font-mono text-xs">{u.upn}</td><td class={cell}>{u.displayName}</td><td class="{cell} font-mono text-xs">{u.id}</td></tr>
					{/each}
				</tbody>
			</table>
		{/if}
	</section>

	<section class="space-y-3">
		<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Groups</h2>
		<form class="flex flex-wrap items-end gap-2" onsubmit={makeGroup} data-testid="entra-group-form">
			<label class="text-xs text-gray-500">Display name<input class="{GCP_INPUT} mt-1" bind:value={groupName} data-testid="entra-group-name" /></label>
			<button class={GCP_PRIMARY} type="submit" disabled={!canGroup} data-testid="entra-group-create">Create group</button>
		</form>
		{#if !loading && groups.length === 0}
			<p class="text-sm text-gray-500" data-testid="entra-groups-empty">No groups.</p>
		{/if}
		{#each groups as g (g.id)}
			<div class="space-y-2 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="entra-group">
				<h3 class="font-medium text-gray-900 dark:text-white" data-testid="entra-group-title">{g.displayName} <span class="text-xs text-gray-500">({g.members.length} members)</span></h3>
				<ul class="space-y-1 text-sm">
					{#each g.members as m (m.id)}
						<li class="flex items-center gap-2" data-testid="entra-member">
							<span>{m.name || m.id}</span><span class="text-xs text-gray-500">{m.type}{m.detail ? ` · ${m.detail}` : ''}</span>
							<button class={GCP_MINI} onclick={() => drop(g, m)} data-testid="entra-member-remove">Remove</button>
						</li>
					{/each}
				</ul>
				<div class="flex items-center gap-2">
					<select class="{GCP_INPUT} !w-72" bind:value={pick[g.id]} data-testid="entra-member-pick">
						<option value="">Add member…</option>
						{#each addable(g) as c (c.id)}<option value={c.id}>{c.type}: {c.name}{c.detail ? ` (${c.detail})` : ''}</option>{/each}
					</select>
					<button class={GCP_MINI} onclick={() => add(g)} disabled={!pick[g.id]} data-testid="entra-member-add">Add</button>
				</div>
			</div>
		{/each}
	</section>
</div>
