<script lang="ts">
	import { onMount } from 'svelte';
	import { Copy } from 'lucide-svelte';
	import Modal from '#lib/components/Modal.svelte';
	import {
		addSecret,
		createApplication,
		deleteApplication,
		getApplication,
		listApplications,
		removeSecret,
		secretExpiry,
		type Application,
		type ApplicationDetail,
		type NewSecret
	} from '#lib/azureentra.ts';
	import { attempt, confirmedAttempt, copyText, GCP_GHOST, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	const EXPIRIES = [
		{ months: 3, label: '3 months' },
		{ months: 6, label: '6 months' },
		{ months: 12, label: '12 months' },
		{ months: 24, label: '24 months' }
	];

	let apps = $state<Application[]>([]);
	let loading = $state(true);
	let error = $state('');
	let name = $state('');
	let selected = $state('');
	let detail = $state<ApplicationDetail | null>(null);
	let detailError = $state('');
	let secretName = $state('');
	let months = $state(12);
	let shown = $state<NewSecret | null>(null);
	let modal = $state<Modal | null>(null);

	async function load() {
		try {
			apps = await listApplications();
			error = '';
			if (!apps.some((a) => a.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		await loadDetail();
	}

	async function loadDetail() {
		if (!selected) {
			detail = null;
			detailError = '';
			return;
		}
		try {
			detail = await getApplication(selected);
			detailError = '';
		} catch (e) {
			detailError = describeGcpError(e);
		}
	}

	function select(a: Application) {
		selected = selected === a.id ? '' : a.id;
		detail = null;
		detailError = '';
		void loadDetail();
	}

	async function create(event: Event) {
		event.preventDefault();
		if (!name.trim()) return;
		const ok = await attempt(() => createApplication(name), `Application ${name} created`, `Failed to create ${name}`);
		if (ok) {
			name = '';
			await load();
		}
	}

	async function remove(a: Application) {
		const ok = await confirmedAttempt({ title: 'Delete application', message: `Delete "${a.displayName}"? Its service principal and secrets are removed too.`, confirmLabel: 'Delete' }, () => deleteApplication(a.id), `Application ${a.displayName} deleted`, `Failed to delete ${a.displayName}`);
		if (ok) await load();
	}

	async function addPassword(event: Event, a: Application) {
		event.preventDefault();
		const box: { value: NewSecret | null } = { value: null };
		const ok = await attempt(
			async () => {
				box.value = await addSecret(a.id, secretName, secretExpiry(months));
			},
			'',
			'Failed to add secret'
		);
		if (!ok || !box.value) return;
		secretName = '';
		shown = box.value;
		modal?.open();
		await load();
	}

	function closeSecret() {
		shown = null;
	}

	async function dropSecret(a: Application, keyId: string, label: string) {
		const ok = await confirmedAttempt({ title: 'Remove secret', message: `Remove secret "${label}" from ${a.displayName}?`, confirmLabel: 'Remove' }, () => removeSecret(a.id, keyId), 'Secret removed', 'Failed to remove secret');
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const when = (v: string) => (v ? v.slice(0, 10) : '—');

	onMount(load);
</script>

<div class="space-y-4" data-testid="entra-apps">
	<form class="flex flex-wrap items-end gap-2" onsubmit={create} data-testid="entra-app-form">
		<label class="text-xs text-gray-500">Display name<input class="{GCP_INPUT} mt-1" bind:value={name} data-testid="entra-app-name" /></label>
		<button class={GCP_PRIMARY} type="submit" disabled={!name.trim()} data-testid="entra-app-create">Create application</button>
	</form>

	{#if error}
		<p class="text-sm text-red-600" role="alert" data-testid="entra-apps-error">{error}</p>
	{:else if loading}
		<p class="text-sm text-gray-500">Loading…</p>
	{:else if apps.length === 0}
		<p class="py-8 text-center text-gray-500" data-testid="entra-apps-empty">No applications.</p>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="entra-apps-table">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Name</th><th class={cell}>Application (client) ID</th><th class={cell}>Created</th><th class={cell}>Secrets</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each apps as a (a.id)}
						<tr class={selected === a.id ? 'bg-indigo-50/60 dark:bg-indigo-950/20' : ''} data-testid="entra-app">
							<td class={cell}><button class="underline decoration-dotted" onclick={() => select(a)} aria-expanded={selected === a.id} data-testid="entra-app-select">{a.displayName}</button></td>
							<td class="{cell} font-mono text-xs" data-testid="entra-app-id">{a.appId}</td>
							<td class="{cell} text-xs">{when(a.created)}</td>
							<td class="{cell} text-xs" data-testid="entra-app-secrets">{a.secrets.length}</td>
							<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => remove(a)} data-testid="entra-app-delete">Delete</button></td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	{#if detailError}<p class="text-sm text-red-600" role="alert" data-testid="entra-app-detail-error">{detailError}</p>{/if}

	{#if detail}
		<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="entra-app-detail">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{detail.displayName}</h2>
			<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-1 text-xs">
				<dt class="text-gray-500">Object ID</dt><dd class="font-mono">{detail.id}</dd>
				<dt class="text-gray-500">Redirect URIs</dt>
				<dd class="font-mono" data-testid="entra-app-redirects">{detail.redirectUris.length ? detail.redirectUris.join(', ') : '—'}</dd>
				<dt class="text-gray-500">API permissions</dt>
				<dd data-testid="entra-app-access">{detail.resourceAccess.length ? detail.resourceAccess.map((r) => `${r.resourceAppId} (${r.scopes} scopes, ${r.roles} roles)`).join('; ') : '—'}</dd>
				<dt class="text-gray-500">App roles</dt>
				<dd data-testid="entra-app-roles">{detail.appRoles.length ? detail.appRoles.map((r) => r.value || r.displayName).join(', ') : '—'}</dd>
				<dt class="text-gray-500">Owners</dt>
				<dd data-testid="entra-app-owners">{detail.owners.length ? detail.owners.map((o) => o.name || o.id).join(', ') : '—'}</dd>
			</dl>

			<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Client secrets</h3>
			<p class="text-xs text-gray-500">A secret value is shown once when it is created. Afterwards only its hint and expiry are available.</p>
			<form class="flex flex-wrap items-end gap-2" onsubmit={(e) => addPassword(e, detail!)} data-testid="entra-secret-form">
				<label class="text-xs text-gray-500">Description<input class="{GCP_INPUT} mt-1" bind:value={secretName} data-testid="entra-secret-name" /></label>
				<label class="text-xs text-gray-500">Expires
					<select class="{GCP_INPUT} mt-1" bind:value={months} data-testid="entra-secret-expiry">
						{#each EXPIRIES as x (x.months)}<option value={x.months}>{x.label}</option>{/each}
					</select>
				</label>
				<button class={GCP_PRIMARY} type="submit" data-testid="entra-secret-add">Add client secret</button>
			</form>
			{#if detail.secrets.length === 0}
				<p class="text-sm text-gray-500" data-testid="entra-secrets-empty">No client secrets.</p>
			{:else}
				<table class="w-full text-sm" data-testid="entra-secrets">
					<thead class="text-left text-gray-500"><tr><th class={cell}>Description</th><th class={cell}>Hint</th><th class={cell}>Expires</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each detail.secrets as s (s.keyId)}
							<tr data-testid="entra-secret">
								<td class="{cell} text-xs">{s.displayName || '—'}</td>
								<td class="{cell} font-mono text-xs" data-testid="entra-secret-hint">{s.hint ? `${s.hint}…` : '—'}</td>
								<td class="{cell} text-xs">{when(s.end)}{#if s.expired} <span class="text-red-600">(expired)</span>{/if}</td>
								<td class="{cell} text-right"><button class={GCP_MINI} onclick={() => dropSecret(detail!, s.keyId, s.displayName || s.hint)} data-testid="entra-secret-remove">Remove</button></td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{/if}

	<Modal title="Client secret created" bind:this={modal} onClose={closeSecret} widthClass="max-w-lg">
		{#if shown}
			<div class="space-y-3" data-testid="entra-secret-modal">
				<p class="text-sm text-amber-700 dark:text-amber-300">Copy this value now. It cannot be shown again.</p>
				<div class="flex items-center gap-2">
					<code class="flex-1 break-all rounded bg-gray-100 p-2 text-xs dark:bg-slate-800" data-testid="entra-secret-value">{shown.value}</code>
					<button class={GCP_MINI} onclick={() => copyText(shown?.value ?? '')} aria-label="Copy secret" data-testid="entra-secret-copy"><Copy class="h-4 w-4" /></button>
				</div>
				<div class="flex justify-end"><button class={GCP_GHOST} onclick={() => modal?.close('done')} data-testid="entra-secret-close">I have copied it</button></div>
			</div>
		{/if}
	</Modal>
</div>
