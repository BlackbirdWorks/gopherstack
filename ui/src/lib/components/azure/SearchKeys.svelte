<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { RefreshCw } from 'lucide-svelte';
	import { attempt, confirmedAttempt, copyText, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		createQueryKey,
		deleteQueryKey,
		getKeys,
		revealAdminKeys,
		revealQueryKey,
		validateKeyName,
		type AdminKeys,
		type KeyOverview,
		type SearchTarget
	} from '#lib/azuresearch.ts';

	let { target }: { target: SearchTarget } = $props();

	let keys = $state<KeyOverview>();
	let loading = $state(true);
	let error = $state('');
	let admin = $state<AdminKeys>();
	let revealed = $state<Record<string, string>>({});
	let newName = $state('');

	const nameError = $derived(newName === '' ? '' : validateKeyName(newName));

	async function load() {
		try {
			keys = await getKeys(target);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function showAdmin() {
		try {
			admin = await revealAdminKeys(target);
		} catch (e) {
			toast.error(`Failed to get the admin keys: ${describeGcpError(e)}`);
		}
	}

	async function showQuery(fp: string) {
		try {
			revealed = { ...revealed, [fp]: await revealQueryKey(target, fp) };
		} catch (e) {
			toast.error(`Failed to get the query key: ${describeGcpError(e)}`);
		}
	}

	function hideQuery(fp: string) {
		const { [fp]: _gone, ...rest } = revealed;
		revealed = rest;
	}

	async function create() {
		if (nameError || newName === '') return;
		if (await attempt(() => createQueryKey(target, newName), `Created query key ${newName}`, 'Failed to create the key')) {
			newName = '';
			await load();
		}
	}

	async function remove(fp: string, name: string) {
		const ok = await confirmedAttempt(
			{ title: 'Delete query key', message: `Delete the query key "${name || 'unnamed'}"? Clients using it lose access.`, confirmLabel: 'Delete' },
			() => deleteQueryKey(target, fp),
			'Deleted the query key',
			'Failed to delete the key'
		);
		if (ok) {
			hideQuery(fp);
			await load();
		}
	}

	onDestroy(() => {
		admin = undefined;
		revealed = {};
	});
	onMount(load);

	const cell = 'px-4 py-3';
	const thead = 'bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400';
</script>

<div class="space-y-6" data-testid="search-keys">
	{#if error}<p class="text-sm text-red-600" role="alert" data-testid="search-keys-error">Could not load keys: {error}</p>{/if}
	{#if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if keys}
		<section class="space-y-2">
			<h3 class="text-sm font-semibold">Admin keys</h3>
			<p class="text-xs text-gray-500">Admin keys grant full access. They are only fetched when you choose to show them.</p>
			<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1 font-mono text-xs">
				<dt>Primary</dt><dd class="break-all" data-testid="search-admin-primary">{admin ? admin.primary : keys.primary}</dd>
				<dt>Secondary</dt><dd class="break-all" data-testid="search-admin-secondary">{admin ? admin.secondary : keys.secondary}</dd>
			</dl>
			{#if admin}
				<button class={GCP_MINI} onclick={() => (admin = undefined)} data-testid="search-admin-hide">Hide admin keys</button>
				<button class={GCP_MINI} onclick={() => copyText(admin?.primary ?? '')} data-testid="search-admin-copy">Copy primary</button>
			{:else}
				<button class={GCP_MINI} onclick={showAdmin} data-testid="search-admin-reveal">Show admin keys</button>
			{/if}
		</section>

		<section class="space-y-2">
			<h3 class="text-sm font-semibold">Query keys</h3>
			<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
				<table class="w-full text-sm" data-testid="search-query-keys">
					<thead class={thead}><tr><th class={cell}>Name</th><th class={cell}>Key</th><th class="{cell} text-right">Actions</th></tr></thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each keys.query as k (k.fingerprint)}
							<tr data-testid="search-query-key">
								<td class="{cell} text-xs">{k.name || '(unnamed)'}</td>
								<td class="{cell} font-mono text-xs break-all" data-testid="search-query-key-value">{revealed[k.fingerprint] ?? k.masked}</td>
								<td class="{cell} text-right whitespace-nowrap">
									{#if revealed[k.fingerprint]}
										<button class={GCP_MINI} onclick={() => hideQuery(k.fingerprint)} data-testid="search-query-hide">Hide</button>
									{:else}
										<button class={GCP_MINI} onclick={() => showQuery(k.fingerprint)} data-testid="search-query-reveal">Show</button>
									{/if}
									<button class={GCP_MINI} onclick={() => remove(k.fingerprint, k.name)} data-testid="search-query-delete">Delete</button>
								</td>
							</tr>
						{:else}
							<tr><td colspan="3" class="{cell} text-center text-gray-500" data-testid="search-query-keys-empty">No query keys.</td></tr>
						{/each}
					</tbody>
				</table>
			</div>
			<div class="flex flex-wrap items-start gap-2">
				<div>
					<input class="{GCP_INPUT} !w-64" placeholder="New query key name" bind:value={newName} aria-label="Query key name" data-testid="search-query-name" />
					{#if nameError}<p class="mt-1 text-xs text-red-600" data-testid="search-query-name-error">{nameError}</p>{/if}
				</div>
				<button class={GCP_PRIMARY} disabled={newName === '' || nameError !== ''} onclick={create} data-testid="search-query-create">Create query key</button>
			</div>
		</section>
	{/if}
</div>
