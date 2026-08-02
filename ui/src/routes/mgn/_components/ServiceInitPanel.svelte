<script lang="ts">
	// Service init & managed accounts (services/mgn/PARITY.md family K, 2
	// ops): InitializeService, ListManagedAccounts. InitializeService is the
	// account-level "opt in" call every other legacy op's
	// UninitializedAccountException implicitly depends on (69 of 95 ops --
	// PARITY.md's error-generation split) -- if every other tab's calls are
	// failing with UninitializedAccountException, call it here first.
	import { InitializeServiceCommand, ListManagedAccountsCommand, type ManagedAccount, type MgnClient } from '@aws-sdk/client-mgn';
	import { toast } from 'svelte-sonner';
	import { onRegionChange } from '$lib/region-effect.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import { describeError } from './shared';

	type Props = { client: () => MgnClient; searchQuery: string };
	let { client, searchQuery }: Props = $props();

	let accounts = $state<ManagedAccount[]>([]);
	let nextToken = $state<string | undefined>();
	let loading = $state(false);
	let loadingMore = $state(false);
	let error = $state<string | null>(null);
	let initializing = $state(false);
	let initError = $state<string | null>(null);
	let initialized = $state(false);

	async function fetchAccounts(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListManagedAccountsCommand({ maxResults: 50, nextToken: reset ? undefined : nextToken })
		);
		accounts = reset ? (resp.items ?? []) : [...accounts, ...(resp.items ?? [])];
		nextToken = resp.nextToken;
	}

	export async function refresh(): Promise<void> {
		loading = true;
		error = null;
		try {
			await fetchAccounts(true);
		} catch (e) {
			error = describeError(e);
		} finally {
			loading = false;
		}
	}

	async function loadMore(): Promise<void> {
		loadingMore = true;
		try {
			await fetchAccounts(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMore = false;
		}
	}

	onRegionChange(() => void refresh());

	const filtered = $derived(
		accounts.filter((a) => (a.accountId ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	async function initializeService(): Promise<void> {
		initializing = true;
		initError = null;
		try {
			await client().send(new InitializeServiceCommand({}));
			initialized = true;
			toast.success('Account initialized for MGN');
		} catch (e) {
			initError = describeError(e);
			toast.error(initError);
		} finally {
			initializing = false;
		}
	}

	const columns = defineColumns<ManagedAccount>([{ key: 'accountId', label: 'Account ID' }]);
</script>

<div class="rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-800 p-4 space-y-2">
	<p class="text-sm text-slate-600 dark:text-slate-300">
		69 of MGN's 95 operations require the calling account to be initialized
		first (they return <code>UninitializedAccountException</code> otherwise).
		Call InitializeService once per account/region before using the other
		tabs.
	</p>
	<button onclick={initializeService} disabled={initializing} class="px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm disabled:opacity-50">
		{initializing ? 'Initializing…' : 'Initialize service'}
	</button>
	{#if initialized}<p class="text-sm text-green-600 dark:text-green-400">Initialized.</p>{/if}
	{#if initError}<p class="text-sm text-red-600 dark:text-red-400">{initError}</p>{/if}
</div>

<div>
	<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-2">Managed accounts</h3>
	<p class="text-sm text-slate-500 dark:text-slate-400 mb-2">
		This emulator does not simulate cross-account delegation (PARITY.md's
		AccountID gap) -- this list reflects only the calling account.
	</p>
	{#if error}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300 mb-2">
			<p class="font-medium">Failed to load data</p>
			<p>{error}</p>
		</div>
	{/if}
	<DataTable rows={filtered} rowKey={(a) => a.accountId ?? ''} {columns} {loading} emptyMessage="No managed accounts found" />
	<LoadMore hasMore={!!nextToken} loading={loadingMore} onLoadMore={loadMore} />
</div>
