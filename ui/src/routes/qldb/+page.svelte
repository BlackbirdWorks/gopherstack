<script lang="ts">
	import { onMount } from 'svelte';
	import { getQLDBClient } from '$lib/aws-client';
	import { ListLedgersCommand, CreateLedgerCommand, type LedgerSummary } from '@aws-sdk/client-qldb';
	import { toast } from 'svelte-sonner';

	const qldb = getQLDBClient();

	let loading = $state(false);
	let creating = $state(false);
	let ledgers = $state<LedgerSummary[]>([]);
	let ledgerName = $state('');

	async function loadLedgers() {
		loading = true;
		try {
			const out = await qldb.send(new ListLedgersCommand({ MaxResults: 100 }));
			ledgers = out.Ledgers ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to list ledgers: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createLedger() {
		if (!ledgerName.trim()) {
			return;
		}

		creating = true;
		try {
			await qldb.send(new CreateLedgerCommand({ Name: ledgerName.trim(), PermissionsMode: 'ALLOW_ALL' }));
			toast.success(`Ledger "${ledgerName.trim()}" created`);
			ledgerName = '';
			await loadLedgers();
		} catch (err: unknown) {
			toast.error(`Failed to create ledger: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	onMount(() => {
		void loadLedgers();
	});
</script>

<div class="space-y-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">QLDB Ledgers</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Amazon Quantum Ledger Database</p>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="flex flex-col gap-3 sm:flex-row">
			<input
				type="text"
				bind:value={ledgerName}
				placeholder="Enter ledger name"
				class="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 dark:border-slate-600 dark:bg-slate-900 dark:text-white"
			/>
			<button
				type="button"
				onclick={createLedger}
				disabled={creating}
				class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>
				{creating ? 'Creating...' : 'Create Ledger'}
			</button>
		</div>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if loading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading ledgers...</p>
		{:else if ledgers.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No ledgers found</p>
		{:else}
			<div class="space-y-2">
				{#each ledgers as ledger}
					<div class="rounded-lg border border-slate-200 p-3 text-sm dark:border-slate-700">
						<div class="font-medium text-slate-900 dark:text-white">{ledger.Name}</div>
						<div class="text-xs text-slate-500 dark:text-slate-400">{ledger.State}</div>
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
