<script lang="ts">
	import { getQLDBSessionClient } from '$lib/aws-client';
	import { SendCommandCommand } from '@aws-sdk/client-qldb-session';
	import { toast } from 'svelte-sonner';

	type SessionRow = {
		ledgerName: string;
		sessionToken: string;
	};

	const qldbSession = getQLDBSessionClient();

	let ledgerName = $state('');
	let creating = $state(false);
	let sessions = $state<SessionRow[]>([]);

	async function startSession() {
		if (!ledgerName.trim()) {
			return;
		}

		creating = true;
		try {
			const out = await qldbSession.send(
				new SendCommandCommand({
					StartSession: { LedgerName: ledgerName.trim() }
				})
			);

			const token = out.StartSession?.SessionToken ?? '';
			if (token) {
				sessions = [{ ledgerName: ledgerName.trim(), sessionToken: token }, ...sessions];
			}
			toast.success(`Session started for ${ledgerName.trim()}`);
			ledgerName = '';
		} catch (err: unknown) {
			toast.error(`Failed to start session: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}
</script>

<div class="space-y-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">QLDB Session</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">Start and inspect QLDB sessions</p>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="flex flex-col gap-3 sm:flex-row">
			<input
				type="text"
				bind:value={ledgerName}
				placeholder="Ledger name"
				class="w-full rounded-lg border border-slate-300 bg-white px-3 py-2 text-sm text-slate-900 dark:border-slate-600 dark:bg-slate-900 dark:text-white"
			/>
			<button
				type="button"
				onclick={startSession}
				disabled={creating}
				class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>
				{creating ? 'Starting...' : 'Start Session'}
			</button>
		</div>
	</div>

	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		{#if sessions.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No active sessions</p>
		{:else}
			<div class="space-y-2">
				{#each sessions as session}
					<div class="rounded-lg border border-slate-200 p-3 text-sm dark:border-slate-700">
						<div class="font-medium text-slate-900 dark:text-white">{session.ledgerName}</div>
						<div class="break-all text-xs text-slate-500 dark:text-slate-400">{session.sessionToken}</div>
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>
