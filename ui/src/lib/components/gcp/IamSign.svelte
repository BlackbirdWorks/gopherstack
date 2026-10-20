<script lang="ts">
	import { PenLine, Copy } from 'lucide-svelte';
	import { GCP_INPUT, GCP_PRIMARY, GCP_GHOST, attempt, copyText } from '#lib/gcp-action.ts';
	import { signBlob, type AccountInfo, type SignResult } from '#lib/gcpiam.ts';

	type Props = { project: string; accounts: AccountInfo[]; selected: string };

	let { project, accounts, selected = $bindable() }: Props = $props();

	let text = $state('hello from gopherstack');
	let result = $state<SignResult | null>(null);
	let busy = $state(false);

	const account = $derived(accounts.find((a) => a.email === selected));

	async function sign(event: Event) {
		event.preventDefault();
		busy = true;
		result = null;
		let out: SignResult | null = null;
		await attempt(async () => (out = await signBlob(project, selected, text)), '', 'Failed to sign');
		busy = false;
		result = out;
	}
</script>

<div class="max-w-3xl space-y-4" data-testid="iam-sign">
	<p class="text-sm text-gray-600 dark:text-gray-300">Sign bytes with a service account's system-managed key (RSASSA-PKCS1-v1_5 over SHA-256) using <code>SignBlob</code>.</p>
	{#if accounts.length === 0}
		<p class="py-8 text-center text-gray-500 dark:text-gray-400">Create a service account first.</p>
	{:else}
		<form class="space-y-3" onsubmit={sign}>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Service account</span>
				<select class="{GCP_INPUT} font-mono text-xs" bind:value={selected} aria-label="Signing account" data-testid="iam-sign-account">
					{#each accounts as a (a.uniqueId)}<option value={a.email}>{a.email}{a.disabled ? ' (disabled)' : ''}</option>{/each}
				</select>
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Text to sign</span>
				<textarea class="{GCP_INPUT} font-mono" rows="4" bind:value={text} aria-label="Text to sign" data-testid="iam-sign-text"></textarea>
			</label>
			{#if account?.disabled}<p class="text-xs text-amber-700 dark:text-amber-400">This account is disabled; signing will be rejected.</p>{/if}
			<button type="submit" class="flex items-center gap-2 {GCP_PRIMARY}" disabled={busy || !selected} data-testid="iam-sign-submit"><PenLine class="h-4 w-4" /> {busy ? 'Signing…' : 'Sign'}</button>
		</form>

		{#if result}
			{@const signed = result}
			<div class="space-y-2 rounded-md border border-gray-200 dark:border-gray-700 p-3" data-testid="iam-sign-result">
				<p class="text-xs"><span class="font-medium text-gray-500 dark:text-gray-400">Key ID</span> <span class="font-mono" data-testid="iam-sign-keyid">{signed.keyId}</span></p>
				<p class="text-xs font-medium text-gray-500 dark:text-gray-400">Signature (base64)</p>
				<pre class="max-h-40 overflow-auto whitespace-pre-wrap break-all rounded-md bg-gray-50 dark:bg-slate-900 p-2 font-mono text-xs" data-testid="iam-sign-signature">{signed.signature}</pre>
				<button type="button" class={GCP_GHOST} onclick={() => copyText(signed.signature)}><Copy class="inline h-4 w-4" /> Copy signature</button>
			</div>
		{/if}
	{/if}
</div>
