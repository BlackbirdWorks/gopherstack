<script lang="ts">
	import { onMount } from 'svelte';
	import { getSTSClient } from '$lib/aws-client';
	import {
		DecodeAuthorizationMessageCommand,
		GetAccessKeyInfoCommand,
		GetCallerIdentityCommand,
		GetSessionTokenCommand
	} from '@aws-sdk/client-sts';
	import { toast } from 'svelte-sonner';
	import { User, RefreshCw, Key, Shield, Copy } from 'lucide-svelte';

	const sts = getSTSClient();

	let loading = $state(false);
	let identity = $state<{ Account?: string; Arn?: string; UserId?: string } | null>(null);
	let sessionToken = $state<{ AccessKeyId?: string; SecretAccessKey?: string; SessionToken?: string; Expiration?: Date } | null>(null);
	let validatorAccessKeyId = $state('');
	let validatorAccount = $state('');
	let encodedMessage = $state('');
	let decodedMessage = $state('');
	let stsMetrics = $state<{ activeSessions: number; expiredSessions: number; sweepCount: number; expiredEvictions: number } | null>(null);

	async function loadIdentity() {
		loading = true;
		try {
			const resp = await sts.send(new GetCallerIdentityCommand({}));
			identity = { Account: resp.Account, Arn: resp.Arn, UserId: resp.UserId };
		} catch (e) {
			toast.error('Failed to get caller identity: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function generateSessionToken() {
		try {
			const resp = await sts.send(new GetSessionTokenCommand({ DurationSeconds: 3600 }));
			sessionToken = resp.Credentials ?? null;
			validatorAccessKeyId = sessionToken?.AccessKeyId ?? '';
			toast.success('Session token generated successfully');
			await loadSTSMetrics();
		} catch (e) {
			toast.error('Failed to generate session token: ' + String(e));
		}
	}

	async function copyToClipboard(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Failed to copy');
		}
	}

	async function validateSessionToken() {
		if (!validatorAccessKeyId.trim()) {
			toast.error('Access Key ID is required');
			return;
		}

		try {
			const resp = await sts.send(new GetAccessKeyInfoCommand({ AccessKeyId: validatorAccessKeyId.trim() }));
			validatorAccount = resp.Account ?? '';
			toast.success('Session token is valid');
		} catch (e) {
			validatorAccount = '';
			toast.error('Failed to validate session token: ' + String(e));
		}
	}

	async function decodeAuthMessage() {
		if (!encodedMessage.trim()) {
			toast.error('Encoded authorization message is required');
			return;
		}

		try {
			const resp = await sts.send(new DecodeAuthorizationMessageCommand({ EncodedMessage: encodedMessage.trim() }));
			decodedMessage = resp.DecodedMessage ?? '';
			toast.success('Authorization message decoded');
		} catch (e) {
			decodedMessage = '';
			toast.error('Failed to decode message: ' + String(e));
		}
	}

	async function loadSTSMetrics() {
		try {
			const resp = await fetch('/dashboard/api/sts/metrics');
			if (!resp.ok) {
				return;
			}

			stsMetrics = await resp.json();
		} catch {
			stsMetrics = null;
		}
	}

	onMount(async () => {
		await loadIdentity();
		await loadSTSMetrics();
	});
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="w-7 h-7 text-yellow-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Security Token Service</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Manage temporary security credentials for AWS resources</p>
			</div>
		</div>
		<button onclick={loadIdentity} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	{#if loading}
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading identity...</div>
	{:else if identity}
		<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<div class="flex items-center gap-2 mb-2">
					<User class="w-5 h-5 text-yellow-500" />
					<p class="text-sm font-semibold text-gray-700 dark:text-gray-300">Account</p>
				</div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{identity.Account}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<div class="flex items-center gap-2 mb-2">
					<User class="w-5 h-5 text-blue-500" />
					<p class="text-sm font-semibold text-gray-700 dark:text-gray-300">User ID</p>
				</div>
				<p class="text-sm font-mono text-gray-900 dark:text-white break-all">{identity.UserId}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<div class="flex items-center gap-2 mb-2">
					<Shield class="w-5 h-5 text-green-500" />
					<p class="text-sm font-semibold text-gray-700 dark:text-gray-300">ARN</p>
				</div>
				<p class="text-sm font-mono text-gray-900 dark:text-white break-all">{identity.Arn}</p>
			</div>
		</div>
	{/if}

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
		<div class="flex items-center justify-between mb-4">
			<div class="flex items-center gap-2">
				<Key class="w-5 h-5 text-yellow-500" />
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Session Token</h2>
			</div>
			<button onclick={generateSessionToken} class="flex items-center gap-2 px-4 py-2 bg-yellow-500 hover:bg-yellow-600 text-white rounded-lg text-sm font-medium">
				<Key class="w-4 h-4" /> Generate Session Token
			</button>
		</div>

		{#if sessionToken}
			<div class="space-y-3 bg-gray-50 dark:bg-slate-700/50 rounded-lg p-4">
				<div class="flex items-center justify-between">
					<div>
						<p class="text-xs text-gray-500 dark:text-gray-400">Access Key ID</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white">{sessionToken.AccessKeyId}</p>
					</div>
					<button onclick={() => copyToClipboard(sessionToken?.AccessKeyId ?? '')} class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded" title="Copy Access Key ID"><Copy class="w-4 h-4 text-gray-400" /></button>
				</div>
				<div class="flex items-center justify-between">
					<div>
						<p class="text-xs text-gray-500 dark:text-gray-400">Secret Access Key</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white">{'•'.repeat(40)}</p>
					</div>
					<button onclick={() => copyToClipboard(sessionToken?.SecretAccessKey ?? '')} class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded" title="Copy Secret Access Key"><Copy class="w-4 h-4 text-gray-400" /></button>
				</div>
				<div class="flex items-center justify-between">
					<div>
						<p class="text-xs text-gray-500 dark:text-gray-400">Session Token</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white">{sessionToken.SessionToken ? `${sessionToken.SessionToken.slice(0, 16)}...` : ''}</p>
					</div>
					<button onclick={() => copyToClipboard(sessionToken?.SessionToken ?? '')} class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded" title="Copy Session Token"><Copy class="w-4 h-4 text-gray-400" /></button>
				</div>
				<div>
					<p class="text-xs text-gray-500 dark:text-gray-400">Expiration</p>
					<p class="text-sm text-gray-900 dark:text-white">{sessionToken.Expiration?.toISOString()}</p>
				</div>
			</div>
		{:else}
			<p class="text-gray-500 dark:text-gray-400 text-sm">Click Generate Session Token to create temporary credentials with a 1-hour expiration.</p>
		{/if}
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
		<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Session Token Validator</h2>
		<div class="flex flex-col md:flex-row gap-2">
			<input bind:value={validatorAccessKeyId} placeholder="ASIA..." class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
			<button onclick={validateSessionToken} class="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-sm font-medium whitespace-nowrap">Validate</button>
		</div>
		{#if validatorAccount}
			<p class="text-sm text-green-600 dark:text-green-400">Valid session credentials for account {validatorAccount}</p>
		{/if}
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
		<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Decode Authorization Message</h2>
		<textarea bind:value={encodedMessage} rows="4" placeholder="Paste base64 encoded authorization message" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono"></textarea>
		<div class="flex gap-2">
			<button onclick={decodeAuthMessage} class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm font-medium">Decode</button>
			{#if decodedMessage}
				<button onclick={() => copyToClipboard(decodedMessage)} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-slate-50 dark:hover:bg-slate-700/70">Copy Decoded Message</button>
			{/if}
		</div>
		{#if decodedMessage}
			<pre class="text-xs whitespace-pre-wrap bg-gray-50 dark:bg-slate-700/50 rounded-lg p-3 text-gray-900 dark:text-white">{decodedMessage}</pre>
		{/if}
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
		<div class="flex items-center justify-between mb-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">STS Janitor Metrics</h2>
			<button onclick={loadSTSMetrics} class="text-sm text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white">Refresh metrics</button>
		</div>
		{#if stsMetrics}
			<div class="grid grid-cols-2 md:grid-cols-4 gap-3">
				<div class="rounded border border-slate-200 dark:border-slate-700 p-3">
					<p class="text-xs text-gray-500 dark:text-gray-400">Active sessions</p>
					<p class="text-lg font-semibold text-gray-900 dark:text-white">{stsMetrics.activeSessions}</p>
				</div>
				<div class="rounded border border-slate-200 dark:border-slate-700 p-3">
					<p class="text-xs text-gray-500 dark:text-gray-400">Expired sessions</p>
					<p class="text-lg font-semibold text-gray-900 dark:text-white">{stsMetrics.expiredSessions}</p>
				</div>
				<div class="rounded border border-slate-200 dark:border-slate-700 p-3">
					<p class="text-xs text-gray-500 dark:text-gray-400">Sweep count</p>
					<p class="text-lg font-semibold text-gray-900 dark:text-white">{stsMetrics.sweepCount}</p>
				</div>
				<div class="rounded border border-slate-200 dark:border-slate-700 p-3">
					<p class="text-xs text-gray-500 dark:text-gray-400">Expired evictions</p>
					<p class="text-lg font-semibold text-gray-900 dark:text-white">{stsMetrics.expiredEvictions}</p>
				</div>
			</div>
		{:else}
			<p class="text-gray-500 dark:text-gray-400 text-sm">Metrics are unavailable.</p>
		{/if}
	</div>
</div>
