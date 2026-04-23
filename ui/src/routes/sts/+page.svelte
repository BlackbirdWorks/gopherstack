<script lang="ts">
	import { onMount } from 'svelte';
	import { getSTSClient } from '$lib/aws-client';
	import {
		AssumeRoleCommand,
		DecodeAuthorizationMessageCommand,
		GetAccessKeyInfoCommand,
		GetCallerIdentityCommand,
		GetSessionTokenCommand
	} from '@aws-sdk/client-sts';
	import { toast } from 'svelte-sonner';
	import { Eye, EyeOff, Key, RefreshCw, Shield, User, Copy, CheckCircle } from 'lucide-svelte';

	const sts = getSTSClient();

	let loading = $state(false);
	let identity = $state<{ Account?: string; Arn?: string; UserId?: string } | null>(null);
	let sessionToken =
		$state<{
			AccessKeyId?: string;
			SecretAccessKey?: string;
			SessionToken?: string;
			Expiration?: Date;
		} | null>(null);
	let revealSecret = $state(false);
	let tokenDuration = $state(3600);
	let isGenerating = $state(false);

	let validatorAccessKeyId = $state('');
	let validatorAccount = $state('');
	let isValidating = $state(false);

	let encodedMessage = $state('');
	let decodedMessage = $state('');
	let prettyDecodedMessage = $state('');
	let isDecoding = $state(false);

	let assumeRoleArn = $state('arn:aws:iam::000000000000:role/MyRole');
	let assumeRoleSessionName = $state('my-session');
	let assumedCredentials =
		$state<{
			AccessKeyId?: string;
			SecretAccessKey?: string;
			SessionToken?: string;
			Expiration?: Date;
		} | null>(null);
	let isAssuming = $state(false);
	let revealAssumedSecret = $state(false);

	let stsMetrics = $state<{
		activeSessions: number;
		expiredSessions: number;
		sweepCount: number;
		expiredEvictions: number;
		totalSessionsCreated?: number;
		opsAssumeRole?: number;
		opsGetSessionToken?: number;
		opsGetCallerIdentity?: number;
		opsGetFederationToken?: number;
		opsAssumeRoleWithSAML?: number;
		opsAssumeRoleWithWebIdentity?: number;
		opsGetAccessKeyInfo?: number;
		opsDecodeAuthorizationMessage?: number;
	} | null>(null);

	// Relative time countdown for expiration
	function formatRelativeExpiration(exp: Date | undefined): string {
		if (!exp) return '';
		const ms = exp.getTime() - Date.now();
		if (ms <= 0) return 'Expired';
		const totalSec = Math.floor(ms / 1000);
		const h = Math.floor(totalSec / 3600);
		const m = Math.floor((totalSec % 3600) / 60);
		const s = totalSec % 60;
		if (h > 0) return `expires in ${h}h ${m}m`;
		if (m > 0) return `expires in ${m}m ${s}s`;
		return `expires in ${s}s`;
	}

	// Build AWS CLI export snippet from credentials
	function buildEnvVars(
		creds: {
			AccessKeyId?: string;
			SecretAccessKey?: string;
			SessionToken?: string;
		} | null
	): string {
		if (!creds) return '';
		return [
			`export AWS_ACCESS_KEY_ID=${creds.AccessKeyId ?? ''}`,
			`export AWS_SECRET_ACCESS_KEY=${creds.SecretAccessKey ?? ''}`,
			`export AWS_SESSION_TOKEN=${creds.SessionToken ?? ''}`
		].join('\n');
	}

	// Build JSON snippet from credentials
	function buildCredJSON(
		creds: {
			AccessKeyId?: string;
			SecretAccessKey?: string;
			SessionToken?: string;
			Expiration?: Date;
		} | null
	): string {
		if (!creds) return '';
		return JSON.stringify(
			{
				AccessKeyId: creds.AccessKeyId,
				SecretAccessKey: creds.SecretAccessKey,
				SessionToken: creds.SessionToken,
				Expiration: creds.Expiration?.toISOString()
			},
			null,
			2
		);
	}

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
		isGenerating = true;
		try {
			const resp = await sts.send(
				new GetSessionTokenCommand({ DurationSeconds: Math.round(tokenDuration) })
			);
			sessionToken = resp.Credentials ?? null;
			validatorAccessKeyId = sessionToken?.AccessKeyId ?? '';
			revealSecret = false;
			toast.success('Session token generated');
			await loadSTSMetrics();
		} catch (e) {
			toast.error('Failed to generate session token: ' + String(e));
		} finally {
			isGenerating = false;
		}
	}

	async function assumeRole() {
		if (!assumeRoleArn.trim() || !assumeRoleSessionName.trim()) {
			toast.error('Role ARN and session name are required');
			return;
		}
		isAssuming = true;
		try {
			const resp = await sts.send(
				new AssumeRoleCommand({
					RoleArn: assumeRoleArn.trim(),
					RoleSessionName: assumeRoleSessionName.trim()
				})
			);
			assumedCredentials = resp.Credentials ?? null;
			revealAssumedSecret = false;
			toast.success('Role assumed successfully');
			await loadSTSMetrics();
		} catch (e) {
			toast.error('AssumeRole failed: ' + String(e));
		} finally {
			isAssuming = false;
		}
	}

	async function copyToClipboard(text: string, label = 'value') {
		try {
			await navigator.clipboard.writeText(text);
			toast.success(`Copied ${label} to clipboard`);
		} catch {
			toast.error('Failed to copy');
		}
	}

	async function validateSessionToken() {
		if (!validatorAccessKeyId.trim()) {
			toast.error('Access Key ID is required');
			return;
		}
		isValidating = true;
		try {
			const resp = await sts.send(
				new GetAccessKeyInfoCommand({ AccessKeyId: validatorAccessKeyId.trim() })
			);
			validatorAccount = resp.Account ?? '';
			toast.success('Session token is valid');
		} catch (e) {
			validatorAccount = '';
			toast.error('Failed to validate: ' + String(e));
		} finally {
			isValidating = false;
		}
	}

	function clearValidator() {
		validatorAccessKeyId = '';
		validatorAccount = '';
	}

	async function decodeAuthMessage() {
		if (!encodedMessage.trim()) {
			toast.error('Encoded authorization message is required');
			return;
		}
		isDecoding = true;
		try {
			const resp = await sts.send(
				new DecodeAuthorizationMessageCommand({ EncodedMessage: encodedMessage.trim() })
			);
			decodedMessage = resp.DecodedMessage ?? '';
			try {
				prettyDecodedMessage = JSON.stringify(JSON.parse(decodedMessage), null, 2);
			} catch {
				prettyDecodedMessage = decodedMessage;
			}
			toast.success('Authorization message decoded');
		} catch (e) {
			decodedMessage = '';
			prettyDecodedMessage = '';
			toast.error('Failed to decode message: ' + String(e));
		} finally {
			isDecoding = false;
		}
	}

	function clearDecoder() {
		encodedMessage = '';
		decodedMessage = '';
		prettyDecodedMessage = '';
	}

	async function loadSTSMetrics() {
		try {
			const resp = await fetch('/dashboard/api/sts/metrics');
			if (!resp.ok) return;
			stsMetrics = await resp.json();
		} catch {
			stsMetrics = null;
		}
	}

	// Identity cards definition extracted for readability.
	type IdentityCard = { label: string; value: string | undefined; icon: typeof User; color: string };
	const identityCards = $derived<IdentityCard[]>(
		identity
			? [
					{ label: 'Account', value: identity.Account, icon: User, color: 'text-yellow-500' },
					{ label: 'User ID', value: identity.UserId, icon: User, color: 'text-blue-500' },
					{ label: 'ARN', value: identity.Arn, icon: Shield, color: 'text-green-500' }
				]
			: []
	);

	// Session metric summary cards extracted for readability.
	type MetricCard = { label: string; value: number; color: string };
	const sessionMetricCards = $derived<MetricCard[]>(
		stsMetrics
			? [
					{
						label: 'Active sessions',
						value: stsMetrics.activeSessions,
						color: 'text-green-600 dark:text-green-400'
					},
					{ label: 'Expired sessions', value: stsMetrics.expiredSessions, color: 'text-red-500' },
					{
						label: 'Total created',
						value: stsMetrics.totalSessionsCreated ?? 0,
						color: 'text-blue-600 dark:text-blue-400'
					},
					{
						label: 'Janitor sweeps',
						value: stsMetrics.sweepCount,
						color: 'text-gray-700 dark:text-white'
					},
					{
						label: 'Expired evictions',
						value: stsMetrics.expiredEvictions,
						color: 'text-orange-500'
					}
				]
			: []
	);

	// Per-operation call counter cards extracted for readability.
	type OpCard = { label: string; value: number };
	const opCountCards = $derived<OpCard[]>(
		stsMetrics
			? [
					{ label: 'AssumeRole', value: stsMetrics.opsAssumeRole ?? 0 },
					{ label: 'GetSessionToken', value: stsMetrics.opsGetSessionToken ?? 0 },
					{ label: 'GetCallerIdentity', value: stsMetrics.opsGetCallerIdentity ?? 0 },
					{ label: 'GetFederationToken', value: stsMetrics.opsGetFederationToken ?? 0 },
					{ label: 'AssumeRoleWithSAML', value: stsMetrics.opsAssumeRoleWithSAML ?? 0 },
					{ label: 'AssumeRoleWithWI', value: stsMetrics.opsAssumeRoleWithWebIdentity ?? 0 },
					{ label: 'GetAccessKeyInfo', value: stsMetrics.opsGetAccessKeyInfo ?? 0 },
					{ label: 'DecodeAuthMessage', value: stsMetrics.opsDecodeAuthorizationMessage ?? 0 }
				]
			: []
	);

	const hasOpCounts = $derived(opCountCards.some((c) => c.value > 0));

	onMount(async () => {
		await Promise.all([loadIdentity(), loadSTSMetrics()]);
	});
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Shield class="w-7 h-7 text-yellow-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Security Token Service</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">
					Manage temporary security credentials for AWS resources
				</p>
			</div>
		</div>
		<button
			onclick={loadIdentity}
			title="Refresh"
			class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
		>
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<!-- Caller Identity -->
	{#if loading}
		<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading identity...</div>
	{:else if identity}
		<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
			{#each identityCards as card}
				<div
					class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4"
				>
					<div class="flex items-center justify-between mb-2">
						<div class="flex items-center gap-2">
							<card.icon class="w-5 h-5 {card.color}" />
							<p class="text-sm font-semibold text-gray-700 dark:text-gray-300">{card.label}</p>
						</div>
						<button
							onclick={() => copyToClipboard(card.value ?? '', card.label)}
							class="p-1 hover:bg-gray-100 dark:hover:bg-slate-700 rounded"
							title="Copy {card.label}"
						>
							<Copy class="w-3.5 h-3.5 text-gray-400" />
						</button>
					</div>
					<p class="text-sm font-mono text-gray-900 dark:text-white break-all">{card.value}</p>
				</div>
			{/each}
		</div>
	{/if}

	<!-- Session Token Generator -->
	<div
		class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6"
	>
		<div class="flex items-center justify-between mb-4">
			<div class="flex items-center gap-2">
				<Key class="w-5 h-5 text-yellow-500" />
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Session Token</h2>
			</div>
		</div>

		<!-- Duration selector -->
		<div class="mb-4 space-y-1">
			<label
				for="token-duration"
				class="text-xs text-gray-500 dark:text-gray-400 flex justify-between"
			>
				<span>Duration (seconds)</span>
				<span class="font-medium text-gray-700 dark:text-gray-300"
					>{tokenDuration}s ({Math.round(tokenDuration / 60)}m)</span
				>
			</label>
			<input
				id="token-duration"
				type="range"
				bind:value={tokenDuration}
				min="900"
				max="43200"
				step="900"
				class="w-full accent-yellow-500"
			/>
			<div class="flex justify-between text-xs text-gray-400">
				<span>15m</span>
				<span>12h</span>
			</div>
		</div>

		<button
			onclick={generateSessionToken}
			disabled={isGenerating}
			class="flex items-center gap-2 px-4 py-2 bg-yellow-500 hover:bg-yellow-600 disabled:opacity-60 text-white rounded-lg text-sm font-medium mb-4"
		>
			<Key class="w-4 h-4" />
			{isGenerating ? 'Generating...' : 'Generate Session Token'}
		</button>

		{#if sessionToken}
			<div class="space-y-3 bg-gray-50 dark:bg-slate-700/50 rounded-lg p-4">
				<!-- Access Key ID -->
				<div class="flex items-center justify-between">
					<div>
						<p class="text-xs text-gray-500 dark:text-gray-400">Access Key ID</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white">{sessionToken.AccessKeyId}</p>
					</div>
					<button
						onclick={() => copyToClipboard(sessionToken?.AccessKeyId ?? '', 'Access Key ID')}
						class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
						title="Copy Access Key ID"
					>
						<Copy class="w-4 h-4 text-gray-400" />
					</button>
				</div>
				<!-- Secret Access Key with reveal toggle -->
				<div class="flex items-center justify-between">
					<div class="flex-1 min-w-0">
						<p class="text-xs text-gray-500 dark:text-gray-400">Secret Access Key</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white break-all">
							{revealSecret ? sessionToken.SecretAccessKey : '•'.repeat(40)}
						</p>
					</div>
					<div class="flex items-center gap-1 ml-2 flex-shrink-0">
						<button
							onclick={() => (revealSecret = !revealSecret)}
							class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
							title="{revealSecret ? 'Hide' : 'Reveal'} Secret Access Key"
						>
							{#if revealSecret}
								<EyeOff class="w-4 h-4 text-gray-400" />
							{:else}
								<Eye class="w-4 h-4 text-gray-400" />
							{/if}
						</button>
						<button
							onclick={() =>
								copyToClipboard(sessionToken?.SecretAccessKey ?? '', 'Secret Access Key')}
							class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
							title="Copy Secret Access Key"
						>
							<Copy class="w-4 h-4 text-gray-400" />
						</button>
					</div>
				</div>
				<!-- Session Token preview -->
				<div class="flex items-center justify-between">
					<div>
						<p class="text-xs text-gray-500 dark:text-gray-400">Session Token</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white">
							{sessionToken.SessionToken ? `${sessionToken.SessionToken.slice(0, 20)}…` : ''}
						</p>
					</div>
					<button
						onclick={() => copyToClipboard(sessionToken?.SessionToken ?? '', 'Session Token')}
						class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
						title="Copy Session Token"
					>
						<Copy class="w-4 h-4 text-gray-400" />
					</button>
				</div>
				<!-- Expiration with relative time -->
				<div>
					<p class="text-xs text-gray-500 dark:text-gray-400">Expiration</p>
					<p class="text-sm text-gray-900 dark:text-white">
						{sessionToken.Expiration?.toISOString()}
						<span class="text-xs text-yellow-600 dark:text-yellow-400 ml-1"
							>({formatRelativeExpiration(sessionToken.Expiration)})</span
						>
					</p>
				</div>
				<!-- Copy All buttons -->
				<div class="flex gap-2 pt-2 border-t border-gray-200 dark:border-gray-600">
					<button
						onclick={() => copyToClipboard(buildEnvVars(sessionToken), 'environment variables')}
						class="flex items-center gap-1 px-3 py-1.5 text-xs border border-slate-200 dark:border-slate-600 rounded hover:bg-slate-50 dark:hover:bg-slate-700 text-gray-700 dark:text-gray-300"
					>
						<Copy class="w-3 h-3" /> Copy as env vars
					</button>
					<button
						onclick={() => copyToClipboard(buildCredJSON(sessionToken), 'JSON credentials')}
						class="flex items-center gap-1 px-3 py-1.5 text-xs border border-slate-200 dark:border-slate-600 rounded hover:bg-slate-50 dark:hover:bg-slate-700 text-gray-700 dark:text-gray-300"
					>
						<Copy class="w-3 h-3" /> Copy as JSON
					</button>
				</div>
			</div>
		{:else}
			<p class="text-gray-500 dark:text-gray-400 text-sm">
				Click Generate Session Token to create temporary credentials.
			</p>
		{/if}
	</div>

	<!-- Assume Role -->
	<div
		class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4"
	>
		<div class="flex items-center gap-2">
			<Shield class="w-5 h-5 text-indigo-500" />
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Assume Role</h2>
		</div>
		<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
			<div>
				<label
					for="assume-role-arn"
					class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Role ARN</label
				>
				<input
					id="assume-role-arn"
					bind:value={assumeRoleArn}
					placeholder="arn:aws:iam::000000000000:role/MyRole"
					class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm"
				/>
			</div>
			<div>
				<label
					for="assume-role-session"
					class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Session Name</label
				>
				<input
					id="assume-role-session"
					bind:value={assumeRoleSessionName}
					placeholder="my-session"
					class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm"
				/>
			</div>
		</div>
		<button
			onclick={assumeRole}
			disabled={isAssuming}
			class="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium"
		>
			<Shield class="w-4 h-4" />
			{isAssuming ? 'Assuming...' : 'Assume Role'}
		</button>

		{#if assumedCredentials}
			<div class="space-y-3 bg-gray-50 dark:bg-slate-700/50 rounded-lg p-4">
				<div class="flex items-center justify-between">
					<div>
						<p class="text-xs text-gray-500 dark:text-gray-400">Access Key ID</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white">
							{assumedCredentials.AccessKeyId}
						</p>
					</div>
					<button
						onclick={() =>
							copyToClipboard(assumedCredentials?.AccessKeyId ?? '', 'Access Key ID')}
						class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
					>
						<Copy class="w-4 h-4 text-gray-400" />
					</button>
				</div>
				<div class="flex items-center justify-between">
					<div class="flex-1 min-w-0">
						<p class="text-xs text-gray-500 dark:text-gray-400">Secret Access Key</p>
						<p class="text-sm font-mono text-gray-900 dark:text-white break-all">
							{revealAssumedSecret ? assumedCredentials.SecretAccessKey : '•'.repeat(40)}
						</p>
					</div>
					<div class="flex items-center gap-1 ml-2 flex-shrink-0">
						<button
							onclick={() => (revealAssumedSecret = !revealAssumedSecret)}
							class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
							title="{revealAssumedSecret ? 'Hide' : 'Reveal'} Secret"
						>
							{#if revealAssumedSecret}
								<EyeOff class="w-4 h-4 text-gray-400" />
							{:else}
								<Eye class="w-4 h-4 text-gray-400" />
							{/if}
						</button>
						<button
							onclick={() =>
								copyToClipboard(assumedCredentials?.SecretAccessKey ?? '', 'Secret Access Key')}
							class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
						>
							<Copy class="w-4 h-4 text-gray-400" />
						</button>
					</div>
				</div>
				<div>
					<p class="text-xs text-gray-500 dark:text-gray-400">Expiration</p>
					<p class="text-sm text-gray-900 dark:text-white">
						{assumedCredentials.Expiration?.toISOString()}
						<span class="text-xs text-indigo-500 ml-1"
							>({formatRelativeExpiration(assumedCredentials.Expiration)})</span
						>
					</p>
				</div>
				<div class="flex gap-2 pt-2 border-t border-gray-200 dark:border-gray-600">
					<button
						onclick={() =>
							copyToClipboard(buildEnvVars(assumedCredentials), 'environment variables')}
						class="flex items-center gap-1 px-3 py-1.5 text-xs border border-slate-200 dark:border-slate-600 rounded hover:bg-slate-50 dark:hover:bg-slate-700 text-gray-700 dark:text-gray-300"
					>
						<Copy class="w-3 h-3" /> Copy as env vars
					</button>
					<button
						onclick={() =>
							copyToClipboard(buildCredJSON(assumedCredentials), 'JSON credentials')}
						class="flex items-center gap-1 px-3 py-1.5 text-xs border border-slate-200 dark:border-slate-600 rounded hover:bg-slate-50 dark:hover:bg-slate-700 text-gray-700 dark:text-gray-300"
					>
						<Copy class="w-3 h-3" /> Copy as JSON
					</button>
				</div>
			</div>
		{/if}
	</div>

	<!-- Validator -->
	<div
		class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4"
	>
		<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Session Token Validator</h2>
		<div class="flex flex-col md:flex-row gap-2">
			<input
				bind:value={validatorAccessKeyId}
				placeholder="ASIA..."
				class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm"
			/>
			<button
				onclick={validateSessionToken}
				disabled={isValidating}
				class="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium whitespace-nowrap"
			>
				{isValidating ? 'Validating...' : 'Validate'}
			</button>
			{#if validatorAccessKeyId || validatorAccount}
				<button
					onclick={clearValidator}
					class="px-3 py-2 border border-slate-200 dark:border-slate-700 text-gray-600 dark:text-gray-300 rounded-lg text-sm hover:bg-slate-50 dark:hover:bg-slate-700"
				>
					Clear
				</button>
			{/if}
		</div>
		{#if validatorAccount}
			<div class="flex items-center gap-2 text-sm text-green-600 dark:text-green-400">
				<CheckCircle class="w-4 h-4" />
				<span>Valid session credentials for account {validatorAccount}</span>
			</div>
		{/if}
	</div>

	<!-- Decode Authorization Message -->
	<div
		class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4"
	>
		<h2 class="text-lg font-semibold text-gray-900 dark:text-white">
			Decode Authorization Message
		</h2>
		<textarea
			bind:value={encodedMessage}
			rows="4"
			placeholder="Paste base64 encoded authorization message"
			class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono"
		></textarea>
		<div class="flex gap-2 flex-wrap">
			<button
				onclick={decodeAuthMessage}
				disabled={isDecoding}
				class="px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium"
			>
				{isDecoding ? 'Decoding...' : 'Decode'}
			</button>
			{#if decodedMessage}
				<button
					onclick={() => copyToClipboard(prettyDecodedMessage || decodedMessage, 'Decoded Message')}
					class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-slate-50 dark:hover:bg-slate-700/70"
				>
					Copy Decoded Message
				</button>
			{/if}
			{#if encodedMessage || decodedMessage}
				<button
					onclick={clearDecoder}
					class="px-3 py-2 border border-slate-200 dark:border-slate-700 text-gray-600 dark:text-gray-300 rounded-lg text-sm hover:bg-slate-50 dark:hover:bg-slate-700"
				>
					Clear
				</button>
			{/if}
		</div>
		{#if prettyDecodedMessage}
			<pre
				class="text-xs whitespace-pre-wrap bg-gray-50 dark:bg-slate-700/50 rounded-lg p-3 text-gray-900 dark:text-white overflow-x-auto max-h-64"
			>{prettyDecodedMessage}</pre>
		{/if}
	</div>

	<!-- Metrics -->
	<div
		class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6"
	>
		<div class="flex items-center justify-between mb-4">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">STS Janitor Metrics</h2>
			<button
				onclick={loadSTSMetrics}
				class="text-sm text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white"
				>Refresh metrics</button
			>
		</div>
		{#if stsMetrics}
			<div class="space-y-4">
				<!-- Session counts -->
				<div class="grid grid-cols-2 md:grid-cols-4 gap-3">
					{#each sessionMetricCards as m}
						<div class="rounded border border-slate-200 dark:border-slate-700 p-3">
							<p class="text-xs text-gray-500 dark:text-gray-400">{m.label}</p>
							<p class="text-lg font-semibold {m.color}">{m.value}</p>
						</div>
					{/each}
				</div>
				<!-- Operation counters -->
				{#if hasOpCounts}
					<div>
						<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">
							Operation call counts
						</p>
						<div class="grid grid-cols-2 md:grid-cols-4 gap-2">
							{#each opCountCards as op}
								<div class="rounded border border-slate-200 dark:border-slate-700 p-2">
									<p class="text-xs text-gray-500 dark:text-gray-400 truncate">{op.label}</p>
									<p class="text-base font-semibold text-gray-900 dark:text-white">{op.value}</p>
								</div>
							{/each}
						</div>
					</div>
				{/if}
			</div>
		{:else}
			<p class="text-gray-500 dark:text-gray-400 text-sm">
				No metrics yet — generate credentials to see session stats.
			</p>
		{/if}
	</div>
</div>
