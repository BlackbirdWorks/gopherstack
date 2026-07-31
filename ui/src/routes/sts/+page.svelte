<script lang="ts">
	// STS is a credential-issuing service, not a resource-management one:
	// AssumeRole, AssumeRoleWithSAML, AssumeRoleWithWebIdentity, AssumeRoot,
	// GetSessionToken, GetFederationToken, GetWebIdentityToken, and
	// GetDelegatedAccessToken all *issue* short-lived credentials or tokens —
	// nothing is created/updated/deleted/listed in the CRUD sense, and
	// credentials expire rather than get "deleted". GetCallerIdentity answers
	// "who am I right now", and GetAccessKeyInfo/DecodeAuthorizationMessage are
	// read-only inspection utilities. So this page is organized around those
	// three real shapes instead of a forced CRUD floor:
	//   - Identity: GetCallerIdentity plus server-side SessionMetrics (from
	//     /dashboard/api/sts/metrics) — aggregate counters only (active/expired
	//     session counts, janitor sweep counts, per-op call counts). The
	//     backend (services/sts/handler.go's SessionMetrics()) does NOT expose
	//     a per-session list API — no session IDs, ARNs, or expiries are
	//     retrievable in bulk — so this page does not invent one.
	//   - Assume Role: the four operations that exchange some other form of
	//     trust (a role ARN + optional ExternalId, a SAML assertion, a web
	//     identity token, or root-task delegation) for temporary credentials.
	//   - Get Token: the four operations that issue credentials/tokens
	//     directly from the caller's own identity (session token, federation
	//     token, a signed JWT, or a delegated-access trade-in token).
	//   - Utilities: GetAccessKeyInfo (look up the account behind an access
	//     key) and DecodeAuthorizationMessage (decode an encoded AWS error).
	// "Credentials issued this session" below the tabs is real, but real in a
	// narrower sense than a backend-tracked history: since STS has no list
	// API for it, it is this browser tab's own record of the credentials IT
	// personally requested, kept in plain $state (not localStorage, not
	// logged) and cleared on reload — never presented as server truth.
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getSTSClient } from '$lib/aws-client';
	import {
		AssumeRoleCommand,
		AssumeRoleWithSAMLCommand,
		AssumeRoleWithWebIdentityCommand,
		AssumeRootCommand,
		DecodeAuthorizationMessageCommand,
		GetAccessKeyInfoCommand,
		GetCallerIdentityCommand,
		GetDelegatedAccessTokenCommand,
		GetFederationTokenCommand,
		GetSessionTokenCommand,
		GetWebIdentityTokenCommand,
		type Credentials
	} from '@aws-sdk/client-sts';
	import { toast } from 'svelte-sonner';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import {
		Shield,
		Key,
		User,
		Users,
		Globe,
		Lock,
		Crown,
		FileKey,
		ArrowRightLeft,
		CheckCircle,
		History,
		Copy,
		Eye,
		EyeOff
	} from 'lucide-svelte';

	const client = regionalClient(getSTSClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	async function copyToClipboard(text: string, label = 'value'): Promise<void> {
		try {
			await navigator.clipboard.writeText(text);
			toast.success(`Copied ${label} to clipboard`);
		} catch {
			toast.error('Failed to copy');
		}
	}

	function formatDuration(seconds: number): string {
		const h = Math.floor(seconds / 3600);
		const m = Math.floor((seconds % 3600) / 60);
		if (h > 0 && m > 0) return `${h}h ${m}m`;
		if (h > 0) return `${h}h`;
		if (m > 0) return `${m}m`;
		return `${seconds}s`;
	}

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

	function buildEnvVars(creds: Credentials | null): string {
		if (!creds) return '';
		return [
			`export AWS_ACCESS_KEY_ID=${creds.AccessKeyId ?? ''}`,
			`export AWS_SECRET_ACCESS_KEY=${creds.SecretAccessKey ?? ''}`,
			`export AWS_SESSION_TOKEN=${creds.SessionToken ?? ''}`
		].join('\n');
	}

	function buildCredJSON(creds: Credentials | null): string {
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

	// ==================== Tabs ====================

	type TabId = 'identity' | 'assume' | 'tokens' | 'utilities';

	const tabs: TabDef[] = [
		{ id: 'identity', label: 'Identity' },
		{ id: 'assume', label: 'Assume Role' },
		{ id: 'tokens', label: 'Get Token' },
		{ id: 'utilities', label: 'Utilities' }
	];

	let activeTab = $state<TabId>('identity');

	// ==================== Identity (GetCallerIdentity) + server metrics ====================

	let identity = $state<{ Account?: string; Arn?: string; UserId?: string } | null>(null);

	type IdentityCard = { label: string; value: string | undefined; icon: typeof User; color: string };
	const identityCards = $derived<IdentityCard[]>(
		identity
			? [
					{ label: 'Account', value: identity.Account, icon: User, color: 'text-amber-500' },
					{ label: 'User ID', value: identity.UserId, icon: User, color: 'text-blue-500' },
					{ label: 'ARN', value: identity.Arn, icon: Shield, color: 'text-green-500' }
				]
			: []
	);

	async function loadIdentity(): Promise<void> {
		const resp = await client().send(new GetCallerIdentityCommand({}));
		identity = { Account: resp.Account, Arn: resp.Arn, UserId: resp.UserId };
	}

	// SessionMetrics is aggregate-only (services/sts/models.go): counts, not
	// records. There is no operation that returns the list of live sessions.
	type STSMetrics = {
		activeSessions: number;
		expiredSessions: number;
		sweepCount: number;
		expiredEvictions: number;
		totalSessionsCreated?: number;
		opsAssumeRole?: number;
		opsAssumeRoleWithSAML?: number;
		opsAssumeRoleWithWebIdentity?: number;
		opsAssumeRoot?: number;
		opsGetCallerIdentity?: number;
		opsGetFederationToken?: number;
		opsGetSessionToken?: number;
		opsGetWebIdentityToken?: number;
		opsGetAccessKeyInfo?: number;
		opsDecodeAuthorizationMessage?: number;
		opsGetDelegatedAccessToken?: number;
	};

	let metrics = $state<STSMetrics | null>(null);

	async function loadMetrics(): Promise<void> {
		try {
			const resp = await fetch('/dashboard/api/sts/metrics');
			if (!resp.ok) return;
			metrics = await resp.json();
		} catch {
			metrics = null;
		}
	}

	type MetricCard = { label: string; value: number; color: string };
	const sessionMetricCards = $derived<MetricCard[]>(
		metrics
			? [
					{ label: 'Active sessions', value: metrics.activeSessions, color: 'text-green-600 dark:text-green-400' },
					{ label: 'Expired sessions', value: metrics.expiredSessions, color: 'text-red-500' },
					{ label: 'Total created', value: metrics.totalSessionsCreated ?? 0, color: 'text-blue-600 dark:text-blue-400' },
					{ label: 'Janitor sweeps', value: metrics.sweepCount, color: 'text-gray-700 dark:text-white' },
					{ label: 'Expired evictions', value: metrics.expiredEvictions, color: 'text-orange-500' }
				]
			: []
	);

	type OpCard = { label: string; value: number };
	const opCountCards = $derived<OpCard[]>(
		metrics
			? [
					{ label: 'AssumeRole', value: metrics.opsAssumeRole ?? 0 },
					{ label: 'AssumeRoleWithSAML', value: metrics.opsAssumeRoleWithSAML ?? 0 },
					{ label: 'AssumeRoleWithWebIdentity', value: metrics.opsAssumeRoleWithWebIdentity ?? 0 },
					{ label: 'AssumeRoot', value: metrics.opsAssumeRoot ?? 0 },
					{ label: 'GetCallerIdentity', value: metrics.opsGetCallerIdentity ?? 0 },
					{ label: 'GetFederationToken', value: metrics.opsGetFederationToken ?? 0 },
					{ label: 'GetSessionToken', value: metrics.opsGetSessionToken ?? 0 },
					{ label: 'GetWebIdentityToken', value: metrics.opsGetWebIdentityToken ?? 0 },
					{ label: 'GetAccessKeyInfo', value: metrics.opsGetAccessKeyInfo ?? 0 },
					{ label: 'DecodeAuthMessage', value: metrics.opsDecodeAuthorizationMessage ?? 0 },
					{ label: 'GetDelegatedToken', value: metrics.opsGetDelegatedAccessToken ?? 0 }
				]
			: []
	);
	const hasOpCounts = $derived(opCountCards.some((c) => c.value > 0));

	// ==================== Credentials issued this session (client-side only) ====================

	type HistoryEntry = {
		id: number;
		op: string;
		issuedAt: Date;
		accessKeyId?: string;
		sessionToken?: string;
		expiration?: Date;
		detail?: string;
	};

	let historySeq = 0;
	let history = $state<HistoryEntry[]>([]);
	let historySearch = $state('');

	// Only pushed for operations that return an actual Credentials triple
	// (AccessKeyId/SecretAccessKey/SessionToken/Expiration). GetWebIdentityToken
	// returns a signed JWT instead — a different credential shape — and is
	// intentionally not recorded here (see its own result panel below).
	function recordHistory(op: string, creds: Credentials | undefined, detail?: string): void {
		if (!creds) return;
		historySeq += 1;
		history = [
			{
				id: historySeq,
				op,
				issuedAt: new Date(),
				accessKeyId: creds.AccessKeyId,
				sessionToken: creds.SessionToken,
				expiration: creds.Expiration,
				detail
			},
			...history
		];
	}

	function clearHistory(): void {
		history = [];
	}

	const filteredHistory = $derived(
		history.filter((h) => matches(historySearch, h.op, h.accessKeyId, h.detail))
	);

	// ==================== Assume Role ====================

	let arRoleArn = $state('arn:aws:iam::000000000000:role/MyRole');
	let arSessionName = $state('my-session');
	let arDuration = $state(3600);
	let arExternalId = $state('');
	let arCreds = $state<Credentials | null>(null);
	let arAssumedUser = $state<{ Arn?: string; AssumedRoleId?: string } | null>(null);
	let arReveal = $state(false);
	let arBusy = $state(false);

	async function assumeRole(): Promise<void> {
		if (!arRoleArn.trim() || !arSessionName.trim()) {
			toast.error('Role ARN and session name are required');
			return;
		}
		arBusy = true;
		try {
			const resp = await client().send(
				new AssumeRoleCommand({
					RoleArn: arRoleArn.trim(),
					RoleSessionName: arSessionName.trim(),
					DurationSeconds: Math.round(arDuration),
					ExternalId: arExternalId.trim() || undefined
				})
			);
			arCreds = resp.Credentials ?? null;
			arAssumedUser = resp.AssumedRoleUser ?? null;
			arReveal = false;
			toast.success('Role assumed');
			recordHistory('AssumeRole', resp.Credentials, resp.AssumedRoleUser?.Arn);
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			arBusy = false;
		}
	}

	// ==================== Assume Role With SAML ====================

	// Working sample: base64 of a minimal SAML assertion with an Issuer and a
	// NameID, both of which the backend's extractSAMLAssertionData derives
	// Subject/Issuer from (see services/sts/saml_attributes.go).
	const SAMPLE_SAML_ASSERTION =
		'PHNhbWxwOkFzc2VydGlvbiB4bWxuczpzYW1scD0idXJuOm9hc2lzOm5hbWVzOnRjOlNBTUw6Mi4wOmFzc2VydGlvbiI+PElzc3Vlcj5odHRwczovL2lkcC5leGFtcGxlLmNvbS9zYW1sPC9Jc3N1ZXI+PFN1YmplY3Q+PE5hbWVJRD5hbGljZUBleGFtcGxlLmNvbTwvTmFtZUlEPjwvU3ViamVjdD48L3NhbWxwOkFzc2VydGlvbj4=';

	let samlRoleArn = $state('arn:aws:iam::000000000000:role/MyRole');
	let samlPrincipalArn = $state('arn:aws:iam::000000000000:saml-provider/MyIdP');
	let samlAssertion = $state(SAMPLE_SAML_ASSERTION);
	let samlDuration = $state(3600);
	let samlCreds = $state<Credentials | null>(null);
	let samlDetail = $state<{ Subject?: string; Issuer?: string; SourceIdentity?: string } | null>(null);
	let samlReveal = $state(false);
	let samlBusy = $state(false);

	async function assumeRoleWithSAML(): Promise<void> {
		if (!samlRoleArn.trim() || !samlPrincipalArn.trim() || !samlAssertion.trim()) {
			toast.error('Role ARN, principal ARN, and SAML assertion are required');
			return;
		}
		samlBusy = true;
		try {
			const resp = await client().send(
				new AssumeRoleWithSAMLCommand({
					RoleArn: samlRoleArn.trim(),
					PrincipalArn: samlPrincipalArn.trim(),
					SAMLAssertion: samlAssertion.trim(),
					DurationSeconds: Math.round(samlDuration)
				})
			);
			samlCreds = resp.Credentials ?? null;
			samlDetail = { Subject: resp.Subject, Issuer: resp.Issuer, SourceIdentity: resp.SourceIdentity };
			samlReveal = false;
			toast.success('SAML role assumed');
			recordHistory('AssumeRoleWithSAML', resp.Credentials, resp.Subject);
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			samlBusy = false;
		}
	}

	// ==================== Assume Role With Web Identity ====================

	// Working sample: an unsigned JWT (header.payload.sig) with a "sub" claim,
	// matching the shape services/sts/web_identity_test.go's makeJWT builds —
	// the mock does not verify signatures, only claim structure.
	const SAMPLE_WEB_IDENTITY_TOKEN =
		'eyJhbGciOiAibm9uZSIsICJ0eXAiOiAiSldUIn0.eyJzdWIiOiAidXNlci0xMjM0NSIsICJhdWQiOiAibXktYXBwIn0.sig';

	let wiRoleArn = $state('arn:aws:iam::000000000000:role/MyRole');
	let wiSessionName = $state('my-web-session');
	let wiToken = $state(SAMPLE_WEB_IDENTITY_TOKEN);
	let wiProviderId = $state('');
	let wiDuration = $state(3600);
	let wiCreds = $state<Credentials | null>(null);
	let wiDetail = $state<{ SubjectFromWebIdentityToken?: string; Provider?: string; SourceIdentity?: string } | null>(
		null
	);
	let wiReveal = $state(false);
	let wiBusy = $state(false);

	async function assumeRoleWithWebIdentity(): Promise<void> {
		if (!wiRoleArn.trim() || !wiSessionName.trim() || !wiToken.trim()) {
			toast.error('Role ARN, session name, and web identity token are required');
			return;
		}
		wiBusy = true;
		try {
			const resp = await client().send(
				new AssumeRoleWithWebIdentityCommand({
					RoleArn: wiRoleArn.trim(),
					RoleSessionName: wiSessionName.trim(),
					WebIdentityToken: wiToken.trim(),
					ProviderId: wiProviderId.trim() || undefined,
					DurationSeconds: Math.round(wiDuration)
				})
			);
			wiCreds = resp.Credentials ?? null;
			wiDetail = {
				SubjectFromWebIdentityToken: resp.SubjectFromWebIdentityToken,
				Provider: resp.Provider,
				SourceIdentity: resp.SourceIdentity
			};
			wiReveal = false;
			toast.success('Web identity role assumed');
			recordHistory('AssumeRoleWithWebIdentity', resp.Credentials, resp.SubjectFromWebIdentityToken);
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			wiBusy = false;
		}
	}

	// ==================== Assume Root ====================

	// The five AWS-approved root-task policies (services/sts/assume_root.go's
	// approvedRootTaskPolicies) — TargetPrincipal + TaskPolicyArn.arn must come
	// from this exact set or the real API (and this emulator) rejects it.
	const ROOT_TASK_POLICIES = [
		'arn:aws:iam::aws:policy/root-task/IAMAuditRootUserCredentials',
		'arn:aws:iam::aws:policy/root-task/IAMCreateRootUserPassword',
		'arn:aws:iam::aws:policy/root-task/IAMDeleteRootUserCredentials',
		'arn:aws:iam::aws:policy/root-task/S3UnlockBucketPolicy',
		'arn:aws:iam::aws:policy/root-task/SQSUnlockQueuePolicy'
	];

	let rootTargetPrincipal = $state('000000000000');
	let rootTaskPolicy = $state(ROOT_TASK_POLICIES[0]);
	let rootDuration = $state(900);
	let rootCreds = $state<Credentials | null>(null);
	let rootSourceIdentity = $state('');
	let rootReveal = $state(false);
	let rootBusy = $state(false);

	async function assumeRoot(): Promise<void> {
		if (!rootTargetPrincipal.trim()) {
			toast.error('Target principal is required');
			return;
		}
		rootBusy = true;
		try {
			const resp = await client().send(
				new AssumeRootCommand({
					TargetPrincipal: rootTargetPrincipal.trim(),
					TaskPolicyArn: { arn: rootTaskPolicy },
					DurationSeconds: Math.round(rootDuration)
				})
			);
			rootCreds = resp.Credentials ?? null;
			rootSourceIdentity = resp.SourceIdentity ?? '';
			rootReveal = false;
			toast.success('Root session issued');
			recordHistory('AssumeRoot', resp.Credentials, rootTaskPolicy.split('/').pop());
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			rootBusy = false;
		}
	}

	// ==================== Get Session Token ====================

	let gtDuration = $state(3600);
	let gtMfaSerial = $state('');
	let gtMfaCode = $state('');
	let gtCreds = $state<Credentials | null>(null);
	let gtReveal = $state(false);
	let gtBusy = $state(false);

	async function getSessionToken(): Promise<void> {
		gtBusy = true;
		try {
			const params: { DurationSeconds: number; SerialNumber?: string; TokenCode?: string } = {
				DurationSeconds: Math.round(gtDuration)
			};
			if (gtMfaSerial.trim()) {
				params.SerialNumber = gtMfaSerial.trim();
				params.TokenCode = gtMfaCode.trim();
			}
			const resp = await client().send(new GetSessionTokenCommand(params));
			gtCreds = resp.Credentials ?? null;
			gtReveal = false;
			toast.success('Session token generated');
			recordHistory('GetSessionToken', resp.Credentials);
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			gtBusy = false;
		}
	}

	// ==================== Get Federation Token ====================

	let fedName = $state('my-user');
	let fedDuration = $state(43200);
	let fedCreds = $state<Credentials | null>(null);
	let fedUser = $state<{ Arn?: string; FederatedUserId?: string } | null>(null);
	let fedReveal = $state(false);
	let fedBusy = $state(false);

	async function getFederationToken(): Promise<void> {
		if (!fedName.trim()) {
			toast.error('Federation token name is required');
			return;
		}
		fedBusy = true;
		try {
			const resp = await client().send(
				new GetFederationTokenCommand({ Name: fedName.trim(), DurationSeconds: Math.round(fedDuration) })
			);
			fedCreds = resp.Credentials ?? null;
			fedUser = resp.FederatedUser ?? null;
			fedReveal = false;
			toast.success('Federation token issued');
			recordHistory('GetFederationToken', resp.Credentials, resp.FederatedUser?.Arn);
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			fedBusy = false;
		}
	}

	// ==================== Get Web Identity Token ====================
	//
	// Returns a signed JWT, not IAM Credentials — a genuinely different
	// output shape from the other seven credential-issuing operations, so it
	// gets its own result panel instead of the shared credsBlock snippet.

	let wtAudience = $state('my-app');
	let wtSigningAlgorithm = $state<'RS256' | 'ES384'>('RS256');
	let wtDuration = $state(300);
	let wtToken = $state('');
	let wtExpiration = $state<Date | null>(null);
	let wtBusy = $state(false);

	async function getWebIdentityToken(): Promise<void> {
		const audience = wtAudience
			.split(',')
			.map((a) => a.trim())
			.filter(Boolean);
		if (audience.length === 0) {
			toast.error('At least one audience value is required');
			return;
		}
		wtBusy = true;
		try {
			const resp = await client().send(
				new GetWebIdentityTokenCommand({
					Audience: audience,
					SigningAlgorithm: wtSigningAlgorithm,
					DurationSeconds: Math.round(wtDuration)
				})
			);
			wtToken = resp.WebIdentityToken ?? '';
			wtExpiration = resp.Expiration ?? null;
			toast.success('Web identity token issued');
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			wtBusy = false;
		}
	}

	// ==================== Get Delegated Access Token ====================

	let dtTradeInToken = $state('sample-trade-in-token');
	let dtCreds = $state<Credentials | null>(null);
	let dtAssumedPrincipal = $state('');
	let dtReveal = $state(false);
	let dtBusy = $state(false);

	async function getDelegatedAccessToken(): Promise<void> {
		if (!dtTradeInToken.trim()) {
			toast.error('Trade-in token is required');
			return;
		}
		dtBusy = true;
		try {
			const resp = await client().send(new GetDelegatedAccessTokenCommand({ TradeInToken: dtTradeInToken.trim() }));
			dtCreds = resp.Credentials ?? null;
			dtAssumedPrincipal = resp.AssumedPrincipal ?? '';
			dtReveal = false;
			toast.success('Delegated access token exchanged');
			recordHistory('GetDelegatedAccessToken', resp.Credentials, resp.AssumedPrincipal);
			await loadMetrics();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			dtBusy = false;
		}
	}

	// ==================== Utilities ====================

	let validatorAccessKeyId = $state('');
	let validatorAccount = $state('');
	let isValidating = $state(false);

	async function validateAccessKey(): Promise<void> {
		if (!validatorAccessKeyId.trim()) {
			toast.error('Access Key ID is required');
			return;
		}
		isValidating = true;
		try {
			const resp = await client().send(new GetAccessKeyInfoCommand({ AccessKeyId: validatorAccessKeyId.trim() }));
			validatorAccount = resp.Account ?? '';
			toast.success('Access key resolved');
		} catch (e) {
			validatorAccount = '';
			toast.error(describeError(e));
		} finally {
			isValidating = false;
		}
	}

	function clearValidator(): void {
		validatorAccessKeyId = '';
		validatorAccount = '';
	}

	let encodedMessage = $state('');
	let decodedMessage = $state('');
	let prettyDecodedMessage = $state('');
	let isDecoding = $state(false);

	async function decodeAuthMessage(): Promise<void> {
		if (!encodedMessage.trim()) {
			toast.error('Encoded authorization message is required');
			return;
		}
		isDecoding = true;
		try {
			const resp = await client().send(new DecodeAuthorizationMessageCommand({ EncodedMessage: encodedMessage.trim() }));
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
			toast.error(describeError(e));
		} finally {
			isDecoding = false;
		}
	}

	function clearDecoder(): void {
		encodedMessage = '';
		decodedMessage = '';
		prettyDecodedMessage = '';
	}

	// ==================== Tab loader ====================

	const tabLoader = createTabLoader<TabId>({
		identity: () =>
			Promise.all([loadIdentity(), loadMetrics()])
				.then(() => {})
				.catch(rethrowDescribed),
		// Assume Role / Get Token / Utilities have nothing to bootstrap-fetch —
		// every result on those tabs only appears once the operator submits a
		// form from within the tab itself.
		assume: () => Promise.resolve(),
		tokens: () => Promise.resolve(),
		utilities: () => Promise.resolve()
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		tabLoader.refresh(activeTab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// ==================== History table columns ====================

	function isExpired(exp: Date | undefined): boolean {
		return !!exp && exp.getTime() <= Date.now();
	}
</script>

{#snippet credsBlock(
	creds: Credentials | null,
	revealed: boolean,
	onToggleReveal: () => void,
	accentText: string
)}
	{#if creds}
		<div class="space-y-3 bg-gray-50 dark:bg-slate-700/50 rounded-lg p-4">
			<div class="flex items-center justify-between">
				<div>
					<p class="text-xs text-gray-500 dark:text-gray-400">Access Key ID</p>
					<p class="text-sm font-mono text-gray-900 dark:text-white">{creds.AccessKeyId}</p>
				</div>
				<button
					onclick={() => copyToClipboard(creds.AccessKeyId ?? '', 'Access Key ID')}
					class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
					title="Copy Access Key ID"
				>
					<Copy class="w-4 h-4 text-gray-400" />
				</button>
			</div>
			<div class="flex items-center justify-between">
				<div class="flex-1 min-w-0">
					<p class="text-xs text-gray-500 dark:text-gray-400">Secret Access Key</p>
					<p class="text-sm font-mono text-gray-900 dark:text-white break-all">
						{revealed ? creds.SecretAccessKey : '•'.repeat(40)}
					</p>
				</div>
				<div class="flex items-center gap-1 ml-2 flex-shrink-0">
					<button
						onclick={onToggleReveal}
						class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
						title="{revealed ? 'Hide' : 'Reveal'} Secret Access Key"
					>
						{#if revealed}
							<EyeOff class="w-4 h-4 text-gray-400" />
						{:else}
							<Eye class="w-4 h-4 text-gray-400" />
						{/if}
					</button>
					<button
						onclick={() => copyToClipboard(creds.SecretAccessKey ?? '', 'Secret Access Key')}
						class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
						title="Copy Secret Access Key"
					>
						<Copy class="w-4 h-4 text-gray-400" />
					</button>
				</div>
			</div>
			<div class="flex items-center justify-between">
				<div>
					<p class="text-xs text-gray-500 dark:text-gray-400">Session Token</p>
					<p class="text-sm font-mono text-gray-900 dark:text-white">
						{creds.SessionToken ? `${creds.SessionToken.slice(0, 20)}…` : ''}
					</p>
				</div>
				<button
					onclick={() => copyToClipboard(creds.SessionToken ?? '', 'Session Token')}
					class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded"
					title="Copy Session Token"
				>
					<Copy class="w-4 h-4 text-gray-400" />
				</button>
			</div>
			<div>
				<p class="text-xs text-gray-500 dark:text-gray-400">Expiration</p>
				<p class="text-sm text-gray-900 dark:text-white">
					{creds.Expiration?.toISOString()}
					<span class="text-xs {accentText} ml-1">({formatRelativeExpiration(creds.Expiration)})</span>
				</p>
			</div>
			<div class="flex gap-2 pt-2 border-t border-gray-200 dark:border-gray-600">
				<button
					onclick={() => copyToClipboard(buildEnvVars(creds), 'environment variables')}
					class="flex items-center gap-1 px-3 py-1.5 text-xs border border-slate-200 dark:border-slate-600 rounded hover:bg-slate-50 dark:hover:bg-slate-700 text-gray-700 dark:text-gray-300"
				>
					<Copy class="w-3 h-3" /> Copy as env vars
				</button>
				<button
					onclick={() => copyToClipboard(buildCredJSON(creds), 'JSON credentials')}
					class="flex items-center gap-1 px-3 py-1.5 text-xs border border-slate-200 dark:border-slate-600 rounded hover:bg-slate-50 dark:hover:bg-slate-700 text-gray-700 dark:text-gray-300"
				>
					<Copy class="w-3 h-3" /> Copy as JSON
				</button>
			</div>
		</div>
	{/if}
{/snippet}

{#snippet factsList(facts: [string, string | undefined][])}
	<div class="grid grid-cols-1 sm:grid-cols-2 gap-x-4 gap-y-1 text-xs">
		{#each facts as [label, value] (label)}
			<div class="flex justify-between gap-2 border-b border-gray-100 dark:border-gray-800 pb-1">
				<span class="text-gray-500">{label}</span>
				<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value || '—'}</span>
			</div>
		{/each}
	</div>
{/snippet}

<div class="p-6 space-y-6">
	<PageHeader
		icon={Shield}
		title="AWS Security Token Service"
		description="Issue temporary credentials, exchange trust for access, and inspect identity — STS has no listable resources."
		onRefresh={handleRefresh}
		color="amber"
	/>

	<Tabs {tabs} active={activeTab} onSelect={switchTab} color="amber" />

	{#if activeTabError}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load data</p>
			<p>{activeTabError}</p>
		</div>
	{/if}

	<!-- ==================== IDENTITY ==================== -->
	{#if activeTab === 'identity'}
		<div class="space-y-6">
			{#if tabLoader.isLoading('identity')}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading identity...</div>
			{:else if identity}
				<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
					{#each identityCards as card (card.label)}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
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

			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white mb-1">Session Metrics</h2>
				<p class="text-xs text-gray-500 dark:text-gray-400 mb-4">
					Aggregate counters from the backend janitor (services/sts) — active/expired session counts and
					per-operation call counts. There is no server API for the individual sessions behind these
					counts; see "Credentials issued this session" below for what this browser tab has personally requested.
				</p>
				{#if metrics}
					<div class="space-y-4">
						<div class="grid grid-cols-2 md:grid-cols-4 gap-3">
							{#each sessionMetricCards as m (m.label)}
								<div class="rounded border border-slate-200 dark:border-slate-700 p-3">
									<p class="text-xs text-gray-500 dark:text-gray-400">{m.label}</p>
									<p class="text-lg font-semibold {m.color}">{m.value}</p>
								</div>
							{/each}
						</div>
						{#if hasOpCounts}
							<div>
								<p class="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase mb-2">Operation call counts</p>
								<div class="grid grid-cols-2 md:grid-cols-4 gap-2">
									{#each opCountCards as op (op.label)}
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
					<p class="text-gray-500 dark:text-gray-400 text-sm">No metrics yet — generate credentials to see session stats.</p>
				{/if}
			</div>
		</div>

	<!-- ==================== ASSUME ROLE ==================== -->
	{:else if activeTab === 'assume'}
		<div class="space-y-6">
			<!-- AssumeRole -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<Shield class="w-5 h-5 text-indigo-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">AssumeRole</h2>
				</div>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
					<div>
						<label for="ar-role-arn" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Role ARN</label>
						<input id="ar-role-arn" bind:value={arRoleArn} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="ar-session-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Session Name</label>
						<input id="ar-session-name" bind:value={arSessionName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="ar-external-id" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">External ID <span class="text-gray-400">(optional)</span></label>
						<input id="ar-external-id" bind:value={arExternalId} placeholder="unique-cross-account-id" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="ar-duration" class="text-xs text-gray-500 dark:text-gray-400 flex justify-between mb-1">
							<span>Duration</span>
							<span class="font-medium text-gray-700 dark:text-gray-300">{formatDuration(arDuration)}</span>
						</label>
						<input id="ar-duration" type="range" bind:value={arDuration} min="900" max="43200" step="900" class="w-full accent-indigo-500" />
					</div>
				</div>
				<button id="assume-role-btn" onclick={assumeRole} disabled={arBusy} class="flex items-center gap-2 px-4 py-2 bg-indigo-600 hover:bg-indigo-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<Shield class="w-4 h-4" /> {arBusy ? 'Assuming...' : 'Assume Role'}
				</button>
				{#if arAssumedUser}
					{@render factsList([['AssumedRoleId', arAssumedUser.AssumedRoleId], ['Arn', arAssumedUser.Arn]])}
				{/if}
				{@render credsBlock(arCreds, arReveal, () => (arReveal = !arReveal), 'text-indigo-500')}
			</div>

			<!-- AssumeRoleWithSAML -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<Globe class="w-5 h-5 text-purple-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">AssumeRoleWithSAML</h2>
				</div>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
					<div>
						<label for="saml-role-arn" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Role ARN</label>
						<input id="saml-role-arn" bind:value={samlRoleArn} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="saml-principal-arn" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">SAML Provider ARN (PrincipalArn)</label>
						<input id="saml-principal-arn" bind:value={samlPrincipalArn} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div class="md:col-span-2">
						<label for="saml-assertion" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">SAML Assertion (base64)</label>
						<textarea id="saml-assertion" bind:value={samlAssertion} rows="3" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono"></textarea>
					</div>
					<div>
						<label for="saml-duration" class="text-xs text-gray-500 dark:text-gray-400 flex justify-between mb-1">
							<span>Duration</span>
							<span class="font-medium text-gray-700 dark:text-gray-300">{formatDuration(samlDuration)}</span>
						</label>
						<input id="saml-duration" type="range" bind:value={samlDuration} min="900" max="43200" step="900" class="w-full accent-purple-500" />
					</div>
				</div>
				<button onclick={assumeRoleWithSAML} disabled={samlBusy} class="flex items-center gap-2 px-4 py-2 bg-purple-600 hover:bg-purple-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<Globe class="w-4 h-4" /> {samlBusy ? 'Assuming...' : 'Assume Role With SAML'}
				</button>
				{#if samlDetail}
					{@render factsList([['Subject', samlDetail.Subject], ['Issuer', samlDetail.Issuer], ['SourceIdentity', samlDetail.SourceIdentity]])}
				{/if}
				{@render credsBlock(samlCreds, samlReveal, () => (samlReveal = !samlReveal), 'text-purple-500')}
			</div>

			<!-- AssumeRoleWithWebIdentity -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<Key class="w-5 h-5 text-cyan-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">AssumeRoleWithWebIdentity</h2>
				</div>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
					<div>
						<label for="wi-role-arn" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Role ARN</label>
						<input id="wi-role-arn" bind:value={wiRoleArn} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="wi-session-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Session Name</label>
						<input id="wi-session-name" bind:value={wiSessionName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div class="md:col-span-2">
						<label for="wi-token" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Web Identity Token (JWT)</label>
						<textarea id="wi-token" bind:value={wiToken} rows="3" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono"></textarea>
					</div>
					<div>
						<label for="wi-provider-id" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Provider ID <span class="text-gray-400">(OAuth 2.0 only)</span></label>
						<input id="wi-provider-id" bind:value={wiProviderId} placeholder="www.amazon.com" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="wi-duration" class="text-xs text-gray-500 dark:text-gray-400 flex justify-between mb-1">
							<span>Duration</span>
							<span class="font-medium text-gray-700 dark:text-gray-300">{formatDuration(wiDuration)}</span>
						</label>
						<input id="wi-duration" type="range" bind:value={wiDuration} min="900" max="43200" step="900" class="w-full accent-cyan-500" />
					</div>
				</div>
				<button onclick={assumeRoleWithWebIdentity} disabled={wiBusy} class="flex items-center gap-2 px-4 py-2 bg-cyan-600 hover:bg-cyan-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<Key class="w-4 h-4" /> {wiBusy ? 'Assuming...' : 'Assume Role With Web Identity'}
				</button>
				{#if wiDetail}
					{@render factsList([
						['SubjectFromWebIdentityToken', wiDetail.SubjectFromWebIdentityToken],
						['Provider', wiDetail.Provider],
						['SourceIdentity', wiDetail.SourceIdentity]
					])}
				{/if}
				{@render credsBlock(wiCreds, wiReveal, () => (wiReveal = !wiReveal), 'text-cyan-500')}
			</div>

			<!-- AssumeRoot -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<Crown class="w-5 h-5 text-red-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">AssumeRoot</h2>
				</div>
				<p class="text-xs text-gray-500 dark:text-gray-400">
					Privileged root-task session. TaskPolicyArn must be one of the five AWS-approved managed policies.
				</p>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
					<div>
						<label for="root-target-principal" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Target Principal (account ID or ARN)</label>
						<input id="root-target-principal" bind:value={rootTargetPrincipal} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="root-task-policy" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Task Policy ARN</label>
						<select id="root-task-policy" bind:value={rootTaskPolicy} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
							{#each ROOT_TASK_POLICIES as p (p)}
								<option value={p}>{p.split('/').pop()}</option>
							{/each}
						</select>
					</div>
					<div>
						<label for="root-duration" class="text-xs text-gray-500 dark:text-gray-400 flex justify-between mb-1">
							<span>Duration (max 900s)</span>
							<span class="font-medium text-gray-700 dark:text-gray-300">{formatDuration(rootDuration)}</span>
						</label>
						<input id="root-duration" type="range" bind:value={rootDuration} min="0" max="900" step="60" class="w-full accent-red-500" />
					</div>
				</div>
				<button onclick={assumeRoot} disabled={rootBusy} class="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<Crown class="w-4 h-4" /> {rootBusy ? 'Issuing...' : 'Assume Root'}
				</button>
				{#if rootSourceIdentity}
					{@render factsList([['SourceIdentity', rootSourceIdentity]])}
				{/if}
				{@render credsBlock(rootCreds, rootReveal, () => (rootReveal = !rootReveal), 'text-red-500')}
			</div>
		</div>

	<!-- ==================== GET TOKEN ==================== -->
	{:else if activeTab === 'tokens'}
		<div class="space-y-6">
			<!-- GetSessionToken -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<Key class="w-5 h-5 text-amber-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">GetSessionToken</h2>
				</div>
				<div class="space-y-1">
					<label for="gt-duration" class="text-xs text-gray-500 dark:text-gray-400 flex justify-between">
						<span>Duration</span>
						<span class="font-medium text-gray-700 dark:text-gray-300">{formatDuration(gtDuration)}</span>
					</label>
					<input id="gt-duration" type="range" bind:value={gtDuration} min="900" max="129600" step="900" class="w-full accent-amber-500" />
				</div>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
					<div>
						<label for="gt-mfa-serial" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">MFA Serial Number <span class="text-gray-400">(optional)</span></label>
						<input id="gt-mfa-serial" bind:value={gtMfaSerial} placeholder="arn:aws:iam::000000000000:mfa/MyDevice" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					{#if gtMfaSerial.trim()}
						<div>
							<label for="gt-mfa-code" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">MFA Token Code</label>
							<input id="gt-mfa-code" bind:value={gtMfaCode} placeholder="123456" maxlength="6" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
						</div>
					{/if}
				</div>
				<button id="gen-session-btn" onclick={getSessionToken} disabled={gtBusy} class="flex items-center gap-2 px-4 py-2 bg-amber-500 hover:bg-amber-600 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<Key class="w-4 h-4" /> {gtBusy ? 'Generating...' : 'Generate Session Token'}
				</button>
				{@render credsBlock(gtCreds, gtReveal, () => (gtReveal = !gtReveal), 'text-amber-500')}
			</div>

			<!-- GetFederationToken -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<User class="w-5 h-5 text-teal-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">GetFederationToken</h2>
				</div>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
					<div>
						<label for="fed-name" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Federated User Name</label>
						<input id="fed-name" bind:value={fedName} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="fed-duration" class="text-xs text-gray-500 dark:text-gray-400 flex justify-between mb-1">
							<span>Duration</span>
							<span class="font-medium text-gray-700 dark:text-gray-300">{formatDuration(fedDuration)}</span>
						</label>
						<input id="fed-duration" type="range" bind:value={fedDuration} min="900" max="129600" step="3600" class="w-full accent-teal-500" />
					</div>
				</div>
				<button id="fed-token-btn" onclick={getFederationToken} disabled={fedBusy} class="flex items-center gap-2 px-4 py-2 bg-teal-600 hover:bg-teal-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<User class="w-4 h-4" /> {fedBusy ? 'Issuing...' : 'Get Federation Token'}
				</button>
				{#if fedUser}
					{@render factsList([['FederatedUserId', fedUser.FederatedUserId], ['Arn', fedUser.Arn]])}
				{/if}
				{@render credsBlock(fedCreds, fedReveal, () => (fedReveal = !fedReveal), 'text-teal-500')}
			</div>

			<!-- GetWebIdentityToken -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<FileKey class="w-5 h-5 text-sky-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">GetWebIdentityToken</h2>
				</div>
				<p class="text-xs text-gray-500 dark:text-gray-400">
					Issues a signed JSON Web Token identifying this caller — not IAM credentials, so it is not added to
					the credentials history below.
				</p>
				<div class="grid grid-cols-1 md:grid-cols-2 gap-3">
					<div>
						<label for="wt-audience" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Audience (comma-separated)</label>
						<input id="wt-audience" bind:value={wtAudience} placeholder="my-app, my-other-app" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					</div>
					<div>
						<label for="wt-signing-algorithm" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Signing Algorithm</label>
						<select id="wt-signing-algorithm" bind:value={wtSigningAlgorithm} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm">
							<option value="RS256">RS256</option>
							<option value="ES384">ES384</option>
						</select>
					</div>
					<div class="md:col-span-2">
						<label for="wt-duration" class="text-xs text-gray-500 dark:text-gray-400 flex justify-between mb-1">
							<span>Duration</span>
							<span class="font-medium text-gray-700 dark:text-gray-300">{formatDuration(wtDuration)}</span>
						</label>
						<input id="wt-duration" type="range" bind:value={wtDuration} min="60" max="3600" step="60" class="w-full accent-sky-500" />
					</div>
				</div>
				<button onclick={getWebIdentityToken} disabled={wtBusy} class="flex items-center gap-2 px-4 py-2 bg-sky-600 hover:bg-sky-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<FileKey class="w-4 h-4" /> {wtBusy ? 'Issuing...' : 'Get Web Identity Token'}
				</button>
				{#if wtToken}
					<div class="space-y-2 bg-gray-50 dark:bg-slate-700/50 rounded-lg p-4">
						<div class="flex items-center justify-between">
							<p class="text-xs text-gray-500 dark:text-gray-400">Web Identity Token (JWT)</p>
							<button onclick={() => copyToClipboard(wtToken, 'Web Identity Token')} class="p-1 hover:bg-gray-200 dark:hover:bg-slate-600 rounded" title="Copy token">
								<Copy class="w-4 h-4 text-gray-400" />
							</button>
						</div>
						<p class="text-xs font-mono text-gray-900 dark:text-white break-all">{wtToken}</p>
						{#if wtExpiration}
							<p class="text-xs text-sky-600 dark:text-sky-400">{formatRelativeExpiration(wtExpiration)} ({wtExpiration.toISOString()})</p>
						{/if}
					</div>
				{/if}
			</div>

			<!-- GetDelegatedAccessToken -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<ArrowRightLeft class="w-5 h-5 text-emerald-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">GetDelegatedAccessToken</h2>
				</div>
				<p class="text-xs text-gray-500 dark:text-gray-400">
					Exchanges a trade-in token for temporary credentials. Duration is fixed by the backend — the real
					API has no DurationSeconds request member for this operation.
				</p>
				<div>
					<label for="dt-trade-in-token" class="block text-xs text-gray-500 dark:text-gray-400 mb-1">Trade-In Token</label>
					<input id="dt-trade-in-token" bind:value={dtTradeInToken} class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono" />
				</div>
				<button onclick={getDelegatedAccessToken} disabled={dtBusy} class="flex items-center gap-2 px-4 py-2 bg-emerald-600 hover:bg-emerald-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
					<ArrowRightLeft class="w-4 h-4" /> {dtBusy ? 'Exchanging...' : 'Get Delegated Access Token'}
				</button>
				{#if dtAssumedPrincipal}
					{@render factsList([['AssumedPrincipal', dtAssumedPrincipal]])}
				{/if}
				{@render credsBlock(dtCreds, dtReveal, () => (dtReveal = !dtReveal), 'text-emerald-500')}
			</div>
		</div>

	<!-- ==================== UTILITIES ==================== -->
	{:else if activeTab === 'utilities'}
		<div class="space-y-6">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<div class="flex items-center gap-2">
					<Lock class="w-5 h-5 text-blue-500" />
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">GetAccessKeyInfo</h2>
				</div>
				<p class="text-xs text-gray-500 dark:text-gray-400">Look up which account issued an access key ID.</p>
				<div class="flex flex-col md:flex-row gap-2">
					<input id="validator-access-key-id" bind:value={validatorAccessKeyId} placeholder="ASIA..." class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm" />
					<button onclick={validateAccessKey} disabled={isValidating} class="px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-60 text-white rounded-lg text-sm font-medium whitespace-nowrap">
						{isValidating ? 'Looking up...' : 'Look Up'}
					</button>
					{#if validatorAccessKeyId || validatorAccount}
						<button onclick={clearValidator} class="px-3 py-2 border border-slate-200 dark:border-slate-700 text-gray-600 dark:text-gray-300 rounded-lg text-sm hover:bg-slate-50 dark:hover:bg-slate-700">Clear</button>
					{/if}
				</div>
				{#if validatorAccount}
					<div class="flex items-center gap-2 text-sm text-green-600 dark:text-green-400">
						<CheckCircle class="w-4 h-4" />
						<span>Access key belongs to account {validatorAccount}</span>
					</div>
				{/if}
			</div>

			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">DecodeAuthorizationMessage</h2>
				<textarea bind:value={encodedMessage} rows="4" placeholder="Paste base64 encoded authorization message" class="w-full px-3 py-2 rounded border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 text-sm font-mono"></textarea>
				<div class="flex gap-2 flex-wrap">
					<button id="decode-auth-btn" onclick={decodeAuthMessage} disabled={isDecoding} class="px-4 py-2 bg-gray-700 hover:bg-gray-800 disabled:opacity-60 text-white rounded-lg text-sm font-medium">
						{isDecoding ? 'Decoding...' : 'Decode'}
					</button>
					{#if decodedMessage}
						<button onclick={() => copyToClipboard(prettyDecodedMessage || decodedMessage, 'Decoded Message')} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-sm font-medium text-gray-700 dark:text-gray-300 hover:bg-slate-50 dark:hover:bg-slate-700/70">
							Copy Decoded Message
						</button>
					{/if}
					{#if encodedMessage || decodedMessage}
						<button onclick={clearDecoder} class="px-3 py-2 border border-slate-200 dark:border-slate-700 text-gray-600 dark:text-gray-300 rounded-lg text-sm hover:bg-slate-50 dark:hover:bg-slate-700">Clear</button>
					{/if}
				</div>
				{#if prettyDecodedMessage}
					<pre class="text-xs whitespace-pre-wrap bg-gray-50 dark:bg-slate-700/50 rounded-lg p-3 text-gray-900 dark:text-white overflow-x-auto max-h-64">{prettyDecodedMessage}</pre>
				{/if}
			</div>
		</div>
	{/if}

	<!-- ==================== CREDENTIALS ISSUED THIS SESSION ==================== -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
		<div class="flex flex-col sm:flex-row sm:items-center justify-between gap-3">
			<div class="flex items-center gap-2">
				<History class="w-5 h-5 text-gray-500" />
				<div>
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Credentials issued this session</h2>
					<p class="text-xs text-gray-500 dark:text-gray-400">
						Tracked in this browser tab only — never persisted or sent anywhere, cleared on reload. STS has no
						server-side API for a per-session list; this is not that.
					</p>
				</div>
			</div>
			<div class="flex items-center gap-2">
				<SearchInput bind:value={historySearch} placeholder="Search by operation or access key…" />
				{#if history.length > 0}
					<button onclick={clearHistory} class="px-3 py-2 border border-slate-200 dark:border-slate-700 text-gray-600 dark:text-gray-300 rounded-lg text-sm hover:bg-slate-50 dark:hover:bg-slate-700 whitespace-nowrap">Clear</button>
				{/if}
			</div>
		</div>

		{#snippet histOpCell(h: HistoryEntry)}
			<span class="text-xs font-semibold px-2 py-1 rounded-full bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300">{h.op}</span>
		{/snippet}
		{#snippet histAccessKeyCell(h: HistoryEntry)}
			{#if h.accessKeyId}
				<button onclick={() => copyToClipboard(h.accessKeyId ?? '', 'Access Key ID')} class="font-mono text-xs text-gray-700 dark:text-gray-300 hover:underline">{h.accessKeyId}</button>
			{:else}
				<span class="text-gray-400">—</span>
			{/if}
		{/snippet}
		{#snippet histExpirationCell(h: HistoryEntry)}
			{#if h.expiration}
				<span class={isExpired(h.expiration) ? 'text-xs text-red-500' : 'text-xs text-green-600 dark:text-green-400'} title={h.expiration.toISOString()}>
					{isExpired(h.expiration) ? 'Expired' : formatRelativeExpiration(h.expiration)}
				</span>
			{:else}
				<span class="text-gray-400">—</span>
			{/if}
		{/snippet}
		{#snippet histIssuedCell(h: HistoryEntry)}
			{formatDate(h.issuedAt)}
		{/snippet}
		{#snippet histDetailCell(h: HistoryEntry)}
			<span class="text-xs text-gray-500 dark:text-gray-400 break-all">{h.detail ?? '—'}</span>
		{/snippet}
		<DataTable
			rows={filteredHistory}
			rowKey={(h) => String(h.id)}
			columns={defineColumns<HistoryEntry>([
				{ key: 'op', label: 'Operation', render: histOpCell },
				{ key: 'accessKeyId', label: 'Access Key ID', render: histAccessKeyCell },
				{ key: 'expiration', label: 'Expires', render: histExpirationCell },
				{ key: 'issuedAt', label: 'Issued', render: histIssuedCell },
				{ key: 'detail', label: 'Detail', render: histDetailCell }
			])}
			loading={false}
			emptyMessage="No credentials issued yet this session. Use Assume Role or Get Token above."
		/>
	</div>
</div>
