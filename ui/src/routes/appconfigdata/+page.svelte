<script lang="ts">
	// AppConfig Data is the AppConfig *data plane*: exactly two operations,
	// StartConfigurationSession and GetLatestConfiguration. A client starts a
	// session against an app/env/configuration-profile triple, then polls with
	// a token that AWS rotates on every successful call (the response's
	// Next-Poll-Configuration-Token is what the NEXT poll must use; the token
	// from StartConfigurationSession is single-use). There is nothing to
	// create, update, list, or delete in the AWS API surface -- the CRUD floor
	// does not apply here, the same call made for redshiftdata/+page.svelte
	// (data-plane, no listable resources) and sts/+page.svelte
	// (credential-issuing, no listable resources). So this page is organized
	// around the real two-op poll workflow instead of a forced CRUD shape.
	//
	// ---------------------------------------------------------------------
	// No installed SDK client for this service (verified, not assumed)
	// ---------------------------------------------------------------------
	// Every other rewritten page in this sweep calls `regionalClient(getXClient)`
	// and sends typed Commands from an `@aws-sdk/client-*` package. That is NOT
	// possible here: `@aws-sdk/client-appconfigdata` is absent from both
	// ui/package.json and ui/node_modules (confirmed with `npm ls
	// @aws-sdk/client-appconfigdata` -> "(empty)"), and there is no
	// `getAppConfigDataClient` in $lib/aws-client.ts -- every other AWS service
	// page in this app has one, this is the only one that does not. Adding the
	// dependency would mean editing package.json/package-lock.json, which is
	// outside this page's edit scope for this pass. Since the gopherstack
	// backend does not verify SigV4 signatures (aws-client.ts's clientConfig()
	// uses static "test"/"test" credentials purely to satisfy the SDK's
	// signer -- see pkgs/service/cloudtrail_capture.go, which only ever READS
	// the Authorization header for telemetry, never validates it), a plain
	// `fetch()` against the exact documented wire shape reaches the same
	// backend the SDK would: POST /configurationsessions (restjson1, 201) and
	// GET /configuration?configuration_token=... (200, raw body blob + control
	// headers) -- both verified directly against
	// services/appconfigdata/handler.go, models.go, and errors.go, not
	// invented. `onRegionChange` is still used (this service's storage is
	// region-agnostic -- Session/ConfigurationProfile in
	// services/appconfigdata/models.go carry no region field -- but every
	// other page refreshes its active tab on region switch, and there is no
	// reason for this one to silently do less than that if region-scoping is
	// ever added).
	//
	// ---------------------------------------------------------------------
	// bd gopherstack-uiyi ("appconfigdata disconnected from appconfig
	// control-plane") -- verified REAL, not already fixed
	// ---------------------------------------------------------------------
	// services/appconfigdata/sessions.go's StartSession() requires
	// `b.profiles.Has(key)` for the exact app/env/profile triple or it returns
	// ErrNoActiveDeployment (404 ResourceNotFoundException) -- and the ONLY
	// way to populate that profile map is InMemoryBackend.SetConfiguration,
	// reachable exclusively through gopherstack's own dashboard admin
	// endpoints (POST /dashboard/api/appconfigdata/profiles,
	// dashboard/ui.go:1342). services/appconfig (the control-plane service
	// that owns real Applications/Environments/ConfigurationProfiles/
	// Deployments) never calls SetConfiguration -- confirmed by grepping
	// services/appconfig for any reference to the appconfigdata package and
	// finding none. services/appconfigdata/PARITY.md (2026-07-24, commit
	// 128350087) independently re-confirms this exact gap and cites the same
	// bd issue as still open. So: a configuration created through the real
	// services/appconfig control plane is NOT retrievable through
	// AppConfigData -- the two backends are unaware of each other. This page
	// cannot pretend otherwise, so the "Test Fixtures" tab below is framed
	// explicitly as a gopherstack-only seeding shortcut (using the same
	// dashboard admin endpoints the old version of this page called, just
	// honestly labeled), not as part of the real AppConfig deployment
	// lifecycle, and the Poll Console explains the 404 a visitor will hit if
	// they start a session before seeding one.
	import { onRegionChange } from '$lib/region-effect.svelte';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import {
		Radio, Key, Database, Play, RefreshCw, Copy, Trash2,
		ChevronRight, History, Clock, AlertTriangle, FileJson, FileCode,
		FileText, ArrowRight
	} from 'lucide-svelte';

	// ==================== Wire types (verified against services/appconfigdata/models.go json tags) ====================

	type SafeSession = {
		createdAt: string;
		lastAccessedAt: string;
		lastPollAt: string;
		expiresAt: string;
		// Deliberately never the full token -- store.go's truncateToken()
		// yields "first8…last4" and ListSessionsSafe()'s own doc comment says
		// "never return full tokens in list responses". The previous version
		// of this page read a `token` field from this same endpoint (which
		// does not exist in the response at all -- the real field is
		// `tokenPrefix`) and used it for reveal/copy/terminate actions; since
		// that field was always undefined, `session.token.slice(0, 8)` would
		// throw on the first render of any non-empty session list. Fixed by
		// reading the field that is actually sent, and not offering
		// full-token actions the API cannot supply. See tokenAction below.
		tokenPrefix: string;
		tokenFamilyId: string;
		applicationIdentifier: string;
		environmentIdentifier: string;
		configurationProfileIdentifier: string;
		pollIntervalInSeconds: number;
		pollCount: number;
	};

	type ConfigVersion = {
		updatedAt: string;
		content: string;
		contentType: string;
		contentHash: string;
		versionLabel: string;
		deploymentId: string;
		versionNumber: number;
	};

	type ConfigurationProfile = {
		updatedAt: string;
		applicationIdentifier: string;
		environmentIdentifier: string;
		configurationProfileIdentifier: string;
		content: string;
		contentType: string;
		contentHash: string;
		versionLabel: string;
		deploymentId: string;
		history: ConfigVersion[];
		versionNumber: number;
	};

	type ServiceStats = {
		lastSweepAt: string;
		sessionTtl: string;
		janitorPeriod: string;
		sessionCount: number;
		profileCount: number;
		totalPollCount: number;
		totalPollFailures: number;
		configurationChangeCount: number;
	};

	type PollLogEntry = {
		id: number;
		at: Date;
		changed: boolean;
		error?: string;
		versionLabel?: string;
		pollIntervalInSeconds?: number;
		tokenBefore: string;
		tokenAfter?: string;
	};

	// ==================== Helpers ====================

	// Same shape every other page's describeError produces (err.name from the
	// AWS exception type, err.$metadata.httpStatusCode from the response,
	// err.message from the body) -- built here from a raw fetch Response
	// instead of an SDK-thrown error, since there is no SDK on this page.
	async function awsErrorFromResponse(
		res: Response
	): Promise<Error & { $metadata: { httpStatusCode: number } }> {
		let body: Record<string, unknown> = {};
		try {
			body = await res.json();
		} catch {
			// no/invalid JSON body
		}
		// AWS's REST-JSON error bodies use the literal field name "__type" (see
		// services/appconfigdata/models.go's awsErrorBody) -- read it via
		// bracket notation so the property name (not a variable we chose) isn't
		// flagged by the dangling-underscore lint rule.
		const exceptionType = typeof body['__type'] === 'string' ? (body['__type'] as string) : undefined;
		const exceptionMessage = typeof body['message'] === 'string' ? (body['message'] as string) : undefined;
		return Object.assign(new Error(exceptionMessage ?? res.statusText ?? `status ${res.status}`), {
			name: exceptionType ?? `HTTPError${res.status}`,
			$metadata: { httpStatusCode: res.status }
		});
	}

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
			toast.success(`Copied ${label}`);
		} catch {
			toast.error('Failed to copy');
		}
	}

	function contentTypeIcon(ct: string) {
		if (ct.includes('json')) return FileJson;
		if (ct.includes('yaml') || ct.includes('yml')) return FileCode;
		return FileText;
	}

	function formatContent(content: string, ct: string): string {
		if (!content) return '';
		if (ct.includes('json')) {
			try {
				return JSON.stringify(JSON.parse(content), null, 2);
			} catch {
				/* not valid JSON — show as-is */
			}
		}
		return content;
	}

	const LS_PREFIX = 'gopherstack_appconfigdata_';
	function lsGet(key: string, fallback: string): string {
		try {
			return localStorage.getItem(LS_PREFIX + key) ?? fallback;
		} catch {
			return fallback;
		}
	}
	function lsSet(key: string, value: string): void {
		try {
			localStorage.setItem(LS_PREFIX + key, value);
		} catch {
			// storage unavailable
		}
	}

	// ==================== Poll Console (StartConfigurationSession / GetLatestConfiguration) ====================

	let pcApp = $state(lsGet('app', ''));
	let pcEnv = $state(lsGet('env', ''));
	let pcProfile = $state(lsGet('profile', ''));
	let pcPollInterval = $state(lsGet('pollInterval', ''));

	$effect(() => { lsSet('app', pcApp); });
	$effect(() => { lsSet('env', pcEnv); });
	$effect(() => { lsSet('profile', pcProfile); });
	$effect(() => { lsSet('pollInterval', pcPollInterval); });

	let currentToken = $state<string | null>(null);
	let sessionStartedAt = $state<Date | null>(null);
	let startBusy = $state(false);
	let pollBusy = $state(false);
	let pollError = $state<string | null>(null);
	let noActiveDeployment = $state(false);

	type LastPoll = {
		changed: boolean;
		content: string;
		contentType: string;
		versionLabel: string;
		etag: string;
		nextPollInterval: number;
		at: Date;
	};
	let lastPoll = $state<LastPoll | null>(null);

	let pollLogSeq = 0;
	let pollLog = $state<PollLogEntry[]>([]);

	let autoPoll = $state(false);
	let autoPollCountdown = $state(0);
	let autoPollTimer: ReturnType<typeof setTimeout> | null = null;
	let countdownTimer: ReturnType<typeof setInterval> | null = null;

	function stopAutoPollTimers(): void {
		if (autoPollTimer) clearTimeout(autoPollTimer);
		if (countdownTimer) clearInterval(countdownTimer);
		autoPollTimer = null;
		countdownTimer = null;
	}

	function scheduleNextAutoPoll(seconds: number): void {
		stopAutoPollTimers();
		const secs = Math.max(1, seconds);
		autoPollCountdown = secs;
		countdownTimer = setInterval(() => {
			autoPollCountdown = Math.max(0, autoPollCountdown - 1);
		}, 1000);
		autoPollTimer = setTimeout(() => {
			void pollNow();
		}, secs * 1000);
	}

	function toggleAutoPoll(): void {
		autoPoll = !autoPoll;
		if (autoPoll && currentToken) {
			void pollNow();
		} else {
			stopAutoPollTimers();
		}
	}

	function resetSessionState(): void {
		currentToken = null;
		sessionStartedAt = null;
		lastPoll = null;
		pollLog = [];
		pollError = null;
		noActiveDeployment = false;
		stopAutoPollTimers();
		autoPoll = false;
	}

	async function startSession(): Promise<void> {
		const app = pcApp.trim();
		const env = pcEnv.trim();
		const profile = pcProfile.trim();
		if (!app || !env || !profile) {
			toast.error('Application, environment, and configuration profile identifiers are required');
			return;
		}
		startBusy = true;
		pollError = null;
		noActiveDeployment = false;
		try {
			const interval = pcPollInterval.trim() ? Number(pcPollInterval.trim()) : 0;
			const res = await fetch('/configurationsessions', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					ApplicationIdentifier: app,
					EnvironmentIdentifier: env,
					ConfigurationProfileIdentifier: profile,
					RequiredMinimumPollIntervalInSeconds: interval
				})
			});
			if (!res.ok) {
				const err = await awsErrorFromResponse(res);
				if (err.$metadata.httpStatusCode === 404) noActiveDeployment = true;
				throw err;
			}
			const body = (await res.json()) as { InitialConfigurationToken?: string };
			resetSessionState();
			currentToken = body.InitialConfigurationToken ?? null;
			sessionStartedAt = new Date();
			toast.success('Configuration session started');
			await loadStats();
		} catch (e) {
			pollError = describeError(e);
			toast.error(describeError(e));
		} finally {
			startBusy = false;
		}
	}

	async function handlePollFailure(res: Response, tokenBefore: string): Promise<void> {
		const err = await awsErrorFromResponse(res);
		const retryAfter = res.headers.get('Retry-After');
		const message = retryAfter
			? `${describeError(err)} (Retry-After: ${retryAfter}s)`
			: describeError(err);
		pollLog = [
			{
				id: ++pollLogSeq,
				at: new Date(),
				changed: false,
				error: message,
				tokenBefore
			},
			...pollLog
		];
		pollError = message;
		toast.error(message);
		stopAutoPollTimers();
		autoPoll = false;
	}

	// Applies a successful GetLatestConfiguration response to session state and
	// returns the interval the caller should use to schedule the next auto-poll.
	async function applyPollSuccess(res: Response, tokenBefore: string): Promise<number> {
		// Per services/appconfigdata/handler.go's writeGetLatestConfigurationResponse:
		// these poll-control headers are ALWAYS set, whether or not content
		// changed; Content-Type/ETag are only set when content changed.
		const nextToken = res.headers.get('Next-Poll-Configuration-Token') ?? '';
		const nextIntervalHeader = res.headers.get('Next-Poll-Interval-In-Seconds');
		const nextInterval = nextIntervalHeader ? Number(nextIntervalHeader) : 30;
		const versionLabel = res.headers.get('Version-Label') ?? '';
		const contentTypeHeader = res.headers.get('Content-Type') ?? '';
		const etag = res.headers.get('ETag') ?? '';
		const text = await res.text();
		const changed = text.length > 0;

		// The token from this poll response is the ONLY token valid for the
		// next poll -- tokenBefore is now spent. This is the real single-use
		// rotation the AWS docs describe; if the backend ever failed to
		// rotate this (echoing the same token back, or leaving it blank),
		// this assignment is exactly where that bug would surface, since
		// every subsequent poll uses whatever this variable holds.
		currentToken = nextToken || null;

		lastPoll = {
			changed,
			content: changed ? text : (lastPoll?.content ?? ''),
			contentType: changed ? contentTypeHeader : (lastPoll?.contentType ?? ''),
			versionLabel: versionLabel || (lastPoll?.versionLabel ?? ''),
			etag: etag || (lastPoll?.etag ?? ''),
			nextPollInterval: nextInterval,
			at: new Date()
		};
		pollLog = [
			{
				id: ++pollLogSeq,
				at: new Date(),
				changed,
				versionLabel: versionLabel || undefined,
				pollIntervalInSeconds: nextInterval,
				tokenBefore,
				tokenAfter: nextToken
			},
			...pollLog
		];
		pollError = null;

		return nextInterval;
	}

	async function pollNow(): Promise<void> {
		if (!currentToken) return;
		pollBusy = true;
		const tokenBefore = currentToken;
		try {
			const res = await fetch(`/configuration?configuration_token=${encodeURIComponent(currentToken)}`);
			if (!res.ok) {
				await handlePollFailure(res, tokenBefore);
				return;
			}

			const nextInterval = await applyPollSuccess(res, tokenBefore);

			if (autoPoll) scheduleNextAutoPoll(nextInterval);
		} catch (e) {
			pollError = describeError(e);
			toast.error(describeError(e));
			stopAutoPollTimers();
			autoPoll = false;
		} finally {
			pollBusy = false;
		}
	}

	// Debug-only: real AWS AppConfigData has no operation to end a session --
	// a client simply stops polling and the token idles out (24h absolute /
	// 1h idle TTL, per services/appconfigdata/models.go). This calls
	// gopherstack's own admin endpoint using the full token this browser tab
	// itself holds (never a token discovered from the Sessions tab, which
	// only ever sees a redacted prefix -- see the Sessions tab's info banner).
	async function forceEndCurrentSession(): Promise<void> {
		if (!currentToken) return;
		const ok = await confirmDestructive({
			title: 'Force-expire session',
			message: 'End this session now? This is a gopherstack debug action — real AWS has no API to end a session.',
			confirmLabel: 'Force-expire',
			dangerous: false
		});
		if (!ok) return;
		try {
			const res = await fetch(`/dashboard/api/appconfigdata/sessions/${encodeURIComponent(currentToken)}`, {
				method: 'DELETE'
			});
			if (!res.ok && res.status !== 404) throw new Error(`status ${res.status}`);
			toast.success('Session force-expired (gopherstack debug action, not a real AWS operation)');
			resetSessionState();
			await loadStats();
			if (tabLoader.isLoaded('sessions')) await tabLoader.refresh('sessions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	function jumpToFixturesWithCurrent(): void {
		activeTab = 'fixtures';
		tabLoader.load('fixtures');
	}

	// ==================== Sessions (admin/debug observability — real backend state, not an AWS API) ====================

	let sessions = $state<SafeSession[]>([]);
	let sessionSearch = $state('');

	async function fetchSessions(): Promise<void> {
		const res = await fetch('/dashboard/api/appconfigdata/sessions');
		if (!res.ok) throw new Error(`status ${res.status}`);
		const data = (await res.json()) as { sessions?: SafeSession[] };
		sessions = data.sessions ?? [];
	}

	const filteredSessions = $derived(
		sessions.filter((s) =>
			matches(
				sessionSearch,
				s.applicationIdentifier,
				s.environmentIdentifier,
				s.configurationProfileIdentifier,
				s.tokenPrefix
			)
		)
	);

	function idlePercent(s: SafeSession): number {
		if (!s.lastAccessedAt || s.lastAccessedAt === '0001-01-01T00:00:00Z') return 0;
		const idleMs = Date.now() - new Date(s.lastAccessedAt).getTime();
		// DefaultSessionTTL: 1h idle cap
		const ttlMs = 60 * 60 * 1000;
		return Math.min(100, Math.round((idleMs / ttlMs) * 100));
	}

	// ==================== Test Fixtures (gopherstack-only seeding; see header comment / gopherstack-uiyi) ====================

	let profiles = $state<ConfigurationProfile[]>([]);
	let profileSearch = $state('');
	let selectedProfile = $state<ConfigurationProfile | null>(null);
	let showHistory = $state(false);

	async function fetchProfiles(): Promise<void> {
		const res = await fetch('/dashboard/api/appconfigdata/profiles');
		if (!res.ok) throw new Error(`status ${res.status}`);
		const data = (await res.json()) as { profiles?: ConfigurationProfile[] };
		profiles = data.profiles ?? [];
		if (selectedProfile) {
			const refreshed = profiles.find(
				(p) =>
					p.applicationIdentifier === selectedProfile!.applicationIdentifier &&
					p.environmentIdentifier === selectedProfile!.environmentIdentifier &&
					p.configurationProfileIdentifier === selectedProfile!.configurationProfileIdentifier
			);
			selectedProfile = refreshed ?? null;
		}
	}

	const filteredProfiles = $derived(
		profiles.filter((p) =>
			matches(profileSearch, p.applicationIdentifier, p.environmentIdentifier, p.configurationProfileIdentifier)
		)
	);

	let seedApp = $state('');
	let seedEnv = $state('');
	let seedProfileId = $state('');
	let seedContent = $state('{}');
	let seedContentType = $state('application/json');
	let seeding = $state(false);

	function prefillSeedFromPollConsole(): void {
		seedApp = pcApp.trim();
		seedEnv = pcEnv.trim();
		seedProfileId = pcProfile.trim();
	}

	async function seedFixture(): Promise<void> {
		if (!seedApp.trim() || !seedEnv.trim() || !seedProfileId.trim()) {
			toast.error('Application, environment, and profile identifiers are required');
			return;
		}
		seeding = true;
		try {
			const res = await fetch('/dashboard/api/appconfigdata/profiles', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					applicationIdentifier: seedApp.trim(),
					environmentIdentifier: seedEnv.trim(),
					configurationProfileIdentifier: seedProfileId.trim(),
					content: seedContent,
					contentType: seedContentType
				})
			});
			if (!res.ok) {
				const err = (await res.json()) as { message?: string };
				throw new Error(err.message ?? `status ${res.status}`);
			}
			toast.success('Fixture seeded (gopherstack admin only — not visible to services/appconfig)');
			seedApp = '';
			seedEnv = '';
			seedProfileId = '';
			seedContent = '{}';
			seedContentType = 'application/json';
			await fetchProfiles();
			await loadStats();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			seeding = false;
		}
	}

	async function deleteFixture(p: ConfigurationProfile): Promise<void> {
		const ok = await confirmDestructive(
			`Delete fixture "${p.configurationProfileIdentifier}"? Any session started against it will fail its next poll.`
		);
		if (!ok) return;

		const params = new URLSearchParams({
			applicationIdentifier: p.applicationIdentifier,
			environmentIdentifier: p.environmentIdentifier,
			configurationProfileIdentifier: p.configurationProfileIdentifier
		});
		try {
			const res = await fetch(`/dashboard/api/appconfigdata/profiles?${params}`, { method: 'DELETE' });
			if (!res.ok && res.status !== 404) throw new Error(`status ${res.status}`);
			toast.success('Fixture deleted');
			if (
				selectedProfile?.applicationIdentifier === p.applicationIdentifier &&
				selectedProfile?.environmentIdentifier === p.environmentIdentifier &&
				selectedProfile?.configurationProfileIdentifier === p.configurationProfileIdentifier
			) {
				selectedProfile = null;
			}
			await fetchProfiles();
			await loadStats();
			if (tabLoader.isLoaded('sessions')) await tabLoader.refresh('sessions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	function useFixtureInPollConsole(p: ConfigurationProfile): void {
		pcApp = p.applicationIdentifier;
		pcEnv = p.environmentIdentifier;
		pcProfile = p.configurationProfileIdentifier;
		activeTab = 'poll';
		toast.success('Filled into Poll Console — click Start Session');
	}

	// ==================== Stats (aggregate counters, real backend state) ====================

	let stats = $state<ServiceStats | null>(null);

	async function loadStats(): Promise<void> {
		try {
			const res = await fetch('/dashboard/api/appconfigdata/stats');
			if (!res.ok) return;
			stats = await res.json();
		} catch {
			stats = null;
		}
	}

	// ==================== Tabs ====================

	type TabId = 'poll' | 'sessions' | 'fixtures';

	const tabs: TabDef[] = [
		{ id: 'poll', label: 'Poll Console' },
		{ id: 'sessions', label: 'Sessions' },
		{ id: 'fixtures', label: 'Test Fixtures' }
	];

	let activeTab = $state<TabId>('poll');

	const tabLoader = createTabLoader<TabId>({
		// Nothing to bootstrap-fetch: the poll console only shows data once the
		// operator starts a session from within the tab itself.
		poll: () => Promise.resolve(),
		sessions: () => fetchSessions().catch(rethrowDescribed),
		fixtures: () => fetchProfiles().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		if (id === 'sessions') sessionSearch = '';
		if (id === 'fixtures') profileSearch = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		void loadStats();
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		void loadStats();
		tabLoader.refresh(activeTab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	function tokenPreview(token: string): string {
		if (token.length <= 12) return token;
		return `${token.slice(0, 8)}…${token.slice(-4)}`;
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Radio}
		title="AppConfig Data"
		description="Poll for the latest configuration with StartConfigurationSession / GetLatestConfiguration — AppConfigData has no listable resources of its own."
		onRefresh={handleRefresh}
		color="indigo"
	/>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 gap-4 sm:grid-cols-4">
		<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4">
			<div class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				<Key class="h-3.5 w-3.5" /> Active Sessions
			</div>
			<div class="mt-1 text-2xl font-bold text-gray-900 dark:text-white">{stats?.sessionCount ?? sessions.length}</div>
		</div>
		<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4">
			<div class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				<Database class="h-3.5 w-3.5" /> Fixtures
			</div>
			<div class="mt-1 text-2xl font-bold text-gray-900 dark:text-white">{stats?.profileCount ?? profiles.length}</div>
		</div>
		<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4">
			<div class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				<Clock class="h-3.5 w-3.5" /> Total Polls
			</div>
			<div class="mt-1 text-2xl font-bold text-gray-900 dark:text-white">{stats?.totalPollCount ?? 0}</div>
			{#if stats && stats.totalPollFailures > 0}
				<div class="mt-0.5 text-xs text-red-500">{stats.totalPollFailures} failed</div>
			{/if}
		</div>
		<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4">
			<div class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
				<RefreshCw class="h-3.5 w-3.5" /> Session TTL
			</div>
			<div class="mt-1 text-2xl font-bold text-gray-900 dark:text-white">{stats?.sessionTtl ?? '1h idle / 24h'}</div>
		</div>
	</div>

	<Tabs {tabs} active={activeTab} onSelect={switchTab} color="indigo" />

	{#if activeTabError}
		<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
			<p class="font-medium">Failed to load data</p>
			<p>{activeTabError}</p>
		</div>
	{/if}

	<!-- ==================== POLL CONSOLE ==================== -->
	{#if activeTab === 'poll'}
		<div class="space-y-4">
			{#if pollError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Request failed</p>
					<p>{pollError}</p>
				</div>
			{/if}
			<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-3">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">StartConfigurationSession</h2>
				<div class="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-3 text-sm">
					<div>
						<label for="pc-app" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Application *</label>
						<input id="pc-app" bind:value={pcApp} disabled={!!currentToken} type="text" placeholder="my-app" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm disabled:opacity-60" />
					</div>
					<div>
						<label for="pc-env" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Environment *</label>
						<input id="pc-env" bind:value={pcEnv} disabled={!!currentToken} type="text" placeholder="prod" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm disabled:opacity-60" />
					</div>
					<div>
						<label for="pc-profile" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Configuration Profile *</label>
						<input id="pc-profile" bind:value={pcProfile} disabled={!!currentToken} type="text" placeholder="my-profile" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm disabled:opacity-60" />
					</div>
					<div>
						<label for="pc-interval" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Poll Interval (s) <span class="text-gray-400">15–86400, optional</span></label>
						<input id="pc-interval" bind:value={pcPollInterval} disabled={!!currentToken} type="number" min="15" max="86400" placeholder="default 30" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm disabled:opacity-60" />
					</div>
				</div>

				{#if !currentToken}
					<button
						onclick={startSession}
						disabled={startBusy || !pcApp.trim() || !pcEnv.trim() || !pcProfile.trim()}
						class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium disabled:opacity-50"
					>
						<Play class="w-4 h-4" /> {startBusy ? 'Starting…' : 'Start Session'}
					</button>
				{:else}
					<div class="flex flex-wrap items-center gap-3">
						<div class="text-xs text-gray-500 dark:text-gray-400">
							Session started {sessionStartedAt ? formatDate(sessionStartedAt) : ''} · current token
							<code class="rounded bg-gray-100 dark:bg-gray-800 px-1.5 py-0.5 font-mono">{tokenPreview(currentToken)}</code>
						</div>
						<button onclick={forceEndCurrentSession} class="text-xs text-red-600 dark:text-red-400 hover:underline" title="gopherstack debug action — real AWS has no operation to end a session">
							Force-expire session (debug)
						</button>
						<button onclick={resetSessionState} class="text-xs text-gray-500 hover:underline">Forget locally</button>
					</div>
				{/if}

				{#if noActiveDeployment}
					<div class="rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-4 py-3 text-sm text-amber-800 dark:text-amber-300 flex items-start gap-2">
						<AlertTriangle class="w-4 h-4 mt-0.5 shrink-0" />
						<div>
							<p class="font-medium">No active deployment for this app/env/profile.</p>
							<p class="mt-1">
								This matches real AWS: <code>GetLatestConfiguration</code> only ever serves an
								<em>active AppConfig deployment</em>. gopherstack's AppConfigData store is not wired to the
								AppConfig control plane (services/appconfig) — see bd <code>gopherstack-uiyi</code> — so the only
								way to give this session something to return is to seed it as a test fixture.
							</p>
							<button onclick={() => { prefillSeedFromPollConsole(); jumpToFixturesWithCurrent(); }} class="mt-2 inline-flex items-center gap-1 text-amber-900 dark:text-amber-200 font-medium hover:underline">
								Seed this profile in Test Fixtures <ArrowRight class="w-3.5 h-3.5" />
							</button>
						</div>
					</div>
				{/if}
			</div>

			{#if currentToken || lastPoll}
				<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-3">
					<div class="flex items-center justify-between">
						<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">GetLatestConfiguration</h2>
						<div class="flex items-center gap-2">
							<button
								onclick={toggleAutoPoll}
								disabled={!currentToken}
								class="flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-xs font-medium disabled:opacity-50
									{autoPoll
										? 'border-indigo-300 bg-indigo-50 text-indigo-700 dark:border-indigo-600 dark:bg-indigo-900/30 dark:text-indigo-300'
										: 'border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-400 hover:bg-gray-50 dark:hover:bg-gray-800'}"
							>
								<RefreshCw class="h-3.5 w-3.5 {autoPoll ? 'animate-spin' : ''}" />
								{autoPoll ? `Auto-polling (next in ${autoPollCountdown}s)` : 'Auto-poll'}
							</button>
							<button
								onclick={() => void pollNow()}
								disabled={!currentToken || pollBusy}
								class="flex items-center gap-2 px-3 py-1.5 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-xs font-medium disabled:opacity-50"
							>
								<RefreshCw class="w-3.5 h-3.5 {pollBusy ? 'animate-spin' : ''}" /> Poll Now
							</button>
						</div>
					</div>

					{#if lastPoll}
						<div class="flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
							<span class="rounded-full px-2 py-0.5 {lastPoll.changed ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300'}">
								{lastPoll.changed ? 'Content updated' : 'No change since last poll'}
							</span>
							{#if lastPoll.contentType}<span class="rounded bg-gray-100 dark:bg-gray-800 px-1.5 py-0.5">{lastPoll.contentType}</span>{/if}
							{#if lastPoll.versionLabel}<span>Version-Label: <strong>{lastPoll.versionLabel}</strong></span>{/if}
							<span>Next-Poll-Interval-In-Seconds: <strong>{lastPoll.nextPollInterval}</strong></span>
							{#if lastPoll.etag}<span class="font-mono">ETag: {lastPoll.etag}</span>{/if}
							<span>{formatDate(lastPoll.at)}</span>
						</div>

						{#if lastPoll.content}
							<div class="relative">
								<button onclick={() => copyToClipboard(lastPoll!.content, 'configuration content')} class="absolute right-3 top-3 rounded bg-gray-800/50 p-1.5 text-gray-300 hover:bg-gray-800/80" title="Copy content">
									<Copy class="h-4 w-4" />
								</button>
								<pre class="max-h-72 overflow-auto rounded-lg bg-gray-950 p-4 text-xs leading-relaxed text-green-300">{formatContent(lastPoll.content, lastPoll.contentType)}</pre>
							</div>
						{:else}
							<p class="text-sm text-gray-500 dark:text-gray-400 italic">
								Empty body — the configuration has not changed since the last poll. Real AWS never returns 204 for
								this; it is always 200 with an empty payload, exactly what happened here.
							</p>
						{/if}
					{/if}

					{#if pollLog.length > 0}
						<div>
							<h3 class="text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wide mb-2">
								Poll log (this browser tab only — AppConfigData has no server-side poll history API)
							</h3>
							<div class="space-y-1 max-h-56 overflow-auto pr-1">
								{#each pollLog as entry (entry.id)}
									<div class="flex items-center gap-3 text-xs border-b border-gray-100 dark:border-gray-800 pb-1">
										<span class="text-gray-400 w-20 shrink-0">{formatDate(entry.at)}</span>
										{#if entry.error}
											<span class="text-red-500 truncate">{entry.error}</span>
										{:else}
											<span class="{entry.changed ? 'text-green-600 dark:text-green-400' : 'text-gray-500'} w-32 shrink-0">
												{entry.changed ? 'changed' : 'unchanged'}
											</span>
											<span class="font-mono text-gray-400 flex items-center gap-1">
												{tokenPreview(entry.tokenBefore)}
												{#if entry.tokenAfter}<ChevronRight class="w-3 h-3" />{tokenPreview(entry.tokenAfter)}{/if}
											</span>
											{#if entry.versionLabel}<span class="text-gray-400">v:{entry.versionLabel}</span>{/if}
										{/if}
									</div>
								{/each}
							</div>
						</div>
					{/if}
				</div>
			{/if}
		</div>

	<!-- ==================== SESSIONS (gopherstack admin observability) ==================== -->
	{:else if activeTab === 'sessions'}
		<div class="rounded-lg border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 px-4 py-2 text-xs text-gray-500 dark:text-gray-400 mb-3">
			Admin-only view of live sessions across every client, sourced from gopherstack's internal janitor state —
			not an AWS API. Tokens are shown as the truncated prefix the backend safely exposes
			(<code>tokenPrefix</code>); the full token is never returned here, so sessions cannot be terminated from
			this list. To end a session you started yourself, use "Force-expire session" on the Poll Console tab.
		</div>
		<div class="flex items-center justify-between gap-3 mb-3">
			<span class="text-sm text-gray-600 dark:text-gray-400">{filteredSessions.length} session{filteredSessions.length === 1 ? '' : 's'}</span>
			<SearchInput bind:value={sessionSearch} placeholder="Filter by app, env, profile, token…" />
		</div>

		{#snippet sessTokenCell(s: SafeSession)}
				<code class="text-xs font-mono">{s.tokenPrefix}</code>
			{/snippet}
			{#snippet sessTargetCell(s: SafeSession)}
				<div>{s.applicationIdentifier}</div>
				<div class="text-xs text-gray-500 dark:text-gray-400">{s.environmentIdentifier} / {s.configurationProfileIdentifier}</div>
			{/snippet}
			{#snippet sessCreatedCell(s: SafeSession)}
				{formatDate(s.createdAt)}
			{/snippet}
			{#snippet sessLastPollCell(s: SafeSession)}
				{s.lastPollAt && s.lastPollAt !== '0001-01-01T00:00:00Z' ? formatDate(s.lastPollAt) : '—'}
			{/snippet}
			{#snippet sessIntervalCell(s: SafeSession)}
				{s.pollIntervalInSeconds > 0 ? `${s.pollIntervalInSeconds}s` : '30s (default)'}
			{/snippet}
			{#snippet sessIdleCell(s: SafeSession)}
				{@const pct = idlePercent(s)}
				<div class="flex items-center gap-1.5">
					<div class="h-1.5 w-16 overflow-hidden rounded-full bg-gray-100 dark:bg-gray-700">
						<div class="h-full rounded-full {pct >= 90 ? 'bg-red-500' : pct >= 75 ? 'bg-orange-500' : pct >= 50 ? 'bg-yellow-500' : 'bg-green-500'}" style="width: {pct}%"></div>
					</div>
					<span class="text-xs text-gray-500 dark:text-gray-400">{pct}%</span>
				</div>
			{/snippet}
			{@const sessionColumns = defineColumns<SafeSession>([
				{ key: 'tokenPrefix', label: 'Token', render: sessTokenCell },
				{ key: 'target', label: 'Application / Env / Profile', render: sessTargetCell },
				{ key: 'createdAt', label: 'Created', render: sessCreatedCell },
				{ key: 'lastPollAt', label: 'Last Poll', render: sessLastPollCell },
				{ key: 'pollCount', label: 'Polls' },
				{ key: 'pollIntervalInSeconds', label: 'Interval', render: sessIntervalCell },
				{ key: 'idle', label: 'Idle', render: sessIdleCell }
			])}
			<DataTable
				rows={filteredSessions}
				rowKey={(s) => s.tokenPrefix + s.createdAt}
				columns={sessionColumns}
				loading={tabLoader.isLoading('sessions')}
				emptyMessage="No active sessions. Start one from the Poll Console tab."
			/>

	<!-- ==================== TEST FIXTURES (gopherstack-only seeding — see header comment) ==================== -->
	{:else if activeTab === 'fixtures'}
		<div class="rounded-lg border border-amber-200 dark:border-amber-800 bg-amber-50 dark:bg-amber-900/20 px-4 py-3 text-xs text-amber-800 dark:text-amber-300 mb-4">
			<strong>Not a real AWS operation.</strong> Seeding here calls gopherstack's internal dashboard admin
			endpoint directly — it is the only way to give <code>StartConfigurationSession</code> something to
			return, because services/appconfig (the real control plane that owns Applications, Environments,
			Configuration Profiles, and Deployments) is not wired to this data store. See bd
			<code>gopherstack-uiyi</code>. A fixture seeded here is invisible to services/appconfig; it has no
			DeploymentId, no rollout state, and no relationship to a real deployment.
		</div>

		<div class="rounded-xl border border-gray-200 dark:border-gray-700 p-4 space-y-3 mb-4">
			<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Seed a fixture</h2>
			<div class="grid grid-cols-1 sm:grid-cols-3 gap-3 text-sm">
				<div>
					<label for="seed-app" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Application</label>
					<input id="seed-app" bind:value={seedApp} type="text" placeholder="my-app" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
				</div>
				<div>
					<label for="seed-env" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Environment</label>
					<input id="seed-env" bind:value={seedEnv} type="text" placeholder="prod" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
				</div>
				<div>
					<label for="seed-profile" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Profile ID</label>
					<input id="seed-profile" bind:value={seedProfileId} type="text" placeholder="my-profile" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
				</div>
			</div>
			<div>
				<label for="seed-content-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Content Type</label>
				<select id="seed-content-type" bind:value={seedContentType} class="w-full sm:w-64 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm">
					<option value="application/json">application/json</option>
					<option value="application/x-yaml">application/x-yaml</option>
					<option value="text/plain">text/plain</option>
					<option value="application/octet-stream">application/octet-stream</option>
				</select>
			</div>
			<div>
				<label for="seed-content" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Content</label>
				<textarea id="seed-content" bind:value={seedContent} rows={5} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono"></textarea>
			</div>
			<button onclick={seedFixture} disabled={seeding} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm font-medium disabled:opacity-50">
				{seeding ? 'Seeding…' : 'Seed Fixture'}
			</button>
		</div>

		<div class="flex items-center justify-between gap-3 mb-4">
			<span class="text-sm text-gray-600 dark:text-gray-400">{filteredProfiles.length} fixture{filteredProfiles.length === 1 ? '' : 's'}</span>
			<SearchInput bind:value={profileSearch} placeholder="Filter fixtures…" />
		</div>

		{#snippet fixNameCell(p: ConfigurationProfile)}
				{@const Icon = contentTypeIcon(p.contentType)}
				<button onclick={() => { selectedProfile = p; showHistory = false; }} class="flex items-center gap-1.5 text-left hover:text-indigo-600 dark:hover:text-indigo-400">
					<Icon class="h-4 w-4 shrink-0 text-gray-400" />
					<span>
						<div class="font-medium text-gray-900 dark:text-white">{p.applicationIdentifier}</div>
						<div class="text-xs text-gray-500 dark:text-gray-400">{p.environmentIdentifier} / {p.configurationProfileIdentifier}</div>
					</span>
				</button>
			{/snippet}
			{#snippet fixUpdatedCell(p: ConfigurationProfile)}
				{formatDate(p.updatedAt)}
			{/snippet}
			{#snippet fixActionsCell(p: ConfigurationProfile)}
				<div class="flex items-center gap-3 justify-end">
					<button onclick={() => useFixtureInPollConsole(p)} class="text-xs text-indigo-600 dark:text-indigo-400 hover:underline">Use in Poll Console</button>
					<button onclick={() => void deleteFixture(p)} aria-label="Delete fixture {p.configurationProfileIdentifier}" class="text-gray-400 hover:text-red-600"><Trash2 class="w-4 h-4" /></button>
				</div>
			{/snippet}
			{@const fixtureColumns = defineColumns<ConfigurationProfile>([
				{ key: 'name', label: 'Application / Env / Profile', render: fixNameCell },
				{ key: 'updatedAt', label: 'Updated', render: fixUpdatedCell },
				{ key: 'actions', label: '', render: fixActionsCell }
			])}
			<div class="grid gap-6 lg:grid-cols-5">
				<div class="lg:col-span-2">
					<DataTable
						rows={filteredProfiles}
						rowKey={(p) => `${p.applicationIdentifier}/${p.environmentIdentifier}/${p.configurationProfileIdentifier}`}
						columns={fixtureColumns}
						loading={tabLoader.isLoading('fixtures')}
						emptyMessage="No fixtures seeded yet."
					/>
				</div>

				<div class="rounded-2xl border border-gray-200 dark:border-gray-700 lg:col-span-3">
					{#if !selectedProfile}
						<div class="flex h-full min-h-64 items-center justify-center px-6 py-20">
							<p class="text-sm text-gray-500 dark:text-gray-400">Select a fixture to preview its content</p>
						</div>
					{:else}
						{@const Icon = contentTypeIcon(selectedProfile.contentType)}
						<div class="border-b border-gray-200 dark:border-gray-700 px-6 py-4">
							<div class="flex items-center gap-2">
								<Icon class="h-5 w-5 text-indigo-500" />
								<h3 class="font-semibold text-gray-900 dark:text-white">
									{selectedProfile.applicationIdentifier} / {selectedProfile.environmentIdentifier} / {selectedProfile.configurationProfileIdentifier}
								</h3>
							</div>
							<div class="mt-1 flex flex-wrap items-center gap-3 text-xs text-gray-500 dark:text-gray-400">
								{#if selectedProfile.contentType}<span class="rounded bg-gray-100 dark:bg-gray-700 px-1.5 py-0.5">{selectedProfile.contentType}</span>{/if}
								<span>Updated {formatDate(selectedProfile.updatedAt)}</span>
								{#if selectedProfile.contentHash}<span class="font-mono">sha256: {selectedProfile.contentHash.slice(0, 12)}…</span>{/if}
								<span title="Real AWS: DeploymentId ties a config version to a specific StartDeployment call. Always empty here — see gopherstack-uiyi.">
									DeploymentId: {selectedProfile.deploymentId || '(never populated — gopherstack-uiyi)'}
								</span>
							</div>
						</div>

						<div class="flex items-center gap-2 border-b border-gray-200 dark:border-gray-700 px-6 py-2">
							<button onclick={() => (showHistory = false)} class="rounded-md px-3 py-1.5 text-sm font-medium {!showHistory ? 'bg-indigo-50 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300' : 'text-gray-600 hover:bg-gray-50 dark:text-gray-400 dark:hover:bg-gray-800'}">
								Current
							</button>
							<button onclick={() => (showHistory = true)} class="flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium {showHistory ? 'bg-indigo-50 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300' : 'text-gray-600 hover:bg-gray-50 dark:text-gray-400 dark:hover:bg-gray-800'}">
								<History class="h-3.5 w-3.5" /> History
								{#if selectedProfile.history?.length > 0}
									<span class="rounded-full bg-gray-200 dark:bg-gray-600 px-1.5 py-0.5 text-xs">{selectedProfile.history.length}</span>
								{/if}
							</button>
						</div>

						<div class="p-6">
							{#if !showHistory}
								{#if selectedProfile.content}
									<pre class="max-h-96 overflow-auto rounded-lg bg-gray-950 p-4 text-xs leading-relaxed text-green-300">{formatContent(selectedProfile.content, selectedProfile.contentType)}</pre>
								{:else}
									<p class="text-sm text-gray-500 dark:text-gray-400">No content stored for this fixture.</p>
								{/if}
							{:else if !selectedProfile.history || selectedProfile.history.length === 0}
								<p class="text-sm text-gray-500 dark:text-gray-400">No history yet — captured each time this fixture's content is re-seeded.</p>
							{:else}
								<div class="space-y-3">
									{#each selectedProfile.history as version, i (version.updatedAt + String(i))}
										<details class="group rounded-lg border border-gray-200 dark:border-gray-700">
											<summary class="flex cursor-pointer list-none items-center justify-between px-4 py-2.5">
												<span class="text-sm font-medium text-gray-700 dark:text-gray-300">v{selectedProfile.history.length - i}</span>
												<span class="text-xs text-gray-500 dark:text-gray-400">{formatDate(version.updatedAt)}</span>
												<ChevronRight class="h-4 w-4 text-gray-400 transition-transform group-open:rotate-90" />
											</summary>
											<div class="border-t border-gray-100 dark:border-gray-700">
												<pre class="max-h-64 overflow-auto p-4 text-xs leading-relaxed text-gray-700 dark:text-gray-300">{formatContent(version.content, version.contentType) || '(empty)'}</pre>
											</div>
										</details>
									{/each}
								</div>
							{/if}
						</div>
					{/if}
				</div>
			</div>
	{/if}
</div>
