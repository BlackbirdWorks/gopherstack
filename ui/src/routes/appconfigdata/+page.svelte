<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import {
		RefreshCw, Clock, Key, Database, ChevronRight, History,
		Gauge, Copy, Check, Trash2, Plus, Search, X, AlertTriangle,
		Activity, Timer, Eye, EyeOff, FileCode, FileJson, FileText
	} from 'lucide-svelte';

	// ─── Types ──────────────────────────────────────────────────────────────────

	type ConfigVersion = {
		content: string;
		contentType: string;
		contentHash: string;
		updatedAt: string;
	};

	type Profile = {
		applicationIdentifier: string;
		environmentIdentifier: string;
		configurationProfileIdentifier: string;
		content: string;
		contentType: string;
		contentHash: string;
		updatedAt: string;
		history: ConfigVersion[];
	};

	type Session = {
		token: string;
		applicationIdentifier: string;
		environmentIdentifier: string;
		configurationProfileIdentifier: string;
		createdAt: string;
		lastAccessedAt: string;
		pollIntervalInSeconds: number;
		pollCount: number;
	};

	type Stats = {
		sessionCount: number;
		profileCount: number;
		lastSweepAt: string;
		sessionTtl: string;
		janitorPeriod: string;
	};

	// ─── State ──────────────────────────────────────────────────────────────────

	let sessions = $state<Session[]>([]);
	let profiles = $state<Profile[]>([]);
	let stats = $state<Stats | null>(null);
	let loadingSessions = $state(false);
	let loadingProfiles = $state(false);
	let activeTab = $state<'sessions' | 'profiles'>('sessions');
	let selectedProfile = $state<Profile | null>(null);
	let showHistory = $state(false);

	// Search/filter
	let sessionSearch = $state('');
	let profileSearch = $state('');

	// Auto-refresh
	let autoRefresh = $state(false);
	let refreshInterval = $state<ReturnType<typeof setInterval> | null>(null);
	let countdown = $state(0);
	let countdownInterval = $state<ReturnType<typeof setInterval> | null>(null);
	const autoRefreshSeconds = 30;

	// Sort
	let sessionSortKey = $state<'createdAt' | 'lastAccessedAt' | 'pollCount'>('lastAccessedAt');
	let sessionSortAsc = $state(false);

	// Copy feedback
	let copiedToken = $state<string | null>(null);
	let copiedContent = $state(false);

	// Expand token
	let expandedToken = $state<string | null>(null);

	// Create profile modal
	let showCreateModal = $state(false);
	let creating = $state(false);
	let newApp = $state('');
	let newEnv = $state('');
	let newProfileId = $state('');
	let newContent = $state('{}');
	let newContentType = $state('application/json');

	// ─── Derived ────────────────────────────────────────────────────────────────

	const sessionTTLHours = $derived(() => {
		if (!stats?.sessionTtl) return 24;
		const match = stats.sessionTtl.match(/(\d+)h/);
		return match ? parseInt(match[1], 10) : 24;
	});

	const filteredSessions = $derived(
		sessions
			.filter((s) => {
				const q = sessionSearch.toLowerCase();
				return (
					!q ||
					s.applicationIdentifier.toLowerCase().includes(q) ||
					s.environmentIdentifier.toLowerCase().includes(q) ||
					s.configurationProfileIdentifier.toLowerCase().includes(q) ||
					s.token.toLowerCase().includes(q)
				);
			})
			.toSorted((a, b) => {
				let av = a[sessionSortKey];
				let bv = b[sessionSortKey];
				if (typeof av === 'string') av = new Date(av).getTime();
				if (typeof bv === 'string') bv = new Date(bv).getTime();
				return sessionSortAsc ? (av as number) - (bv as number) : (bv as number) - (av as number);
			})
	);

	const filteredProfiles = $derived(
		profiles.filter((p) => {
			const q = profileSearch.toLowerCase();
			return (
				!q ||
				p.applicationIdentifier.toLowerCase().includes(q) ||
				p.environmentIdentifier.toLowerCase().includes(q) ||
				p.configurationProfileIdentifier.toLowerCase().includes(q)
			);
		})
	);

	// ─── Helpers ────────────────────────────────────────────────────────────────

	function formatTime(iso: string): string {
		if (!iso || iso === '0001-01-01T00:00:00Z') return '—';
		return new Date(iso).toLocaleString();
	}

	function timeAgo(iso: string): string {
		if (!iso || iso === '0001-01-01T00:00:00Z') return '';
		const diff = Date.now() - new Date(iso).getTime();
		const s = Math.floor(diff / 1000);
		if (s < 60) return `${s}s ago`;
		const m = Math.floor(s / 60);
		if (m < 60) return `${m}m ago`;
		const h = Math.floor(m / 60);
		if (h < 24) return `${h}h ago`;
		return `${Math.floor(h / 24)}d ago`;
	}

	function sessionAgePercent(session: Session): number {
		if (!session.lastAccessedAt || session.lastAccessedAt === '0001-01-01T00:00:00Z') return 0;
		const idleMs = Date.now() - new Date(session.lastAccessedAt).getTime();
		const ttlMs = sessionTTLHours() * 60 * 60 * 1000;
		return Math.min(100, Math.round((idleMs / ttlMs) * 100));
	}

	function sessionStaleClass(session: Session): string {
		const pct = sessionAgePercent(session);
		if (pct >= 90) return 'border-l-4 border-red-400';
		if (pct >= 75) return 'border-l-4 border-orange-400';
		if (pct >= 50) return 'border-l-4 border-yellow-400';
		return '';
	}

	function pollIntervalColor(seconds: number): string {
		if (seconds === 0) return 'bg-slate-300 dark:bg-slate-600';
		if (seconds <= 15) return 'bg-green-500';
		if (seconds <= 60) return 'bg-yellow-500';
		return 'bg-orange-500';
	}

	function pollIntervalWidth(seconds: number): number {
		if (seconds === 0) return 30;
		return Math.min(100, Math.round((seconds / 300) * 100));
	}

	function pollIntervalLabel(seconds: number): string {
		if (seconds === 0) return '30s (default)';
		return `${seconds}s`;
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

	async function copyToClipboard(text: string, feedback: () => void) {
		try {
			await navigator.clipboard.writeText(text);
			feedback();
		} catch {
			toast.error('Copy failed');
		}
	}

	function toggleSort(key: typeof sessionSortKey) {
		if (sessionSortKey === key) {
			sessionSortAsc = !sessionSortAsc;
		} else {
			sessionSortKey = key;
			sessionSortAsc = false;
		}
	}

	// ─── Data loading ────────────────────────────────────────────────────────────

	async function loadSessions() {
		loadingSessions = true;
		try {
			const res = await fetch('/dashboard/api/appconfigdata/sessions');
			if (!res.ok) throw new Error(`status ${res.status}`);
			const data = await res.json() as { sessions?: Session[] };
			sessions = data.sessions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load sessions: ${(err as Error).message}`);
		} finally {
			loadingSessions = false;
		}
	}

	async function loadProfiles() {
		loadingProfiles = true;
		try {
			const res = await fetch('/dashboard/api/appconfigdata/profiles');
			if (!res.ok) throw new Error(`status ${res.status}`);
			const data = await res.json() as { profiles?: Profile[] };
			profiles = data.profiles ?? [];
			// Refresh selected profile data if it's still in the list.
			if (selectedProfile) {
				const refreshed = profiles.find(
					(p) =>
						p.applicationIdentifier === selectedProfile!.applicationIdentifier &&
						p.environmentIdentifier === selectedProfile!.environmentIdentifier &&
						p.configurationProfileIdentifier === selectedProfile!.configurationProfileIdentifier
				);
				selectedProfile = refreshed ?? null;
			}
		} catch (err: unknown) {
			toast.error(`Failed to load profiles: ${(err as Error).message}`);
		} finally {
			loadingProfiles = false;
		}
	}

	async function loadStats() {
		try {
			const res = await fetch('/dashboard/api/appconfigdata/stats');
			if (!res.ok) return;
			stats = await res.json() as Stats;
		} catch {
			/* non-critical */
		}
	}

	async function refreshAll() {
		await Promise.all([loadSessions(), loadProfiles(), loadStats()]);
	}

	// ─── Session actions ─────────────────────────────────────────────────────────

	async function terminateSession(token: string) {
		const ok = await confirmDestructive('Terminate this session? The client will receive an error on its next poll.');
		if (!ok) return;

		try {
			const res = await fetch(`/dashboard/api/appconfigdata/sessions/${encodeURIComponent(token)}`, {
				method: 'DELETE',
			});
			if (!res.ok && res.status !== 404) throw new Error(`status ${res.status}`);
			toast.success('Session terminated');
			await loadSessions();
		} catch (err: unknown) {
			toast.error(`Failed to terminate session: ${(err as Error).message}`);
		}
	}

	// ─── Profile actions ─────────────────────────────────────────────────────────

	async function deleteProfile(p: Profile) {
		const ok = await confirmDestructive(
			`Delete profile "${p.configurationProfileIdentifier}"? All associated sessions will also be terminated.`
		);
		if (!ok) return;

		const params = new URLSearchParams({
			applicationIdentifier: p.applicationIdentifier,
			environmentIdentifier: p.environmentIdentifier,
			configurationProfileIdentifier: p.configurationProfileIdentifier,
		});

		try {
			const res = await fetch(`/dashboard/api/appconfigdata/profiles?${params}`, { method: 'DELETE' });
			if (!res.ok && res.status !== 404) throw new Error(`status ${res.status}`);
			toast.success('Profile deleted');
			if (
				selectedProfile?.applicationIdentifier === p.applicationIdentifier &&
				selectedProfile?.environmentIdentifier === p.environmentIdentifier &&
				selectedProfile?.configurationProfileIdentifier === p.configurationProfileIdentifier
			) {
				selectedProfile = null;
			}
			await Promise.all([loadProfiles(), loadSessions()]);
		} catch (err: unknown) {
			toast.error(`Failed to delete profile: ${(err as Error).message}`);
		}
	}

	async function createProfile() {
		if (!newApp.trim() || !newEnv.trim() || !newProfileId.trim()) {
			toast.error('Application, environment, and profile ID are required');
			return;
		}

		creating = true;
		try {
			const res = await fetch('/dashboard/api/appconfigdata/profiles', {
				method: 'POST',
				headers: { 'Content-Type': 'application/json' },
				body: JSON.stringify({
					applicationIdentifier: newApp.trim(),
					environmentIdentifier: newEnv.trim(),
					configurationProfileIdentifier: newProfileId.trim(),
					content: newContent,
					contentType: newContentType,
				}),
			});

			if (!res.ok) {
				const err = await res.json() as { message?: string };
				throw new Error(err.message ?? `status ${res.status}`);
			}

			toast.success('Profile saved');
			showCreateModal = false;
			newApp = '';
			newEnv = '';
			newProfileId = '';
			newContent = '{}';
			newContentType = 'application/json';
			await loadProfiles();
		} catch (err: unknown) {
			toast.error(`Failed to save profile: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	// ─── Auto-refresh ────────────────────────────────────────────────────────────

	function startAutoRefresh() {
		countdown = autoRefreshSeconds;
		countdownInterval = setInterval(() => {
			countdown -= 1;
		}, 1000);
		refreshInterval = setInterval(() => {
			countdown = autoRefreshSeconds;
			void refreshAll();
		}, autoRefreshSeconds * 1000);
	}

	function stopAutoRefresh() {
		if (refreshInterval) clearInterval(refreshInterval);
		if (countdownInterval) clearInterval(countdownInterval);
		refreshInterval = null;
		countdownInterval = null;
	}

	function toggleAutoRefresh() {
		autoRefresh = !autoRefresh;
		if (autoRefresh) {
			startAutoRefresh();
		} else {
			stopAutoRefresh();
		}
	}

	// ─── Lifecycle ───────────────────────────────────────────────────────────────

	onMount(() => {
		void refreshAll();
	});

	onDestroy(() => {
		stopAutoRefresh();
	});
</script>

<div class="space-y-6">

	<!-- Header -->
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="flex items-center justify-between">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">AppConfig Data</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
					Active retrieval sessions, configuration profiles, and poll interval visualization
				</p>
			</div>
			<div class="flex items-center gap-2">
				<button
					type="button"
					onclick={toggleAutoRefresh}
					class="flex items-center gap-1.5 rounded-lg border px-3 py-1.5 text-sm font-medium transition-colors
						{autoRefresh
							? 'border-indigo-300 bg-indigo-50 text-indigo-700 dark:border-indigo-600 dark:bg-indigo-900/30 dark:text-indigo-300'
							: 'border-slate-200 text-slate-600 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-400 dark:hover:bg-slate-700'}"
				>
					<Activity class="h-4 w-4" />
					{autoRefresh ? `Auto (${countdown}s)` : 'Auto-refresh'}
				</button>
				<button
					type="button"
					onclick={() => void refreshAll()}
					disabled={loadingSessions || loadingProfiles}
					class="flex items-center gap-1.5 rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-50
						disabled:opacity-50 dark:border-slate-600 dark:text-slate-400 dark:hover:bg-slate-700"
				>
					<RefreshCw class="h-4 w-4 {(loadingSessions || loadingProfiles) ? 'animate-spin' : ''}" />
					Refresh
				</button>
			</div>
		</div>
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 gap-4 sm:grid-cols-4">
		<div class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
				<Key class="h-3.5 w-3.5" /> Active Sessions
			</div>
			<div class="mt-1 text-2xl font-bold text-slate-900 dark:text-white">
				{stats?.sessionCount ?? sessions.length}
			</div>
		</div>
		<div class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
				<Database class="h-3.5 w-3.5" /> Profiles
			</div>
			<div class="mt-1 text-2xl font-bold text-slate-900 dark:text-white">
				{stats?.profileCount ?? profiles.length}
			</div>
		</div>
		<div class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
				<Timer class="h-3.5 w-3.5" /> Session TTL
			</div>
			<div class="mt-1 text-2xl font-bold text-slate-900 dark:text-white">
				{stats?.sessionTtl ?? '24h'}
			</div>
		</div>
		<div class="rounded-xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="flex items-center gap-2 text-xs text-slate-500 dark:text-slate-400">
				<RefreshCw class="h-3.5 w-3.5" /> Janitor
			</div>
			<div class="mt-1 text-2xl font-bold text-slate-900 dark:text-white">
				{stats?.janitorPeriod ?? '1h'}
			</div>
			{#if stats?.lastSweepAt && stats.lastSweepAt !== '0001-01-01T00:00:00Z'}
				<div class="mt-0.5 text-xs text-slate-400 dark:text-slate-500">
					last: {timeAgo(stats.lastSweepAt)}
				</div>
			{/if}
		</div>
	</div>

	<!-- Tab bar -->
	<div class="flex gap-1 rounded-xl border border-slate-200 bg-slate-50 p-1 dark:border-slate-700 dark:bg-slate-800/50">
		<button
			type="button"
			onclick={() => { activeTab = 'sessions'; void loadSessions(); }}
			class="flex flex-1 items-center justify-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors
				{activeTab === 'sessions'
					? 'bg-white text-slate-900 shadow-sm dark:bg-slate-700 dark:text-white'
					: 'text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'}"
		>
			<Key class="h-4 w-4" />
			Sessions
			{#if sessions.length > 0}
				<span class="rounded-full bg-indigo-100 px-2 py-0.5 text-xs text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300">
					{sessions.length}
				</span>
			{/if}
		</button>
		<button
			type="button"
			onclick={() => { activeTab = 'profiles'; void loadProfiles(); }}
			class="flex flex-1 items-center justify-center gap-2 rounded-lg px-4 py-2 text-sm font-medium transition-colors
				{activeTab === 'profiles'
					? 'bg-white text-slate-900 shadow-sm dark:bg-slate-700 dark:text-white'
					: 'text-slate-600 hover:text-slate-900 dark:text-slate-400 dark:hover:text-white'}"
		>
			<Database class="h-4 w-4" />
			Profiles
			{#if profiles.length > 0}
				<span class="rounded-full bg-indigo-100 px-2 py-0.5 text-xs text-indigo-700 dark:bg-indigo-900/40 dark:text-indigo-300">
					{profiles.length}
				</span>
			{/if}
		</button>
	</div>

	<!-- ── Sessions tab ──────────────────────────────────────────────────────── -->
	{#if activeTab === 'sessions'}
		<div class="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="flex flex-col gap-3 border-b border-slate-200 px-6 py-4 dark:border-slate-700 sm:flex-row sm:items-center sm:justify-between">
				<div class="flex items-center gap-2">
					<Key class="h-5 w-5 text-indigo-500" />
					<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Active Sessions</h2>
				</div>
				<div class="relative w-full sm:w-64">
					<Search class="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
					<input
						type="text"
						bind:value={sessionSearch}
						placeholder="Filter by app, env, profile…"
						class="w-full rounded-lg border border-slate-200 bg-white py-1.5 pl-8 pr-3 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white dark:placeholder-slate-500"
					/>
					{#if sessionSearch}
						<button
							type="button"
							onclick={() => (sessionSearch = '')}
							class="absolute right-2.5 top-1/2 -translate-y-1/2 text-slate-400 hover:text-slate-600"
						>
							<X class="h-3.5 w-3.5" />
						</button>
					{/if}
				</div>
			</div>

			{#if loadingSessions}
				<div class="px-6 py-8 text-center text-sm text-slate-500 dark:text-slate-400">Loading sessions…</div>
			{:else if sessions.length === 0}
				<div class="px-6 py-8 text-center">
					<Key class="mx-auto mb-3 h-10 w-10 text-slate-300 dark:text-slate-600" />
					<p class="text-sm text-slate-500 dark:text-slate-400">No active sessions.</p>
					<p class="mt-1 text-xs text-slate-400 dark:text-slate-500">
						Sessions are created via <code class="rounded bg-slate-100 px-1 dark:bg-slate-700">StartConfigurationSession</code>
						and expire after {stats?.sessionTtl ?? '24h'} of inactivity.
					</p>
				</div>
			{:else if filteredSessions.length === 0}
				<div class="px-6 py-8 text-center text-sm text-slate-500 dark:text-slate-400">
					No sessions match "{sessionSearch}"
				</div>
			{:else}
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-slate-100 text-left dark:border-slate-700">
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">Token</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">Application / Env / Profile</th>
								<th class="px-6 py-3">
									<button
										type="button"
										onclick={() => toggleSort('createdAt')}
										class="font-medium text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
									>
										Created {sessionSortKey === 'createdAt' ? (sessionSortAsc ? '↑' : '↓') : ''}
									</button>
								</th>
								<th class="px-6 py-3">
									<button
										type="button"
										onclick={() => toggleSort('lastAccessedAt')}
										class="font-medium text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
									>
										Last Access {sessionSortKey === 'lastAccessedAt' ? (sessionSortAsc ? '↑' : '↓') : ''}
									</button>
								</th>
								<th class="px-6 py-3">
									<button
										type="button"
										onclick={() => toggleSort('pollCount')}
										class="font-medium text-slate-500 hover:text-slate-700 dark:text-slate-400 dark:hover:text-slate-200"
									>
										Polls {sessionSortKey === 'pollCount' ? (sessionSortAsc ? '↑' : '↓') : ''}
									</button>
								</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">
									<span class="flex items-center gap-1"><Gauge class="h-3.5 w-3.5" />Poll Interval</span>
								</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">
									<span class="flex items-center gap-1"><Clock class="h-3.5 w-3.5" />Idle</span>
								</th>
								<th class="px-6 py-3"></th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each filteredSessions as session (session.token)}
								{@const agePct = sessionAgePercent(session)}
								<tr class="transition-colors hover:bg-slate-50 dark:hover:bg-slate-700/50 {sessionStaleClass(session)}">
									<td class="px-6 py-3">
										<div class="flex items-center gap-1.5">
											{#if expandedToken === session.token}
												<code class="max-w-xs break-all rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-700 dark:text-slate-300">
													{session.token}
												</code>
												<button
													type="button"
													onclick={() => (expandedToken = null)}
													class="shrink-0 text-slate-400 hover:text-slate-600"
													title="Collapse"
												>
													<EyeOff class="h-3.5 w-3.5" />
												</button>
											{:else}
												<code class="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-700 dark:text-slate-300">
													{session.token.slice(0, 8)}…{session.token.slice(-4)}
												</code>
												<button
													type="button"
													onclick={() => (expandedToken = session.token)}
													class="shrink-0 text-slate-400 hover:text-slate-600"
													title="Reveal full token"
												>
													<Eye class="h-3.5 w-3.5" />
												</button>
											{/if}
											<button
												type="button"
												onclick={() => {
													copyToClipboard(session.token, () => {
														copiedToken = session.token;
														setTimeout(() => (copiedToken = null), 1500);
													});
												}}
												class="shrink-0 text-slate-400 hover:text-slate-600"
												title="Copy token"
											>
												{#if copiedToken === session.token}
													<Check class="h-3.5 w-3.5 text-green-500" />
												{:else}
													<Copy class="h-3.5 w-3.5" />
												{/if}
											</button>
										</div>
									</td>
									<td class="px-6 py-3">
										<div class="text-slate-900 dark:text-white">{session.applicationIdentifier}</div>
										<div class="text-xs text-slate-500 dark:text-slate-400">
											{session.environmentIdentifier} / {session.configurationProfileIdentifier}
										</div>
									</td>
									<td class="px-6 py-3 text-slate-500 dark:text-slate-400">
										<div>{formatTime(session.createdAt)}</div>
									</td>
									<td class="px-6 py-3 text-slate-500 dark:text-slate-400">
										<div>{formatTime(session.lastAccessedAt)}</div>
										<div class="text-xs">{timeAgo(session.lastAccessedAt)}</div>
									</td>
									<td class="px-6 py-3 text-center text-slate-700 dark:text-slate-300">
										{session.pollCount}
									</td>
									<td class="px-6 py-3">
										<div class="flex items-center gap-2">
											<div class="h-2 w-20 overflow-hidden rounded-full bg-slate-100 dark:bg-slate-700">
												<div
													class="h-full rounded-full transition-all {pollIntervalColor(session.pollIntervalInSeconds)}"
													style="width: {pollIntervalWidth(session.pollIntervalInSeconds)}%"
												></div>
											</div>
											<span class="text-xs text-slate-500 dark:text-slate-400">
												{pollIntervalLabel(session.pollIntervalInSeconds)}
											</span>
										</div>
									</td>
									<td class="px-6 py-3">
										{#if agePct > 0}
											<div class="flex items-center gap-1.5">
												<div class="h-1.5 w-16 overflow-hidden rounded-full bg-slate-100 dark:bg-slate-700">
													<div
														class="h-full rounded-full transition-all
															{agePct >= 90 ? 'bg-red-500' : agePct >= 75 ? 'bg-orange-500' : agePct >= 50 ? 'bg-yellow-500' : 'bg-green-500'}"
														style="width: {agePct}%"
													></div>
												</div>
												<span class="text-xs text-slate-500 dark:text-slate-400">{agePct}%</span>
											</div>
											{#if agePct >= 75}
												<div class="mt-0.5 flex items-center gap-1 text-xs text-orange-500">
													<AlertTriangle class="h-3 w-3" />
													near expiry
												</div>
											{/if}
										{/if}
									</td>
									<td class="px-6 py-3">
										<button
											type="button"
											onclick={() => terminateSession(session.token)}
											class="rounded p-1 text-slate-400 hover:bg-red-50 hover:text-red-600 dark:hover:bg-red-900/20"
											title="Terminate session"
										>
											<Trash2 class="h-4 w-4" />
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- ── Profiles tab ──────────────────────────────────────────────────────── -->
	{#if activeTab === 'profiles'}
		<div class="grid gap-6 lg:grid-cols-5">
			<!-- Profile list -->
			<div class="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800 lg:col-span-2">
				<div class="flex items-center justify-between border-b border-slate-200 px-6 py-4 dark:border-slate-700">
					<div class="flex items-center gap-2">
						<Database class="h-5 w-5 text-indigo-500" />
						<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Profiles</h2>
					</div>
					<button
						type="button"
						onclick={() => (showCreateModal = true)}
						class="flex items-center gap-1.5 rounded-lg bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700"
					>
						<Plus class="h-4 w-4" />
						Set Config
					</button>
				</div>

				<div class="border-b border-slate-200 px-4 py-2 dark:border-slate-700">
					<div class="relative">
						<Search class="pointer-events-none absolute left-2.5 top-1/2 h-4 w-4 -translate-y-1/2 text-slate-400" />
						<input
							type="text"
							bind:value={profileSearch}
							placeholder="Filter profiles…"
							class="w-full rounded-lg border border-slate-200 bg-white py-1.5 pl-8 pr-3 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white dark:placeholder-slate-500"
						/>
					</div>
				</div>

				{#if loadingProfiles}
					<div class="px-6 py-8 text-center text-sm text-slate-500 dark:text-slate-400">Loading…</div>
				{:else if profiles.length === 0}
					<div class="px-6 py-8 text-center">
						<Database class="mx-auto mb-3 h-10 w-10 text-slate-300 dark:text-slate-600" />
						<p class="text-sm text-slate-500 dark:text-slate-400">No profiles stored.</p>
						<button
							type="button"
							onclick={() => (showCreateModal = true)}
							class="mt-3 rounded-lg bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700"
						>
							Set configuration
						</button>
					</div>
				{:else if filteredProfiles.length === 0}
					<div class="px-6 py-6 text-center text-sm text-slate-500 dark:text-slate-400">
						No profiles match "{profileSearch}"
					</div>
				{:else}
					<div class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each filteredProfiles as profile}
							{@const Icon = contentTypeIcon(profile.contentType)}
							<div class="group flex items-center gap-2 px-4 py-0.5">
								<button
									type="button"
									onclick={() => { selectedProfile = profile; showHistory = false; }}
									class="flex flex-1 items-center justify-between py-3 text-left transition-colors
										{selectedProfile?.applicationIdentifier === profile.applicationIdentifier &&
										selectedProfile?.environmentIdentifier === profile.environmentIdentifier &&
										selectedProfile?.configurationProfileIdentifier === profile.configurationProfileIdentifier
											? 'text-indigo-700 dark:text-indigo-300'
											: 'hover:text-slate-900 dark:hover:text-white'}"
								>
									<div class="min-w-0">
										<div class="flex items-center gap-1.5">
											<Icon class="h-4 w-4 shrink-0 text-slate-400" />
											<span class="truncate font-medium text-slate-900 dark:text-white">
												{profile.applicationIdentifier}
											</span>
										</div>
										<div class="mt-0.5 truncate pl-5 text-xs text-slate-500 dark:text-slate-400">
											{profile.environmentIdentifier} / {profile.configurationProfileIdentifier}
										</div>
										{#if profile.updatedAt && profile.updatedAt !== '0001-01-01T00:00:00Z'}
											<div class="mt-0.5 pl-5 text-xs text-slate-400 dark:text-slate-500">
												{timeAgo(profile.updatedAt)}
												{#if profile.history?.length > 0}
													· {profile.history.length} revision{profile.history.length !== 1 ? 's' : ''}
												{/if}
											</div>
										{/if}
									</div>
									<ChevronRight class="ml-2 h-4 w-4 shrink-0 text-slate-400" />
								</button>
								<button
									type="button"
									onclick={() => deleteProfile(profile)}
									class="invisible rounded p-1 text-slate-400 hover:bg-red-50 hover:text-red-600 group-hover:visible dark:hover:bg-red-900/20"
									title="Delete profile"
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</div>
						{/each}
					</div>
				{/if}
			</div>

			<!-- Profile detail -->
			<div class="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800 lg:col-span-3">
				{#if !selectedProfile}
					<div class="flex h-full min-h-64 items-center justify-center px-6 py-20">
						<div class="text-center">
							<Database class="mx-auto mb-3 h-10 w-10 text-slate-300 dark:text-slate-600" />
							<p class="text-sm text-slate-500 dark:text-slate-400">Select a profile to preview its content</p>
						</div>
					</div>
				{:else}
					{@const Icon = contentTypeIcon(selectedProfile.contentType)}
					<div class="border-b border-slate-200 px-6 py-4 dark:border-slate-700">
						<div class="flex items-center gap-2">
							<Icon class="h-5 w-5 text-indigo-500" />
							<h3 class="font-semibold text-slate-900 dark:text-white">
								{selectedProfile.applicationIdentifier} / {selectedProfile.environmentIdentifier} / {selectedProfile.configurationProfileIdentifier}
							</h3>
						</div>
						<div class="mt-1 flex flex-wrap items-center gap-3 text-xs text-slate-500 dark:text-slate-400">
							{#if selectedProfile.contentType}
								<span class="rounded bg-slate-100 px-1.5 py-0.5 dark:bg-slate-700">{selectedProfile.contentType}</span>
							{/if}
							{#if selectedProfile.updatedAt && selectedProfile.updatedAt !== '0001-01-01T00:00:00Z'}
								<span>Updated {timeAgo(selectedProfile.updatedAt)} · {formatTime(selectedProfile.updatedAt)}</span>
							{/if}
							{#if selectedProfile.contentHash}
								<span class="font-mono">sha256: {selectedProfile.contentHash.slice(0, 12)}…</span>
							{/if}
						</div>
					</div>

					<!-- Content/history toggle -->
					<div class="flex items-center gap-2 border-b border-slate-200 px-6 py-2 dark:border-slate-700">
						<button
							type="button"
							onclick={() => (showHistory = false)}
							class="rounded-md px-3 py-1.5 text-sm font-medium transition-colors
								{!showHistory
									? 'bg-indigo-50 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300'
									: 'text-slate-600 hover:bg-slate-50 dark:text-slate-400 dark:hover:bg-slate-700'}"
						>
							Current
						</button>
						<button
							type="button"
							onclick={() => (showHistory = true)}
							class="flex items-center gap-1.5 rounded-md px-3 py-1.5 text-sm font-medium transition-colors
								{showHistory
									? 'bg-indigo-50 text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300'
									: 'text-slate-600 hover:bg-slate-50 dark:text-slate-400 dark:hover:bg-slate-700'}"
						>
							<History class="h-3.5 w-3.5" />
							History
							{#if selectedProfile.history?.length > 0}
								<span class="rounded-full bg-slate-200 px-1.5 py-0.5 text-xs dark:bg-slate-600">
									{selectedProfile.history.length}
								</span>
							{/if}
						</button>
					</div>

					<div class="p-6">
						{#if !showHistory}
							{#if selectedProfile.content}
								<div class="relative">
									<button
										type="button"
										onclick={() => {
											copyToClipboard(selectedProfile!.content, () => {
												copiedContent = true;
												setTimeout(() => (copiedContent = false), 1500);
											});
										}}
										class="absolute right-3 top-3 rounded bg-slate-800/50 p-1.5 text-slate-300 hover:bg-slate-800/80"
										title="Copy content"
									>
										{#if copiedContent}
											<Check class="h-4 w-4 text-green-400" />
										{:else}
											<Copy class="h-4 w-4" />
										{/if}
									</button>
									<pre class="max-h-96 overflow-auto rounded-lg bg-slate-950 p-4 text-xs leading-relaxed text-green-300 dark:bg-slate-900">{formatContent(selectedProfile.content, selectedProfile.contentType)}</pre>
								</div>
							{:else}
								<div class="rounded-lg border-2 border-dashed border-slate-200 p-6 text-center dark:border-slate-700">
									<p class="text-sm text-slate-500 dark:text-slate-400">No content stored for this profile.</p>
									<button
										type="button"
										onclick={() => {
											newApp = selectedProfile!.applicationIdentifier;
											newEnv = selectedProfile!.environmentIdentifier;
											newProfileId = selectedProfile!.configurationProfileIdentifier;
											showCreateModal = true;
										}}
										class="mt-2 text-sm text-indigo-600 hover:underline dark:text-indigo-400"
									>
										Set content
									</button>
								</div>
							{/if}
						{:else}
							{#if !selectedProfile.history || selectedProfile.history.length === 0}
								<p class="text-sm text-slate-500 dark:text-slate-400">
									No history yet. History is captured each time the configuration is updated.
								</p>
							{:else}
								<div class="space-y-3">
									{#each selectedProfile.history as version, i}
										{@const vIcon = contentTypeIcon(version.contentType)}
										<details class="group rounded-lg border border-slate-200 dark:border-slate-700">
											<summary class="flex cursor-pointer list-none items-center justify-between px-4 py-2.5">
												<div class="flex items-center gap-2">
													<vIcon class="h-4 w-4 text-slate-400" />
													<span class="text-sm font-medium text-slate-700 dark:text-slate-300">
														v{selectedProfile.history.length - i}
													</span>
													<span class="text-xs text-slate-500 dark:text-slate-400">
														{formatTime(version.updatedAt)} ({timeAgo(version.updatedAt)})
													</span>
												</div>
												<div class="flex items-center gap-2">
													{#if version.contentHash}
														<span class="font-mono text-xs text-slate-400">{version.contentHash.slice(0, 8)}…</span>
													{/if}
													<ChevronRight class="h-4 w-4 text-slate-400 transition-transform group-open:rotate-90" />
												</div>
											</summary>
											<div class="border-t border-slate-100 dark:border-slate-700">
												<pre class="max-h-64 overflow-auto p-4 text-xs leading-relaxed text-slate-700 dark:text-slate-300">{formatContent(version.content, version.contentType) || '(empty)'}</pre>
											</div>
										</details>
									{/each}
								</div>
							{/if}
						{/if}
					</div>
				{/if}
			</div>
		</div>
	{/if}
</div>

<!-- ── Create/Set configuration modal ────────────────────────────────────── -->
{#if showCreateModal}
	<div
		role="dialog"
		aria-modal="true"
		class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4"
		onkeydown={(e) => e.key === 'Escape' && (showCreateModal = false)}
	>
		<div class="w-full max-w-lg rounded-2xl bg-white shadow-xl dark:bg-slate-800">
			<div class="flex items-center justify-between border-b border-slate-200 px-6 py-4 dark:border-slate-700">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Set Configuration</h2>
				<button
					type="button"
					onclick={() => (showCreateModal = false)}
					class="rounded p-1 text-slate-400 hover:bg-slate-100 dark:hover:bg-slate-700"
				>
					<X class="h-5 w-5" />
				</button>
			</div>
			<div class="space-y-4 px-6 py-4">
				<div class="grid grid-cols-3 gap-3">
					<div>
						<label class="mb-1 block text-xs font-medium text-slate-700 dark:text-slate-300" for="new-app">Application</label>
						<input
							id="new-app"
							type="text"
							bind:value={newApp}
							placeholder="my-app"
							class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="mb-1 block text-xs font-medium text-slate-700 dark:text-slate-300" for="new-env">Environment</label>
						<input
							id="new-env"
							type="text"
							bind:value={newEnv}
							placeholder="prod"
							class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
						/>
					</div>
					<div>
						<label class="mb-1 block text-xs font-medium text-slate-700 dark:text-slate-300" for="new-profile-id">Profile ID</label>
						<input
							id="new-profile-id"
							type="text"
							bind:value={newProfileId}
							placeholder="my-profile"
							class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
						/>
					</div>
				</div>
				<div>
					<label class="mb-1 block text-xs font-medium text-slate-700 dark:text-slate-300" for="new-content-type">Content Type</label>
					<select
						id="new-content-type"
						bind:value={newContentType}
						class="w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
					>
						<option value="application/json">application/json</option>
						<option value="application/x-yaml">application/x-yaml</option>
						<option value="text/plain">text/plain</option>
						<option value="application/octet-stream">application/octet-stream</option>
					</select>
				</div>
				<div>
					<label class="mb-1 block text-xs font-medium text-slate-700 dark:text-slate-300" for="new-content">Content</label>
					<textarea
						id="new-content"
						bind:value={newContent}
						rows={8}
						placeholder="&#123;&quot;featureFlag&quot;: true&#125;"
						class="w-full rounded-lg border border-slate-300 px-3 py-2 font-mono text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
					></textarea>
				</div>
			</div>
			<div class="flex justify-end gap-3 border-t border-slate-200 px-6 py-4 dark:border-slate-700">
				<button
					type="button"
					onclick={() => (showCreateModal = false)}
					class="rounded-lg border border-slate-200 px-4 py-2 text-sm text-slate-600 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-400"
				>
					Cancel
				</button>
				<button
					type="button"
					onclick={createProfile}
					disabled={creating}
					class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
				>
					{creating ? 'Saving…' : 'Save Configuration'}
				</button>
			</div>
		</div>
	</div>
{/if}
