<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { RefreshCw, Clock, Key, Database, ChevronRight, History, Gauge } from 'lucide-svelte';

	type ConfigVersion = {
		content: string;
		contentType: string;
		updatedAt: string;
	};

	type Profile = {
		applicationIdentifier: string;
		environmentIdentifier: string;
		configurationProfileIdentifier: string;
		content: string;
		contentType: string;
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
	};

	let sessions = $state<Session[]>([]);
	let profiles = $state<Profile[]>([]);
	let loadingSessions = $state(false);
	let loadingProfiles = $state(false);
	let activeTab = $state<'sessions' | 'profiles'>('sessions');
	let selectedProfile = $state<Profile | null>(null);
	let showHistory = $state(false);

	const maxDisplayedPollInterval = 300;

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
		} catch (err: unknown) {
			toast.error(`Failed to load profiles: ${(err as Error).message}`);
		} finally {
			loadingProfiles = false;
		}
	}

	function formatTime(iso: string): string {
		if (!iso || iso === '0001-01-01T00:00:00Z') return '—';
		return new Date(iso).toLocaleString();
	}

	function tokenPreview(token: string): string {
		return token.length > 12 ? token.slice(0, 8) + '…' + token.slice(-4) : token;
	}

	function pollIntervalColor(seconds: number): string {
		if (seconds === 0) return 'bg-slate-300 dark:bg-slate-600';
		if (seconds <= 15) return 'bg-green-500';
		if (seconds <= 60) return 'bg-yellow-500';
		return 'bg-orange-500';
	}

	function pollIntervalWidth(seconds: number): number {
		if (seconds === 0) return 100;
		return Math.min(100, Math.round((seconds / maxDisplayedPollInterval) * 100));
	}

	function selectProfile(p: Profile) {
		selectedProfile = p;
		showHistory = false;
	}

	onMount(() => {
		void loadSessions();
		void loadProfiles();
	});
</script>

<div class="space-y-6">
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h1 class="text-3xl font-bold text-slate-900 dark:text-white">AppConfig Data</h1>
		<p class="mt-2 text-sm text-slate-600 dark:text-slate-300">
			Active retrieval sessions, configuration profiles, and poll interval visualization
		</p>
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

	<!-- Sessions tab -->
	{#if activeTab === 'sessions'}
		<div class="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="flex items-center justify-between border-b border-slate-200 px-6 py-4 dark:border-slate-700">
				<div class="flex items-center gap-2">
					<Key class="h-5 w-5 text-indigo-500" />
					<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Active Sessions</h2>
				</div>
				<button
					type="button"
					onclick={() => void loadSessions()}
					disabled={loadingSessions}
					class="flex items-center gap-1.5 rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-50
						disabled:opacity-50 dark:border-slate-600 dark:text-slate-400 dark:hover:bg-slate-700"
				>
					<RefreshCw class="h-4 w-4 {loadingSessions ? 'animate-spin' : ''}" />
					Refresh
				</button>
			</div>

			{#if loadingSessions}
				<div class="px-6 py-8 text-center text-sm text-slate-500 dark:text-slate-400">Loading sessions…</div>
			{:else if sessions.length === 0}
				<div class="px-6 py-8 text-center">
					<Key class="mx-auto mb-3 h-10 w-10 text-slate-300 dark:text-slate-600" />
					<p class="text-sm text-slate-500 dark:text-slate-400">No active sessions. Sessions expire after 24 hours of inactivity.</p>
				</div>
			{:else}
				<div class="overflow-x-auto">
					<table class="w-full text-sm">
						<thead>
							<tr class="border-b border-slate-100 text-left dark:border-slate-700">
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">Token</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">Application</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">Environment</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">Profile</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">
									<span class="flex items-center gap-1"><Clock class="h-3.5 w-3.5" />Created</span>
								</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">
									<span class="flex items-center gap-1"><Clock class="h-3.5 w-3.5" />Last Access</span>
								</th>
								<th class="px-6 py-3 font-medium text-slate-500 dark:text-slate-400">
									<span class="flex items-center gap-1"><Gauge class="h-3.5 w-3.5" />Poll Interval</span>
								</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each sessions as session (session.token)}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
									<td class="px-6 py-3">
										<code class="rounded bg-slate-100 px-1.5 py-0.5 font-mono text-xs text-slate-700 dark:bg-slate-700 dark:text-slate-300">
											{tokenPreview(session.token)}
										</code>
									</td>
									<td class="px-6 py-3 text-slate-700 dark:text-slate-300">{session.applicationIdentifier}</td>
									<td class="px-6 py-3 text-slate-700 dark:text-slate-300">{session.environmentIdentifier}</td>
									<td class="px-6 py-3 text-slate-700 dark:text-slate-300">{session.configurationProfileIdentifier}</td>
									<td class="px-6 py-3 text-slate-500 dark:text-slate-400">{formatTime(session.createdAt)}</td>
									<td class="px-6 py-3 text-slate-500 dark:text-slate-400">{formatTime(session.lastAccessedAt)}</td>
									<td class="px-6 py-3">
										<div class="flex items-center gap-2">
											<div class="h-2 w-24 overflow-hidden rounded-full bg-slate-100 dark:bg-slate-700">
												<div
													class="h-full rounded-full transition-all {pollIntervalColor(session.pollIntervalInSeconds)}"
													style="width: {pollIntervalWidth(session.pollIntervalInSeconds)}%"
												></div>
											</div>
											<span class="text-xs text-slate-500 dark:text-slate-400">
												{session.pollIntervalInSeconds === 0 ? 'default' : `${session.pollIntervalInSeconds}s`}
											</span>
										</div>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Profiles tab -->
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
						onclick={() => void loadProfiles()}
						disabled={loadingProfiles}
						class="flex items-center gap-1.5 rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-600 hover:bg-slate-50
							disabled:opacity-50 dark:border-slate-600 dark:text-slate-400 dark:hover:bg-slate-700"
					>
						<RefreshCw class="h-4 w-4 {loadingProfiles ? 'animate-spin' : ''}" />
						Refresh
					</button>
				</div>

				{#if loadingProfiles}
					<div class="px-6 py-8 text-center text-sm text-slate-500 dark:text-slate-400">Loading…</div>
				{:else if profiles.length === 0}
					<div class="px-6 py-8 text-center">
						<Database class="mx-auto mb-3 h-10 w-10 text-slate-300 dark:text-slate-600" />
						<p class="text-sm text-slate-500 dark:text-slate-400">No profiles stored.</p>
					</div>
				{:else}
					<div class="divide-y divide-slate-100 dark:divide-slate-700">
						{#each profiles as profile}
							<button
								type="button"
								onclick={() => selectProfile(profile)}
								class="flex w-full items-center justify-between px-6 py-4 text-left transition-colors hover:bg-slate-50 dark:hover:bg-slate-700/50
									{selectedProfile?.applicationIdentifier === profile.applicationIdentifier &&
									selectedProfile?.environmentIdentifier === profile.environmentIdentifier &&
									selectedProfile?.configurationProfileIdentifier === profile.configurationProfileIdentifier
										? 'bg-indigo-50 dark:bg-indigo-900/20'
										: ''}"
							>
								<div class="min-w-0">
									<div class="truncate font-medium text-slate-900 dark:text-white">
										{profile.applicationIdentifier}
									</div>
									<div class="mt-0.5 truncate text-xs text-slate-500 dark:text-slate-400">
										{profile.environmentIdentifier} / {profile.configurationProfileIdentifier}
									</div>
									{#if profile.contentType}
										<span class="mt-1 inline-block rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-600 dark:bg-slate-700 dark:text-slate-400">
											{profile.contentType}
										</span>
									{/if}
								</div>
								<ChevronRight class="ml-2 h-4 w-4 shrink-0 text-slate-400" />
							</button>
						{/each}
					</div>
				{/if}
			</div>

			<!-- Profile detail -->
			<div class="rounded-2xl border border-slate-200 bg-white shadow-sm dark:border-slate-700 dark:bg-slate-800 lg:col-span-3">
				{#if !selectedProfile}
					<div class="flex h-full items-center justify-center px-6 py-20">
						<div class="text-center">
							<Database class="mx-auto mb-3 h-10 w-10 text-slate-300 dark:text-slate-600" />
							<p class="text-sm text-slate-500 dark:text-slate-400">Select a profile to preview its content</p>
						</div>
					</div>
				{:else}
					<div class="border-b border-slate-200 px-6 py-4 dark:border-slate-700">
						<h3 class="font-semibold text-slate-900 dark:text-white">
							{selectedProfile.applicationIdentifier} / {selectedProfile.environmentIdentifier} / {selectedProfile.configurationProfileIdentifier}
						</h3>
						{#if selectedProfile.updatedAt && selectedProfile.updatedAt !== '0001-01-01T00:00:00Z'}
							<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">
								Last updated: {formatTime(selectedProfile.updatedAt)}
							</p>
						{/if}
					</div>

					<!-- Content preview / history toggle -->
					<div class="flex gap-2 border-b border-slate-200 px-6 py-2 dark:border-slate-700">
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
								<pre class="overflow-auto rounded-lg bg-slate-950 p-4 text-xs text-green-300 dark:bg-slate-900">{selectedProfile.content}</pre>
							{:else}
								<p class="text-sm text-slate-500 dark:text-slate-400">No content stored for this profile.</p>
							{/if}
						{:else}
							{#if !selectedProfile.history || selectedProfile.history.length === 0}
								<p class="text-sm text-slate-500 dark:text-slate-400">No history recorded yet. History is captured each time the configuration is updated.</p>
							{:else}
								<div class="space-y-4">
									{#each selectedProfile.history as version, i}
										<div class="rounded-lg border border-slate-200 dark:border-slate-700">
											<div class="flex items-center justify-between border-b border-slate-100 px-4 py-2 dark:border-slate-700">
												<span class="text-xs font-medium text-slate-500 dark:text-slate-400">
													v{selectedProfile.history.length - i} — {formatTime(version.updatedAt)}
												</span>
												<span class="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-600 dark:bg-slate-700 dark:text-slate-400">
													{version.contentType}
												</span>
											</div>
											<pre class="overflow-auto p-4 text-xs text-slate-700 dark:text-slate-300">{version.content || '(empty)'}</pre>
										</div>
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
