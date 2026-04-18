<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';

	type ActiveTab = 'global' | 'services' | 'documentation' | 'purge';
	type SettingsState = {
		accountID: string;
		region: string;
		latencyMs: number;
		autoPurgeTTL: string;
		enforceIAM: boolean;
		portRangeStart: number;
		portRangeEnd: number;
		initScriptTimeout: string;
		autoRefresh: boolean;
		refreshInterval: number;
		maxConsoleEntries: number;
	};

	const storageKey = 'gopherstack_settings';

	const defaults: SettingsState = {
		accountID: '000000000000',
		region: 'us-east-1',
		latencyMs: 0,
		autoPurgeTTL: '0',
		enforceIAM: false,
		portRangeStart: 5000,
		portRangeEnd: 10000,
		initScriptTimeout: '30s',
		autoRefresh: true,
		refreshInterval: 5,
		maxConsoleEntries: 200
	};

	let activeTab = $state<ActiveTab>('global');
	let settings = $state<SettingsState>({ ...defaults });
	let dirty = $state(false);
	let saveNotice = $state('');
	let purgeTypes = $state({
		settings: false,
		localStorage: false,
		sessionStorage: false
	});
	let resetServices = $state<string[]>([]);
	let selectedResetService = $state('all');
	let resettingService = $state(false);
	let version = $state('dev');

	function mergeSettings(raw: unknown): SettingsState {
		if (!raw || typeof raw !== 'object') {
			return { ...defaults };
		}

		const source = raw as Partial<SettingsState>;

		return {
			...defaults,
			...source,
			latencyMs: Number(source.latencyMs ?? defaults.latencyMs),
			portRangeStart: Number(source.portRangeStart ?? defaults.portRangeStart),
			portRangeEnd: Number(source.portRangeEnd ?? defaults.portRangeEnd),
			refreshInterval: Number(source.refreshInterval ?? defaults.refreshInterval),
			maxConsoleEntries: Number(source.maxConsoleEntries ?? defaults.maxConsoleEntries),
			enforceIAM: Boolean(source.enforceIAM ?? defaults.enforceIAM),
			autoRefresh: Boolean(source.autoRefresh ?? defaults.autoRefresh)
		};
	}

	onMount(() => {
		const raw = window.localStorage.getItem(storageKey);
		if (!raw) {
			settings = { ...defaults };
		} else {
			try {
				settings = mergeSettings(JSON.parse(raw));
			} catch {
				settings = { ...defaults };
			}
		}

		try {
			fetch('/_gopherstack/health')
				.then((res) => (res.ok ? res.json() : null))
				.then((data) => {
					if (!data) return;
					if (data.version) version = data.version;
					if (Array.isArray(data.services)) {
						resetServices = data.services
							.map((service: unknown) => String(service))
							.sort((a: string, b: string) => a.localeCompare(b));
					}
				})
				.catch(() => {
					resetServices = [];
				});
		} catch {
			resetServices = [];
		}
	});

	function saveSettings(): void {
		window.localStorage.setItem(storageKey, JSON.stringify(settings));
		dirty = false;
		saveNotice = 'updated';
		toast.success('Settings updated successfully');
	}

	function resetDefaults(): void {
		settings = { ...defaults };
		dirty = true;
	}

	function purgeData(): void {
		if (purgeTypes.settings) {
			window.localStorage.removeItem(storageKey);
			settings = { ...defaults };
			dirty = false;
		}

		if (purgeTypes.localStorage) {
			window.localStorage.clear();
		}

		if (purgeTypes.sessionStorage) {
			window.sessionStorage.clear();
		}

		purgeTypes.settings = false;
		purgeTypes.localStorage = false;
		purgeTypes.sessionStorage = false;

		toast.success('Data purged successfully');
	}

	async function resetServiceData(): Promise<void> {
		resettingService = true;
		try {
			const suffix = selectedResetService === 'all'
				? ''
				: `?service=${encodeURIComponent(selectedResetService)}`;
			const res = await fetch(`/_gopherstack/reset${suffix}`, { method: 'POST' });
			if (!res.ok) {
				throw new Error(`reset failed with status ${res.status}`);
			}
			const body = await res.json();
			toast.success(body.message || 'Service reset complete');
		} catch (err) {
			toast.error(`Failed to reset service: ${(err as Error).message}`);
		} finally {
			resettingService = false;
		}
	}
</script>

<div class="max-w-7xl mx-auto pb-20 space-y-6">
	<div class="flex items-center justify-between">
		<div>
			<div class="flex items-center gap-3">
				<h1 class="text-2xl font-bold text-slate-900 dark:text-white">Settings</h1>
				<span class="px-2 py-0.5 text-[10px] font-bold tracking-wider uppercase bg-slate-100 dark:bg-slate-800 text-slate-500 dark:text-slate-400 rounded-full border border-slate-200 dark:border-white/10">{version}</span>
			</div>
			<p class="text-sm text-slate-500 dark:text-slate-400 mt-1">Runtime configuration and persistence</p>
		</div>
		<div class="flex items-center gap-2">
			<button type="button" onclick={resetDefaults} class="px-4 py-2 rounded-lg text-sm border border-slate-300 dark:border-slate-600 hover:bg-slate-100 dark:hover:bg-slate-800">Reset Defaults</button>
			<button id="save-settings-btn" type="button" onclick={saveSettings} class="px-5 py-2 rounded-lg text-sm font-semibold bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50" disabled={!dirty}>Save Settings</button>
		</div>
	</div>

	{#if saveNotice}
		<div id="toast-container" class="rounded border border-emerald-300 bg-emerald-50 px-3 py-2 text-sm text-emerald-800">Settings {saveNotice}</div>
	{/if}

	<div class="border-b border-slate-200 dark:border-slate-700">
		<div class="flex gap-2">
			<button type="button" onclick={() => activeTab = 'global'} class={`px-4 py-3 text-sm font-medium border-b-2 ${activeTab === 'global' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}`}>Global Settings</button>
			<button type="button" onclick={() => activeTab = 'services'} class={`px-4 py-3 text-sm font-medium border-b-2 ${activeTab === 'services' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}`}>Service Settings</button>
			<button type="button" onclick={() => activeTab = 'documentation'} class={`px-4 py-3 text-sm font-medium border-b-2 ${activeTab === 'documentation' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}`}>Documentation</button>
			<button type="button" onclick={() => activeTab = 'purge'} class={`px-4 py-3 text-sm font-medium border-b-2 ${activeTab === 'purge' ? 'border-indigo-500 text-indigo-600 dark:text-indigo-400' : 'border-transparent text-slate-500 hover:text-slate-700 dark:hover:text-slate-300'}`}>Purge Data</button>
		</div>
	</div>

	{#if activeTab === 'global'}
		<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<h2 class="text-lg font-semibold">Core & Network</h2>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="accountID" class="block text-xs font-medium uppercase tracking-wide text-slate-500 mb-1">Account ID</label>
						<input id="accountID" type="text" bind:value={settings.accountID} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
					</div>
					<div>
						<label for="region" class="block text-xs font-medium uppercase tracking-wide text-slate-500 mb-1">Default Region</label>
						<input id="region" type="text" bind:value={settings.region} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
					</div>
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="portRangeStart" class="block text-xs font-medium uppercase tracking-wide text-slate-500 mb-1">Port Range Start</label>
						<input id="portRangeStart" type="number" bind:value={settings.portRangeStart} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
					</div>
					<div>
						<label for="portRangeEnd" class="block text-xs font-medium uppercase tracking-wide text-slate-500 mb-1">Port Range End</label>
						<input id="portRangeEnd" type="number" bind:value={settings.portRangeEnd} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
					</div>
				</div>
			</div>

			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<h2 class="text-lg font-semibold">Performance & Lifecycle</h2>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="latencyMs" class="block text-xs font-medium uppercase tracking-wide text-slate-500 mb-1">Latency (ms)</label>
						<input id="latencyMs" type="number" min="0" bind:value={settings.latencyMs} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
					</div>
					<div>
						<label for="autoPurgeTTL" class="block text-xs font-medium uppercase tracking-wide text-slate-500 mb-1">Auto-Purge TTL</label>
						<input id="autoPurgeTTL" name="autoPurgeTTL" type="text" bind:value={settings.autoPurgeTTL} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
					</div>
				</div>
				<div>
					<label for="initScriptTimeout" class="block text-xs font-medium uppercase tracking-wide text-slate-500 mb-1">Init Script Timeout</label>
					<input id="initScriptTimeout" type="text" bind:value={settings.initScriptTimeout} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
				</div>
				<div class="flex items-center justify-between rounded-lg border border-slate-200 dark:border-slate-700 p-3">
					<div>
						<div class="text-sm font-medium">IAM Execution</div>
						<div class="text-xs text-slate-500">Enforce IAM policy simulation for requests</div>
					</div>
					<input id="enforceIAM" aria-label="IAM Execution" type="checkbox" bind:checked={settings.enforceIAM} onchange={() => dirty = true} class="h-4 w-4" />
				</div>
			</div>
		</div>
	{:else if activeTab === 'services'}
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-5">
			<h2 class="text-lg font-semibold">Dashboard Service Settings</h2>
			<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
				<div class="rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-2">
					<label for="autoRefresh" class="text-sm font-medium">Auto-refresh Service Tables</label>
					<div class="flex items-center justify-between">
						<span class="text-xs text-slate-500">Refresh service lists automatically</span>
						<input id="autoRefresh" type="checkbox" bind:checked={settings.autoRefresh} onchange={() => dirty = true} class="h-4 w-4" />
					</div>
				</div>
				<div class="rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-2">
					<label for="refreshInterval" class="text-sm font-medium">Refresh Interval (seconds)</label>
					<input id="refreshInterval" type="number" min="1" bind:value={settings.refreshInterval} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
				</div>
				<div class="rounded-lg border border-slate-200 dark:border-slate-700 p-4 space-y-2">
					<label for="maxConsoleEntries" class="text-sm font-medium">Max Console Entries Limit</label>
					<input id="maxConsoleEntries" type="number" min="10" bind:value={settings.maxConsoleEntries} oninput={() => dirty = true} class="w-full px-3 py-2 text-sm rounded-md border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900" />
				</div>
			</div>
		</div>
	{:else if activeTab === 'documentation'}
		<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<h2 class="text-lg font-semibold">Core & Network Settings</h2>
				<div class="space-y-3">
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Account ID</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">The AWS account ID used for API calls and resource tagging. Default: 000000000000</p>
					</div>
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Default Region</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">The default AWS region for all service operations. Examples: us-east-1, eu-west-1, ap-southeast-1</p>
					</div>
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Port Range (Start/End)</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">Network port range for local services. Default: 5000-10000. Adjust if ports are already in use.</p>
					</div>
				</div>
			</div>

			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<h2 class="text-lg font-semibold">Performance & Lifecycle</h2>
				<div class="space-y-3">
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Latency (ms)</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">Add artificial latency to API calls for testing. Set to 0 for no latency. Useful for testing timeout scenarios.</p>
					</div>
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Auto-Purge TTL</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">Time-to-live for automatic data cleanup. Set to 0 to disable auto-purge. Examples: 1h, 30m, 24h</p>
					</div>
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Init Script Timeout</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">Maximum time allowed for initialization scripts to execute. Examples: 30s, 1m, 5m</p>
					</div>
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">IAM Execution</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">When enabled, enforces IAM policy simulation for all requests. Useful for testing access control.</p>
					</div>
				</div>
			</div>

			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<h2 class="text-lg font-semibold">Dashboard Service Settings</h2>
				<div class="space-y-3">
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Auto-refresh Service Tables</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">When enabled, service lists will automatically refresh at the specified interval. Disable if updates cause performance issues.</p>
					</div>
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Refresh Interval (seconds)</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">How often service tables update when auto-refresh is enabled. Minimum: 1 second. Default: 5 seconds</p>
					</div>
					<div>
						<h3 class="text-sm font-medium text-indigo-600 dark:text-indigo-400">Max Console Entries Limit</h3>
						<p class="text-sm text-slate-600 dark:text-slate-400">Maximum number of API call entries to keep in the console. Older entries are discarded. Default: 200 entries</p>
					</div>
				</div>
			</div>

			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-4">
				<h2 class="text-lg font-semibold">Getting Started</h2>
				<div class="space-y-3 text-sm text-slate-600 dark:text-slate-400">
					<p>Gopherstack emulates AWS services locally for testing and development. Use these settings to configure:</p>
					<ul class="list-disc list-inside space-y-1">
						<li>AWS account and region context for your testing environment</li>
						<li>Network configuration for local service access</li>
						<li>Performance characteristics (latency simulation)</li>
						<li>Dashboard UI behavior and refresh rates</li>
					</ul>
					<p class="pt-2 font-medium text-slate-700 dark:text-slate-300">All settings are persisted in your browser's localStorage and apply immediately.</p>
				</div>
			</div>
		</div>
	{:else if activeTab === 'purge'}
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-6">
			<div>
				<h2 class="text-lg font-semibold mb-2">Purge Data</h2>
				<p class="text-sm text-slate-600 dark:text-slate-400">Permanently delete selected data. This action cannot be undone.</p>
			</div>

			<div class="space-y-3 border-t border-slate-200 dark:border-slate-700 pt-6">
				<div class="flex items-start gap-3 rounded-lg border border-slate-200 dark:border-slate-700 p-4 hover:bg-red-50 dark:hover:bg-red-950/20 transition-colors">
					<input id="purge-settings" type="checkbox" bind:checked={purgeTypes.settings} class="h-4 w-4 mt-0.5" />
					<div class="flex-1">
						<label for="purge-settings" class="text-sm font-medium cursor-pointer">Gopherstack Settings</label>
						<p class="text-xs text-slate-600 dark:text-slate-400">Clear all settings and reset to defaults (Account ID, Region, Port Range, Performance settings, etc.)</p>
					</div>
				</div>

				<div class="flex items-start gap-3 rounded-lg border border-slate-200 dark:border-slate-700 p-4 hover:bg-red-50 dark:hover:bg-red-950/20 transition-colors">
					<input id="purge-local" type="checkbox" bind:checked={purgeTypes.localStorage} class="h-4 w-4 mt-0.5" />
					<div class="flex-1">
						<label for="purge-local" class="text-sm font-medium cursor-pointer">All Browser Storage (localStorage)</label>
						<p class="text-xs text-slate-600 dark:text-slate-400">Permanently delete all data stored in browser storage. This is not easily recoverable.</p>
					</div>
				</div>

				<div class="flex items-start gap-3 rounded-lg border border-slate-200 dark:border-slate-700 p-4 hover:bg-red-50 dark:hover:bg-red-950/20 transition-colors">
					<input id="purge-session" type="checkbox" bind:checked={purgeTypes.sessionStorage} class="h-4 w-4 mt-0.5" />
					<div class="flex-1">
						<label for="purge-session" class="text-sm font-medium cursor-pointer">Session Storage</label>
						<p class="text-xs text-slate-600 dark:text-slate-400">Clear temporary session data. This is cleared when you close the browser anyway.</p>
					</div>
				</div>
			</div>

			<div class="space-y-3 border-t border-slate-200 dark:border-slate-700 pt-6">
				<h3 class="text-sm font-semibold">Reset Service State</h3>
				<p class="text-xs text-slate-600 dark:text-slate-400">Reset all resources for a single service using the backend reset endpoint.</p>
				<div class="flex items-center gap-3">
					<select bind:value={selectedResetService} class="px-3 py-2 rounded-lg border border-slate-300 dark:border-slate-600 bg-white dark:bg-slate-900 text-sm">
						<option value="all">All services</option>
						{#each resetServices as service}
							<option value={service}>{service}</option>
						{/each}
					</select>
					<button type="button" onclick={resetServiceData} disabled={resettingService} class="px-4 py-2 rounded-lg text-sm font-medium bg-amber-600 text-white hover:bg-amber-700 disabled:opacity-50 disabled:cursor-not-allowed">
						{resettingService ? 'Resetting...' : 'Reset Selected Service'}
					</button>
				</div>
			</div>

			<div class="flex items-center gap-3 pt-4 border-t border-slate-200 dark:border-slate-700">
				<button type="button" onclick={purgeData} disabled={!purgeTypes.settings && !purgeTypes.localStorage && !purgeTypes.sessionStorage} class="px-4 py-2 rounded-lg text-sm font-medium bg-red-600 text-white hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed">Purge Selected Data</button>
				<p class="text-xs text-slate-500">Select at least one option to purge</p>
			</div>
		</div>
	{/if}
</div>
