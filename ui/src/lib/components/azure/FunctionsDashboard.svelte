<script lang="ts">
	import { onMount } from 'svelte';
	import { Zap, Eye, EyeOff, RefreshCw, Trash2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt, copyText, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		appAction,
		deleteFunctionApp,
		listAppFunctions,
		listDeployments,
		listFunctionApps,
		listSettings,
		revealSetting,
		type AppAction,
		type AppFunction,
		type AppSetting,
		type Deployment,
		type FunctionApp
	} from '#lib/azurefunctions.ts';

	const TABS = [
		{ id: 'settings', label: 'App settings' },
		{ id: 'functions', label: 'Functions' },
		{ id: 'deployments', label: 'Deployments' }
	];

	let apps = $state<FunctionApp[]>([]);
	let loading = $state(true);
	let error = $state('');
	let selected = $state('');
	let tab = $state('settings');
	let settings = $state<AppSetting[]>([]);
	let revealed = $state<Record<string, string>>({});
	let functions = $state<AppFunction[]>([]);
	let deployments = $state<Deployment[]>([]);
	let childLoading = $state(false);
	let childError = $state('');

	async function load() {
		try {
			apps = await listFunctionApps();
			error = '';
			if (!apps.some((a) => a.name === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChildren() {
		if (!selected) return;
		childLoading = true;
		childError = '';
		revealed = {};
		try {
			[settings, functions, deployments] = await Promise.all([listSettings(selected), listAppFunctions(selected), listDeployments(selected)]);
		} catch (e) {
			childError = describeGcpError(e);
			settings = [];
			functions = [];
			deployments = [];
		} finally {
			childLoading = false;
		}
	}

	function open(name: string) {
		selected = selected === name ? '' : name;
		if (selected) void loadChildren();
	}

	async function act(app: FunctionApp, action: AppAction) {
		if (await attempt(() => appAction(app.name, action), `${action[0].toUpperCase()}${action.slice(1)} requested for ${app.name}`, `Failed to ${action} ${app.name}`)) await load();
	}

	async function remove(app: FunctionApp) {
		if (!(await confirmDestructive({ title: 'Delete function app', message: `Delete function app "${app.name}" with its settings and deployments?` }))) return;
		if (await attempt(() => deleteFunctionApp(app), `Deleted ${app.name}`, `Failed to delete ${app.name}`)) {
			await load();
		}
	}

	async function toggle(name: string) {
		if (name in revealed) {
			const { [name]: _drop, ...rest } = revealed;
			revealed = rest;
			return;
		}
		try {
			revealed = { ...revealed, [name]: await revealSetting(selected, name) };
		} catch (e) {
			childError = describeGcpError(e);
		}
	}

	const cell = 'px-4 py-3';
	const pill = (state: string) =>
		state === 'Running' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="functions-dashboard">
	<PageHeader icon={Zap} title="Azure Functions" description="Azure Functions: function apps, app settings, functions and deployments" color="teal" onRefresh={load} />

	{#if error && apps.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="functions-error">
			<p class="font-medium">Could not load function apps</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if apps.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="functions-empty">No function apps. Create one through Resource Manager (Microsoft.Web/sites with kind functionapp).</p>
	{:else}
		<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto" data-testid="functions-apps">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
					<tr>
						<th class={cell}>App</th><th class={cell}>State</th><th class={cell}>Plan</th><th class={cell}>Runtime</th><th class={cell}>Host name</th><th class="{cell} text-right">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each apps as app (app.id)}
						<tr class={selected === app.name ? 'bg-teal-50/60 dark:bg-teal-950/20' : ''} data-testid="functions-app">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => open(app.name)} data-testid="functions-select">{app.name}</button></td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(app.state)}" data-testid="functions-state">{app.state}</span></td>
							<td class="{cell} text-xs">{app.plan || '—'}{app.planSku ? ` (${app.planSku})` : ''}</td>
							<td class="{cell} text-xs">{app.runtimeStack || '—'}</td>
							<td class="{cell} font-mono text-xs break-all">{app.hostName || '—'}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								{#if app.state === 'Running'}
									<button class={GCP_MINI} onclick={() => act(app, 'stop')} data-testid="functions-stop">Stop</button>
								{:else}
									<button class={GCP_MINI} onclick={() => act(app, 'start')} data-testid="functions-start">Start</button>
								{/if}
								<button class={GCP_MINI} onclick={() => act(app, 'restart')} data-testid="functions-restart">Restart</button>
								<button class="{GCP_MINI} text-red-600" onclick={() => remove(app)} aria-label="Delete {app.name}" data-testid="functions-delete"><Trash2 class="inline h-3.5 w-3.5" /></button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if selected}
			<div class="space-y-3" data-testid="functions-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selected}</h2>
				<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="teal" />
				{#if childError}
					<p class="text-sm text-red-600" role="alert" data-testid="functions-detail-error">{childError}</p>
				{/if}
				{#if childLoading}
					<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
				{:else if tab === 'settings'}
					{#if settings.length === 0}
						<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="functions-settings-empty">No application settings</p>
					{:else}
						<table class="w-full text-sm" data-testid="functions-settings">
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each settings as s (s.name)}
									<tr data-testid="functions-setting">
										<td class="{cell} font-mono text-xs">{s.name}</td>
										<td class="{cell} font-mono text-xs break-all" data-testid="functions-setting-value">{s.name in revealed ? revealed[s.name] : s.value}</td>
										<td class="{cell} text-right whitespace-nowrap space-x-1">
											<button class={GCP_MINI} onclick={() => toggle(s.name)} aria-label="{s.name in revealed ? 'Hide' : 'Reveal'} {s.name}" data-testid="functions-reveal">
												{#if s.name in revealed}<EyeOff class="inline h-3.5 w-3.5" />{:else}<Eye class="inline h-3.5 w-3.5" />{/if}
											</button>
											{#if s.name in revealed}
												<button class={GCP_MINI} onclick={() => copyText(revealed[s.name])}>Copy</button>
											{/if}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
					{/if}
				{:else if tab === 'functions'}
					{#if functions.length === 0}
						<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="functions-fn-empty">No functions. Deploy a package to this app.</p>
					{:else}
						<table class="w-full text-sm" data-testid="functions-fns">
							<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Function</th><th class={cell}>Trigger</th><th class={cell}>Auth level</th><th class={cell}>Invoke URL</th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each functions as f (f.name)}
									<tr data-testid="functions-fn">
										<td class="{cell} font-mono text-xs">{f.name}{f.disabled ? ' (disabled)' : ''}</td>
										<td class="{cell} text-xs">{f.triggerType || '—'}</td>
										<td class="{cell} text-xs">{f.authLevel || '—'}</td>
										<td class="{cell} font-mono text-xs break-all">{f.invokeUrl || '—'}</td>
									</tr>
								{/each}
							</tbody>
						</table>
					{/if}
				{:else if deployments.length === 0}
					<p class="py-6 text-center text-gray-500 dark:text-gray-400" data-testid="functions-dep-empty">No deployments</p>
				{:else}
					<table class="w-full text-sm" data-testid="functions-deps">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Deployment</th><th class={cell}>Status</th><th class={cell}>Deployer</th><th class={cell}>Message</th><th class={cell}>Finished</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each deployments as d (d.id)}
								<tr data-testid="functions-dep">
									<td class="{cell} font-mono text-xs">{d.id}</td>
									<td class="{cell} text-xs">{d.succeeded ? 'Succeeded' : 'Failed'}</td>
									<td class="{cell} text-xs">{d.deployer || '—'}</td>
									<td class="{cell} text-xs">{d.message || '—'}</td>
									<td class="{cell} text-xs">{d.finished}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				{/if}
			</div>
		{/if}
	{/if}
</div>
