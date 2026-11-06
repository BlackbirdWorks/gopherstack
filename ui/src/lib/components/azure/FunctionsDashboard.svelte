<script lang="ts">
	import { onMount } from 'svelte';
	import { Zap, Eye, EyeOff, RefreshCw, Trash2 } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt, copyText, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		appAction,
		deleteFunctionApp,
		describePlan,
		invokeAppFunction,
		listHostKeys,
		revealHostKey,
		listAppFunctions,
		listDeployments,
		listFunctionApps,
		listSettings,
		revealSetting,
		type AppAction,
		type AppFunction,
		type AppSetting,
		type Deployment,
		type HostKey,
		type InvokeOutput,
		type FunctionApp
	} from '#lib/azurefunctions.ts';

	const TABS = [
		{ id: 'settings', label: 'App settings' },
		{ id: 'functions', label: 'Functions' },
		{ id: 'keys', label: 'Host keys' },
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
	let keys = $state<HostKey[]>([]);
	let revealedKeys = $state<Record<string, string>>({});
	let invokeFn = $state('');
	let invokeMethod = $state('GET');
	let invokeQuery = $state('');
	let invokeBody = $state('');
	let invokeType = $state('application/json');
	let invoking = $state(false);
	let invokeError = $state('');
	let invokeResult = $state<InvokeOutput | null>(null);
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
		revealedKeys = {};
		invokeFn = '';
		invokeResult = null;
		invokeError = '';
		try {
			[settings, functions, deployments, keys] = await Promise.all([listSettings(selected), listAppFunctions(selected), listDeployments(selected), listHostKeys(selected)]);
		} catch (e) {
			childError = describeGcpError(e);
			settings = [];
			functions = [];
			deployments = [];
			keys = [];
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

	const keyId = (k: HostKey) => `${k.kind}/${k.name}`;

	async function toggleKey(k: HostKey) {
		const id = keyId(k);
		if (id in revealedKeys) {
			const { [id]: _drop, ...rest } = revealedKeys;
			revealedKeys = rest;
			return;
		}
		try {
			revealedKeys = { ...revealedKeys, [id]: await revealHostKey(selected, k) };
		} catch (e) {
			childError = describeGcpError(e);
		}
	}

	async function runInvoke() {
		invoking = true;
		invokeError = '';
		try {
			invokeResult = await invokeAppFunction(selected, { function: invokeFn, method: invokeMethod, query: invokeQuery, body: invokeBody, contentType: invokeType });
		} catch (e) {
			invokeResult = null;
			invokeError = describeGcpError(e);
		} finally {
			invoking = false;
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
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(app.state)}" data-testid="functions-state">{app.state}</span>
								{#if app.failure}<p class="mt-1 max-w-xs text-xs text-amber-700 dark:text-amber-400" data-testid="functions-host-failure">Host not running: {app.failure}</p>{/if}</td>
							<td class="{cell} text-xs">{describePlan(app.plan, app.planSku) || '—'}</td>
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
							<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Function</th><th class={cell}>Trigger</th><th class={cell}>Auth level</th><th class={cell}>Invoke URL</th><th class={cell}></th></tr></thead>
							<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
								{#each functions as f (f.name)}
									<tr data-testid="functions-fn">
										<td class="{cell} font-mono text-xs">{f.name}{f.disabled ? ' (disabled)' : ''}</td>
										<td class="{cell} text-xs">{f.triggerType || '—'}</td>
										<td class="{cell} text-xs">{f.authLevel || '—'}</td>
										<td class="{cell} font-mono text-xs break-all">{f.invokeUrl || '—'}</td>
										<td class="{cell} text-right">
											{#if f.invokeUrl}<button class={GCP_MINI} onclick={() => { invokeFn = f.name; invokeResult = null; invokeError = ''; }} data-testid="functions-invoke-open">Invoke</button>{/if}
										</td>
									</tr>
								{/each}
							</tbody>
						</table>
						{#if invokeFn}
							<div class="space-y-2 rounded-md border border-gray-200 p-3 dark:border-gray-700" data-testid="functions-invoke">
								<h4 class="font-semibold text-gray-700 dark:text-gray-300">Invoke {invokeFn}</h4>
								<div class="flex flex-wrap gap-2">
									<select class="{GCP_INPUT} w-28" bind:value={invokeMethod} aria-label="Method" data-testid="functions-invoke-method">
										{#each ['GET', 'POST', 'PUT', 'PATCH', 'DELETE'] as m (m)}<option>{m}</option>{/each}
									</select>
									<input class="{GCP_INPUT} flex-1 font-mono" bind:value={invokeQuery} placeholder="Query string, e.g. name=gopher" aria-label="Query string" data-testid="functions-invoke-query" />
									<input class="{GCP_INPUT} w-48 font-mono" bind:value={invokeType} aria-label="Content type" />
									<button class={GCP_PRIMARY} disabled={invoking} onclick={runInvoke} data-testid="functions-invoke-run">{invoking ? 'Calling…' : 'Invoke'}</button>
								</div>
								<textarea class="{GCP_INPUT} h-20 w-full font-mono text-xs" bind:value={invokeBody} placeholder="Request body" aria-label="Request body" data-testid="functions-invoke-body"></textarea>
								{#if invokeError}<p class="text-red-600 dark:text-red-400" role="alert" data-testid="functions-invoke-error">{invokeError}</p>{/if}
								{#if invokeResult}
									<div data-testid="functions-invoke-result">
										<p class="font-mono font-semibold" data-testid="functions-invoke-status">{invokeResult.status}</p>
										<pre class="mt-1 max-h-60 overflow-auto whitespace-pre-wrap break-all font-mono text-xs" data-testid="functions-invoke-response">{invokeResult.body}</pre>
									</div>
								{/if}
							</div>
						{/if}
					{/if}
				{:else if tab === 'keys'}
					<table class="w-full text-sm" data-testid="functions-keys">
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each keys as k (keyId(k))}
								<tr data-testid="functions-key">
									<td class="{cell} text-xs">{k.kind === 'masterKey' ? 'Master' : k.kind === 'systemKeys' ? 'System' : 'Host'}</td>
									<td class="{cell} font-mono text-xs">{k.name}</td>
									<td class="{cell} font-mono text-xs break-all" data-testid="functions-key-value">{keyId(k) in revealedKeys ? revealedKeys[keyId(k)] : k.value}</td>
									<td class="{cell} text-right whitespace-nowrap space-x-1">
										<button class={GCP_MINI} onclick={() => toggleKey(k)} aria-label="{keyId(k) in revealedKeys ? 'Hide' : 'Reveal'} key {k.name}" data-testid="functions-key-reveal">
											{#if keyId(k) in revealedKeys}<EyeOff class="inline h-3.5 w-3.5" />{:else}<Eye class="inline h-3.5 w-3.5" />{/if}
										</button>
										{#if keyId(k) in revealedKeys}<button class={GCP_MINI} onclick={() => copyText(revealedKeys[keyId(k)])}>Copy</button>{/if}
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
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
