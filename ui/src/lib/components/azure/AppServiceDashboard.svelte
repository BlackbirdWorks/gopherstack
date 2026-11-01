<script lang="ts">
	import { onMount } from 'svelte';
	import { Globe, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import { getSubscriptionId } from '#lib/azurearm.ts';
	import {
		PRODUCTION_SLOT,
		getStickySettings,
		listSlots,
		listWebApps,
		siteAction,
		siteUrl,
		stickyCount,
		swapSlots,
		swapTargets,
		type StickySettings,
		type SiteAction,
		type SwapMode,
		type WebSite
	} from '#lib/azureappservice.ts';
	import { attempt, confirmedAttempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let subscription = $state('');
	let apps = $state<WebSite[]>([]);
	let loading = $state(true);
	let error = $state('');
	let filter = $state('');
	let selected = $state('');
	let slots = $state<WebSite[]>([]);
	let sticky = $state<StickySettings>({ appSettings: [], connectionStrings: [], azureStorage: [] });
	let childLoading = $state(false);
	let childError = $state('');
	let source = $state(PRODUCTION_SLOT);
	let target = $state('');

	const current = $derived(apps.find((a) => a.id === selected));
	const rgs = $derived([...new Set(apps.map((a) => a.resourceGroup))]);
	const shown = $derived(apps.filter((a) => !filter || a.resourceGroup === filter));
	const sources = $derived([PRODUCTION_SLOT, ...slots.map((s) => s.slot)]);
	const targets = $derived(swapTargets(slots, source));
	const previewing = $derived(slots.filter((s) => s.previewTarget));

	async function load() {
		try {
			if (!subscription) subscription = await getSubscriptionId();
			apps = await listWebApps(subscription);
			error = '';
			if (!apps.some((a) => a.id === selected)) selected = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
		if (selected) await loadChild();
	}

	async function loadChild() {
		const app = current;
		if (!app) return;
		childLoading = true;
		childError = '';
		try {
			[slots, sticky] = await Promise.all([listSlots(subscription, app), getStickySettings(subscription, app)]);
			if (!sources.includes(source)) source = PRODUCTION_SLOT;
			if (!targets.includes(target)) target = targets[0] ?? '';
		} catch (e) {
			childError = describeGcpError(e);
		} finally {
			childLoading = false;
		}
	}

	function select(app: WebSite) {
		selected = selected === app.id ? '' : app.id;
		slots = [];
		source = PRODUCTION_SLOT;
		target = '';
		if (selected) void loadChild();
	}

	async function act(site: WebSite, action: SiteAction) {
		const label = site.slot ? `${site.app}/${site.slot}` : site.app;
		const verb = `${action[0].toUpperCase()}${action.slice(1)}`;
		const run = () => siteAction(subscription, site, action);
		const ok =
			action === 'stop'
				? await confirmedAttempt({ title: 'Stop web app', message: `Stop "${label}"?`, confirmLabel: 'Stop' }, run, `${verb} requested for ${label}`, `Failed to ${action} ${label}`)
				: await attempt(run, `${verb} requested for ${label}`, `Failed to ${action} ${label}`);
		if (ok) await load();
	}

	async function swap(mode: SwapMode, from = source, to = target) {
		const app = current;
		if (!app || (mode !== 'cancel' && !to)) return;
		const titles: Record<SwapMode, string> = { swap: 'Swapped', preview: 'Swap preview started', complete: 'Swap completed', cancel: 'Swap cancelled' };
		const run = () => swapSlots(subscription, app, from, to, mode);
		const ok =
			mode === 'swap' || mode === 'complete'
				? await confirmedAttempt({ title: 'Swap slots', message: `Swap "${from}" with "${to}" on ${app.app}?`, confirmLabel: 'Swap' }, run, `${titles[mode]}: ${from} ↔ ${to}`, 'Swap failed')
				: await attempt(run, `${titles[mode]}: ${from} ↔ ${to}`, 'Swap failed');
		if (ok) await load();
	}

	const cell = 'px-4 py-3';
	const pill = (s: string) =>
		s === 'Running' ? 'bg-green-100 text-green-800 dark:bg-green-900/40 dark:text-green-300' : s === 'Stopped' ? 'bg-gray-100 text-gray-700 dark:bg-slate-800 dark:text-gray-300' : 'bg-amber-100 text-amber-800 dark:bg-amber-900/40 dark:text-amber-300';

	onMount(load);
</script>

{#snippet hostLink(site: WebSite, testId: string)}
	{#if site.defaultHostName}
		<a class="underline decoration-dotted break-all" href={siteUrl(site)} target="_blank" rel="noopener noreferrer" data-testid={testId}>{site.defaultHostName}</a>
	{:else}—{/if}
{/snippet}

<div class="space-y-6 pb-20" data-testid="appservice-dashboard">
	<PageHeader icon={Globe} title="App Service" description="Azure App Service web apps: runtime, state, hostnames and deployment slots" color="sky" onRefresh={load} />

	{#if error && apps.length === 0}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="appservice-error">
			<p class="font-medium">Could not load web apps</p>
			<p>{error}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if apps.length === 0}
		<p class="py-12 text-center text-gray-500 dark:text-gray-400" data-testid="appservice-empty">No web apps. Create one through Resource Manager (Microsoft.Web/sites).</p>
	{:else}
		<label class="flex items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
			Resource group
			<select class="{GCP_INPUT} !w-56 !py-1 text-xs" bind:value={filter} data-testid="appservice-rg-filter">
				<option value="">All resource groups</option>
				{#each rgs as rg (rg)}<option value={rg}>{rg}</option>{/each}
			</select>
		</label>

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="appservice-apps">
				<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400">
					<tr><th class={cell}>Web app</th><th class={cell}>Resource group</th><th class={cell}>State</th><th class={cell}>Runtime</th><th class={cell}>Hostname</th><th class="{cell} text-right">Actions</th></tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each shown as a (a.id)}
						<tr class={selected === a.id ? 'bg-sky-50/60 dark:bg-sky-950/20' : ''} data-testid="appservice-app">
							<td class="{cell} font-mono text-xs"><button class="underline decoration-dotted" onclick={() => select(a)} aria-expanded={selected === a.id} data-testid="appservice-select">{a.app}</button></td>
							<td class="{cell} font-mono text-xs">{a.resourceGroup}</td>
							<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(a.state)}" data-testid="appservice-state">{a.state || '—'}</span></td>
							<td class="{cell} text-xs">{a.runtime || '—'}</td>
							<td class="{cell} font-mono text-xs">{@render hostLink(a, 'appservice-host')}</td>
							<td class="{cell} text-right whitespace-nowrap space-x-1">
								<button class={GCP_MINI} onclick={() => act(a, 'start')} data-testid="appservice-start">Start</button>
								<button class={GCP_MINI} onclick={() => act(a, 'stop')} data-testid="appservice-stop">Stop</button>
								<button class={GCP_MINI} onclick={() => act(a, 'restart')} data-testid="appservice-restart">Restart</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if current}
			<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="appservice-detail">
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{current.app}</h2>
				{#if current.previewTarget}
					<p class="text-sm text-amber-700 dark:text-amber-300" data-testid="appservice-app-preview">
						Swap preview pending: production with {current.previewTarget}
						<button class={GCP_MINI} onclick={() => swap('complete', PRODUCTION_SLOT, current.previewTarget)} data-testid="appservice-app-complete-swap">Complete swap</button>
						<button class={GCP_MINI} onclick={() => swap('cancel', PRODUCTION_SLOT, current.previewTarget)} data-testid="appservice-app-cancel-swap">Cancel swap</button>
					</p>
				{/if}
				{#if current.lastSwap}<p class="text-xs text-gray-500" data-testid="appservice-last-swap">Last swap: {current.lastSwap}</p>{/if}
				{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="appservice-detail-error">{childError}</p>{/if}

				{#if childLoading && slots.length === 0}
					<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
				{:else}
					<section class="space-y-2">
						<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Deployment slots</h3>
						{#if slots.length === 0}
							<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="appservice-slots-empty">No deployment slots (the production slot only)</p>
						{:else}
							<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
								<table class="w-full text-sm" data-testid="appservice-slots">
									<thead class="bg-gray-50 text-left text-gray-500 dark:bg-slate-800/60 dark:text-gray-400"><tr><th class={cell}>Slot</th><th class={cell}>State</th><th class={cell}>Runtime</th><th class={cell}>Hostname</th><th class={cell}>Swap</th><th class="{cell} text-right">Actions</th></tr></thead>
									<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
										{#each slots as s (s.id)}
											<tr data-testid="appservice-slot">
												<td class="{cell} font-mono text-xs">{s.slot}</td>
												<td class={cell}><span class="rounded-full px-2 py-0.5 text-xs {pill(s.state)}" data-testid="appservice-slot-state">{s.state || '—'}</span></td>
												<td class="{cell} text-xs">{s.runtime || '—'}</td>
												<td class="{cell} font-mono text-xs">{@render hostLink(s, 'appservice-slot-host')}</td>
												<td class="{cell} text-xs" data-testid="appservice-slot-preview">{s.previewTarget ? `preview with ${s.previewTarget}` : '—'}</td>
												<td class="{cell} text-right whitespace-nowrap space-x-1">
													<button class={GCP_MINI} onclick={() => act(s, 'start')} data-testid="appservice-slot-start">Start</button>
													<button class={GCP_MINI} onclick={() => act(s, 'stop')} data-testid="appservice-slot-stop">Stop</button>
													<button class={GCP_MINI} onclick={() => act(s, 'restart')} data-testid="appservice-slot-restart">Restart</button>
													{#if s.previewTarget}
														<button class={GCP_MINI} onclick={() => swap('complete', s.slot, s.previewTarget)} data-testid="appservice-complete-swap">Complete swap</button>
														<button class={GCP_MINI} onclick={() => swap('cancel', s.slot, s.previewTarget)} data-testid="appservice-cancel-swap">Cancel swap</button>
													{/if}
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						{/if}
					</section>

					{#if slots.length > 0}
						<section class="space-y-2" data-testid="appservice-swap">
							<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Swap slots</h3>
							<div class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400">
								<label for="appservice-source">Source</label>
								<select id="appservice-source" class="{GCP_INPUT} !w-40 !py-1 text-xs" bind:value={source} onchange={() => (target = targets[0] ?? '')} data-testid="appservice-source">
									{#each sources as n (n)}<option value={n}>{n}</option>{/each}
								</select>
								<label for="appservice-target">Target</label>
								<select id="appservice-target" class="{GCP_INPUT} !w-40 !py-1 text-xs" bind:value={target} data-testid="appservice-target">
									{#each targets as n (n)}<option value={n}>{n}</option>{/each}
								</select>
								<button class={GCP_PRIMARY} onclick={() => swap('swap')} disabled={!target} data-testid="appservice-swap-button">Swap</button>
								<button class={GCP_MINI} onclick={() => swap('preview')} disabled={!target || previewing.length > 0 || !!current.previewTarget} data-testid="appservice-preview-button">Swap with preview</button>
							</div>
						</section>
					{/if}

					<section class="space-y-2" data-testid="appservice-sticky">
						<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Deployment slot settings (sticky) <span class="font-normal text-gray-400">({stickyCount(sticky)})</span></h3>
						{#if stickyCount(sticky) === 0}
							<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="appservice-sticky-empty">No settings stay with their slot; every setting moves with the content on a swap.</p>
						{:else}
							<ul class="flex flex-wrap gap-2 font-mono text-xs">
								{#each sticky.appSettings as n (`a-${n}`)}<li class="rounded-full bg-gray-100 px-2 py-0.5 dark:bg-slate-800" data-testid="appservice-sticky-name">app setting: {n}</li>{/each}
								{#each sticky.connectionStrings as n (`c-${n}`)}<li class="rounded-full bg-gray-100 px-2 py-0.5 dark:bg-slate-800" data-testid="appservice-sticky-name">connection string: {n}</li>{/each}
								{#each sticky.azureStorage as n (`s-${n}`)}<li class="rounded-full bg-gray-100 px-2 py-0.5 dark:bg-slate-800" data-testid="appservice-sticky-name">storage mount: {n}</li>{/each}
							</ul>
						{/if}
					</section>
				{/if}
			</div>
		{/if}
	{/if}
</div>
