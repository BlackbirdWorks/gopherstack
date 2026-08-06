<script lang="ts">
	import { onMount } from 'svelte';
	import { Toaster } from 'svelte-sonner';
	import './layout.css';
	import { page } from '$app/state';
	import { sidebarCategories, implementedDashboardRouteIds, getCommonServices, getUncommonCategories } from '$lib/nav';
	import { goto } from '$app/navigation';
	import { initializeTheme, isDarkTheme, setTheme, themes, type ThemeName } from '$lib/theme';
	import ServiceIcon from '$lib/components/ServiceIcon.svelte';
	import ConfirmDialog from '$lib/components/ConfirmDialog.svelte';
	import RegionPicker from '$lib/components/RegionPicker.svelte';
	import { registerConfirmDialog, unregisterConfirmDialog } from '$lib/confirm-dialog';

	let { children } = $props();
	let theme = $state<ThemeName>('light');
	let themeDropdownOpen = $state(false);
	let searchQuery = $state('');
	let searchOpen = $state(false);
	
	let expandedSections = $state<Record<string, boolean>>(
		Object.fromEntries(sidebarCategories.map((category) => [category.id, true]))
	);

	let sidebarOpen = $state(false);
	let sidebarMini = $state(false);
	let showAllServices = $state(false);
	let confirmDialog = $state<ConfirmDialog | null>(null);

	const visibleSidebarCategories = $derived.by(() => {
		// By default, show only the common (most-used) implemented services. When
		// showAllServices is toggled on, also surface the "uncommon" (long-tail)
		// implemented services via getUncommonCategories(), organized by category.
		const commonServiceIds = new Set(getCommonServices().map(s => s.id));
		const uncommonServiceIds = showAllServices
			? new Set(getUncommonCategories().flatMap((category) => category.routes).map((r) => r.id))
			: new Set<string>();

		const filtered = sidebarCategories
			.map((category) => ({
				...category,
				routes: category.routes.filter((route) =>
					implementedDashboardRouteIds.has(route.id) &&
					(commonServiceIds.has(route.id) || uncommonServiceIds.has(route.id))
				)
			}))
			.filter((category) => category.routes.length > 0);

		return filtered;
	});

	const searchResults = $derived.by(() => {
		if (!searchQuery.trim()) return [];
		const query = searchQuery.toLowerCase();
		const results = [];
		
		for (const category of sidebarCategories) {
			for (const route of category.routes) {
				if ((route.label.toLowerCase().includes(query) || route.id.toLowerCase().includes(query)) &&
					implementedDashboardRouteIds.has(route.id)) {
					results.push({ ...route, categoryLabel: category.label });
				}
			}
		}
		return results;
	});

	function selectSearchResult(href: string) {
		searchQuery = '';
		searchOpen = false;
		goto(href);
	}

	onMount(() => {
		const confirmHandler = (options: Parameters<ConfirmDialog['show']>[0]) => {
			if (!confirmDialog) {
				return Promise.resolve(false);
			}

			return confirmDialog.show(options);
		};
		registerConfirmDialog(confirmHandler);
		theme = initializeTheme(document, window.localStorage, window.matchMedia('(prefers-color-scheme: dark)').matches);
		sidebarMini = window.localStorage.getItem('gopherstack-sidebar-mini') === 'true';
		showAllServices = window.localStorage.getItem('gopherstack-sidebar-show-all') === 'true';

		// Close search on escape
		const handleKeydown = (e: KeyboardEvent) => {
			if (e.key === 'Escape') searchOpen = false;
			if ((e.metaKey || e.ctrlKey) && e.key === 'k') {
				e.preventDefault();
				searchOpen = true;
				document.querySelector<HTMLInputElement>('#global-search')?.focus();
			}
		};
		document.addEventListener('keydown', handleKeydown);
		return () => {
			unregisterConfirmDialog(confirmHandler);
			document.removeEventListener('keydown', handleKeydown);
		};
	});

	function selectTheme(t: ThemeName): void {
		theme = setTheme(document, window.localStorage, t);
		themeDropdownOpen = false;
	}

	const themeLabels: Record<ThemeName, string> = {
		light: 'Light',
		dark: 'Dark (Black)',
		github: 'GitHub Dark',
		'github-light': 'GitHub Light',
		ocean: 'Ocean',
		'cyberpunk-2077': 'Cyberpunk 2077',
		aurora: 'Aurora',
		solstice: 'Solstice',
		arasaka: 'Arasaka'
	};


	function toggleMiniMode(): void {
		sidebarMini = !sidebarMini;
		window.localStorage.setItem('gopherstack-sidebar-mini', String(sidebarMini));
	}

	function toggleShowAllServices(): void {
		showAllServices = !showAllServices;
		window.localStorage.setItem('gopherstack-sidebar-show-all', String(showAllServices));
	}

	function toggleSection(id: string) {
		expandedSections[id] = !expandedSections[id];
	}

	function isActive(pathname: string, href: string): boolean {
		if (href === '/dashboard') return pathname === '/dashboard' || pathname === '/dashboard/';
		return pathname.startsWith(href);
	}
	
	function isActiveTab(tab: string): boolean {
		const pathname = page.url.pathname;
		if (tab === 'metrics' && pathname.startsWith('/dashboard/metrics')) return true;
		if (tab === 'console' && pathname.startsWith('/dashboard/console')) return true;
		if (tab === 'chaos' && pathname.startsWith('/dashboard/chaos')) return true;
		if (tab === 'docs' && pathname.startsWith('/dashboard/docs')) return true;
		if (tab === 'settings' && pathname.startsWith('/dashboard/settings')) return true;
		if (tab === 'resources' && pathname.startsWith('/dashboard/resources')) return true;
		return false;
	}
</script>

<svelte:head>
	<link rel="icon" href="/dashboard/static/favicon.png" />
	<title>Gopherstack Dashboard</title>
</svelte:head>

<Toaster position="bottom-left" />

<div class="bg-slate-50 dark:bg-slate-900 text-slate-800 dark:text-slate-200 antialiased font-sans min-h-screen">
	<!-- Top Navbar -->
	<nav id="topbar" class="fixed top-0 z-50 w-full bg-white/80 backdrop-blur-md border-b border-slate-200 dark:bg-slate-900/80 dark:border-white/5 shadow-sm">
		<div class="px-3 py-3 lg:px-5 lg:pl-3 flex items-center justify-start">
			<div class="flex items-center lg:w-[15.25rem] shrink-0">
				<button onclick={() => sidebarOpen = !sidebarOpen} type="button" class="inline-flex items-center p-2 text-sm text-gray-500 rounded-lg lg:hidden hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-gray-200 dark:text-gray-400 dark:hover:bg-gray-700 dark:focus:ring-gray-600">
					<span class="sr-only">Open sidebar</span>
					<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 20 20"><path clip-rule="evenodd" fill-rule="evenodd" d="M2 4.75A.75.75 0 012.75 4h14.5a.75.75 0 010 1.5H2.75A.75.75 0 012 4.75zm0 10.5a.75.75 0 01.75-.75h7.5a.75.75 0 010 1.5h-7.5a.75.75 0 01-.75-.75zM2 10a.75.75 0 01.75-.75h14.5a.75.75 0 010 1.5H2.75A.75.75 0 012 10z" /></svg>
				</button>
				<a href="/dashboard" class="flex ml-2 md:mr-4 items-center gap-2 text-slate-900 dark:text-white font-semibold text-lg hover:opacity-80 transition-opacity">
					<img src="/dashboard/static/favicon.png" alt="Logo" class="w-8 h-8 rounded-lg drop-shadow-[0_0_8px_rgba(99,102,241,0.8)]" />
					<span class="drop-shadow-[0_0_8px_rgba(255,255,255,0.5)] dark:drop-shadow-[0_0_8px_rgba(255,255,255,0.5)]">Gopherstack</span>
				</a>
			</div>
			
			<div class="hidden md:flex justify-start max-w-2xl lg:ml-4 relative group">
                <div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                    <svg class="w-5 h-5 text-slate-400 group-focus-within:text-indigo-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                </div>
                <input 
                	type="text" 
                	id="global-search" 
                	bind:value={searchQuery}
                	onfocus={() => searchOpen = true}
                	onblur={() => setTimeout(() => searchOpen = false, 150)}
                	class="w-full pl-10 pr-12 py-1.5 bg-slate-100/40 dark:bg-white/5 border border-transparent hover:border-slate-200/60 dark:hover:border-white/10 rounded-xl text-sm focus:ring-4 focus:ring-indigo-500/10 focus:border-indigo-500/50 dark:text-white transition-all backdrop-blur-sm placeholder-slate-400 group-focus-within:bg-white dark:group-focus-within:bg-slate-900" 
                	placeholder="Search services..." 
                	autocomplete="off">
                <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                    <span class="text-xs font-semibold text-slate-400 dark:text-slate-500 bg-slate-200 dark:bg-slate-700 px-1.5 py-0.5 rounded border border-slate-300 dark:border-slate-600">⌘K</span>
                </div>
                
                <!-- Search Results Dropdown -->
                {#if searchOpen && searchResults.length > 0}
                	<div class="absolute top-full left-0 right-0 mt-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg shadow-2xl z-50 max-h-96 overflow-y-auto">
                		{#each searchResults as result}
                			<button 
                				onclick={() => selectSearchResult(result.href)}
                				class="w-full text-left px-4 py-3 hover:bg-slate-100 dark:hover:bg-slate-700 transition-colors flex items-center gap-3 border-b border-slate-100 dark:border-slate-700 last:border-b-0">
                				<ServiceIcon icon={result.icon} label={result.label} class="w-5 h-5 rounded flex-shrink-0" />
                				<div class="flex-1 min-w-0">
                					<p class="font-medium text-slate-900 dark:text-white truncate">{result.label}</p>
                					<p class="text-xs text-slate-500 dark:text-slate-400">{result.categoryLabel}</p>
                				</div>
                			</button>
                		{/each}
                	</div>
                {/if}
            </div>

			<div class="flex items-center gap-1 ml-auto">
				<RegionPicker />
				<a id="nav-metrics" href="/dashboard/metrics" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('metrics') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="System Metrics">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>
				</a>
				<a id="nav-resources" href="/dashboard/resources" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('resources') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="Resource Health">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M19 11H5m14 0a2 2 0 012 2v6a2 2 0 01-2 2H5a2 2 0 01-2-2v-6a2 2 0 012-2m14 0V9a2 2 0 00-2-2M5 11V9a2 2 0 012-2m0 0V5a2 2 0 012-2h6a2 2 0 012 2v2M7 7h10" /></svg>
				</a>
				<a id="nav-console" href="/dashboard/console" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('console') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="Live API Console">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"></path></svg>
				</a>
				<a id="nav-chaos" href="/dashboard/chaos" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('chaos') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="Chaos Engineering">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
				</a>
				<a id="nav-docs" href="/dashboard/docs" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('docs') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="Docs">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M12 6.253v13m0-13C10.832 5.477 9.246 5 7.5 5S4.168 5.477 3 6.253v13C4.168 18.477 5.754 18 7.5 18s3.332.477 4.5 1.253m0-13C13.168 5.477 14.754 5 16.5 5c1.747 0 3.332.477 4.5 1.253v13C19.832 18.477 18.247 18 16.5 18c-1.746 0-3.332.477-4.5 1.253" /></svg>
				</a>
				<a id="nav-settings" href="/dashboard/settings" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('settings') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="Settings">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" /><path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" /></svg>
				</a>
				<div class="relative">
					<button id="theme-selector" onclick={() => themeDropdownOpen = !themeDropdownOpen} type="button" class="inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors" title="Theme: {themeLabels[theme]}">
						{#if isDarkTheme(theme)}
							<svg class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z" /></svg>
						{:else}
							<svg class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 2a1 1 0 011 1v1a1 1 0 11-2 0V3a1 1 0 011-1zm4 8a4 4 0 11-8 0 4 4 0 018 0zm-.464 4.95l.707.707a1 1 0 001.414-1.414l-.707-.707a1 1 0 00-1.414 1.414zm2.12-10.607a1 1 0 010 1.414l-.706.707a1 1 0 11-1.414-1.414l.707-.707a1 1 0 011.414 0zM17 11a1 1 0 100-2h-1a1 1 0 100 2h1zm-7 4a1 1 0 011 1v1a1 1 0 11-2 0v-1a1 1 0 011-1zM5.05 6.464A1 1 0 106.465 5.05l-.708-.707a1 1 0 00-1.414 1.414l.707.707zm1.414 8.486l-.707.707a1 1 0 01-1.414-1.414l.707-.707a1 1 0 011.414 1.414zM4 11a1 1 0 100-2H3a1 1 0 000 2h1z" clip-rule="evenodd" /></svg>
						{/if}
					</button>
					{#if themeDropdownOpen}
						<div class="absolute right-0 mt-1 w-40 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg shadow-lg z-50">
							<div class="py-1 text-sm">
								{#each themes as t}
									<button data-theme={t} onclick={() => selectTheme(t)} class="w-full text-left px-4 py-2 hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 flex items-center gap-2 {theme === t ? 'bg-slate-50 dark:bg-slate-700 font-semibold text-indigo-600 dark:text-indigo-400' : ''}">
										<span class="w-3 h-3 rounded-full {t === 'light' ? 'bg-amber-400' : t === 'dark' ? 'bg-black border border-white/20' : t === 'github' ? 'bg-gray-800' : t === 'github-light' ? 'bg-blue-400 border border-gray-300' : t === 'ocean' ? 'bg-cyan-500' : t === 'cyberpunk-2077' ? 'bg-fuchsia-500' : t === 'aurora' ? 'bg-emerald-400' : 'bg-orange-300 border border-orange-400/60'}"></span>
										{themeLabels[t]}
									</button>
								{/each}
							</div>
						</div>
					{/if}
				</div>
			</div>
		</div>
	</nav>

	<!-- Sidebar -->
	<aside id="sidebar" class={`fixed top-0 left-0 z-40 h-screen pt-16 transition-[width,transform] bg-white border-r border-slate-200 dark:bg-slate-900 dark:border-white/5 flex flex-col ${sidebarOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'} ${sidebarMini ? 'w-[4.5rem]' : 'w-64'}`} aria-label="Sidebar">
		<div class="flex-1 px-3 pb-4 overflow-y-auto bg-white dark:bg-transparent">
			<ul class="space-y-1 font-medium mt-4">
				{#each visibleSidebarCategories as category}
					<li>
						{#if !sidebarMini}
							<button class="category-btn flex items-center justify-between w-full px-3 py-1 mt-4 text-xs font-bold uppercase tracking-widest text-slate-400 dark:text-slate-500 hover:text-slate-600 dark:hover:text-slate-300 transition-colors" onclick={() => toggleSection(category.id)}>
								<span class="category-header-text">{category.label}</span>
								<svg class={`w-3 h-3 chevron-icon transition-transform duration-200 ${expandedSections[category.id] ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
							</button>
						{:else}
							<div class="mt-4 border-t border-slate-100 dark:border-white/5"></div>
						{/if}
						{#if sidebarMini || expandedSections[category.id]}
							<ul class="space-y-1 mt-1 origin-top block">
								{#each category.routes as route}
									<li>
										<a id="nav-{route.id}" href={route.href} class={`flex items-center gap-3 p-2 rounded-lg text-slate-600 hover:bg-slate-100 hover:text-indigo-600 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-indigo-400 transition-colors ${isActive(page.url.pathname, route.href) ? 'bg-indigo-50 text-indigo-700 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''} ${sidebarMini ? 'justify-center px-0' : ''}`}>
											<ServiceIcon icon={route.icon} label={route.label} class="w-5 h-5 flex-shrink-0" />
											{#if !sidebarMini}
												<span class="sidebar-text truncate" title={route.label}>{route.label}</span>
											{/if}
										</a>
									</li>
								{/each}
							</ul>
						{/if}
					</li>
				{/each}
			</ul>
		</div>

		<!-- Sidebar Footer Toggle -->
		<div class="p-4 border-t border-slate-200 dark:border-white/5 bg-slate-50 dark:bg-transparent hidden lg:flex justify-between items-center pr-4 shrink-0" id="sidebar-footer-toggle">
			<button id="sidebar-show-all-toggle" onclick={toggleShowAllServices} type="button" aria-pressed={showAllServices} class={`p-1 transition-colors bg-white dark:bg-slate-800 rounded border shadow-sm ${showAllServices ? 'text-indigo-600 dark:text-indigo-400 border-indigo-300 dark:border-indigo-500/40' : 'text-slate-400 hover:text-slate-900 dark:text-slate-500 dark:hover:text-white border-slate-200 dark:border-white/10'}`} title={showAllServices ? 'Showing all services — click to show common services only' : 'Showing common services only — click to show all services'}>
				<svg class="w-4 h-4" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M4 6h16M4 12h16M4 18h16" /></svg>
			</button>
			<button onclick={toggleMiniMode} class="p-1 text-slate-400 hover:text-slate-900 dark:text-slate-500 dark:hover:text-white transition-colors bg-white dark:bg-slate-800 rounded border border-slate-200 dark:border-white/10 shadow-sm" title="Toggle Sidebar Width">
				<svg class="w-4 h-4 transition-transform duration-200" id="sidebar-toggle-icon" fill="none" stroke="currentColor" viewBox="0 0 24 24" style={sidebarMini ? 'transform: rotate(180deg)' : ''}>
					<path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M11 19l-7-7 7-7m8 14l-7-7 7-7"></path>
				</svg>
			</button>
		</div>
	</aside>

	<div class={`p-4 pt-20 transition-all duration-200 main-content ${sidebarMini ? 'lg:ml-[4.5rem]' : 'lg:ml-64'}`}>
		<main>
			{@render children()}
		</main>
	</div>
	<ConfirmDialog bind:this={confirmDialog} />
</div>
