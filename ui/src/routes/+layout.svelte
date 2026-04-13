<script lang="ts">
	import { onMount } from 'svelte';
	import { Toaster } from 'svelte-sonner';
	import './layout.css';
	import { page } from '$app/state';
	import { sidebarCategories } from '$lib/nav';
	import { initializeTheme, toggleStoredTheme, type ThemeMode } from '$lib/theme';

	let { children } = $props();
	let theme = $state<ThemeMode>('light');
	
	let expandedSections = $state<Record<string, boolean>>({
		'core-services': true
	});

	let sidebarOpen = $state(false);
	let sidebarMini = $state(false);

	const AWS_REGIONS = [
		'us-east-1', 'us-east-2', 'us-west-1', 'us-west-2',
		'eu-central-1', 'eu-west-1', 'eu-west-2',
		'ap-south-1', 'ap-northeast-1', 'ap-southeast-1', 'ap-southeast-2'
	];
	let currentRegion = $state('us-east-1');
	let regionDropdownOpen = $state(false);

	function selectRegion(region: string) {
		currentRegion = region;
		regionDropdownOpen = false;
	}

	onMount(() => {
		theme = initializeTheme(document, window.localStorage, window.matchMedia('(prefers-color-scheme: dark)').matches);
		sidebarMini = window.localStorage.getItem('gopherstack-sidebar-mini') === 'true';
	});

	function toggleTheme(): void {
		theme = toggleStoredTheme(document, window.localStorage, theme);
	}

	function toggleMiniMode(): void {
		sidebarMini = !sidebarMini;
		window.localStorage.setItem('gopherstack-sidebar-mini', String(sidebarMini));
	}

	function toggleSection(id: string) {
		expandedSections[id] = !expandedSections[id];
	}

	function isActive(pathname: string, href: string): boolean {
		if (href === '/dashboard2') return pathname === '/dashboard2' || pathname === '/dashboard2/';
		return pathname.startsWith(href);
	}
	
	function isActiveTab(tab: string): boolean {
		const pathname = page.url.pathname;
		if (tab === 'metrics' && pathname.startsWith('/dashboard2/metrics')) return true;
		if (tab === 'console' && pathname.startsWith('/dashboard2/console')) return true;
		if (tab === 'chaos' && pathname.startsWith('/dashboard2/chaos')) return true;
		if (tab === 'settings' && pathname.startsWith('/dashboard2/settings')) return true;
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
	<nav class="fixed top-0 z-50 w-full bg-white/80 backdrop-blur-md border-b border-slate-200 dark:bg-slate-900/80 dark:border-white/5 shadow-sm">
		<div class="px-3 py-3 lg:px-5 lg:pl-3 flex items-center justify-between">
			<div class="flex items-center">
				<button onclick={() => sidebarOpen = !sidebarOpen} type="button" class="inline-flex items-center p-2 text-sm text-gray-500 rounded-lg lg:hidden hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-gray-200 dark:text-gray-400 dark:hover:bg-gray-700 dark:focus:ring-gray-600">
					<span class="sr-only">Open sidebar</span>
					<svg class="w-6 h-6" fill="currentColor" viewBox="0 0 20 20"><path clip-rule="evenodd" fill-rule="evenodd" d="M2 4.75A.75.75 0 012.75 4h14.5a.75.75 0 010 1.5H2.75A.75.75 0 012 4.75zm0 10.5a.75.75 0 01.75-.75h7.5a.75.75 0 010 1.5h-7.5a.75.75 0 01-.75-.75zM2 10a.75.75 0 01.75-.75h14.5a.75.75 0 010 1.5H2.75A.75.75 0 012 10z" /></svg>
				</button>
				<a href="/dashboard2" class="flex ml-2 md:mr-4 items-center gap-2 text-slate-900 dark:text-white font-semibold text-lg hover:opacity-80 transition-opacity">
					<img src="/dashboard/static/favicon.png" alt="Logo" class="w-8 h-8 rounded-lg drop-shadow-[0_0_8px_rgba(99,102,241,0.8)]" />
					<span class="drop-shadow-[0_0_8px_rgba(255,255,255,0.5)] dark:drop-shadow-[0_0_8px_rgba(255,255,255,0.5)]">Gopherstack</span>
				</a>
			</div>
			
			<div class="hidden md:flex flex-grow justify-start max-w-3xl lg:ml-32 relative group">
                <div class="absolute inset-y-0 left-0 flex items-center pl-3 pointer-events-none">
                    <svg class="w-5 h-5 text-slate-400 group-focus-within:text-indigo-500 transition-colors" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"></path></svg>
                </div>
                <!-- Interactive search input is cosmetic in standard layout but matches original visual markup -->
                <input type="text" id="global-search" class="w-full pl-10 pr-12 py-2 bg-slate-100/50 dark:bg-white/5 border border-slate-200 dark:border-white/10 rounded-xl text-sm focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 dark:text-white transition-all backdrop-blur-sm placeholder-slate-400 shadow-inner group-focus-within:bg-white dark:group-focus-within:bg-slate-800" placeholder="Search for services, resources, or documentation..." autocomplete="off">
                <div class="absolute inset-y-0 right-0 flex items-center pr-3 pointer-events-none">
                    <span class="text-xs font-semibold text-slate-400 dark:text-slate-500 bg-slate-200 dark:bg-slate-700 px-1.5 py-0.5 rounded border border-slate-300 dark:border-slate-600">⌘K</span>
                </div>
            </div>

			<div class="flex items-center gap-1">
				<!-- Region Selector -->
				<div class="relative">
					<button onclick={() => regionDropdownOpen = !regionDropdownOpen} type="button" class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-mono font-medium text-slate-600 dark:text-slate-300 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors" title="Switch region" aria-haspopup="listbox" aria-expanded={regionDropdownOpen}>
						<svg class="w-3.5 h-3.5 text-indigo-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064" /></svg>
						<span>{currentRegion}</span>
						<svg class={`w-3 h-3 transition-transform duration-200 ${regionDropdownOpen ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" /></svg>
					</button>
					{#if regionDropdownOpen}
						<div class="absolute right-0 mt-1 w-48 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg shadow-lg z-50">
							<div class="py-1 text-sm max-h-64 overflow-y-auto">
								{#each AWS_REGIONS as region}
									<button onclick={() => selectRegion(region)} class="w-full text-left px-4 py-2 hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 {currentRegion === region ? 'bg-slate-50 text-indigo-600 dark:bg-slate-700 dark:text-indigo-400 font-semibold' : ''}">
										{region}
									</button>
								{/each}
							</div>
						</div>
					{/if}
				</div>
				<a href="/dashboard2/metrics" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('metrics') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="System Metrics">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>
				</a>
				<a href="/dashboard2/console" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('console') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="Live API Console">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M8 9l3 3-3 3m5 0h3M5 20h14a2 2 0 002-2V6a2 2 0 00-2-2H5a2 2 0 00-2 2v12a2 2 0 002 2z"></path></svg>
				</a>
				<button class="inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors opacity-40 cursor-not-allowed" title="Chaos Testing (Unavailable)">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M13 10V3L4 14h7v7l9-11h-7z" /></svg>
				</button>
				<a href="/dashboard2/settings" class={`inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors ${isActiveTab('settings') ? 'bg-slate-100 text-indigo-600 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''}`} title="Settings">
					<svg class="w-5 h-5" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M10.325 4.317c.426-1.756 2.924-1.756 3.35 0a1.724 1.724 0 002.573 1.066c1.543-.94 3.31.826 2.37 2.37a1.724 1.724 0 001.065 2.572c1.756.426 1.756 2.924 0 3.35a1.724 1.724 0 00-1.066 2.573c.94 1.543-.826 3.31-2.37 2.37a1.724 1.724 0 00-2.572 1.065c-.426 1.756-2.924 1.756-3.35 0a1.724 1.724 0 00-2.573-1.066c-1.543.94-3.31-.826-2.37-2.37a1.724 1.724 0 00-1.065-2.572c-1.756-.426-1.756-2.924 0-3.35a1.724 1.724 0 001.066-2.573c-.94-1.543.826-3.31 2.37-2.37.996.608 2.296.07 2.572-1.065z" /><path stroke-linecap="round" stroke-linejoin="round" d="M15 12a3 3 0 11-6 0 3 3 0 016 0z" /></svg>
				</a>
				<button onclick={toggleTheme} type="button" class="inline-flex items-center justify-center w-9 h-9 text-slate-500 rounded-lg hover:bg-slate-100 hover:text-slate-900 focus:outline-none focus:ring-2 focus:ring-slate-200 dark:text-slate-400 dark:hover:bg-slate-700 dark:hover:text-white transition-colors" title="Toggle dark mode">
					{#if theme === 'dark'}
						<svg class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path d="M17.293 13.293A8 8 0 016.707 2.707a8.001 8.001 0 1010.586 10.586z" /></svg>
					{:else}
						<svg class="w-5 h-5" fill="currentColor" viewBox="0 0 20 20"><path fill-rule="evenodd" d="M10 2a1 1 0 011 1v1a1 1 0 11-2 0V3a1 1 0 011-1zm4 8a4 4 0 11-8 0 4 4 0 018 0zm-.464 4.95l.707.707a1 1 0 001.414-1.414l-.707-.707a1 1 0 00-1.414 1.414zm2.12-10.607a1 1 0 010 1.414l-.706.707a1 1 0 11-1.414-1.414l.707-.707a1 1 0 011.414 0zM17 11a1 1 0 100-2h-1a1 1 0 100 2h1zm-7 4a1 1 0 011 1v1a1 1 0 11-2 0v-1a1 1 0 011-1zM5.05 6.464A1 1 0 106.465 5.05l-.708-.707a1 1 0 00-1.414 1.414l.707.707zm1.414 8.486l-.707.707a1 1 0 01-1.414-1.414l.707-.707a1 1 0 011.414 1.414zM4 11a1 1 0 100-2H3a1 1 0 000 2h1z" clip-rule="evenodd" /></svg>
					{/if}
				</button>
			</div>
		</div>
	</nav>

	<!-- Sidebar -->
	<aside id="sidebar" class={`fixed top-0 left-0 z-40 h-screen pt-16 transition-[width,transform] bg-white border-r border-slate-200 dark:bg-slate-900 dark:border-white/5 flex flex-col ${sidebarOpen ? 'translate-x-0' : '-translate-x-full lg:translate-x-0'} ${sidebarMini ? 'w-[4.5rem]' : 'w-64'}`} aria-label="Sidebar">
		<div class="flex-1 px-3 pb-4 overflow-y-auto bg-white dark:bg-transparent">
			<ul class="space-y-1 font-medium mt-4">
				{#each sidebarCategories as category}
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
										<a href={route.href} class={`flex items-center gap-3 p-2 rounded-lg text-slate-600 hover:bg-slate-100 hover:text-indigo-600 dark:text-slate-400 dark:hover:bg-slate-800 dark:hover:text-indigo-400 transition-colors ${isActive(page.url.pathname, route.href) ? 'bg-indigo-50 text-indigo-700 dark:bg-slate-800 dark:text-indigo-400 font-semibold' : ''} ${sidebarMini ? 'justify-center px-0' : ''}`}>
											<img src={`/dashboard/static/icons/${route.icon}.svg`} class="w-5 h-5 flex-shrink-0 rounded-md shadow-sm" alt={route.id} />
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
		<div class="p-4 border-t border-slate-200 dark:border-white/5 bg-slate-50 dark:bg-transparent hidden lg:flex justify-end pr-4 shrink-0" id="sidebar-footer-toggle">
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
</div>
