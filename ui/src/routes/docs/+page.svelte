<script lang="ts">
	import { implementedDashboardRouteIds, sidebarCategories } from '$lib/nav';
	import { BookOpen, Check, ExternalLink } from 'lucide-svelte';

	const implementedRoutes = sidebarCategories
		.map(cat => ({
			...cat,
			routes: cat.routes.filter(r => implementedDashboardRouteIds.has(r.id))
		}))
		.filter(cat => cat.routes.length > 0);

	const githubRepoURL = 'https://github.com/BlackbirdWorks/gopherstack';
</script>

<section class="space-y-6">
	<div class="space-y-3">
		<div class="flex items-center justify-between mb-4">
			<div class="flex items-center gap-3">
				<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
					<BookOpen class="w-6 h-6 text-blue-600 dark:text-blue-400" />
				</div>
				<div>
					<p class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">Documentation</p>
					<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Supported Services</h1>
				</div>
			</div>
			<a
				href={githubRepoURL}
				target="_blank"
				rel="noopener noreferrer"
				class="inline-flex items-center gap-2 px-4 py-2 bg-slate-900 dark:bg-slate-700 text-white rounded-lg hover:bg-slate-700 dark:hover:bg-slate-600 transition-colors text-sm font-medium"
			>
				<svg class="w-4 h-4" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd" /></svg>
				View on GitHub
				<ExternalLink class="w-3 h-3 opacity-70" />
			</a>
		</div>
		<p class="text-slate-600 dark:text-slate-300">
			Complete list of AWS services supported by Gopherstack, including dashboard UIs and all SDK operations.
			Click a service name to open its dashboard.
		</p>
	</div>

	<!-- Stats bar -->
	<div class="grid grid-cols-2 sm:grid-cols-3 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 text-center">
			<p class="text-2xl font-bold text-indigo-600 dark:text-indigo-400">{implementedDashboardRouteIds.size}</p>
			<p class="text-sm text-slate-600 dark:text-slate-400 mt-1">Services with Dashboard UI</p>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 text-center">
			<p class="text-2xl font-bold text-green-600 dark:text-green-400">{implementedRoutes.reduce((acc, cat) => acc + cat.routes.length, 0)}</p>
			<p class="text-sm text-slate-600 dark:text-slate-400 mt-1">Categorised Services</p>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 text-center col-span-2 sm:col-span-1">
			<p class="text-2xl font-bold text-blue-600 dark:text-blue-400">{implementedRoutes.length}</p>
			<p class="text-sm text-slate-600 dark:text-slate-400 mt-1">Service Categories</p>
		</div>
	</div>

	<!-- Service grid -->
	<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
		{#each implementedRoutes as category}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
				<h2 class="font-semibold text-slate-900 dark:text-white mb-4 flex items-center gap-2">
					{category.label}
					<span class="text-xs font-normal text-slate-500 dark:text-slate-400 bg-slate-100 dark:bg-slate-700 px-2 py-0.5 rounded-full">
						{category.routes.length}
					</span>
				</h2>
				<ul class="space-y-2">
					{#each category.routes as route}
						<li class="flex items-center gap-3">
							<Check class="w-4 h-4 text-green-600 dark:text-green-400 flex-shrink-0" />
							<a
								href={route.href}
								class="text-slate-600 dark:text-slate-300 hover:text-indigo-600 dark:hover:text-indigo-400 transition-colors font-medium"
							>
								{route.label}
							</a>
						</li>
					{/each}
				</ul>
			</div>
		{/each}
	</div>

	<!-- GitHub link footer -->
	<div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-900/50 rounded-lg p-6 flex flex-col sm:flex-row items-start sm:items-center justify-between gap-4">
		<div>
			<h3 class="font-semibold text-blue-900 dark:text-blue-200 mb-1">Open Source</h3>
			<p class="text-sm text-blue-700 dark:text-blue-300">
				Gopherstack is open source. Browse the source code, report issues, or contribute on GitHub.
			</p>
		</div>
		<a
			href={githubRepoURL}
			target="_blank"
			rel="noopener noreferrer"
			class="inline-flex items-center gap-2 px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors text-sm font-medium whitespace-nowrap flex-shrink-0"
		>
			<svg class="w-4 h-4" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true"><path fill-rule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clip-rule="evenodd" /></svg>
			GitHub Repository
			<ExternalLink class="w-3 h-3 opacity-80" />
		</a>
	</div>
</section>


