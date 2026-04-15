<script lang="ts">
	import { implementedDashboardRouteIds, sidebarCategories } from '$lib/nav';
	import { BookOpen, Check } from 'lucide-svelte';

	const implementedRoutes = sidebarCategories
		.map(cat => ({
			...cat,
			routes: cat.routes.filter(r => implementedDashboardRouteIds.has(r.id))
		}))
		.filter(cat => cat.routes.length > 0);
</script>

<section class="space-y-6">
	<div class="space-y-3">
		<div class="flex items-center gap-3 mb-4">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<BookOpen class="w-6 h-6 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500 dark:text-slate-400">Documentation</p>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Implemented Services</h1>
			</div>
		</div>
		<p class="text-slate-600 dark:text-slate-300">
			Complete list of AWS services with dashboard UIs available in Gopherstack.
		</p>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
		{#each implementedRoutes as category}
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
				<h2 class="font-semibold text-slate-900 dark:text-white mb-4">{category.label}</h2>
				<ul class="space-y-2">
					{#each category.routes as route}
						<li class="flex items-center gap-3">
							<Check class="w-4 h-4 text-green-600 dark:text-green-400 flex-shrink-0" />
							<a href={route.href} class="text-slate-600 dark:text-slate-300 hover:text-indigo-600 dark:hover:text-indigo-400 transition-colors font-medium">
								{route.label}
							</a>
						</li>
					{/each}
				</ul>
			</div>
		{/each}
	</div>

	<div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-900/50 rounded-lg p-6">
		<h3 class="font-semibold text-blue-900 dark:text-blue-200 mb-2">Total Services</h3>
		<p class="text-sm text-blue-700 dark:text-blue-300">
			{implementedDashboardRouteIds.size} services implemented with full dashboard UI support
		</p>
	</div>
</section>

