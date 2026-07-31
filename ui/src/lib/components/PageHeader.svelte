<script lang="ts">
	import type { ComponentType, SvelteComponent, Snippet } from 'svelte';
	import { RefreshCw, type IconProps } from 'lucide-svelte';

	// The icon accent color varies per service (rose, teal, emerald, blue, …) —
	// see Tabs.svelte, which faces the same per-service-accent problem for its
	// active-tab background. Following that file's approach: a static
	// color→class map, so every class name Tailwind needs to see is present
	// verbatim in this file's source, regardless of which color a given page
	// picks at runtime. Dynamic construction (e.g. `text-${color}-500`) would
	// be invisible to the Tailwind scanner and silently render unstyled.
	export type PageHeaderColor =
		| 'rose'
		| 'emerald'
		| 'red'
		| 'orange'
		| 'teal'
		| 'blue'
		| 'violet'
		| 'green'
		| 'cyan'
		| 'sky'
		| 'amber'
		| 'indigo'
		| 'purple'
		| 'pink'
		| 'fuchsia'
		| 'gray';

	type Props = {
		icon: ComponentType<SvelteComponent<IconProps>>;
		title: string;
		description: string;
		onRefresh?: () => void;
		actions?: Snippet;
		color?: PageHeaderColor;
	};

	let { icon: Icon, title, description, onRefresh, actions, color = 'rose' }: Props = $props();

	const ICON_CLASSES: Record<PageHeaderColor, string> = {
		rose: 'text-rose-500',
		emerald: 'text-emerald-500',
		red: 'text-red-500',
		orange: 'text-orange-500',
		teal: 'text-teal-500',
		blue: 'text-blue-500',
		violet: 'text-violet-500',
		green: 'text-green-500',
		cyan: 'text-cyan-500',
		sky: 'text-sky-500',
		amber: 'text-amber-500',
		indigo: 'text-indigo-500',
		purple: 'text-purple-500',
		pink: 'text-pink-500',
		fuchsia: 'text-fuchsia-500',
		gray: 'text-gray-500'
	};
</script>

<div class="flex items-center justify-between flex-wrap gap-3">
	<div class="flex items-center gap-3">
		<Icon class="w-7 h-7 {ICON_CLASSES[color]}" />
		<div>
			<h1 class="text-2xl font-bold text-gray-900 dark:text-white">{title}</h1>
			<p class="text-sm text-gray-500 dark:text-gray-400">{description}</p>
		</div>
	</div>
	<div class="flex items-center gap-2 flex-wrap">
		{#if onRefresh}
			<button
				onclick={onRefresh}
				title="Refresh"
				class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm"
			>
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
		{/if}
		{@render actions?.()}
	</div>
</div>
