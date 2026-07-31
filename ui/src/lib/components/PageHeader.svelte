<script lang="ts">
	import type { Component, Snippet } from 'svelte';
	import { RefreshCw } from 'lucide-svelte';

	type Props = {
		icon: Component<Record<string, unknown>>;
		title: string;
		description: string;
		onRefresh?: () => void;
		actions?: Snippet;
	};

	let { icon: Icon, title, description, onRefresh, actions }: Props = $props();
</script>

<div class="flex items-center justify-between flex-wrap gap-3">
	<div class="flex items-center gap-3">
		<Icon class="w-7 h-7 text-rose-500" />
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
