<script lang="ts">
	// Matches the search box markup used across service pages, e.g.
	// detective/+page.svelte:69-72.
	import { Search } from 'lucide-svelte';

	type Props = {
		value: string;
		placeholder?: string;
		onInput?: (value: string) => void;
		class?: string;
	};

	let {
		value = $bindable(),
		placeholder = 'Search...',
		onInput,
		class: className = 'w-full sm:w-64'
	}: Props = $props();

	function handleInput(event: Event): void {
		value = (event.target as HTMLInputElement).value;
		onInput?.(value);
	}
</script>

<div class="relative">
	<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
	<input
		{value}
		oninput={handleInput}
		{placeholder}
		class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white {className}"
	/>
</div>
