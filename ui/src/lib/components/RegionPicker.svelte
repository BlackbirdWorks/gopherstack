<script lang="ts">
	import { onMount } from 'svelte';
	import { ALL_REGIONS, currentRegionSelection, setStoredRegion } from '$lib/region.svelte';
	import { fetchRegionCatalog } from '$lib/region-catalog';

	let open = $state(false);
	let query = $state('');
	let catalog = $state<string[]>([]);
	let inputEl = $state<HTMLInputElement | null>(null);

	onMount(() => {
		fetchRegionCatalog().then((regions) => {
			catalog = regions;
		});
	});

	// Filtered against the full DescribeRegions catalog, not the smaller
	// "regions with data" set -- this is an autocomplete over every region
	// that exists, not just ones already holding resources.
	const filtered = $derived.by(() => {
		const q = query.trim().toLowerCase();
		return q ? catalog.filter((r) => r.toLowerCase().includes(q)) : catalog;
	});

	const showAllOption = $derived('all'.includes(query.trim().toLowerCase()));

	// A typed region is only offered as a distinct "use as-is" option once it
	// doesn't already match a catalog entry -- otherwise selecting from the
	// list and pressing Enter would both appear as separate rows for the
	// same value.
	const showTypedOption = $derived.by(() => {
		const q = query.trim();
		return q.length > 0 && !catalog.some((r) => r.toLowerCase() === q.toLowerCase());
	});

	function label(selection: string): string {
		return selection === ALL_REGIONS ? 'All' : selection;
	}

	function select(value: string): void {
		setStoredRegion(value);
		query = '';
		open = false;
	}

	function toggle(): void {
		open = !open;
		if (open) queueMicrotask(() => inputEl?.focus());
	}

	function onKeydown(e: KeyboardEvent): void {
		if (e.key === 'Enter') {
			const typed = query.trim();
			if (typed) select(typed);
		} else if (e.key === 'Escape') {
			open = false;
		}
	}

	function onWindowClick(e: MouseEvent): void {
		if (!open) return;
		if (e.target instanceof Element && e.target.closest('#region-picker')) return;
		open = false;
	}
</script>

<svelte:window onclick={onWindowClick} />

<div class="relative" id="region-picker">
	<button
		onclick={toggle}
		type="button"
		class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-mono font-medium text-slate-600 dark:text-slate-300 bg-slate-100 dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-200 dark:hover:bg-slate-700 transition-colors"
		title="Switch region"
		aria-haspopup="listbox"
		aria-expanded={open}
	>
		<svg class="w-3.5 h-3.5 text-indigo-500" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M3.055 11H5a2 2 0 012 2v1a2 2 0 002 2 2 2 0 012 2v2.945M8 3.935V5.5A2.5 2.5 0 0010.5 8h.5a2 2 0 012 2 2 2 0 104 0 2 2 0 012-2h1.064M15 20.488V18a2 2 0 012-2h3.064" /></svg>
		<span data-testid="region-picker-label">{label(currentRegionSelection())}</span>
		<svg class={`w-3 h-3 transition-transform duration-200 ${open ? 'rotate-180' : ''}`} fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7" /></svg>
	</button>
	{#if open}
		<div class="absolute right-0 mt-1 w-56 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg shadow-lg z-50">
			<div class="p-2 border-b border-slate-200 dark:border-slate-700">
				<input
					bind:this={inputEl}
					bind:value={query}
					onkeydown={onKeydown}
					type="text"
					placeholder="Search or type a region..."
					class="w-full px-2 py-1.5 text-sm rounded border border-slate-200 dark:border-slate-600 bg-white dark:bg-slate-900 text-slate-800 dark:text-slate-200"
				/>
			</div>
			<div class="py-1 text-sm max-h-64 overflow-y-auto">
				{#if showAllOption}
					<button
						onclick={() => select(ALL_REGIONS)}
						class="w-full text-left px-4 py-2 hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 font-semibold {currentRegionSelection() === ALL_REGIONS ? 'bg-slate-50 text-indigo-600 dark:bg-slate-700 dark:text-indigo-400' : ''}"
					>
						All regions
					</button>
				{/if}
				{#each filtered as region (region)}
					<button
						onclick={() => select(region)}
						class="w-full text-left px-4 py-2 hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-700 dark:text-slate-200 {currentRegionSelection() === region ? 'bg-slate-50 text-indigo-600 dark:bg-slate-700 dark:text-indigo-400 font-semibold' : ''}"
					>
						{region}
					</button>
				{/each}
				{#if showTypedOption}
					<button onclick={() => select(query.trim())} class="w-full text-left px-4 py-2 hover:bg-slate-100 dark:hover:bg-slate-700 text-indigo-600 dark:text-indigo-400 border-t border-slate-100 dark:border-slate-700">
						Use "{query.trim()}"
					</button>
				{/if}
				{#if filtered.length === 0 && !showAllOption && !showTypedOption}
					<div class="px-4 py-2 text-slate-400 dark:text-slate-500">No matching regions</div>
				{/if}
			</div>
		</div>
	{/if}
</div>
