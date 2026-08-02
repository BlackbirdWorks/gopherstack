<script lang="ts">
	// Tab bar matching the visual style used across service pages (e.g.
	// detective/+page.svelte:61-68), generalized into a shared component.
	//
	// The active-tab accent color varies per service (rose, emerald, blue,
	// violet, …) — see the grep across src/routes for the full set — so it is
	// exposed as a `color` prop rather than hardcoded. The color→class map is
	// a static object so every class name Tailwind needs to see is present
	// verbatim in this file's source, regardless of which color a given page
	// picks at runtime.

	export type TabColor =
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

	export type Tab = {
		id: string;
		label: string;
	};

	type Props = {
		tabs: Tab[];
		active: string;
		onSelect: (id: string) => void;
		color?: TabColor;
	};

	let { tabs, active, onSelect, color = 'rose' }: Props = $props();

	const ACTIVE_CLASSES: Record<TabColor, string> = {
		rose: 'bg-rose-600 text-white',
		emerald: 'bg-emerald-600 text-white',
		red: 'bg-red-600 text-white',
		orange: 'bg-orange-600 text-white',
		teal: 'bg-teal-600 text-white',
		blue: 'bg-blue-600 text-white',
		violet: 'bg-violet-600 text-white',
		green: 'bg-green-600 text-white',
		cyan: 'bg-cyan-600 text-white',
		sky: 'bg-sky-600 text-white',
		amber: 'bg-amber-600 text-white',
		indigo: 'bg-indigo-600 text-white',
		purple: 'bg-purple-600 text-white',
		pink: 'bg-pink-600 text-white',
		fuchsia: 'bg-fuchsia-600 text-white',
		gray: 'bg-gray-600 text-white'
	};

	const INACTIVE_CLASSES =
		'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600';

	let tabElements: (HTMLButtonElement | null)[] = [];

	function selectByIndex(index: number): void {
		const tab = tabs[index];
		if (!tab) {
			return;
		}
		onSelect(tab.id);
		tabElements[index]?.focus();
	}

	function handleKeydown(event: KeyboardEvent, index: number): void {
		if (event.key === 'ArrowRight') {
			event.preventDefault();
			selectByIndex((index + 1) % tabs.length);
		} else if (event.key === 'ArrowLeft') {
			event.preventDefault();
			selectByIndex((index - 1 + tabs.length) % tabs.length);
		} else if (event.key === 'Home') {
			event.preventDefault();
			selectByIndex(0);
		} else if (event.key === 'End') {
			event.preventDefault();
			selectByIndex(tabs.length - 1);
		}
	}
</script>

<div role="tablist" class="flex gap-2 flex-wrap">
	{#each tabs as tab, index (tab.id)}
		<button
			bind:this={tabElements[index]}
			role="tab"
			id="tab-{tab.id}"
			aria-selected={active === tab.id}
			aria-controls="tabpanel-{tab.id}"
			tabindex={active === tab.id ? 0 : -1}
			onclick={() => onSelect(tab.id)}
			onkeydown={(event) => handleKeydown(event, index)}
			class="px-4 py-2 rounded-lg text-sm font-medium {active === tab.id
				? ACTIVE_CLASSES[color]
				: INACTIVE_CLASSES}"
		>
			{tab.label}
		</button>
	{/each}
</div>
