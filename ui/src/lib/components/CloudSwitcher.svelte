<script lang="ts">
	import { goto } from '$app/navigation';
	import { page } from '$app/state';
	import { tick } from 'svelte';
	import { CLOUDS, withCloud, type Cloud } from '#lib/cloud.ts';
	import { currentCloud, setCloud } from '#lib/cloud.svelte.ts';

	let open = $state(false);
	let trigger = $state<HTMLButtonElement | null>(null);
	let list = $state<HTMLUListElement | null>(null);
	const selected = $derived(CLOUDS.find((c) => c.id === currentCloud()) ?? CLOUDS[0]);

	function choose(id: Cloud): void {
		close(true);
		setCloud(id);
		// Land on the dashboard home: the current page may belong to another cloud.
		goto(withCloud(new URL('/dashboard', page.url.origin), id), { replaceState: false });
	}

	function options(): HTMLButtonElement[] {
		return list ? Array.from(list.querySelectorAll<HTMLButtonElement>('button[role="option"]')) : [];
	}

	async function openList(): Promise<void> {
		open = true;
		await tick();
		const all = options();
		(all.find((b) => b.dataset.cloud === selected.id) ?? all[0])?.focus();
	}

	function close(refocus: boolean): void {
		open = false;
		if (refocus) trigger?.focus();
	}

	function move(step: number): void {
		const all = options();
		if (all.length === 0) return;
		const at = all.indexOf(document.activeElement as HTMLButtonElement);
		all[(at + step + all.length) % all.length].focus();
	}

	function onTriggerKeydown(e: KeyboardEvent): void {
		if (e.key === 'ArrowDown' || e.key === 'ArrowUp') {
			e.preventDefault();
			void openList();
		}
	}

	function onListKeydown(e: KeyboardEvent): void {
		const steps: Record<string, () => void> = {
			ArrowDown: () => move(1),
			ArrowUp: () => move(-1),
			Home: () => options()[0]?.focus(),
			End: () => options().at(-1)?.focus(),
			Escape: () => close(true),
			Tab: () => close(false)
		};
		const step = steps[e.key];
		if (!step) return;
		if (e.key !== 'Tab') e.preventDefault();
		step();
	}

	function onWindowClick(e: MouseEvent): void {
		if (open && !(e.target as HTMLElement).closest('#cloud-switcher')) open = false;
	}

	function onWindowKeydown(e: KeyboardEvent): void {
		if (e.key === 'Escape' && open && !list?.contains(document.activeElement)) close(false);
	}
</script>

<svelte:window onclick={onWindowClick} onkeydown={onWindowKeydown} />

<div id="cloud-switcher" class="relative">
	<button
		id="cloud-selector"
		bind:this={trigger}
		type="button"
		aria-haspopup="listbox"
		aria-expanded={open}
		aria-label="Cloud: {selected.label}"
		onclick={() => (open ? close(false) : void openList())}
		onkeydown={onTriggerKeydown}
		class="inline-flex items-center gap-1.5 h-9 px-3 text-sm font-semibold text-slate-700 rounded-lg border border-slate-200 hover:bg-slate-100 focus:outline-none focus:ring-2 focus:ring-slate-200 dark:text-slate-200 dark:border-white/10 dark:hover:bg-slate-700 transition-colors"
	>
		<svg class="w-4 h-4 text-indigo-500" fill="none" stroke="currentColor" stroke-width="2" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" d="M3 15a4 4 0 004 4h9a5 5 0 10-.1-9.999 5.002 5.002 0 10-9.78 2.096A4.001 4.001 0 003 15z" /></svg>
		<span>{selected.label}</span>
		<svg class="w-3 h-3 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg>
	</button>
	{#if open}
		<ul bind:this={list} role="listbox" aria-label="Cloud" onkeydown={onListKeydown} class="absolute left-0 mt-1 w-36 py-1 text-sm bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg shadow-lg z-50">
			{#each CLOUDS as c (c.id)}
				<li role="presentation">
					<button
						type="button"
						role="option"
						aria-selected={c.id === selected.id}
						data-cloud={c.id}
						onclick={() => choose(c.id)}
						class="w-full text-left px-4 py-2 hover:bg-slate-100 focus:bg-slate-100 focus:outline-none dark:hover:bg-slate-700 dark:focus:bg-slate-700 text-slate-700 dark:text-slate-200 {c.id === selected.id ? 'bg-slate-50 dark:bg-slate-700 font-semibold text-indigo-600 dark:text-indigo-400' : ''}"
					>{c.label}</button>
				</li>
			{/each}
		</ul>
	{/if}
</div>
