<script lang="ts">
	import { onMount, onDestroy, tick } from 'svelte';

	let { title, src } = $props<{ title: string; src: string }>();
	let container: HTMLDivElement;
	let html = $state('');
	let loading = $state(true);
	let error = $state('');
	let observer: MutationObserver | undefined;

	async function loadContent() {
		loading = true;
		error = '';
		try {
			const separator = src.includes('?') ? '&' : '?';
			const res = await fetch(`${src}${separator}embed=1`);
			if (!res.ok) throw new Error(`HTTP ${res.status}`);
			html = await res.text();
		} catch (e: unknown) {
			error = (e as Error).message;
		} finally {
			loading = false;
		}
	}

	function activateScripts(el: HTMLElement) {
		const scripts = el.querySelectorAll('script');
		scripts.forEach((old) => {
			const s = document.createElement('script');
			for (const attr of old.attributes) {
				s.setAttribute(attr.name, attr.value);
			}
			s.textContent = old.textContent;
			old.replaceWith(s);
		});
	}

	function initHtmx(el: HTMLElement) {
		if (typeof window !== 'undefined' && (window as any).htmx) {
			(window as any).htmx.process(el);

			// Ensure all HTMX requests include embed=1 so responses skip the layout
			document.body.addEventListener('htmx:configRequest', ((e: CustomEvent) => {
				const detail = e.detail;
				if (detail && detail.parameters) {
					detail.parameters['embed'] = '1';
				}
			}) as EventListener);
		}
	}

	async function afterRender() {
		await tick();
		if (container) {
			activateScripts(container);
			initHtmx(container);
		}
	}

	$effect(() => {
		if (html && container) {
			afterRender();
		}
	});

	// Reload content when src changes
	$effect(() => {
		void src;
		loadContent();
	});

	onMount(async () => {
		// Load vendor dependencies that HTMX pages rely on
		const loadScript = (src: string): Promise<void> =>
			new Promise((resolve) => {
				if (document.querySelector(`script[src="${src}"]`)) {
					resolve();
					return;
				}
				const s = document.createElement('script');
				s.src = src;
				s.onload = () => resolve();
				document.head.appendChild(s);
			});

		const loadCSS = (href: string) => {
			if (document.querySelector(`link[href="${href}"]`)) return;
			const l = document.createElement('link');
			l.rel = 'stylesheet';
			l.href = href;
			document.head.appendChild(l);
		};

		loadCSS('/dashboard/static/vendor/flowbite.min.css');
		await loadScript('/dashboard/static/vendor/tailwind.min.js');

		// Configure Tailwind Play CDN for the embedded content
		if ((window as any).tailwind) {
			(window as any).tailwind.config = { darkMode: 'class' };
		}

		// Load HTMX if not present
		if (!(window as any).htmx) {
			await loadScript('https://unpkg.com/htmx.org@2.0.4');
		}

		if (container && html) initHtmx(container);

		// Intercept htmx navigation so it stays inside the container
		observer = new MutationObserver(() => {
			if (container) initHtmx(container);
		});
		if (container) {
			observer.observe(container, { childList: true, subtree: true });
		}
	});

	onDestroy(() => {
		observer?.disconnect();
	});
</script>

<section>
	{#if loading}
		<div class="flex items-center justify-center h-[80vh]">
			<div class="text-slate-400 dark:text-slate-500 flex flex-col items-center gap-3">
				<svg class="w-8 h-8 animate-spin" fill="none" viewBox="0 0 24 24">
					<circle class="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" stroke-width="4"></circle>
					<path class="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
				</svg>
				<span>Loading {title}...</span>
			</div>
		</div>
	{:else if error}
		<div class="flex items-center justify-center h-[80vh]">
			<div class="text-red-500 text-center">
				<p class="text-lg font-semibold">Failed to load {title}</p>
				<p class="text-sm mt-1">{error}</p>
				<button onclick={loadContent} class="mt-4 px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm hover:bg-indigo-700">Retry</button>
			</div>
		</div>
	{:else}
		<div bind:this={container} class="embedded-service-content">
			{@html html}
		</div>
	{/if}
</section>

<style>
	.embedded-service-content :global(script) {
		display: none;
	}
</style>
