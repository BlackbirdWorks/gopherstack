<script lang="ts">
	// Generic native <dialog>-based modal: focus trap, Escape-to-close, and
	// restore-focus-on-close, factored out of ConfirmDialog.svelte so other
	// modals (of which there are ~20 hand-rolled implementations across the
	// route pages) can be built on the same primitive.
	import { tick } from 'svelte';
	import type { Snippet } from 'svelte';

	type Props = {
		title: string;
		children: Snippet;
		headerAction?: Snippet;
		footer?: Snippet;
		titleId?: string;
		descriptionId?: string;
		role?: 'dialog' | 'alertdialog';
		/** Element to focus once the dialog opens. Defaults to the first focusable element. */
		initialFocus?: () => HTMLElement | null;
		/** Called before Modal's own Escape/Tab handling; call event.preventDefault() to suppress it. */
		onKeydown?: (event: KeyboardEvent) => void;
		/** Called when the dialog closes, with the native dialog's returnValue. */
		onClose?: (returnValue: string) => void;
	};

	let {
		title,
		children,
		headerAction,
		footer,
		titleId = 'modal-title',
		descriptionId,
		role = 'dialog',
		initialFocus,
		onKeydown,
		onClose
	}: Props = $props();

	let dialogElement = $state<HTMLDialogElement | null>(null);
	let activeElementBeforeOpen: HTMLElement | null = null;

	function focusables(): HTMLElement[] {
		if (!dialogElement) {
			return [];
		}

		return Array.from(
			dialogElement.querySelectorAll<HTMLElement>(
				'button,[href],input,select,textarea,[tabindex]:not([tabindex="-1"])'
			)
		).filter((element) => !element.hasAttribute('disabled'));
	}

	function handleKeydown(event: KeyboardEvent): void {
		onKeydown?.(event);
		if (event.defaultPrevented) {
			return;
		}

		if (!dialogElement) {
			return;
		}

		if (event.key === 'Escape') {
			event.preventDefault();
			dialogElement.close('cancel');
			return;
		}

		if (event.key !== 'Tab') {
			return;
		}

		const elements = focusables();
		if (elements.length === 0) {
			return;
		}

		const first = elements[0];
		const last = elements.at(-1) ?? first;
		const target = event.target as HTMLElement;

		if (event.shiftKey && target === first) {
			event.preventDefault();
			last.focus();
			return;
		}

		if (!event.shiftKey && target === last) {
			event.preventDefault();
			first.focus();
		}
	}

	function handleCancel(event: Event): void {
		if (!dialogElement) {
			return;
		}

		event.preventDefault();
		dialogElement.close('cancel');
	}

	function handleClose(): void {
		onClose?.(dialogElement?.returnValue ?? '');
		activeElementBeforeOpen?.focus();
	}

	export function open(): void {
		if (!dialogElement) {
			return;
		}

		activeElementBeforeOpen =
			document.activeElement instanceof HTMLElement ? document.activeElement : null;

		dialogElement.showModal();
		// Focus after Svelte flushes the reactive updates driven by the caller
		// (e.g. title/message state set just before calling open()).
		void tick().then(() => {
			(initialFocus?.() ?? focusables()[0])?.focus();
		});
	}

	export function close(result = 'cancel'): void {
		dialogElement?.close(result);
	}
</script>

<dialog
	bind:this={dialogElement}
	{role}
	aria-labelledby={titleId}
	aria-describedby={descriptionId}
	onkeydown={handleKeydown}
	oncancel={handleCancel}
	onclose={handleClose}
	class="rounded-2xl border border-slate-200 bg-white p-0 text-slate-900 shadow-2xl backdrop:bg-slate-950/60 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
>
	<div class="w-full max-w-md space-y-4 p-6">
		<div class="flex items-center justify-between">
			<h2 id={titleId} class="text-lg font-semibold tracking-tight">{title}</h2>
			{#if headerAction}
				{@render headerAction()}
			{/if}
		</div>
		{@render children()}
		{#if footer}
			<div class="flex justify-end gap-2 pt-2">
				{@render footer()}
			</div>
		{/if}
	</div>
</dialog>
