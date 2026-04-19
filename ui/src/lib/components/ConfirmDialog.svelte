<script lang="ts">
	import { tick } from 'svelte';
	import type { ConfirmDestructiveOptions } from '$lib/confirm-dialog';

	let dialogElement = $state<HTMLDialogElement | null>(null);
	let confirmButton = $state<HTMLButtonElement | null>(null);
	let activeElementBeforeOpen: HTMLElement | null = null;
	let resolvePending: ((result: boolean) => void) | null = null;

	let title = $state('Confirm action');
	let message = $state('');
	let confirmLabel = $state('Delete');
	let cancelLabel = $state('Cancel');
	let dangerous = $state(true);

	const titleId = 'confirm-dialog-title';
	const descriptionId = 'confirm-dialog-description';

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
		if (!dialogElement) {
			return;
		}

		if (event.key === 'Enter') {
			event.preventDefault();
			dialogElement.close('confirm');
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
		if (!dialogElement) {
			resolvePending?.(false);
			resolvePending = null;
			activeElementBeforeOpen?.focus();
			return;
		}

		const pending = resolvePending;
		resolvePending = null;
		pending?.(dialogElement.returnValue === 'confirm');
		activeElementBeforeOpen?.focus();
	}

	function cancel(): void {
		dialogElement?.close('cancel');
	}

	function confirm(): void {
		dialogElement?.close('confirm');
	}

	export function show(options: ConfirmDestructiveOptions): Promise<boolean> {
		if (!dialogElement) {
			return Promise.resolve(false);
		}

		if (resolvePending) {
			dialogElement.close('cancel');
		}

		title = options.title ?? 'Confirm action';
		message = options.message;
		confirmLabel = options.confirmLabel ?? 'Delete';
		cancelLabel = options.cancelLabel ?? 'Cancel';
		dangerous = options.dangerous ?? true;
		activeElementBeforeOpen =
			document.activeElement instanceof HTMLElement ? document.activeElement : null;

		// Set resolvePending synchronously so click / keyboard handlers that
		// fire before the next tick can still resolve the promise.
		const promise = new Promise<boolean>((resolve) => {
			resolvePending = resolve;
		});

		dialogElement.showModal();
		// Focus the primary action after Svelte flushes the reactive updates.
		void tick().then(() => {
			(confirmButton ?? focusables()[0])?.focus();
		});

		return promise;
	}
</script>

<dialog
	bind:this={dialogElement}
	role="alertdialog"
	aria-labelledby={titleId}
	aria-describedby={descriptionId}
	onkeydown={handleKeydown}
	oncancel={handleCancel}
	onclose={handleClose}
	class="rounded-2xl border border-slate-200 bg-white p-0 text-slate-900 shadow-2xl backdrop:bg-slate-950/60 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
>
	<div class="w-full max-w-md space-y-4 p-6">
		<h2 id={titleId} class="text-lg font-semibold tracking-tight">{title}</h2>
		<p id={descriptionId} class="text-sm text-slate-600 dark:text-slate-300">{message}</p>
		<div class="flex justify-end gap-2 pt-2">
			<button
				type="button"
				onclick={cancel}
				class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>
				{cancelLabel}
			</button>
			<button
				bind:this={confirmButton}
				type="button"
				onclick={confirm}
				class={`rounded-lg px-4 py-2 text-sm font-semibold text-white transition ${
					dangerous
						? 'bg-rose-600 hover:bg-rose-700 dark:bg-rose-500 dark:hover:bg-rose-600'
						: 'bg-indigo-600 hover:bg-indigo-700 dark:bg-indigo-500 dark:hover:bg-indigo-600'
				}`}
			>
				{confirmLabel}
			</button>
		</div>
	</div>
</dialog>
