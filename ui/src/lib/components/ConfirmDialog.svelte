<script lang="ts">
	import Modal from './Modal.svelte';
	import type { ConfirmDestructiveOptions } from '$lib/confirm-dialog';

	let modal = $state<Modal | null>(null);
	let confirmButton = $state<HTMLButtonElement | null>(null);
	let resolvePending: ((result: boolean) => void) | null = null;

	let title = $state('Confirm action');
	let message = $state('');
	let confirmLabel = $state('Delete');
	let cancelLabel = $state('Cancel');
	let dangerous = $state(true);

	const titleId = 'confirm-dialog-title';
	const descriptionId = 'confirm-dialog-description';

	function handleKeydown(event: KeyboardEvent): void {
		if (event.key === 'Enter') {
			event.preventDefault();
			modal?.close('confirm');
		}
	}

	function handleClose(returnValue: string): void {
		const pending = resolvePending;
		resolvePending = null;
		// returnValue is '' (not 'confirm') when the dialog is dismissed some
		// other way, so the resolver always receives false in that edge case.
		pending?.(returnValue === 'confirm');
	}

	function cancel(): void {
		modal?.close('cancel');
	}

	function confirm(): void {
		modal?.close('confirm');
	}

	export function show(options: ConfirmDestructiveOptions): Promise<boolean> {
		if (!modal) {
			return Promise.resolve(false);
		}

		if (resolvePending) {
			// Capture and nullify before close() so the synchronous close event
			// (handleClose) cannot double-resolve the old promise.
			const stale = resolvePending;
			resolvePending = null;
			modal.close('cancel');
			stale(false);
		}

		title = options.title ?? 'Confirm action';
		message = options.message;
		confirmLabel = options.confirmLabel ?? 'Delete';
		cancelLabel = options.cancelLabel ?? 'Cancel';
		dangerous = options.dangerous ?? true;

		// Set resolvePending synchronously so click / keyboard handlers that
		// fire before the next tick can still resolve the promise.
		const promise = new Promise<boolean>((resolve) => {
			resolvePending = resolve;
		});

		modal.open();

		return promise;
	}
</script>

<Modal
	bind:this={modal}
	{title}
	role="alertdialog"
	{titleId}
	{descriptionId}
	initialFocus={() => confirmButton}
	onKeydown={handleKeydown}
	onClose={handleClose}
>
	{#snippet children()}
		<p id={descriptionId} class="text-sm text-slate-600 dark:text-slate-300">{message}</p>
	{/snippet}
	{#snippet footer()}
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
	{/snippet}
</Modal>
