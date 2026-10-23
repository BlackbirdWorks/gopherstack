<script lang="ts">
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { GCP_GHOST, GCP_INPUT, GCP_PRIMARY } from '#lib/gcp-action.ts';
	import { validateRows, type MetaRow } from '#lib/gcpstorage.ts';
	import { envToRows, validateEnvName, validateImage, validatePort, validateRunId, rowsToEnv, type EnvVar, type ServiceInfo } from '#lib/gcprun.ts';

	type Props = {
		busy: boolean;
		onCreate: (id: string, image: string, port: string, env: EnvVar[]) => void;
		onDeploy: (svc: ServiceInfo, image: string, env: EnvVar[]) => void;
	};
	let { busy, onCreate, onDeploy }: Props = $props();

	let modal = $state<Modal | null>(null);
	let target = $state<ServiceInfo | null>(null);
	let fId = $state('');
	let fImage = $state('');
	let fPort = $state('8080');
	let fEnv = $state<MetaRow[]>([]);
	let touched = $state(false);

	const idError = $derived(target ? null : validateRunId(fId.trim()));
	const imageError = $derived(validateImage(fImage));
	const portError = $derived(target ? null : validatePort(fPort));
	const envError = $derived(validateRows(fEnv, validateEnvName));
	const invalid = $derived(Boolean(idError || imageError || portError || envError));

	export function openCreate() {
		target = null;
		fId = '';
		fImage = '';
		fPort = '8080';
		fEnv = [];
		touched = false;
		modal?.open();
	}

	export function openDeploy(svc: ServiceInfo) {
		target = svc;
		fImage = svc.image;
		fEnv = envToRows(svc.env);
		touched = false;
		modal?.open();
	}

	export function close() {
		modal?.close('done');
	}

	function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (invalid) return;
		const env = rowsToEnv(fEnv);
		if (target) onDeploy(target, fImage, env);
		else onCreate(fId.trim(), fImage, fPort, env);
	}
</script>

<Modal bind:this={modal} title={target ? `Deploy new revision of ${target.id}` : 'Create service'} titleId="run-service-title">
	{#snippet children()}
		<form id="run-service-form" class="space-y-3" onsubmit={submit} novalidate>
			{#if !target}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Service name</span>
					<input class={GCP_INPUT} bind:value={fId} placeholder="hello" autocomplete="off" aria-label="Service name" />
					{#if touched && idError}<span class="block text-xs text-red-600 dark:text-red-400">{idError}</span>{/if}
				</label>
			{/if}
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Container image</span>
				<input class="{GCP_INPUT} font-mono" bind:value={fImage} placeholder="gcr.io/my-project/app:latest" autocomplete="off" aria-label="Container image" />
				{#if touched && imageError}<span class="block text-xs text-red-600 dark:text-red-400">{imageError}</span>{/if}
			</label>
			{#if !target}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Container port</span>
					<input class="{GCP_INPUT} font-mono" bind:value={fPort} inputmode="numeric" autocomplete="off" aria-label="Container port" />
					{#if touched && portError}<span class="block text-xs text-red-600 dark:text-red-400">{portError}</span>{/if}
				</label>
			{/if}
			<KeyValueEditor bind:rows={fEnv} label="Environment variables" testId="run-service-env" validateKey={validateEnvName} hideSave />
			{#if target}
				<p class="text-xs text-gray-500 dark:text-gray-400">A changed image or environment creates a new revision; traffic follows the latest revision unless pinned.</p>
			{/if}
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={GCP_GHOST} onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="run-service-form" class={GCP_PRIMARY} disabled={busy} data-testid="run-service-submit">{busy ? 'Working…' : target ? 'Deploy' : 'Create'}</button>
	{/snippet}
</Modal>
