<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { SlidersHorizontal, Plus, Trash2, Inbox, Lock } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt, GCP_INPUT, GCP_MINI, GCP_PRIMARY, GCP_GHOST } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		NULL_LABEL,
		deleteKeyValue,
		listKeyValues,
		listLabels,
		listStores,
		putKeyValue,
		validateKey,
		validateLabel,
		type AcKeyValue
	} from '#lib/azureappconfig.ts';

	let stores = $state<string[]>([]);
	let store = $state('');
	let items = $state<AcKeyValue[]>([]);
	let labels = $state<string[]>([]);
	let keyFilter = $state('');
	let labelFilter = $state('');
	let loading = $state(true);
	let loadError = $state('');

	let modal = $state<Modal | null>(null);
	let fKey = $state('');
	let fLabel = $state('');
	let fValue = $state('');
	let fType = $state('');
	let fEditing = $state(false);
	let fTouched = $state(false);
	let saving = $state(false);

	const formError = $derived(validateKey(fKey) || validateLabel(fLabel));

	async function loadItems() {
		if (!store) {
			items = [];
			return;
		}
		[items, labels] = await Promise.all([listKeyValues(store, keyFilter.trim(), labelFilter), listLabels(store)]);
	}

	async function load() {
		loading = true;
		loadError = '';
		try {
			stores = await listStores();
			if (!stores.includes(store)) store = stores[0] ?? '';
			await loadItems();
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load App Configuration: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function refilter() {
		await attempt(loadItems, '', 'Failed to filter key-values');
	}

	async function pickStore(name: string) {
		store = name;
		keyFilter = '';
		labelFilter = '';
		await refilter();
	}

	function openEditor(kv?: AcKeyValue) {
		fKey = kv?.key ?? '';
		fLabel = kv?.label ?? '';
		fValue = kv?.value ?? '';
		fType = kv?.contentType ?? '';
		fEditing = Boolean(kv);
		fTouched = false;
		modal?.open();
	}

	async function save(e: Event) {
		e.preventDefault();
		fTouched = true;
		if (formError) return;
		saving = true;
		const ok = await attempt(
			() => putKeyValue(store, { key: fKey, label: fLabel, value: fValue, contentType: fType }),
			`Saved ${fKey}`,
			`Failed to save ${fKey}`
		);
		saving = false;
		if (ok) {
			modal?.close('saved');
			await refilter();
		}
	}

	async function remove(kv: AcKeyValue) {
		if (!(await confirmDestructive({ title: 'Delete key-value', message: `Delete "${kv.key}"${kv.label ? ` (label ${kv.label})` : ''}?` }))) return;
		if (await attempt(() => deleteKeyValue(store, kv.key, kv.label), `Deleted ${kv.key}`, `Failed to delete ${kv.key}`)) await refilter();
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="appconfig-dashboard">
	<PageHeader icon={SlidersHorizontal} title="App Configuration" description="Azure App Configuration: key-values and labels" color="teal" onRefresh={load}>
		{#snippet actions()}
			<button class={GCP_PRIMARY} onclick={() => openEditor()} disabled={loading || !store} data-testid="appconfig-new"><Plus class="mr-1 inline h-4 w-4" />New key-value</button>
		{/snippet}
	</PageHeader>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="appconfig-error">
			<p class="font-medium">Could not reach Azure App Configuration</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if stores.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="appconfig-empty">
			<Inbox class="h-10 w-10 mb-3 opacity-30" />
			<p>No configuration stores. Create one with the Azure CLI, Terraform or ARM.</p>
		</div>
	{:else}
		<form class="flex flex-wrap items-end gap-4" onsubmit={(e) => { e.preventDefault(); void refilter(); }}>
			<label class="text-sm">Store
				<select class={GCP_INPUT} value={store} onchange={(e) => pickStore(e.currentTarget.value)} data-testid="appconfig-store">
					{#each stores as s (s)}<option value={s}>{s}</option>{/each}
				</select>
			</label>
			<label class="text-sm">Key filter<input class={GCP_INPUT} bind:value={keyFilter} placeholder="e.g. app/*" data-testid="appconfig-key-filter" /></label>
			<label class="text-sm">Label filter
				<select class={GCP_INPUT} bind:value={labelFilter} onchange={refilter} data-testid="appconfig-label-filter">
					<option value="">All labels</option>
					<option value={NULL_LABEL}>(no label)</option>
					{#each labels as l (l)}<option value={l}>{l}</option>{/each}
				</select>
			</label>
			<button type="submit" class={GCP_GHOST}>Filter</button>
		</form>

		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="appconfig-table">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Key</th><th class="px-4 py-2">Label</th><th class="px-4 py-2">Value</th><th class="px-4 py-2">Content type</th><th class="px-4 py-2"></th></tr></thead>
				<tbody>
					{#each items as kv (kv.key + '\0' + kv.label)}
						<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="appconfig-row">
							<td class="px-4 py-2 font-mono">{kv.key}{#if kv.locked}<Lock class="ml-1 inline h-3 w-3" />{/if}</td>
							<td class="px-4 py-2">{kv.label || '(none)'}</td>
							<td class="max-w-xs truncate px-4 py-2 font-mono" title={kv.value}>{kv.value}</td>
							<td class="px-4 py-2">{kv.contentType || '-'}</td>
							<td class="space-x-2 px-4 py-2 text-right">
								<button class={GCP_MINI} onclick={() => openEditor(kv)} disabled={kv.locked}>Edit</button>
								<button class={GCP_MINI} onclick={() => remove(kv)} disabled={kv.locked} aria-label="Delete {kv.key}"><Trash2 class="inline h-3 w-3" /></button>
							</td>
						</tr>
					{:else}
						<tr><td class="px-4 py-6 text-center text-gray-500" colspan="5">No key-values match.</td></tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	<Modal title={fEditing ? 'Edit key-value' : 'New key-value'} bind:this={modal}>
		<form class="space-y-3" onsubmit={save} data-testid="appconfig-form">
			<label class="block text-sm">Key<input class={GCP_INPUT} bind:value={fKey} readonly={fEditing} data-testid="appconfig-key" /></label>
			<label class="block text-sm">Label<input class={GCP_INPUT} bind:value={fLabel} readonly={fEditing} placeholder="optional" data-testid="appconfig-label" /></label>
			<label class="block text-sm">Value<textarea class="{GCP_INPUT} font-mono" rows="4" bind:value={fValue} data-testid="appconfig-value"></textarea></label>
			<label class="block text-sm">Content type<input class={GCP_INPUT} bind:value={fType} placeholder="optional" data-testid="appconfig-type" /></label>
			{#if fTouched && formError}<p class="text-sm text-red-600">{formError}</p>{/if}
			<button type="submit" class={GCP_PRIMARY} disabled={saving} data-testid="appconfig-save">Save</button>
		</form>
	</Modal>
</div>
