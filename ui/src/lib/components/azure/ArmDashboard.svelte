<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Layers, Plus, Trash2, KeyRound, Copy, X, RefreshCw, Boxes, Database, Network } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		ArmError,
		DEFAULT_LOCATION,
		LOCATIONS,
		SERVICE_BUS_SKUS,
		STORAGE_SKUS,
		deleteNamespace,
		deleteResourceGroup,
		deleteStorageAccount,
		formatTags,
		getSubscriptionId,
		listNamespaceKeys,
		listNamespaces,
		listResourceGroups,
		listStorageAccounts,
		listStorageKeys,
		parseArmId,
		parseTags,
		putNamespace,
		putResourceGroup,
		putStorageAccount,
		resourcesInGroup,
		shortType,
		validateNamespaceName,
		validateResourceGroupName,
		validateStorageAccountName,
		type ArmResource,
		type ServiceBusKeys,
		type StorageKey
	} from '#lib/azurearm.ts';

	type TabId = 'groups' | 'storage' | 'servicebus';
	const tabs = [
		{ id: 'groups', label: 'Resource Groups' },
		{ id: 'storage', label: 'Storage Accounts' },
		{ id: 'servicebus', label: 'Service Bus' }
	];

	let tab = $state<TabId>('groups');
	let subscription = $state('');
	let loading = $state(true);
	let loadError = $state('');
	let groups = $state<ArmResource[]>([]);
	let accounts = $state<ArmResource[]>([]);
	let namespaces = $state<ArmResource[]>([]);
	let groupFilter = $state('');
	let selectedId = $state<string | null>(null);
	let storageKeys = $state<StorageKey[] | null>(null);
	let busKeys = $state<ServiceBusKeys | null>(null);
	let loadingKeys = $state(false);

	let modal = $state<Modal | null>(null);
	let creating = $state(false);
	let formName = $state('');
	let formGroup = $state('');
	let formLocation = $state(DEFAULT_LOCATION);
	let formSku = $state('Standard_LRS');
	let formKind = $state('StorageV2');
	let formTags = $state('');
	let touched = $state(false);

	function describe(e: unknown): string {
		if (e instanceof ArmError) return e.code ? `${e.code}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	const selected = $derived.by(() => {
		const pool = tab === 'groups' ? groups : tab === 'storage' ? accounts : namespaces;
		return pool.find((r) => r.id === selectedId) ?? null;
	});
	const visibleAccounts = $derived(groupFilter ? resourcesInGroup(accounts, groupFilter) : accounts);
	const visibleNamespaces = $derived(groupFilter ? resourcesInGroup(namespaces, groupFilter) : namespaces);
	const nameError = $derived(
		tab === 'groups'
			? validateResourceGroupName(formName)
			: tab === 'storage'
				? validateStorageAccountName(formName)
				: validateNamespaceName(formName)
	);
	const groupError = $derived(tab !== 'groups' && !formGroup ? 'Choose a resource group' : null);
	const tagError = $derived.by(() => {
		try {
			parseTags(formTags);
			return null;
		} catch (e) {
			return describe(e);
		}
	});
	const formInvalid = $derived(Boolean(nameError ?? groupError ?? tagError));

	async function load() {
		loading = true;
		loadError = '';
		try {
			if (!subscription) subscription = await getSubscriptionId();
			[groups, accounts, namespaces] = await Promise.all([
				listResourceGroups(subscription),
				listStorageAccounts(subscription),
				listNamespaces(subscription)
			]);
			if (selectedId && !selected) selectedId = null;
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load Azure resources: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	function selectTab(id: string) {
		tab = id as TabId;
		select(null);
	}

	function select(id: string | null) {
		selectedId = id;
		storageKeys = null;
		busKeys = null;
	}

	function openCreate() {
		formName = '';
		formTags = '';
		touched = false;
		formLocation = DEFAULT_LOCATION;
		formSku = tab === 'storage' ? 'Standard_LRS' : 'Standard';
		formKind = 'StorageV2';
		formGroup = groupFilter || groups[0]?.name || '';
		modal?.open();
	}

	async function submit(event: Event) {
		event.preventDefault();
		touched = true;
		if (formInvalid) return;
		creating = true;
		const name = formName.trim();
		try {
			const tags = parseTags(formTags);
			if (tab === 'groups') {
				await putResourceGroup(subscription, name, formLocation, tags);
			} else if (tab === 'storage') {
				await putStorageAccount(subscription, {
					name,
					resourceGroup: formGroup,
					location: formLocation,
					sku: formSku,
					kind: formKind,
					tags
				});
			} else {
				await putNamespace(subscription, {
					name,
					resourceGroup: formGroup,
					location: formLocation,
					sku: formSku,
					tags
				});
			}
			toast.success(`Created ${name}`);
			modal?.close('created');
			await load();
		} catch (e) {
			toast.error(`Failed to create ${name}: ${describe(e)}`);
		} finally {
			creating = false;
		}
	}

	async function remove(r: ArmResource) {
		const what = tab === 'groups' ? 'resource group' : tab === 'storage' ? 'storage account' : 'Service Bus namespace';
		const extra = tab === 'groups' ? ' All resources inside it will be deleted too.' : '';
		if (!(await confirmDestructive({ title: `Delete ${what}`, message: `Delete ${what} "${r.name}"?${extra} This cannot be undone.` }))) return;
		try {
			if (tab === 'groups') await deleteResourceGroup(subscription, r.name);
			else if (tab === 'storage') await deleteStorageAccount(subscription, r);
			else await deleteNamespace(subscription, r);
			toast.success(`Deleted ${r.name}`);
			if (selectedId === r.id) select(null);
			await load();
		} catch (e) {
			toast.error(`Failed to delete ${r.name}: ${describe(e)}`);
		}
	}

	async function revealKeys(r: ArmResource) {
		loadingKeys = true;
		try {
			if (tab === 'storage') storageKeys = await listStorageKeys(subscription, r);
			else busKeys = await listNamespaceKeys(subscription, r);
		} catch (e) {
			toast.error(`Failed to list keys: ${describe(e)}`);
		} finally {
			loadingKeys = false;
		}
	}

	async function copy(text: string | undefined) {
		if (!text) return;
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	function groupOf(r: ArmResource): string {
		return parseArmId(r.id).resourceGroup;
	}

	function endpoints(r: ArmResource): [string, string][] {
		const props = r.properties ?? {};
		const out: [string, string][] = [];
		const primary = props.primaryEndpoints as Record<string, string> | undefined;
		for (const [k, v] of Object.entries(primary ?? {})) out.push([k, v]);
		if (typeof props.serviceBusEndpoint === 'string') out.push(['serviceBus', props.serviceBusEndpoint]);
		return out;
	}

	const groupContents = $derived(
		selected && tab === 'groups'
			? [...resourcesInGroup(accounts, selected.name), ...resourcesInGroup(namespaces, selected.name)]
			: []
	);

	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const badge = 'rounded-full px-2 py-0.5 text-xs font-medium bg-blue-100 text-blue-700 dark:bg-blue-900 dark:text-blue-300';

	onMount(load);
</script>

{#snippet field(label: string, error: string | null, control: import('svelte').Snippet)}
	<label class="block space-y-1 text-sm">
		<span class="font-medium text-gray-700 dark:text-gray-300">{label}</span>
		{@render control()}
		{#if touched && error}<span class="block text-xs text-red-600 dark:text-red-400">{error}</span>{/if}
	</label>
{/snippet}

{#snippet table(rows: ArmResource[], emptyTitle: string, emptyHint: string, showGroup: boolean)}
	{#if loading}
		<div class="flex justify-center py-12"><RefreshCw class="h-8 w-8 animate-spin text-gray-400" /></div>
	{:else if rows.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-gray-500 dark:text-gray-400" data-testid="arm-empty">
			<Layers class="h-12 w-12 mb-3 opacity-30" />
			<p>{emptyTitle}</p>
			<p class="text-sm">{emptyHint}</p>
		</div>
	{:else}
		<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
			<table class="w-full text-sm">
				<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400">
					<tr>
						<th class="px-4 py-3 font-medium">Name</th>
						{#if showGroup}<th class="px-4 py-3 font-medium">Resource group</th>{/if}
						<th class="px-4 py-3 font-medium">Location</th>
						<th class="px-4 py-3 font-medium">{tab === 'groups' ? 'Tags' : 'SKU'}</th>
						<th class="px-4 py-3 text-right font-medium">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
					{#each rows as r (r.id)}
						<tr
							class="cursor-pointer hover:bg-gray-50 dark:hover:bg-slate-800/50 {selectedId === r.id ? 'bg-blue-50 dark:bg-blue-950/40' : ''}"
							data-testid="arm-row"
							onclick={() => select(r.id)}
						>
							<td class="px-4 py-3 font-medium text-gray-900 dark:text-gray-100">{r.name}</td>
							{#if showGroup}<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{groupOf(r)}</td>{/if}
							<td class="px-4 py-3 text-gray-600 dark:text-gray-400">{r.location ?? '—'}</td>
							<td class="px-4 py-3 text-gray-600 dark:text-gray-400">
								{#if tab === 'groups'}{formatTags(r.tags) || '—'}{:else}<span class={badge}>{r.sku?.name ?? '—'}</span>{/if}
							</td>
							<td class="px-4 py-3 text-right">
								<button
									class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
									title="Delete {r.name}"
									aria-label="Delete {r.name}"
									onclick={(e) => { e.stopPropagation(); void remove(r); }}
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}
{/snippet}

<div class="space-y-6 pb-20" data-testid="arm-dashboard">
	<PageHeader
		icon={Layers}
		title="Resource Manager"
		description="Azure Resource Manager: resource groups, storage accounts and Service Bus namespaces"
		color="blue"
		onRefresh={load}
	>
		{#snippet actions()}
			<button
				class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50"
				onclick={openCreate}
				disabled={loading || Boolean(loadError)}
			>
				<Plus class="h-4 w-4" />
				{tab === 'groups' ? 'New resource group' : tab === 'storage' ? 'New storage account' : 'New namespace'}
			</button>
		{/snippet}
	</PageHeader>

	{#if subscription}
		<p class="text-xs text-gray-500 dark:text-gray-400">
			Subscription <span class="font-mono">{subscription}</span>
		</p>
	{/if}

	<Tabs {tabs} active={tab} onSelect={selectTab} color="blue" />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="arm-error">
			<p class="font-medium">Could not reach Azure Resource Manager</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={load}>Retry</button>
		</div>
	{:else}
		{#if tab !== 'groups'}
			<div class="flex items-center gap-2 text-sm">
				<label for="arm-group-filter" class="text-gray-600 dark:text-gray-400">Resource group</label>
				<select id="arm-group-filter" bind:value={groupFilter} class="{input} !w-auto">
					<option value="">All</option>
					{#each groups as g (g.id)}<option value={g.name}>{g.name}</option>{/each}
				</select>
			</div>
		{/if}

		{#if tab === 'groups'}
			{@render table(groups, 'No resource groups', 'Create a resource group to get started', false)}
		{:else if tab === 'storage'}
			{@render table(visibleAccounts, 'No storage accounts', 'Create a storage account in a resource group', true)}
		{:else}
			{@render table(visibleNamespaces, 'No Service Bus namespaces', 'Create a namespace in a resource group', true)}
		{/if}

		{#if selected}
			<section class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-4" data-testid="arm-detail" aria-label="Details for {selected.name}">
				<div class="flex items-start justify-between gap-4">
					<div>
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selected.name}</h2>
						<p class="text-xs text-gray-500 dark:text-gray-400">{selected.type}</p>
					</div>
					<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => select(null)} aria-label="Close details">
						<X class="h-4 w-4" />
					</button>
				</div>

				<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
					<dt class="font-medium text-gray-500 dark:text-gray-400">Resource ID</dt>
					<dd class="font-mono text-xs break-all text-gray-800 dark:text-gray-200">{selected.id}</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">Location</dt>
					<dd class="text-gray-800 dark:text-gray-200">{selected.location ?? '—'}</dd>
					{#if tab !== 'groups'}
						<dt class="font-medium text-gray-500 dark:text-gray-400">Resource group</dt>
						<dd class="text-gray-800 dark:text-gray-200">{groupOf(selected)}</dd>
						<dt class="font-medium text-gray-500 dark:text-gray-400">SKU</dt>
						<dd class="text-gray-800 dark:text-gray-200">{selected.sku?.name ?? '—'}{selected.kind ? ` (${selected.kind})` : ''}</dd>
					{/if}
					<dt class="font-medium text-gray-500 dark:text-gray-400">Provisioning</dt>
					<dd class="text-gray-800 dark:text-gray-200">{String(selected.properties?.provisioningState ?? '—')}</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">Tags</dt>
					<dd class="text-gray-800 dark:text-gray-200">{formatTags(selected.tags) || '—'}</dd>
					{#each endpoints(selected) as [label, url] (label)}
						<dt class="font-medium text-gray-500 dark:text-gray-400">{label} endpoint</dt>
						<dd class="flex items-center gap-2 font-mono text-xs text-gray-800 dark:text-gray-200">
							{url}
							<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy {label} endpoint" onclick={() => copy(url)}><Copy class="h-3.5 w-3.5" /></button>
						</dd>
					{/each}
				</dl>

				{#if tab === 'groups'}
					<div>
						<h3 class="mb-2 flex items-center gap-2 text-sm font-semibold text-gray-700 dark:text-gray-300"><Boxes class="h-4 w-4" /> Resources in this group ({groupContents.length})</h3>
						{#if groupContents.length === 0}
							<p class="text-sm text-gray-500 dark:text-gray-400" data-testid="arm-group-empty">This resource group is empty.</p>
						{:else}
							<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-md border border-gray-200 dark:border-gray-700 text-sm" data-testid="arm-group-resources">
								{#each groupContents as r (r.id)}
									<li class="flex items-center justify-between px-3 py-2">
										<span class="flex items-center gap-2 text-gray-800 dark:text-gray-200">
											{#if r.type.includes('Storage')}<Database class="h-4 w-4 text-sky-500" />{:else}<Network class="h-4 w-4 text-violet-500" />{/if}
											{r.name}
										</span>
										<span class="text-xs text-gray-500 dark:text-gray-400">{shortType(r.type)}</span>
									</li>
								{/each}
							</ul>
						{/if}
					</div>
				{:else}
					<div class="space-y-2">
						<button
							class="flex items-center gap-2 rounded-md border border-gray-200 dark:border-gray-700 px-3 py-1.5 text-sm hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50"
							onclick={() => revealKeys(selected)}
							disabled={loadingKeys}
						>
							<KeyRound class="h-4 w-4" /> {storageKeys || busKeys ? 'Refresh' : 'Show'} access keys
						</button>
						{#if storageKeys}
							<ul class="space-y-1 text-xs" data-testid="arm-keys">
								{#each storageKeys as k (k.keyName)}
									<li class="flex items-center gap-2">
										<span class="font-medium w-12">{k.keyName}</span>
										<code class="break-all rounded bg-gray-100 dark:bg-slate-800 px-2 py-1">{k.value}</code>
										<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy {k.keyName}" onclick={() => copy(k.value)}><Copy class="h-3.5 w-3.5" /></button>
									</li>
								{/each}
							</ul>
						{/if}
						{#if busKeys}
							<ul class="space-y-1 text-xs" data-testid="arm-keys">
								{#each [['Primary key', busKeys.primaryKey], ['Primary connection string', busKeys.primaryConnectionString]] as [label, value] (label)}
									<li class="flex items-center gap-2">
										<span class="font-medium w-40 shrink-0">{label}</span>
										<code class="break-all rounded bg-gray-100 dark:bg-slate-800 px-2 py-1">{value}</code>
										<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy {label}" onclick={() => copy(value)}><Copy class="h-3.5 w-3.5" /></button>
									</li>
								{/each}
							</ul>
						{/if}
					</div>
				{/if}
			</section>
		{/if}
	{/if}
</div>

<Modal bind:this={modal} title={tab === 'groups' ? 'New resource group' : tab === 'storage' ? 'New storage account' : 'New Service Bus namespace'} titleId="arm-create-title">
	{#snippet children()}
		<form id="arm-create-form" class="space-y-3" onsubmit={submit} novalidate>
			{#snippet nameControl()}
				<input class={input} bind:value={formName} placeholder={tab === 'storage' ? 'mystorageacct' : tab === 'groups' ? 'my-group' : 'my-namespace'} autocomplete="off" aria-label="Name" />
			{/snippet}
			{@render field('Name', nameError, nameControl)}

			{#if tab !== 'groups'}
				{#snippet groupControl()}
					<select class={input} bind:value={formGroup} aria-label="Resource group">
						<option value="" disabled>Select…</option>
						{#each groups as g (g.id)}<option value={g.name}>{g.name}</option>{/each}
					</select>
				{/snippet}
				{@render field('Resource group', groupError, groupControl)}
			{/if}

			{#snippet locControl()}
				<select class={input} bind:value={formLocation} aria-label="Location">
					{#each LOCATIONS as l (l)}<option value={l}>{l}</option>{/each}
				</select>
			{/snippet}
			{@render field('Location', null, locControl)}

			{#if tab !== 'groups'}
				{#snippet skuControl()}
					<select class={input} bind:value={formSku} aria-label="SKU">
						{#each tab === 'storage' ? STORAGE_SKUS : SERVICE_BUS_SKUS as s (s)}<option value={s}>{s}</option>{/each}
					</select>
				{/snippet}
				{@render field('SKU', null, skuControl)}
			{/if}

			{#if tab === 'storage'}
				{#snippet kindControl()}
					<select class={input} bind:value={formKind} aria-label="Kind">
						{#each ['StorageV2', 'Storage', 'BlobStorage', 'BlockBlobStorage'] as k (k)}<option value={k}>{k}</option>{/each}
					</select>
				{/snippet}
				{@render field('Kind', null, kindControl)}
			{/if}

			{#snippet tagControl()}
				<input class={input} bind:value={formTags} placeholder="env=dev, team=core" autocomplete="off" aria-label="Tags" />
			{/snippet}
			{@render field('Tags (key=value, comma separated)', tagError, tagControl)}
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="arm-create-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={creating}>
			{creating ? 'Creating…' : 'Create'}
		</button>
	{/snippet}
</Modal>
