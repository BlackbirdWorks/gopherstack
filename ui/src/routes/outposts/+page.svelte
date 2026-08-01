<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getOutpostsClient } from '$lib/aws-client';
	import {
		ListOutpostsCommand,
		CreateOutpostCommand,
		GetOutpostCommand,
		UpdateOutpostCommand,
		DeleteOutpostCommand,
		ListSitesCommand,
		CreateSiteCommand,
		GetSiteCommand,
		UpdateSiteCommand,
		DeleteSiteCommand,
		ListOrdersCommand,
		CreateOrderCommand,
		GetOrderCommand,
		CancelOrderCommand,
		type Outpost,
		type Site,
		type OrderSummary,
		type Order,
		type SupportedHardwareType,
		type PaymentOption,
		type LineItemRequest
	} from '@aws-sdk/client-outposts';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import { Server, Plus, Trash2, Eye, Pencil, Ban, X } from 'lucide-svelte';

	// Outposts has three top-level listable resource families: Outposts,
	// Sites, and Orders.
	//
	// Deliberately NOT built here (see project report, not invented as UI):
	//  - Catalog items (GetCatalogItemCommand/ListCatalogItemsCommand): AWS
	//    publishes these as a fixed hardware-SKU reference catalog -- there
	//    is no Create/Update/Delete for a catalog item anywhere in the API,
	//    so it is read-only reference data, not a CRUD resource.
	//  - Quotes, capacity tasks, assets, connections, renewals: each nests
	//    under an existing Outpost/Order (QuoteIdentifier, CapacityTaskId,
	//    AssetId, ConnectionId) and several (quotes, renewals) have no
	//    Update at all -- out of scope for this floor pass.
	//  - Orders has no UpdateOrder operation at all: once placed, an order
	//    can only be read (GetOrder) or cancelled (CancelOrder), which is
	//    NOT the same as delete -- cancelling moves the order to CANCELLED
	//    status, it does not remove the record. The Orders tab below labels
	//    the action "Cancel order", not "Delete", to avoid misrepresenting
	//    what the real API does.
	const op = regionalClient(getOutpostsClient);

	type TabId = 'outposts' | 'sites' | 'orders';
	const tabs: TabDef[] = [
		{ id: 'outposts', label: 'Outposts' },
		{ id: 'sites', label: 'Sites' },
		{ id: 'orders', label: 'Orders' }
	];
	let activeTab = $state<TabId>('outposts');
	let searchQuery = $state('');

	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	function statusClass(status: string | undefined): string {
		if (status === 'ACTIVE' || status === 'COMPLETED' || status === 'FULFILLED') {
			return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		}
		if (status === 'ERROR' || status === 'CANCELLED') {
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	// --- Outposts ---

	let outposts = $state<Outpost[]>([]);

	async function fetchOutposts(): Promise<void> {
		const resp = await op().send(new ListOutpostsCommand({}));
		outposts = resp.Outposts ?? [];
	}

	// --- Sites ---

	let sites = $state<Site[]>([]);

	async function fetchSites(): Promise<void> {
		const resp = await op().send(new ListSitesCommand({}));
		sites = resp.Sites ?? [];
	}

	// --- Orders ---

	let orders = $state<OrderSummary[]>([]);

	async function fetchOrders(): Promise<void> {
		const resp = await op().send(new ListOrdersCommand({}));
		orders = resp.Orders ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		outposts: () => fetchOutposts().catch(rethrowDescribed),
		sites: () => fetchSites().catch(rethrowDescribed),
		orders: () => fetchOrders().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Every selected-resource id (outpost/site/order) is only unique within
	// the region it was fetched from -- clear all detail selections on
	// region change, then reload only whichever tab is active (matching
	// dlm/appconfig's onRegionChange pattern). `activeTab` is read via
	// untrack() because switchTab() also writes it: without untrack, every
	// tab switch would re-trigger this region effect and double-fetch.
	onRegionChange(() => {
		selectedOutpost = null;
		selectedSite = null;
		selectedOrder = null;
		tabLoader.refresh(untrack(() => activeTab));
	});

	const filteredOutposts = $derived(
		outposts.filter((o) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(o.Name ?? '').toLowerCase().includes(q) ||
				(o.OutpostId ?? '').toLowerCase().includes(q) ||
				(o.LifeCycleStatus ?? '').toLowerCase().includes(q) ||
				(o.AvailabilityZone ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredSites = $derived(
		sites.filter((s) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(s.Name ?? '').toLowerCase().includes(q) ||
				(s.SiteId ?? '').toLowerCase().includes(q) ||
				(s.OperatingAddressCity ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredOrders = $derived(
		orders.filter((o) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (
				(o.OrderId ?? '').toLowerCase().includes(q) ||
				(o.OutpostId ?? '').toLowerCase().includes(q) ||
				(o.Status ?? '').toLowerCase().includes(q)
			);
		})
	);

	const activeTabError = $derived(tabLoader.getError(activeTab));

	// --- Outpost CRUD ---

	let createOutpostModal = $state<Modal | null>(null);
	let creatingOutpost = $state(false);
	let createOutpostError = $state<string | null>(null);
	let newOutpostName = $state('');
	let newOutpostDescription = $state('');
	let newOutpostSiteId = $state('');
	let newOutpostAvailabilityZone = $state('');
	let newOutpostHardwareType = $state<SupportedHardwareType | ''>('');

	function openCreateOutpostModal(): void {
		createOutpostError = null;
		newOutpostName = '';
		newOutpostDescription = '';
		newOutpostSiteId = '';
		newOutpostAvailabilityZone = '';
		newOutpostHardwareType = '';
		createOutpostModal?.open();
	}

	async function submitCreateOutpost(): Promise<void> {
		if (!newOutpostName.trim() || !newOutpostSiteId.trim()) {
			createOutpostError = 'Name and Site ID are required.';
			return;
		}
		creatingOutpost = true;
		createOutpostError = null;
		try {
			await op().send(
				new CreateOutpostCommand({
					Name: newOutpostName.trim(),
					Description: newOutpostDescription.trim() || undefined,
					SiteId: newOutpostSiteId.trim(),
					AvailabilityZone: newOutpostAvailabilityZone.trim() || undefined,
					SupportedHardwareType: newOutpostHardwareType || undefined
				})
			);
			toast.success(`Outpost "${newOutpostName}" created`);
			createOutpostModal?.close();
			await tabLoader.refresh('outposts');
		} catch (e) {
			const msg = describeError(e);
			createOutpostError = msg;
			toast.error(msg);
		} finally {
			creatingOutpost = false;
		}
	}

	async function handleDeleteOutpost(o: Outpost): Promise<void> {
		if (!o.OutpostId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Outpost',
			message: `Delete Outpost "${o.Name ?? o.OutpostId}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await op().send(new DeleteOutpostCommand({ OutpostId: o.OutpostId }));
			toast.success('Outpost deleted');
			if (selectedOutpost?.OutpostId === o.OutpostId) {
				selectedOutpost = null;
				outpostDetailModal?.close();
			}
			await tabLoader.refresh('outposts');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let outpostDetailModal = $state<Modal | null>(null);
	let selectedOutpost = $state<Outpost | null>(null);
	let outpostDetailLoading = $state(false);
	let outpostDetailError = $state<string | null>(null);

	async function openOutpostDetail(o: Outpost): Promise<void> {
		selectedOutpost = null;
		outpostDetailError = null;
		outpostDetailModal?.open();
		if (!o.OutpostId) return;
		outpostDetailLoading = true;
		try {
			const resp = await op().send(new GetOutpostCommand({ OutpostId: o.OutpostId }));
			selectedOutpost = resp.Outpost ?? null;
		} catch (e) {
			outpostDetailError = describeError(e);
		} finally {
			outpostDetailLoading = false;
		}
	}

	let editOutpostModal = $state<Modal | null>(null);
	let editingOutpost = $state(false);
	let editOutpostError = $state<string | null>(null);
	let editOutpostId = $state('');
	let editOutpostName = $state('');
	let editOutpostDescription = $state('');
	let editOutpostHardwareType = $state<SupportedHardwareType | ''>('');

	function openEditOutpostModal(o: Outpost): void {
		editOutpostError = null;
		editOutpostId = o.OutpostId ?? '';
		editOutpostName = o.Name ?? '';
		editOutpostDescription = o.Description ?? '';
		editOutpostHardwareType = o.SupportedHardwareType ?? '';
		editOutpostModal?.open();
	}

	async function submitEditOutpost(): Promise<void> {
		if (!editOutpostId) return;
		editingOutpost = true;
		editOutpostError = null;
		try {
			await op().send(
				new UpdateOutpostCommand({
					OutpostId: editOutpostId,
					Name: editOutpostName.trim() || undefined,
					Description: editOutpostDescription.trim() || undefined,
					SupportedHardwareType: editOutpostHardwareType || undefined
				})
			);
			toast.success('Outpost updated');
			editOutpostModal?.close();
			await tabLoader.refresh('outposts');
			if (selectedOutpost?.OutpostId === editOutpostId) await openOutpostDetail(selectedOutpost);
		} catch (e) {
			const msg = describeError(e);
			editOutpostError = msg;
			toast.error(msg);
		} finally {
			editingOutpost = false;
		}
	}

	// --- Site CRUD ---

	let createSiteModal = $state<Modal | null>(null);
	let creatingSite = $state(false);
	let createSiteError = $state<string | null>(null);
	let newSiteName = $state('');
	let newSiteDescription = $state('');
	let newSiteNotes = $state('');

	function openCreateSiteModal(): void {
		createSiteError = null;
		newSiteName = '';
		newSiteDescription = '';
		newSiteNotes = '';
		createSiteModal?.open();
	}

	async function submitCreateSite(): Promise<void> {
		if (!newSiteName.trim()) {
			createSiteError = 'Name is required.';
			return;
		}
		creatingSite = true;
		createSiteError = null;
		try {
			await op().send(
				new CreateSiteCommand({
					Name: newSiteName.trim(),
					Description: newSiteDescription.trim() || undefined,
					Notes: newSiteNotes.trim() || undefined
				})
			);
			toast.success(`Site "${newSiteName}" created`);
			createSiteModal?.close();
			await tabLoader.refresh('sites');
		} catch (e) {
			const msg = describeError(e);
			createSiteError = msg;
			toast.error(msg);
		} finally {
			creatingSite = false;
		}
	}

	async function handleDeleteSite(s: Site): Promise<void> {
		if (!s.SiteId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete site',
			message: `Delete site "${s.Name ?? s.SiteId}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await op().send(new DeleteSiteCommand({ SiteId: s.SiteId }));
			toast.success('Site deleted');
			if (selectedSite?.SiteId === s.SiteId) {
				selectedSite = null;
				siteDetailModal?.close();
			}
			await tabLoader.refresh('sites');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let siteDetailModal = $state<Modal | null>(null);
	let selectedSite = $state<Site | null>(null);
	let siteDetailLoading = $state(false);
	let siteDetailError = $state<string | null>(null);

	async function openSiteDetail(s: Site): Promise<void> {
		selectedSite = null;
		siteDetailError = null;
		siteDetailModal?.open();
		if (!s.SiteId) return;
		siteDetailLoading = true;
		try {
			const resp = await op().send(new GetSiteCommand({ SiteId: s.SiteId }));
			selectedSite = resp.Site ?? null;
		} catch (e) {
			siteDetailError = describeError(e);
		} finally {
			siteDetailLoading = false;
		}
	}

	let editSiteModal = $state<Modal | null>(null);
	let editingSite = $state(false);
	let editSiteError = $state<string | null>(null);
	let editSiteId = $state('');
	let editSiteName = $state('');
	let editSiteDescription = $state('');
	let editSiteNotes = $state('');

	function openEditSiteModal(s: Site): void {
		editSiteError = null;
		editSiteId = s.SiteId ?? '';
		editSiteName = s.Name ?? '';
		editSiteDescription = s.Description ?? '';
		editSiteNotes = s.Notes ?? '';
		editSiteModal?.open();
	}

	async function submitEditSite(): Promise<void> {
		if (!editSiteId) return;
		editingSite = true;
		editSiteError = null;
		try {
			await op().send(
				new UpdateSiteCommand({
					SiteId: editSiteId,
					Name: editSiteName.trim() || undefined,
					Description: editSiteDescription.trim() || undefined,
					Notes: editSiteNotes.trim() || undefined
				})
			);
			toast.success('Site updated');
			editSiteModal?.close();
			await tabLoader.refresh('sites');
			if (selectedSite?.SiteId === editSiteId) await openSiteDetail(selectedSite);
		} catch (e) {
			const msg = describeError(e);
			editSiteError = msg;
			toast.error(msg);
		} finally {
			editingSite = false;
		}
	}

	// --- Order create / cancel / detail (no Update -- see header note) ---

	let createOrderModal = $state<Modal | null>(null);
	let creatingOrder = $state(false);
	let createOrderError = $state<string | null>(null);
	let newOrderOutpostId = $state('');
	let newOrderPaymentOption = $state<PaymentOption>('ALL_UPFRONT');
	let newOrderLineItems = $state<{ catalogItemId: string; quantity: number }[]>([{ catalogItemId: '', quantity: 1 }]);

	function openCreateOrderModal(): void {
		createOrderError = null;
		newOrderOutpostId = outposts[0]?.OutpostId ?? '';
		newOrderPaymentOption = 'ALL_UPFRONT';
		newOrderLineItems = [{ catalogItemId: '', quantity: 1 }];
		createOrderModal?.open();
	}

	function addOrderLineItem(): void {
		newOrderLineItems = [...newOrderLineItems, { catalogItemId: '', quantity: 1 }];
	}

	function removeOrderLineItem(index: number): void {
		newOrderLineItems = newOrderLineItems.filter((_, i) => i !== index);
	}

	async function submitCreateOrder(): Promise<void> {
		if (!newOrderOutpostId.trim()) {
			createOrderError = 'Outpost is required.';
			return;
		}
		creatingOrder = true;
		createOrderError = null;
		try {
			const lineItems: LineItemRequest[] = newOrderLineItems
				.filter((li) => li.catalogItemId.trim())
				.map((li) => ({ CatalogItemId: li.catalogItemId.trim(), Quantity: li.quantity }));
			await op().send(
				new CreateOrderCommand({
					OutpostIdentifier: newOrderOutpostId.trim(),
					PaymentOption: newOrderPaymentOption,
					LineItems: lineItems.length > 0 ? lineItems : undefined
				})
			);
			toast.success('Order created');
			createOrderModal?.close();
			await tabLoader.refresh('orders');
		} catch (e) {
			const msg = describeError(e);
			createOrderError = msg;
			toast.error(msg);
		} finally {
			creatingOrder = false;
		}
	}

	async function handleCancelOrder(o: OrderSummary): Promise<void> {
		if (!o.OrderId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel order',
			message: `Cancel order "${o.OrderId}"? This does not delete the order record, only moves it to CANCELLED status, and cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await op().send(new CancelOrderCommand({ OrderId: o.OrderId }));
			toast.success('Order cancelled');
			if (selectedOrder?.OrderId === o.OrderId) await openOrderDetail(o);
			await tabLoader.refresh('orders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let orderDetailModal = $state<Modal | null>(null);
	let selectedOrder = $state<Order | null>(null);
	let orderDetailLoading = $state(false);
	let orderDetailError = $state<string | null>(null);

	async function openOrderDetail(o: OrderSummary): Promise<void> {
		selectedOrder = null;
		orderDetailError = null;
		orderDetailModal?.open();
		if (!o.OrderId) return;
		orderDetailLoading = true;
		try {
			const resp = await op().send(new GetOrderCommand({ OrderId: o.OrderId }));
			selectedOrder = resp.Order ?? null;
		} catch (e) {
			orderDetailError = describeError(e);
		} finally {
			orderDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Server}
		title="AWS Outposts"
		description="Run AWS infrastructure and services on-premises"
		onRefresh={handleRefresh}
		color="orange"
	>
		{#snippet actions()}
			{#if activeTab === 'outposts'}
				<button onclick={openCreateOutpostModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm">
					<Plus class="w-4 h-4" /> Create Outpost
				</button>
			{:else if activeTab === 'sites'}
				<button onclick={openCreateSiteModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm">
					<Plus class="w-4 h-4" /> Create site
				</button>
			{:else if activeTab === 'orders'}
				<button onclick={openCreateOrderModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-orange-600 text-white hover:bg-orange-700 text-sm">
					<Plus class="w-4 h-4" /> Create order
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="orange" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'outposts'}
				{#snippet outpostStatusCell(o: Outpost)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(o.LifeCycleStatus)}">{o.LifeCycleStatus ?? '—'}</span>
				{/snippet}
				{#snippet outpostActionsCell(o: Outpost)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openOutpostDetail(o)} title="View" aria-label="View Outpost {o.Name}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteOutpost(o)} title="Delete" aria-label="Delete Outpost {o.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const outpostColumns = defineColumns<Outpost>([
					{ key: 'Name', label: 'Name' },
					{ key: 'OutpostId', label: 'ID' },
					{ key: 'LifeCycleStatus', label: 'Status', render: outpostStatusCell },
					{ key: 'AvailabilityZone', label: 'AZ' },
					{ key: 'SiteId', label: 'Site' },
					{ key: 'actions', label: '', render: outpostActionsCell }
				])}
				<DataTable
					rows={filteredOutposts}
					rowKey={(o) => o.OutpostId ?? ''}
					columns={outpostColumns}
					loading={tabLoader.isLoading('outposts')}
					emptyMessage="No Outposts found"
				/>
			{:else if activeTab === 'sites'}
				{#snippet siteActionsCell(s: Site)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSiteDetail(s)} title="View" aria-label="View site {s.Name}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteSite(s)} title="Delete" aria-label="Delete site {s.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const siteColumns = defineColumns<Site>([
					{ key: 'Name', label: 'Name' },
					{ key: 'SiteId', label: 'ID' },
					{ key: 'OperatingAddressCity', label: 'City' },
					{ key: 'OperatingAddressCountryCode', label: 'Country' },
					{ key: 'actions', label: '', render: siteActionsCell }
				])}
				<DataTable
					rows={filteredSites}
					rowKey={(s) => s.SiteId ?? ''}
					columns={siteColumns}
					loading={tabLoader.isLoading('sites')}
					emptyMessage="No sites found"
				/>
			{:else if activeTab === 'orders'}
				{#snippet orderStatusCell(o: OrderSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(o.Status)}">{o.Status ?? '—'}</span>
				{/snippet}
				{#snippet orderSubmittedCell(o: OrderSummary)}
					{formatDate(o.OrderSubmissionDate)}
				{/snippet}
				{#snippet orderActionsCell(o: OrderSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openOrderDetail(o)} title="View" aria-label="View order {o.OrderId}" class="text-gray-400 hover:text-orange-500"><Eye class="w-4 h-4" /></button>
						{#if o.Status !== 'CANCELLED' && o.Status !== 'COMPLETED'}
							<button onclick={() => handleCancelOrder(o)} title="Cancel order" aria-label="Cancel order {o.OrderId}" class="text-gray-400 hover:text-red-500"><Ban class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{@const orderColumns = defineColumns<OrderSummary>([
					{ key: 'OrderId', label: 'Order ID' },
					{ key: 'OutpostId', label: 'Outpost' },
					{ key: 'OrderType', label: 'Type' },
					{ key: 'Status', label: 'Status', render: orderStatusCell },
					{ key: 'OrderSubmissionDate', label: 'Submitted', render: orderSubmittedCell },
					{ key: 'actions', label: '', render: orderActionsCell }
				])}
				<DataTable
					rows={filteredOrders}
					rowKey={(o) => o.OrderId ?? ''}
					columns={orderColumns}
					loading={tabLoader.isLoading('orders')}
					emptyMessage="No orders found"
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createOutpostModal} title="Create Outpost">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="op-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="op-new-name" bind:value={newOutpostName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="op-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="op-new-desc" bind:value={newOutpostDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="op-new-site" class="text-sm text-slate-600 dark:text-slate-300">Site ID</label>
				<input id="op-new-site" bind:value={newOutpostSiteId} placeholder="os-xxxxxxxxxxxxxxxxx" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="op-new-az" class="text-sm text-slate-600 dark:text-slate-300">Availability zone</label>
				<input id="op-new-az" bind:value={newOutpostAvailabilityZone} placeholder="us-east-1a" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="op-new-hw" class="text-sm text-slate-600 dark:text-slate-300">Hardware type</label>
				<select id="op-new-hw" bind:value={newOutpostHardwareType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">(unspecified)</option>
					<option value="RACK">RACK</option>
					<option value="SERVER">SERVER</option>
				</select>
			</div>
			{#if createOutpostError}
				<p class="text-sm text-red-600 dark:text-red-400">{createOutpostError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createOutpostModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateOutpost} disabled={creatingOutpost} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingOutpost ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editOutpostModal} title="Edit Outpost">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="op-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="op-edit-name" bind:value={editOutpostName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="op-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="op-edit-desc" bind:value={editOutpostDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="op-edit-hw" class="text-sm text-slate-600 dark:text-slate-300">Hardware type</label>
				<select id="op-edit-hw" bind:value={editOutpostHardwareType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">(unspecified)</option>
					<option value="RACK">RACK</option>
					<option value="SERVER">SERVER</option>
				</select>
			</div>
			{#if editOutpostError}
				<p class="text-sm text-red-600 dark:text-red-400">{editOutpostError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editOutpostModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditOutpost} disabled={editingOutpost} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{editingOutpost ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={outpostDetailModal} title="Outpost">
	{#snippet children()}
		{#if outpostDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if outpostDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{outpostDetailError}</p>
		{:else if selectedOutpost}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{selectedOutpost.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{selectedOutpost.OutpostId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{selectedOutpost.OutpostArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Site</dt><dd class="text-slate-900 dark:text-white">{selectedOutpost.SiteId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{selectedOutpost.LifeCycleStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Availability zone</dt><dd class="text-slate-900 dark:text-white">{selectedOutpost.AvailabilityZone ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Hardware type</dt><dd class="text-slate-900 dark:text-white">{selectedOutpost.SupportedHardwareType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{selectedOutpost.Description ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => outpostDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if selectedOutpost}
			<button type="button" onclick={() => selectedOutpost && openEditOutpostModal(selectedOutpost)} class="flex items-center gap-2 rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={createSiteModal} title="Create Site">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="site-new-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="site-new-name" bind:value={newSiteName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="site-new-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="site-new-desc" bind:value={newSiteDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="site-new-notes" class="text-sm text-slate-600 dark:text-slate-300">Notes</label>
				<input id="site-new-notes" bind:value={newSiteNotes} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Operating/shipping address and rack physical properties are not editable here -- see the project follow-up notes.</p>
			{#if createSiteError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSiteError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createSiteModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateSite} disabled={creatingSite} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingSite ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editSiteModal} title="Edit Site">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="site-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="site-edit-name" bind:value={editSiteName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="site-edit-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="site-edit-desc" bind:value={editSiteDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="site-edit-notes" class="text-sm text-slate-600 dark:text-slate-300">Notes</label>
				<input id="site-edit-notes" bind:value={editSiteNotes} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editSiteError}
				<p class="text-sm text-red-600 dark:text-red-400">{editSiteError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editSiteModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditSite} disabled={editingSite} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{editingSite ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={siteDetailModal} title="Site">
	{#snippet children()}
		{#if siteDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if siteDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{siteDetailError}</p>
		{:else if selectedSite}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{selectedSite.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{selectedSite.SiteId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="break-all text-slate-900 dark:text-white">{selectedSite.SiteArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{selectedSite.Description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Notes</dt><dd class="text-slate-900 dark:text-white">{selectedSite.Notes ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">City</dt><dd class="text-slate-900 dark:text-white">{selectedSite.OperatingAddressCity ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Country</dt><dd class="text-slate-900 dark:text-white">{selectedSite.OperatingAddressCountryCode ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => siteDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if selectedSite}
			<button type="button" onclick={() => selectedSite && openEditSiteModal(selectedSite)} class="flex items-center gap-2 rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700"><Pencil class="w-4 h-4" /> Edit</button>
		{/if}
	{/snippet}
</Modal>

<Modal bind:this={createOrderModal} title="Create Order">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="order-new-outpost" class="text-sm text-slate-600 dark:text-slate-300">Outpost</label>
				<select id="order-new-outpost" bind:value={newOrderOutpostId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="">— Select Outpost —</option>
					{#each outposts as o (o.OutpostId)}
						<option value={o.OutpostId}>{o.Name ?? o.OutpostId}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="order-new-payment" class="text-sm text-slate-600 dark:text-slate-300">Payment option</label>
				<select id="order-new-payment" bind:value={newOrderPaymentOption} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="ALL_UPFRONT">ALL_UPFRONT</option>
					<option value="NO_UPFRONT">NO_UPFRONT</option>
					<option value="PARTIAL_UPFRONT">PARTIAL_UPFRONT</option>
				</select>
			</div>
			<div>
				<div class="flex items-center justify-between">
					<span class="text-sm text-slate-600 dark:text-slate-300">Line items</span>
					<button type="button" onclick={addOrderLineItem} class="text-xs text-orange-600 dark:text-orange-400 hover:underline">Add line item</button>
				</div>
				{#each newOrderLineItems as item, index (index)}
					<div class="mt-2 flex items-center gap-2">
						<input bind:value={item.catalogItemId} placeholder="Catalog item ID" aria-label="Catalog item ID" class="w-2/3 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<input type="number" min="1" bind:value={item.quantity} aria-label="Quantity" class="w-1/4 px-2 py-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
						<button type="button" onclick={() => removeOrderLineItem(index)} aria-label="Remove line item" class="text-gray-400 hover:text-red-500"><X class="w-4 h-4" /></button>
					</div>
				{/each}
			</div>
			{#if createOrderError}
				<p class="text-sm text-red-600 dark:text-red-400">{createOrderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createOrderModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateOrder} disabled={creatingOrder} class="rounded-lg bg-orange-600 px-4 py-2 text-sm font-semibold text-white hover:bg-orange-700 disabled:opacity-50">{creatingOrder ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={orderDetailModal} title="Order">
	{#snippet children()}
		{#if orderDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if orderDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{orderDetailError}</p>
		{:else if selectedOrder}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Order ID</dt><dd class="text-slate-900 dark:text-white">{selectedOrder.OrderId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Outpost</dt><dd class="text-slate-900 dark:text-white">{selectedOrder.OutpostId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{selectedOrder.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Payment option</dt><dd class="text-slate-900 dark:text-white">{selectedOrder.PaymentOption ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Submitted</dt><dd class="text-slate-900 dark:text-white">{formatDate(selectedOrder.OrderSubmissionDate)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Fulfilled</dt><dd class="text-slate-900 dark:text-white">{formatDate(selectedOrder.OrderFulfilledDate)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Line items</dt>
					<dd class="text-slate-900 dark:text-white">
						{#if (selectedOrder.LineItems ?? []).length === 0}
							<span class="text-slate-500 dark:text-slate-400">None</span>
						{:else}
							<ul class="space-y-1">
								{#each selectedOrder.LineItems ?? [] as li (li.LineItemId)}
									<li>{li.CatalogItemId} × {li.Quantity} — {li.Status}</li>
								{/each}
							</ul>
						{/if}
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => orderDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if selectedOrder && selectedOrder.Status !== 'CANCELLED' && selectedOrder.Status !== 'COMPLETED'}
			<button type="button" onclick={() => selectedOrder && handleCancelOrder(selectedOrder)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Ban class="w-4 h-4" /> Cancel order</button>
		{/if}
	{/snippet}
</Modal>
