<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import { getOutpostsClient } from '$lib/aws-client';
	import {
		ListOutpostsCommand,
		CreateOutpostCommand,
		GetOutpostCommand,
		UpdateOutpostCommand,
		DeleteOutpostCommand,
		GetOutpostInstanceTypesCommand,
		GetOutpostSupportedInstanceTypesCommand,
		GetOutpostBillingInformationCommand,
		StartOutpostDecommissionCommand,
		CreateRenewalCommand,
		GetRenewalPricingCommand,
		StartConnectionCommand,
		GetConnectionCommand,
		ListSitesCommand,
		CreateSiteCommand,
		GetSiteCommand,
		UpdateSiteCommand,
		DeleteSiteCommand,
		GetSiteAddressCommand,
		UpdateSiteAddressCommand,
		UpdateSiteRackPhysicalPropertiesCommand,
		ListOrdersCommand,
		CreateOrderCommand,
		GetOrderCommand,
		CancelOrderCommand,
		ListQuotesCommand,
		CreateQuoteCommand,
		GetQuoteCommand,
		UpdateQuoteCommand,
		DeleteQuoteCommand,
		ListCapacityTasksCommand,
		StartCapacityTaskCommand,
		GetCapacityTaskCommand,
		CancelCapacityTaskCommand,
		ListBlockingInstancesForCapacityTaskCommand,
		ListAssetsCommand,
		ListAssetInstancesCommand,
		ListCatalogItemsCommand,
		GetCatalogItemCommand,
		ListOrderableInstanceTypesCommand,
		ListTagsForResourceCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Outpost,
		type Site,
		type OrderSummary,
		type Order,
		type QuoteSummary,
		type Quote,
		type CapacityTaskSummary,
		type AssetInfo,
		type AssetInstance,
		type CatalogItem,
		type InstanceTypeItem,
		type Subscription,
		type BlockingInstance,
		type PricingOption
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
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { Server, Plus, Trash2, Eye, Pencil, Ban } from 'lucide-svelte';

	const client = regionalClient(getOutpostsClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so both the toast and the inline
	// error banner show the actual code, not just a generic message.
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

	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	type TabId =
		| 'outposts'
		| 'sites'
		| 'orders'
		| 'quotes'
		| 'capacityTasks'
		| 'assets'
		| 'catalog'
		| 'orderableInstanceTypes';

	const tabs: TabDef[] = [
		{ id: 'outposts', label: 'Outposts' },
		{ id: 'sites', label: 'Sites' },
		{ id: 'orders', label: 'Orders' },
		{ id: 'quotes', label: 'Quotes' },
		{ id: 'capacityTasks', label: 'Capacity Tasks' },
		{ id: 'assets', label: 'Assets' },
		{ id: 'catalog', label: 'Catalog' },
		{ id: 'orderableInstanceTypes', label: 'Orderable Instance Types' }
	];

	// URL-backed (?tab=...); see url-state.svelte.ts. Read via untrack() inside
	// the onRegionChange effect below (switchTab() also writes it): without
	// untrack, every tab switch would re-trigger the region effect and
	// double-fetch.
	const pageTabParam = urlState<TabId>('tab', 'outposts');
	let activeTab = $derived(pageTabParam.get());
	let searchQuery = $state('');

	// Assets is the only tab scoped to a parent resource (an Outpost) -- the
	// same shared-selector pattern accessanalyzer uses for its
	// analyzer-scoped tabs, just with a single scoped tab instead of four.
	let selectedOutpostId = $state('');

	let outposts = $state<Outpost[]>([]);
	let outpostsNextToken = $state<string | undefined>();
	let loadingMoreOutposts = $state(false);

	let sites = $state<Site[]>([]);
	let sitesNextToken = $state<string | undefined>();
	let loadingMoreSites = $state(false);

	let orders = $state<OrderSummary[]>([]);
	let ordersNextToken = $state<string | undefined>();
	let loadingMoreOrders = $state(false);

	let quotes = $state<QuoteSummary[]>([]);
	let quotesNextToken = $state<string | undefined>();
	let loadingMoreQuotes = $state(false);

	let capacityTasks = $state<CapacityTaskSummary[]>([]);
	let capacityTasksNextToken = $state<string | undefined>();
	let loadingMoreCapacityTasks = $state(false);

	let assets = $state<AssetInfo[]>([]);
	let assetsNextToken = $state<string | undefined>();
	let loadingMoreAssets = $state(false);

	let catalogItems = $state<CatalogItem[]>([]);
	let catalogNextToken = $state<string | undefined>();
	let loadingMoreCatalog = $state(false);

	let orderableInstanceTypes = $state<InstanceTypeItem[]>([]);
	let oitNextToken = $state<string | undefined>();
	let loadingMoreOit = $state(false);
	let oitGenerationFilter = $state<'' | 'GENERATION_1' | 'GENERATION_2'>('');

	async function fetchOutposts(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListOutpostsCommand({ NextToken: reset ? undefined : outpostsNextToken })
		);
		outposts = reset ? (resp.Outposts ?? []) : [...outposts, ...(resp.Outposts ?? [])];
		outpostsNextToken = resp.NextToken;
		if (!selectedOutpostId && outposts.length > 0) {
			selectedOutpostId = outposts[0].OutpostId ?? '';
		}
	}

	async function fetchSites(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListSitesCommand({ NextToken: reset ? undefined : sitesNextToken })
		);
		sites = reset ? (resp.Sites ?? []) : [...sites, ...(resp.Sites ?? [])];
		sitesNextToken = resp.NextToken;
	}

	async function fetchOrders(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListOrdersCommand({ NextToken: reset ? undefined : ordersNextToken })
		);
		orders = reset ? (resp.Orders ?? []) : [...orders, ...(resp.Orders ?? [])];
		ordersNextToken = resp.NextToken;
	}

	async function fetchQuotes(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListQuotesCommand({ NextToken: reset ? undefined : quotesNextToken })
		);
		quotes = reset ? (resp.Quotes ?? []) : [...quotes, ...(resp.Quotes ?? [])];
		quotesNextToken = resp.NextToken;
	}

	async function fetchCapacityTasks(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListCapacityTasksCommand({ NextToken: reset ? undefined : capacityTasksNextToken })
		);
		capacityTasks = reset
			? (resp.CapacityTasks ?? [])
			: [...capacityTasks, ...(resp.CapacityTasks ?? [])];
		capacityTasksNextToken = resp.NextToken;
	}

	async function fetchAssets(reset: boolean): Promise<void> {
		if (!selectedOutpostId) {
			assets = [];
			assetsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListAssetsCommand({
				OutpostIdentifier: selectedOutpostId,
				NextToken: reset ? undefined : assetsNextToken
			})
		);
		assets = reset ? (resp.Assets ?? []) : [...assets, ...(resp.Assets ?? [])];
		assetsNextToken = resp.NextToken;
	}

	async function fetchCatalog(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListCatalogItemsCommand({ NextToken: reset ? undefined : catalogNextToken })
		);
		catalogItems = reset ? (resp.CatalogItems ?? []) : [...catalogItems, ...(resp.CatalogItems ?? [])];
		catalogNextToken = resp.NextToken;
	}

	async function fetchOit(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListOrderableInstanceTypesCommand({
				OutpostGenerationFilter: oitGenerationFilter || undefined,
				NextToken: reset ? undefined : oitNextToken
			})
		);
		orderableInstanceTypes = reset
			? (resp.InstanceTypes ?? [])
			: [...orderableInstanceTypes, ...(resp.InstanceTypes ?? [])];
		oitNextToken = resp.NextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		outposts: () => fetchOutposts(true).catch(rethrowDescribed),
		sites: () => fetchSites(true).catch(rethrowDescribed),
		orders: () => fetchOrders(true).catch(rethrowDescribed),
		quotes: () => fetchQuotes(true).catch(rethrowDescribed),
		capacityTasks: () => fetchCapacityTasks(true).catch(rethrowDescribed),
		assets: () => fetchAssets(true).catch(rethrowDescribed),
		catalog: () => fetchCatalog(true).catch(rethrowDescribed),
		orderableInstanceTypes: () => fetchOit(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		pageTabParam.set(id as TabId);
		searchQuery = '';
		tabLoader.load(id as TabId);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onOutpostSelect(id: string): void {
		selectedOutpostId = id;
		if (activeTab === 'assets') {
			tabLoader.refresh('assets');
		}
	}

	// Outposts is the parent resource for the Assets tab: on a region change
	// the previously selected Outpost ID belongs to the old region and must
	// not be reused, so reload outposts first (which re-selects one for the
	// new region) before reloading whichever tab is active. `activeTab` is
	// read via untrack() because switchTab() also writes it (via
	// pageTabParam): without untrack, every tab switch would re-trigger this
	// region effect and double-fetch.
	onRegionChange(() => {
		selectedOutpostId = '';
		outposts = [];
		outpostsNextToken = undefined;
		void tabLoader.refresh('outposts').then(() => {
			const tab = untrack(() => activeTab);
			if (tab !== 'outposts') {
				tabLoader.refresh(tab);
			}
		});
	});

	const filteredOutposts = $derived(
		outposts.filter((o) => {
			const q = searchQuery.toLowerCase();
			return (
				(o.Name ?? '').toLowerCase().includes(q) ||
				(o.OutpostId ?? '').toLowerCase().includes(q) ||
				(o.LifeCycleStatus ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredSites = $derived(
		sites.filter((s) => (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredOrders = $derived(
		orders.filter((o) => {
			const q = searchQuery.toLowerCase();
			return (
				(o.OrderId ?? '').toLowerCase().includes(q) || (o.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredQuotes = $derived(
		quotes.filter((q) => (q.QuoteId ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredCapacityTasks = $derived(
		capacityTasks.filter((t) =>
			(t.CapacityTaskId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredAssets = $derived(
		assets.filter((a) => (a.AssetId ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredCatalog = $derived(
		catalogItems.filter((c) =>
			(c.CatalogItemId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const filteredOit = $derived(
		orderableInstanceTypes.filter((i) =>
			(i.InstanceType ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreOutposts(): Promise<void> {
		loadingMoreOutposts = true;
		try {
			await fetchOutposts(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreOutposts = false;
		}
	}
	async function loadMoreSites(): Promise<void> {
		loadingMoreSites = true;
		try {
			await fetchSites(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSites = false;
		}
	}
	async function loadMoreOrders(): Promise<void> {
		loadingMoreOrders = true;
		try {
			await fetchOrders(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreOrders = false;
		}
	}
	async function loadMoreQuotes(): Promise<void> {
		loadingMoreQuotes = true;
		try {
			await fetchQuotes(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreQuotes = false;
		}
	}
	async function loadMoreCapacityTasks(): Promise<void> {
		loadingMoreCapacityTasks = true;
		try {
			await fetchCapacityTasks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCapacityTasks = false;
		}
	}
	async function loadMoreAssets(): Promise<void> {
		loadingMoreAssets = true;
		try {
			await fetchAssets(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAssets = false;
		}
	}
	async function loadMoreCatalog(): Promise<void> {
		loadingMoreCatalog = true;
		try {
			await fetchCatalog(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCatalog = false;
		}
	}
	async function loadMoreOit(): Promise<void> {
		loadingMoreOit = true;
		try {
			await fetchOit(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreOit = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// --- Outposts: create / delete / detail / edit ---

	let createOutpostModal = $state<Modal | null>(null);
	let creatingOutpost = $state(false);
	let createOutpostError = $state<string | null>(null);
	let newOutpostName = $state('');
	let newOutpostDescription = $state('');
	let newOutpostSiteId = $state('');
	let newOutpostAz = $state('');
	let newOutpostHardware = $state<'' | 'RACK' | 'SERVER'>('');

	function openCreateOutpostModal(): void {
		createOutpostError = null;
		newOutpostName = '';
		newOutpostDescription = '';
		newOutpostSiteId = '';
		newOutpostAz = '';
		newOutpostHardware = '';
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
			await client().send(
				new CreateOutpostCommand({
					Name: newOutpostName.trim(),
					Description: newOutpostDescription.trim() || undefined,
					SiteId: newOutpostSiteId.trim(),
					AvailabilityZone: newOutpostAz.trim() || undefined,
					SupportedHardwareType: newOutpostHardware || undefined
				})
			);
			toast.success('Outpost created');
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
			message: `Delete Outpost "${o.Name ?? o.OutpostId}"? This also removes its seeded asset.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteOutpostCommand({ OutpostId: o.OutpostId }));
			toast.success('Outpost deleted');
			if (selectedOutpostId === o.OutpostId) selectedOutpostId = '';
			await tabLoader.refresh('outposts');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editOutpostModal = $state<Modal | null>(null);
	let editingOutpost = $state(false);
	let editOutpostError = $state<string | null>(null);
	let editOutpostId = $state('');
	let editOutpostName = $state('');
	let editOutpostDescription = $state('');
	let editOutpostHardware = $state<'' | 'RACK' | 'SERVER'>('');

	function openEditOutpostModal(o: Outpost): void {
		if (!o.OutpostId) return;
		editOutpostError = null;
		editOutpostId = o.OutpostId;
		editOutpostName = o.Name ?? '';
		editOutpostDescription = o.Description ?? '';
		editOutpostHardware = (o.SupportedHardwareType as '' | 'RACK' | 'SERVER') ?? '';
		editOutpostModal?.open();
	}

	async function submitEditOutpost(): Promise<void> {
		if (!editOutpostId) return;
		editingOutpost = true;
		editOutpostError = null;
		try {
			await client().send(
				new UpdateOutpostCommand({
					OutpostId: editOutpostId,
					Name: editOutpostName.trim() || undefined,
					Description: editOutpostDescription.trim() || undefined,
					SupportedHardwareType: editOutpostHardware || undefined
				})
			);
			toast.success('Outpost updated');
			editOutpostModal?.close();
			await tabLoader.refresh('outposts');
		} catch (e) {
			const msg = describeError(e);
			editOutpostError = msg;
			toast.error(msg);
		} finally {
			editingOutpost = false;
		}
	}

	// Outpost detail: base description plus the non-listable sub-resources
	// (instance types, billing, decommission, renewal, connections, tags)
	// this service scopes to a single Outpost rather than exposing as their
	// own list operations.
	let outpostDetailModal = $state<Modal | null>(null);
	let viewedOutpost = $state<Outpost | null>(null);
	let outpostDetailLoading = $state(false);
	let outpostDetailError = $state<string | null>(null);
	let outpostDetailId = $state('');
	let outpostTags = $state<Record<string, string>>({});

	async function openOutpostDetail(o: Outpost): Promise<void> {
		if (!o.OutpostId) return;
		outpostDetailId = o.OutpostId;
		viewedOutpost = null;
		outpostDetailError = null;
		outpostTags = {};
		instanceTypesConfigured = [];
		instanceTypesSupported = [];
		billingSubscriptions = [];
		billingContractEndDate = null;
		renewalResult = null;
		renewalPricingResult = null;
		connStartResult = null;
		connLookupResult = null;
		outpostDetailModal?.open();
		outpostDetailLoading = true;
		try {
			const resp = await client().send(new GetOutpostCommand({ OutpostId: o.OutpostId }));
			viewedOutpost = resp.Outpost ?? null;
			if (viewedOutpost?.OutpostArn) {
				await refreshOutpostTags();
			}
		} catch (e) {
			outpostDetailError = describeError(e);
		} finally {
			outpostDetailLoading = false;
		}
	}

	async function refreshOutpostTags(): Promise<void> {
		if (!viewedOutpost?.OutpostArn) return;
		try {
			const resp = await client().send(
				new ListTagsForResourceCommand({ ResourceArn: viewedOutpost.OutpostArn })
			);
			outpostTags = resp.Tags ?? {};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let addTagKey = $state('');
	let addTagValue = $state('');

	async function submitAddOutpostTag(): Promise<void> {
		if (!viewedOutpost?.OutpostArn || !addTagKey.trim()) return;
		try {
			await client().send(
				new TagResourceCommand({
					ResourceArn: viewedOutpost.OutpostArn,
					Tags: { [addTagKey.trim()]: addTagValue }
				})
			);
			toast.success('Tag added');
			addTagKey = '';
			addTagValue = '';
			await refreshOutpostTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeOutpostTag(key: string): Promise<void> {
		if (!viewedOutpost?.OutpostArn) return;
		try {
			await client().send(
				new UntagResourceCommand({ ResourceArn: viewedOutpost.OutpostArn, TagKeys: [key] })
			);
			toast.success('Tag removed');
			await refreshOutpostTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// -- Instance types (configured vs supported) --

	let instanceTypesConfigured = $state<InstanceTypeItem[]>([]);
	let instanceTypesSupported = $state<InstanceTypeItem[]>([]);
	let loadingInstanceTypes = $state(false);

	async function loadConfiguredInstanceTypes(): Promise<void> {
		if (!outpostDetailId) return;
		loadingInstanceTypes = true;
		try {
			const resp = await client().send(
				new GetOutpostInstanceTypesCommand({ OutpostId: outpostDetailId })
			);
			instanceTypesConfigured = resp.InstanceTypes ?? [];
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingInstanceTypes = false;
		}
	}

	async function loadSupportedInstanceTypes(): Promise<void> {
		if (!outpostDetailId) return;
		loadingInstanceTypes = true;
		try {
			const resp = await client().send(
				new GetOutpostSupportedInstanceTypesCommand({ OutpostIdentifier: outpostDetailId })
			);
			instanceTypesSupported = resp.InstanceTypes ?? [];
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingInstanceTypes = false;
		}
	}

	// -- Billing --

	let billingSubscriptions = $state<Subscription[]>([]);
	let billingContractEndDate = $state<string | null>(null);
	let loadingBilling = $state(false);

	async function loadBilling(): Promise<void> {
		if (!outpostDetailId) return;
		loadingBilling = true;
		try {
			const resp = await client().send(
				new GetOutpostBillingInformationCommand({ OutpostIdentifier: outpostDetailId })
			);
			billingSubscriptions = resp.Subscriptions ?? [];
			billingContractEndDate = resp.ContractEndDate ?? null;
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingBilling = false;
		}
	}

	// -- Decommission --

	async function handleDecommission(): Promise<void> {
		if (!outpostDetailId) return;
		const confirmed = await confirmDestructive({
			title: 'Start decommission',
			message: 'Start the decommission process for this Outpost?'
		});
		if (!confirmed) return;
		try {
			const resp = await client().send(
				new StartOutpostDecommissionCommand({ OutpostIdentifier: outpostDetailId })
			);
			toast.success(`Decommission request: ${resp.Status ?? 'REQUESTED'}`);
			const refreshed = await client().send(new GetOutpostCommand({ OutpostId: outpostDetailId }));
			viewedOutpost = refreshed.Outpost ?? viewedOutpost;
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// -- Renewal --

	let renewalPaymentOption = $state<'ALL_UPFRONT' | 'NO_UPFRONT' | 'PARTIAL_UPFRONT'>('NO_UPFRONT');
	let renewalPaymentTerm = $state<'ONE_YEAR' | 'THREE_YEARS' | 'FIVE_YEARS'>('ONE_YEAR');
	let renewalResult = $state<{
		upfrontPrice?: number;
		monthlyRecurringPrice?: number;
		currency?: string;
	} | null>(null);
	let renewalPricingResult = $state<{ result?: string; options: PricingOption[] } | null>(null);
	let savingRenewal = $state(false);

	async function submitCreateRenewal(): Promise<void> {
		if (!outpostDetailId) return;
		savingRenewal = true;
		try {
			const resp = await client().send(
				new CreateRenewalCommand({
					OutpostIdentifier: outpostDetailId,
					PaymentOption: renewalPaymentOption,
					PaymentTerm: renewalPaymentTerm
				})
			);
			renewalResult = {
				upfrontPrice: resp.UpfrontPrice,
				monthlyRecurringPrice: resp.MonthlyRecurringPrice,
				currency: resp.Currency
			};
			toast.success('Renewal created');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			savingRenewal = false;
		}
	}

	async function loadRenewalPricing(): Promise<void> {
		if (!outpostDetailId) return;
		try {
			const resp = await client().send(
				new GetRenewalPricingCommand({ OutpostIdentifier: outpostDetailId })
			);
			renewalPricingResult = { result: resp.PricingResult, options: resp.PricingOptions ?? [] };
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// -- Connections --

	let connAssetId = $state('');
	let connClientPublicKey = $state('');
	let connDeviceIndex = $state(0);
	let connDeviceSerial = $state('');
	let connStartResult = $state<{ connectionId?: string; underlayIpAddress?: string } | null>(null);
	let startingConn = $state(false);
	let connLookupId = $state('');
	let connLookupResult = $state<{
		connectionId?: string;
		serverEndpoint?: string;
		clientTunnelAddress?: string;
		serverTunnelAddress?: string;
	} | null>(null);

	async function submitStartConnection(): Promise<void> {
		if (!connAssetId.trim() || !connClientPublicKey.trim()) {
			toast.error('Asset ID and client public key are required.');
			return;
		}
		startingConn = true;
		try {
			const resp = await client().send(
				new StartConnectionCommand({
					AssetId: connAssetId.trim(),
					ClientPublicKey: connClientPublicKey.trim(),
					NetworkInterfaceDeviceIndex: connDeviceIndex,
					DeviceSerialNumber: connDeviceSerial.trim() || undefined
				})
			);
			connStartResult = {
				connectionId: resp.ConnectionId,
				underlayIpAddress: resp.UnderlayIpAddress
			};
			toast.success('Connection started');
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			startingConn = false;
		}
	}

	async function lookupConnection(): Promise<void> {
		if (!connLookupId.trim()) return;
		try {
			const resp = await client().send(
				new GetConnectionCommand({ ConnectionId: connLookupId.trim() })
			);
			connLookupResult = {
				connectionId: resp.ConnectionId,
				serverEndpoint: resp.ConnectionDetails?.ServerEndpoint,
				clientTunnelAddress: resp.ConnectionDetails?.ClientTunnelAddress,
				serverTunnelAddress: resp.ConnectionDetails?.ServerTunnelAddress
			};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// --- Sites: create / delete / detail / edit ---

	let createSiteModal = $state<Modal | null>(null);
	let creatingSite = $state(false);
	let createSiteError = $state<string | null>(null);
	let newSiteName = $state('');
	let newSiteDescription = $state('');
	let newSiteNotes = $state('');
	let newSiteIncludeAddress = $state(false);
	let newSiteContactName = $state('');
	let newSiteContactPhone = $state('');
	let newSiteAddressLine1 = $state('');
	let newSiteCity = $state('');
	let newSiteStateOrRegion = $state('');
	let newSitePostalCode = $state('');
	let newSiteCountryCode = $state('');

	function openCreateSiteModal(): void {
		createSiteError = null;
		newSiteName = '';
		newSiteDescription = '';
		newSiteNotes = '';
		newSiteIncludeAddress = false;
		newSiteContactName = '';
		newSiteContactPhone = '';
		newSiteAddressLine1 = '';
		newSiteCity = '';
		newSiteStateOrRegion = '';
		newSitePostalCode = '';
		newSiteCountryCode = '';
		createSiteModal?.open();
	}

	async function submitCreateSite(): Promise<void> {
		if (!newSiteName.trim()) {
			createSiteError = 'Name is required.';
			return;
		}
		if (
			newSiteIncludeAddress &&
			(!newSiteContactName.trim() ||
				!newSiteContactPhone.trim() ||
				!newSiteAddressLine1.trim() ||
				!newSiteCity.trim() ||
				!newSiteStateOrRegion.trim() ||
				!newSitePostalCode.trim() ||
				!newSiteCountryCode.trim())
		) {
			createSiteError = 'All operating address fields are required when included.';
			return;
		}
		creatingSite = true;
		createSiteError = null;
		try {
			await client().send(
				new CreateSiteCommand({
					Name: newSiteName.trim(),
					Description: newSiteDescription.trim() || undefined,
					Notes: newSiteNotes.trim() || undefined,
					OperatingAddress: newSiteIncludeAddress
						? {
								ContactName: newSiteContactName.trim(),
								ContactPhoneNumber: newSiteContactPhone.trim(),
								AddressLine1: newSiteAddressLine1.trim(),
								City: newSiteCity.trim(),
								StateOrRegion: newSiteStateOrRegion.trim(),
								PostalCode: newSitePostalCode.trim(),
								CountryCode: newSiteCountryCode.trim()
							}
						: undefined
				})
			);
			toast.success('Site created');
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
			title: 'Delete Site',
			message: `Delete site "${s.Name ?? s.SiteId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSiteCommand({ SiteId: s.SiteId }));
			toast.success('Site deleted');
			await tabLoader.refresh('sites');
		} catch (e) {
			toast.error(describeError(e));
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
		if (!s.SiteId) return;
		editSiteError = null;
		editSiteId = s.SiteId;
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
			await client().send(
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
		} catch (e) {
			const msg = describeError(e);
			editSiteError = msg;
			toast.error(msg);
		} finally {
			editingSite = false;
		}
	}

	let siteDetailModal = $state<Modal | null>(null);
	let viewedSite = $state<Site | null>(null);
	let siteDetailLoading = $state(false);
	let siteDetailError = $state<string | null>(null);
	let siteDetailId = $state('');
	let siteTags = $state<Record<string, string>>({});

	async function openSiteDetail(s: Site): Promise<void> {
		if (!s.SiteId) return;
		siteDetailId = s.SiteId;
		viewedSite = null;
		siteDetailError = null;
		siteTags = {};
		siteAddressResult = null;
		siteDetailModal?.open();
		siteDetailLoading = true;
		try {
			const resp = await client().send(new GetSiteCommand({ SiteId: s.SiteId }));
			viewedSite = resp.Site ?? null;
			if (viewedSite?.SiteArn) await refreshSiteTags();
		} catch (e) {
			siteDetailError = describeError(e);
		} finally {
			siteDetailLoading = false;
		}
	}

	async function refreshSiteTags(): Promise<void> {
		if (!viewedSite?.SiteArn) return;
		try {
			const resp = await client().send(
				new ListTagsForResourceCommand({ ResourceArn: viewedSite.SiteArn })
			);
			siteTags = resp.Tags ?? {};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAddSiteTag(): Promise<void> {
		if (!viewedSite?.SiteArn || !addTagKey.trim()) return;
		try {
			await client().send(
				new TagResourceCommand({
					ResourceArn: viewedSite.SiteArn,
					Tags: { [addTagKey.trim()]: addTagValue }
				})
			);
			toast.success('Tag added');
			addTagKey = '';
			addTagValue = '';
			await refreshSiteTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeSiteTag(key: string): Promise<void> {
		if (!viewedSite?.SiteArn) return;
		try {
			await client().send(new UntagResourceCommand({ ResourceArn: viewedSite.SiteArn, TagKeys: [key] }));
			toast.success('Tag removed');
			await refreshSiteTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// -- Site address (GetSiteAddress / UpdateSiteAddress) --

	let siteAddressType = $state<'OPERATING_ADDRESS' | 'SHIPPING_ADDRESS'>('OPERATING_ADDRESS');
	let siteAddressResult = $state<{
		contactName?: string;
		addressLine1?: string;
		city?: string;
		stateOrRegion?: string;
		postalCode?: string;
		countryCode?: string;
	} | null>(null);
	let loadingSiteAddress = $state(false);
	let updAddrContactName = $state('');
	let updAddrContactPhone = $state('');
	let updAddrLine1 = $state('');
	let updAddrCity = $state('');
	let updAddrStateOrRegion = $state('');
	let updAddrPostalCode = $state('');
	let updAddrCountryCode = $state('');
	let savingSiteAddress = $state(false);

	async function loadSiteAddress(): Promise<void> {
		if (!siteDetailId) return;
		loadingSiteAddress = true;
		try {
			const resp = await client().send(
				new GetSiteAddressCommand({ SiteId: siteDetailId, AddressType: siteAddressType })
			);
			siteAddressResult = {
				contactName: resp.Address?.ContactName,
				addressLine1: resp.Address?.AddressLine1,
				city: resp.Address?.City,
				stateOrRegion: resp.Address?.StateOrRegion,
				postalCode: resp.Address?.PostalCode,
				countryCode: resp.Address?.CountryCode
			};
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingSiteAddress = false;
		}
	}

	async function submitUpdateSiteAddress(): Promise<void> {
		if (!siteDetailId) return;
		if (
			!updAddrContactName.trim() ||
			!updAddrContactPhone.trim() ||
			!updAddrLine1.trim() ||
			!updAddrCity.trim() ||
			!updAddrStateOrRegion.trim() ||
			!updAddrPostalCode.trim() ||
			!updAddrCountryCode.trim()
		) {
			toast.error('All address fields are required.');
			return;
		}
		savingSiteAddress = true;
		try {
			await client().send(
				new UpdateSiteAddressCommand({
					SiteId: siteDetailId,
					AddressType: siteAddressType,
					Address: {
						ContactName: updAddrContactName.trim(),
						ContactPhoneNumber: updAddrContactPhone.trim(),
						AddressLine1: updAddrLine1.trim(),
						City: updAddrCity.trim(),
						StateOrRegion: updAddrStateOrRegion.trim(),
						PostalCode: updAddrPostalCode.trim(),
						CountryCode: updAddrCountryCode.trim()
					}
				})
			);
			toast.success('Address updated');
			await loadSiteAddress();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			savingSiteAddress = false;
		}
	}

	// -- Rack physical properties --

	let rackPowerDrawKva = $state<'' | 'POWER_5_KVA' | 'POWER_10_KVA' | 'POWER_15_KVA' | 'POWER_30_KVA'>(
		''
	);
	let rackPowerPhase = $state<'' | 'SINGLE_PHASE' | 'THREE_PHASE'>('');
	let rackPowerConnector = $state<
		'' | 'AH530P7W' | 'AH532P6W' | 'CS8365C' | 'IEC309' | 'L6_30P'
	>('');
	let rackPowerFeedDrop = $state<'' | 'ABOVE_RACK' | 'BELOW_RACK'>('');
	let rackUplinkGbps = $state<'' | 'UPLINK_1G' | 'UPLINK_10G' | 'UPLINK_40G' | 'UPLINK_100G'>('');
	let rackUplinkCount = $state<
		'' | 'UPLINK_COUNT_1' | 'UPLINK_COUNT_2' | 'UPLINK_COUNT_3' | 'UPLINK_COUNT_4'
	>('');
	let savingRackProperties = $state(false);

	async function submitRackProperties(): Promise<void> {
		if (!siteDetailId) return;
		savingRackProperties = true;
		try {
			await client().send(
				new UpdateSiteRackPhysicalPropertiesCommand({
					SiteId: siteDetailId,
					PowerDrawKva: rackPowerDrawKva || undefined,
					PowerPhase: rackPowerPhase || undefined,
					PowerConnector: rackPowerConnector || undefined,
					PowerFeedDrop: rackPowerFeedDrop || undefined,
					UplinkGbps: rackUplinkGbps || undefined,
					UplinkCount: rackUplinkCount || undefined
				})
			);
			toast.success('Rack physical properties updated');
			const resp = await client().send(new GetSiteCommand({ SiteId: siteDetailId }));
			viewedSite = resp.Site ?? viewedSite;
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			savingRackProperties = false;
		}
	}

	// --- Orders: create / cancel / detail (no update, no plain delete) ---

	let createOrderModal = $state<Modal | null>(null);
	let creatingOrder = $state(false);
	let createOrderError = $state<string | null>(null);
	let newOrderOutpostId = $state('');
	let newOrderPaymentOption = $state<'ALL_UPFRONT' | 'NO_UPFRONT' | 'PARTIAL_UPFRONT'>('NO_UPFRONT');
	let newOrderPaymentTerm = $state<'' | 'ONE_YEAR' | 'THREE_YEARS' | 'FIVE_YEARS'>('');
	let newOrderQuoteId = $state('');
	let newOrderCatalogItemId = $state('');
	let newOrderQuantity = $state(1);

	function openCreateOrderModal(): void {
		createOrderError = null;
		newOrderOutpostId = selectedOutpostId;
		newOrderPaymentOption = 'NO_UPFRONT';
		newOrderPaymentTerm = '';
		newOrderQuoteId = '';
		newOrderCatalogItemId = '';
		newOrderQuantity = 1;
		createOrderModal?.open();
	}

	async function submitCreateOrder(): Promise<void> {
		if (!newOrderOutpostId.trim()) {
			createOrderError = 'Outpost ID is required.';
			return;
		}
		creatingOrder = true;
		createOrderError = null;
		try {
			await client().send(
				new CreateOrderCommand({
					OutpostIdentifier: newOrderOutpostId.trim(),
					PaymentOption: newOrderPaymentOption,
					PaymentTerm: newOrderPaymentTerm || undefined,
					QuoteIdentifier: newOrderQuoteId.trim() || undefined,
					LineItems: newOrderCatalogItemId.trim()
						? [{ CatalogItemId: newOrderCatalogItemId.trim(), Quantity: newOrderQuantity }]
						: undefined
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
			message: `Cancel order "${o.OrderId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new CancelOrderCommand({ OrderId: o.OrderId }));
			toast.success('Order cancelled');
			await tabLoader.refresh('orders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let orderDetailModal = $state<Modal | null>(null);
	let viewedOrder = $state<Order | null>(null);
	let orderDetailLoading = $state(false);
	let orderDetailError = $state<string | null>(null);

	async function openOrderDetail(o: OrderSummary): Promise<void> {
		if (!o.OrderId) return;
		viewedOrder = null;
		orderDetailError = null;
		orderDetailModal?.open();
		orderDetailLoading = true;
		try {
			const resp = await client().send(new GetOrderCommand({ OrderId: o.OrderId }));
			viewedOrder = resp.Order ?? null;
		} catch (e) {
			orderDetailError = describeError(e);
		} finally {
			orderDetailLoading = false;
		}
	}

	// --- Quotes: create / update / delete / detail ---

	let createQuoteModal = $state<Modal | null>(null);
	let creatingQuote = $state(false);
	let createQuoteError = $state<string | null>(null);
	let newQuoteOutpostId = $state('');
	let newQuoteCountryCode = $state('');
	let newQuoteCapacityType = $state<'EC2' | 'EBS' | 'S3'>('EC2');
	let newQuoteUnit = $state('');
	let newQuoteQuantity = $state(1);
	let newQuoteDescription = $state('');

	function openCreateQuoteModal(): void {
		createQuoteError = null;
		newQuoteOutpostId = selectedOutpostId;
		newQuoteCountryCode = '';
		newQuoteCapacityType = 'EC2';
		newQuoteUnit = '';
		newQuoteQuantity = 1;
		newQuoteDescription = '';
		createQuoteModal?.open();
	}

	async function submitCreateQuote(): Promise<void> {
		if (!newQuoteCountryCode.trim() || !newQuoteUnit.trim()) {
			createQuoteError = 'Country code and capacity unit are required.';
			return;
		}
		creatingQuote = true;
		createQuoteError = null;
		try {
			await client().send(
				new CreateQuoteCommand({
					OutpostIdentifier: newQuoteOutpostId.trim() || undefined,
					CountryCode: newQuoteCountryCode.trim(),
					RequestedCapacities: [
						{
							QuoteCapacityType: newQuoteCapacityType,
							Unit: newQuoteUnit.trim(),
							Quantity: newQuoteQuantity
						}
					],
					Description: newQuoteDescription.trim() || undefined
				})
			);
			toast.success('Quote created');
			createQuoteModal?.close();
			await tabLoader.refresh('quotes');
		} catch (e) {
			const msg = describeError(e);
			createQuoteError = msg;
			toast.error(msg);
		} finally {
			creatingQuote = false;
		}
	}

	let editQuoteModal = $state<Modal | null>(null);
	let editingQuote = $state(false);
	let editQuoteError = $state<string | null>(null);
	let editQuoteId = $state('');
	let editQuoteOutpostId = $state('');
	let editQuoteCountryCode = $state('');
	let editQuoteDescription = $state('');

	function openEditQuoteModal(q: QuoteSummary): void {
		if (!q.QuoteId) return;
		editQuoteError = null;
		editQuoteId = q.QuoteId;
		editQuoteOutpostId = q.OutpostArn ?? '';
		editQuoteCountryCode = q.CountryCode ?? '';
		editQuoteDescription = q.Description ?? '';
		editQuoteModal?.open();
	}

	async function submitEditQuote(): Promise<void> {
		if (!editQuoteId) return;
		editingQuote = true;
		editQuoteError = null;
		try {
			await client().send(
				new UpdateQuoteCommand({
					QuoteIdentifier: editQuoteId,
					OutpostIdentifier: editQuoteOutpostId.trim() || undefined,
					CountryCode: editQuoteCountryCode.trim() || undefined,
					Description: editQuoteDescription.trim() || undefined
				})
			);
			toast.success('Quote updated');
			editQuoteModal?.close();
			await tabLoader.refresh('quotes');
		} catch (e) {
			const msg = describeError(e);
			editQuoteError = msg;
			toast.error(msg);
		} finally {
			editingQuote = false;
		}
	}

	async function handleDeleteQuote(q: QuoteSummary): Promise<void> {
		if (!q.QuoteId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete quote',
			message: `Delete quote "${q.QuoteId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteQuoteCommand({ QuoteIdentifier: q.QuoteId }));
			toast.success('Quote deleted');
			await tabLoader.refresh('quotes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let quoteDetailModal = $state<Modal | null>(null);
	let viewedQuote = $state<Quote | null>(null);
	let quoteDetailLoading = $state(false);
	let quoteDetailError = $state<string | null>(null);

	async function openQuoteDetail(q: QuoteSummary): Promise<void> {
		if (!q.QuoteId) return;
		viewedQuote = null;
		quoteDetailError = null;
		quoteDetailModal?.open();
		quoteDetailLoading = true;
		try {
			const resp = await client().send(new GetQuoteCommand({ QuoteIdentifier: q.QuoteId }));
			viewedQuote = resp.Quote ?? null;
		} catch (e) {
			quoteDetailError = describeError(e);
		} finally {
			quoteDetailLoading = false;
		}
	}

	// --- Capacity Tasks: start / cancel / detail ---

	let startCapacityTaskModal = $state<Modal | null>(null);
	let startingCapacityTask = $state(false);
	let startCapacityTaskError = $state<string | null>(null);
	let newCtOutpostId = $state('');
	let newCtOrderId = $state('');
	let newCtAssetId = $state('');
	let newCtInstanceType = $state('');
	let newCtCount = $state(1);
	let newCtDryRun = $state(false);

	function openStartCapacityTaskModal(): void {
		startCapacityTaskError = null;
		newCtOutpostId = selectedOutpostId;
		newCtOrderId = '';
		newCtAssetId = '';
		newCtInstanceType = '';
		newCtCount = 1;
		newCtDryRun = false;
		startCapacityTaskModal?.open();
	}

	async function submitStartCapacityTask(): Promise<void> {
		if (!newCtOutpostId.trim() || !newCtInstanceType.trim()) {
			startCapacityTaskError = 'Outpost ID and instance type are required.';
			return;
		}
		startingCapacityTask = true;
		startCapacityTaskError = null;
		try {
			await client().send(
				new StartCapacityTaskCommand({
					OutpostIdentifier: newCtOutpostId.trim(),
					OrderId: newCtOrderId.trim() || undefined,
					AssetId: newCtAssetId.trim() || undefined,
					InstancePools: [{ InstanceType: newCtInstanceType.trim(), Count: newCtCount }],
					DryRun: newCtDryRun
				})
			);
			toast.success('Capacity task started');
			startCapacityTaskModal?.close();
			await tabLoader.refresh('capacityTasks');
		} catch (e) {
			const msg = describeError(e);
			startCapacityTaskError = msg;
			toast.error(msg);
		} finally {
			startingCapacityTask = false;
		}
	}

	async function handleCancelCapacityTask(t: CapacityTaskSummary): Promise<void> {
		if (!t.CapacityTaskId || !t.OutpostId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel capacity task',
			message: `Cancel capacity task "${t.CapacityTaskId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new CancelCapacityTaskCommand({
					CapacityTaskId: t.CapacityTaskId,
					OutpostIdentifier: t.OutpostId
				})
			);
			toast.success('Capacity task cancelled');
			await tabLoader.refresh('capacityTasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let capacityTaskDetailModal = $state<Modal | null>(null);
	let viewedCapacityTask = $state<CapacityTaskSummary | null>(null);
	let capacityTaskDetailLoading = $state(false);
	let capacityTaskDetailError = $state<string | null>(null);
	let blockingInstances = $state<BlockingInstance[]>([]);
	let loadingBlockingInstances = $state(false);

	async function openCapacityTaskDetail(t: CapacityTaskSummary): Promise<void> {
		if (!t.CapacityTaskId || !t.OutpostId) return;
		viewedCapacityTask = null;
		capacityTaskDetailError = null;
		blockingInstances = [];
		capacityTaskDetailModal?.open();
		capacityTaskDetailLoading = true;
		try {
			const resp = await client().send(
				new GetCapacityTaskCommand({ CapacityTaskId: t.CapacityTaskId, OutpostIdentifier: t.OutpostId })
			);
			viewedCapacityTask = {
				CapacityTaskId: resp.CapacityTaskId,
				OutpostId: resp.OutpostId,
				OrderId: resp.OrderId,
				AssetId: resp.AssetId,
				CapacityTaskStatus: resp.CapacityTaskStatus,
				CreationDate: resp.CreationDate,
				CompletionDate: resp.CompletionDate,
				LastModifiedDate: resp.LastModifiedDate
			};
		} catch (e) {
			capacityTaskDetailError = describeError(e);
		} finally {
			capacityTaskDetailLoading = false;
		}
	}

	// ListBlockingInstancesForCapacityTask/ListAssetInstances always return
	// empty in this backend today -- it has no cross-service EC2-on-Outposts
	// instance-placement data (documented gap in services/outposts/PARITY.md).
	// This is an honest empty result from a real call, not a stub.
	async function loadBlockingInstances(): Promise<void> {
		if (!viewedCapacityTask?.CapacityTaskId || !viewedCapacityTask.OutpostId) return;
		loadingBlockingInstances = true;
		try {
			const resp = await client().send(
				new ListBlockingInstancesForCapacityTaskCommand({
					CapacityTaskId: viewedCapacityTask.CapacityTaskId,
					OutpostIdentifier: viewedCapacityTask.OutpostId
				})
			);
			blockingInstances = resp.BlockingInstances ?? [];
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingBlockingInstances = false;
		}
	}

	// --- Assets: list only (no create/delete op exists) + detail ---

	let assetDetailModal = $state<Modal | null>(null);
	let viewedAsset = $state<AssetInfo | null>(null);
	let assetInstances = $state<AssetInstance[]>([]);
	let loadingAssetInstances = $state(false);

	function openAssetDetail(a: AssetInfo): void {
		viewedAsset = a;
		assetInstances = [];
		assetDetailModal?.open();
	}

	// Same honest-empty EC2-coupling gap as blocking instances above.
	async function loadAssetInstances(): Promise<void> {
		if (!viewedAsset?.AssetId || !selectedOutpostId) return;
		loadingAssetInstances = true;
		try {
			const resp = await client().send(
				new ListAssetInstancesCommand({
					OutpostIdentifier: selectedOutpostId,
					AssetIdFilter: [viewedAsset.AssetId]
				})
			);
			assetInstances = resp.AssetInstances ?? [];
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingAssetInstances = false;
		}
	}

	// --- Catalog: list + detail (read-only, AWS-maintained catalog) ---

	let catalogDetailModal = $state<Modal | null>(null);
	let viewedCatalogItem = $state<CatalogItem | null>(null);
	let catalogDetailLoading = $state(false);
	let catalogDetailError = $state<string | null>(null);

	async function openCatalogDetail(c: CatalogItem): Promise<void> {
		if (!c.CatalogItemId) return;
		viewedCatalogItem = null;
		catalogDetailError = null;
		catalogDetailModal?.open();
		catalogDetailLoading = true;
		try {
			const resp = await client().send(
				new GetCatalogItemCommand({ CatalogItemId: c.CatalogItemId })
			);
			viewedCatalogItem = resp.CatalogItem ?? null;
		} catch (e) {
			catalogDetailError = describeError(e);
		} finally {
			catalogDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Server}
		title="AWS Outposts"
		description="Extend AWS infrastructure and services to on-premises hardware"
		onRefresh={handleRefresh}
		color="indigo"
		service="outposts"
	>
		{#snippet actions()}
			{#if activeTab === 'outposts'}
				<button
					onclick={openCreateOutpostModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Outpost
				</button>
			{:else if activeTab === 'sites'}
				<button
					onclick={openCreateSiteModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Site
				</button>
			{:else if activeTab === 'orders'}
				<button
					onclick={openCreateOrderModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Order
				</button>
			{:else if activeTab === 'quotes'}
				<button
					onclick={openCreateQuoteModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Quote
				</button>
			{:else if activeTab === 'capacityTasks'}
				<button
					onclick={openStartCapacityTaskModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start Capacity Task
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="indigo" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTab === 'assets'}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="outpost-select" class="text-sm text-gray-500 dark:text-gray-400">Outpost</label>
					<select
						id="outpost-select"
						value={selectedOutpostId}
						onchange={(e) => onOutpostSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if outposts.length === 0}
							<option value="">No Outposts</option>
						{/if}
						{#each outposts as o (o.OutpostId)}
							<option value={o.OutpostId}>{o.Name || o.OutpostId}</option>
						{/each}
					</select>
				</div>
			{/if}

			{#if activeTab === 'orderableInstanceTypes'}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="oit-generation" class="text-sm text-gray-500 dark:text-gray-400"
						>Generation</label
					>
					<select
						id="oit-generation"
						bind:value={oitGenerationFilter}
						onchange={() => tabLoader.refresh('orderableInstanceTypes')}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="">All</option>
						<option value="GENERATION_1">Generation 1</option>
						<option value="GENERATION_2">Generation 2</option>
					</select>
				</div>
			{/if}

			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'outposts'}
				{#snippet outpostStatusCell(o: Outpost)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(o.LifeCycleStatus === 'ACTIVE')}"
						>{o.LifeCycleStatus ?? '—'}</span
					>
				{/snippet}
				{#snippet outpostActionsCell(o: Outpost)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openOutpostDetail(o)}
							title="View"
							aria-label="View Outpost {o.Name}"
							class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditOutpostModal(o)}
							title="Edit"
							aria-label="Edit Outpost {o.Name}"
							class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteOutpost(o)}
							title="Delete"
							aria-label="Delete Outpost {o.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const outpostColumns = defineColumns<Outpost>([
					{ key: 'Name', label: 'Name' },
					{ key: 'OutpostId', label: 'ID' },
					{ key: 'SiteId', label: 'Site ID' },
					{ key: 'LifeCycleStatus', label: 'Status', render: outpostStatusCell },
					{ key: 'AvailabilityZone', label: 'AZ' },
					{ key: 'actions', label: '', render: outpostActionsCell }
				])}
				<DataTable
					rows={filteredOutposts}
					rowKey={(o) => o.OutpostId ?? ''}
					columns={outpostColumns}
					loading={tabLoader.isLoading('outposts')}
					emptyMessage="No Outposts found"
				/>
				<LoadMore
					hasMore={!!outpostsNextToken}
					loading={loadingMoreOutposts}
					onLoadMore={loadMoreOutposts}
				/>
			{:else if activeTab === 'sites'}
				{#snippet siteActionsCell(s: Site)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openSiteDetail(s)}
							title="View"
							aria-label="View site {s.Name}"
							class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditSiteModal(s)}
							title="Edit"
							aria-label="Edit site {s.Name}"
							class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteSite(s)}
							title="Delete"
							aria-label="Delete site {s.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
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
				<LoadMore hasMore={!!sitesNextToken} loading={loadingMoreSites} onLoadMore={loadMoreSites} />
			{:else if activeTab === 'orders'}
				{#snippet orderStatusCell(o: OrderSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(o.Status === 'COMPLETED')}"
						>{o.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet orderSubmittedCell(o: OrderSummary)}
					{formatDate(o.OrderSubmissionDate)}
				{/snippet}
				{#snippet orderActionsCell(o: OrderSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openOrderDetail(o)}
							title="View"
							aria-label="View order {o.OrderId}"
							class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button
						>
						{#if o.Status !== 'CANCELLED' && o.Status !== 'COMPLETED' && o.Status !== 'ERROR'}
							<button
								onclick={() => handleCancelOrder(o)}
								title="Cancel"
								aria-label="Cancel order {o.OrderId}"
								class="text-gray-400 hover:text-red-500"><Ban class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const orderColumns = defineColumns<OrderSummary>([
					{ key: 'OrderId', label: 'Order ID' },
					{ key: 'OutpostId', label: 'Outpost ID' },
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
				<LoadMore
					hasMore={!!ordersNextToken}
					loading={loadingMoreOrders}
					onLoadMore={loadMoreOrders}
				/>
			{:else if activeTab === 'quotes'}
				{#snippet quoteStatusCell(q: QuoteSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(q.QuoteStatus === 'CREATED')}"
						>{q.QuoteStatus ?? '—'}</span
					>
				{/snippet}
				{#snippet quoteExpiresCell(q: QuoteSummary)}
					{formatDate(q.ExpirationDate)}
				{/snippet}
				{#snippet quoteActionsCell(q: QuoteSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openQuoteDetail(q)}
							title="View"
							aria-label="View quote {q.QuoteId}"
							class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditQuoteModal(q)}
							title="Edit"
							aria-label="Edit quote {q.QuoteId}"
							class="text-gray-400 hover:text-indigo-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteQuote(q)}
							title="Delete"
							aria-label="Delete quote {q.QuoteId}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const quoteColumns = defineColumns<QuoteSummary>([
					{ key: 'QuoteId', label: 'Quote ID' },
					{ key: 'CountryCode', label: 'Country' },
					{ key: 'QuoteStatus', label: 'Status', render: quoteStatusCell },
					{ key: 'ExpirationDate', label: 'Expires', render: quoteExpiresCell },
					{ key: 'actions', label: '', render: quoteActionsCell }
				])}
				<DataTable
					rows={filteredQuotes}
					rowKey={(q) => q.QuoteId ?? ''}
					columns={quoteColumns}
					loading={tabLoader.isLoading('quotes')}
					emptyMessage="No quotes found"
				/>
				<LoadMore
					hasMore={!!quotesNextToken}
					loading={loadingMoreQuotes}
					onLoadMore={loadMoreQuotes}
				/>
			{:else if activeTab === 'capacityTasks'}
				{#snippet ctStatusCell(t: CapacityTaskSummary)}
					<span
						class="text-xs px-2 py-1 rounded-full {statusClass(t.CapacityTaskStatus === 'COMPLETED')}"
						>{t.CapacityTaskStatus ?? '—'}</span
					>
				{/snippet}
				{#snippet ctCreatedCell(t: CapacityTaskSummary)}
					{formatDate(t.CreationDate)}
				{/snippet}
				{#snippet ctActionsCell(t: CapacityTaskSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openCapacityTaskDetail(t)}
							title="View"
							aria-label="View capacity task {t.CapacityTaskId}"
							class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button
						>
						{#if t.CapacityTaskStatus === 'REQUESTED' || t.CapacityTaskStatus === 'IN_PROGRESS'}
							<button
								onclick={() => handleCancelCapacityTask(t)}
								title="Cancel"
								aria-label="Cancel capacity task {t.CapacityTaskId}"
								class="text-gray-400 hover:text-red-500"><Ban class="w-4 h-4" /></button
							>
						{/if}
					</div>
				{/snippet}
				{@const ctColumns = defineColumns<CapacityTaskSummary>([
					{ key: 'CapacityTaskId', label: 'Task ID' },
					{ key: 'OutpostId', label: 'Outpost ID' },
					{ key: 'CapacityTaskStatus', label: 'Status', render: ctStatusCell },
					{ key: 'CreationDate', label: 'Created', render: ctCreatedCell },
					{ key: 'actions', label: '', render: ctActionsCell }
				])}
				<DataTable
					rows={filteredCapacityTasks}
					rowKey={(t) => t.CapacityTaskId ?? ''}
					columns={ctColumns}
					loading={tabLoader.isLoading('capacityTasks')}
					emptyMessage="No capacity tasks found"
				/>
				<LoadMore
					hasMore={!!capacityTasksNextToken}
					loading={loadingMoreCapacityTasks}
					onLoadMore={loadMoreCapacityTasks}
				/>
			{:else if activeTab === 'assets'}
				{#snippet assetActionsCell(a: AssetInfo)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => openAssetDetail(a)}
							title="View"
							aria-label="View asset {a.AssetId}"
							class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const assetColumns = defineColumns<AssetInfo>([
					{ key: 'AssetId', label: 'Asset ID' },
					{ key: 'RackId', label: 'Rack ID' },
					{ key: 'AssetType', label: 'Type' },
					{ key: 'actions', label: '', render: assetActionsCell }
				])}
				<DataTable
					rows={filteredAssets}
					rowKey={(a) => a.AssetId ?? ''}
					columns={assetColumns}
					loading={tabLoader.isLoading('assets')}
					emptyMessage={selectedOutpostId ? 'No assets found' : 'Select an Outpost to see its assets'}
				/>
				<LoadMore
					hasMore={!!assetsNextToken}
					loading={loadingMoreAssets}
					onLoadMore={loadMoreAssets}
				/>
			{:else if activeTab === 'catalog'}
				{#snippet catalogActionsCell(c: CatalogItem)}
					<div class="flex items-center justify-end">
						<button
							onclick={() => openCatalogDetail(c)}
							title="View"
							aria-label="View catalog item {c.CatalogItemId}"
							class="text-gray-400 hover:text-indigo-500"><Eye class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const catalogColumns = defineColumns<CatalogItem>([
					{ key: 'CatalogItemId', label: 'Item ID' },
					{ key: 'ItemStatus', label: 'Status' },
					{ key: 'PowerKva', label: 'Power (kVA)' },
					{ key: 'WeightLbs', label: 'Weight (lbs)' },
					{ key: 'actions', label: '', render: catalogActionsCell }
				])}
				<DataTable
					rows={filteredCatalog}
					rowKey={(c) => c.CatalogItemId ?? ''}
					columns={catalogColumns}
					loading={tabLoader.isLoading('catalog')}
					emptyMessage="No catalog items found"
				/>
				<LoadMore
					hasMore={!!catalogNextToken}
					loading={loadingMoreCatalog}
					onLoadMore={loadMoreCatalog}
				/>
			{:else if activeTab === 'orderableInstanceTypes'}
				{@const oitColumns = defineColumns<InstanceTypeItem>([
					{ key: 'InstanceType', label: 'Instance Type' },
					{ key: 'VCPUs', label: 'vCPUs' }
				])}
				<DataTable
					rows={filteredOit}
					rowKey={(i) => i.InstanceType ?? ''}
					columns={oitColumns}
					loading={tabLoader.isLoading('orderableInstanceTypes')}
					emptyMessage="No orderable instance types found"
				/>
				<LoadMore
					hasMore={!!oitNextToken}
					loading={loadingMoreOit}
					onLoadMore={loadMoreOit}
				/>
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createOutpostModal} title="Create Outpost">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-op-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-op-name"
					bind:value={newOutpostName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-op-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="new-op-desc"
					bind:value={newOutpostDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-op-site" class="text-sm text-slate-600 dark:text-slate-300"
					>Site ID or ARN</label
				>
				<input
					id="new-op-site"
					bind:value={newOutpostSiteId}
					list="site-id-options"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
				<datalist id="site-id-options">
					{#each sites as s (s.SiteId)}
						<option value={s.SiteId}>{s.Name}</option>
					{/each}
				</datalist>
			</div>
			<div>
				<label for="new-op-az" class="text-sm text-slate-600 dark:text-slate-300"
					>Availability Zone (optional)</label
				>
				<input
					id="new-op-az"
					bind:value={newOutpostAz}
					placeholder="us-east-1a"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-op-hw" class="text-sm text-slate-600 dark:text-slate-300"
					>Hardware Type (optional)</label
				>
				<select
					id="new-op-hw"
					bind:value={newOutpostHardware}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Unspecified</option>
					<option value="RACK">Rack</option>
					<option value="SERVER">Server</option>
				</select>
			</div>
			{#if createOutpostError}
				<p class="text-sm text-red-600 dark:text-red-400">{createOutpostError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createOutpostModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateOutpost}
			disabled={creatingOutpost}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{creatingOutpost ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editOutpostModal} title="Edit Outpost">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-op-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-op-name"
					bind:value={editOutpostName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-op-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="edit-op-desc"
					bind:value={editOutpostDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-op-hw" class="text-sm text-slate-600 dark:text-slate-300">Hardware Type</label>
				<select
					id="edit-op-hw"
					bind:value={editOutpostHardware}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Unspecified</option>
					<option value="RACK">Rack</option>
					<option value="SERVER">Server</option>
				</select>
			</div>
			{#if editOutpostError}
				<p class="text-sm text-red-600 dark:text-red-400">{editOutpostError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editOutpostModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditOutpost}
			disabled={editingOutpost}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{editingOutpost ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={outpostDetailModal} title="Outpost Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if outpostDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if outpostDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{outpostDetailError}</p>
			{:else if viewedOutpost}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedOutpost.OutpostId}</div>
					<div><span class="text-slate-500">Status:</span> {viewedOutpost.LifeCycleStatus}</div>
					<div><span class="text-slate-500">Site ID:</span> {viewedOutpost.SiteId}</div>
					<div><span class="text-slate-500">AZ:</span> {viewedOutpost.AvailabilityZone}</div>
					<div>
						<span class="text-slate-500">Hardware:</span> {viewedOutpost.SupportedHardwareType ?? '—'}
					</div>
					{#if viewedOutpost.OutpostArn}
						<div class="col-span-2 break-all">
							<span class="text-slate-500">ARN:</span> {viewedOutpost.OutpostArn}
						</div>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<div class="flex items-center gap-2">
						<h3 class="text-sm font-semibold flex-1">Instance Types</h3>
						<button
							onclick={loadConfiguredInstanceTypes}
							disabled={loadingInstanceTypes}
							class="text-xs text-indigo-600 hover:underline">Load configured</button
						>
						<button
							onclick={loadSupportedInstanceTypes}
							disabled={loadingInstanceTypes}
							class="text-xs text-indigo-600 hover:underline">Load supported</button
						>
					</div>
					{#if instanceTypesConfigured.length > 0}
						<p class="text-xs text-slate-500">
							Configured: {instanceTypesConfigured
								.map((i) => `${i.InstanceType} (${i.VCPUs} vCPU)`)
								.join(', ')}
						</p>
					{/if}
					{#if instanceTypesSupported.length > 0}
						<p class="text-xs text-slate-500">
							Supported: {instanceTypesSupported
								.map((i) => `${i.InstanceType} (${i.VCPUs} vCPU)`)
								.join(', ')}
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<div class="flex items-center justify-between">
						<h3 class="text-sm font-semibold">Billing</h3>
						<button onclick={loadBilling} disabled={loadingBilling} class="text-xs text-indigo-600 hover:underline"
							>Load</button
						>
					</div>
					{#if billingContractEndDate}
						<p class="text-xs text-slate-500">Contract ends: {billingContractEndDate}</p>
					{/if}
					{#each billingSubscriptions as sub (sub.SubscriptionId)}
						<p class="text-xs text-slate-500">
							{sub.SubscriptionType}: {sub.SubscriptionStatus}, {sub.UpfrontPrice ?? 0} upfront + {sub.MonthlyRecurringPrice ??
								0}/mo {sub.Currency}
						</p>
					{/each}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<div class="flex items-center justify-between">
						<h3 class="text-sm font-semibold">Decommission</h3>
						<button onclick={handleDecommission} class="text-xs text-red-600 hover:underline"
							>Start decommission</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Renewal</h3>
					<div class="flex gap-2 flex-wrap items-center">
						<label class="sr-only" for="renewal-payment-option">Payment option</label>
						<select
							id="renewal-payment-option"
							bind:value={renewalPaymentOption}
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="ALL_UPFRONT">All Upfront</option>
							<option value="NO_UPFRONT">No Upfront</option>
							<option value="PARTIAL_UPFRONT">Partial Upfront</option>
						</select>
						<label class="sr-only" for="renewal-payment-term">Payment term</label>
						<select
							id="renewal-payment-term"
							bind:value={renewalPaymentTerm}
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="ONE_YEAR">1 Year</option>
							<option value="THREE_YEARS">3 Years</option>
							<option value="FIVE_YEARS">5 Years</option>
						</select>
						<button
							onclick={loadRenewalPricing}
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-800"
							>Preview pricing</button
						>
						<button
							onclick={submitCreateRenewal}
							disabled={savingRenewal}
							class="text-xs px-2 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700"
							>{savingRenewal ? 'Submitting...' : 'Create renewal'}</button
						>
					</div>
					{#if renewalPricingResult}
						<p class="text-xs text-slate-500">
							Pricing result: {renewalPricingResult.result}, {renewalPricingResult.options.length} option(s)
						</p>
					{/if}
					{#if renewalResult}
						<p class="text-xs text-slate-500">
							Renewal: {renewalResult.upfrontPrice ?? 0} upfront + {renewalResult.monthlyRecurringPrice ??
								0}/mo {renewalResult.currency}
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Connections</h3>
					<p class="text-xs text-slate-500">
						No list operation exists for connections -- start one or look one up by ID.
					</p>
					<div class="grid grid-cols-2 gap-2">
						<label class="sr-only" for="conn-asset-id">Asset ID</label>
						<input
							id="conn-asset-id"
							bind:value={connAssetId}
							placeholder="Asset ID"
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="conn-device-index">Network interface device index</label>
						<input
							id="conn-device-index"
							type="number"
							bind:value={connDeviceIndex}
							placeholder="Device index"
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="conn-public-key">Client public key</label>
						<input
							id="conn-public-key"
							bind:value={connClientPublicKey}
							placeholder="Client public key"
							class="col-span-2 text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="conn-serial">Device serial number</label>
						<input
							id="conn-serial"
							bind:value={connDeviceSerial}
							placeholder="Device serial (optional)"
							class="col-span-2 text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
					</div>
					<button
						onclick={submitStartConnection}
						disabled={startingConn}
						class="text-xs px-2 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700"
						>{startingConn ? 'Starting...' : 'Start connection'}</button
					>
					{#if connStartResult}
						<p class="text-xs text-slate-500 break-all">
							Connection {connStartResult.connectionId}, underlay IP {connStartResult.underlayIpAddress}
						</p>
					{/if}
					<div class="flex gap-2">
						<label class="sr-only" for="conn-lookup-id">Connection ID to look up</label>
						<input
							id="conn-lookup-id"
							bind:value={connLookupId}
							placeholder="Connection ID"
							class="flex-1 text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={lookupConnection}
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-800"
							>Look up</button
						>
					</div>
					{#if connLookupResult}
						<p class="text-xs text-slate-500 break-all">
							Endpoint {connLookupResult.serverEndpoint}, tunnel {connLookupResult.clientTunnelAddress} /
							{connLookupResult.serverTunnelAddress}
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each Object.entries(outpostTags) as [key, value] (key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{key}={value}
								<button
									onclick={() => removeOutpostTag(key)}
									aria-label="Remove tag {key}"
									class="text-slate-400 hover:text-red-500"><Ban class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="outpost-tag-key">Tag key</label>
						<input
							id="outpost-tag-key"
							bind:value={addTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="outpost-tag-value">Tag value</label>
						<input
							id="outpost-tag-value"
							bind:value={addTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddOutpostTag}
							class="text-xs px-2 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700">Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => outpostDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createSiteModal} title="Create Site">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-site-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-site-name"
					bind:value={newSiteName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-site-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="new-site-desc"
					bind:value={newSiteDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-site-notes" class="text-sm text-slate-600 dark:text-slate-300">Notes</label>
				<input
					id="new-site-notes"
					bind:value={newSiteNotes}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={newSiteIncludeAddress} /> Include operating address
			</label>
			{#if newSiteIncludeAddress}
				<div class="grid grid-cols-2 gap-2">
					<input
						bind:value={newSiteContactName}
						placeholder="Contact name"
						aria-label="Contact name"
						class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
					/>
					<input
						bind:value={newSiteContactPhone}
						placeholder="Contact phone"
						aria-label="Contact phone"
						class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
					/>
					<input
						bind:value={newSiteAddressLine1}
						placeholder="Address line 1"
						aria-label="Address line 1"
						class="col-span-2 px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
					/>
					<input
						bind:value={newSiteCity}
						placeholder="City"
						aria-label="City"
						class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
					/>
					<input
						bind:value={newSiteStateOrRegion}
						placeholder="State/Region"
						aria-label="State or region"
						class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
					/>
					<input
						bind:value={newSitePostalCode}
						placeholder="Postal code"
						aria-label="Postal code"
						class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
					/>
					<input
						bind:value={newSiteCountryCode}
						placeholder="Country code"
						aria-label="Country code"
						class="px-2 py-1 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
					/>
				</div>
			{/if}
			{#if createSiteError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSiteError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createSiteModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateSite}
			disabled={creatingSite}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{creatingSite ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editSiteModal} title="Edit Site">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-site-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-site-name"
					bind:value={editSiteName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-site-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="edit-site-desc"
					bind:value={editSiteDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-site-notes" class="text-sm text-slate-600 dark:text-slate-300">Notes</label>
				<input
					id="edit-site-notes"
					bind:value={editSiteNotes}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editSiteError}
				<p class="text-sm text-red-600 dark:text-red-400">{editSiteError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editSiteModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditSite}
			disabled={editingSite}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{editingSite ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={siteDetailModal} title="Site Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if siteDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if siteDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{siteDetailError}</p>
			{:else if viewedSite}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedSite.SiteId}</div>
					<div><span class="text-slate-500">Name:</span> {viewedSite.Name}</div>
					<div><span class="text-slate-500">City:</span> {viewedSite.OperatingAddressCity ?? '—'}</div>
					<div>
						<span class="text-slate-500">Country:</span> {viewedSite.OperatingAddressCountryCode ?? '—'}
					</div>
					{#if viewedSite.Notes}
						<div class="col-span-2"><span class="text-slate-500">Notes:</span> {viewedSite.Notes}</div>
					{/if}
					{#if viewedSite.SiteArn}
						<div class="col-span-2 break-all">
							<span class="text-slate-500">ARN:</span> {viewedSite.SiteArn}
						</div>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Address</h3>
					<div class="flex gap-2 items-center">
						<label class="sr-only" for="site-address-type">Address type</label>
						<select
							id="site-address-type"
							bind:value={siteAddressType}
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="OPERATING_ADDRESS">Operating Address</option>
							<option value="SHIPPING_ADDRESS">Shipping Address</option>
						</select>
						<button
							onclick={loadSiteAddress}
							disabled={loadingSiteAddress}
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-800"
							>Get</button
						>
					</div>
					{#if siteAddressResult}
						<p class="text-xs text-slate-500">
							{siteAddressResult.contactName}, {siteAddressResult.addressLine1}, {siteAddressResult.city},
							{siteAddressResult.stateOrRegion} {siteAddressResult.postalCode}, {siteAddressResult.countryCode}
						</p>
					{/if}
					<details class="text-xs">
						<summary class="cursor-pointer text-indigo-600">Update address</summary>
						<div class="grid grid-cols-2 gap-2 mt-2">
							<input
								bind:value={updAddrContactName}
								placeholder="Contact name"
								aria-label="Contact name"
								class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
							<input
								bind:value={updAddrContactPhone}
								placeholder="Contact phone"
								aria-label="Contact phone"
								class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
							<input
								bind:value={updAddrLine1}
								placeholder="Address line 1"
								aria-label="Address line 1"
								class="col-span-2 px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
							<input
								bind:value={updAddrCity}
								placeholder="City"
								aria-label="City"
								class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
							<input
								bind:value={updAddrStateOrRegion}
								placeholder="State/Region"
								aria-label="State or region"
								class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
							<input
								bind:value={updAddrPostalCode}
								placeholder="Postal code"
								aria-label="Postal code"
								class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
							<input
								bind:value={updAddrCountryCode}
								placeholder="Country code"
								aria-label="Country code"
								class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
						</div>
						<button
							onclick={submitUpdateSiteAddress}
							disabled={savingSiteAddress}
							class="mt-2 text-xs px-2 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700"
							>{savingSiteAddress ? 'Saving...' : 'Update address'}</button
						>
					</details>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Rack Physical Properties</h3>
					<div class="grid grid-cols-2 gap-2 text-xs">
						<label class="sr-only" for="rack-power-draw">Power draw</label>
						<select
							id="rack-power-draw"
							bind:value={rackPowerDrawKva}
							class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="">Power draw (kVA)</option>
							<option value="POWER_5_KVA">5 kVA</option>
							<option value="POWER_10_KVA">10 kVA</option>
							<option value="POWER_15_KVA">15 kVA</option>
							<option value="POWER_30_KVA">30 kVA</option>
						</select>
						<label class="sr-only" for="rack-power-phase">Power phase</label>
						<select
							id="rack-power-phase"
							bind:value={rackPowerPhase}
							class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="">Power phase</option>
							<option value="SINGLE_PHASE">Single phase</option>
							<option value="THREE_PHASE">Three phase</option>
						</select>
						<label class="sr-only" for="rack-power-connector">Power connector</label>
						<select
							id="rack-power-connector"
							bind:value={rackPowerConnector}
							class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="">Power connector</option>
							<option value="AH530P7W">AH530P7W</option>
							<option value="AH532P6W">AH532P6W</option>
							<option value="CS8365C">CS8365C</option>
							<option value="IEC309">IEC309</option>
							<option value="L6_30P">L6-30P</option>
						</select>
						<label class="sr-only" for="rack-power-feed">Power feed drop</label>
						<select
							id="rack-power-feed"
							bind:value={rackPowerFeedDrop}
							class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="">Power feed drop</option>
							<option value="ABOVE_RACK">Above rack</option>
							<option value="BELOW_RACK">Below rack</option>
						</select>
						<label class="sr-only" for="rack-uplink-gbps">Uplink speed</label>
						<select
							id="rack-uplink-gbps"
							bind:value={rackUplinkGbps}
							class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="">Uplink speed</option>
							<option value="UPLINK_1G">1 Gbps</option>
							<option value="UPLINK_10G">10 Gbps</option>
							<option value="UPLINK_40G">40 Gbps</option>
							<option value="UPLINK_100G">100 Gbps</option>
						</select>
						<label class="sr-only" for="rack-uplink-count">Uplink count</label>
						<select
							id="rack-uplink-count"
							bind:value={rackUplinkCount}
							class="px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="">Uplink count</option>
							<option value="UPLINK_COUNT_1">1</option>
							<option value="UPLINK_COUNT_2">2</option>
							<option value="UPLINK_COUNT_3">3</option>
							<option value="UPLINK_COUNT_4">4</option>
						</select>
					</div>
					<button
						onclick={submitRackProperties}
						disabled={savingRackProperties}
						class="text-xs px-2 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700"
						>{savingRackProperties ? 'Saving...' : 'Update rack properties'}</button
					>
					{#if viewedSite.RackPhysicalProperties}
						<p class="text-xs text-slate-500">
							Current: {viewedSite.RackPhysicalProperties.PowerDrawKva ?? '—'} / {viewedSite
								.RackPhysicalProperties.PowerPhase ?? '—'} / {viewedSite.RackPhysicalProperties
								.UplinkGbps ?? '—'}
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each Object.entries(siteTags) as [key, value] (key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{key}={value}
								<button
									onclick={() => removeSiteTag(key)}
									aria-label="Remove tag {key}"
									class="text-slate-400 hover:text-red-500"><Ban class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="site-tag-key">Tag key</label>
						<input
							id="site-tag-key"
							bind:value={addTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="site-tag-value">Tag value</label>
						<input
							id="site-tag-value"
							bind:value={addTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddSiteTag}
							class="text-xs px-2 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700">Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => siteDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createOrderModal} title="Create Order">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-order-outpost" class="text-sm text-slate-600 dark:text-slate-300"
					>Outpost ID or ARN</label
				>
				<input
					id="new-order-outpost"
					bind:value={newOrderOutpostId}
					list="outpost-id-options"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
				<datalist id="outpost-id-options">
					{#each outposts as o (o.OutpostId)}
						<option value={o.OutpostId}>{o.Name}</option>
					{/each}
				</datalist>
			</div>
			<div>
				<label for="new-order-payment-option" class="text-sm text-slate-600 dark:text-slate-300"
					>Payment Option</label
				>
				<select
					id="new-order-payment-option"
					bind:value={newOrderPaymentOption}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="ALL_UPFRONT">All Upfront</option>
					<option value="NO_UPFRONT">No Upfront</option>
					<option value="PARTIAL_UPFRONT">Partial Upfront</option>
				</select>
			</div>
			<div>
				<label for="new-order-payment-term" class="text-sm text-slate-600 dark:text-slate-300"
					>Payment Term (optional)</label
				>
				<select
					id="new-order-payment-term"
					bind:value={newOrderPaymentTerm}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Unspecified</option>
					<option value="ONE_YEAR">1 Year</option>
					<option value="THREE_YEARS">3 Years</option>
					<option value="FIVE_YEARS">5 Years</option>
				</select>
			</div>
			<div>
				<label for="new-order-quote" class="text-sm text-slate-600 dark:text-slate-300"
					>Quote ID (optional)</label
				>
				<input
					id="new-order-quote"
					bind:value={newOrderQuoteId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="new-order-catalog-item" class="text-sm text-slate-600 dark:text-slate-300"
						>Catalog Item ID (optional)</label
					>
					<input
						id="new-order-catalog-item"
						bind:value={newOrderCatalogItemId}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="new-order-qty" class="text-sm text-slate-600 dark:text-slate-300">Quantity</label>
					<input
						id="new-order-qty"
						type="number"
						bind:value={newOrderQuantity}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			{#if createOrderError}
				<p class="text-sm text-red-600 dark:text-red-400">{createOrderError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createOrderModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateOrder}
			disabled={creatingOrder}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{creatingOrder ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={orderDetailModal} title="Order Detail">
	{#snippet children()}
		<div class="space-y-3 max-h-[70vh] overflow-y-auto">
			{#if orderDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if orderDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{orderDetailError}</p>
			{:else if viewedOrder}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">Order ID:</span> {viewedOrder.OrderId}</div>
					<div><span class="text-slate-500">Outpost ID:</span> {viewedOrder.OutpostId}</div>
					<div><span class="text-slate-500">Status:</span> {viewedOrder.Status}</div>
					<div><span class="text-slate-500">Payment:</span> {viewedOrder.PaymentOption}</div>
					<div>
						<span class="text-slate-500">Submitted:</span> {formatDate(viewedOrder.OrderSubmissionDate)}
					</div>
					<div>
						<span class="text-slate-500">Fulfilled:</span> {formatDate(viewedOrder.OrderFulfilledDate)}
					</div>
				</div>
				{#if viewedOrder.LineItems && viewedOrder.LineItems.length > 0}
					<h4 class="text-sm font-semibold">Line Items</h4>
					<table class="w-full text-xs">
						<thead>
							<tr class="text-left text-slate-500">
								<th class="pr-2">Catalog Item</th>
								<th class="pr-2">Quantity</th>
								<th>Status</th>
							</tr>
						</thead>
						<tbody>
							{#each viewedOrder.LineItems as li (li.LineItemId)}
								<tr>
									<td class="pr-2">{li.CatalogItemId}</td>
									<td class="pr-2">{li.Quantity}</td>
									<td>{li.Status}</td>
								</tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => orderDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createQuoteModal} title="Create Quote">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-quote-outpost" class="text-sm text-slate-600 dark:text-slate-300"
					>Outpost ID or ARN (optional)</label
				>
				<input
					id="new-quote-outpost"
					bind:value={newQuoteOutpostId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-quote-country" class="text-sm text-slate-600 dark:text-slate-300"
					>Country Code</label
				>
				<input
					id="new-quote-country"
					bind:value={newQuoteCountryCode}
					placeholder="US"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div class="grid grid-cols-3 gap-2">
				<div>
					<label for="new-quote-cap-type" class="text-sm text-slate-600 dark:text-slate-300"
						>Capacity Type</label
					>
					<select
						id="new-quote-cap-type"
						bind:value={newQuoteCapacityType}
						class="mt-1 w-full px-2 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					>
						<option value="EC2">EC2</option>
						<option value="EBS">EBS</option>
						<option value="S3">S3</option>
					</select>
				</div>
				<div>
					<label for="new-quote-unit" class="text-sm text-slate-600 dark:text-slate-300">Unit</label>
					<input
						id="new-quote-unit"
						bind:value={newQuoteUnit}
						placeholder="c5.24xlarge"
						class="mt-1 w-full px-2 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="new-quote-qty" class="text-sm text-slate-600 dark:text-slate-300">Quantity</label>
					<input
						id="new-quote-qty"
						type="number"
						bind:value={newQuoteQuantity}
						class="mt-1 w-full px-2 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			<div>
				<label for="new-quote-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description (optional)</label
				>
				<input
					id="new-quote-desc"
					bind:value={newQuoteDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<p class="text-xs text-slate-500">
				This form submits a single capacity requirement; the real API accepts multiple.
			</p>
			{#if createQuoteError}
				<p class="text-sm text-red-600 dark:text-red-400">{createQuoteError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createQuoteModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateQuote}
			disabled={creatingQuote}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{creatingQuote ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editQuoteModal} title="Edit Quote">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-quote-outpost" class="text-sm text-slate-600 dark:text-slate-300"
					>Outpost ID or ARN</label
				>
				<input
					id="edit-quote-outpost"
					bind:value={editQuoteOutpostId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-quote-country" class="text-sm text-slate-600 dark:text-slate-300"
					>Country Code</label
				>
				<input
					id="edit-quote-country"
					bind:value={editQuoteCountryCode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-quote-desc" class="text-sm text-slate-600 dark:text-slate-300"
					>Description</label
				>
				<input
					id="edit-quote-desc"
					bind:value={editQuoteDescription}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editQuoteError}
				<p class="text-sm text-red-600 dark:text-red-400">{editQuoteError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editQuoteModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditQuote}
			disabled={editingQuote}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{editingQuote ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={quoteDetailModal} title="Quote Detail">
	{#snippet children()}
		<div class="space-y-3 max-h-[70vh] overflow-y-auto">
			{#if quoteDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if quoteDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{quoteDetailError}</p>
			{:else if viewedQuote}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">Quote ID:</span> {viewedQuote.QuoteId}</div>
					<div><span class="text-slate-500">Status:</span> {viewedQuote.QuoteStatus}</div>
					<div><span class="text-slate-500">Country:</span> {viewedQuote.CountryCode}</div>
					<div><span class="text-slate-500">Outpost ARN:</span> {viewedQuote.OutpostArn ?? '—'}</div>
					<div>
						<span class="text-slate-500">Created:</span> {formatDate(viewedQuote.CreatedDate)}
					</div>
					<div>
						<span class="text-slate-500">Expires:</span> {formatDate(viewedQuote.ExpirationDate)}
					</div>
				</div>
				{#if viewedQuote.OrderingRequirements && viewedQuote.OrderingRequirements.length > 0}
					<h4 class="text-sm font-semibold">Ordering Requirements</h4>
					<ul class="text-xs list-disc list-inside">
						{#each viewedQuote.OrderingRequirements as req, i (i)}
							<li>{req.OrderingRequirementType}: {req.StatusMessage}</li>
						{/each}
					</ul>
				{/if}
				{#if viewedQuote.QuoteOptions && viewedQuote.QuoteOptions.length > 0}
					<h4 class="text-sm font-semibold">Quote Options</h4>
					{#each viewedQuote.QuoteOptions as opt, i (i)}
						{#each opt.PricingOptions ?? [] as pricing, j (j)}
							<p class="text-xs text-slate-500">
								{pricing.SubscriptionPricingDetails?.UpfrontPrice ?? 0} upfront + {pricing
									.SubscriptionPricingDetails?.MonthlyRecurringPrice ?? 0}/mo
							</p>
						{/each}
					{/each}
				{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => quoteDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={startCapacityTaskModal} title="Start Capacity Task">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-ct-outpost" class="text-sm text-slate-600 dark:text-slate-300"
					>Outpost ID or ARN</label
				>
				<input
					id="new-ct-outpost"
					bind:value={newCtOutpostId}
					list="outpost-id-options-ct"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
				<datalist id="outpost-id-options-ct">
					{#each outposts as o (o.OutpostId)}
						<option value={o.OutpostId}>{o.Name}</option>
					{/each}
				</datalist>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="new-ct-order" class="text-sm text-slate-600 dark:text-slate-300"
						>Order ID (optional)</label
					>
					<input
						id="new-ct-order"
						bind:value={newCtOrderId}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="new-ct-asset" class="text-sm text-slate-600 dark:text-slate-300"
						>Asset ID (optional)</label
					>
					<input
						id="new-ct-asset"
						bind:value={newCtAssetId}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="new-ct-instance-type" class="text-sm text-slate-600 dark:text-slate-300"
						>Instance Type</label
					>
					<input
						id="new-ct-instance-type"
						bind:value={newCtInstanceType}
						placeholder="m5.large"
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="new-ct-count" class="text-sm text-slate-600 dark:text-slate-300">Count</label>
					<input
						id="new-ct-count"
						type="number"
						bind:value={newCtCount}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			<label class="flex items-center gap-2 text-sm">
				<input type="checkbox" bind:checked={newCtDryRun} /> Dry run
			</label>
			<p class="text-xs text-slate-500">
				This form submits a single instance pool; the real API accepts multiple.
			</p>
			{#if startCapacityTaskError}
				<p class="text-sm text-red-600 dark:text-red-400">{startCapacityTaskError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => startCapacityTaskModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitStartCapacityTask}
			disabled={startingCapacityTask}
			class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
			>{startingCapacityTask ? 'Starting...' : 'Start'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={capacityTaskDetailModal} title="Capacity Task Detail">
	{#snippet children()}
		<div class="space-y-3 max-h-[70vh] overflow-y-auto">
			{#if capacityTaskDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if capacityTaskDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{capacityTaskDetailError}</p>
			{:else if viewedCapacityTask}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">Task ID:</span> {viewedCapacityTask.CapacityTaskId}</div>
					<div><span class="text-slate-500">Outpost ID:</span> {viewedCapacityTask.OutpostId}</div>
					<div><span class="text-slate-500">Order ID:</span> {viewedCapacityTask.OrderId ?? '—'}</div>
					<div><span class="text-slate-500">Asset ID:</span> {viewedCapacityTask.AssetId ?? '—'}</div>
					<div>
						<span class="text-slate-500">Status:</span> {viewedCapacityTask.CapacityTaskStatus}
					</div>
					<div>
						<span class="text-slate-500">Created:</span> {formatDate(viewedCapacityTask.CreationDate)}
					</div>
				</div>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<div class="flex items-center justify-between">
						<h4 class="text-sm font-semibold">Blocking Instances</h4>
						<button
							onclick={loadBlockingInstances}
							disabled={loadingBlockingInstances}
							class="text-xs text-indigo-600 hover:underline">Load</button
						>
					</div>
					<p class="text-xs text-slate-500 mt-1">
						{#if blockingInstances.length === 0}
							None (this backend has no cross-service EC2-on-Outposts placement data).
						{:else}
							{blockingInstances.map((b) => b.InstanceId).join(', ')}
						{/if}
					</p>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => capacityTaskDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={assetDetailModal} title="Asset Detail">
	{#snippet children()}
		<div class="space-y-3">
			{#if viewedAsset}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">Asset ID:</span> {viewedAsset.AssetId}</div>
					<div><span class="text-slate-500">Rack ID:</span> {viewedAsset.RackId ?? '—'}</div>
					<div><span class="text-slate-500">Type:</span> {viewedAsset.AssetType ?? '—'}</div>
					<div>
						<span class="text-slate-500">State:</span> {viewedAsset.ComputeAttributes?.State ?? '—'}
					</div>
					<div>
						<span class="text-slate-500">Max vCPUs:</span> {viewedAsset.ComputeAttributes?.MaxVcpus ??
							'—'}
					</div>
					<div>
						<span class="text-slate-500">Rack elevation:</span> {viewedAsset.AssetLocation
							?.RackElevation ?? '—'}
					</div>
				</div>
				{#if viewedAsset.ComputeAttributes?.InstanceTypeCapacities && viewedAsset.ComputeAttributes.InstanceTypeCapacities.length > 0}
					<h4 class="text-sm font-semibold">Instance Type Capacities</h4>
					<ul class="text-xs list-disc list-inside">
						{#each viewedAsset.ComputeAttributes.InstanceTypeCapacities as cap, i (i)}
							<li>{cap.InstanceType}: {cap.Count}</li>
						{/each}
					</ul>
				{/if}
				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<div class="flex items-center justify-between">
						<h4 class="text-sm font-semibold">Asset Instances</h4>
						<button
							onclick={loadAssetInstances}
							disabled={loadingAssetInstances}
							class="text-xs text-indigo-600 hover:underline">Load</button
						>
					</div>
					<p class="text-xs text-slate-500 mt-1">
						{#if assetInstances.length === 0}
							None (this backend has no cross-service EC2-on-Outposts placement data).
						{:else}
							{assetInstances.map((a) => a.InstanceId).join(', ')}
						{/if}
					</p>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => assetDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={catalogDetailModal} title="Catalog Item Detail">
	{#snippet children()}
		<div class="space-y-3">
			{#if catalogDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if catalogDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{catalogDetailError}</p>
			{:else if viewedCatalogItem}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">Item ID:</span> {viewedCatalogItem.CatalogItemId}</div>
					<div><span class="text-slate-500">Status:</span> {viewedCatalogItem.ItemStatus}</div>
					<div><span class="text-slate-500">Power (kVA):</span> {viewedCatalogItem.PowerKva ?? '—'}</div>
					<div>
						<span class="text-slate-500">Weight (lbs):</span> {viewedCatalogItem.WeightLbs ?? '—'}
					</div>
					<div class="col-span-2">
						<span class="text-slate-500">Supported storage:</span>
						{(viewedCatalogItem.SupportedStorage ?? []).join(', ') || '—'}
					</div>
				</div>
				{#if viewedCatalogItem.EC2Capacities && viewedCatalogItem.EC2Capacities.length > 0}
					<h4 class="text-sm font-semibold">EC2 Capacities</h4>
					<ul class="text-xs list-disc list-inside">
						{#each viewedCatalogItem.EC2Capacities as cap, i (i)}
							<li>{cap.Family}: up to {cap.MaxSize}, quantity {cap.Quantity}</li>
						{/each}
					</ul>
				{/if}
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => catalogDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
