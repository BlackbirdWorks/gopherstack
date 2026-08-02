<script lang="ts">
	import { untrack } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { currentRegion } from '$lib/region.svelte';
	import { urlState } from '$lib/url-state.svelte';
	import { getDirectConnectClient } from '$lib/aws-client';
	import {
		DescribeConnectionsCommand,
		CreateConnectionCommand,
		UpdateConnectionCommand,
		DeleteConnectionCommand,
		ConfirmConnectionCommand,
		AssociateMacSecKeyCommand,
		DisassociateMacSecKeyCommand,
		DescribeLoaCommand,
		DescribeLagsCommand,
		CreateLagCommand,
		UpdateLagCommand,
		DeleteLagCommand,
		AssociateConnectionWithLagCommand,
		DisassociateConnectionFromLagCommand,
		DescribeInterconnectsCommand,
		CreateInterconnectCommand,
		DeleteInterconnectCommand,
		DescribeInterconnectLoaCommand,
		DescribeVirtualInterfacesCommand,
		CreatePrivateVirtualInterfaceCommand,
		CreatePublicVirtualInterfaceCommand,
		CreateTransitVirtualInterfaceCommand,
		UpdateVirtualInterfaceAttributesCommand,
		DeleteVirtualInterfaceCommand,
		CreateBGPPeerCommand,
		DeleteBGPPeerCommand,
		ConfirmPrivateVirtualInterfaceCommand,
		ConfirmPublicVirtualInterfaceCommand,
		ConfirmTransitVirtualInterfaceCommand,
		AssociateVirtualInterfaceCommand,
		DescribeRouterConfigurationCommand,
		StartBgpFailoverTestCommand,
		StopBgpFailoverTestCommand,
		ListVirtualInterfaceTestHistoryCommand,
		DescribeDirectConnectGatewaysCommand,
		CreateDirectConnectGatewayCommand,
		UpdateDirectConnectGatewayCommand,
		DeleteDirectConnectGatewayCommand,
		DescribeDirectConnectGatewayAssociationsCommand,
		CreateDirectConnectGatewayAssociationCommand,
		UpdateDirectConnectGatewayAssociationCommand,
		DeleteDirectConnectGatewayAssociationCommand,
		DescribeDirectConnectGatewayAssociationProposalsCommand,
		CreateDirectConnectGatewayAssociationProposalCommand,
		AcceptDirectConnectGatewayAssociationProposalCommand,
		DeleteDirectConnectGatewayAssociationProposalCommand,
		DescribeDirectConnectGatewayAttachmentsCommand,
		DescribeVirtualGatewaysCommand,
		DescribeLocationsCommand,
		DescribeCustomerMetadataCommand,
		ConfirmCustomerAgreementCommand,
		DescribeTagsCommand,
		TagResourceCommand,
		UntagResourceCommand,
		type Connection,
		type Lag,
		type Interconnect,
		type VirtualInterface,
		type BGPPeer,
		type RouteFilterPrefix,
		type DirectConnectGateway,
		type DirectConnectGatewayAssociation,
		type DirectConnectGatewayAssociationProposal,
		type DirectConnectGatewayAttachment,
		type VirtualGateway,
		type Location,
		type VirtualInterfaceTestHistory,
		type CustomerAgreement
	} from '@aws-sdk/client-direct-connect';
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
	import { Cable, Plus, Trash2, Eye, Pencil, Check, Play, Square } from 'lucide-svelte';

	// This service has only 5 modeled exception shapes shared across all 63
	// operations -- DirectConnectClientException, DirectConnectServerException,
	// DuplicateTagKeysException, LimitExceededException, TooManyTagsException
	// -- and, notably, NO ResourceNotFoundException/ValidationException at all
	// (see services/directconnect/PARITY.md). Every not-found/bad-input
	// condition folds into DirectConnectClientException, so the generic
	// name+message+status rendering below already shows exactly what the API
	// returns -- there is no richer per-op error detail to surface.
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

	// Shared status-badge coloring across every *State/*Status enum in this
	// service (ConnectionState/LagState/InterconnectState/VirtualInterfaceState/
	// BgpPeerState/BgpStatus/DirectConnectGatewayState/
	// DirectConnectGatewayAssociationState/
	// DirectConnectGatewayAssociationProposalState/
	// DirectConnectGatewayAttachmentState) -- the real enum values, read from
	// the installed SDK's enums.d.ts, not invented.
	function stateBadgeClass(state: string | undefined): string {
		const s = (state ?? '').toLowerCase();
		if (['available', 'associated', 'attached', 'accepted', 'up'].includes(s)) {
			return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400';
		}
		if (['down', 'deleted', 'rejected', 'disassociated', 'detached'].includes(s)) {
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		}
		if (
			[
				'pending',
				'requested',
				'confirming',
				'testing',
				'verifying',
				'updating',
				'associating',
				'attaching',
				'ordering',
				'deleting',
				'disassociating',
				'detaching'
			].includes(s)
		) {
			return 'bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400';
		}
		return 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// Direct Connect's Describe/List responses never carry an ARN field for
	// any resource (confirmed: none of connectionWire/lagWire/
	// interconnectWire/virtualInterfaceWire/directConnectGatewayWire in
	// services/directconnect/wire.go have one) -- unlike outposts/
	// resiliencehub, whose list rows carry their own *Arn field straight from
	// the API. TagResource/UntagResource/DescribeTags all key off a
	// caller-supplied ResourceArn, so this page builds one client-side using
	// the exact resource-kind marker services/directconnect/store.go's
	// resolveTaggableLocked matches on (":dxcon/", ":dxlag/", ":dxvif/",
	// ":dx-gateway/") -- the backend only checks for that marker substring,
	// not an exact account-id/partition match, so a placeholder account id is
	// safe here. DirectConnectGateway is the one GLOBAL resource (no region
	// segment), per PARITY.md's ARN section.
	const PLACEHOLDER_ACCOUNT_ID = '000000000000';

	function taggableArn(kind: 'dxcon' | 'dxlag' | 'dxvif' | 'dx-gateway', id: string): string {
		if (kind === 'dx-gateway') {
			return `arn:aws:directconnect::${PLACEHOLDER_ACCOUNT_ID}:dx-gateway/${id}`;
		}
		return `arn:aws:directconnect:${currentRegion()}:${PLACEHOLDER_ACCOUNT_ID}:${kind}/${id}`;
	}

	function parseCidrList(text: string): RouteFilterPrefix[] {
		return text
			.split(/[\n,]/)
			.map((s) => s.trim())
			.filter(Boolean)
			.map((cidr) => ({ cidr }));
	}

	function formatCidrList(prefixes: RouteFilterPrefix[] | undefined): string {
		return (prefixes ?? [])
			.map((p) => p.cidr ?? '')
			.filter(Boolean)
			.join(', ');
	}

	function formatTags(tags: { key?: string; value?: string }[] | undefined): string {
		return (tags ?? []).map((t) => `${t.key}=${t.value ?? ''}`).join(', ');
	}

	type TabId =
		| 'connections'
		| 'lags'
		| 'interconnects'
		| 'virtualInterfaces'
		| 'gateways'
		| 'gatewayAssociations'
		| 'gatewayProposals'
		| 'gatewayAttachments'
		| 'virtualGateways'
		| 'locations'
		| 'vifTestHistory';

	const tabs: TabDef[] = [
		{ id: 'connections', label: 'Connections' },
		{ id: 'lags', label: 'LAGs' },
		{ id: 'interconnects', label: 'Interconnects' },
		{ id: 'virtualInterfaces', label: 'Virtual Interfaces' },
		{ id: 'gateways', label: 'Gateways' },
		{ id: 'gatewayAssociations', label: 'Gateway Associations' },
		{ id: 'gatewayProposals', label: 'Association Proposals' },
		{ id: 'gatewayAttachments', label: 'Gateway Attachments' },
		{ id: 'virtualGateways', label: 'Virtual Gateways' },
		{ id: 'locations', label: 'Locations' },
		{ id: 'vifTestHistory', label: 'VIF Test History' }
	];

	const client = regionalClient(getDirectConnectClient);

	// URL-backed (?tab=...); see url-state.svelte.ts. Read via untrack() inside
	// the onRegionChange effect below (switchTab() also writes it): without
	// untrack, every tab switch would re-trigger the region effect and
	// double-fetch.
	const pageTabParam = urlState<TabId>('tab', 'connections');
	let activeTab = $derived(pageTabParam.get());
	let searchQuery = $state('');

	let connections = $state<Connection[]>([]);
	let connectionsNextToken = $state<string | undefined>();
	let loadingMoreConnections = $state(false);

	let lags = $state<Lag[]>([]);
	let lagsNextToken = $state<string | undefined>();
	let loadingMoreLags = $state(false);

	let interconnects = $state<Interconnect[]>([]);
	let interconnectsNextToken = $state<string | undefined>();
	let loadingMoreInterconnects = $state(false);

	let vifs = $state<VirtualInterface[]>([]);
	let vifsNextToken = $state<string | undefined>();
	let loadingMoreVifs = $state(false);

	let gateways = $state<DirectConnectGateway[]>([]);
	let gatewaysNextToken = $state<string | undefined>();
	let loadingMoreGateways = $state(false);

	let gatewayAssociations = $state<DirectConnectGatewayAssociation[]>([]);
	let gatewayAssociationsNextToken = $state<string | undefined>();
	let loadingMoreGatewayAssociations = $state(false);

	let gatewayProposals = $state<DirectConnectGatewayAssociationProposal[]>([]);
	let gatewayProposalsNextToken = $state<string | undefined>();
	let loadingMoreGatewayProposals = $state(false);

	let gatewayAttachments = $state<DirectConnectGatewayAttachment[]>([]);
	let gatewayAttachmentsNextToken = $state<string | undefined>();
	let loadingMoreGatewayAttachments = $state(false);

	let virtualGateways = $state<VirtualGateway[]>([]);

	let locations = $state<Location[]>([]);
	let customerAgreements = $state<CustomerAgreement[]>([]);
	let nniPartnerType = $state<string | undefined>();

	let vifTestHistory = $state<VirtualInterfaceTestHistory[]>([]);
	let vifTestHistoryNextToken = $state<string | undefined>();
	let loadingMoreVifTestHistory = $state(false);

	async function fetchConnections(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeConnectionsCommand({ nextToken: reset ? undefined : connectionsNextToken })
		);
		connections = reset ? (resp.connections ?? []) : [...connections, ...(resp.connections ?? [])];
		connectionsNextToken = resp.nextToken;
	}

	async function fetchLags(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeLagsCommand({ nextToken: reset ? undefined : lagsNextToken })
		);
		lags = reset ? (resp.lags ?? []) : [...lags, ...(resp.lags ?? [])];
		lagsNextToken = resp.nextToken;
	}

	async function fetchInterconnects(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeInterconnectsCommand({ nextToken: reset ? undefined : interconnectsNextToken })
		);
		interconnects = reset
			? (resp.interconnects ?? [])
			: [...interconnects, ...(resp.interconnects ?? [])];
		interconnectsNextToken = resp.nextToken;
	}

	async function fetchVifs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeVirtualInterfacesCommand({ nextToken: reset ? undefined : vifsNextToken })
		);
		vifs = reset ? (resp.virtualInterfaces ?? []) : [...vifs, ...(resp.virtualInterfaces ?? [])];
		vifsNextToken = resp.nextToken;
	}

	async function fetchGateways(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeDirectConnectGatewaysCommand({ nextToken: reset ? undefined : gatewaysNextToken })
		);
		gateways = reset
			? (resp.directConnectGateways ?? [])
			: [...gateways, ...(resp.directConnectGateways ?? [])];
		gatewaysNextToken = resp.nextToken;
	}

	async function fetchGatewayAssociations(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeDirectConnectGatewayAssociationsCommand({
				nextToken: reset ? undefined : gatewayAssociationsNextToken
			})
		);
		gatewayAssociations = reset
			? (resp.directConnectGatewayAssociations ?? [])
			: [...gatewayAssociations, ...(resp.directConnectGatewayAssociations ?? [])];
		gatewayAssociationsNextToken = resp.nextToken;
	}

	async function fetchGatewayProposals(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeDirectConnectGatewayAssociationProposalsCommand({
				nextToken: reset ? undefined : gatewayProposalsNextToken
			})
		);
		gatewayProposals = reset
			? (resp.directConnectGatewayAssociationProposals ?? [])
			: [...gatewayProposals, ...(resp.directConnectGatewayAssociationProposals ?? [])];
		gatewayProposalsNextToken = resp.nextToken;
	}

	async function fetchGatewayAttachments(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeDirectConnectGatewayAttachmentsCommand({
				nextToken: reset ? undefined : gatewayAttachmentsNextToken
			})
		);
		gatewayAttachments = reset
			? (resp.directConnectGatewayAttachments ?? [])
			: [...gatewayAttachments, ...(resp.directConnectGatewayAttachments ?? [])];
		gatewayAttachmentsNextToken = resp.nextToken;
	}

	// No MaxResults/NextToken on either side of the wire for this op (PARITY.md
	// wire-trap #4) -- always a single full-account call.
	async function fetchVirtualGateways(): Promise<void> {
		const resp = await client().send(new DescribeVirtualGatewaysCommand({}));
		virtualGateways = resp.virtualGateways ?? [];
	}

	// Locations and DescribeCustomerMetadata share one tab: both are
	// account/region-scoped reference data with no pagination on either op.
	async function fetchLocationsAndMetadata(): Promise<void> {
		const [locResp, metaResp] = await Promise.all([
			client().send(new DescribeLocationsCommand({})),
			client().send(new DescribeCustomerMetadataCommand({}))
		]);
		locations = locResp.locations ?? [];
		customerAgreements = metaResp.agreements ?? [];
		nniPartnerType = metaResp.nniPartnerType;
	}

	async function fetchVifTestHistory(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListVirtualInterfaceTestHistoryCommand({
				nextToken: reset ? undefined : vifTestHistoryNextToken
			})
		);
		vifTestHistory = reset
			? (resp.virtualInterfaceTestHistory ?? [])
			: [...vifTestHistory, ...(resp.virtualInterfaceTestHistory ?? [])];
		vifTestHistoryNextToken = resp.nextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		connections: () => fetchConnections(true).catch(rethrowDescribed),
		lags: () => fetchLags(true).catch(rethrowDescribed),
		interconnects: () => fetchInterconnects(true).catch(rethrowDescribed),
		virtualInterfaces: () => fetchVifs(true).catch(rethrowDescribed),
		gateways: () => fetchGateways(true).catch(rethrowDescribed),
		gatewayAssociations: () => fetchGatewayAssociations(true).catch(rethrowDescribed),
		gatewayProposals: () => fetchGatewayProposals(true).catch(rethrowDescribed),
		gatewayAttachments: () => fetchGatewayAttachments(true).catch(rethrowDescribed),
		virtualGateways: () => fetchVirtualGateways().catch(rethrowDescribed),
		locations: () => fetchLocationsAndMetadata().catch(rethrowDescribed),
		vifTestHistory: () => fetchVifTestHistory(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		pageTabParam.set(id as TabId);
		searchQuery = '';
		tabLoader.load(id as TabId);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// No tab here is scoped to a shared "parent" resource selector (unlike
	// outposts' Assets-scoped-to-Outpost), so a region change only needs to
	// force-reload whichever tab is currently visible -- `activeTab` is read
	// via untrack() because switchTab() also writes it (via pageTabParam):
	// without untrack, every tab switch would re-trigger this very effect and
	// double-fetch.
	onRegionChange(() => {
		const tab = untrack(() => activeTab);
		void tabLoader.refresh(tab);
	});

	const filteredConnections = $derived(
		connections.filter((c) => {
			const q = searchQuery.toLowerCase();
			return (
				(c.connectionId ?? '').toLowerCase().includes(q) ||
				(c.connectionName ?? '').toLowerCase().includes(q) ||
				(c.connectionState ?? '').toLowerCase().includes(q) ||
				(c.location ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredLags = $derived(
		lags.filter((l) => {
			const q = searchQuery.toLowerCase();
			return (
				(l.lagId ?? '').toLowerCase().includes(q) ||
				(l.lagName ?? '').toLowerCase().includes(q) ||
				(l.lagState ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredInterconnects = $derived(
		interconnects.filter((i) => {
			const q = searchQuery.toLowerCase();
			return (
				(i.interconnectId ?? '').toLowerCase().includes(q) ||
				(i.interconnectName ?? '').toLowerCase().includes(q) ||
				(i.interconnectState ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredVifs = $derived(
		vifs.filter((v) => {
			const q = searchQuery.toLowerCase();
			return (
				(v.virtualInterfaceId ?? '').toLowerCase().includes(q) ||
				(v.virtualInterfaceName ?? '').toLowerCase().includes(q) ||
				(v.virtualInterfaceState ?? '').toLowerCase().includes(q) ||
				(v.virtualInterfaceType ?? '').toLowerCase().includes(q) ||
				(v.connectionId ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredGateways = $derived(
		gateways.filter((g) => {
			const q = searchQuery.toLowerCase();
			return (
				(g.directConnectGatewayId ?? '').toLowerCase().includes(q) ||
				(g.directConnectGatewayName ?? '').toLowerCase().includes(q) ||
				(g.directConnectGatewayState ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredGatewayAssociations = $derived(
		gatewayAssociations.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.associationId ?? '').toLowerCase().includes(q) ||
				(a.directConnectGatewayId ?? '').toLowerCase().includes(q) ||
				(a.associationState ?? '').toLowerCase().includes(q) ||
				(a.associatedGateway?.id ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredGatewayProposals = $derived(
		gatewayProposals.filter((p) => {
			const q = searchQuery.toLowerCase();
			return (
				(p.proposalId ?? '').toLowerCase().includes(q) ||
				(p.directConnectGatewayId ?? '').toLowerCase().includes(q) ||
				(p.proposalState ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredGatewayAttachments = $derived(
		gatewayAttachments.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.virtualInterfaceId ?? '').toLowerCase().includes(q) ||
				(a.directConnectGatewayId ?? '').toLowerCase().includes(q) ||
				(a.attachmentState ?? '').toLowerCase().includes(q) ||
				(a.attachmentType ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredVirtualGateways = $derived(
		virtualGateways.filter((g) => {
			const q = searchQuery.toLowerCase();
			return (
				(g.virtualGatewayId ?? '').toLowerCase().includes(q) ||
				(g.virtualGatewayState ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredLocations = $derived(
		locations.filter((l) => {
			const q = searchQuery.toLowerCase();
			return (
				(l.locationCode ?? '').toLowerCase().includes(q) ||
				(l.locationName ?? '').toLowerCase().includes(q) ||
				(l.region ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredVifTestHistory = $derived(
		vifTestHistory.filter((t) => {
			const q = searchQuery.toLowerCase();
			return (
				(t.testId ?? '').toLowerCase().includes(q) ||
				(t.virtualInterfaceId ?? '').toLowerCase().includes(q) ||
				(t.status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreConnections(): Promise<void> {
		loadingMoreConnections = true;
		try {
			await fetchConnections(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreConnections = false;
		}
	}
	async function loadMoreLags(): Promise<void> {
		loadingMoreLags = true;
		try {
			await fetchLags(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreLags = false;
		}
	}
	async function loadMoreInterconnects(): Promise<void> {
		loadingMoreInterconnects = true;
		try {
			await fetchInterconnects(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInterconnects = false;
		}
	}
	async function loadMoreVifs(): Promise<void> {
		loadingMoreVifs = true;
		try {
			await fetchVifs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreVifs = false;
		}
	}
	async function loadMoreGateways(): Promise<void> {
		loadingMoreGateways = true;
		try {
			await fetchGateways(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGateways = false;
		}
	}
	async function loadMoreGatewayAssociations(): Promise<void> {
		loadingMoreGatewayAssociations = true;
		try {
			await fetchGatewayAssociations(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGatewayAssociations = false;
		}
	}
	async function loadMoreGatewayProposals(): Promise<void> {
		loadingMoreGatewayProposals = true;
		try {
			await fetchGatewayProposals(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGatewayProposals = false;
		}
	}
	async function loadMoreGatewayAttachments(): Promise<void> {
		loadingMoreGatewayAttachments = true;
		try {
			await fetchGatewayAttachments(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGatewayAttachments = false;
		}
	}
	async function loadMoreVifTestHistory(): Promise<void> {
		loadingMoreVifTestHistory = true;
		try {
			await fetchVifTestHistory(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreVifTestHistory = false;
		}
	}

	// ============================= Connections =============================

	let createConnectionModal = $state<Modal | null>(null);
	let creatingConnection = $state(false);
	let createConnectionError = $state<string | null>(null);
	let newConnName = $state('');
	let newConnBandwidth = $state('1Gbps');
	let newConnLocation = $state('');
	let newConnLagId = $state('');
	let newConnProviderName = $state('');
	let newConnRequestMacSec = $state(false);

	function openCreateConnectionModal(): void {
		createConnectionError = null;
		newConnName = '';
		newConnBandwidth = '1Gbps';
		newConnLocation = '';
		newConnLagId = '';
		newConnProviderName = '';
		newConnRequestMacSec = false;
		createConnectionModal?.open();
	}

	async function submitCreateConnection(): Promise<void> {
		if (!newConnName.trim() || !newConnBandwidth.trim() || !newConnLocation.trim()) {
			createConnectionError = 'Name, bandwidth, and location are required.';
			return;
		}
		creatingConnection = true;
		createConnectionError = null;
		try {
			await client().send(
				new CreateConnectionCommand({
					connectionName: newConnName.trim(),
					bandwidth: newConnBandwidth.trim(),
					location: newConnLocation.trim(),
					lagId: newConnLagId.trim() || undefined,
					providerName: newConnProviderName.trim() || undefined,
					requestMACSec: newConnRequestMacSec
				})
			);
			toast.success('Connection created');
			createConnectionModal?.close();
			await tabLoader.refresh('connections');
		} catch (e) {
			const msg = describeError(e);
			createConnectionError = msg;
			toast.error(msg);
		} finally {
			creatingConnection = false;
		}
	}

	async function handleDeleteConnection(c: Connection): Promise<void> {
		if (!c.connectionId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete connection',
			message: `Delete connection "${c.connectionName ?? c.connectionId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteConnectionCommand({ connectionId: c.connectionId }));
			toast.success('Connection deleted');
			await tabLoader.refresh('connections');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editConnectionModal = $state<Modal | null>(null);
	let editingConnection = $state(false);
	let editConnectionError = $state<string | null>(null);
	let editConnId = $state('');
	let editConnName = $state('');
	let editConnEncryptionMode = $state<'' | 'no_encrypt' | 'should_encrypt' | 'must_encrypt'>('');

	function openEditConnectionModal(c: Connection): void {
		if (!c.connectionId) return;
		editConnectionError = null;
		editConnId = c.connectionId;
		editConnName = c.connectionName ?? '';
		editConnEncryptionMode = (c.encryptionMode as typeof editConnEncryptionMode) ?? '';
		editConnectionModal?.open();
	}

	async function submitEditConnection(): Promise<void> {
		if (!editConnId) return;
		editingConnection = true;
		editConnectionError = null;
		try {
			await client().send(
				new UpdateConnectionCommand({
					connectionId: editConnId,
					connectionName: editConnName.trim() || undefined,
					encryptionMode: editConnEncryptionMode || undefined
				})
			);
			toast.success('Connection updated');
			editConnectionModal?.close();
			await tabLoader.refresh('connections');
		} catch (e) {
			const msg = describeError(e);
			editConnectionError = msg;
			toast.error(msg);
		} finally {
			editingConnection = false;
		}
	}

	let connectionDetailModal = $state<Modal | null>(null);
	let viewedConnection = $state<Connection | null>(null);
	let connectionDetailLoading = $state(false);
	let connectionDetailError = $state<string | null>(null);
	let connTags = $state<{ key?: string; value?: string }[]>([]);
	let connAddTagKey = $state('');
	let connAddTagValue = $state('');
	let connMacSecMode = $state<'raw' | 'secretArn'>('raw');
	let connMacSecCak = $state('');
	let connMacSecCkn = $state('');
	let connMacSecSecretArn = $state('');
	let connLoaResult = $state<{ contentType?: string; byteLength: number; dataUrl: string } | null>(
		null
	);

	async function openConnectionDetail(c: Connection): Promise<void> {
		if (!c.connectionId) return;
		viewedConnection = null;
		connectionDetailError = null;
		connTags = [];
		connLoaResult = null;
		connMacSecCak = '';
		connMacSecCkn = '';
		connMacSecSecretArn = '';
		connectionDetailModal?.open();
		connectionDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeConnectionsCommand({ connectionId: c.connectionId })
			);
			viewedConnection = resp.connections?.[0] ?? null;
			connTags = viewedConnection?.tags ?? [];
		} catch (e) {
			connectionDetailError = describeError(e);
		} finally {
			connectionDetailLoading = false;
		}
	}

	async function refreshConnectionDetail(): Promise<void> {
		if (!viewedConnection?.connectionId) return;
		const resp = await client().send(
			new DescribeConnectionsCommand({ connectionId: viewedConnection.connectionId })
		);
		viewedConnection = resp.connections?.[0] ?? viewedConnection;
	}

	async function handleConfirmConnection(): Promise<void> {
		if (!viewedConnection?.connectionId) return;
		try {
			await client().send(new ConfirmConnectionCommand({ connectionId: viewedConnection.connectionId }));
			toast.success('Connection confirmed');
			await refreshConnectionDetail();
			await tabLoader.refresh('connections');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAssociateMacSecKey(): Promise<void> {
		if (!viewedConnection?.connectionId) return;
		try {
			const resp = await client().send(
				new AssociateMacSecKeyCommand(
					connMacSecMode === 'raw'
						? {
								connectionId: viewedConnection.connectionId,
								cak: connMacSecCak.trim() || undefined,
								ckn: connMacSecCkn.trim() || undefined
							}
						: {
								connectionId: viewedConnection.connectionId,
								secretARN: connMacSecSecretArn.trim() || undefined
							}
				)
			);
			viewedConnection = { ...viewedConnection, macSecKeys: resp.macSecKeys };
			toast.success('MACsec key associated');
			connMacSecCak = '';
			connMacSecCkn = '';
			connMacSecSecretArn = '';
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleDisassociateMacSecKey(secretArn: string | undefined): Promise<void> {
		if (!viewedConnection?.connectionId || !secretArn) return;
		try {
			const resp = await client().send(
				new DisassociateMacSecKeyCommand({
					connectionId: viewedConnection.connectionId,
					secretARN: secretArn
				})
			);
			viewedConnection = { ...viewedConnection, macSecKeys: resp.macSecKeys };
			toast.success('MACsec key disassociated');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleViewConnectionLoa(): Promise<void> {
		if (!viewedConnection?.connectionId) return;
		try {
			const resp = await client().send(
				new DescribeLoaCommand({ connectionId: viewedConnection.connectionId })
			);
			const bytes = resp.loaContent ?? new Uint8Array();
			const blob = new Blob([bytes as BlobPart], { type: resp.loaContentType ?? 'application/pdf' });
			connLoaResult = {
				contentType: resp.loaContentType,
				byteLength: bytes.length,
				dataUrl: URL.createObjectURL(blob)
			};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function refreshConnectionTags(): Promise<void> {
		if (!viewedConnection?.connectionId) return;
		try {
			const arn = taggableArn('dxcon', viewedConnection.connectionId);
			const resp = await client().send(new DescribeTagsCommand({ resourceArns: [arn] }));
			connTags = resp.resourceTags?.[0]?.tags ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAddConnTag(): Promise<void> {
		if (!viewedConnection?.connectionId || !connAddTagKey.trim()) return;
		try {
			const arn = taggableArn('dxcon', viewedConnection.connectionId);
			await client().send(
				new TagResourceCommand({ resourceArn: arn, tags: [{ key: connAddTagKey.trim(), value: connAddTagValue }] })
			);
			toast.success('Tag added');
			connAddTagKey = '';
			connAddTagValue = '';
			await refreshConnectionTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeConnTag(key: string | undefined): Promise<void> {
		if (!viewedConnection?.connectionId || !key) return;
		try {
			const arn = taggableArn('dxcon', viewedConnection.connectionId);
			await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
			toast.success('Tag removed');
			await refreshConnectionTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// =============================== LAGs ===================================

	let createLagModal = $state<Modal | null>(null);
	let creatingLag = $state(false);
	let createLagError = $state<string | null>(null);
	let newLagName = $state('');
	let newLagBandwidth = $state('1Gbps');
	let newLagLocation = $state('');
	let newLagNumConnections = $state(1);
	let newLagConnectionId = $state('');
	let newLagProviderName = $state('');
	let newLagRequestMacSec = $state(false);

	function openCreateLagModal(): void {
		createLagError = null;
		newLagName = '';
		newLagBandwidth = '1Gbps';
		newLagLocation = '';
		newLagNumConnections = 1;
		newLagConnectionId = '';
		newLagProviderName = '';
		newLagRequestMacSec = false;
		createLagModal?.open();
	}

	async function submitCreateLag(): Promise<void> {
		if (!newLagName.trim() || !newLagBandwidth.trim() || !newLagLocation.trim()) {
			createLagError = 'Name, bandwidth, and location are required.';
			return;
		}
		creatingLag = true;
		createLagError = null;
		try {
			await client().send(
				new CreateLagCommand({
					lagName: newLagName.trim(),
					connectionsBandwidth: newLagBandwidth.trim(),
					location: newLagLocation.trim(),
					numberOfConnections: newLagNumConnections,
					connectionId: newLagConnectionId.trim() || undefined,
					providerName: newLagProviderName.trim() || undefined,
					requestMACSec: newLagRequestMacSec
				})
			);
			toast.success('LAG created');
			createLagModal?.close();
			await tabLoader.refresh('lags');
		} catch (e) {
			const msg = describeError(e);
			createLagError = msg;
			toast.error(msg);
		} finally {
			creatingLag = false;
		}
	}

	async function handleDeleteLag(l: Lag): Promise<void> {
		if (!l.lagId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete LAG',
			message: `Delete LAG "${l.lagName ?? l.lagId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLagCommand({ lagId: l.lagId }));
			toast.success('LAG deleted');
			await tabLoader.refresh('lags');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editLagModal = $state<Modal | null>(null);
	let editingLag = $state(false);
	let editLagError = $state<string | null>(null);
	let editLagId = $state('');
	let editLagName = $state('');
	let editLagMinLinks = $state(0);
	let editLagEncryptionMode = $state<'' | 'no_encrypt' | 'should_encrypt' | 'must_encrypt'>('');

	function openEditLagModal(l: Lag): void {
		if (!l.lagId) return;
		editLagError = null;
		editLagId = l.lagId;
		editLagName = l.lagName ?? '';
		editLagMinLinks = l.minimumLinks ?? 0;
		editLagEncryptionMode = (l.encryptionMode as typeof editLagEncryptionMode) ?? '';
		editLagModal?.open();
	}

	async function submitEditLag(): Promise<void> {
		if (!editLagId) return;
		editingLag = true;
		editLagError = null;
		try {
			await client().send(
				new UpdateLagCommand({
					lagId: editLagId,
					lagName: editLagName.trim() || undefined,
					minimumLinks: editLagMinLinks || undefined,
					encryptionMode: editLagEncryptionMode || undefined
				})
			);
			toast.success('LAG updated');
			editLagModal?.close();
			await tabLoader.refresh('lags');
		} catch (e) {
			const msg = describeError(e);
			editLagError = msg;
			toast.error(msg);
		} finally {
			editingLag = false;
		}
	}

	let lagDetailModal = $state<Modal | null>(null);
	let viewedLag = $state<Lag | null>(null);
	let lagDetailLoading = $state(false);
	let lagDetailError = $state<string | null>(null);
	let lagTags = $state<{ key?: string; value?: string }[]>([]);
	let lagAddTagKey = $state('');
	let lagAddTagValue = $state('');
	let lagAssociateConnId = $state('');

	async function openLagDetail(l: Lag): Promise<void> {
		if (!l.lagId) return;
		viewedLag = null;
		lagDetailError = null;
		lagTags = [];
		lagAssociateConnId = '';
		lagDetailModal?.open();
		lagDetailLoading = true;
		try {
			const resp = await client().send(new DescribeLagsCommand({ lagId: l.lagId }));
			viewedLag = resp.lags?.[0] ?? null;
			lagTags = viewedLag?.tags ?? [];
		} catch (e) {
			lagDetailError = describeError(e);
		} finally {
			lagDetailLoading = false;
		}
	}

	async function refreshLagDetail(): Promise<void> {
		if (!viewedLag?.lagId) return;
		const resp = await client().send(new DescribeLagsCommand({ lagId: viewedLag.lagId }));
		viewedLag = resp.lags?.[0] ?? viewedLag;
	}

	async function submitAssociateConnectionWithLag(): Promise<void> {
		if (!viewedLag?.lagId || !lagAssociateConnId.trim()) return;
		try {
			await client().send(
				new AssociateConnectionWithLagCommand({
					connectionId: lagAssociateConnId.trim(),
					lagId: viewedLag.lagId
				})
			);
			toast.success('Connection associated with LAG');
			lagAssociateConnId = '';
			await refreshLagDetail();
			await tabLoader.refresh('lags');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleDisassociateConnectionFromLag(connectionId: string | undefined): Promise<void> {
		if (!viewedLag?.lagId || !connectionId) return;
		try {
			await client().send(
				new DisassociateConnectionFromLagCommand({ connectionId, lagId: viewedLag.lagId })
			);
			toast.success('Connection disassociated from LAG');
			await refreshLagDetail();
			await tabLoader.refresh('lags');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function refreshLagTags(): Promise<void> {
		if (!viewedLag?.lagId) return;
		try {
			const arn = taggableArn('dxlag', viewedLag.lagId);
			const resp = await client().send(new DescribeTagsCommand({ resourceArns: [arn] }));
			lagTags = resp.resourceTags?.[0]?.tags ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAddLagTag(): Promise<void> {
		if (!viewedLag?.lagId || !lagAddTagKey.trim()) return;
		try {
			const arn = taggableArn('dxlag', viewedLag.lagId);
			await client().send(
				new TagResourceCommand({ resourceArn: arn, tags: [{ key: lagAddTagKey.trim(), value: lagAddTagValue }] })
			);
			toast.success('Tag added');
			lagAddTagKey = '';
			lagAddTagValue = '';
			await refreshLagTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeLagTag(key: string | undefined): Promise<void> {
		if (!viewedLag?.lagId || !key) return;
		try {
			const arn = taggableArn('dxlag', viewedLag.lagId);
			await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
			toast.success('Tag removed');
			await refreshLagTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ============================ Interconnects ==============================
	// PARTNER/bookkeeping-only, per PARITY.md: real Direct Connect Interconnects
	// are physical cross-connects provisioned for a Direct Connect Partner.
	// There is no physical link to simulate -- this is pure state bookkeeping,
	// not a claim that anything is actually cross-connected.

	let createInterconnectModal = $state<Modal | null>(null);
	let creatingInterconnect = $state(false);
	let createInterconnectError = $state<string | null>(null);
	let newIcName = $state('');
	let newIcBandwidth = $state('1Gbps');
	let newIcLocation = $state('');
	let newIcLagId = $state('');
	let newIcProviderName = $state('');
	let newIcRequestMacSec = $state(false);

	function openCreateInterconnectModal(): void {
		createInterconnectError = null;
		newIcName = '';
		newIcBandwidth = '1Gbps';
		newIcLocation = '';
		newIcLagId = '';
		newIcProviderName = '';
		newIcRequestMacSec = false;
		createInterconnectModal?.open();
	}

	async function submitCreateInterconnect(): Promise<void> {
		if (!newIcName.trim() || !newIcBandwidth.trim() || !newIcLocation.trim()) {
			createInterconnectError = 'Name, bandwidth, and location are required.';
			return;
		}
		creatingInterconnect = true;
		createInterconnectError = null;
		try {
			await client().send(
				new CreateInterconnectCommand({
					interconnectName: newIcName.trim(),
					bandwidth: newIcBandwidth.trim(),
					location: newIcLocation.trim(),
					lagId: newIcLagId.trim() || undefined,
					providerName: newIcProviderName.trim() || undefined,
					requestMACSec: newIcRequestMacSec
				})
			);
			toast.success('Interconnect created');
			createInterconnectModal?.close();
			await tabLoader.refresh('interconnects');
		} catch (e) {
			const msg = describeError(e);
			createInterconnectError = msg;
			toast.error(msg);
		} finally {
			creatingInterconnect = false;
		}
	}

	async function handleDeleteInterconnect(i: Interconnect): Promise<void> {
		if (!i.interconnectId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete interconnect',
			message: `Delete interconnect "${i.interconnectName ?? i.interconnectId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteInterconnectCommand({ interconnectId: i.interconnectId }));
			toast.success('Interconnect deleted');
			await tabLoader.refresh('interconnects');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let interconnectDetailModal = $state<Modal | null>(null);
	let viewedInterconnect = $state<Interconnect | null>(null);
	let interconnectDetailLoading = $state(false);
	let interconnectDetailError = $state<string | null>(null);
	let icTags = $state<{ key?: string; value?: string }[]>([]);
	let icAddTagKey = $state('');
	let icAddTagValue = $state('');
	let icLoaResult = $state<{ contentType?: string; byteLength: number; dataUrl: string } | null>(
		null
	);

	async function openInterconnectDetail(i: Interconnect): Promise<void> {
		if (!i.interconnectId) return;
		viewedInterconnect = null;
		interconnectDetailError = null;
		icTags = [];
		icLoaResult = null;
		interconnectDetailModal?.open();
		interconnectDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeInterconnectsCommand({ interconnectId: i.interconnectId })
			);
			viewedInterconnect = resp.interconnects?.[0] ?? null;
			icTags = viewedInterconnect?.tags ?? [];
		} catch (e) {
			interconnectDetailError = describeError(e);
		} finally {
			interconnectDetailLoading = false;
		}
	}

	async function handleViewInterconnectLoa(): Promise<void> {
		if (!viewedInterconnect?.interconnectId) return;
		try {
			const resp = await client().send(
				new DescribeInterconnectLoaCommand({ interconnectId: viewedInterconnect.interconnectId })
			);
			const bytes = resp.loa?.loaContent ?? new Uint8Array();
			const blob = new Blob([bytes as BlobPart], {
				type: resp.loa?.loaContentType ?? 'application/pdf'
			});
			icLoaResult = {
				contentType: resp.loa?.loaContentType,
				byteLength: bytes.length,
				dataUrl: URL.createObjectURL(blob)
			};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// Interconnect ARNs reuse the "dxcon/" marker (services/directconnect's
	// InterconnectARN doc comment: no Terraform-managed resource type exists
	// to confirm Interconnect's own ARN segment, so the backend deliberately
	// resolves it the same way as a Connection ARN).
	async function refreshInterconnectTags(): Promise<void> {
		if (!viewedInterconnect?.interconnectId) return;
		try {
			const arn = taggableArn('dxcon', viewedInterconnect.interconnectId);
			const resp = await client().send(new DescribeTagsCommand({ resourceArns: [arn] }));
			icTags = resp.resourceTags?.[0]?.tags ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAddIcTag(): Promise<void> {
		if (!viewedInterconnect?.interconnectId || !icAddTagKey.trim()) return;
		try {
			const arn = taggableArn('dxcon', viewedInterconnect.interconnectId);
			await client().send(
				new TagResourceCommand({ resourceArn: arn, tags: [{ key: icAddTagKey.trim(), value: icAddTagValue }] })
			);
			toast.success('Tag added');
			icAddTagKey = '';
			icAddTagValue = '';
			await refreshInterconnectTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeIcTag(key: string | undefined): Promise<void> {
		if (!viewedInterconnect?.interconnectId || !key) return;
		try {
			const arn = taggableArn('dxcon', viewedInterconnect.interconnectId);
			await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
			toast.success('Tag removed');
			await refreshInterconnectTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ========================= Virtual Interfaces =============================

	let createVifModal = $state<Modal | null>(null);
	let creatingVif = $state(false);
	let createVifError = $state<string | null>(null);
	let newVifType = $state<'private' | 'public' | 'transit'>('private');
	let newVifConnectionId = $state('');
	let newVifName = $state('');
	let newVifVlan = $state(100);
	let newVifAsn = $state<number | ''>('');
	let newVifAddressFamily = $state<'' | 'ipv4' | 'ipv6'>('');
	let newVifAmazonAddress = $state('');
	let newVifCustomerAddress = $state('');
	let newVifAuthKey = $state('');
	let newVifMtu = $state(1500);
	let newVifEnableSiteLink = $state(false);
	let newVifGatewayKind = $state<'dxgw' | 'vgw'>('dxgw');
	let newVifGatewayId = $state('');
	let newVifRouteFilterPrefixes = $state('');

	function openCreateVifModal(): void {
		createVifError = null;
		newVifType = 'private';
		newVifConnectionId = '';
		newVifName = '';
		newVifVlan = 100;
		newVifAsn = '';
		newVifAddressFamily = '';
		newVifAmazonAddress = '';
		newVifCustomerAddress = '';
		newVifAuthKey = '';
		newVifMtu = 1500;
		newVifEnableSiteLink = false;
		newVifGatewayKind = 'dxgw';
		newVifGatewayId = '';
		newVifRouteFilterPrefixes = '';
		createVifModal?.open();
	}

	async function submitCreateVif(): Promise<void> {
		if (!newVifConnectionId.trim() || !newVifName.trim()) {
			createVifError = 'Connection ID and virtual interface name are required.';
			return;
		}
		creatingVif = true;
		createVifError = null;
		try {
			const common = {
				virtualInterfaceName: newVifName.trim(),
				vlan: newVifVlan,
				asn: newVifAsn === '' ? undefined : Number(newVifAsn),
				addressFamily: newVifAddressFamily || undefined,
				amazonAddress: newVifAmazonAddress.trim() || undefined,
				customerAddress: newVifCustomerAddress.trim() || undefined,
				authKey: newVifAuthKey.trim() || undefined
			};
			if (newVifType === 'private') {
				await client().send(
					new CreatePrivateVirtualInterfaceCommand({
						connectionId: newVifConnectionId.trim(),
						newPrivateVirtualInterface: {
							...common,
							mtu: newVifMtu,
							enableSiteLink: newVifEnableSiteLink,
							directConnectGatewayId:
								newVifGatewayKind === 'dxgw' ? newVifGatewayId.trim() || undefined : undefined,
							virtualGatewayId:
								newVifGatewayKind === 'vgw' ? newVifGatewayId.trim() || undefined : undefined
						}
					})
				);
			} else if (newVifType === 'public') {
				await client().send(
					new CreatePublicVirtualInterfaceCommand({
						connectionId: newVifConnectionId.trim(),
						newPublicVirtualInterface: {
							...common,
							routeFilterPrefixes: parseCidrList(newVifRouteFilterPrefixes)
						}
					})
				);
			} else {
				await client().send(
					new CreateTransitVirtualInterfaceCommand({
						connectionId: newVifConnectionId.trim(),
						newTransitVirtualInterface: {
							...common,
							mtu: newVifMtu,
							enableSiteLink: newVifEnableSiteLink,
							directConnectGatewayId: newVifGatewayId.trim() || undefined
						}
					})
				);
			}
			toast.success('Virtual interface created');
			createVifModal?.close();
			await tabLoader.refresh('virtualInterfaces');
		} catch (e) {
			const msg = describeError(e);
			createVifError = msg;
			toast.error(msg);
		} finally {
			creatingVif = false;
		}
	}

	async function handleDeleteVif(v: VirtualInterface): Promise<void> {
		if (!v.virtualInterfaceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete virtual interface',
			message: `Delete virtual interface "${v.virtualInterfaceName ?? v.virtualInterfaceId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteVirtualInterfaceCommand({ virtualInterfaceId: v.virtualInterfaceId }));
			toast.success('Virtual interface deleted');
			await tabLoader.refresh('virtualInterfaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editVifModal = $state<Modal | null>(null);
	let editingVif = $state(false);
	let editVifError = $state<string | null>(null);
	let editVifId = $state('');
	let editVifName = $state('');
	let editVifMtu = $state<number | ''>('');
	let editVifRateLimit = $state('');
	let editVifEnableSiteLink = $state<'' | 'true' | 'false'>('');

	function openEditVifModal(v: VirtualInterface): void {
		if (!v.virtualInterfaceId) return;
		editVifError = null;
		editVifId = v.virtualInterfaceId;
		editVifName = v.virtualInterfaceName ?? '';
		editVifMtu = v.mtu ?? '';
		editVifRateLimit = v.rateLimit ?? '';
		editVifEnableSiteLink = '';
		editVifModal?.open();
	}

	async function submitEditVif(): Promise<void> {
		if (!editVifId) return;
		editingVif = true;
		editVifError = null;
		try {
			await client().send(
				new UpdateVirtualInterfaceAttributesCommand({
					virtualInterfaceId: editVifId,
					virtualInterfaceName: editVifName.trim() || undefined,
					mtu: editVifMtu === '' ? undefined : Number(editVifMtu),
					rateLimit: editVifRateLimit.trim() || undefined,
					enableSiteLink: editVifEnableSiteLink === '' ? undefined : editVifEnableSiteLink === 'true'
				})
			);
			toast.success('Virtual interface updated');
			editVifModal?.close();
			await tabLoader.refresh('virtualInterfaces');
		} catch (e) {
			const msg = describeError(e);
			editVifError = msg;
			toast.error(msg);
		} finally {
			editingVif = false;
		}
	}

	let vifDetailModal = $state<Modal | null>(null);
	let viewedVif = $state<VirtualInterface | null>(null);
	let vifDetailLoading = $state(false);
	let vifDetailError = $state<string | null>(null);
	let vifTags = $state<{ key?: string; value?: string }[]>([]);
	let vifAddTagKey = $state('');
	let vifAddTagValue = $state('');
	let confirmGatewayId = $state('');
	let associateVifConnId = $state('');
	let bgpNewAsn = $state<number | ''>('');
	let bgpNewAddressFamily = $state<'' | 'ipv4' | 'ipv6'>('');
	let bgpNewAmazonAddress = $state('');
	let bgpNewCustomerAddress = $state('');
	let bgpNewAuthKey = $state('');
	let failoverTestPeers = $state('');
	let failoverTestDuration = $state<number | ''>('');
	let routerTypeIdentifier = $state('');
	let routerConfigResult = $state<{
		customerRouterConfig?: string;
		vendor?: string;
		platform?: string;
		software?: string;
	} | null>(null);

	async function openVifDetail(v: VirtualInterface): Promise<void> {
		if (!v.virtualInterfaceId) return;
		viewedVif = null;
		vifDetailError = null;
		vifTags = [];
		confirmGatewayId = '';
		associateVifConnId = '';
		routerConfigResult = null;
		vifDetailModal?.open();
		vifDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVirtualInterfacesCommand({ virtualInterfaceId: v.virtualInterfaceId })
			);
			viewedVif = resp.virtualInterfaces?.[0] ?? null;
			vifTags = viewedVif?.tags ?? [];
		} catch (e) {
			vifDetailError = describeError(e);
		} finally {
			vifDetailLoading = false;
		}
	}

	async function refreshVifDetail(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		const resp = await client().send(
			new DescribeVirtualInterfacesCommand({ virtualInterfaceId: viewedVif.virtualInterfaceId })
		);
		viewedVif = resp.virtualInterfaces?.[0] ?? viewedVif;
	}

	async function handleConfirmVif(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		try {
			if (viewedVif.virtualInterfaceType === 'private') {
				await client().send(
					new ConfirmPrivateVirtualInterfaceCommand({
						virtualInterfaceId: viewedVif.virtualInterfaceId,
						directConnectGatewayId: confirmGatewayId.trim() || undefined,
						virtualGatewayId: confirmGatewayId.trim() || undefined
					})
				);
			} else if (viewedVif.virtualInterfaceType === 'public') {
				await client().send(
					new ConfirmPublicVirtualInterfaceCommand({ virtualInterfaceId: viewedVif.virtualInterfaceId })
				);
			} else {
				await client().send(
					new ConfirmTransitVirtualInterfaceCommand({
						virtualInterfaceId: viewedVif.virtualInterfaceId,
						directConnectGatewayId: confirmGatewayId.trim()
					})
				);
			}
			toast.success('Virtual interface confirmed');
			await refreshVifDetail();
			await tabLoader.refresh('virtualInterfaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAssociateVif(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId || !associateVifConnId.trim()) return;
		try {
			await client().send(
				new AssociateVirtualInterfaceCommand({
					virtualInterfaceId: viewedVif.virtualInterfaceId,
					connectionId: associateVifConnId.trim()
				})
			);
			toast.success('Virtual interface reassociated');
			associateVifConnId = '';
			await refreshVifDetail();
			await tabLoader.refresh('virtualInterfaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitCreateBgpPeer(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		try {
			const resp = await client().send(
				new CreateBGPPeerCommand({
					virtualInterfaceId: viewedVif.virtualInterfaceId,
					newBGPPeer: {
						asn: bgpNewAsn === '' ? undefined : Number(bgpNewAsn),
						addressFamily: bgpNewAddressFamily || undefined,
						amazonAddress: bgpNewAmazonAddress.trim() || undefined,
						customerAddress: bgpNewCustomerAddress.trim() || undefined,
						authKey: bgpNewAuthKey.trim() || undefined
					}
				})
			);
			viewedVif = resp.virtualInterface ?? viewedVif;
			toast.success('BGP peer created');
			bgpNewAsn = '';
			bgpNewAddressFamily = '';
			bgpNewAmazonAddress = '';
			bgpNewCustomerAddress = '';
			bgpNewAuthKey = '';
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleDeleteBgpPeer(peer: BGPPeer): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete BGP peer',
			message: `Delete BGP peer "${peer.bgpPeerId}"?`
		});
		if (!confirmed) return;
		try {
			const resp = await client().send(
				new DeleteBGPPeerCommand({
					virtualInterfaceId: viewedVif.virtualInterfaceId,
					bgpPeerId: peer.bgpPeerId
				})
			);
			viewedVif = resp.virtualInterface ?? viewedVif;
			toast.success('BGP peer deleted');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleStartFailoverTest(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		try {
			await client().send(
				new StartBgpFailoverTestCommand({
					virtualInterfaceId: viewedVif.virtualInterfaceId,
					bgpPeers: failoverTestPeers
						.split(',')
						.map((s) => s.trim())
						.filter(Boolean),
					testDurationInMinutes: failoverTestDuration === '' ? undefined : Number(failoverTestDuration)
				})
			);
			toast.success('BGP failover test started');
			await refreshVifDetail();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleStopFailoverTest(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		try {
			await client().send(
				new StopBgpFailoverTestCommand({ virtualInterfaceId: viewedVif.virtualInterfaceId })
			);
			toast.success('BGP failover test stopped');
			await refreshVifDetail();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleFetchRouterConfig(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		try {
			const resp = await client().send(
				new DescribeRouterConfigurationCommand({
					virtualInterfaceId: viewedVif.virtualInterfaceId,
					routerTypeIdentifier: routerTypeIdentifier.trim() || undefined
				})
			);
			routerConfigResult = {
				customerRouterConfig: resp.customerRouterConfig,
				vendor: resp.router?.vendor,
				platform: resp.router?.platform,
				software: resp.router?.software
			};
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function refreshVifTags(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId) return;
		try {
			const arn = taggableArn('dxvif', viewedVif.virtualInterfaceId);
			const resp = await client().send(new DescribeTagsCommand({ resourceArns: [arn] }));
			vifTags = resp.resourceTags?.[0]?.tags ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAddVifTag(): Promise<void> {
		if (!viewedVif?.virtualInterfaceId || !vifAddTagKey.trim()) return;
		try {
			const arn = taggableArn('dxvif', viewedVif.virtualInterfaceId);
			await client().send(
				new TagResourceCommand({ resourceArn: arn, tags: [{ key: vifAddTagKey.trim(), value: vifAddTagValue }] })
			);
			toast.success('Tag added');
			vifAddTagKey = '';
			vifAddTagValue = '';
			await refreshVifTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeVifTag(key: string | undefined): Promise<void> {
		if (!viewedVif?.virtualInterfaceId || !key) return;
		try {
			const arn = taggableArn('dxvif', viewedVif.virtualInterfaceId);
			await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
			toast.success('Tag removed');
			await refreshVifTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ============================== Gateways ==================================

	let createGatewayModal = $state<Modal | null>(null);
	let creatingGateway = $state(false);
	let createGatewayError = $state<string | null>(null);
	let newGwName = $state('');
	let newGwAmazonSideAsn = $state<number | ''>('');

	function openCreateGatewayModal(): void {
		createGatewayError = null;
		newGwName = '';
		newGwAmazonSideAsn = '';
		createGatewayModal?.open();
	}

	async function submitCreateGateway(): Promise<void> {
		if (!newGwName.trim()) {
			createGatewayError = 'Name is required.';
			return;
		}
		creatingGateway = true;
		createGatewayError = null;
		try {
			await client().send(
				new CreateDirectConnectGatewayCommand({
					directConnectGatewayName: newGwName.trim(),
					amazonSideAsn: newGwAmazonSideAsn === '' ? undefined : Number(newGwAmazonSideAsn)
				})
			);
			toast.success('Direct Connect gateway created');
			createGatewayModal?.close();
			await tabLoader.refresh('gateways');
		} catch (e) {
			const msg = describeError(e);
			createGatewayError = msg;
			toast.error(msg);
		} finally {
			creatingGateway = false;
		}
	}

	async function handleDeleteGateway(g: DirectConnectGateway): Promise<void> {
		if (!g.directConnectGatewayId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Direct Connect gateway',
			message: `Delete gateway "${g.directConnectGatewayName ?? g.directConnectGatewayId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDirectConnectGatewayCommand({ directConnectGatewayId: g.directConnectGatewayId })
			);
			toast.success('Gateway deleted');
			await tabLoader.refresh('gateways');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editGatewayModal = $state<Modal | null>(null);
	let editingGateway = $state(false);
	let editGatewayError = $state<string | null>(null);
	let editGatewayId = $state('');
	let editGatewayName = $state('');

	function openEditGatewayModal(g: DirectConnectGateway): void {
		if (!g.directConnectGatewayId) return;
		editGatewayError = null;
		editGatewayId = g.directConnectGatewayId;
		editGatewayName = g.directConnectGatewayName ?? '';
		editGatewayModal?.open();
	}

	async function submitEditGateway(): Promise<void> {
		if (!editGatewayId || !editGatewayName.trim()) {
			editGatewayError = 'New name is required.';
			return;
		}
		editingGateway = true;
		editGatewayError = null;
		try {
			await client().send(
				new UpdateDirectConnectGatewayCommand({
					directConnectGatewayId: editGatewayId,
					newDirectConnectGatewayName: editGatewayName.trim()
				})
			);
			toast.success('Gateway updated');
			editGatewayModal?.close();
			await tabLoader.refresh('gateways');
		} catch (e) {
			const msg = describeError(e);
			editGatewayError = msg;
			toast.error(msg);
		} finally {
			editingGateway = false;
		}
	}

	let gatewayDetailModal = $state<Modal | null>(null);
	let viewedGateway = $state<DirectConnectGateway | null>(null);
	let gatewayDetailLoading = $state(false);
	let gatewayDetailError = $state<string | null>(null);
	let gwTags = $state<{ key?: string; value?: string }[]>([]);
	let gwAddTagKey = $state('');
	let gwAddTagValue = $state('');

	async function openGatewayDetail(g: DirectConnectGateway): Promise<void> {
		if (!g.directConnectGatewayId) return;
		viewedGateway = null;
		gatewayDetailError = null;
		gwTags = [];
		gatewayDetailModal?.open();
		gatewayDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeDirectConnectGatewaysCommand({ directConnectGatewayId: g.directConnectGatewayId })
			);
			viewedGateway = resp.directConnectGateways?.[0] ?? null;
			gwTags = viewedGateway?.tags ?? [];
		} catch (e) {
			gatewayDetailError = describeError(e);
		} finally {
			gatewayDetailLoading = false;
		}
	}

	async function refreshGatewayTags(): Promise<void> {
		if (!viewedGateway?.directConnectGatewayId) return;
		try {
			const arn = taggableArn('dx-gateway', viewedGateway.directConnectGatewayId);
			const resp = await client().send(new DescribeTagsCommand({ resourceArns: [arn] }));
			gwTags = resp.resourceTags?.[0]?.tags ?? [];
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function submitAddGwTag(): Promise<void> {
		if (!viewedGateway?.directConnectGatewayId || !gwAddTagKey.trim()) return;
		try {
			const arn = taggableArn('dx-gateway', viewedGateway.directConnectGatewayId);
			await client().send(
				new TagResourceCommand({ resourceArn: arn, tags: [{ key: gwAddTagKey.trim(), value: gwAddTagValue }] })
			);
			toast.success('Tag added');
			gwAddTagKey = '';
			gwAddTagValue = '';
			await refreshGatewayTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function removeGwTag(key: string | undefined): Promise<void> {
		if (!viewedGateway?.directConnectGatewayId || !key) return;
		try {
			const arn = taggableArn('dx-gateway', viewedGateway.directConnectGatewayId);
			await client().send(new UntagResourceCommand({ resourceArn: arn, tagKeys: [key] }));
			toast.success('Tag removed');
			await refreshGatewayTags();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ========================= Gateway Associations ============================
	// GatewayId is validated by the real backend against actual (mock) EC2
	// VpnGateway/TransitGateway records (SetEC2GatewayResolver) -- the
	// Virtual Gateways tab's own DescribeVirtualGateways list (proxied
	// straight from EC2) feeds this datalist so real IDs can be picked
	// instead of guessed.

	let createGaModal = $state<Modal | null>(null);
	let creatingGa = $state(false);
	let createGaError = $state<string | null>(null);
	let newGaGatewayId = $state('');
	let newGaDcgwId = $state('');
	let newGaAddPrefixes = $state('');

	function openCreateGaModal(): void {
		createGaError = null;
		newGaGatewayId = '';
		newGaDcgwId = '';
		newGaAddPrefixes = '';
		void tabLoader.load('virtualGateways');
		createGaModal?.open();
	}

	async function submitCreateGa(): Promise<void> {
		if (!newGaDcgwId.trim() || !newGaGatewayId.trim()) {
			createGaError = 'Direct Connect gateway ID and VGW/TGW ID are required.';
			return;
		}
		creatingGa = true;
		createGaError = null;
		try {
			await client().send(
				new CreateDirectConnectGatewayAssociationCommand({
					directConnectGatewayId: newGaDcgwId.trim(),
					gatewayId: newGaGatewayId.trim(),
					addAllowedPrefixesToDirectConnectGateway: parseCidrList(newGaAddPrefixes)
				})
			);
			toast.success('Gateway association created');
			createGaModal?.close();
			await tabLoader.refresh('gatewayAssociations');
		} catch (e) {
			const msg = describeError(e);
			createGaError = msg;
			toast.error(msg);
		} finally {
			creatingGa = false;
		}
	}

	async function handleDeleteGa(a: DirectConnectGatewayAssociation): Promise<void> {
		if (!a.associationId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete gateway association',
			message: `Delete association "${a.associationId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDirectConnectGatewayAssociationCommand({ associationId: a.associationId }));
			toast.success('Association deleted');
			await tabLoader.refresh('gatewayAssociations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editGaModal = $state<Modal | null>(null);
	let editingGa = $state(false);
	let editGaError = $state<string | null>(null);
	let editGaId = $state('');
	let editGaAddPrefixes = $state('');
	let editGaRemovePrefixes = $state('');

	function openEditGaModal(a: DirectConnectGatewayAssociation): void {
		if (!a.associationId) return;
		editGaError = null;
		editGaId = a.associationId;
		editGaAddPrefixes = '';
		editGaRemovePrefixes = '';
		editGaModal?.open();
	}

	async function submitEditGa(): Promise<void> {
		if (!editGaId) return;
		editingGa = true;
		editGaError = null;
		try {
			await client().send(
				new UpdateDirectConnectGatewayAssociationCommand({
					associationId: editGaId,
					addAllowedPrefixesToDirectConnectGateway: parseCidrList(editGaAddPrefixes),
					removeAllowedPrefixesToDirectConnectGateway: parseCidrList(editGaRemovePrefixes)
				})
			);
			toast.success('Association updated');
			editGaModal?.close();
			await tabLoader.refresh('gatewayAssociations');
		} catch (e) {
			const msg = describeError(e);
			editGaError = msg;
			toast.error(msg);
		} finally {
			editingGa = false;
		}
	}

	let gaDetailModal = $state<Modal | null>(null);
	let viewedGa = $state<DirectConnectGatewayAssociation | null>(null);

	function openGaDetail(a: DirectConnectGatewayAssociation): void {
		viewedGa = a;
		gaDetailModal?.open();
	}

	// ======================= Gateway Association Proposals ======================

	let createProposalModal = $state<Modal | null>(null);
	let creatingProposal = $state(false);
	let createProposalError = $state<string | null>(null);
	let newPropDcgwId = $state('');
	let newPropOwnerAccount = $state('');
	let newPropGatewayId = $state('');
	let newPropAddPrefixes = $state('');

	function openCreateProposalModal(): void {
		createProposalError = null;
		newPropDcgwId = '';
		newPropOwnerAccount = '';
		newPropGatewayId = '';
		newPropAddPrefixes = '';
		createProposalModal?.open();
	}

	async function submitCreateProposal(): Promise<void> {
		if (!newPropDcgwId.trim() || !newPropOwnerAccount.trim() || !newPropGatewayId.trim()) {
			createProposalError = 'Gateway ID, owner account, and VGW/TGW ID are all required.';
			return;
		}
		creatingProposal = true;
		createProposalError = null;
		try {
			await client().send(
				new CreateDirectConnectGatewayAssociationProposalCommand({
					directConnectGatewayId: newPropDcgwId.trim(),
					directConnectGatewayOwnerAccount: newPropOwnerAccount.trim(),
					gatewayId: newPropGatewayId.trim(),
					addAllowedPrefixesToDirectConnectGateway: parseCidrList(newPropAddPrefixes)
				})
			);
			toast.success('Association proposal created');
			createProposalModal?.close();
			await tabLoader.refresh('gatewayProposals');
		} catch (e) {
			const msg = describeError(e);
			createProposalError = msg;
			toast.error(msg);
		} finally {
			creatingProposal = false;
		}
	}

	async function handleDeleteProposal(p: DirectConnectGatewayAssociationProposal): Promise<void> {
		if (!p.proposalId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete association proposal',
			message: `Delete proposal "${p.proposalId}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteDirectConnectGatewayAssociationProposalCommand({ proposalId: p.proposalId })
			);
			toast.success('Proposal deleted');
			await tabLoader.refresh('gatewayProposals');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let proposalDetailModal = $state<Modal | null>(null);
	let viewedProposal = $state<DirectConnectGatewayAssociationProposal | null>(null);
	let acceptOwnerAccount = $state('');
	let acceptOverridePrefixes = $state('');

	function openProposalDetail(p: DirectConnectGatewayAssociationProposal): void {
		viewedProposal = p;
		acceptOwnerAccount = '';
		acceptOverridePrefixes = '';
		proposalDetailModal?.open();
	}

	async function handleAcceptProposal(): Promise<void> {
		if (!viewedProposal?.proposalId || !viewedProposal.directConnectGatewayId) return;
		if (!acceptOwnerAccount.trim()) {
			toast.error('Associated gateway owner account is required.');
			return;
		}
		try {
			await client().send(
				new AcceptDirectConnectGatewayAssociationProposalCommand({
					directConnectGatewayId: viewedProposal.directConnectGatewayId,
					proposalId: viewedProposal.proposalId,
					associatedGatewayOwnerAccount: acceptOwnerAccount.trim(),
					overrideAllowedPrefixesToDirectConnectGateway: acceptOverridePrefixes.trim()
						? parseCidrList(acceptOverridePrefixes)
						: undefined
				})
			);
			toast.success('Proposal accepted');
			proposalDetailModal?.close();
			await tabLoader.refresh('gatewayProposals');
			await tabLoader.refresh('gatewayAssociations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ============================== Locations ==================================

	let confirmAgreementName = $state('');
	let confirmingAgreement = $state(false);

	async function handleConfirmCustomerAgreement(): Promise<void> {
		confirmingAgreement = true;
		try {
			const resp = await client().send(
				new ConfirmCustomerAgreementCommand({ agreementName: confirmAgreementName.trim() || undefined })
			);
			toast.success(`Agreement status: ${resp.status ?? 'unknown'}`);
			confirmAgreementName = '';
			await fetchLocationsAndMetadata();
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			confirmingAgreement = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Cable}
		title="AWS Direct Connect"
		description="Dedicated network connections between your infrastructure and AWS"
		onRefresh={handleRefresh}
		color="sky"
		service="directconnect"
	>
		{#snippet actions()}
			{#if activeTab === 'connections'}
				<button
					onclick={openCreateConnectionModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Connection
				</button>
			{:else if activeTab === 'lags'}
				<button
					onclick={openCreateLagModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create LAG
				</button>
			{:else if activeTab === 'interconnects'}
				<button
					onclick={openCreateInterconnectModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Interconnect
				</button>
			{:else if activeTab === 'virtualInterfaces'}
				<button
					onclick={openCreateVifModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Virtual Interface
				</button>
			{:else if activeTab === 'gateways'}
				<button
					onclick={openCreateGatewayModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Gateway
				</button>
			{:else if activeTab === 'gatewayAssociations'}
				<button
					onclick={openCreateGaModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Association
				</button>
			{:else if activeTab === 'gatewayProposals'}
				<button
					onclick={openCreateProposalModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Proposal
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="sky" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTab === 'interconnects'}
				<div
					role="note"
					class="text-xs rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-3 py-2 text-amber-800 dark:text-amber-300"
				>
					Interconnects model AWS Direct Connect Partner physical cross-connect infrastructure --
					this is state bookkeeping only, not a simulation of an actual physical link.
				</div>
			{:else if activeTab === 'gatewayAttachments'}
				<div
					role="note"
					class="text-xs rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-3 py-2 text-amber-800 dark:text-amber-300"
				>
					Attachments are derived automatically whenever a private/transit virtual interface names
					a Direct Connect gateway -- there is no dedicated create/delete operation for this
					resource.
				</div>
			{:else if activeTab === 'virtualGateways'}
				<div
					role="note"
					class="text-xs rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-3 py-2 text-amber-800 dark:text-amber-300"
				>
					Proxied straight from EC2's own VpnGateway records -- read-only here, and usable as
					real Gateway IDs when creating a Gateway Association.
				</div>
			{:else if activeTab === 'gateways'}
				<div
					role="note"
					class="text-xs rounded-lg border border-amber-300 bg-amber-50 dark:bg-amber-900/20 dark:border-amber-800 px-3 py-2 text-amber-800 dark:text-amber-300"
				>
					A Direct Connect gateway is a GLOBAL resource -- its ARN carries no region segment,
					unlike Connections/LAGs/Virtual Interfaces.
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

			{#if activeTab === 'connections'}
				{#snippet connStateCell(c: Connection)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(c.connectionState)}"
						>{c.connectionState ?? '—'}</span
					>
				{/snippet}
				{#snippet connActionsCell(c: Connection)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openConnectionDetail(c)}
							title="View"
							aria-label="View connection {c.connectionName}"
							class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditConnectionModal(c)}
							title="Edit"
							aria-label="Edit connection {c.connectionName}"
							class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteConnection(c)}
							title="Delete"
							aria-label="Delete connection {c.connectionName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const connColumns = defineColumns<Connection>([
					{ key: 'connectionId', label: 'ID' },
					{ key: 'connectionName', label: 'Name' },
					{ key: 'connectionState', label: 'State', render: connStateCell },
					{ key: 'bandwidth', label: 'Bandwidth' },
					{ key: 'location', label: 'Location' },
					{ key: 'lagId', label: 'LAG ID' },
					{ key: 'actions', label: '', render: connActionsCell }
				])}
				<DataTable
					rows={filteredConnections}
					rowKey={(c) => c.connectionId ?? ''}
					columns={connColumns}
					loading={tabLoader.isLoading('connections')}
					emptyMessage="No connections found"
				/>
				<LoadMore
					hasMore={!!connectionsNextToken}
					loading={loadingMoreConnections}
					onLoadMore={loadMoreConnections}
				/>
			{:else if activeTab === 'lags'}
				{#snippet lagStateCell(l: Lag)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(l.lagState)}"
						>{l.lagState ?? '—'}</span
					>
				{/snippet}
				{#snippet lagActionsCell(l: Lag)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openLagDetail(l)}
							title="View"
							aria-label="View LAG {l.lagName}"
							class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditLagModal(l)}
							title="Edit"
							aria-label="Edit LAG {l.lagName}"
							class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteLag(l)}
							title="Delete"
							aria-label="Delete LAG {l.lagName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const lagColumns = defineColumns<Lag>([
					{ key: 'lagId', label: 'ID' },
					{ key: 'lagName', label: 'Name' },
					{ key: 'lagState', label: 'State', render: lagStateCell },
					{ key: 'connectionsBandwidth', label: 'Bandwidth' },
					{ key: 'numberOfConnections', label: '# Connections' },
					{ key: 'minimumLinks', label: 'Min Links' },
					{ key: 'actions', label: '', render: lagActionsCell }
				])}
				<DataTable
					rows={filteredLags}
					rowKey={(l) => l.lagId ?? ''}
					columns={lagColumns}
					loading={tabLoader.isLoading('lags')}
					emptyMessage="No LAGs found"
				/>
				<LoadMore hasMore={!!lagsNextToken} loading={loadingMoreLags} onLoadMore={loadMoreLags} />
			{:else if activeTab === 'interconnects'}
				{#snippet icStateCell(i: Interconnect)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(i.interconnectState)}"
						>{i.interconnectState ?? '—'}</span
					>
				{/snippet}
				{#snippet icActionsCell(i: Interconnect)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openInterconnectDetail(i)}
							title="View"
							aria-label="View interconnect {i.interconnectName}"
							class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteInterconnect(i)}
							title="Delete"
							aria-label="Delete interconnect {i.interconnectName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const icColumns = defineColumns<Interconnect>([
					{ key: 'interconnectId', label: 'ID' },
					{ key: 'interconnectName', label: 'Name' },
					{ key: 'interconnectState', label: 'State', render: icStateCell },
					{ key: 'bandwidth', label: 'Bandwidth' },
					{ key: 'location', label: 'Location' },
					{ key: 'actions', label: '', render: icActionsCell }
				])}
				<DataTable
					rows={filteredInterconnects}
					rowKey={(i) => i.interconnectId ?? ''}
					columns={icColumns}
					loading={tabLoader.isLoading('interconnects')}
					emptyMessage="No interconnects found"
				/>
				<LoadMore
					hasMore={!!interconnectsNextToken}
					loading={loadingMoreInterconnects}
					onLoadMore={loadMoreInterconnects}
				/>
			{:else if activeTab === 'virtualInterfaces'}
				{#snippet vifStateCell(v: VirtualInterface)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(v.virtualInterfaceState)}"
						>{v.virtualInterfaceState ?? '—'}</span
					>
				{/snippet}
				{#snippet vifActionsCell(v: VirtualInterface)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openVifDetail(v)}
							title="View"
							aria-label="View virtual interface {v.virtualInterfaceName}"
							class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditVifModal(v)}
							title="Edit"
							aria-label="Edit virtual interface {v.virtualInterfaceName}"
							class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteVif(v)}
							title="Delete"
							aria-label="Delete virtual interface {v.virtualInterfaceName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const vifColumns = defineColumns<VirtualInterface>([
					{ key: 'virtualInterfaceId', label: 'ID' },
					{ key: 'virtualInterfaceName', label: 'Name' },
					{ key: 'virtualInterfaceType', label: 'Type' },
					{ key: 'virtualInterfaceState', label: 'State', render: vifStateCell },
					{ key: 'connectionId', label: 'Connection' },
					{ key: 'vlan', label: 'VLAN' },
					{ key: 'actions', label: '', render: vifActionsCell }
				])}
				<DataTable
					rows={filteredVifs}
					rowKey={(v) => v.virtualInterfaceId ?? ''}
					columns={vifColumns}
					loading={tabLoader.isLoading('virtualInterfaces')}
					emptyMessage="No virtual interfaces found"
				/>
				<LoadMore hasMore={!!vifsNextToken} loading={loadingMoreVifs} onLoadMore={loadMoreVifs} />
			{:else if activeTab === 'gateways'}
				{#snippet gwStateCell(g: DirectConnectGateway)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(g.directConnectGatewayState)}"
						>{g.directConnectGatewayState ?? '—'}</span
					>
				{/snippet}
				{#snippet gwActionsCell(g: DirectConnectGateway)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openGatewayDetail(g)}
							title="View"
							aria-label="View gateway {g.directConnectGatewayName}"
							class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditGatewayModal(g)}
							title="Edit"
							aria-label="Edit gateway {g.directConnectGatewayName}"
							class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteGateway(g)}
							title="Delete"
							aria-label="Delete gateway {g.directConnectGatewayName}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const gwColumns = defineColumns<DirectConnectGateway>([
					{ key: 'directConnectGatewayId', label: 'ID' },
					{ key: 'directConnectGatewayName', label: 'Name' },
					{ key: 'directConnectGatewayState', label: 'State', render: gwStateCell },
					{ key: 'amazonSideAsn', label: 'Amazon Side ASN' },
					{ key: 'ownerAccount', label: 'Owner Account' },
					{ key: 'actions', label: '', render: gwActionsCell }
				])}
				<DataTable
					rows={filteredGateways}
					rowKey={(g) => g.directConnectGatewayId ?? ''}
					columns={gwColumns}
					loading={tabLoader.isLoading('gateways')}
					emptyMessage="No Direct Connect gateways found"
				/>
				<LoadMore
					hasMore={!!gatewaysNextToken}
					loading={loadingMoreGateways}
					onLoadMore={loadMoreGateways}
				/>
			{:else if activeTab === 'gatewayAssociations'}
				{#snippet gaStateCell(a: DirectConnectGatewayAssociation)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(a.associationState)}"
						>{a.associationState ?? '—'}</span
					>
				{/snippet}
				{#snippet gaActionsCell(a: DirectConnectGatewayAssociation)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openGaDetail(a)}
							title="View"
							aria-label="View association {a.associationId}"
							class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditGaModal(a)}
							title="Edit"
							aria-label="Edit association {a.associationId}"
							class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteGa(a)}
							title="Delete"
							aria-label="Delete association {a.associationId}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{#snippet gaAssociatedGatewayCell(a: DirectConnectGatewayAssociation)}
					{a.associatedGateway?.id ?? a.virtualGatewayId ?? '—'}
				{/snippet}
				{@const gaColumns = defineColumns<DirectConnectGatewayAssociation>([
					{ key: 'associationId', label: 'Association ID' },
					{ key: 'directConnectGatewayId', label: 'Gateway ID' },
					{ key: 'associationState', label: 'State', render: gaStateCell },
					{
						key: 'associatedGatewayId',
						label: 'Associated Gateway',
						render: gaAssociatedGatewayCell
					},
					{ key: 'actions', label: '', render: gaActionsCell }
				])}
				<DataTable
					rows={filteredGatewayAssociations}
					rowKey={(a) => a.associationId ?? ''}
					columns={gaColumns}
					loading={tabLoader.isLoading('gatewayAssociations')}
					emptyMessage="No gateway associations found"
				/>
				<LoadMore
					hasMore={!!gatewayAssociationsNextToken}
					loading={loadingMoreGatewayAssociations}
					onLoadMore={loadMoreGatewayAssociations}
				/>
			{:else if activeTab === 'gatewayProposals'}
				{#snippet propStateCell(p: DirectConnectGatewayAssociationProposal)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(p.proposalState)}"
						>{p.proposalState ?? '—'}</span
					>
				{/snippet}
				{#snippet propActionsCell(p: DirectConnectGatewayAssociationProposal)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openProposalDetail(p)}
							title="View / Accept"
							aria-label="View proposal {p.proposalId}"
							class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteProposal(p)}
							title="Delete"
							aria-label="Delete proposal {p.proposalId}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const propColumns = defineColumns<DirectConnectGatewayAssociationProposal>([
					{ key: 'proposalId', label: 'Proposal ID' },
					{ key: 'directConnectGatewayId', label: 'Gateway ID' },
					{ key: 'directConnectGatewayOwnerAccount', label: 'Gateway Owner Account' },
					{ key: 'proposalState', label: 'State', render: propStateCell },
					{ key: 'actions', label: '', render: propActionsCell }
				])}
				<DataTable
					rows={filteredGatewayProposals}
					rowKey={(p) => p.proposalId ?? ''}
					columns={propColumns}
					loading={tabLoader.isLoading('gatewayProposals')}
					emptyMessage="No association proposals found"
				/>
				<LoadMore
					hasMore={!!gatewayProposalsNextToken}
					loading={loadingMoreGatewayProposals}
					onLoadMore={loadMoreGatewayProposals}
				/>
			{:else if activeTab === 'gatewayAttachments'}
				{#snippet attStateCell(a: DirectConnectGatewayAttachment)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(a.attachmentState)}"
						>{a.attachmentState ?? '—'}</span
					>
				{/snippet}
				{@const attColumns = defineColumns<DirectConnectGatewayAttachment>([
					{ key: 'directConnectGatewayId', label: 'Gateway ID' },
					{ key: 'virtualInterfaceId', label: 'Virtual Interface ID' },
					{ key: 'attachmentType', label: 'Type' },
					{ key: 'attachmentState', label: 'State', render: attStateCell },
					{ key: 'virtualInterfaceOwnerAccount', label: 'VIF Owner Account' },
					{ key: 'virtualInterfaceRegion', label: 'VIF Region' }
				])}
				<DataTable
					rows={filteredGatewayAttachments}
					rowKey={(a) => `${a.directConnectGatewayId ?? ''}/${a.virtualInterfaceId ?? ''}`}
					columns={attColumns}
					loading={tabLoader.isLoading('gatewayAttachments')}
					emptyMessage="No gateway attachments found"
				/>
				<LoadMore
					hasMore={!!gatewayAttachmentsNextToken}
					loading={loadingMoreGatewayAttachments}
					onLoadMore={loadMoreGatewayAttachments}
				/>
			{:else if activeTab === 'virtualGateways'}
				{#snippet vgwStateCell(g: VirtualGateway)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(g.virtualGatewayState)}"
						>{g.virtualGatewayState ?? '—'}</span
					>
				{/snippet}
				{@const vgwColumns = defineColumns<VirtualGateway>([
					{ key: 'virtualGatewayId', label: 'ID' },
					{ key: 'virtualGatewayState', label: 'State', render: vgwStateCell }
				])}
				<DataTable
					rows={filteredVirtualGateways}
					rowKey={(g) => g.virtualGatewayId ?? ''}
					columns={vgwColumns}
					loading={tabLoader.isLoading('virtualGateways')}
					emptyMessage="No virtual gateways found"
				/>
			{:else if activeTab === 'locations'}
				{#snippet locProvidersCell(l: Location)}
					{(l.availableProviders ?? []).join(', ') || '—'}
				{/snippet}
				{#snippet locPortSpeedsCell(l: Location)}
					{(l.availablePortSpeeds ?? []).join(', ') || '—'}
				{/snippet}
				{@const locColumns = defineColumns<Location>([
					{ key: 'locationCode', label: 'Code' },
					{ key: 'locationName', label: 'Name' },
					{ key: 'region', label: 'Region' },
					{ key: 'availablePortSpeeds', label: 'Port Speeds', render: locPortSpeedsCell },
					{ key: 'availableProviders', label: 'Providers', render: locProvidersCell }
				])}
				<DataTable
					rows={filteredLocations}
					rowKey={(l) => l.locationCode ?? ''}
					columns={locColumns}
					loading={tabLoader.isLoading('locations')}
					emptyMessage="No locations found"
				/>
				<div class="border-t border-slate-200 dark:border-slate-700 pt-4 space-y-2">
					<h3 class="text-sm font-semibold">Customer Metadata</h3>
					<p class="text-xs text-slate-500">
						NNI partner type: {nniPartnerType ?? 'nonPartner'} -- no real signed agreement workflow
						is modeled; this is honestly an always-empty default until you confirm one below.
					</p>
					{#each customerAgreements as agreement (agreement.agreementName)}
						<p class="text-xs text-slate-500">{agreement.agreementName}: {agreement.status}</p>
					{:else}
						<p class="text-xs text-slate-500">No customer agreements.</p>
					{/each}
					<div class="flex items-center gap-2">
						<label class="sr-only" for="confirm-agreement-name">Agreement name</label>
						<input
							id="confirm-agreement-name"
							bind:value={confirmAgreementName}
							placeholder="Agreement name (optional)"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={handleConfirmCustomerAgreement}
							disabled={confirmingAgreement}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700 disabled:opacity-50"
							>Confirm Agreement</button
						>
					</div>
				</div>
			{:else if activeTab === 'vifTestHistory'}
				{#snippet testStatusCell(t: VirtualInterfaceTestHistory)}
					<span class="text-xs px-2 py-1 rounded-full {stateBadgeClass(t.status)}"
						>{t.status ?? '—'}</span
					>
				{/snippet}
				{#snippet testPeersCell(t: VirtualInterfaceTestHistory)}
					{(t.bgpPeers ?? []).join(', ') || '—'}
				{/snippet}
				{#snippet testStartCell(t: VirtualInterfaceTestHistory)}
					{formatDate(t.startTime)}
				{/snippet}
				{#snippet testEndCell(t: VirtualInterfaceTestHistory)}
					{formatDate(t.endTime)}
				{/snippet}
				{@const testColumns = defineColumns<VirtualInterfaceTestHistory>([
					{ key: 'testId', label: 'Test ID' },
					{ key: 'virtualInterfaceId', label: 'Virtual Interface ID' },
					{ key: 'status', label: 'Status', render: testStatusCell },
					{ key: 'bgpPeers', label: 'BGP Peers', render: testPeersCell },
					{ key: 'testDurationInMinutes', label: 'Duration (min)' },
					{ key: 'startTime', label: 'Start', render: testStartCell },
					{ key: 'endTime', label: 'End', render: testEndCell }
				])}
				<DataTable
					rows={filteredVifTestHistory}
					rowKey={(t) => t.testId ?? ''}
					columns={testColumns}
					loading={tabLoader.isLoading('vifTestHistory')}
					emptyMessage="No BGP failover test history found"
				/>
				<LoadMore
					hasMore={!!vifTestHistoryNextToken}
					loading={loadingMoreVifTestHistory}
					onLoadMore={loadMoreVifTestHistory}
				/>
			{/if}
		</div>
	</div>
</div>

<!-- ============================= Connections ============================= -->

<Modal bind:this={createConnectionModal} title="Create Connection">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-conn-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-conn-name"
					bind:value={newConnName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-conn-bandwidth" class="text-sm text-slate-600 dark:text-slate-300"
					>Bandwidth</label
				>
				<input
					id="new-conn-bandwidth"
					bind:value={newConnBandwidth}
					placeholder="e.g. 1Gbps"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-conn-location" class="text-sm text-slate-600 dark:text-slate-300"
					>Location</label
				>
				<input
					id="new-conn-location"
					bind:value={newConnLocation}
					placeholder="a location code, see the Locations tab"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-conn-lag" class="text-sm text-slate-600 dark:text-slate-300"
					>LAG ID (optional)</label
				>
				<input
					id="new-conn-lag"
					bind:value={newConnLagId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-conn-provider" class="text-sm text-slate-600 dark:text-slate-300"
					>Provider Name (optional)</label
				>
				<input
					id="new-conn-provider"
					bind:value={newConnProviderName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newConnRequestMacSec} /> Request MACsec
			</label>
			{#if createConnectionError}
				<p class="text-sm text-red-600 dark:text-red-400">{createConnectionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createConnectionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateConnection}
			disabled={creatingConnection}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{creatingConnection ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editConnectionModal} title="Edit Connection">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-conn-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-conn-name"
					bind:value={editConnName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-conn-encryption" class="text-sm text-slate-600 dark:text-slate-300"
					>Encryption Mode</label
				>
				<select
					id="edit-conn-encryption"
					bind:value={editConnEncryptionMode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Unchanged</option>
					<option value="no_encrypt">no_encrypt</option>
					<option value="should_encrypt">should_encrypt</option>
					<option value="must_encrypt">must_encrypt</option>
				</select>
			</div>
			{#if editConnectionError}
				<p class="text-sm text-red-600 dark:text-red-400">{editConnectionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editConnectionModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditConnection}
			disabled={editingConnection}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{editingConnection ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={connectionDetailModal} title="Connection Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if connectionDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if connectionDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{connectionDetailError}</p>
			{:else if viewedConnection}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedConnection.connectionId}</div>
					<div>
						<span class="text-slate-500">State:</span>
						<span class="px-2 py-0.5 rounded-full {stateBadgeClass(viewedConnection.connectionState)}"
							>{viewedConnection.connectionState}</span
						>
					</div>
					<div><span class="text-slate-500">Bandwidth:</span> {viewedConnection.bandwidth}</div>
					<div><span class="text-slate-500">Location:</span> {viewedConnection.location}</div>
					<div><span class="text-slate-500">Region:</span> {viewedConnection.region}</div>
					<div><span class="text-slate-500">LAG ID:</span> {viewedConnection.lagId ?? '—'}</div>
					<div><span class="text-slate-500">VLAN:</span> {viewedConnection.vlan ?? '—'}</div>
					<div>
						<span class="text-slate-500">Owner Account:</span> {viewedConnection.ownerAccount}
					</div>
					<div>
						<span class="text-slate-500">Provider:</span> {viewedConnection.providerName ?? '—'}
					</div>
					<div>
						<span class="text-slate-500">Encryption Mode:</span>
						{viewedConnection.encryptionMode ?? '—'}
					</div>
					<div>
						<span class="text-slate-500">MACsec Capable:</span>
						{viewedConnection.macSecCapable ?? false}
					</div>
					<div>
						<span class="text-slate-500">Jumbo Frame Capable:</span>
						{viewedConnection.jumboFrameCapable ?? false}
					</div>
					{#if viewedConnection.rateLimiterStatus}
						<div class="col-span-2">
							<span class="text-slate-500">Rate Limiter:</span>
							{viewedConnection.rateLimiterStatus.inUse}/{viewedConnection.rateLimiterStatus
								.maxAllowed} used, {viewedConnection.rateLimiterStatus.remaining} remaining ({viewedConnection
								.rateLimiterStatus.totalBandwidth})
						</div>
					{/if}
				</div>

				{#if viewedConnection.connectionState === 'ordering'}
					<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
						<button
							onclick={handleConfirmConnection}
							class="flex items-center gap-1 text-xs px-2 py-1 rounded bg-emerald-600 text-white hover:bg-emerald-700"
							><Check class="w-3 h-3" /> Confirm Connection</button
						>
					</div>
				{/if}

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Letter of Authorization</h3>
					<button
						onclick={handleViewConnectionLoa}
						class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Fetch LOA</button
					>
					{#if connLoaResult}
						<p class="text-xs text-slate-500">
							{connLoaResult.contentType}, {connLoaResult.byteLength} bytes --
							<a href={connLoaResult.dataUrl} download="loa.pdf" class="text-sky-600 hover:underline"
								>download</a
							>
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">MACsec Keys</h3>
					{#each viewedConnection.macSecKeys ?? [] as key (key.secretARN)}
						<div class="flex items-center justify-between text-xs text-slate-500">
							<span>{key.secretARN} -- {key.state}</span>
							<button
								onclick={() => handleDisassociateMacSecKey(key.secretARN)}
								class="text-red-600 hover:underline">Disassociate</button
							>
						</div>
					{:else}
						<p class="text-xs text-slate-500">No MACsec keys associated.</p>
					{/each}
					<div class="flex items-center gap-2 flex-wrap">
						<label class="sr-only" for="macsec-mode">MACsec input mode</label>
						<select
							id="macsec-mode"
							bind:value={connMacSecMode}
							class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="raw">CAK / CKN</option>
							<option value="secretArn">Secrets Manager ARN</option>
						</select>
						{#if connMacSecMode === 'raw'}
							<label class="sr-only" for="macsec-cak">CAK</label>
							<input
								id="macsec-cak"
								bind:value={connMacSecCak}
								placeholder="CAK"
								class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
							<label class="sr-only" for="macsec-ckn">CKN</label>
							<input
								id="macsec-ckn"
								bind:value={connMacSecCkn}
								placeholder="CKN"
								class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
						{:else}
							<label class="sr-only" for="macsec-secretarn">Secret ARN</label>
							<input
								id="macsec-secretarn"
								bind:value={connMacSecSecretArn}
								placeholder="Secret ARN"
								class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
							/>
						{/if}
						<button
							onclick={submitAssociateMacSecKey}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Associate</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each connTags as t (t.key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{t.key}={t.value ?? ''}
								<button
									onclick={() => removeConnTag(t.key)}
									aria-label="Remove tag {t.key}"
									class="text-slate-400 hover:text-red-500"><Trash2 class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="conn-tag-key">Tag key</label>
						<input
							id="conn-tag-key"
							bind:value={connAddTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="conn-tag-value">Tag value</label>
						<input
							id="conn-tag-value"
							bind:value={connAddTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddConnTag}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => connectionDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- =================================== LAGs ================================ -->

<Modal bind:this={createLagModal} title="Create LAG">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-lag-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-lag-name"
					bind:value={newLagName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-lag-bandwidth" class="text-sm text-slate-600 dark:text-slate-300"
					>Connections Bandwidth</label
				>
				<input
					id="new-lag-bandwidth"
					bind:value={newLagBandwidth}
					placeholder="e.g. 1Gbps"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-lag-location" class="text-sm text-slate-600 dark:text-slate-300"
					>Location</label
				>
				<input
					id="new-lag-location"
					bind:value={newLagLocation}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-lag-numconn" class="text-sm text-slate-600 dark:text-slate-300"
					>Number of Connections</label
				>
				<input
					id="new-lag-numconn"
					type="number"
					bind:value={newLagNumConnections}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-lag-connid" class="text-sm text-slate-600 dark:text-slate-300"
					>Existing Connection ID (optional)</label
				>
				<input
					id="new-lag-connid"
					bind:value={newLagConnectionId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-lag-provider" class="text-sm text-slate-600 dark:text-slate-300"
					>Provider Name (optional)</label
				>
				<input
					id="new-lag-provider"
					bind:value={newLagProviderName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newLagRequestMacSec} /> Request MACsec
			</label>
			{#if createLagError}
				<p class="text-sm text-red-600 dark:text-red-400">{createLagError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createLagModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateLag}
			disabled={creatingLag}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{creatingLag ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editLagModal} title="Edit LAG">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-lag-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-lag-name"
					bind:value={editLagName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-lag-minlinks" class="text-sm text-slate-600 dark:text-slate-300"
					>Minimum Links</label
				>
				<input
					id="edit-lag-minlinks"
					type="number"
					bind:value={editLagMinLinks}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-lag-encryption" class="text-sm text-slate-600 dark:text-slate-300"
					>Encryption Mode</label
				>
				<select
					id="edit-lag-encryption"
					bind:value={editLagEncryptionMode}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Unchanged</option>
					<option value="no_encrypt">no_encrypt</option>
					<option value="should_encrypt">should_encrypt</option>
					<option value="must_encrypt">must_encrypt</option>
				</select>
			</div>
			{#if editLagError}
				<p class="text-sm text-red-600 dark:text-red-400">{editLagError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editLagModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditLag}
			disabled={editingLag}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{editingLag ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={lagDetailModal} title="LAG Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if lagDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if lagDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{lagDetailError}</p>
			{:else if viewedLag}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedLag.lagId}</div>
					<div>
						<span class="text-slate-500">State:</span>
						<span class="px-2 py-0.5 rounded-full {stateBadgeClass(viewedLag.lagState)}"
							>{viewedLag.lagState}</span
						>
					</div>
					<div><span class="text-slate-500">Bandwidth:</span> {viewedLag.connectionsBandwidth}</div>
					<div><span class="text-slate-500">Location:</span> {viewedLag.location}</div>
					<div><span class="text-slate-500">Min Links:</span> {viewedLag.minimumLinks}</div>
					<div>
						<span class="text-slate-500">Allows Hosted Connections:</span>
						{viewedLag.allowsHostedConnections ?? false}
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Member Connections</h3>
					{#each viewedLag.connections ?? [] as conn (conn.connectionId)}
						<div class="flex items-center justify-between text-xs text-slate-500">
							<span
								>{conn.connectionId} -- {conn.connectionName}
								<span class="px-2 py-0.5 rounded-full {stateBadgeClass(conn.connectionState)}"
									>{conn.connectionState}</span
								></span
							>
							<button
								onclick={() => handleDisassociateConnectionFromLag(conn.connectionId)}
								class="text-red-600 hover:underline">Remove from LAG</button
							>
						</div>
					{:else}
						<p class="text-xs text-slate-500">No member connections.</p>
					{/each}
					<div class="flex items-center gap-2">
						<label class="sr-only" for="lag-associate-conn">Connection ID to associate</label>
						<input
							id="lag-associate-conn"
							bind:value={lagAssociateConnId}
							placeholder="Connection ID"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAssociateConnectionWithLag}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700"
							>Associate</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each lagTags as t (t.key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{t.key}={t.value ?? ''}
								<button
									onclick={() => removeLagTag(t.key)}
									aria-label="Remove tag {t.key}"
									class="text-slate-400 hover:text-red-500"><Trash2 class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="lag-tag-key">Tag key</label>
						<input
							id="lag-tag-key"
							bind:value={lagAddTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="lag-tag-value">Tag value</label>
						<input
							id="lag-tag-value"
							bind:value={lagAddTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddLagTag}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => lagDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ============================= Interconnects ============================= -->

<Modal bind:this={createInterconnectModal} title="Create Interconnect">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-ic-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-ic-name"
					bind:value={newIcName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-ic-bandwidth" class="text-sm text-slate-600 dark:text-slate-300"
					>Bandwidth</label
				>
				<input
					id="new-ic-bandwidth"
					bind:value={newIcBandwidth}
					placeholder="e.g. 1Gbps"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-ic-location" class="text-sm text-slate-600 dark:text-slate-300"
					>Location</label
				>
				<input
					id="new-ic-location"
					bind:value={newIcLocation}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-ic-lag" class="text-sm text-slate-600 dark:text-slate-300"
					>LAG ID (optional)</label
				>
				<input
					id="new-ic-lag"
					bind:value={newIcLagId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-ic-provider" class="text-sm text-slate-600 dark:text-slate-300"
					>Provider Name (optional)</label
				>
				<input
					id="new-ic-provider"
					bind:value={newIcProviderName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newIcRequestMacSec} /> Request MACsec
			</label>
			{#if createInterconnectError}
				<p class="text-sm text-red-600 dark:text-red-400">{createInterconnectError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createInterconnectModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateInterconnect}
			disabled={creatingInterconnect}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{creatingInterconnect ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={interconnectDetailModal} title="Interconnect Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if interconnectDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if interconnectDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{interconnectDetailError}</p>
			{:else if viewedInterconnect}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedInterconnect.interconnectId}</div>
					<div>
						<span class="text-slate-500">State:</span>
						<span class="px-2 py-0.5 rounded-full {stateBadgeClass(viewedInterconnect.interconnectState)}"
							>{viewedInterconnect.interconnectState}</span
						>
					</div>
					<div>
						<span class="text-slate-500">Bandwidth:</span> {viewedInterconnect.bandwidth}
					</div>
					<div><span class="text-slate-500">Location:</span> {viewedInterconnect.location}</div>
					<div>
						<span class="text-slate-500">Provider:</span> {viewedInterconnect.providerName ?? '—'}
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Letter of Authorization</h3>
					<button
						onclick={handleViewInterconnectLoa}
						class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Fetch LOA</button
					>
					{#if icLoaResult}
						<p class="text-xs text-slate-500">
							{icLoaResult.contentType}, {icLoaResult.byteLength} bytes --
							<a href={icLoaResult.dataUrl} download="loa.pdf" class="text-sky-600 hover:underline"
								>download</a
							>
						</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each icTags as t (t.key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{t.key}={t.value ?? ''}
								<button
									onclick={() => removeIcTag(t.key)}
									aria-label="Remove tag {t.key}"
									class="text-slate-400 hover:text-red-500"><Trash2 class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="ic-tag-key">Tag key</label>
						<input
							id="ic-tag-key"
							bind:value={icAddTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="ic-tag-value">Tag value</label>
						<input
							id="ic-tag-value"
							bind:value={icAddTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddIcTag}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => interconnectDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ========================= Virtual Interfaces ============================ -->

<Modal bind:this={createVifModal} title="Create Virtual Interface">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-vif-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select
					id="new-vif-type"
					bind:value={newVifType}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="private">Private</option>
					<option value="public">Public</option>
					<option value="transit">Transit</option>
				</select>
			</div>
			<div>
				<label for="new-vif-connid" class="text-sm text-slate-600 dark:text-slate-300"
					>Connection ID</label
				>
				<input
					id="new-vif-connid"
					bind:value={newVifConnectionId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-vif-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-vif-name"
					bind:value={newVifName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-vif-vlan" class="text-sm text-slate-600 dark:text-slate-300">VLAN</label>
					<input
						id="new-vif-vlan"
						type="number"
						bind:value={newVifVlan}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="new-vif-asn" class="text-sm text-slate-600 dark:text-slate-300"
						>ASN (optional)</label
					>
					<input
						id="new-vif-asn"
						type="number"
						bind:value={newVifAsn}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			<div>
				<label for="new-vif-addrfam" class="text-sm text-slate-600 dark:text-slate-300"
					>Address Family (optional)</label
				>
				<select
					id="new-vif-addrfam"
					bind:value={newVifAddressFamily}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Unspecified</option>
					<option value="ipv4">ipv4</option>
					<option value="ipv6">ipv6</option>
				</select>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-vif-amzaddr" class="text-sm text-slate-600 dark:text-slate-300"
						>Amazon Address (optional)</label
					>
					<input
						id="new-vif-amzaddr"
						bind:value={newVifAmazonAddress}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
				<div>
					<label for="new-vif-custaddr" class="text-sm text-slate-600 dark:text-slate-300"
						>Customer Address (optional)</label
					>
					<input
						id="new-vif-custaddr"
						bind:value={newVifCustomerAddress}
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			</div>
			<div>
				<label for="new-vif-authkey" class="text-sm text-slate-600 dark:text-slate-300"
					>BGP Auth Key (optional)</label
				>
				<input
					id="new-vif-authkey"
					bind:value={newVifAuthKey}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if newVifType === 'public'}
				<div>
					<label for="new-vif-prefixes" class="text-sm text-slate-600 dark:text-slate-300"
						>Route Filter Prefixes (comma or newline separated CIDRs)</label
					>
					<textarea
						id="new-vif-prefixes"
						bind:value={newVifRouteFilterPrefixes}
						rows="2"
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					></textarea>
				</div>
			{:else}
				<div class="grid grid-cols-2 gap-3">
					<div>
						<label for="new-vif-mtu" class="text-sm text-slate-600 dark:text-slate-300">MTU</label>
						<input
							id="new-vif-mtu"
							type="number"
							bind:value={newVifMtu}
							class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
					<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300 self-end pb-2">
						<input type="checkbox" bind:checked={newVifEnableSiteLink} /> Enable SiteLink
					</label>
				</div>
				{#if newVifType === 'private'}
					<div class="flex items-center gap-2">
						<select
							bind:value={newVifGatewayKind}
							aria-label="Gateway kind"
							class="px-2 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="dxgw">Direct Connect Gateway ID</option>
							<option value="vgw">Virtual Private Gateway ID</option>
						</select>
						<input
							bind:value={newVifGatewayId}
							aria-label="Gateway ID"
							placeholder="optional"
							class="flex-1 px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				{:else}
					<div>
						<label for="new-vif-dcgw" class="text-sm text-slate-600 dark:text-slate-300"
							>Direct Connect Gateway ID (optional)</label
						>
						<input
							id="new-vif-dcgw"
							bind:value={newVifGatewayId}
							class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
					</div>
				{/if}
			{/if}
			{#if createVifError}
				<p class="text-sm text-red-600 dark:text-red-400">{createVifError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createVifModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateVif}
			disabled={creatingVif}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{creatingVif ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editVifModal} title="Edit Virtual Interface Attributes">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-vif-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="edit-vif-name"
					bind:value={editVifName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vif-mtu" class="text-sm text-slate-600 dark:text-slate-300"
					>MTU (1500 or 8500)</label
				>
				<input
					id="edit-vif-mtu"
					type="number"
					bind:value={editVifMtu}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vif-ratelimit" class="text-sm text-slate-600 dark:text-slate-300"
					>Rate Limit (optional)</label
				>
				<input
					id="edit-vif-ratelimit"
					bind:value={editVifRateLimit}
					placeholder="e.g. 1Gbps"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="edit-vif-sitelink" class="text-sm text-slate-600 dark:text-slate-300"
					>SiteLink</label
				>
				<select
					id="edit-vif-sitelink"
					bind:value={editVifEnableSiteLink}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Unchanged</option>
					<option value="true">Enabled</option>
					<option value="false">Disabled</option>
				</select>
			</div>
			{#if editVifError}
				<p class="text-sm text-red-600 dark:text-red-400">{editVifError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editVifModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditVif}
			disabled={editingVif}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{editingVif ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={vifDetailModal} title="Virtual Interface Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if vifDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if vifDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{vifDetailError}</p>
			{:else if viewedVif}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedVif.virtualInterfaceId}</div>
					<div><span class="text-slate-500">Type:</span> {viewedVif.virtualInterfaceType}</div>
					<div>
						<span class="text-slate-500">State:</span>
						<span class="px-2 py-0.5 rounded-full {stateBadgeClass(viewedVif.virtualInterfaceState)}"
							>{viewedVif.virtualInterfaceState}</span
						>
					</div>
					<div><span class="text-slate-500">Connection:</span> {viewedVif.connectionId}</div>
					<div><span class="text-slate-500">VLAN:</span> {viewedVif.vlan}</div>
					<div>
						<span class="text-slate-500">ASN:</span> {viewedVif.asn} (long: {viewedVif.asnLong ?? '—'})
					</div>
					<div><span class="text-slate-500">Amazon Side ASN:</span> {viewedVif.amazonSideAsn ?? '—'}</div>
					<div><span class="text-slate-500">Address Family:</span> {viewedVif.addressFamily ?? '—'}</div>
					<div><span class="text-slate-500">Amazon Address:</span> {viewedVif.amazonAddress ?? '—'}</div>
					<div><span class="text-slate-500">Customer Address:</span> {viewedVif.customerAddress ?? '—'}</div>
					<div><span class="text-slate-500">MTU:</span> {viewedVif.mtu ?? '—'}</div>
					<div><span class="text-slate-500">Rate Limit:</span> {viewedVif.rateLimit ?? '—'}</div>
					<div><span class="text-slate-500">SiteLink Enabled:</span> {viewedVif.siteLinkEnabled ?? false}</div>
					<div>
						<span class="text-slate-500">Gateway:</span>
						{viewedVif.directConnectGatewayId ?? viewedVif.virtualGatewayId ?? '—'}
					</div>
					{#if viewedVif.virtualInterfaceType === 'public'}
						<div class="col-span-2">
							<span class="text-slate-500">Route Filter Prefixes:</span>
							{formatCidrList(viewedVif.routeFilterPrefixes) || '—'}
						</div>
					{/if}
				</div>

				{#if viewedVif.virtualInterfaceState === 'confirming'}
					<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
						<h3 class="text-sm font-semibold">Confirm Virtual Interface</h3>
						{#if viewedVif.virtualInterfaceType !== 'public'}
							<label class="sr-only" for="confirm-gateway-id">Gateway ID</label>
							<input
								id="confirm-gateway-id"
								bind:value={confirmGatewayId}
								placeholder={viewedVif.virtualInterfaceType === 'transit'
									? 'Direct Connect Gateway ID (required)'
									: 'Direct Connect / Virtual Gateway ID (optional)'}
								class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 w-full"
							/>
						{/if}
						<button
							onclick={handleConfirmVif}
							class="flex items-center gap-1 text-xs px-2 py-1 rounded bg-emerald-600 text-white hover:bg-emerald-700"
							><Check class="w-3 h-3" /> Confirm</button
						>
					</div>
				{/if}

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Reassign Connection</h3>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="associate-vif-conn">New connection or LAG ID</label>
						<input
							id="associate-vif-conn"
							bind:value={associateVifConnId}
							placeholder="Connection or LAG ID"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAssociateVif}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Associate</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">BGP Peers</h3>
					{#each viewedVif.bgpPeers ?? [] as peer (peer.bgpPeerId)}
						<div class="flex items-center justify-between text-xs text-slate-500">
							<span
								>{peer.bgpPeerId} -- ASN {peer.asn}
								<span class="px-2 py-0.5 rounded-full {stateBadgeClass(peer.bgpPeerState)}"
									>{peer.bgpPeerState}</span
								>
								<span class="px-2 py-0.5 rounded-full {stateBadgeClass(peer.bgpStatus)}"
									>{peer.bgpStatus}</span
								></span
							>
							<button onclick={() => handleDeleteBgpPeer(peer)} class="text-red-600 hover:underline"
								>Delete</button
							>
						</div>
					{:else}
						<p class="text-xs text-slate-500">No BGP peers.</p>
					{/each}
					<div class="grid grid-cols-2 gap-2">
						<label class="sr-only" for="bgp-new-asn">New peer ASN</label>
						<input
							id="bgp-new-asn"
							type="number"
							bind:value={bgpNewAsn}
							placeholder="ASN"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="bgp-new-addrfam">New peer address family</label>
						<select
							id="bgp-new-addrfam"
							bind:value={bgpNewAddressFamily}
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						>
							<option value="">Address family</option>
							<option value="ipv4">ipv4</option>
							<option value="ipv6">ipv6</option>
						</select>
						<label class="sr-only" for="bgp-new-amzaddr">New peer amazon address</label>
						<input
							id="bgp-new-amzaddr"
							bind:value={bgpNewAmazonAddress}
							placeholder="Amazon address"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="bgp-new-custaddr">New peer customer address</label>
						<input
							id="bgp-new-custaddr"
							bind:value={bgpNewCustomerAddress}
							placeholder="Customer address"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="bgp-new-authkey">New peer auth key</label>
						<input
							id="bgp-new-authkey"
							bind:value={bgpNewAuthKey}
							placeholder="Auth key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
					</div>
					<button
						onclick={submitCreateBgpPeer}
						class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Add BGP Peer</button
					>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">BGP Failover Test</h3>
					<div class="flex items-center gap-2 flex-wrap">
						<label class="sr-only" for="failover-peers">BGP peer IDs to test</label>
						<input
							id="failover-peers"
							bind:value={failoverTestPeers}
							placeholder="BGP peer IDs, comma separated (blank = all)"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="failover-duration">Test duration in minutes</label>
						<input
							id="failover-duration"
							type="number"
							bind:value={failoverTestDuration}
							placeholder="Duration (min)"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={handleStartFailoverTest}
							class="flex items-center gap-1 text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700"
							><Play class="w-3 h-3" /> Start</button
						>
						<button
							onclick={handleStopFailoverTest}
							class="flex items-center gap-1 text-xs px-2 py-1 rounded bg-gray-500 text-white hover:bg-gray-600"
							><Square class="w-3 h-3" /> Stop</button
						>
					</div>
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold">Router Configuration</h3>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="router-type-id">Router type identifier</label>
						<input
							id="router-type-id"
							bind:value={routerTypeIdentifier}
							placeholder="Router type identifier (optional)"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={handleFetchRouterConfig}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Fetch</button
						>
					</div>
					{#if routerConfigResult}
						<p class="text-xs text-slate-500">
							{routerConfigResult.vendor} {routerConfigResult.platform} {routerConfigResult.software}
						</p>
						{#if routerConfigResult.customerRouterConfig}
							<pre
								class="text-xs bg-slate-100 dark:bg-slate-900 p-2 rounded overflow-x-auto whitespace-pre-wrap">{routerConfigResult.customerRouterConfig}</pre>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each vifTags as t (t.key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{t.key}={t.value ?? ''}
								<button
									onclick={() => removeVifTag(t.key)}
									aria-label="Remove tag {t.key}"
									class="text-slate-400 hover:text-red-500"><Trash2 class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="vif-tag-key">Tag key</label>
						<input
							id="vif-tag-key"
							bind:value={vifAddTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="vif-tag-value">Tag value</label>
						<input
							id="vif-tag-value"
							bind:value={vifAddTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddVifTag}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => vifDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ================================ Gateways ================================ -->

<Modal bind:this={createGatewayModal} title="Create Direct Connect Gateway">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-gw-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="new-gw-name"
					bind:value={newGwName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-gw-asn" class="text-sm text-slate-600 dark:text-slate-300"
					>Amazon Side ASN (optional, auto-assigned if blank)</label
				>
				<input
					id="new-gw-asn"
					type="number"
					bind:value={newGwAmazonSideAsn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createGatewayError}
				<p class="text-sm text-red-600 dark:text-red-400">{createGatewayError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createGatewayModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateGateway}
			disabled={creatingGateway}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{creatingGateway ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editGatewayModal} title="Rename Direct Connect Gateway">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-gw-name" class="text-sm text-slate-600 dark:text-slate-300"
					>New Name</label
				>
				<input
					id="edit-gw-name"
					bind:value={editGatewayName}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editGatewayError}
				<p class="text-sm text-red-600 dark:text-red-400">{editGatewayError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editGatewayModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditGateway}
			disabled={editingGateway}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{editingGateway ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={gatewayDetailModal} title="Direct Connect Gateway Detail">
	{#snippet children()}
		<div class="space-y-4 max-h-[70vh] overflow-y-auto">
			{#if gatewayDetailLoading}
				<p class="text-sm text-slate-500">Loading...</p>
			{:else if gatewayDetailError}
				<p class="text-sm text-red-600 dark:text-red-400">{gatewayDetailError}</p>
			{:else if viewedGateway}
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">ID:</span> {viewedGateway.directConnectGatewayId}</div>
					<div><span class="text-slate-500">Name:</span> {viewedGateway.directConnectGatewayName}</div>
					<div>
						<span class="text-slate-500">State:</span>
						<span class="px-2 py-0.5 rounded-full {stateBadgeClass(viewedGateway.directConnectGatewayState)}"
							>{viewedGateway.directConnectGatewayState}</span
						>
					</div>
					<div><span class="text-slate-500">Amazon Side ASN:</span> {viewedGateway.amazonSideAsn ?? '—'}</div>
					<div><span class="text-slate-500">Owner Account:</span> {viewedGateway.ownerAccount}</div>
					{#if viewedGateway.stateChangeError}
						<div class="col-span-2 text-red-600">
							<span class="text-slate-500">State Change Error:</span> {viewedGateway.stateChangeError}
						</div>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3">
					<h3 class="text-sm font-semibold mb-2">Tags</h3>
					<div class="flex flex-wrap gap-2 mb-2">
						{#each gwTags as t (t.key)}
							<span
								class="inline-flex items-center gap-1 text-xs px-2 py-1 rounded-full bg-slate-100 dark:bg-slate-700"
							>
								{t.key}={t.value ?? ''}
								<button
									onclick={() => removeGwTag(t.key)}
									aria-label="Remove tag {t.key}"
									class="text-slate-400 hover:text-red-500"><Trash2 class="w-3 h-3" /></button
								>
							</span>
						{:else}
							<span class="text-xs text-slate-500">No tags</span>
						{/each}
					</div>
					<div class="flex items-center gap-2">
						<label class="sr-only" for="gw-tag-key">Tag key</label>
						<input
							id="gw-tag-key"
							bind:value={gwAddTagKey}
							placeholder="key"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="gw-tag-value">Tag value</label>
						<input
							id="gw-tag-value"
							bind:value={gwAddTagValue}
							placeholder="value"
							class="px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<button
							onclick={submitAddGwTag}
							class="text-xs px-2 py-1 rounded bg-sky-600 text-white hover:bg-sky-700">Add</button
						>
					</div>
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => gatewayDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ========================= Gateway Associations =========================== -->

<Modal bind:this={createGaModal} title="Create Gateway Association">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-ga-dcgw" class="text-sm text-slate-600 dark:text-slate-300"
					>Direct Connect Gateway ID</label
				>
				<input
					id="new-ga-dcgw"
					bind:value={newGaDcgwId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-ga-gwid" class="text-sm text-slate-600 dark:text-slate-300"
					>VGW / TGW ID</label
				>
				<input
					id="new-ga-gwid"
					bind:value={newGaGatewayId}
					list="vgw-datalist"
					placeholder="vgw-... or tgw-..."
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
				<datalist id="vgw-datalist">
					{#each virtualGateways as g (g.virtualGatewayId)}
						<option value={g.virtualGatewayId}>{g.virtualGatewayState}</option>
					{/each}
				</datalist>
			</div>
			<div>
				<label for="new-ga-prefixes" class="text-sm text-slate-600 dark:text-slate-300"
					>Allowed Prefixes (optional, comma or newline separated CIDRs)</label
				>
				<textarea
					id="new-ga-prefixes"
					bind:value={newGaAddPrefixes}
					rows="2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createGaError}
				<p class="text-sm text-red-600 dark:text-red-400">{createGaError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createGaModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateGa}
			disabled={creatingGa}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{creatingGa ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editGaModal} title="Update Gateway Association">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="edit-ga-add" class="text-sm text-slate-600 dark:text-slate-300"
					>Add Allowed Prefixes (comma or newline separated CIDRs)</label
				>
				<textarea
					id="edit-ga-add"
					bind:value={editGaAddPrefixes}
					rows="2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			<div>
				<label for="edit-ga-remove" class="text-sm text-slate-600 dark:text-slate-300"
					>Remove Allowed Prefixes (comma or newline separated CIDRs)</label
				>
				<textarea
					id="edit-ga-remove"
					bind:value={editGaRemovePrefixes}
					rows="2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if editGaError}
				<p class="text-sm text-red-600 dark:text-red-400">{editGaError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editGaModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditGa}
			disabled={editingGa}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{editingGa ? 'Saving...' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={gaDetailModal} title="Gateway Association Detail">
	{#snippet children()}
		{#if viewedGa}
			<div class="grid grid-cols-2 gap-3 text-sm">
				<div><span class="text-slate-500">Association ID:</span> {viewedGa.associationId}</div>
				<div>
					<span class="text-slate-500">State:</span>
					<span class="px-2 py-0.5 rounded-full {stateBadgeClass(viewedGa.associationState)}"
						>{viewedGa.associationState}</span
					>
				</div>
				<div>
					<span class="text-slate-500">Gateway ID:</span> {viewedGa.directConnectGatewayId}
				</div>
				<div>
					<span class="text-slate-500">Gateway Owner Account:</span>
					{viewedGa.directConnectGatewayOwnerAccount}
				</div>
				<div>
					<span class="text-slate-500">Associated Gateway:</span>
					{viewedGa.associatedGateway?.id ?? viewedGa.virtualGatewayId ?? '—'} ({viewedGa
						.associatedGateway?.type ?? 'virtualPrivateGateway'})
				</div>
				<div>
					<span class="text-slate-500">Associated Gateway Owner:</span>
					{viewedGa.associatedGateway?.ownerAccount ?? viewedGa.virtualGatewayOwnerAccount ?? '—'}
				</div>
				<div class="col-span-2">
					<span class="text-slate-500">Allowed Prefixes:</span>
					{formatCidrList(viewedGa.allowedPrefixesToDirectConnectGateway) || '—'}
				</div>
				{#if viewedGa.stateChangeError}
					<div class="col-span-2 text-red-600">
						<span class="text-slate-500">State Change Error:</span> {viewedGa.stateChangeError}
					</div>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => gaDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ======================= Gateway Association Proposals ===================== -->

<Modal bind:this={createProposalModal} title="Create Association Proposal">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="new-prop-dcgw" class="text-sm text-slate-600 dark:text-slate-300"
					>Direct Connect Gateway ID</label
				>
				<input
					id="new-prop-dcgw"
					bind:value={newPropDcgwId}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-prop-owner" class="text-sm text-slate-600 dark:text-slate-300"
					>Direct Connect Gateway Owner Account</label
				>
				<input
					id="new-prop-owner"
					bind:value={newPropOwnerAccount}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-prop-gwid" class="text-sm text-slate-600 dark:text-slate-300"
					>VGW / TGW ID</label
				>
				<input
					id="new-prop-gwid"
					bind:value={newPropGatewayId}
					list="vgw-datalist"
					placeholder="vgw-... or tgw-..."
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="new-prop-prefixes" class="text-sm text-slate-600 dark:text-slate-300"
					>Allowed Prefixes (optional, comma or newline separated CIDRs)</label
				>
				<textarea
					id="new-prop-prefixes"
					bind:value={newPropAddPrefixes}
					rows="2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				></textarea>
			</div>
			{#if createProposalError}
				<p class="text-sm text-red-600 dark:text-red-400">{createProposalError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createProposalModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateProposal}
			disabled={creatingProposal}
			class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-medium text-white hover:bg-sky-700 disabled:opacity-50"
			>{creatingProposal ? 'Creating...' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={proposalDetailModal} title="Association Proposal Detail">
	{#snippet children()}
		{#if viewedProposal}
			<div class="space-y-4">
				<div class="grid grid-cols-2 gap-3 text-sm">
					<div><span class="text-slate-500">Proposal ID:</span> {viewedProposal.proposalId}</div>
					<div>
						<span class="text-slate-500">State:</span>
						<span class="px-2 py-0.5 rounded-full {stateBadgeClass(viewedProposal.proposalState)}"
							>{viewedProposal.proposalState}</span
						>
					</div>
					<div>
						<span class="text-slate-500">Gateway ID:</span> {viewedProposal.directConnectGatewayId}
					</div>
					<div>
						<span class="text-slate-500">Gateway Owner Account:</span>
						{viewedProposal.directConnectGatewayOwnerAccount}
					</div>
					<div>
						<span class="text-slate-500">Associated Gateway:</span>
						{viewedProposal.associatedGateway?.id ?? '—'} ({viewedProposal.associatedGateway?.type ??
							'—'})
					</div>
					<div class="col-span-2">
						<span class="text-slate-500">Existing Allowed Prefixes:</span>
						{formatCidrList(viewedProposal.existingAllowedPrefixesToDirectConnectGateway) || '—'}
					</div>
					<div class="col-span-2">
						<span class="text-slate-500">Requested Allowed Prefixes:</span>
						{formatCidrList(viewedProposal.requestedAllowedPrefixesToDirectConnectGateway) || '—'}
					</div>
				</div>

				{#if viewedProposal.proposalState === 'requested'}
					<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
						<h3 class="text-sm font-semibold">Accept Proposal</h3>
						<p class="text-xs text-slate-500">
							Only the account that owns the associated gateway (the VGW/TGW owner) accepts a
							proposal -- confirm that account ID here.
						</p>
						<label class="sr-only" for="accept-owner-account">Associated gateway owner account</label>
						<input
							id="accept-owner-account"
							bind:value={acceptOwnerAccount}
							placeholder="Associated gateway owner account"
							class="w-full px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						/>
						<label class="sr-only" for="accept-override-prefixes"
							>Override allowed prefixes</label
						>
						<textarea
							id="accept-override-prefixes"
							bind:value={acceptOverridePrefixes}
							rows="2"
							placeholder="Override allowed prefixes (optional, comma or newline separated CIDRs)"
							class="w-full px-2 py-1 text-xs rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700"
						></textarea>
						<button
							onclick={handleAcceptProposal}
							class="flex items-center gap-1 text-xs px-2 py-1 rounded bg-emerald-600 text-white hover:bg-emerald-700"
							><Check class="w-3 h-3" /> Accept Proposal</button
						>
					</div>
				{/if}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => proposalDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

