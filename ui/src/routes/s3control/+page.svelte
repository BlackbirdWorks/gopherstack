<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getS3ControlClient } from '$lib/aws-client';
	import {
		ListAccessPointsCommand,
		CreateAccessPointCommand,
		DeleteAccessPointCommand,
		GetAccessPointCommand,
		ListMultiRegionAccessPointsCommand,
		CreateMultiRegionAccessPointCommand,
		DeleteMultiRegionAccessPointCommand,
		GetMultiRegionAccessPointCommand,
		ListStorageLensConfigurationsCommand,
		PutStorageLensConfigurationCommand,
		DeleteStorageLensConfigurationCommand,
		GetStorageLensConfigurationCommand,
		ListStorageLensGroupsCommand,
		CreateStorageLensGroupCommand,
		UpdateStorageLensGroupCommand,
		DeleteStorageLensGroupCommand,
		GetStorageLensGroupCommand,
		ListAccessGrantsLocationsCommand,
		CreateAccessGrantsLocationCommand,
		UpdateAccessGrantsLocationCommand,
		DeleteAccessGrantsLocationCommand,
		GetAccessGrantsLocationCommand,
		ListRegionalBucketsCommand,
		CreateBucketCommand,
		DeleteBucketCommand,
		GetBucketCommand,
		type AccessPoint,
		type GetAccessPointResult,
		type MultiRegionAccessPointReport,
		type ListStorageLensConfigurationEntry,
		type StorageLensConfiguration,
		type ListStorageLensGroupEntry,
		type StorageLensGroup,
		type ListAccessGrantsLocationsEntry,
		type GetAccessGrantsLocationResult,
		type RegionalBucket,
		type GetBucketResult
	} from '@aws-sdk/client-s3-control';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import type { Column } from '$lib/components/data-table';
	import LoadMore from '$lib/components/LoadMore.svelte';
	import Modal from '$lib/components/Modal.svelte';
	import { Database, Plus, Trash2, Eye, Pencil } from 'lucide-svelte';

	const client = regionalClient(getS3ControlClient);

	type TabId =
		| 'accesspoints'
		| 'mrap'
		| 'storagelens'
		| 'storagelensgroups'
		| 'grantslocations'
		| 'buckets';

	const tabs: TabDef[] = [
		{ id: 'accesspoints', label: 'Access Points' },
		{ id: 'mrap', label: 'Multi-Region APs' },
		{ id: 'storagelens', label: 'Storage Lens' },
		{ id: 'storagelensgroups', label: 'Storage Lens Groups' },
		{ id: 'grantslocations', label: 'Access Grants Locations' },
		{ id: 'buckets', label: 'Outposts Buckets' }
	];

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

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	let activeTab = $state<TabId>('accesspoints');
	let searchQuery = $state('');

	// Every S3 Control call carries an AccountId (as the x-amz-account-id
	// header -- see services/s3control/handler.go's accountIDFromRequest).
	// Rather than hardcode a placeholder, this is resolved once from the
	// server's real effective account id (GET /dashboard/api/system/settings,
	// the same endpoint the Settings page reads) so list/create/update calls
	// address the account this gopherstack instance actually runs as. See
	// quicksight/+page.svelte's identical ensureAccountId for the reference
	// implementation this mirrors. '000000000000' is kept only as the
	// fallback for an older server build that doesn't expose the endpoint
	// yet, or when it's unreachable.
	let accountId = $state('000000000000');
	let accountIdLoaded = false;

	async function ensureAccountId(): Promise<void> {
		if (accountIdLoaded) return;
		accountIdLoaded = true;
		try {
			const res = await fetch('/dashboard/api/system/settings');
			if (!res.ok) return;
			const data = (await res.json()) as { accountID?: unknown };
			if (typeof data.accountID === 'string' && data.accountID) {
				accountId = data.accountID;
			}
		} catch {
			// Endpoint missing (older server build) or unreachable -- keep the
			// '000000000000' placeholder default.
		}
	}

	let accessPoints = $state<AccessPoint[]>([]);
	let accessPointsNextToken = $state<string | undefined>();
	let loadingMoreAccessPoints = $state(false);

	// Multi-Region Access Points are deliberately NOT paginated in this UI:
	// aws-sdk-go-v2/service/s3control's ListMultiRegionAccessPointsRequest
	// documents both NextToken and MaxResults as "Not currently used. Do not
	// use this parameter." (verified in the installed
	// @aws-sdk/client-s3-control@3.1094.0 type doc comments) -- unlike every
	// other list op on this page, real AWS itself does not paginate this one.
	let mraps = $state<MultiRegionAccessPointReport[]>([]);

	let storageLensConfigs = $state<ListStorageLensConfigurationEntry[]>([]);
	let storageLensConfigsNextToken = $state<string | undefined>();
	let loadingMoreStorageLensConfigs = $state(false);

	let storageLensGroups = $state<ListStorageLensGroupEntry[]>([]);
	let storageLensGroupsNextToken = $state<string | undefined>();
	let loadingMoreStorageLensGroups = $state(false);

	let grantsLocations = $state<ListAccessGrantsLocationsEntry[]>([]);
	let grantsLocationsNextToken = $state<string | undefined>();
	let loadingMoreGrantsLocations = $state(false);

	let buckets = $state<RegionalBucket[]>([]);
	let bucketsNextToken = $state<string | undefined>();
	let loadingMoreBuckets = $state(false);

	async function fetchAccessPoints(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAccessPointsCommand({
				AccountId: accountId,
				NextToken: reset ? undefined : accessPointsNextToken
			})
		);
		accessPoints = reset
			? (resp.AccessPointList ?? [])
			: [...accessPoints, ...(resp.AccessPointList ?? [])];
		accessPointsNextToken = resp.NextToken;
	}

	async function fetchMrap(): Promise<void> {
		const resp = await client().send(new ListMultiRegionAccessPointsCommand({ AccountId: accountId }));
		mraps = resp.AccessPoints ?? [];
	}

	async function fetchStorageLensConfigs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListStorageLensConfigurationsCommand({
				AccountId: accountId,
				NextToken: reset ? undefined : storageLensConfigsNextToken
			})
		);
		storageLensConfigs = reset
			? (resp.StorageLensConfigurationList ?? [])
			: [...storageLensConfigs, ...(resp.StorageLensConfigurationList ?? [])];
		storageLensConfigsNextToken = resp.NextToken;
	}

	async function fetchStorageLensGroups(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListStorageLensGroupsCommand({
				AccountId: accountId,
				NextToken: reset ? undefined : storageLensGroupsNextToken
			})
		);
		storageLensGroups = reset
			? (resp.StorageLensGroupList ?? [])
			: [...storageLensGroups, ...(resp.StorageLensGroupList ?? [])];
		storageLensGroupsNextToken = resp.NextToken;
	}

	async function fetchGrantsLocations(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListAccessGrantsLocationsCommand({
				AccountId: accountId,
				NextToken: reset ? undefined : grantsLocationsNextToken
			})
		);
		grantsLocations = reset
			? (resp.AccessGrantsLocationsList ?? [])
			: [...grantsLocations, ...(resp.AccessGrantsLocationsList ?? [])];
		grantsLocationsNextToken = resp.NextToken;
	}

	async function fetchBuckets(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListRegionalBucketsCommand({
				AccountId: accountId,
				NextToken: reset ? undefined : bucketsNextToken
			})
		);
		buckets = reset ? (resp.RegionalBucketList ?? []) : [...buckets, ...(resp.RegionalBucketList ?? [])];
		bucketsNextToken = resp.NextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		accesspoints: () => fetchAccessPoints(true).catch(rethrowDescribed),
		mrap: () => fetchMrap().catch(rethrowDescribed),
		storagelens: () => fetchStorageLensConfigs(true).catch(rethrowDescribed),
		storagelensgroups: () => fetchStorageLensGroups(true).catch(rethrowDescribed),
		grantslocations: () => fetchGrantsLocations(true).catch(rethrowDescribed),
		buckets: () => fetchBuckets(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	// Every one of the six tabs holds data scoped by AccountId + region.
	// `onRegionChange`'s effect always fires once immediately on mount, in
	// addition to on every later region switch (see region-effect.svelte.ts).
	// This is used for two different jobs depending on which of those it is
	// -- see quicksight/+page.svelte's identical comment for the full
	// reasoning this mirrors.
	let regionChangeCount = 0;
	onRegionChange(() => {
		const isInitialMount = regionChangeCount === 0;
		regionChangeCount++;
		void ensureAccountId().then(() => {
			if (isInitialMount) {
				void tabLoader.refresh(activeTab);
				return;
			}
			for (const t of tabs) {
				void tabLoader.refresh(t.id as TabId);
			}
		});
	});

	const filteredAccessPoints = $derived(
		accessPoints.filter((a) => {
			const q = searchQuery.toLowerCase();
			return (
				(a.Name ?? '').toLowerCase().includes(q) ||
				(a.Bucket ?? '').toLowerCase().includes(q) ||
				(a.NetworkOrigin ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredMraps = $derived(
		mraps.filter((m) => {
			const q = searchQuery.toLowerCase();
			return (
				(m.Name ?? '').toLowerCase().includes(q) ||
				(m.Alias ?? '').toLowerCase().includes(q) ||
				(m.Status ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredStorageLensConfigs = $derived(
		storageLensConfigs.filter((c) => (c.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredStorageLensGroups = $derived(
		storageLensGroups.filter((g) => {
			const q = searchQuery.toLowerCase();
			return (
				(g.Name ?? '').toLowerCase().includes(q) ||
				(g.StorageLensGroupArn ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredGrantsLocations = $derived(
		grantsLocations.filter((l) => {
			const q = searchQuery.toLowerCase();
			return (
				(l.AccessGrantsLocationId ?? '').toLowerCase().includes(q) ||
				(l.LocationScope ?? '').toLowerCase().includes(q) ||
				(l.IAMRoleArn ?? '').toLowerCase().includes(q)
			);
		})
	);
	const filteredBuckets = $derived(
		buckets.filter((b) => {
			const q = searchQuery.toLowerCase();
			return (
				(b.Bucket ?? '').toLowerCase().includes(q) || (b.BucketArn ?? '').toLowerCase().includes(q)
			);
		})
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

	async function loadMoreAccessPoints(): Promise<void> {
		loadingMoreAccessPoints = true;
		try {
			await fetchAccessPoints(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAccessPoints = false;
		}
	}

	async function loadMoreStorageLensConfigs(): Promise<void> {
		loadingMoreStorageLensConfigs = true;
		try {
			await fetchStorageLensConfigs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreStorageLensConfigs = false;
		}
	}

	async function loadMoreStorageLensGroups(): Promise<void> {
		loadingMoreStorageLensGroups = true;
		try {
			await fetchStorageLensGroups(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreStorageLensGroups = false;
		}
	}

	async function loadMoreGrantsLocations(): Promise<void> {
		loadingMoreGrantsLocations = true;
		try {
			await fetchGrantsLocations(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreGrantsLocations = false;
		}
	}

	async function loadMoreBuckets(): Promise<void> {
		loadingMoreBuckets = true;
		try {
			await fetchBuckets(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreBuckets = false;
		}
	}

	// ==================== Access Points: create / delete / detail ====================
	//
	// No edit action: aws-sdk-go-v2/service/s3control has no
	// UpdateAccessPoint operation at all -- an access point's Name/Bucket are
	// immutable once created, and its two mutable sub-resources (policy,
	// scope) are managed by their own dedicated Put/Delete operations rather
	// than a resource-level update. This is a genuine API absence, not a
	// coverage gap, so no edit modal is built for it.

	let createApModal = $state<Modal | null>(null);
	let creatingAp = $state(false);
	let createApError = $state<string | null>(null);
	let newApName = $state('');
	let newApBucket = $state('');
	let newApVpcId = $state('');

	function openCreateApModal(): void {
		createApError = null;
		newApName = '';
		newApBucket = '';
		newApVpcId = '';
		createApModal?.open();
	}

	async function submitCreateAp(): Promise<void> {
		if (!newApName || !newApBucket) {
			createApError = 'Name and bucket are required.';
			return;
		}
		creatingAp = true;
		createApError = null;
		try {
			await client().send(
				new CreateAccessPointCommand({
					AccountId: accountId,
					Name: newApName,
					Bucket: newApBucket,
					VpcConfiguration: newApVpcId ? { VpcId: newApVpcId } : undefined
				})
			);
			toast.success('Access point created');
			createApModal?.close();
			await tabLoader.refresh('accesspoints');
		} catch (e) {
			const msg = describeError(e);
			createApError = msg;
			toast.error(msg);
		} finally {
			creatingAp = false;
		}
	}

	async function handleDeleteAp(a: AccessPoint): Promise<void> {
		if (!a.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete access point',
			message: `Delete access point ${a.Name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteAccessPointCommand({ AccountId: accountId, Name: a.Name }));
			toast.success('Access point deleted');
			await tabLoader.refresh('accesspoints');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let apDetailModal = $state<Modal | null>(null);
	let viewedAp = $state<AccessPoint | GetAccessPointResult | null>(null);
	let apDetailLoading = $state(false);
	let apDetailError = $state<string | null>(null);

	async function openApDetail(a: AccessPoint): Promise<void> {
		viewedAp = a;
		apDetailError = null;
		apDetailModal?.open();
		if (!a.Name) return;
		apDetailLoading = true;
		try {
			const resp = await client().send(new GetAccessPointCommand({ AccountId: accountId, Name: a.Name }));
			viewedAp = resp;
		} catch (e) {
			apDetailError = describeError(e);
		} finally {
			apDetailLoading = false;
		}
	}

	// ==================== Multi-Region Access Points: create / delete / detail ====================
	//
	// No edit action: aws-sdk-go-v2/service/s3control has no
	// UpdateMultiRegionAccessPoint operation. The only mutable sub-resource is
	// its policy (PutMultiRegionAccessPointPolicy), managed by its own
	// dedicated async operation rather than a resource-level update -- a
	// genuine API absence.

	let createMrapModal = $state<Modal | null>(null);
	let creatingMrap = $state(false);
	let createMrapError = $state<string | null>(null);
	let newMrapName = $state('');
	let newMrapBuckets = $state('');

	function openCreateMrapModal(): void {
		createMrapError = null;
		newMrapName = '';
		newMrapBuckets = '';
		createMrapModal?.open();
	}

	async function submitCreateMrap(): Promise<void> {
		const regionBuckets = parseCommaList(newMrapBuckets);
		if (!newMrapName || regionBuckets.length === 0) {
			createMrapError = 'Name and at least one region bucket are required.';
			return;
		}
		creatingMrap = true;
		createMrapError = null;
		try {
			await client().send(
				new CreateMultiRegionAccessPointCommand({
					AccountId: accountId,
					Details: {
						Name: newMrapName,
						Regions: regionBuckets.map((b) => ({ Bucket: b }))
					}
				})
			);
			toast.success('Multi-Region Access Point creation submitted');
			createMrapModal?.close();
			await tabLoader.refresh('mrap');
		} catch (e) {
			const msg = describeError(e);
			createMrapError = msg;
			toast.error(msg);
		} finally {
			creatingMrap = false;
		}
	}

	async function handleDeleteMrap(m: MultiRegionAccessPointReport): Promise<void> {
		if (!m.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Multi-Region Access Point',
			message: `Delete Multi-Region Access Point ${m.Name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteMultiRegionAccessPointCommand({ AccountId: accountId, Details: { Name: m.Name } })
			);
			toast.success('Multi-Region Access Point deletion submitted');
			await tabLoader.refresh('mrap');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let mrapDetailModal = $state<Modal | null>(null);
	let viewedMrap = $state<MultiRegionAccessPointReport | null>(null);
	let mrapDetailLoading = $state(false);
	let mrapDetailError = $state<string | null>(null);

	async function openMrapDetail(m: MultiRegionAccessPointReport): Promise<void> {
		viewedMrap = m;
		mrapDetailError = null;
		mrapDetailModal?.open();
		if (!m.Name) return;
		mrapDetailLoading = true;
		try {
			const resp = await client().send(
				new GetMultiRegionAccessPointCommand({ AccountId: accountId, Name: m.Name })
			);
			viewedMrap = resp.AccessPoint ?? m;
		} catch (e) {
			mrapDetailError = describeError(e);
		} finally {
			mrapDetailLoading = false;
		}
	}

	// ==================== Storage Lens Configurations: create / update / delete / detail ====================
	//
	// PutStorageLensConfiguration is the real API's single upsert operation --
	// there is no separate CreateStorageLensConfiguration -- so create and
	// edit both call the same command; the only difference is whether the ID
	// field is editable. This backend stores the configuration body as an
	// opaque XML blob (see services/s3control/handler_storage_lens.go) and
	// only ever echoes back Id/IsEnabled, so the create/edit form only
	// exposes those two real, backed fields rather than building UI for the
	// full AccountLevel/Include/Exclude/DataExport tree this backend cannot
	// meaningfully round-trip -- the same "expose only the fields the mock
	// backend actually models" precedent quicksight's DataSet
	// PhysicalTableMap comment documents. AccountLevel.BucketLevel is
	// required by the SDK's TypeScript type even though this backend ignores
	// its contents, so an empty object satisfies the type without inventing
	// UI for it.

	let createSlcModal = $state<Modal | null>(null);
	let creatingSlc = $state(false);
	let createSlcError = $state<string | null>(null);
	let newSlcId = $state('');
	let newSlcEnabled = $state(true);

	function openCreateSlcModal(): void {
		createSlcError = null;
		newSlcId = '';
		newSlcEnabled = true;
		createSlcModal?.open();
	}

	async function submitCreateSlc(): Promise<void> {
		if (!newSlcId) {
			createSlcError = 'Configuration ID is required.';
			return;
		}
		creatingSlc = true;
		createSlcError = null;
		try {
			await client().send(
				new PutStorageLensConfigurationCommand({
					AccountId: accountId,
					ConfigId: newSlcId,
					StorageLensConfiguration: {
						Id: newSlcId,
						AccountLevel: { BucketLevel: {} },
						IsEnabled: newSlcEnabled
					}
				})
			);
			toast.success('Storage Lens configuration created');
			createSlcModal?.close();
			await tabLoader.refresh('storagelens');
		} catch (e) {
			const msg = describeError(e);
			createSlcError = msg;
			toast.error(msg);
		} finally {
			creatingSlc = false;
		}
	}

	let editSlcModal = $state<Modal | null>(null);
	let editingSlc = $state<ListStorageLensConfigurationEntry | null>(null);
	let editingSlcEnabled = $state(true);
	let savingSlc = $state(false);
	let editSlcError = $state<string | null>(null);

	function openEditSlcModal(c: ListStorageLensConfigurationEntry): void {
		editingSlc = c;
		editingSlcEnabled = c.IsEnabled ?? true;
		editSlcError = null;
		editSlcModal?.open();
	}

	async function submitEditSlc(): Promise<void> {
		if (!editingSlc?.Id) return;
		savingSlc = true;
		editSlcError = null;
		try {
			await client().send(
				new PutStorageLensConfigurationCommand({
					AccountId: accountId,
					ConfigId: editingSlc.Id,
					StorageLensConfiguration: {
						Id: editingSlc.Id,
						AccountLevel: { BucketLevel: {} },
						IsEnabled: editingSlcEnabled
					}
				})
			);
			toast.success('Storage Lens configuration updated');
			editSlcModal?.close();
			await tabLoader.refresh('storagelens');
		} catch (e) {
			const msg = describeError(e);
			editSlcError = msg;
			toast.error(msg);
		} finally {
			savingSlc = false;
		}
	}

	async function handleDeleteSlc(c: ListStorageLensConfigurationEntry): Promise<void> {
		if (!c.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Storage Lens configuration',
			message: `Delete Storage Lens configuration ${c.Id}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteStorageLensConfigurationCommand({ AccountId: accountId, ConfigId: c.Id }));
			toast.success('Storage Lens configuration deleted');
			await tabLoader.refresh('storagelens');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let slcDetailModal = $state<Modal | null>(null);
	let viewedSlc = $state<StorageLensConfiguration | ListStorageLensConfigurationEntry | null>(null);
	let slcDetailLoading = $state(false);
	let slcDetailError = $state<string | null>(null);

	async function openSlcDetail(c: ListStorageLensConfigurationEntry): Promise<void> {
		viewedSlc = c;
		slcDetailError = null;
		slcDetailModal?.open();
		if (!c.Id) return;
		slcDetailLoading = true;
		try {
			const resp = await client().send(
				new GetStorageLensConfigurationCommand({ AccountId: accountId, ConfigId: c.Id })
			);
			viewedSlc = resp.StorageLensConfiguration ?? c;
		} catch (e) {
			slcDetailError = describeError(e);
		} finally {
			slcDetailLoading = false;
		}
	}

	// ==================== Storage Lens Groups: create / update / delete / detail ====================

	let createSlgModal = $state<Modal | null>(null);
	let creatingSlg = $state(false);
	let createSlgError = $state<string | null>(null);
	let newSlgName = $state('');
	let newSlgPrefixes = $state('');

	function openCreateSlgModal(): void {
		createSlgError = null;
		newSlgName = '';
		newSlgPrefixes = '';
		createSlgModal?.open();
	}

	async function submitCreateSlg(): Promise<void> {
		if (!newSlgName) {
			createSlgError = 'Name is required.';
			return;
		}
		creatingSlg = true;
		createSlgError = null;
		try {
			const prefixes = parseCommaList(newSlgPrefixes);
			await client().send(
				new CreateStorageLensGroupCommand({
					AccountId: accountId,
					StorageLensGroup: {
						Name: newSlgName,
						Filter: { MatchAnyPrefix: prefixes.length > 0 ? prefixes : undefined }
					}
				})
			);
			toast.success('Storage Lens group created');
			createSlgModal?.close();
			await tabLoader.refresh('storagelensgroups');
		} catch (e) {
			const msg = describeError(e);
			createSlgError = msg;
			toast.error(msg);
		} finally {
			creatingSlg = false;
		}
	}

	let editSlgModal = $state<Modal | null>(null);
	let editingSlg = $state<ListStorageLensGroupEntry | null>(null);
	let editingSlgPrefixes = $state('');
	let savingSlg = $state(false);
	let editSlgError = $state<string | null>(null);

	function openEditSlgModal(g: ListStorageLensGroupEntry): void {
		editingSlg = g;
		editingSlgPrefixes = '';
		editSlgError = null;
		editSlgModal?.open();
	}

	async function submitEditSlg(): Promise<void> {
		if (!editingSlg?.Name) return;
		savingSlg = true;
		editSlgError = null;
		try {
			const prefixes = parseCommaList(editingSlgPrefixes);
			await client().send(
				new UpdateStorageLensGroupCommand({
					AccountId: accountId,
					Name: editingSlg.Name,
					StorageLensGroup: {
						Name: editingSlg.Name,
						Filter: { MatchAnyPrefix: prefixes.length > 0 ? prefixes : undefined }
					}
				})
			);
			toast.success('Storage Lens group updated');
			editSlgModal?.close();
			await tabLoader.refresh('storagelensgroups');
		} catch (e) {
			const msg = describeError(e);
			editSlgError = msg;
			toast.error(msg);
		} finally {
			savingSlg = false;
		}
	}

	async function handleDeleteSlg(g: ListStorageLensGroupEntry): Promise<void> {
		if (!g.Name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Storage Lens group',
			message: `Delete Storage Lens group ${g.Name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteStorageLensGroupCommand({ AccountId: accountId, Name: g.Name }));
			toast.success('Storage Lens group deleted');
			await tabLoader.refresh('storagelensgroups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let slgDetailModal = $state<Modal | null>(null);
	let viewedSlg = $state<StorageLensGroup | ListStorageLensGroupEntry | null>(null);
	let slgDetailLoading = $state(false);
	let slgDetailError = $state<string | null>(null);

	async function openSlgDetail(g: ListStorageLensGroupEntry): Promise<void> {
		viewedSlg = g;
		slgDetailError = null;
		slgDetailModal?.open();
		if (!g.Name) return;
		slgDetailLoading = true;
		try {
			const resp = await client().send(new GetStorageLensGroupCommand({ AccountId: accountId, Name: g.Name }));
			viewedSlg = resp.StorageLensGroup ?? g;
		} catch (e) {
			slgDetailError = describeError(e);
		} finally {
			slgDetailLoading = false;
		}
	}

	function slgFilterPrefixes(g: StorageLensGroup | ListStorageLensGroupEntry | null): string {
		if (!g || !('Filter' in g) || !g.Filter) return '—';
		const prefixes = g.Filter.MatchAnyPrefix ?? [];
		return prefixes.length > 0 ? prefixes.join(', ') : '—';
	}

	// ==================== Access Grants Locations: create / update / delete / detail ====================
	//
	// UpdateAccessGrantsLocation only accepts a new IAMRoleArn --
	// LocationScope is immutable once registered (confirmed in the installed
	// SDK's doc comment: "You cannot update the scope of the registered
	// location"), so the edit modal only exposes the role ARN field.

	let createLocModal = $state<Modal | null>(null);
	let creatingLoc = $state(false);
	let createLocError = $state<string | null>(null);
	let newLocScope = $state('s3://');
	let newLocRoleArn = $state('');

	function openCreateLocModal(): void {
		createLocError = null;
		newLocScope = 's3://';
		newLocRoleArn = '';
		createLocModal?.open();
	}

	async function submitCreateLoc(): Promise<void> {
		if (!newLocScope || !newLocRoleArn) {
			createLocError = 'Location scope and IAM role ARN are required.';
			return;
		}
		creatingLoc = true;
		createLocError = null;
		try {
			await client().send(
				new CreateAccessGrantsLocationCommand({
					AccountId: accountId,
					LocationScope: newLocScope,
					IAMRoleArn: newLocRoleArn
				})
			);
			toast.success('Access Grants location created');
			createLocModal?.close();
			await tabLoader.refresh('grantslocations');
		} catch (e) {
			const msg = describeError(e);
			createLocError = msg;
			toast.error(msg);
		} finally {
			creatingLoc = false;
		}
	}

	let editLocModal = $state<Modal | null>(null);
	let editingLoc = $state<ListAccessGrantsLocationsEntry | null>(null);
	let editingLocRoleArn = $state('');
	let savingLoc = $state(false);
	let editLocError = $state<string | null>(null);

	function openEditLocModal(l: ListAccessGrantsLocationsEntry): void {
		editingLoc = l;
		editingLocRoleArn = l.IAMRoleArn ?? '';
		editLocError = null;
		editLocModal?.open();
	}

	async function submitEditLoc(): Promise<void> {
		if (!editingLoc?.AccessGrantsLocationId) return;
		if (!editingLocRoleArn) {
			editLocError = 'IAM role ARN is required.';
			return;
		}
		savingLoc = true;
		editLocError = null;
		try {
			await client().send(
				new UpdateAccessGrantsLocationCommand({
					AccountId: accountId,
					AccessGrantsLocationId: editingLoc.AccessGrantsLocationId,
					IAMRoleArn: editingLocRoleArn
				})
			);
			toast.success('Access Grants location updated');
			editLocModal?.close();
			await tabLoader.refresh('grantslocations');
		} catch (e) {
			const msg = describeError(e);
			editLocError = msg;
			toast.error(msg);
		} finally {
			savingLoc = false;
		}
	}

	async function handleDeleteLoc(l: ListAccessGrantsLocationsEntry): Promise<void> {
		if (!l.AccessGrantsLocationId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete Access Grants location',
			message: `Delete Access Grants location ${l.AccessGrantsLocationId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteAccessGrantsLocationCommand({
					AccountId: accountId,
					AccessGrantsLocationId: l.AccessGrantsLocationId
				})
			);
			toast.success('Access Grants location deleted');
			await tabLoader.refresh('grantslocations');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let locDetailModal = $state<Modal | null>(null);
	let viewedLoc = $state<ListAccessGrantsLocationsEntry | GetAccessGrantsLocationResult | null>(null);
	let locDetailLoading = $state(false);
	let locDetailError = $state<string | null>(null);

	async function openLocDetail(l: ListAccessGrantsLocationsEntry): Promise<void> {
		viewedLoc = l;
		locDetailError = null;
		locDetailModal?.open();
		if (!l.AccessGrantsLocationId) return;
		locDetailLoading = true;
		try {
			const resp = await client().send(
				new GetAccessGrantsLocationCommand({
					AccountId: accountId,
					AccessGrantsLocationId: l.AccessGrantsLocationId
				})
			);
			viewedLoc = resp;
		} catch (e) {
			locDetailError = describeError(e);
		} finally {
			locDetailLoading = false;
		}
	}

	// ==================== Outposts Buckets: create / delete / detail ====================
	//
	// No edit action: aws-sdk-go-v2/service/s3control has no UpdateBucket
	// operation. Its mutable sub-resources (lifecycle, policy, replication,
	// tagging, versioning) each have their own dedicated Put/Get/Delete
	// operations rather than a resource-level update -- a genuine API
	// absence, not a coverage gap, and out of scope for the CRUD floor on
	// the bucket resource itself.
	//
	// WIRE-SHAPE FINDING (not fixed here -- out of scope, services/ is
	// another worker's file this pass): CreateBucketCommandInput has NO
	// AccountId field at all (confirmed against the installed
	// @aws-sdk/client-s3-control@3.1094.0 compiled model --
	// CreateBucketRequest$'s member list is Bucket/ACL/
	// CreateBucketConfiguration/GrantFullControl/GrantRead/GrantReadACP/
	// GrantWrite/GrantWriteACP/ObjectLockEnabledForBucket/OutpostId, with no
	// "x-amz-account-id" header binding), unlike GetBucket/DeleteBucket/
	// ListRegionalBuckets, which all bind AccountId to that header. A real
	// aws-sdk-go-v2/aws-sdk-js client's CreateBucket call therefore never
	// sends that header. gopherstack's handleCreateBucket
	// (services/s3control/handler_bucket.go) resolves the owning account via
	// the same accountIDFromRequest(c) every other bucket op uses, which
	// falls back to the literal string "default" when the header is absent
	// -- so every real client's CreateBucket call is silently namespaced
	// under account "default", while that same client's follow-up
	// GetBucket/ListRegionalBuckets/DeleteBucket calls (which DO send its
	// real AccountId) look in a different account and will never find the
	// bucket it just created. This UI intentionally does NOT work around it
	// (e.g. by smuggling AccountId through some other channel) -- there is
	// no such channel in the real wire protocol either, so mirroring the
	// real SDK's CreateBucketCommandInput here (no AccountId) is the
	// correct behavior; the mismatch is a backend account-scoping gap to
	// fix in services/s3control, not a UI concern.

	let createBucketModal = $state<Modal | null>(null);
	let creatingBucket = $state(false);
	let createBucketError = $state<string | null>(null);
	let newBucketName = $state('');

	function openCreateBucketModal(): void {
		createBucketError = null;
		newBucketName = '';
		createBucketModal?.open();
	}

	async function submitCreateBucket(): Promise<void> {
		if (!newBucketName) {
			createBucketError = 'Bucket name is required.';
			return;
		}
		creatingBucket = true;
		createBucketError = null;
		try {
			await client().send(new CreateBucketCommand({ Bucket: newBucketName }));
			toast.success('Bucket created');
			createBucketModal?.close();
			await tabLoader.refresh('buckets');
		} catch (e) {
			const msg = describeError(e);
			createBucketError = msg;
			toast.error(msg);
		} finally {
			creatingBucket = false;
		}
	}

	async function handleDeleteBucket(b: RegionalBucket): Promise<void> {
		if (!b.Bucket) return;
		const confirmed = await confirmDestructive({
			title: 'Delete bucket',
			message: `Delete Outposts bucket ${b.Bucket}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteBucketCommand({ AccountId: accountId, Bucket: b.Bucket }));
			toast.success('Bucket deleted');
			await tabLoader.refresh('buckets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let bucketDetailModal = $state<Modal | null>(null);
	let viewedBucket = $state<RegionalBucket | GetBucketResult | null>(null);
	let bucketDetailLoading = $state(false);
	let bucketDetailError = $state<string | null>(null);

	async function openBucketDetail(b: RegionalBucket): Promise<void> {
		viewedBucket = b;
		bucketDetailError = null;
		bucketDetailModal?.open();
		if (!b.Bucket) return;
		bucketDetailLoading = true;
		try {
			const resp = await client().send(new GetBucketCommand({ AccountId: accountId, Bucket: b.Bucket }));
			viewedBucket = { ...resp, Bucket: resp.Bucket ?? b.Bucket };
		} catch (e) {
			bucketDetailError = describeError(e);
		} finally {
			bucketDetailLoading = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Database}
		title="Amazon S3 Control"
		description="Manage S3 resources across accounts and regions"
		onRefresh={handleRefresh}
	>
		{#snippet actions()}
			{#if activeTab === 'accesspoints'}
				<button
					onclick={openCreateApModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create access point
				</button>
			{:else if activeTab === 'mrap'}
				<button
					onclick={openCreateMrapModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create Multi-Region AP
				</button>
			{:else if activeTab === 'storagelens'}
				<button
					onclick={openCreateSlcModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create configuration
				</button>
			{:else if activeTab === 'storagelensgroups'}
				<button
					onclick={openCreateSlgModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create group
				</button>
			{:else if activeTab === 'grantslocations'}
				<button
					onclick={openCreateLocModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Register location
				</button>
			{:else if activeTab === 'buckets'}
				<button
					onclick={openCreateBucketModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-amber-600 text-white hover:bg-amber-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create bucket
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="amber" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div
					role="alert"
					class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300"
				>
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'accesspoints'}
				{#snippet apActionsCell(a: AccessPoint)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openApDetail(a)}
							title="View"
							aria-label="View access point {a.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteAp(a)}
							title="Delete"
							aria-label="Delete access point {a.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const apColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'Bucket', label: 'Bucket' },
					{ key: 'NetworkOrigin', label: 'Network Origin' },
					{ key: 'Alias', label: 'Alias' },
					{ key: 'actions', label: '', render: apActionsCell }
				] as Column<AccessPoint>[]}
				<DataTable
					rows={filteredAccessPoints}
					rowKey={(a) => a.Name ?? ''}
					columns={apColumns}
					loading={tabLoader.isLoading('accesspoints')}
					emptyMessage="No access points found"
				/>
				<LoadMore
					hasMore={!!accessPointsNextToken}
					loading={loadingMoreAccessPoints}
					onLoadMore={loadMoreAccessPoints}
				/>
			{:else if activeTab === 'mrap'}
				{#snippet mrapStatusCell(m: MultiRegionAccessPointReport)}
					<span
						class="text-xs px-2 py-1 rounded-full {m.Status === 'READY'
							? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
							: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}">{m.Status ?? '—'}</span
					>
				{/snippet}
				{#snippet mrapCreatedCell(m: MultiRegionAccessPointReport)}
					{formatDate(m.CreatedAt)}
				{/snippet}
				{#snippet mrapActionsCell(m: MultiRegionAccessPointReport)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openMrapDetail(m)}
							title="View"
							aria-label="View Multi-Region Access Point {m.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteMrap(m)}
							title="Delete"
							aria-label="Delete Multi-Region Access Point {m.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const mrapColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'Alias', label: 'Alias' },
					{ key: 'Status', label: 'Status', render: mrapStatusCell },
					{ key: 'CreatedAt', label: 'Created', render: mrapCreatedCell },
					{ key: 'actions', label: '', render: mrapActionsCell }
				] as Column<MultiRegionAccessPointReport>[]}
				<DataTable
					rows={filteredMraps}
					rowKey={(m) => m.Name ?? ''}
					columns={mrapColumns}
					loading={tabLoader.isLoading('mrap')}
					emptyMessage="No Multi-Region Access Points found"
				/>
			{:else if activeTab === 'storagelens'}
				{#snippet slcEnabledCell(c: ListStorageLensConfigurationEntry)}
					<span
						class="text-xs px-2 py-1 rounded-full {c.IsEnabled
							? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
							: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
						>{c.IsEnabled ? 'Enabled' : 'Disabled'}</span
					>
				{/snippet}
				{#snippet slcActionsCell(c: ListStorageLensConfigurationEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openSlcDetail(c)}
							title="View"
							aria-label="View Storage Lens configuration {c.Id}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditSlcModal(c)}
							title="Edit"
							aria-label="Edit Storage Lens configuration {c.Id}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteSlc(c)}
							title="Delete"
							aria-label="Delete Storage Lens configuration {c.Id}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const slcColumns = [
					{ key: 'Id', label: 'ID' },
					{ key: 'HomeRegion', label: 'Home Region' },
					{ key: 'IsEnabled', label: 'Status', render: slcEnabledCell },
					{ key: 'actions', label: '', render: slcActionsCell }
				] as Column<ListStorageLensConfigurationEntry>[]}
				<DataTable
					rows={filteredStorageLensConfigs}
					rowKey={(c) => c.Id ?? ''}
					columns={slcColumns}
					loading={tabLoader.isLoading('storagelens')}
					emptyMessage="No Storage Lens configurations found"
				/>
				<LoadMore
					hasMore={!!storageLensConfigsNextToken}
					loading={loadingMoreStorageLensConfigs}
					onLoadMore={loadMoreStorageLensConfigs}
				/>
			{:else if activeTab === 'storagelensgroups'}
				{#snippet slgActionsCell(g: ListStorageLensGroupEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openSlgDetail(g)}
							title="View"
							aria-label="View Storage Lens group {g.Name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditSlgModal(g)}
							title="Edit"
							aria-label="Edit Storage Lens group {g.Name}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteSlg(g)}
							title="Delete"
							aria-label="Delete Storage Lens group {g.Name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const slgColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'StorageLensGroupArn', label: 'ARN' },
					{ key: 'actions', label: '', render: slgActionsCell }
				] as Column<ListStorageLensGroupEntry>[]}
				<DataTable
					rows={filteredStorageLensGroups}
					rowKey={(g) => g.Name ?? ''}
					columns={slgColumns}
					loading={tabLoader.isLoading('storagelensgroups')}
					emptyMessage="No Storage Lens groups found"
				/>
				<LoadMore
					hasMore={!!storageLensGroupsNextToken}
					loading={loadingMoreStorageLensGroups}
					onLoadMore={loadMoreStorageLensGroups}
				/>
			{:else if activeTab === 'grantslocations'}
				{#snippet locCreatedCell(l: ListAccessGrantsLocationsEntry)}
					{formatDate(l.CreatedAt)}
				{/snippet}
				{#snippet locActionsCell(l: ListAccessGrantsLocationsEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openLocDetail(l)}
							title="View"
							aria-label="View Access Grants location {l.AccessGrantsLocationId}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => openEditLocModal(l)}
							title="Edit"
							aria-label="Edit Access Grants location {l.AccessGrantsLocationId}"
							class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteLoc(l)}
							title="Delete"
							aria-label="Delete Access Grants location {l.AccessGrantsLocationId}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const locColumns = [
					{ key: 'AccessGrantsLocationId', label: 'ID' },
					{ key: 'LocationScope', label: 'Scope' },
					{ key: 'IAMRoleArn', label: 'IAM Role ARN' },
					{ key: 'CreatedAt', label: 'Created', render: locCreatedCell },
					{ key: 'actions', label: '', render: locActionsCell }
				] as Column<ListAccessGrantsLocationsEntry>[]}
				<DataTable
					rows={filteredGrantsLocations}
					rowKey={(l) => l.AccessGrantsLocationId ?? ''}
					columns={locColumns}
					loading={tabLoader.isLoading('grantslocations')}
					emptyMessage="No Access Grants locations found"
				/>
				<LoadMore
					hasMore={!!grantsLocationsNextToken}
					loading={loadingMoreGrantsLocations}
					onLoadMore={loadMoreGrantsLocations}
				/>
			{:else if activeTab === 'buckets'}
				{#snippet bucketPabCell(b: RegionalBucket)}
					<span
						class="text-xs px-2 py-1 rounded-full {b.PublicAccessBlockEnabled
							? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
							: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400'}"
						>{b.PublicAccessBlockEnabled ? 'Blocked' : 'Not blocked'}</span
					>
				{/snippet}
				{#snippet bucketCreatedCell(b: RegionalBucket)}
					{formatDate(b.CreationDate)}
				{/snippet}
				{#snippet bucketActionsCell(b: RegionalBucket)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openBucketDetail(b)}
							title="View"
							aria-label="View bucket {b.Bucket}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteBucket(b)}
							title="Delete"
							aria-label="Delete bucket {b.Bucket}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const bucketColumns = [
					{ key: 'Bucket', label: 'Bucket' },
					{ key: 'OutpostId', label: 'Outpost ID' },
					{ key: 'PublicAccessBlockEnabled', label: 'Public Access', render: bucketPabCell },
					{ key: 'CreationDate', label: 'Created', render: bucketCreatedCell },
					{ key: 'actions', label: '', render: bucketActionsCell }
				] as Column<RegionalBucket>[]}
				<DataTable
					rows={filteredBuckets}
					rowKey={(b) => b.Bucket ?? ''}
					columns={bucketColumns}
					loading={tabLoader.isLoading('buckets')}
					emptyMessage="No Outposts buckets found"
				/>
				<LoadMore hasMore={!!bucketsNextToken} loading={loadingMoreBuckets} onLoadMore={loadMoreBuckets} />
			{/if}
		</div>
	</div>
</div>

<!-- ==================== Access Points modals ==================== -->

<Modal bind:this={createApModal} title="Create Access Point">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ap-name" class="text-sm text-slate-600 dark:text-slate-300">Access point name</label>
				<input
					id="ap-name"
					bind:value={newApName}
					placeholder="my-access-point"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="ap-bucket" class="text-sm text-slate-600 dark:text-slate-300">Bucket</label>
				<input
					id="ap-bucket"
					bind:value={newApBucket}
					placeholder="my-bucket"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="ap-vpc" class="text-sm text-slate-600 dark:text-slate-300">VPC ID (optional)</label>
				<input
					id="ap-vpc"
					bind:value={newApVpcId}
					placeholder="vpc-0123456789abcdef0"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createApError}
				<p class="text-sm text-red-600 dark:text-red-400">{createApError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createApModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateAp}
			disabled={creatingAp}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{creatingAp ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={apDetailModal} title="Access Point">
	{#snippet children()}
		{#if apDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAp}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAp.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Bucket</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAp.Bucket ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Network origin</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAp.NetworkOrigin ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Alias</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAp.Alias ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedAp.AccessPointArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">VPC ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedAp.VpcConfiguration?.VpcId ?? '—'}</dd>
				</div>
				{#if 'CreationDate' in viewedAp}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Created</dt>
						<dd class="text-slate-900 dark:text-white">{formatDate(viewedAp.CreationDate)}</dd>
					</div>
				{/if}
				{#if 'PublicAccessBlockConfiguration' in viewedAp && viewedAp.PublicAccessBlockConfiguration}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Public access block</dt>
						<dd class="text-slate-900 dark:text-white">
							Block ACLs: {viewedAp.PublicAccessBlockConfiguration.BlockPublicAcls ? 'Yes' : 'No'},
							Ignore ACLs: {viewedAp.PublicAccessBlockConfiguration.IgnorePublicAcls ? 'Yes' : 'No'},
							Block policy: {viewedAp.PublicAccessBlockConfiguration.BlockPublicPolicy ? 'Yes' : 'No'},
							Restrict buckets: {viewedAp.PublicAccessBlockConfiguration.RestrictPublicBuckets
								? 'Yes'
								: 'No'}
						</dd>
					</div>
				{/if}
			</dl>
			{#if apDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{apDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => apDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Multi-Region Access Points modals ==================== -->

<Modal bind:this={createMrapModal} title="Create Multi-Region Access Point">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mrap-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="mrap-name"
					bind:value={newMrapName}
					placeholder="my-mrap"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="mrap-buckets" class="text-sm text-slate-600 dark:text-slate-300"
					>Region buckets (comma-separated)</label
				>
				<input
					id="mrap-buckets"
					bind:value={newMrapBuckets}
					placeholder="bucket-us-east-1, bucket-us-west-2"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createMrapError}
				<p class="text-sm text-red-600 dark:text-red-400">{createMrapError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createMrapModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateMrap}
			disabled={creatingMrap}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{creatingMrap ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={mrapDetailModal} title="Multi-Region Access Point">
	{#snippet children()}
		{#if mrapDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedMrap}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMrap.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Alias</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMrap.Alias ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Status</dt>
					<dd class="text-slate-900 dark:text-white">{viewedMrap.Status ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedMrap.CreatedAt)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Region buckets</dt>
					<dd class="text-slate-900 dark:text-white">
						{(viewedMrap.Regions ?? []).map((r) => r.Bucket).join(', ') || '—'}
					</dd>
				</div>
			</dl>
			{#if mrapDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{mrapDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => mrapDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Storage Lens Configurations modals ==================== -->

<Modal bind:this={createSlcModal} title="Create Storage Lens Configuration">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="slc-id" class="text-sm text-slate-600 dark:text-slate-300">Configuration ID</label>
				<input
					id="slc-id"
					bind:value={newSlcId}
					placeholder="my-storage-lens-config"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newSlcEnabled} />
				Enabled
			</label>
			{#if createSlcError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSlcError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createSlcModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateSlc}
			disabled={creatingSlc}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{creatingSlc ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editSlcModal} title="Edit Storage Lens Configuration">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingSlc?.Id}</span>
			</p>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={editingSlcEnabled} />
				Enabled
			</label>
			{#if editSlcError}
				<p class="text-sm text-red-600 dark:text-red-400">{editSlcError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editSlcModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditSlc}
			disabled={savingSlc}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{savingSlc ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={slcDetailModal} title="Storage Lens Configuration">
	{#snippet children()}
		{#if slcDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedSlc}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSlc.Id ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Enabled</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSlc.IsEnabled ? 'Yes' : 'No'}</dd>
				</div>
				{#if 'StorageLensArn' in viewedSlc}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
						<dd class="break-all text-slate-900 dark:text-white">{viewedSlc.StorageLensArn ?? '—'}</dd>
					</div>
				{/if}
			</dl>
			{#if slcDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{slcDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => slcDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Storage Lens Groups modals ==================== -->

<Modal bind:this={createSlgModal} title="Create Storage Lens Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="slg-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="slg-name"
					bind:value={newSlgName}
					placeholder="my-storage-lens-group"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="slg-prefixes" class="text-sm text-slate-600 dark:text-slate-300"
					>Match any prefix (comma-separated, optional)</label
				>
				<input
					id="slg-prefixes"
					bind:value={newSlgPrefixes}
					placeholder="logs/, images/"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createSlgError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSlgError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createSlgModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateSlg}
			disabled={creatingSlg}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{creatingSlg ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editSlgModal} title="Edit Storage Lens Group">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingSlg?.Name}</span>
			</p>
			<div>
				<label for="edit-slg-prefixes" class="text-sm text-slate-600 dark:text-slate-300"
					>Match any prefix (comma-separated, replaces the existing filter)</label
				>
				<input
					id="edit-slg-prefixes"
					bind:value={editingSlgPrefixes}
					placeholder="logs/, images/"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editSlgError}
				<p class="text-sm text-red-600 dark:text-red-400">{editSlgError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editSlgModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditSlg}
			disabled={savingSlg}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{savingSlg ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={slgDetailModal} title="Storage Lens Group">
	{#snippet children()}
		{#if slgDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedSlg}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Name</dt>
					<dd class="text-slate-900 dark:text-white">{viewedSlg.Name ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedSlg.StorageLensGroupArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Match any prefix</dt>
					<dd class="text-slate-900 dark:text-white">{slgFilterPrefixes(viewedSlg)}</dd>
				</div>
			</dl>
			{#if slgDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{slgDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => slgDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Access Grants Locations modals ==================== -->

<Modal bind:this={createLocModal} title="Register Access Grants Location">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="loc-scope" class="text-sm text-slate-600 dark:text-slate-300">Location scope</label>
				<input
					id="loc-scope"
					bind:value={newLocScope}
					placeholder="s3://my-bucket/prefix"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="loc-role" class="text-sm text-slate-600 dark:text-slate-300">IAM role ARN</label>
				<input
					id="loc-role"
					bind:value={newLocRoleArn}
					placeholder="arn:aws:iam::123456789012:role/AccessGrantsRole"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createLocError}
				<p class="text-sm text-red-600 dark:text-red-400">{createLocError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createLocModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateLoc}
			disabled={creatingLoc}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{creatingLoc ? 'Registering…' : 'Register'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={editLocModal} title="Edit Access Grants Location">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				Editing <span class="font-medium">{editingLoc?.AccessGrantsLocationId}</span> ({editingLoc?.LocationScope})
			</p>
			<div>
				<label for="edit-loc-role" class="text-sm text-slate-600 dark:text-slate-300">New IAM role ARN</label>
				<input
					id="edit-loc-role"
					bind:value={editingLocRoleArn}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if editLocError}
				<p class="text-sm text-red-600 dark:text-red-400">{editLocError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => editLocModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitEditLoc}
			disabled={savingLoc}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{savingLoc ? 'Saving…' : 'Save'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={locDetailModal} title="Access Grants Location">
	{#snippet children()}
		{#if locDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedLoc}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Location ID</dt>
					<dd class="text-slate-900 dark:text-white">{viewedLoc.AccessGrantsLocationId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Scope</dt>
					<dd class="text-slate-900 dark:text-white">{viewedLoc.LocationScope ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">IAM role ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedLoc.IAMRoleArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
					<dd class="break-all text-slate-900 dark:text-white">{viewedLoc.AccessGrantsLocationArn ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedLoc.CreatedAt)}</dd>
				</div>
			</dl>
			{#if locDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{locDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => locDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<!-- ==================== Outposts Buckets modals ==================== -->

<Modal bind:this={createBucketModal} title="Create Bucket">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="bucket-name" class="text-sm text-slate-600 dark:text-slate-300">Bucket name</label>
				<input
					id="bucket-name"
					bind:value={newBucketName}
					placeholder="my-outposts-bucket"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createBucketError}
				<p class="text-sm text-red-600 dark:text-red-400">{createBucketError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createBucketModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateBucket}
			disabled={creatingBucket}
			class="rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700 disabled:opacity-50"
			>{creatingBucket ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={bucketDetailModal} title="Bucket">
	{#snippet children()}
		{#if bucketDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedBucket}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Bucket</dt>
					<dd class="text-slate-900 dark:text-white">{viewedBucket.Bucket ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Public access block enabled</dt>
					<dd class="text-slate-900 dark:text-white">{viewedBucket.PublicAccessBlockEnabled ? 'Yes' : 'No'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedBucket.CreationDate)}</dd>
				</div>
				{#if 'BucketArn' in viewedBucket}
					<div>
						<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
						<dd class="break-all text-slate-900 dark:text-white">{viewedBucket.BucketArn ?? '—'}</dd>
					</div>
				{/if}
			</dl>
			{#if bucketDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{bucketDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => bucketDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
