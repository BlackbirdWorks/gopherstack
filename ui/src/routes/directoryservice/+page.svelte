<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDirectoryServiceClient } from '$lib/aws-client';
	import {
		DescribeDirectoriesCommand,
		CreateDirectoryCommand,
		CreateMicrosoftADCommand,
		ConnectDirectoryCommand,
		DeleteDirectoryCommand,
		UpdateDirectorySetupCommand,
		DescribeSnapshotsCommand,
		CreateSnapshotCommand,
		DeleteSnapshotCommand,
		RestoreFromSnapshotCommand,
		DescribeTrustsCommand,
		CreateTrustCommand,
		DeleteTrustCommand,
		UpdateTrustCommand,
		VerifyTrustCommand,
		DescribeConditionalForwardersCommand,
		CreateConditionalForwarderCommand,
		UpdateConditionalForwarderCommand,
		DeleteConditionalForwarderCommand,
		ListLogSubscriptionsCommand,
		CreateLogSubscriptionCommand,
		DeleteLogSubscriptionCommand,
		ListIpRoutesCommand,
		AddIpRoutesCommand,
		RemoveIpRoutesCommand,
		ListSchemaExtensionsCommand,
		StartSchemaExtensionCommand,
		CancelSchemaExtensionCommand,
		ListCertificatesCommand,
		RegisterCertificateCommand,
		DeregisterCertificateCommand,
		DescribeCertificateCommand,
		DescribeEventTopicsCommand,
		RegisterEventTopicCommand,
		DeregisterEventTopicCommand,
		DescribeDomainControllersCommand,
		UpdateNumberOfDomainControllersCommand,
		DescribeRegionsCommand,
		AddRegionCommand,
		RemoveRegionCommand,
		DescribeSharedDirectoriesCommand,
		ShareDirectoryCommand,
		UnshareDirectoryCommand,
		AcceptSharedDirectoryCommand,
		RejectSharedDirectoryCommand,
		ListADAssessmentsCommand,
		StartADAssessmentCommand,
		DeleteADAssessmentCommand,
		DescribeADAssessmentCommand,
		DescribeSettingsCommand,
		UpdateSettingsCommand,
		type DirectoryDescription,
		type Snapshot,
		type Trust,
		type ConditionalForwarder,
		type LogSubscription,
		type IpRouteInfo,
		type SchemaExtensionInfo,
		type CertificateInfo,
		type Certificate,
		type EventTopic,
		type DomainController,
		type RegionDescription,
		type SharedDirectory,
		type AssessmentSummary,
		type Assessment,
		type AssessmentReport,
		type SettingEntry
	} from '@aws-sdk/client-directory-service';
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
	import { Network, Plus, Trash2, Eye, Pencil, Check } from 'lucide-svelte';

	const client = regionalClient(getDirectoryServiceClient);

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

	// Replaces the JSON.stringify(x).toLowerCase().includes(...) antipattern:
	// search only over the named fields that are actually meaningful for each
	// resource, not the entire serialized object (which also matches on
	// internal/opaque field values and is O(object size) per row per
	// keystroke).
	function matches(q: string, ...vals: (string | undefined)[]): boolean {
		if (!q) return true;
		const needle = q.toLowerCase();
		return vals.some((v) => (v ?? '').toLowerCase().includes(needle));
	}

	function parseCommaList(s: string): string[] {
		return s
			.split(',')
			.map((x) => x.trim())
			.filter((x) => x.length > 0);
	}

	type TabId =
		| 'directories'
		| 'snapshots'
		| 'trusts'
		| 'conditionalForwarders'
		| 'logSubscriptions'
		| 'ipRoutes'
		| 'schemaExtensions'
		| 'certificates'
		| 'eventTopics'
		| 'domainControllers'
		| 'regions'
		| 'sharedDirectories'
		| 'adAssessments'
		| 'settings';

	const tabs: TabDef[] = [
		{ id: 'directories', label: 'Directories' },
		{ id: 'snapshots', label: 'Snapshots' },
		{ id: 'trusts', label: 'Trusts' },
		{ id: 'conditionalForwarders', label: 'Conditional Forwarders' },
		{ id: 'logSubscriptions', label: 'Log Subscriptions' },
		{ id: 'ipRoutes', label: 'IP Routes' },
		{ id: 'schemaExtensions', label: 'Schema Extensions' },
		{ id: 'certificates', label: 'Certificates' },
		{ id: 'eventTopics', label: 'Event Topics' },
		{ id: 'domainControllers', label: 'Domain Controllers' },
		{ id: 'regions', label: 'Regions' },
		{ id: 'sharedDirectories', label: 'Shared Directories' },
		{ id: 'adAssessments', label: 'AD Assessments' },
		{ id: 'settings', label: 'Settings' }
	];

	// Tabs whose real List/Describe operation REQUIRES a DirectoryId
	// (confirmed against the installed @aws-sdk/client-directory-service
	// compiled types: DescribeConditionalForwardersRequest.DirectoryId,
	// ListIpRoutesRequest.DirectoryId, ListSchemaExtensionsRequest.DirectoryId,
	// ListCertificatesRequest.DirectoryId, DescribeDomainControllersRequest.DirectoryId,
	// DescribeRegionsRequest.DirectoryId, DescribeSettingsRequest.DirectoryId are all
	// non-optional; DescribeSharedDirectoriesRequest.OwnerDirectoryId is likewise
	// non-optional and the backend 400s without it). These tabs show an
	// empty-state prompting directory selection until one is chosen.
	const strictScopedTabs: TabId[] = [
		'conditionalForwarders',
		'ipRoutes',
		'schemaExtensions',
		'certificates',
		'domainControllers',
		'regions',
		'sharedDirectories',
		'settings'
	];
	// Tabs whose List op takes an OPTIONAL DirectoryId (DescribeSnapshotsRequest,
	// DescribeTrustsRequest, ListLogSubscriptionsRequest,
	// DescribeEventTopicsRequest, ListADAssessmentsRequest all mark it `?`) --
	// these list every matching resource in the account/region when no
	// directory is selected, and narrow to the selected directory once one is.
	const optionalScopedTabs: TabId[] = [
		'snapshots',
		'trusts',
		'logSubscriptions',
		'eventTopics',
		'adAssessments'
	];
	const directoryScopedTabs: TabId[] = [...strictScopedTabs, ...optionalScopedTabs];

	let activeTab = $state<TabId>('directories');
	let searchQuery = $state('');

	// Directories is the parent resource for every other tab -- the same
	// shared-selector pattern accessanalyzer uses for its analyzer-scoped
	// tabs. Directory Service's directory types (Simple AD / AD Connector /
	// Managed Microsoft AD / hybrid) are created by three different
	// operations but all show up in the same DescribeDirectories list, so one
	// selector covers all of them.
	let selectedDirectoryId = $state('');

	let dirs = $state<DirectoryDescription[]>([]);
	let dirsNextToken = $state<string | undefined>();
	let loadingMoreDirs = $state(false);

	const selectedDirectory = $derived(dirs.find((d) => d.DirectoryId === selectedDirectoryId));

	let snaps = $state<Snapshot[]>([]);
	let snapsNextToken = $state<string | undefined>();
	let loadingMoreSnaps = $state(false);

	let trusts = $state<Trust[]>([]);
	let trustsNextToken = $state<string | undefined>();
	let loadingMoreTrusts = $state(false);

	// DescribeConditionalForwardersResult has no NextToken member at all
	// (confirmed against the installed SDK's compiled type) -- the real API
	// genuinely returns every conditional forwarder for the directory in one
	// response, so there is nothing to paginate.
	let cfwds = $state<ConditionalForwarder[]>([]);

	let logSubs = $state<LogSubscription[]>([]);
	let logSubsNextToken = $state<string | undefined>();
	let loadingMoreLogSubs = $state(false);

	let ipRoutesList = $state<IpRouteInfo[]>([]);
	let ipRoutesNextToken = $state<string | undefined>();
	let loadingMoreIpRoutes = $state(false);

	let schemaExts = $state<SchemaExtensionInfo[]>([]);
	let schemaExtsNextToken = $state<string | undefined>();
	let loadingMoreSchemaExts = $state(false);

	let certs = $state<CertificateInfo[]>([]);
	let certsNextToken = $state<string | undefined>();
	let loadingMoreCerts = $state(false);

	// DescribeEventTopicsResult likewise has no NextToken member -- genuinely
	// unpaginated in the real API.
	let eventTopics = $state<EventTopic[]>([]);

	let dcs = $state<DomainController[]>([]);
	let dcsNextToken = $state<string | undefined>();
	let loadingMoreDcs = $state(false);

	let regionsList = $state<RegionDescription[]>([]);
	let regionsNextToken = $state<string | undefined>();
	let loadingMoreRegions = $state(false);

	let sharedDirs = $state<SharedDirectory[]>([]);
	let sharedDirsNextToken = $state<string | undefined>();
	let loadingMoreSharedDirs = $state(false);

	let assessments = $state<AssessmentSummary[]>([]);
	let assessmentsNextToken = $state<string | undefined>();
	let loadingMoreAssessments = $state(false);

	let settingsList = $state<SettingEntry[]>([]);
	let settingsNextToken = $state<string | undefined>();
	let loadingMoreSettings = $state(false);

	async function fetchDirs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeDirectoriesCommand({ NextToken: reset ? undefined : dirsNextToken })
		);
		dirs = reset ? (resp.DirectoryDescriptions ?? []) : [...dirs, ...(resp.DirectoryDescriptions ?? [])];
		dirsNextToken = resp.NextToken;
		if (!selectedDirectoryId && dirs.length > 0) {
			selectedDirectoryId = dirs[0].DirectoryId ?? '';
		}
	}

	async function fetchSnaps(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeSnapshotsCommand({
				DirectoryId: selectedDirectoryId || undefined,
				NextToken: reset ? undefined : snapsNextToken
			})
		);
		snaps = reset ? (resp.Snapshots ?? []) : [...snaps, ...(resp.Snapshots ?? [])];
		snapsNextToken = resp.NextToken;
	}

	async function fetchTrusts(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeTrustsCommand({
				DirectoryId: selectedDirectoryId || undefined,
				NextToken: reset ? undefined : trustsNextToken
			})
		);
		trusts = reset ? (resp.Trusts ?? []) : [...trusts, ...(resp.Trusts ?? [])];
		trustsNextToken = resp.NextToken;
	}

	async function fetchCfwds(): Promise<void> {
		if (!selectedDirectoryId) {
			cfwds = [];
			return;
		}
		const resp = await client().send(
			new DescribeConditionalForwardersCommand({ DirectoryId: selectedDirectoryId })
		);
		cfwds = resp.ConditionalForwarders ?? [];
	}

	async function fetchLogSubs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListLogSubscriptionsCommand({
				DirectoryId: selectedDirectoryId || undefined,
				NextToken: reset ? undefined : logSubsNextToken
			})
		);
		logSubs = reset ? (resp.LogSubscriptions ?? []) : [...logSubs, ...(resp.LogSubscriptions ?? [])];
		logSubsNextToken = resp.NextToken;
	}

	async function fetchIpRoutes(reset: boolean): Promise<void> {
		if (!selectedDirectoryId) {
			ipRoutesList = [];
			ipRoutesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListIpRoutesCommand({
				DirectoryId: selectedDirectoryId,
				NextToken: reset ? undefined : ipRoutesNextToken
			})
		);
		ipRoutesList = reset ? (resp.IpRoutesInfo ?? []) : [...ipRoutesList, ...(resp.IpRoutesInfo ?? [])];
		ipRoutesNextToken = resp.NextToken;
	}

	async function fetchSchemaExts(reset: boolean): Promise<void> {
		if (!selectedDirectoryId) {
			schemaExts = [];
			schemaExtsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListSchemaExtensionsCommand({
				DirectoryId: selectedDirectoryId,
				NextToken: reset ? undefined : schemaExtsNextToken
			})
		);
		schemaExts = reset
			? (resp.SchemaExtensionsInfo ?? [])
			: [...schemaExts, ...(resp.SchemaExtensionsInfo ?? [])];
		schemaExtsNextToken = resp.NextToken;
	}

	async function fetchCerts(reset: boolean): Promise<void> {
		if (!selectedDirectoryId) {
			certs = [];
			certsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListCertificatesCommand({
				DirectoryId: selectedDirectoryId,
				NextToken: reset ? undefined : certsNextToken
			})
		);
		certs = reset ? (resp.CertificatesInfo ?? []) : [...certs, ...(resp.CertificatesInfo ?? [])];
		certsNextToken = resp.NextToken;
	}

	async function fetchEventTopics(): Promise<void> {
		const resp = await client().send(
			new DescribeEventTopicsCommand({ DirectoryId: selectedDirectoryId || undefined })
		);
		eventTopics = resp.EventTopics ?? [];
	}

	async function fetchDcs(reset: boolean): Promise<void> {
		if (!selectedDirectoryId) {
			dcs = [];
			dcsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new DescribeDomainControllersCommand({
				DirectoryId: selectedDirectoryId,
				NextToken: reset ? undefined : dcsNextToken
			})
		);
		dcs = reset ? (resp.DomainControllers ?? []) : [...dcs, ...(resp.DomainControllers ?? [])];
		dcsNextToken = resp.NextToken;
	}

	async function fetchRegions(reset: boolean): Promise<void> {
		if (!selectedDirectoryId) {
			regionsList = [];
			regionsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new DescribeRegionsCommand({
				DirectoryId: selectedDirectoryId,
				NextToken: reset ? undefined : regionsNextToken
			})
		);
		regionsList = reset ? (resp.RegionsDescription ?? []) : [...regionsList, ...(resp.RegionsDescription ?? [])];
		regionsNextToken = resp.NextToken;
	}

	async function fetchSharedDirs(reset: boolean): Promise<void> {
		if (!selectedDirectoryId) {
			sharedDirs = [];
			sharedDirsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new DescribeSharedDirectoriesCommand({
				OwnerDirectoryId: selectedDirectoryId,
				NextToken: reset ? undefined : sharedDirsNextToken
			})
		);
		sharedDirs = reset
			? (resp.SharedDirectories ?? [])
			: [...sharedDirs, ...(resp.SharedDirectories ?? [])];
		sharedDirsNextToken = resp.NextToken;
	}

	async function fetchAssessments(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListADAssessmentsCommand({
				DirectoryId: selectedDirectoryId || undefined,
				NextToken: reset ? undefined : assessmentsNextToken
			})
		);
		assessments = reset ? (resp.Assessments ?? []) : [...assessments, ...(resp.Assessments ?? [])];
		assessmentsNextToken = resp.NextToken;
	}

	async function fetchSettings(reset: boolean): Promise<void> {
		if (!selectedDirectoryId) {
			settingsList = [];
			settingsNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new DescribeSettingsCommand({
				DirectoryId: selectedDirectoryId,
				NextToken: reset ? undefined : settingsNextToken
			})
		);
		settingsList = reset ? (resp.SettingEntries ?? []) : [...settingsList, ...(resp.SettingEntries ?? [])];
		settingsNextToken = resp.NextToken;
	}

	const tabLoader = createTabLoader<TabId>({
		directories: () => fetchDirs(true).catch(rethrowDescribed),
		snapshots: () => fetchSnaps(true).catch(rethrowDescribed),
		trusts: () => fetchTrusts(true).catch(rethrowDescribed),
		conditionalForwarders: () => fetchCfwds().catch(rethrowDescribed),
		logSubscriptions: () => fetchLogSubs(true).catch(rethrowDescribed),
		ipRoutes: () => fetchIpRoutes(true).catch(rethrowDescribed),
		schemaExtensions: () => fetchSchemaExts(true).catch(rethrowDescribed),
		certificates: () => fetchCerts(true).catch(rethrowDescribed),
		eventTopics: () => fetchEventTopics().catch(rethrowDescribed),
		domainControllers: () => fetchDcs(true).catch(rethrowDescribed),
		regions: () => fetchRegions(true).catch(rethrowDescribed),
		sharedDirectories: () => fetchSharedDirs(true).catch(rethrowDescribed),
		adAssessments: () => fetchAssessments(true).catch(rethrowDescribed),
		settings: () => fetchSettings(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	function onDirectorySelect(id: string): void {
		selectedDirectoryId = id;
		if (directoryScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	// Directories is the parent resource for every other tab: on a region
	// change the previously selected directory ID belongs to the old region
	// and must not be reused, so reload directories first (which re-selects a
	// directory for the new region) before reloading whichever tab is active.
	onRegionChange(() => {
		selectedDirectoryId = '';
		dirs = [];
		dirsNextToken = undefined;
		void tabLoader.refresh('directories').then(() => {
			if (activeTab !== 'directories') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredDirs = $derived(
		dirs.filter((d) => matches(searchQuery, d.Name, d.DirectoryId, d.Type, d.Alias, d.ShortName))
	);
	const filteredSnaps = $derived(
		snaps.filter((s) => matches(searchQuery, s.SnapshotId, s.Name, s.DirectoryId, s.Status))
	);
	const filteredTrusts = $derived(
		trusts.filter((t) => matches(searchQuery, t.TrustId, t.RemoteDomainName, t.TrustType, t.TrustState))
	);
	const filteredCfwds = $derived(
		cfwds.filter((f) => matches(searchQuery, f.RemoteDomainName, ...(f.DnsIpAddrs ?? [])))
	);
	const filteredLogSubs = $derived(
		logSubs.filter((l) => matches(searchQuery, l.DirectoryId, l.LogGroupName))
	);
	const filteredIpRoutes = $derived(
		ipRoutesList.filter((r) => matches(searchQuery, r.CidrIp, r.CidrIpv6, r.IpRouteStatusMsg, r.Description))
	);
	const filteredSchemaExts = $derived(
		schemaExts.filter((s) =>
			matches(searchQuery, s.SchemaExtensionId, s.Description, s.SchemaExtensionStatus)
		)
	);
	const filteredCerts = $derived(
		certs.filter((c) => matches(searchQuery, c.CertificateId, c.CommonName, c.State, c.Type))
	);
	const filteredEventTopics = $derived(
		eventTopics.filter((t) => matches(searchQuery, t.TopicName, t.TopicArn, t.Status))
	);
	const filteredDcs = $derived(
		dcs.filter((d) =>
			matches(searchQuery, d.DomainControllerId, d.DnsIpAddr, d.AvailabilityZone, d.Status)
		)
	);
	const filteredRegions = $derived(
		regionsList.filter((r) => matches(searchQuery, r.RegionName, r.RegionType, r.Status))
	);
	const filteredSharedDirs = $derived(
		sharedDirs.filter((s) =>
			matches(searchQuery, s.SharedDirectoryId, s.SharedAccountId, s.ShareMethod, s.ShareStatus)
		)
	);
	const filteredAssessments = $derived(
		assessments.filter((a) => matches(searchQuery, a.AssessmentId, a.DnsName, a.Status, a.DirectoryId))
	);
	const filteredSettings = $derived(
		settingsList.filter((s) => matches(searchQuery, s.Name, s.Type, s.AppliedValue, s.RequestStatus))
	);

	async function loadMoreDirs(): Promise<void> {
		loadingMoreDirs = true;
		try {
			await fetchDirs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDirs = false;
		}
	}
	async function loadMoreSnaps(): Promise<void> {
		loadingMoreSnaps = true;
		try {
			await fetchSnaps(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSnaps = false;
		}
	}
	async function loadMoreTrusts(): Promise<void> {
		loadingMoreTrusts = true;
		try {
			await fetchTrusts(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTrusts = false;
		}
	}
	async function loadMoreLogSubs(): Promise<void> {
		loadingMoreLogSubs = true;
		try {
			await fetchLogSubs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreLogSubs = false;
		}
	}
	async function loadMoreIpRoutes(): Promise<void> {
		loadingMoreIpRoutes = true;
		try {
			await fetchIpRoutes(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreIpRoutes = false;
		}
	}
	async function loadMoreSchemaExts(): Promise<void> {
		loadingMoreSchemaExts = true;
		try {
			await fetchSchemaExts(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSchemaExts = false;
		}
	}
	async function loadMoreCerts(): Promise<void> {
		loadingMoreCerts = true;
		try {
			await fetchCerts(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCerts = false;
		}
	}
	async function loadMoreDcs(): Promise<void> {
		loadingMoreDcs = true;
		try {
			await fetchDcs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreDcs = false;
		}
	}
	async function loadMoreRegions(): Promise<void> {
		loadingMoreRegions = true;
		try {
			await fetchRegions(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreRegions = false;
		}
	}
	async function loadMoreSharedDirs(): Promise<void> {
		loadingMoreSharedDirs = true;
		try {
			await fetchSharedDirs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSharedDirs = false;
		}
	}
	async function loadMoreAssessments(): Promise<void> {
		loadingMoreAssessments = true;
		try {
			await fetchAssessments(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreAssessments = false;
		}
	}
	async function loadMoreSettings(): Promise<void> {
		loadingMoreSettings = true;
		try {
			await fetchSettings(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSettings = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// ==================== Directories: create (3 real operations) / delete / update / detail ====================
	//
	// Directory Service models three distinct creation operations for what
	// all end up as rows in the same DescribeDirectories list:
	// CreateDirectory (Simple AD), CreateMicrosoftAD (Managed Microsoft AD),
	// and ConnectDirectory (AD Connector, for a self-managed directory). A
	// fourth, CreateHybridAD, also creates a Directory row but requires a
	// successful AD Assessment first -- it is exposed from the AD
	// Assessments tab (next to a SUCCESS-status assessment) rather than here,
	// since its only real input is {AssessmentId, SecretArn, Tags}, not the
	// Name/Password/VpcSettings shape the other three share.

	type NewDirKind = 'simple' | 'microsoftad' | 'connector';

	let createDirModal = $state<Modal | null>(null);
	let creatingDir = $state(false);
	let createDirError = $state<string | null>(null);
	let newDirKind = $state<NewDirKind>('simple');
	let newDirName = $state('');
	let newDirShortName = $state('');
	let newDirDescription = $state('');
	let newDirPassword = $state('');
	let newDirSize = $state<'Small' | 'Large'>('Small');
	let newDirEdition = $state<'Enterprise' | 'Standard'>('Enterprise');
	let newDirVpcId = $state('');
	let newDirSubnetIds = $state('');
	let newDirConnectUserName = $state('');
	let newDirConnectDnsIps = $state('');

	function openCreateDirModal(): void {
		createDirError = null;
		newDirKind = 'simple';
		newDirName = '';
		newDirShortName = '';
		newDirDescription = '';
		newDirPassword = '';
		newDirSize = 'Small';
		newDirEdition = 'Enterprise';
		newDirVpcId = '';
		newDirSubnetIds = '';
		newDirConnectUserName = '';
		newDirConnectDnsIps = '';
		createDirModal?.open();
	}

	async function submitCreateDir(): Promise<void> {
		if (!newDirName || !newDirPassword) {
			createDirError = 'Name and password are required.';
			return;
		}
		const subnetIds = parseCommaList(newDirSubnetIds);
		if (newDirKind !== 'simple' && (!newDirVpcId || subnetIds.length === 0)) {
			createDirError = 'VPC ID and at least one subnet are required.';
			return;
		}
		if (newDirKind === 'connector' && !newDirConnectUserName) {
			createDirError = 'Self-managed account user name is required for AD Connector.';
			return;
		}
		creatingDir = true;
		createDirError = null;
		try {
			if (newDirKind === 'simple') {
				await client().send(
					new CreateDirectoryCommand({
						Name: newDirName,
						ShortName: newDirShortName || undefined,
						Description: newDirDescription || undefined,
						Password: newDirPassword,
						Size: newDirSize,
						VpcSettings:
							newDirVpcId && subnetIds.length > 0 ? { VpcId: newDirVpcId, SubnetIds: subnetIds } : undefined
					})
				);
			} else if (newDirKind === 'microsoftad') {
				await client().send(
					new CreateMicrosoftADCommand({
						Name: newDirName,
						ShortName: newDirShortName || undefined,
						Description: newDirDescription || undefined,
						Password: newDirPassword,
						Edition: newDirEdition,
						VpcSettings: { VpcId: newDirVpcId, SubnetIds: subnetIds }
					})
				);
			} else {
				await client().send(
					new ConnectDirectoryCommand({
						Name: newDirName,
						ShortName: newDirShortName || undefined,
						Description: newDirDescription || undefined,
						Password: newDirPassword,
						Size: newDirSize,
						ConnectSettings: {
							VpcId: newDirVpcId,
							SubnetIds: subnetIds,
							CustomerUserName: newDirConnectUserName,
							CustomerDnsIps: parseCommaList(newDirConnectDnsIps)
						}
					})
				);
			}
			toast.success('Directory creation started');
			createDirModal?.close();
			await tabLoader.refresh('directories');
		} catch (e) {
			const msg = describeError(e);
			createDirError = msg;
			toast.error(msg);
		} finally {
			creatingDir = false;
		}
	}

	async function handleDeleteDir(d: DirectoryDescription): Promise<void> {
		if (!d.DirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete directory',
			message: `Delete directory ${d.Name ?? d.DirectoryId}? This cascades all of its dependent resources (snapshots, trusts, conditional forwarders, etc.).`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteDirectoryCommand({ DirectoryId: d.DirectoryId }));
			toast.success('Directory deletion started');
			if (selectedDirectoryId === d.DirectoryId) {
				selectedDirectoryId = '';
			}
			await tabLoader.refresh('directories');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// UpdateDirectorySetup is the real API's directory-level "update":
	// UpdateType selects which infrastructure dimension to change (OS
	// version, network type, or directory size). The backend
	// (services/directoryservice/handler_settings.go) validates DirectoryId
	// + UpdateType and accepts CreateSnapshotBeforeUpdate, but does not act
	// on the optional OSUpdateSettings/DirectorySizeUpdateSettings/
	// NetworkUpdateSettings sub-objects, so this form only sends the fields
	// that are actually load-bearing against this backend.
	let updateDirModal = $state<Modal | null>(null);
	let updatingDir = $state(false);
	let updateDirError = $state<string | null>(null);
	let updateDirTarget = $state<DirectoryDescription | null>(null);
	let updateDirType = $state<'OS' | 'NETWORK' | 'SIZE'>('OS');
	let updateDirSnapshotFirst = $state(false);

	function openUpdateDirModal(d: DirectoryDescription): void {
		updateDirTarget = d;
		updateDirType = 'OS';
		updateDirSnapshotFirst = false;
		updateDirError = null;
		updateDirModal?.open();
	}

	async function submitUpdateDir(): Promise<void> {
		if (!updateDirTarget?.DirectoryId) return;
		updatingDir = true;
		updateDirError = null;
		try {
			await client().send(
				new UpdateDirectorySetupCommand({
					DirectoryId: updateDirTarget.DirectoryId,
					UpdateType: updateDirType,
					CreateSnapshotBeforeUpdate: updateDirSnapshotFirst
				})
			);
			toast.success('Directory update started');
			updateDirModal?.close();
			await tabLoader.refresh('directories');
		} catch (e) {
			const msg = describeError(e);
			updateDirError = msg;
			toast.error(msg);
		} finally {
			updatingDir = false;
		}
	}

	// DescribeDirectories already returns the complete DirectoryDescription
	// shape (there is no separate "GetDirectory" single-resource operation),
	// so the detail modal reuses the list row directly with no extra API
	// call.
	let dirDetailModal = $state<Modal | null>(null);
	let viewedDir = $state<DirectoryDescription | null>(null);

	function openDirDetail(d: DirectoryDescription): void {
		viewedDir = d;
		dirDetailModal?.open();
	}

	// ==================== Snapshots: create / delete / restore / detail ====================
	//
	// No update/rename operation exists for a manual snapshot in the real
	// API. RestoreFromSnapshot is the closest real mutation available (it
	// mutates the DIRECTORY, restoring it to the snapshot's state) and is
	// exposed as its own row action rather than folded into "update", since
	// it doesn't change anything about the snapshot resource itself.

	let createSnapModal = $state<Modal | null>(null);
	let creatingSnap = $state(false);
	let createSnapError = $state<string | null>(null);
	let newSnapName = $state('');

	function openCreateSnapModal(): void {
		createSnapError = selectedDirectoryId ? null : 'Select a directory first.';
		newSnapName = '';
		createSnapModal?.open();
	}

	async function submitCreateSnap(): Promise<void> {
		if (!selectedDirectoryId) {
			createSnapError = 'Select a directory first.';
			return;
		}
		creatingSnap = true;
		createSnapError = null;
		try {
			await client().send(
				new CreateSnapshotCommand({ DirectoryId: selectedDirectoryId, Name: newSnapName || undefined })
			);
			toast.success('Snapshot creation started');
			createSnapModal?.close();
			await tabLoader.refresh('snapshots');
		} catch (e) {
			const msg = describeError(e);
			createSnapError = msg;
			toast.error(msg);
		} finally {
			creatingSnap = false;
		}
	}

	async function handleDeleteSnap(s: Snapshot): Promise<void> {
		if (!s.SnapshotId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete snapshot',
			message: `Delete snapshot ${s.Name || s.SnapshotId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteSnapshotCommand({ SnapshotId: s.SnapshotId }));
			toast.success('Snapshot deleted');
			await tabLoader.refresh('snapshots');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleRestoreSnap(s: Snapshot): Promise<void> {
		if (!s.SnapshotId) return;
		const confirmed = await confirmDestructive({
			title: 'Restore from snapshot',
			message: `Restore directory ${s.DirectoryId} to snapshot ${s.Name || s.SnapshotId}? This overwrites the directory's current state.`
		});
		if (!confirmed) return;
		try {
			await client().send(new RestoreFromSnapshotCommand({ SnapshotId: s.SnapshotId }));
			toast.success('Restore started');
			await tabLoader.refresh('snapshots');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let snapDetailModal = $state<Modal | null>(null);
	let viewedSnap = $state<Snapshot | null>(null);

	function openSnapDetail(s: Snapshot): void {
		viewedSnap = s;
		snapDetailModal?.open();
	}

	// ==================== Trusts: create / delete / update / verify / detail ====================

	let createTrustModal = $state<Modal | null>(null);
	let creatingTrust = $state(false);
	let createTrustError = $state<string | null>(null);
	let newTrustRemoteDomain = $state('');
	let newTrustPassword = $state('');
	let newTrustDirection = $state<'One-Way: Outgoing' | 'One-Way: Incoming' | 'Two-Way'>('Two-Way');
	let newTrustType = $state<'Forest' | 'External'>('Forest');

	function openCreateTrustModal(): void {
		createTrustError = selectedDirectoryId ? null : 'Select a directory first.';
		newTrustRemoteDomain = '';
		newTrustPassword = '';
		newTrustDirection = 'Two-Way';
		newTrustType = 'Forest';
		createTrustModal?.open();
	}

	async function submitCreateTrust(): Promise<void> {
		if (!selectedDirectoryId) {
			createTrustError = 'Select a directory first.';
			return;
		}
		if (!newTrustRemoteDomain || !newTrustPassword) {
			createTrustError = 'Remote domain name and trust password are required.';
			return;
		}
		creatingTrust = true;
		createTrustError = null;
		try {
			await client().send(
				new CreateTrustCommand({
					DirectoryId: selectedDirectoryId,
					RemoteDomainName: newTrustRemoteDomain,
					TrustPassword: newTrustPassword,
					TrustDirection: newTrustDirection,
					TrustType: newTrustType
				})
			);
			toast.success('Trust creation started');
			createTrustModal?.close();
			await tabLoader.refresh('trusts');
		} catch (e) {
			const msg = describeError(e);
			createTrustError = msg;
			toast.error(msg);
		} finally {
			creatingTrust = false;
		}
	}

	async function handleDeleteTrust(t: Trust): Promise<void> {
		if (!t.TrustId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete trust',
			message: `Delete trust with ${t.RemoteDomainName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTrustCommand({ TrustId: t.TrustId }));
			toast.success('Trust deletion started');
			await tabLoader.refresh('trusts');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleVerifyTrust(t: Trust): Promise<void> {
		if (!t.TrustId) return;
		try {
			await client().send(new VerifyTrustCommand({ TrustId: t.TrustId }));
			toast.success('Trust verification started');
			await tabLoader.refresh('trusts');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let updateTrustModal = $state<Modal | null>(null);
	let updatingTrust = $state(false);
	let updateTrustError = $state<string | null>(null);
	let updateTrustTarget = $state<Trust | null>(null);
	let updateTrustSelectiveAuth = $state<'Enabled' | 'Disabled'>('Disabled');

	function openUpdateTrustModal(t: Trust): void {
		updateTrustTarget = t;
		updateTrustSelectiveAuth = (t.SelectiveAuth as 'Enabled' | 'Disabled') ?? 'Disabled';
		updateTrustError = null;
		updateTrustModal?.open();
	}

	async function submitUpdateTrust(): Promise<void> {
		if (!updateTrustTarget?.TrustId) return;
		updatingTrust = true;
		updateTrustError = null;
		try {
			await client().send(
				new UpdateTrustCommand({
					TrustId: updateTrustTarget.TrustId,
					SelectiveAuth: updateTrustSelectiveAuth
				})
			);
			toast.success('Trust updated');
			updateTrustModal?.close();
			await tabLoader.refresh('trusts');
		} catch (e) {
			const msg = describeError(e);
			updateTrustError = msg;
			toast.error(msg);
		} finally {
			updatingTrust = false;
		}
	}

	let trustDetailModal = $state<Modal | null>(null);
	let viewedTrust = $state<Trust | null>(null);

	function openTrustDetail(t: Trust): void {
		viewedTrust = t;
		trustDetailModal?.open();
	}

	// ==================== Conditional Forwarders: create / delete / update / detail ====================
	// Keyed by RemoteDomainName (there is no separate ID); scoped strictly to
	// the selected directory since DescribeConditionalForwardersRequest.DirectoryId
	// is required, not optional, in the real API.

	let createCfwdModal = $state<Modal | null>(null);
	let creatingCfwd = $state(false);
	let createCfwdError = $state<string | null>(null);
	let newCfwdDomain = $state('');
	let newCfwdDnsIps = $state('');

	function openCreateCfwdModal(): void {
		createCfwdError = selectedDirectoryId ? null : 'Select a directory first.';
		newCfwdDomain = '';
		newCfwdDnsIps = '';
		createCfwdModal?.open();
	}

	async function submitCreateCfwd(): Promise<void> {
		if (!selectedDirectoryId) {
			createCfwdError = 'Select a directory first.';
			return;
		}
		if (!newCfwdDomain) {
			createCfwdError = 'Remote domain name is required.';
			return;
		}
		creatingCfwd = true;
		createCfwdError = null;
		try {
			await client().send(
				new CreateConditionalForwarderCommand({
					DirectoryId: selectedDirectoryId,
					RemoteDomainName: newCfwdDomain,
					DnsIpAddrs: parseCommaList(newCfwdDnsIps)
				})
			);
			toast.success('Conditional forwarder created');
			createCfwdModal?.close();
			await tabLoader.refresh('conditionalForwarders');
		} catch (e) {
			const msg = describeError(e);
			createCfwdError = msg;
			toast.error(msg);
		} finally {
			creatingCfwd = false;
		}
	}

	async function handleDeleteCfwd(f: ConditionalForwarder): Promise<void> {
		if (!f.RemoteDomainName || !selectedDirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete conditional forwarder',
			message: `Delete conditional forwarder for ${f.RemoteDomainName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteConditionalForwarderCommand({
					DirectoryId: selectedDirectoryId,
					RemoteDomainName: f.RemoteDomainName
				})
			);
			toast.success('Conditional forwarder deleted');
			await tabLoader.refresh('conditionalForwarders');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let editCfwdModal = $state<Modal | null>(null);
	let editingCfwd = $state<ConditionalForwarder | null>(null);
	let editingCfwdDnsIps = $state('');
	let savingCfwd = $state(false);
	let editCfwdError = $state<string | null>(null);

	function openEditCfwdModal(f: ConditionalForwarder): void {
		editingCfwd = f;
		editingCfwdDnsIps = (f.DnsIpAddrs ?? []).join(', ');
		editCfwdError = null;
		editCfwdModal?.open();
	}

	async function submitEditCfwd(): Promise<void> {
		if (!editingCfwd?.RemoteDomainName || !selectedDirectoryId) return;
		savingCfwd = true;
		editCfwdError = null;
		try {
			await client().send(
				new UpdateConditionalForwarderCommand({
					DirectoryId: selectedDirectoryId,
					RemoteDomainName: editingCfwd.RemoteDomainName,
					DnsIpAddrs: parseCommaList(editingCfwdDnsIps)
				})
			);
			toast.success('Conditional forwarder updated');
			editCfwdModal?.close();
			await tabLoader.refresh('conditionalForwarders');
		} catch (e) {
			const msg = describeError(e);
			editCfwdError = msg;
			toast.error(msg);
		} finally {
			savingCfwd = false;
		}
	}

	let cfwdDetailModal = $state<Modal | null>(null);
	let viewedCfwd = $state<ConditionalForwarder | null>(null);

	function openCfwdDetail(f: ConditionalForwarder): void {
		viewedCfwd = f;
		cfwdDetailModal?.open();
	}

	// ==================== Log Subscriptions: create / delete / detail ====================
	// No update operation exists -- LogGroupName is the subscription's only
	// real content, and changing it means deleting and recreating.

	let createLogSubModal = $state<Modal | null>(null);
	let creatingLogSub = $state(false);
	let createLogSubError = $state<string | null>(null);
	let newLogGroupName = $state('');

	function openCreateLogSubModal(): void {
		createLogSubError = selectedDirectoryId ? null : 'Select a directory first.';
		newLogGroupName = '';
		createLogSubModal?.open();
	}

	async function submitCreateLogSub(): Promise<void> {
		if (!selectedDirectoryId) {
			createLogSubError = 'Select a directory first.';
			return;
		}
		if (!newLogGroupName) {
			createLogSubError = 'Log group name is required.';
			return;
		}
		creatingLogSub = true;
		createLogSubError = null;
		try {
			await client().send(
				new CreateLogSubscriptionCommand({
					DirectoryId: selectedDirectoryId,
					LogGroupName: newLogGroupName
				})
			);
			toast.success('Log subscription created');
			createLogSubModal?.close();
			await tabLoader.refresh('logSubscriptions');
		} catch (e) {
			const msg = describeError(e);
			createLogSubError = msg;
			toast.error(msg);
		} finally {
			creatingLogSub = false;
		}
	}

	async function handleDeleteLogSub(l: LogSubscription): Promise<void> {
		if (!l.DirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete log subscription',
			message: `Delete the log subscription for directory ${l.DirectoryId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteLogSubscriptionCommand({ DirectoryId: l.DirectoryId }));
			toast.success('Log subscription deleted');
			await tabLoader.refresh('logSubscriptions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let logSubDetailModal = $state<Modal | null>(null);
	let viewedLogSub = $state<LogSubscription | null>(null);

	function openLogSubDetail(l: LogSubscription): void {
		viewedLogSub = l;
		logSubDetailModal?.open();
	}

	// ==================== IP Routes: add (create) / remove (delete) / detail ====================
	// No update operation -- an existing CIDR route is immutable; changing it
	// means removing the old one and adding the new one.

	let addRouteModal = $state<Modal | null>(null);
	let addingRoute = $state(false);
	let addRouteError = $state<string | null>(null);
	let newRouteCidr = $state('');
	let newRouteDescription = $state('');

	function openAddRouteModal(): void {
		addRouteError = selectedDirectoryId ? null : 'Select a directory first.';
		newRouteCidr = '';
		newRouteDescription = '';
		addRouteModal?.open();
	}

	async function submitAddRoute(): Promise<void> {
		if (!selectedDirectoryId) {
			addRouteError = 'Select a directory first.';
			return;
		}
		if (!newRouteCidr) {
			addRouteError = 'CIDR address block is required.';
			return;
		}
		addingRoute = true;
		addRouteError = null;
		try {
			await client().send(
				new AddIpRoutesCommand({
					DirectoryId: selectedDirectoryId,
					IpRoutes: [{ CidrIp: newRouteCidr, Description: newRouteDescription || undefined }]
				})
			);
			toast.success('IP route added');
			addRouteModal?.close();
			await tabLoader.refresh('ipRoutes');
		} catch (e) {
			const msg = describeError(e);
			addRouteError = msg;
			toast.error(msg);
		} finally {
			addingRoute = false;
		}
	}

	async function handleRemoveRoute(r: IpRouteInfo): Promise<void> {
		if (!r.CidrIp || !selectedDirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Remove IP route',
			message: `Remove IP route ${r.CidrIp}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new RemoveIpRoutesCommand({ DirectoryId: selectedDirectoryId, CidrIps: [r.CidrIp] })
			);
			toast.success('IP route removed');
			await tabLoader.refresh('ipRoutes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let routeDetailModal = $state<Modal | null>(null);
	let viewedRoute = $state<IpRouteInfo | null>(null);

	function openRouteDetail(r: IpRouteInfo): void {
		viewedRoute = r;
		routeDetailModal?.open();
	}

	// ==================== Schema Extensions: start (create) / cancel / detail ====================
	// No update, and no true "delete" once a schema extension has finished
	// applying (schema changes are permanent, matching real Active Directory
	// semantics) -- CancelSchemaExtension only works while the extension is
	// still in progress, so it is exposed as the row's destructive action
	// with that caveat surfaced in the confirmation text.

	let startSchemaModal = $state<Modal | null>(null);
	let startingSchema = $state(false);
	let startSchemaError = $state<string | null>(null);
	let newSchemaDescription = $state('');
	let newSchemaLdif = $state('');
	let newSchemaSnapshotFirst = $state(true);

	function openStartSchemaModal(): void {
		startSchemaError = selectedDirectoryId ? null : 'Select a directory first.';
		newSchemaDescription = '';
		newSchemaLdif = '';
		newSchemaSnapshotFirst = true;
		startSchemaModal?.open();
	}

	async function submitStartSchema(): Promise<void> {
		if (!selectedDirectoryId) {
			startSchemaError = 'Select a directory first.';
			return;
		}
		if (!newSchemaLdif || !newSchemaDescription) {
			startSchemaError = 'Description and LDIF content are required.';
			return;
		}
		startingSchema = true;
		startSchemaError = null;
		try {
			await client().send(
				new StartSchemaExtensionCommand({
					DirectoryId: selectedDirectoryId,
					CreateSnapshotBeforeSchemaExtension: newSchemaSnapshotFirst,
					LdifContent: newSchemaLdif,
					Description: newSchemaDescription
				})
			);
			toast.success('Schema extension started');
			startSchemaModal?.close();
			await tabLoader.refresh('schemaExtensions');
		} catch (e) {
			const msg = describeError(e);
			startSchemaError = msg;
			toast.error(msg);
		} finally {
			startingSchema = false;
		}
	}

	async function handleCancelSchema(s: SchemaExtensionInfo): Promise<void> {
		if (!s.SchemaExtensionId || !selectedDirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Cancel schema extension',
			message: `Cancel schema extension ${s.SchemaExtensionId}? This only has an effect while it is still in progress.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new CancelSchemaExtensionCommand({
					DirectoryId: selectedDirectoryId,
					SchemaExtensionId: s.SchemaExtensionId
				})
			);
			toast.success('Schema extension cancellation requested');
			await tabLoader.refresh('schemaExtensions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let schemaDetailModal = $state<Modal | null>(null);
	let viewedSchema = $state<SchemaExtensionInfo | null>(null);

	function openSchemaDetail(s: SchemaExtensionInfo): void {
		viewedSchema = s;
		schemaDetailModal?.open();
	}

	// ==================== Certificates: register (create) / deregister (delete) / describe (detail) ====================
	// No update -- a certificate is immutable once registered; replacing one
	// means deregistering and registering the new PEM data.

	let registerCertModal = $state<Modal | null>(null);
	let registeringCert = $state(false);
	let registerCertError = $state<string | null>(null);
	let newCertData = $state('');
	let newCertType = $state<'ClientLDAPS' | 'ClientCertAuth'>('ClientLDAPS');

	function openRegisterCertModal(): void {
		registerCertError = selectedDirectoryId ? null : 'Select a directory first.';
		newCertData = '';
		newCertType = 'ClientLDAPS';
		registerCertModal?.open();
	}

	async function submitRegisterCert(): Promise<void> {
		if (!selectedDirectoryId) {
			registerCertError = 'Select a directory first.';
			return;
		}
		if (!newCertData) {
			registerCertError = 'Certificate PEM data is required.';
			return;
		}
		registeringCert = true;
		registerCertError = null;
		try {
			await client().send(
				new RegisterCertificateCommand({
					DirectoryId: selectedDirectoryId,
					CertificateData: newCertData,
					Type: newCertType
				})
			);
			toast.success('Certificate registered');
			registerCertModal?.close();
			await tabLoader.refresh('certificates');
		} catch (e) {
			const msg = describeError(e);
			registerCertError = msg;
			toast.error(msg);
		} finally {
			registeringCert = false;
		}
	}

	async function handleDeregisterCert(c: CertificateInfo): Promise<void> {
		if (!c.CertificateId || !selectedDirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Deregister certificate',
			message: `Deregister certificate ${c.CommonName || c.CertificateId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeregisterCertificateCommand({
					DirectoryId: selectedDirectoryId,
					CertificateId: c.CertificateId
				})
			);
			toast.success('Certificate deregistered');
			await tabLoader.refresh('certificates');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ListCertificates only returns the CertificateInfo summary shape (no
	// StateReason/ClientCertAuthSettings) -- DescribeCertificate returns the
	// full Certificate, so the detail modal fetches it on open.
	let certDetailModal = $state<Modal | null>(null);
	let viewedCert = $state<Certificate | CertificateInfo | null>(null);
	let certDetailLoading = $state(false);
	let certDetailError = $state<string | null>(null);

	async function openCertDetail(c: CertificateInfo): Promise<void> {
		viewedCert = c;
		certDetailError = null;
		certDetailModal?.open();
		if (!c.CertificateId || !selectedDirectoryId) return;
		certDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeCertificateCommand({
					DirectoryId: selectedDirectoryId,
					CertificateId: c.CertificateId
				})
			);
			viewedCert = resp.Certificate ?? c;
		} catch (e) {
			certDetailError = describeError(e);
		} finally {
			certDetailLoading = false;
		}
	}

	// ==================== Event Topics: register (create) / deregister (delete) / detail ====================
	// No update -- re-pointing to a different SNS topic means deregistering
	// the old topic name and registering the new one.

	let registerTopicModal = $state<Modal | null>(null);
	let registeringTopic = $state(false);
	let registerTopicError = $state<string | null>(null);
	let newTopicName = $state('');

	function openRegisterTopicModal(): void {
		registerTopicError = selectedDirectoryId ? null : 'Select a directory first.';
		newTopicName = '';
		registerTopicModal?.open();
	}

	async function submitRegisterTopic(): Promise<void> {
		if (!selectedDirectoryId) {
			registerTopicError = 'Select a directory first.';
			return;
		}
		if (!newTopicName) {
			registerTopicError = 'SNS topic name is required.';
			return;
		}
		registeringTopic = true;
		registerTopicError = null;
		try {
			await client().send(
				new RegisterEventTopicCommand({ DirectoryId: selectedDirectoryId, TopicName: newTopicName })
			);
			toast.success('Event topic registered');
			registerTopicModal?.close();
			await tabLoader.refresh('eventTopics');
		} catch (e) {
			const msg = describeError(e);
			registerTopicError = msg;
			toast.error(msg);
		} finally {
			registeringTopic = false;
		}
	}

	async function handleDeregisterTopic(t: EventTopic): Promise<void> {
		if (!t.TopicName || !t.DirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Deregister event topic',
			message: `Deregister event topic ${t.TopicName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeregisterEventTopicCommand({ DirectoryId: t.DirectoryId, TopicName: t.TopicName })
			);
			toast.success('Event topic deregistered');
			await tabLoader.refresh('eventTopics');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let topicDetailModal = $state<Modal | null>(null);
	let viewedTopic = $state<EventTopic | null>(null);

	function openTopicDetail(t: EventTopic): void {
		viewedTopic = t;
		topicDetailModal?.open();
	}

	// ==================== Domain Controllers: read-only list + detail; resize (update) ====================
	// The real API has no CreateDomainController/DeleteDomainController --
	// individual controllers are provisioned and torn down automatically.
	// The only real mutation is UpdateNumberOfDomainControllers, a
	// directory-level resize (not a per-controller op), exposed as a single
	// "Update controller count" action for the selected directory rather
	// than a per-row action.

	let resizeDcsModal = $state<Modal | null>(null);
	let resizingDcs = $state(false);
	let resizeDcsError = $state<string | null>(null);
	let newDesiredDcs = $state(2);

	function openResizeDcsModal(): void {
		resizeDcsError = selectedDirectoryId ? null : 'Select a directory first.';
		newDesiredDcs = selectedDirectory?.DesiredNumberOfDomainControllers ?? 2;
		resizeDcsModal?.open();
	}

	async function submitResizeDcs(): Promise<void> {
		if (!selectedDirectoryId) {
			resizeDcsError = 'Select a directory first.';
			return;
		}
		resizingDcs = true;
		resizeDcsError = null;
		try {
			await client().send(
				new UpdateNumberOfDomainControllersCommand({
					DirectoryId: selectedDirectoryId,
					DesiredNumber: newDesiredDcs
				})
			);
			toast.success('Domain controller resize started');
			resizeDcsModal?.close();
			await tabLoader.refresh('domainControllers');
		} catch (e) {
			const msg = describeError(e);
			resizeDcsError = msg;
			toast.error(msg);
		} finally {
			resizingDcs = false;
		}
	}

	let dcDetailModal = $state<Modal | null>(null);
	let viewedDc = $state<DomainController | null>(null);

	function openDcDetail(d: DomainController): void {
		viewedDc = d;
		dcDetailModal?.open();
	}

	// ==================== Regions: add (create) / remove (delete) / detail ====================
	// No update -- a replicated Region's VPC settings are fixed at add time;
	// changing them means removing the Region and adding it again.

	let addRegionModal = $state<Modal | null>(null);
	let addingRegion = $state(false);
	let addRegionError = $state<string | null>(null);
	let newRegionName = $state('');
	let newRegionVpcId = $state('');
	let newRegionSubnetIds = $state('');

	function openAddRegionModal(): void {
		addRegionError = selectedDirectoryId ? null : 'Select a directory first.';
		newRegionName = '';
		newRegionVpcId = '';
		newRegionSubnetIds = '';
		addRegionModal?.open();
	}

	async function submitAddRegion(): Promise<void> {
		if (!selectedDirectoryId) {
			addRegionError = 'Select a directory first.';
			return;
		}
		const subnetIds = parseCommaList(newRegionSubnetIds);
		if (!newRegionName || !newRegionVpcId || subnetIds.length === 0) {
			addRegionError = 'Region name, VPC ID, and at least one subnet are required.';
			return;
		}
		addingRegion = true;
		addRegionError = null;
		try {
			await client().send(
				new AddRegionCommand({
					DirectoryId: selectedDirectoryId,
					RegionName: newRegionName,
					VPCSettings: { VpcId: newRegionVpcId, SubnetIds: subnetIds }
				})
			);
			toast.success('Region replication started');
			addRegionModal?.close();
			await tabLoader.refresh('regions');
		} catch (e) {
			const msg = describeError(e);
			addRegionError = msg;
			toast.error(msg);
		} finally {
			addingRegion = false;
		}
	}

	async function handleRemoveRegion(r: RegionDescription): Promise<void> {
		if (!selectedDirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Remove Region replication',
			message: `Remove replication to ${r.RegionName}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new RemoveRegionCommand({ DirectoryId: selectedDirectoryId }));
			toast.success('Region removal started');
			await tabLoader.refresh('regions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let regionDetailModal = $state<Modal | null>(null);
	let viewedRegion = $state<RegionDescription | null>(null);

	function openRegionDetail(r: RegionDescription): void {
		viewedRegion = r;
		regionDetailModal?.open();
	}

	// ==================== Shared Directories: share (create) / unshare (delete) / accept / reject / detail ====================
	// No general-purpose update op; ShareStatus only moves through
	// accept/reject state transitions, exposed as their own row actions.

	let shareDirModal = $state<Modal | null>(null);
	let sharingDir = $state(false);
	let shareDirError = $state<string | null>(null);
	let newShareTargetId = $state('');
	let newShareMethod = $state<'HANDSHAKE' | 'ORGANIZATIONS'>('HANDSHAKE');
	let newShareNotes = $state('');

	function openShareDirModal(): void {
		shareDirError = selectedDirectoryId ? null : 'Select a directory first.';
		newShareTargetId = '';
		newShareMethod = 'HANDSHAKE';
		newShareNotes = '';
		shareDirModal?.open();
	}

	async function submitShareDir(): Promise<void> {
		if (!selectedDirectoryId) {
			shareDirError = 'Select a directory first.';
			return;
		}
		if (!newShareTargetId) {
			shareDirError = 'Target AWS account ID is required.';
			return;
		}
		sharingDir = true;
		shareDirError = null;
		try {
			await client().send(
				new ShareDirectoryCommand({
					DirectoryId: selectedDirectoryId,
					ShareMethod: newShareMethod,
					ShareNotes: newShareNotes || undefined,
					ShareTarget: { Id: newShareTargetId, Type: 'ACCOUNT' }
				})
			);
			toast.success('Directory share created');
			shareDirModal?.close();
			await tabLoader.refresh('sharedDirectories');
		} catch (e) {
			const msg = describeError(e);
			shareDirError = msg;
			toast.error(msg);
		} finally {
			sharingDir = false;
		}
	}

	async function handleUnshareDir(s: SharedDirectory): Promise<void> {
		if (!s.SharedAccountId || !selectedDirectoryId) return;
		const confirmed = await confirmDestructive({
			title: 'Unshare directory',
			message: `Stop sharing with account ${s.SharedAccountId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new UnshareDirectoryCommand({
					DirectoryId: selectedDirectoryId,
					UnshareTarget: { Id: s.SharedAccountId, Type: 'ACCOUNT' }
				})
			);
			toast.success('Directory unshared');
			await tabLoader.refresh('sharedDirectories');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleAcceptShare(s: SharedDirectory): Promise<void> {
		if (!s.SharedDirectoryId) return;
		try {
			await client().send(new AcceptSharedDirectoryCommand({ SharedDirectoryId: s.SharedDirectoryId }));
			toast.success('Shared directory accepted');
			await tabLoader.refresh('sharedDirectories');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleRejectShare(s: SharedDirectory): Promise<void> {
		if (!s.SharedDirectoryId) return;
		try {
			await client().send(new RejectSharedDirectoryCommand({ SharedDirectoryId: s.SharedDirectoryId }));
			toast.success('Shared directory rejected');
			await tabLoader.refresh('sharedDirectories');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let shareDetailModal = $state<Modal | null>(null);
	let viewedShare = $state<SharedDirectory | null>(null);

	function openShareDetail(s: SharedDirectory): void {
		viewedShare = s;
		shareDetailModal?.open();
	}

	// ==================== AD Assessments: start (create) / delete / detail ====================
	// No update op. This backend requires DirectoryId on StartADAssessment
	// (services/directoryservice/handler_ad_assessments.go), unlike real AWS
	// which also supports a directory-less pre-creation assessment mode --
	// see the gopherstack-10hx PARITY.md notes -- so "select a directory
	// first" is a real, current requirement of this emulation, not just a UI
	// choice.

	let startAssessModal = $state<Modal | null>(null);
	let startingAssess = $state(false);
	let startAssessError = $state<string | null>(null);
	let newAssessDnsIps = $state('');
	let newAssessVpcId = $state('');
	let newAssessSubnetIds = $state('');
	let newAssessInstanceIds = $state('');

	function openStartAssessModal(): void {
		startAssessError = selectedDirectoryId ? null : 'Select a directory first.';
		newAssessDnsIps = '';
		newAssessVpcId = '';
		newAssessSubnetIds = '';
		newAssessInstanceIds = '';
		startAssessModal?.open();
	}

	async function submitStartAssess(): Promise<void> {
		if (!selectedDirectoryId) {
			startAssessError = 'Select a directory first.';
			return;
		}
		const dnsIps = parseCommaList(newAssessDnsIps);
		const subnetIds = parseCommaList(newAssessSubnetIds);
		const instanceIds = parseCommaList(newAssessInstanceIds);
		if (dnsIps.length === 0 || !newAssessVpcId || subnetIds.length === 0 || instanceIds.length === 0) {
			startAssessError =
				'Self-managed DNS IPs, VPC ID, at least one subnet, and at least one instance ID are required.';
			return;
		}
		startingAssess = true;
		startAssessError = null;
		try {
			await client().send(
				new StartADAssessmentCommand({
					DirectoryId: selectedDirectoryId,
					AssessmentConfiguration: {
						CustomerDnsIps: dnsIps,
						DnsName: selectedDirectory?.Name ?? '',
						VpcSettings: { VpcId: newAssessVpcId, SubnetIds: subnetIds },
						InstanceIds: instanceIds
					}
				})
			);
			toast.success('Directory assessment started');
			startAssessModal?.close();
			await tabLoader.refresh('adAssessments');
		} catch (e) {
			const msg = describeError(e);
			startAssessError = msg;
			toast.error(msg);
		} finally {
			startingAssess = false;
		}
	}

	async function handleDeleteAssess(a: AssessmentSummary): Promise<void> {
		if (!a.AssessmentId) return;
		const confirmed = await confirmDestructive({
			title: 'Delete assessment',
			message: `Delete assessment ${a.AssessmentId}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteADAssessmentCommand({ AssessmentId: a.AssessmentId }));
			toast.success('Assessment deleted');
			await tabLoader.refresh('adAssessments');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ListADAssessments only returns the AssessmentSummary subset --
	// DescribeADAssessment returns the full Assessment (plus per-domain-
	// controller AssessmentReports), so the detail modal fetches both on
	// open.
	let assessDetailModal = $state<Modal | null>(null);
	let viewedAssess = $state<Assessment | AssessmentSummary | null>(null);
	let viewedAssessReports = $state<AssessmentReport[]>([]);
	let assessDetailLoading = $state(false);
	let assessDetailError = $state<string | null>(null);

	async function openAssessDetail(a: AssessmentSummary): Promise<void> {
		viewedAssess = a;
		viewedAssessReports = [];
		assessDetailError = null;
		assessDetailModal?.open();
		if (!a.AssessmentId) return;
		assessDetailLoading = true;
		try {
			const resp = await client().send(new DescribeADAssessmentCommand({ AssessmentId: a.AssessmentId }));
			viewedAssess = resp.Assessment ?? a;
			viewedAssessReports = resp.AssessmentReports ?? [];
		} catch (e) {
			assessDetailError = describeError(e);
		} finally {
			assessDetailLoading = false;
		}
	}

	// ==================== Settings: update only ====================
	// The real API has no create/delete for an individual directory setting
	// -- AWS predefines the full set of configurable settings per directory
	// type (e.g. TLS_1_0, TLS_1_1), and DescribeSettings/UpdateSettings only
	// let a caller read and change the VALUE of an existing setting. There is
	// therefore no "create setting" or "delete setting" action here, only
	// per-row Update.

	let editSettingModal = $state<Modal | null>(null);
	let editingSetting = $state<SettingEntry | null>(null);
	let editingSettingValue = $state('');
	let savingSetting = $state(false);
	let editSettingError = $state<string | null>(null);

	function openEditSettingModal(s: SettingEntry): void {
		editingSetting = s;
		editingSettingValue = s.AppliedValue ?? '';
		editSettingError = null;
		editSettingModal?.open();
	}

	async function submitEditSetting(): Promise<void> {
		if (!editingSetting?.Name || !selectedDirectoryId) return;
		if (!editingSettingValue) {
			editSettingError = 'Value is required.';
			return;
		}
		savingSetting = true;
		editSettingError = null;
		try {
			await client().send(
				new UpdateSettingsCommand({
					DirectoryId: selectedDirectoryId,
					Settings: [{ Name: editingSetting.Name, Value: editingSettingValue }]
				})
			);
			toast.success('Setting updated');
			editSettingModal?.close();
			await tabLoader.refresh('settings');
		} catch (e) {
			const msg = describeError(e);
			editSettingError = msg;
			toast.error(msg);
		} finally {
			savingSetting = false;
		}
	}

	let settingDetailModal = $state<Modal | null>(null);
	let viewedSetting = $state<SettingEntry | null>(null);

	function openSettingDetail(s: SettingEntry): void {
		viewedSetting = s;
		settingDetailModal?.open();
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Network}
		title="AWS Directory Service"
		description="Managed Microsoft AD, AD Connector, Simple AD, and hybrid directories"
		onRefresh={handleRefresh}
		color="sky"
	>
		{#snippet actions()}
			{#if activeTab === 'directories'}
				<button
					onclick={openCreateDirModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create directory
				</button>
			{:else if activeTab === 'snapshots'}
				<button
					onclick={openCreateSnapModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create snapshot
				</button>
			{:else if activeTab === 'trusts'}
				<button
					onclick={openCreateTrustModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create trust
				</button>
			{:else if activeTab === 'conditionalForwarders'}
				<button
					onclick={openCreateCfwdModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create forwarder
				</button>
			{:else if activeTab === 'logSubscriptions'}
				<button
					onclick={openCreateLogSubModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create subscription
				</button>
			{:else if activeTab === 'ipRoutes'}
				<button
					onclick={openAddRouteModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Add IP route
				</button>
			{:else if activeTab === 'schemaExtensions'}
				<button
					onclick={openStartSchemaModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start extension
				</button>
			{:else if activeTab === 'certificates'}
				<button
					onclick={openRegisterCertModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Register certificate
				</button>
			{:else if activeTab === 'eventTopics'}
				<button
					onclick={openRegisterTopicModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Register topic
				</button>
			{:else if activeTab === 'domainControllers'}
				<button
					onclick={openResizeDcsModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Pencil class="w-4 h-4" /> Update controller count
				</button>
			{:else if activeTab === 'regions'}
				<button
					onclick={openAddRegionModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Add Region
				</button>
			{:else if activeTab === 'sharedDirectories'}
				<button
					onclick={openShareDirModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Share directory
				</button>
			{:else if activeTab === 'adAssessments'}
				<button
					onclick={openStartAssessModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-sky-600 text-white hover:bg-sky-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Start assessment
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="sky" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if directoryScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="directory-select" class="text-sm text-gray-500 dark:text-gray-400">Directory</label>
					<select
						id="directory-select"
						value={selectedDirectoryId}
						onchange={(e) => onDirectorySelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if optionalScopedTabs.includes(activeTab)}
							<option value="">All directories</option>
						{:else if dirs.length === 0}
							<option value="">No directories</option>
						{/if}
						{#each dirs as d (d.DirectoryId)}
							<option value={d.DirectoryId}>{d.Name} ({d.DirectoryId})</option>
						{/each}
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

			{#if activeTab === 'directories'}
				{#snippet dirStageCell(d: DirectoryDescription)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(d.Stage === 'Active')}">{d.Stage ?? '—'}</span>
				{/snippet}
				{#snippet dirLaunchCell(d: DirectoryDescription)}
					{formatDate(d.LaunchTime)}
				{/snippet}
				{#snippet dirActionsCell(d: DirectoryDescription)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openDirDetail(d)} title="View" aria-label="View directory {d.Name}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateDirModal(d)} title="Update" aria-label="Update directory {d.Name}" class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteDir(d)} title="Delete" aria-label="Delete directory {d.Name}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const dirColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'DirectoryId', label: 'ID' },
					{ key: 'Type', label: 'Type' },
					{ key: 'Stage', label: 'Stage', render: dirStageCell },
					{ key: 'LaunchTime', label: 'Launched', render: dirLaunchCell },
					{ key: 'actions', label: '', render: dirActionsCell }
				] as Column<DirectoryDescription>[]}
				<DataTable
					rows={filteredDirs}
					rowKey={(d) => d.DirectoryId ?? ''}
					columns={dirColumns}
					loading={tabLoader.isLoading('directories')}
					emptyMessage="No directories found"
				/>
				<LoadMore hasMore={!!dirsNextToken} loading={loadingMoreDirs} onLoadMore={loadMoreDirs} />
			{:else if activeTab === 'snapshots'}
				{#snippet snapStatusCell(s: Snapshot)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.Status === 'Completed')}">{s.Status ?? '—'}</span>
				{/snippet}
				{#snippet snapStartCell(s: Snapshot)}
					{formatDate(s.StartTime)}
				{/snippet}
				{#snippet snapActionsCell(s: Snapshot)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSnapDetail(s)} title="View" aria-label="View snapshot {s.SnapshotId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleRestoreSnap(s)} title="Restore" aria-label="Restore from snapshot {s.SnapshotId}" class="text-gray-400 hover:text-sky-500"><Check class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteSnap(s)} title="Delete" aria-label="Delete snapshot {s.SnapshotId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const snapColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'SnapshotId', label: 'ID' },
					{ key: 'DirectoryId', label: 'Directory' },
					{ key: 'Type', label: 'Type' },
					{ key: 'Status', label: 'Status', render: snapStatusCell },
					{ key: 'StartTime', label: 'Started', render: snapStartCell },
					{ key: 'actions', label: '', render: snapActionsCell }
				] as Column<Snapshot>[]}
				<DataTable
					rows={filteredSnaps}
					rowKey={(s) => s.SnapshotId ?? ''}
					columns={snapColumns}
					loading={tabLoader.isLoading('snapshots')}
					emptyMessage="No snapshots found"
				/>
				<LoadMore hasMore={!!snapsNextToken} loading={loadingMoreSnaps} onLoadMore={loadMoreSnaps} />
			{:else if activeTab === 'trusts'}
				{#snippet trustStateCell(t: Trust)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(t.TrustState === 'Verified' || t.TrustState === 'Created')}">{t.TrustState ?? '—'}</span>
				{/snippet}
				{#snippet trustActionsCell(t: Trust)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTrustDetail(t)} title="View" aria-label="View trust {t.TrustId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateTrustModal(t)} title="Update" aria-label="Update trust {t.TrustId}" class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleVerifyTrust(t)} title="Verify" aria-label="Verify trust {t.TrustId}" class="text-gray-400 hover:text-sky-500"><Check class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteTrust(t)} title="Delete" aria-label="Delete trust {t.TrustId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const trustColumns = [
					{ key: 'RemoteDomainName', label: 'Remote Domain' },
					{ key: 'DirectoryId', label: 'Directory' },
					{ key: 'TrustType', label: 'Type' },
					{ key: 'TrustDirection', label: 'Direction' },
					{ key: 'TrustState', label: 'State', render: trustStateCell },
					{ key: 'actions', label: '', render: trustActionsCell }
				] as Column<Trust>[]}
				<DataTable
					rows={filteredTrusts}
					rowKey={(t) => t.TrustId ?? ''}
					columns={trustColumns}
					loading={tabLoader.isLoading('trusts')}
					emptyMessage="No trusts found"
				/>
				<LoadMore hasMore={!!trustsNextToken} loading={loadingMoreTrusts} onLoadMore={loadMoreTrusts} />
			{:else if activeTab === 'conditionalForwarders'}
				{#snippet cfwdDnsCell(f: ConditionalForwarder)}
					{(f.DnsIpAddrs ?? []).join(', ') || '—'}
				{/snippet}
				{#snippet cfwdActionsCell(f: ConditionalForwarder)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openCfwdDetail(f)} title="View" aria-label="View forwarder {f.RemoteDomainName}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openEditCfwdModal(f)} title="Edit" aria-label="Edit forwarder {f.RemoteDomainName}" class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteCfwd(f)} title="Delete" aria-label="Delete forwarder {f.RemoteDomainName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const cfwdColumns = [
					{ key: 'RemoteDomainName', label: 'Remote Domain' },
					{ key: 'DnsIpAddrs', label: 'DNS IPs', render: cfwdDnsCell },
					{ key: 'ReplicationScope', label: 'Replication Scope' },
					{ key: 'actions', label: '', render: cfwdActionsCell }
				] as Column<ConditionalForwarder>[]}
				<DataTable
					rows={filteredCfwds}
					rowKey={(f) => f.RemoteDomainName ?? ''}
					columns={cfwdColumns}
					loading={tabLoader.isLoading('conditionalForwarders')}
					emptyMessage={selectedDirectoryId ? 'No conditional forwarders found' : 'Select a directory to see its conditional forwarders'}
				/>
			{:else if activeTab === 'logSubscriptions'}
				{#snippet logSubCreatedCell(l: LogSubscription)}
					{formatDate(l.SubscriptionCreatedDateTime)}
				{/snippet}
				{#snippet logSubActionsCell(l: LogSubscription)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openLogSubDetail(l)} title="View" aria-label="View log subscription {l.DirectoryId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteLogSub(l)} title="Delete" aria-label="Delete log subscription {l.DirectoryId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const logSubColumns = [
					{ key: 'DirectoryId', label: 'Directory' },
					{ key: 'LogGroupName', label: 'Log Group' },
					{ key: 'SubscriptionCreatedDateTime', label: 'Created', render: logSubCreatedCell },
					{ key: 'actions', label: '', render: logSubActionsCell }
				] as Column<LogSubscription>[]}
				<DataTable
					rows={filteredLogSubs}
					rowKey={(l) => l.DirectoryId ?? ''}
					columns={logSubColumns}
					loading={tabLoader.isLoading('logSubscriptions')}
					emptyMessage="No log subscriptions found"
				/>
				<LoadMore hasMore={!!logSubsNextToken} loading={loadingMoreLogSubs} onLoadMore={loadMoreLogSubs} />
			{:else if activeTab === 'ipRoutes'}
				{#snippet routeStatusCell(r: IpRouteInfo)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(r.IpRouteStatusMsg === 'Added')}">{r.IpRouteStatusMsg ?? '—'}</span>
				{/snippet}
				{#snippet routeAddedCell(r: IpRouteInfo)}
					{formatDate(r.AddedDateTime)}
				{/snippet}
				{#snippet routeActionsCell(r: IpRouteInfo)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openRouteDetail(r)} title="View" aria-label="View route {r.CidrIp}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleRemoveRoute(r)} title="Remove" aria-label="Remove route {r.CidrIp}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const routeColumns = [
					{ key: 'CidrIp', label: 'CIDR' },
					{ key: 'Description', label: 'Description' },
					{ key: 'IpRouteStatusMsg', label: 'Status', render: routeStatusCell },
					{ key: 'AddedDateTime', label: 'Added', render: routeAddedCell },
					{ key: 'actions', label: '', render: routeActionsCell }
				] as Column<IpRouteInfo>[]}
				<DataTable
					rows={filteredIpRoutes}
					rowKey={(r) => r.CidrIp ?? ''}
					columns={routeColumns}
					loading={tabLoader.isLoading('ipRoutes')}
					emptyMessage={selectedDirectoryId ? 'No IP routes found' : 'Select a directory to see its IP routes'}
				/>
				<LoadMore hasMore={!!ipRoutesNextToken} loading={loadingMoreIpRoutes} onLoadMore={loadMoreIpRoutes} />
			{:else if activeTab === 'schemaExtensions'}
				{#snippet schemaStatusCell(s: SchemaExtensionInfo)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.SchemaExtensionStatus === 'Completed')}">{s.SchemaExtensionStatus ?? '—'}</span>
				{/snippet}
				{#snippet schemaStartCell(s: SchemaExtensionInfo)}
					{formatDate(s.StartDateTime)}
				{/snippet}
				{#snippet schemaActionsCell(s: SchemaExtensionInfo)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSchemaDetail(s)} title="View" aria-label="View schema extension {s.SchemaExtensionId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleCancelSchema(s)} title="Cancel" aria-label="Cancel schema extension {s.SchemaExtensionId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const schemaColumns = [
					{ key: 'SchemaExtensionId', label: 'ID' },
					{ key: 'Description', label: 'Description' },
					{ key: 'SchemaExtensionStatus', label: 'Status', render: schemaStatusCell },
					{ key: 'StartDateTime', label: 'Started', render: schemaStartCell },
					{ key: 'actions', label: '', render: schemaActionsCell }
				] as Column<SchemaExtensionInfo>[]}
				<DataTable
					rows={filteredSchemaExts}
					rowKey={(s) => s.SchemaExtensionId ?? ''}
					columns={schemaColumns}
					loading={tabLoader.isLoading('schemaExtensions')}
					emptyMessage={selectedDirectoryId ? 'No schema extensions found' : 'Select a directory to see its schema extensions'}
				/>
				<LoadMore hasMore={!!schemaExtsNextToken} loading={loadingMoreSchemaExts} onLoadMore={loadMoreSchemaExts} />
			{:else if activeTab === 'certificates'}
				{#snippet certStateCell(c: CertificateInfo)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(c.State === 'Registered')}">{c.State ?? '—'}</span>
				{/snippet}
				{#snippet certExpiryCell(c: CertificateInfo)}
					{formatDate(c.ExpiryDateTime)}
				{/snippet}
				{#snippet certActionsCell(c: CertificateInfo)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openCertDetail(c)} title="View" aria-label="View certificate {c.CertificateId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeregisterCert(c)} title="Deregister" aria-label="Deregister certificate {c.CertificateId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const certColumns = [
					{ key: 'CommonName', label: 'Common Name' },
					{ key: 'CertificateId', label: 'ID' },
					{ key: 'Type', label: 'Type' },
					{ key: 'State', label: 'State', render: certStateCell },
					{ key: 'ExpiryDateTime', label: 'Expires', render: certExpiryCell },
					{ key: 'actions', label: '', render: certActionsCell }
				] as Column<CertificateInfo>[]}
				<DataTable
					rows={filteredCerts}
					rowKey={(c) => c.CertificateId ?? ''}
					columns={certColumns}
					loading={tabLoader.isLoading('certificates')}
					emptyMessage={selectedDirectoryId ? 'No certificates found' : 'Select a directory to see its certificates'}
				/>
				<LoadMore hasMore={!!certsNextToken} loading={loadingMoreCerts} onLoadMore={loadMoreCerts} />
			{:else if activeTab === 'eventTopics'}
				{#snippet topicStatusCell(t: EventTopic)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(t.Status === 'Registered')}">{t.Status ?? '—'}</span>
				{/snippet}
				{#snippet topicCreatedCell(t: EventTopic)}
					{formatDate(t.CreatedDateTime)}
				{/snippet}
				{#snippet topicActionsCell(t: EventTopic)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTopicDetail(t)} title="View" aria-label="View event topic {t.TopicName}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeregisterTopic(t)} title="Deregister" aria-label="Deregister event topic {t.TopicName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const topicColumns = [
					{ key: 'TopicName', label: 'Topic Name' },
					{ key: 'DirectoryId', label: 'Directory' },
					{ key: 'Status', label: 'Status', render: topicStatusCell },
					{ key: 'CreatedDateTime', label: 'Created', render: topicCreatedCell },
					{ key: 'actions', label: '', render: topicActionsCell }
				] as Column<EventTopic>[]}
				<DataTable
					rows={filteredEventTopics}
					rowKey={(t) => `${t.DirectoryId ?? ''}/${t.TopicName ?? ''}`}
					columns={topicColumns}
					loading={tabLoader.isLoading('eventTopics')}
					emptyMessage="No event topics found"
				/>
			{:else if activeTab === 'domainControllers'}
				{#snippet dcStatusCell(d: DomainController)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(d.Status === 'Active')}">{d.Status ?? '—'}</span>
				{/snippet}
				{#snippet dcLaunchCell(d: DomainController)}
					{formatDate(d.LaunchTime)}
				{/snippet}
				{#snippet dcActionsCell(d: DomainController)}
					<div class="flex items-center justify-end">
						<button onclick={() => openDcDetail(d)} title="View" aria-label="View domain controller {d.DomainControllerId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const dcColumns = [
					{ key: 'DomainControllerId', label: 'ID' },
					{ key: 'DnsIpAddr', label: 'DNS IP' },
					{ key: 'AvailabilityZone', label: 'AZ' },
					{ key: 'Status', label: 'Status', render: dcStatusCell },
					{ key: 'LaunchTime', label: 'Launched', render: dcLaunchCell },
					{ key: 'actions', label: '', render: dcActionsCell }
				] as Column<DomainController>[]}
				<p class="text-xs text-gray-500 dark:text-gray-400">
					Domain controllers have no individual create/delete operation in the real API -- they are
					provisioned and torn down automatically. The only real mutation is a directory-level resize
					("Update controller count" above), not a per-controller operation.
				</p>
				<DataTable
					rows={filteredDcs}
					rowKey={(d) => d.DomainControllerId ?? ''}
					columns={dcColumns}
					loading={tabLoader.isLoading('domainControllers')}
					emptyMessage={selectedDirectoryId ? 'No domain controllers found' : 'Select a directory to see its domain controllers'}
				/>
				<LoadMore hasMore={!!dcsNextToken} loading={loadingMoreDcs} onLoadMore={loadMoreDcs} />
			{:else if activeTab === 'regions'}
				{#snippet regionStatusCell(r: RegionDescription)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(r.Status === 'Active')}">{r.Status ?? '—'}</span>
				{/snippet}
				{#snippet regionLaunchCell(r: RegionDescription)}
					{formatDate(r.LaunchTime)}
				{/snippet}
				{#snippet regionActionsCell(r: RegionDescription)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openRegionDetail(r)} title="View" aria-label="View Region {r.RegionName}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						{#if r.RegionType === 'Additional'}
							<button onclick={() => handleRemoveRegion(r)} title="Remove" aria-label="Remove Region {r.RegionName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						{/if}
					</div>
				{/snippet}
				{@const regionColumns = [
					{ key: 'RegionName', label: 'Region' },
					{ key: 'RegionType', label: 'Type' },
					{ key: 'Status', label: 'Status', render: regionStatusCell },
					{ key: 'LaunchTime', label: 'Added', render: regionLaunchCell },
					{ key: 'actions', label: '', render: regionActionsCell }
				] as Column<RegionDescription>[]}
				<DataTable
					rows={filteredRegions}
					rowKey={(r) => r.RegionName ?? ''}
					columns={regionColumns}
					loading={tabLoader.isLoading('regions')}
					emptyMessage={selectedDirectoryId ? 'No replicated Regions found' : 'Select a directory to see its replicated Regions'}
				/>
				<LoadMore hasMore={!!regionsNextToken} loading={loadingMoreRegions} onLoadMore={loadMoreRegions} />
			{:else if activeTab === 'sharedDirectories'}
				{#snippet shareStatusCell(s: SharedDirectory)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.ShareStatus === 'Shared')}">{s.ShareStatus ?? '—'}</span>
				{/snippet}
				{#snippet shareActionsCell(s: SharedDirectory)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openShareDetail(s)} title="View" aria-label="View share {s.SharedDirectoryId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						{#if s.ShareStatus === 'PendingAcceptance'}
							<button onclick={() => handleAcceptShare(s)} title="Accept" aria-label="Accept share {s.SharedDirectoryId}" class="text-gray-400 hover:text-green-500"><Check class="w-4 h-4" /></button>
						{/if}
						<button onclick={() => handleUnshareDir(s)} title="Unshare" aria-label="Unshare {s.SharedDirectoryId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const shareColumns = [
					{ key: 'SharedDirectoryId', label: 'Shared Directory ID' },
					{ key: 'SharedAccountId', label: 'Shared Account' },
					{ key: 'ShareMethod', label: 'Method' },
					{ key: 'ShareStatus', label: 'Status', render: shareStatusCell },
					{ key: 'actions', label: '', render: shareActionsCell }
				] as Column<SharedDirectory>[]}
				<DataTable
					rows={filteredSharedDirs}
					rowKey={(s) => s.SharedDirectoryId ?? ''}
					columns={shareColumns}
					loading={tabLoader.isLoading('sharedDirectories')}
					emptyMessage={selectedDirectoryId ? 'No shared directories found' : 'Select a directory to see directories it shares'}
				/>
				<LoadMore hasMore={!!sharedDirsNextToken} loading={loadingMoreSharedDirs} onLoadMore={loadMoreSharedDirs} />
			{:else if activeTab === 'adAssessments'}
				{#snippet assessStatusCell(a: AssessmentSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(a.Status === 'SUCCESS')}">{a.Status ?? '—'}</span>
				{/snippet}
				{#snippet assessStartCell(a: AssessmentSummary)}
					{formatDate(a.StartTime)}
				{/snippet}
				{#snippet assessActionsCell(a: AssessmentSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openAssessDetail(a)} title="View" aria-label="View assessment {a.AssessmentId}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteAssess(a)} title="Delete" aria-label="Delete assessment {a.AssessmentId}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const assessColumns = [
					{ key: 'AssessmentId', label: 'ID' },
					{ key: 'DnsName', label: 'DNS Name' },
					{ key: 'DirectoryId', label: 'Directory' },
					{ key: 'Status', label: 'Status', render: assessStatusCell },
					{ key: 'StartTime', label: 'Started', render: assessStartCell },
					{ key: 'actions', label: '', render: assessActionsCell }
				] as Column<AssessmentSummary>[]}
				<DataTable
					rows={filteredAssessments}
					rowKey={(a) => a.AssessmentId ?? ''}
					columns={assessColumns}
					loading={tabLoader.isLoading('adAssessments')}
					emptyMessage="No assessments found"
				/>
				<LoadMore hasMore={!!assessmentsNextToken} loading={loadingMoreAssessments} onLoadMore={loadMoreAssessments} />
			{:else if activeTab === 'settings'}
				{#snippet settingStatusCell(s: SettingEntry)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(s.RequestStatus === 'Updated' || s.RequestStatus === 'Default')}">{s.RequestStatus ?? '—'}</span>
				{/snippet}
				{#snippet settingActionsCell(s: SettingEntry)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSettingDetail(s)} title="View" aria-label="View setting {s.Name}" class="text-gray-400 hover:text-sky-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openEditSettingModal(s)} title="Update" aria-label="Update setting {s.Name}" class="text-gray-400 hover:text-sky-500"><Pencil class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const settingColumns = [
					{ key: 'Name', label: 'Name' },
					{ key: 'Type', label: 'Type' },
					{ key: 'AppliedValue', label: 'Applied Value' },
					{ key: 'RequestStatus', label: 'Status', render: settingStatusCell },
					{ key: 'actions', label: '', render: settingActionsCell }
				] as Column<SettingEntry>[]}
				<p class="text-xs text-gray-500 dark:text-gray-400">
					Directory settings are predefined by AWS per directory type -- there is no create or delete
					operation for an individual setting, only reading and updating its value.
				</p>
				<DataTable
					rows={filteredSettings}
					rowKey={(s) => s.Name ?? ''}
					columns={settingColumns}
					loading={tabLoader.isLoading('settings')}
					emptyMessage={selectedDirectoryId ? 'No settings found' : 'Select a directory to see its settings'}
				/>
				<LoadMore hasMore={!!settingsNextToken} loading={loadingMoreSettings} onLoadMore={loadMoreSettings} />
			{/if}
		</div>
	</div>
</div>

<!-- ==================== Directories modals ==================== -->

<Modal bind:this={createDirModal} title="Create Directory">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="dir-kind" class="text-sm text-slate-600 dark:text-slate-300">Directory type</label>
				<select id="dir-kind" bind:value={newDirKind} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="simple">Simple AD</option>
					<option value="microsoftad">Managed Microsoft AD</option>
					<option value="connector">AD Connector (self-managed)</option>
				</select>
			</div>
			<div>
				<label for="dir-name" class="text-sm text-slate-600 dark:text-slate-300">Fully qualified name</label>
				<input id="dir-name" bind:value={newDirName} placeholder="corp.example.com" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dir-shortname" class="text-sm text-slate-600 dark:text-slate-300">NetBIOS short name (optional)</label>
				<input id="dir-shortname" bind:value={newDirShortName} placeholder="CORP" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dir-desc" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input id="dir-desc" bind:value={newDirDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="dir-password" class="text-sm text-slate-600 dark:text-slate-300">Administrator password</label>
				<input id="dir-password" type="password" bind:value={newDirPassword} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if newDirKind === 'simple' || newDirKind === 'connector'}
				<div>
					<label for="dir-size" class="text-sm text-slate-600 dark:text-slate-300">Size</label>
					<select id="dir-size" bind:value={newDirSize} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="Small">Small</option>
						<option value="Large">Large</option>
					</select>
				</div>
			{/if}
			{#if newDirKind === 'microsoftad'}
				<div>
					<label for="dir-edition" class="text-sm text-slate-600 dark:text-slate-300">Edition</label>
					<select id="dir-edition" bind:value={newDirEdition} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="Enterprise">Enterprise</option>
						<option value="Standard">Standard</option>
					</select>
				</div>
			{/if}
			{#if newDirKind !== 'simple'}
				<div>
					<label for="dir-vpc" class="text-sm text-slate-600 dark:text-slate-300">VPC ID</label>
					<input id="dir-vpc" bind:value={newDirVpcId} placeholder="vpc-0123456789abcdef0" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="dir-subnets" class="text-sm text-slate-600 dark:text-slate-300">Subnet IDs (comma-separated)</label>
					<input id="dir-subnets" bind:value={newDirSubnetIds} placeholder="subnet-abc, subnet-def" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{:else}
				<div>
					<label for="dir-vpc-opt" class="text-sm text-slate-600 dark:text-slate-300">VPC ID (optional)</label>
					<input id="dir-vpc-opt" bind:value={newDirVpcId} placeholder="vpc-0123456789abcdef0" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="dir-subnets-opt" class="text-sm text-slate-600 dark:text-slate-300">Subnet IDs (comma-separated, optional)</label>
					<input id="dir-subnets-opt" bind:value={newDirSubnetIds} placeholder="subnet-abc, subnet-def" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if newDirKind === 'connector'}
				<div>
					<label for="dir-connect-user" class="text-sm text-slate-600 dark:text-slate-300">Self-managed account user name</label>
					<input id="dir-connect-user" bind:value={newDirConnectUserName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="dir-connect-dns" class="text-sm text-slate-600 dark:text-slate-300">Self-managed DNS IPs (comma-separated)</label>
					<input id="dir-connect-dns" bind:value={newDirConnectDnsIps} placeholder="10.0.0.10, 10.0.0.11" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			{/if}
			{#if createDirError}
				<p class="text-sm text-red-600 dark:text-red-400">{createDirError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createDirModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateDir} disabled={creatingDir} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{creatingDir ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateDirModal} title="Update Directory">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{updateDirTarget?.Name}</span></p>
			<div>
				<label for="update-dir-type" class="text-sm text-slate-600 dark:text-slate-300">Update type</label>
				<select id="update-dir-type" bind:value={updateDirType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="OS">OS version</option>
					<option value="NETWORK">Network type</option>
					<option value="SIZE">Directory size</option>
				</select>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={updateDirSnapshotFirst} />
				Create a snapshot before updating
			</label>
			{#if updateDirError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateDirError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updateDirModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdateDir} disabled={updatingDir} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{updatingDir ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={dirDetailModal} title="Directory">
	{#snippet children()}
		{#if viewedDir}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedDir.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Directory ID</dt><dd class="text-slate-900 dark:text-white">{viewedDir.DirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedDir.Type ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Stage</dt><dd class="text-slate-900 dark:text-white">{viewedDir.Stage ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Size / Edition</dt><dd class="text-slate-900 dark:text-white">{viewedDir.Size ?? '—'} / {viewedDir.Edition ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Network type</dt><dd class="text-slate-900 dark:text-white">{viewedDir.NetworkType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Alias</dt><dd class="text-slate-900 dark:text-white">{viewedDir.Alias ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Access URL</dt><dd class="break-all text-slate-900 dark:text-white">{viewedDir.AccessUrl ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">DNS IPs</dt><dd class="text-slate-900 dark:text-white">{(viewedDir.DnsIpAddrs ?? []).join(', ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">SSO enabled</dt><dd class="text-slate-900 dark:text-white">{viewedDir.SsoEnabled ? 'Yes' : 'No'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Launched</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedDir.LaunchTime)}</dd></div>
				{#if viewedDir.VpcSettings}
					<div><dt class="text-slate-500 dark:text-slate-400">VPC</dt><dd class="text-slate-900 dark:text-white">{viewedDir.VpcSettings.VpcId} ({(viewedDir.VpcSettings.SubnetIds ?? []).join(', ')})</dd></div>
				{/if}
				{#if viewedDir.ConnectSettings}
					<div><dt class="text-slate-500 dark:text-slate-400">Connect settings</dt><dd class="text-slate-900 dark:text-white">VPC {viewedDir.ConnectSettings.VpcId}, user {viewedDir.ConnectSettings.CustomerUserName}</dd></div>
				{/if}
				{#if viewedDir.RadiusStatus}
					<div><dt class="text-slate-500 dark:text-slate-400">RADIUS</dt><dd class="text-slate-900 dark:text-white">{viewedDir.RadiusStatus}</dd></div>
				{/if}
				{#if viewedDir.RegionsInfo}
					<div><dt class="text-slate-500 dark:text-slate-400">Regions</dt><dd class="text-slate-900 dark:text-white">Primary: {viewedDir.RegionsInfo.PrimaryRegion}; Additional: {(viewedDir.RegionsInfo.AdditionalRegions ?? []).join(', ') || 'none'}</dd></div>
				{/if}
				{#if viewedDir.HybridSettings}
					<div><dt class="text-slate-500 dark:text-slate-400">Hybrid self-managed DNS IPs</dt><dd class="text-slate-900 dark:text-white">{(viewedDir.HybridSettings.SelfManagedDnsIpAddrs ?? []).join(', ') || '—'}</dd></div>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => dirDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Snapshots modals ==================== -->

<Modal bind:this={createSnapModal} title="Create Snapshot">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="snap-name" class="text-sm text-slate-600 dark:text-slate-300">Name (optional)</label>
				<input id="snap-name" bind:value={newSnapName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createSnapError}
				<p class="text-sm text-red-600 dark:text-red-400">{createSnapError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createSnapModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateSnap} disabled={creatingSnap} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{creatingSnap ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={snapDetailModal} title="Snapshot">
	{#snippet children()}
		{#if viewedSnap}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedSnap.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Snapshot ID</dt><dd class="text-slate-900 dark:text-white">{viewedSnap.SnapshotId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Directory</dt><dd class="text-slate-900 dark:text-white">{viewedSnap.DirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedSnap.Type ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedSnap.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Started</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedSnap.StartTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => snapDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Trusts modals ==================== -->

<Modal bind:this={createTrustModal} title="Create Trust">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="trust-domain" class="text-sm text-slate-600 dark:text-slate-300">Remote domain name</label>
				<input id="trust-domain" bind:value={newTrustRemoteDomain} placeholder="remote.example.com" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="trust-password" class="text-sm text-slate-600 dark:text-slate-300">Trust password</label>
				<input id="trust-password" type="password" bind:value={newTrustPassword} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="trust-direction" class="text-sm text-slate-600 dark:text-slate-300">Direction</label>
				<select id="trust-direction" bind:value={newTrustDirection} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="Two-Way">Two-Way</option>
					<option value="One-Way: Outgoing">One-Way: Outgoing</option>
					<option value="One-Way: Incoming">One-Way: Incoming</option>
				</select>
			</div>
			<div>
				<label for="trust-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select id="trust-type" bind:value={newTrustType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="Forest">Forest</option>
					<option value="External">External</option>
				</select>
			</div>
			{#if createTrustError}
				<p class="text-sm text-red-600 dark:text-red-400">{createTrustError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createTrustModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateTrust} disabled={creatingTrust} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{creatingTrust ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateTrustModal} title="Update Trust">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating trust with <span class="font-medium">{updateTrustTarget?.RemoteDomainName}</span></p>
			<div>
				<label for="trust-selective-auth" class="text-sm text-slate-600 dark:text-slate-300">Selective authentication</label>
				<select id="trust-selective-auth" bind:value={updateTrustSelectiveAuth} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="Disabled">Disabled</option>
					<option value="Enabled">Enabled</option>
				</select>
			</div>
			{#if updateTrustError}
				<p class="text-sm text-red-600 dark:text-red-400">{updateTrustError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => updateTrustModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitUpdateTrust} disabled={updatingTrust} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{updatingTrust ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={trustDetailModal} title="Trust">
	{#snippet children()}
		{#if viewedTrust}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Remote domain</dt><dd class="text-slate-900 dark:text-white">{viewedTrust.RemoteDomainName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Trust ID</dt><dd class="text-slate-900 dark:text-white">{viewedTrust.TrustId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Directory</dt><dd class="text-slate-900 dark:text-white">{viewedTrust.DirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type / Direction</dt><dd class="text-slate-900 dark:text-white">{viewedTrust.TrustType ?? '—'} / {viewedTrust.TrustDirection ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedTrust.TrustState ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Selective auth</dt><dd class="text-slate-900 dark:text-white">{viewedTrust.SelectiveAuth ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedTrust.CreatedDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => trustDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Conditional Forwarders modals ==================== -->

<Modal bind:this={createCfwdModal} title="Create Conditional Forwarder">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="cfwd-domain" class="text-sm text-slate-600 dark:text-slate-300">Remote domain name</label>
				<input id="cfwd-domain" bind:value={newCfwdDomain} placeholder="remote.example.com" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="cfwd-dns" class="text-sm text-slate-600 dark:text-slate-300">DNS IPs (comma-separated)</label>
				<input id="cfwd-dns" bind:value={newCfwdDnsIps} placeholder="10.0.0.10, 10.0.0.11" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createCfwdError}
				<p class="text-sm text-red-600 dark:text-red-400">{createCfwdError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createCfwdModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateCfwd} disabled={creatingCfwd} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{creatingCfwd ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editCfwdModal} title="Edit Conditional Forwarder">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Editing <span class="font-medium">{editingCfwd?.RemoteDomainName}</span></p>
			<div>
				<label for="edit-cfwd-dns" class="text-sm text-slate-600 dark:text-slate-300">DNS IPs (comma-separated, replaces the existing list)</label>
				<input id="edit-cfwd-dns" bind:value={editingCfwdDnsIps} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editCfwdError}
				<p class="text-sm text-red-600 dark:text-red-400">{editCfwdError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editCfwdModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditCfwd} disabled={savingCfwd} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{savingCfwd ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={cfwdDetailModal} title="Conditional Forwarder">
	{#snippet children()}
		{#if viewedCfwd}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Remote domain</dt><dd class="text-slate-900 dark:text-white">{viewedCfwd.RemoteDomainName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">DNS IPs</dt><dd class="text-slate-900 dark:text-white">{(viewedCfwd.DnsIpAddrs ?? []).join(', ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">DNS IPv6</dt><dd class="text-slate-900 dark:text-white">{(viewedCfwd.DnsIpv6Addrs ?? []).join(', ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Replication scope</dt><dd class="text-slate-900 dark:text-white">{viewedCfwd.ReplicationScope ?? '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => cfwdDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Log Subscriptions modals ==================== -->

<Modal bind:this={createLogSubModal} title="Create Log Subscription">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="logsub-group" class="text-sm text-slate-600 dark:text-slate-300">CloudWatch log group name</label>
				<input id="logsub-group" bind:value={newLogGroupName} placeholder="/aws/directoryservice/d-12345" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createLogSubError}
				<p class="text-sm text-red-600 dark:text-red-400">{createLogSubError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createLogSubModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateLogSub} disabled={creatingLogSub} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{creatingLogSub ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={logSubDetailModal} title="Log Subscription">
	{#snippet children()}
		{#if viewedLogSub}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Directory</dt><dd class="text-slate-900 dark:text-white">{viewedLogSub.DirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Log group</dt><dd class="text-slate-900 dark:text-white">{viewedLogSub.LogGroupName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedLogSub.SubscriptionCreatedDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => logSubDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== IP Routes modals ==================== -->

<Modal bind:this={addRouteModal} title="Add IP Route">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="route-cidr" class="text-sm text-slate-600 dark:text-slate-300">CIDR address block</label>
				<input id="route-cidr" bind:value={newRouteCidr} placeholder="10.0.0.0/24" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="route-desc" class="text-sm text-slate-600 dark:text-slate-300">Description (optional)</label>
				<input id="route-desc" bind:value={newRouteDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if addRouteError}
				<p class="text-sm text-red-600 dark:text-red-400">{addRouteError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => addRouteModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitAddRoute} disabled={addingRoute} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{addingRoute ? 'Adding…' : 'Add'}</button>
	{/snippet}
</Modal>

<Modal bind:this={routeDetailModal} title="IP Route">
	{#snippet children()}
		{#if viewedRoute}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">CIDR</dt><dd class="text-slate-900 dark:text-white">{viewedRoute.CidrIp ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedRoute.Description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedRoute.IpRouteStatusMsg ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Added</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedRoute.AddedDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => routeDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Schema Extensions modals ==================== -->

<Modal bind:this={startSchemaModal} title="Start Schema Extension">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="schema-desc" class="text-sm text-slate-600 dark:text-slate-300">Description</label>
				<input id="schema-desc" bind:value={newSchemaDescription} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="schema-ldif" class="text-sm text-slate-600 dark:text-slate-300">LDIF content</label>
				<textarea id="schema-ldif" bind:value={newSchemaLdif} rows="6" class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			<label class="flex items-center gap-2 text-sm text-slate-600 dark:text-slate-300">
				<input type="checkbox" bind:checked={newSchemaSnapshotFirst} />
				Create a snapshot before applying
			</label>
			{#if startSchemaError}
				<p class="text-sm text-red-600 dark:text-red-400">{startSchemaError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => startSchemaModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitStartSchema} disabled={startingSchema} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{startingSchema ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={schemaDetailModal} title="Schema Extension">
	{#snippet children()}
		{#if viewedSchema}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedSchema.SchemaExtensionId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Description</dt><dd class="text-slate-900 dark:text-white">{viewedSchema.Description ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedSchema.SchemaExtensionStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status reason</dt><dd class="text-slate-900 dark:text-white">{viewedSchema.SchemaExtensionStatusReason ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Started</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedSchema.StartDateTime)}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Ended</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedSchema.EndDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => schemaDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Certificates modals ==================== -->

<Modal bind:this={registerCertModal} title="Register Certificate">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="cert-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select id="cert-type" bind:value={newCertType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="ClientLDAPS">Client LDAPS</option>
					<option value="ClientCertAuth">Client certificate authentication</option>
				</select>
			</div>
			<div>
				<label for="cert-data" class="text-sm text-slate-600 dark:text-slate-300">Certificate PEM data</label>
				<textarea id="cert-data" bind:value={newCertData} rows="6" placeholder="-----BEGIN CERTIFICATE-----" class="mt-1 w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if registerCertError}
				<p class="text-sm text-red-600 dark:text-red-400">{registerCertError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => registerCertModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitRegisterCert} disabled={registeringCert} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{registeringCert ? 'Registering…' : 'Register'}</button>
	{/snippet}
</Modal>

<Modal bind:this={certDetailModal} title="Certificate">
	{#snippet children()}
		{#if certDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedCert}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Common name</dt><dd class="text-slate-900 dark:text-white">{viewedCert.CommonName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Certificate ID</dt><dd class="text-slate-900 dark:text-white">{viewedCert.CertificateId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedCert.Type ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedCert.State ?? '—'}</dd></div>
				{#if 'StateReason' in viewedCert}
					<div><dt class="text-slate-500 dark:text-slate-400">State reason</dt><dd class="text-slate-900 dark:text-white">{viewedCert.StateReason ?? '—'}</dd></div>
				{/if}
				<div><dt class="text-slate-500 dark:text-slate-400">Expires</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedCert.ExpiryDateTime)}</dd></div>
				{#if 'RegisteredDateTime' in viewedCert}
					<div><dt class="text-slate-500 dark:text-slate-400">Registered</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedCert.RegisteredDateTime)}</dd></div>
				{/if}
			</dl>
			{#if certDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{certDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => certDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Event Topics modals ==================== -->

<Modal bind:this={registerTopicModal} title="Register Event Topic">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="topic-name" class="text-sm text-slate-600 dark:text-slate-300">SNS topic name</label>
				<input id="topic-name" bind:value={newTopicName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if registerTopicError}
				<p class="text-sm text-red-600 dark:text-red-400">{registerTopicError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => registerTopicModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitRegisterTopic} disabled={registeringTopic} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{registeringTopic ? 'Registering…' : 'Register'}</button>
	{/snippet}
</Modal>

<Modal bind:this={topicDetailModal} title="Event Topic">
	{#snippet children()}
		{#if viewedTopic}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Topic name</dt><dd class="text-slate-900 dark:text-white">{viewedTopic.TopicName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Directory</dt><dd class="text-slate-900 dark:text-white">{viewedTopic.DirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Topic ARN</dt><dd class="break-all text-slate-900 dark:text-white">{viewedTopic.TopicArn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedTopic.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedTopic.CreatedDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => topicDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Domain Controllers modals ==================== -->

<Modal bind:this={resizeDcsModal} title="Update Domain Controller Count">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="dc-desired" class="text-sm text-slate-600 dark:text-slate-300">Desired number of domain controllers</label>
				<input id="dc-desired" type="number" min="1" bind:value={newDesiredDcs} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if resizeDcsError}
				<p class="text-sm text-red-600 dark:text-red-400">{resizeDcsError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => resizeDcsModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitResizeDcs} disabled={resizingDcs} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{resizingDcs ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={dcDetailModal} title="Domain Controller">
	{#snippet children()}
		{#if viewedDc}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedDc.DomainControllerId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Directory</dt><dd class="text-slate-900 dark:text-white">{viewedDc.DirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">DNS IP</dt><dd class="text-slate-900 dark:text-white">{viewedDc.DnsIpAddr ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">VPC / Subnet</dt><dd class="text-slate-900 dark:text-white">{viewedDc.VpcId ?? '—'} / {viewedDc.SubnetId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Availability Zone</dt><dd class="text-slate-900 dark:text-white">{viewedDc.AvailabilityZone ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedDc.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Launched</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedDc.LaunchTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => dcDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Regions modals ==================== -->

<Modal bind:this={addRegionModal} title="Add Region Replication">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="region-name" class="text-sm text-slate-600 dark:text-slate-300">Region name</label>
				<input id="region-name" bind:value={newRegionName} placeholder="us-west-2" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="region-vpc" class="text-sm text-slate-600 dark:text-slate-300">VPC ID</label>
				<input id="region-vpc" bind:value={newRegionVpcId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="region-subnets" class="text-sm text-slate-600 dark:text-slate-300">Subnet IDs (comma-separated)</label>
				<input id="region-subnets" bind:value={newRegionSubnetIds} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if addRegionError}
				<p class="text-sm text-red-600 dark:text-red-400">{addRegionError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => addRegionModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitAddRegion} disabled={addingRegion} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{addingRegion ? 'Adding…' : 'Add'}</button>
	{/snippet}
</Modal>

<Modal bind:this={regionDetailModal} title="Region">
	{#snippet children()}
		{#if viewedRegion}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Region</dt><dd class="text-slate-900 dark:text-white">{viewedRegion.RegionName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedRegion.RegionType ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedRegion.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Desired domain controllers</dt><dd class="text-slate-900 dark:text-white">{viewedRegion.DesiredNumberOfDomainControllers ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">VPC</dt><dd class="text-slate-900 dark:text-white">{viewedRegion.VpcSettings?.VpcId ?? '—'} ({(viewedRegion.VpcSettings?.SubnetIds ?? []).join(', ')})</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Added</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedRegion.LaunchTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => regionDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Shared Directories modals ==================== -->

<Modal bind:this={shareDirModal} title="Share Directory">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Sharing directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="share-target" class="text-sm text-slate-600 dark:text-slate-300">Target AWS account ID</label>
				<input id="share-target" bind:value={newShareTargetId} placeholder="123456789012" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="share-method" class="text-sm text-slate-600 dark:text-slate-300">Share method</label>
				<select id="share-method" bind:value={newShareMethod} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value="HANDSHAKE">Handshake (target must accept)</option>
					<option value="ORGANIZATIONS">Organizations (active immediately)</option>
				</select>
			</div>
			<div>
				<label for="share-notes" class="text-sm text-slate-600 dark:text-slate-300">Share notes (optional)</label>
				<input id="share-notes" bind:value={newShareNotes} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if shareDirError}
				<p class="text-sm text-red-600 dark:text-red-400">{shareDirError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => shareDirModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitShareDir} disabled={sharingDir} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{sharingDir ? 'Sharing…' : 'Share'}</button>
	{/snippet}
</Modal>

<Modal bind:this={shareDetailModal} title="Shared Directory">
	{#snippet children()}
		{#if viewedShare}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Shared directory ID</dt><dd class="text-slate-900 dark:text-white">{viewedShare.SharedDirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Owner directory</dt><dd class="text-slate-900 dark:text-white">{viewedShare.OwnerDirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Owner account</dt><dd class="text-slate-900 dark:text-white">{viewedShare.OwnerAccountId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Shared account</dt><dd class="text-slate-900 dark:text-white">{viewedShare.SharedAccountId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Method</dt><dd class="text-slate-900 dark:text-white">{viewedShare.ShareMethod ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedShare.ShareStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Notes</dt><dd class="text-slate-900 dark:text-white">{viewedShare.ShareNotes ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedShare.CreatedDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		{#if viewedShare?.ShareStatus === 'PendingAcceptance'}
			<button type="button" onclick={() => viewedShare && handleRejectShare(viewedShare)} class="rounded-lg border border-red-300 px-4 py-2 text-sm font-medium text-red-700 hover:bg-red-50 dark:border-red-800 dark:text-red-300 dark:hover:bg-red-900/20">Reject</button>
			<button type="button" onclick={() => viewedShare && handleAcceptShare(viewedShare)} class="rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white hover:bg-green-700">Accept</button>
		{/if}
		<button type="button" onclick={() => shareDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== AD Assessments modals ==================== -->

<Modal bind:this={startAssessModal} title="Start AD Assessment">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">For directory <span class="font-medium">{selectedDirectory?.Name || '(none selected)'}</span></p>
			<div>
				<label for="assess-dns" class="text-sm text-slate-600 dark:text-slate-300">Self-managed DNS IPs (comma-separated)</label>
				<input id="assess-dns" bind:value={newAssessDnsIps} placeholder="10.0.0.10, 10.0.0.11" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="assess-vpc" class="text-sm text-slate-600 dark:text-slate-300">VPC ID</label>
				<input id="assess-vpc" bind:value={newAssessVpcId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="assess-subnets" class="text-sm text-slate-600 dark:text-slate-300">Subnet IDs (comma-separated)</label>
				<input id="assess-subnets" bind:value={newAssessSubnetIds} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="assess-instances" class="text-sm text-slate-600 dark:text-slate-300">Self-managed instance IDs with SSM (comma-separated)</label>
				<input id="assess-instances" bind:value={newAssessInstanceIds} placeholder="mi-0123456789abcdef0" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if startAssessError}
				<p class="text-sm text-red-600 dark:text-red-400">{startAssessError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => startAssessModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitStartAssess} disabled={startingAssess} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{startingAssess ? 'Starting…' : 'Start'}</button>
	{/snippet}
</Modal>

<Modal bind:this={assessDetailModal} title="Directory Assessment">
	{#snippet children()}
		{#if assessDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedAssess}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Assessment ID</dt><dd class="text-slate-900 dark:text-white">{viewedAssess.AssessmentId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Directory</dt><dd class="text-slate-900 dark:text-white">{viewedAssess.DirectoryId ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">DNS name</dt><dd class="text-slate-900 dark:text-white">{viewedAssess.DnsName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedAssess.Status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Customer DNS IPs</dt><dd class="text-slate-900 dark:text-white">{(viewedAssess.CustomerDnsIps ?? []).join(', ') || '—'}</dd></div>
				{#if 'VpcId' in viewedAssess}
					<div><dt class="text-slate-500 dark:text-slate-400">VPC</dt><dd class="text-slate-900 dark:text-white">{viewedAssess.VpcId ?? '—'}</dd></div>
				{/if}
				{#if 'SubnetIds' in viewedAssess}
					<div><dt class="text-slate-500 dark:text-slate-400">Subnets</dt><dd class="text-slate-900 dark:text-white">{(viewedAssess.SubnetIds ?? []).join(', ') || '—'}</dd></div>
				{/if}
				{#if 'SelfManagedInstanceIds' in viewedAssess}
					<div><dt class="text-slate-500 dark:text-slate-400">Self-managed instances</dt><dd class="text-slate-900 dark:text-white">{(viewedAssess.SelfManagedInstanceIds ?? []).join(', ') || '—'}</dd></div>
				{/if}
				<div><dt class="text-slate-500 dark:text-slate-400">Started</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedAssess.StartTime)}</dd></div>
			</dl>
			{#if viewedAssessReports.length > 0}
				<div class="mt-3">
					<p class="text-slate-500 dark:text-slate-400 text-sm mb-1">Domain controller reports</p>
					<ul class="text-xs space-y-1">
						{#each viewedAssessReports as r, i (r.DomainControllerIp ?? i)}
							<li class="text-slate-900 dark:text-white">{r.DomainControllerIp}: {(r.Validations ?? []).map((v) => `${v.Name}=${v.Status}`).join(', ')}</li>
						{/each}
					</ul>
				</div>
			{/if}
			{#if assessDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{assessDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => assessDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

<!-- ==================== Settings modals ==================== -->

<Modal bind:this={editSettingModal} title="Update Setting">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">Updating <span class="font-medium">{editingSetting?.Name}</span> (allowed values: {editingSetting?.AllowedValues ?? '—'})</p>
			<div>
				<label for="setting-value" class="text-sm text-slate-600 dark:text-slate-300">New value</label>
				<input id="setting-value" bind:value={editingSettingValue} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editSettingError}
				<p class="text-sm text-red-600 dark:text-red-400">{editSettingError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editSettingModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditSetting} disabled={savingSetting} class="rounded-lg bg-sky-600 px-4 py-2 text-sm font-semibold text-white hover:bg-sky-700 disabled:opacity-50">{savingSetting ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={settingDetailModal} title="Setting">
	{#snippet children()}
		{#if viewedSetting}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedSetting.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedSetting.Type ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Allowed values</dt><dd class="text-slate-900 dark:text-white">{viewedSetting.AllowedValues ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Applied value</dt><dd class="text-slate-900 dark:text-white">{viewedSetting.AppliedValue ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Requested value</dt><dd class="text-slate-900 dark:text-white">{viewedSetting.RequestedValue ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Request status</dt><dd class="text-slate-900 dark:text-white">{viewedSetting.RequestStatus ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Last updated</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedSetting.LastUpdatedDateTime)}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => settingDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
	{/snippet}
</Modal>

