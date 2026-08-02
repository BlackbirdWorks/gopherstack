<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getDMSClient } from '$lib/aws-client';
	import {
		DescribeReplicationInstancesCommand,
		CreateReplicationInstanceCommand,
		ModifyReplicationInstanceCommand,
		DeleteReplicationInstanceCommand,
		RebootReplicationInstanceCommand,
		DescribeEndpointsCommand,
		CreateEndpointCommand,
		ModifyEndpointCommand,
		DeleteEndpointCommand,
		DescribeReplicationTasksCommand,
		CreateReplicationTaskCommand,
		ModifyReplicationTaskCommand,
		DeleteReplicationTaskCommand,
		StartReplicationTaskCommand,
		StopReplicationTaskCommand,
		DescribeTableStatisticsCommand,
		DescribeReplicationSubnetGroupsCommand,
		CreateReplicationSubnetGroupCommand,
		ModifyReplicationSubnetGroupCommand,
		DeleteReplicationSubnetGroupCommand,
		DescribeEventSubscriptionsCommand,
		CreateEventSubscriptionCommand,
		ModifyEventSubscriptionCommand,
		DeleteEventSubscriptionCommand,
		DescribeCertificatesCommand,
		ImportCertificateCommand,
		DeleteCertificateCommand,
		DescribeConnectionsCommand,
		TestConnectionCommand,
		DeleteConnectionCommand,
		type ReplicationInstance,
		type Endpoint,
		type ReplicationTask,
		type TableStatistics,
		type ReplicationSubnetGroup,
		type EventSubscription,
		type Certificate,
		type Connection
	} from '@aws-sdk/client-database-migration-service';
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
	import { GitBranch, PlugZap, Plus, Trash2, Eye, Pencil, Play, Square, RotateCcw, Table2 } from 'lucide-svelte';

	const client = regionalClient(getDMSClient);

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

	// Replaces the JSON.stringify(x) antipattern: search only over the named
	// fields that are actually meaningful for each resource.
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

	// gopherstack's DMS backend emits the event-subscription identifier under
	// the wire key "SubscriptionName" (matching the *request* field of the
	// same name used by Create/Modify/DeleteEventSubscription), but the real
	// DescribeEventSubscriptions/EventSubscription response shape names this
	// field CustSubscriptionId -- confirmed against the installed SDK's
	// compiled EventSubscription type, which has no SubscriptionName member
	// at all. Read both so the UI keeps working against the actual wire shape
	// this backend sends, while the import stays typed against the real SDK
	// shape. See PARITY.md-adjacent finding reported alongside this change.
	//
	// Same struct also has a second, independent wire-shape bug: it emits
	// EventCategories where the real EventSubscription shape names the
	// member EventCategoriesList (confirmed against the installed SDK type).
	// Unlike the identifier field this one has no fallback -- the real key
	// is simply never sent, so the UI reads EventCategoriesList only and
	// will legitimately show it empty against this backend.
	type EventSubscriptionWire = EventSubscription & { SubscriptionName?: string };
	function subscriptionId(es: EventSubscriptionWire): string {
		return es.CustSubscriptionId ?? es.SubscriptionName ?? '';
	}

	// The real DMS Connection shape (confirmed against the installed SDK) has
	// only ReplicationInstanceArn/EndpointArn/Status/LastFailureMessage -- no
	// identifier fields. Resolve friendly names from the already-loaded
	// instances/endpoints lists instead of trusting extra fields the backend
	// happens to also send on the wire.
	function endpointLabel(arnStr: string | undefined): string {
		if (!arnStr) return '—';
		return endpoints.find((e) => e.EndpointArn === arnStr)?.EndpointIdentifier ?? arnStr;
	}
	function instanceLabel(arnStr: string | undefined): string {
		if (!arnStr) return '—';
		return instances.find((i) => i.ReplicationInstanceArn === arnStr)?.ReplicationInstanceIdentifier ?? arnStr;
	}

	// Valid EngineName values, mirroring services/dms/handler_endpoints.go's
	// validEngineNamesTable exactly so Create/Modify Endpoint never gets
	// rejected by the backend's server-side enum check.
	const ENGINE_NAMES = [
		'mysql', 'oracle', 'postgres', 'mariadb', 'aurora', 'aurora-postgresql',
		'opensearch', 'redshift', 's3', 'db2', 'db2-zos', 'azuredb', 'sybase',
		'dynamodb', 'mongodb', 'kinesis', 'kafka', 'elasticsearch', 'docdb',
		'sqlserver', 'neptune', 'babelfish', 'redshift-serverless',
		'aurora-serverless', 'aurora-postgresql-serverless', 'gcp-mysql',
		'azure-sql-managed-instance', 'redis', 'dms-transfer'
	];

	const INSTANCE_CLASSES = [
		'dms.t3.micro', 'dms.t3.small', 'dms.t3.medium', 'dms.t3.large',
		'dms.c5.large', 'dms.c5.xlarge', 'dms.c5.2xlarge', 'dms.c5.4xlarge',
		'dms.r5.large', 'dms.r5.xlarge', 'dms.r5.2xlarge', 'dms.r5.4xlarge',
		'dms.r5.8xlarge', 'dms.r5.16xlarge'
	];

	type TabId =
		| 'instances'
		| 'endpoints'
		| 'tasks'
		| 'subnetGroups'
		| 'eventSubscriptions'
		| 'certificates'
		| 'connections';

	const tabs: TabDef[] = [
		{ id: 'instances', label: 'Replication Instances' },
		{ id: 'endpoints', label: 'Endpoints' },
		{ id: 'tasks', label: 'Replication Tasks' },
		{ id: 'subnetGroups', label: 'Subnet Groups' },
		{ id: 'eventSubscriptions', label: 'Event Subscriptions' },
		{ id: 'certificates', label: 'Certificates' },
		{ id: 'connections', label: 'Connections' }
	];

	let activeTab = $state<TabId>('instances');
	let searchQuery = $state('');

	let instances = $state<ReplicationInstance[]>([]);
	let instancesMarker = $state<string | undefined>();
	let loadingMoreInstances = $state(false);

	let endpoints = $state<Endpoint[]>([]);
	let endpointsMarker = $state<string | undefined>();
	let loadingMoreEndpoints = $state(false);

	let tasks = $state<ReplicationTask[]>([]);
	let tasksMarker = $state<string | undefined>();
	let loadingMoreTasks = $state(false);

	let subnetGroups = $state<ReplicationSubnetGroup[]>([]);
	let subnetGroupsMarker = $state<string | undefined>();
	let loadingMoreSubnetGroups = $state(false);

	let eventSubs = $state<EventSubscriptionWire[]>([]);
	let eventSubsMarker = $state<string | undefined>();
	let loadingMoreEventSubs = $state(false);

	let certificates = $state<Certificate[]>([]);
	let certificatesMarker = $state<string | undefined>();
	let loadingMoreCertificates = $state(false);

	// DescribeConnections's Go handler (handler_connections.go) never calls
	// dmsPaginate and never sets Marker on the output at all, unlike every
	// other Describe op in this service -- it always returns the full,
	// unpaginated list regardless of MaxRecords/Marker. So there is
	// genuinely nothing to "load more": hasMore is always false in practice.
	// See report.
	let connections = $state<Connection[]>([]);

	async function fetchInstances(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeReplicationInstancesCommand({ Marker: reset ? undefined : instancesMarker })
		);
		instances = reset ? (resp.ReplicationInstances ?? []) : [...instances, ...(resp.ReplicationInstances ?? [])];
		instancesMarker = resp.Marker;
	}

	async function fetchEndpoints(reset: boolean): Promise<void> {
		const resp = await client().send(new DescribeEndpointsCommand({ Marker: reset ? undefined : endpointsMarker }));
		endpoints = reset ? (resp.Endpoints ?? []) : [...endpoints, ...(resp.Endpoints ?? [])];
		endpointsMarker = resp.Marker;
	}

	async function fetchTasks(reset: boolean): Promise<void> {
		const resp = await client().send(new DescribeReplicationTasksCommand({ Marker: reset ? undefined : tasksMarker }));
		tasks = reset ? (resp.ReplicationTasks ?? []) : [...tasks, ...(resp.ReplicationTasks ?? [])];
		tasksMarker = resp.Marker;
	}

	async function fetchSubnetGroups(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeReplicationSubnetGroupsCommand({ Marker: reset ? undefined : subnetGroupsMarker })
		);
		subnetGroups = reset
			? (resp.ReplicationSubnetGroups ?? [])
			: [...subnetGroups, ...(resp.ReplicationSubnetGroups ?? [])];
		subnetGroupsMarker = resp.Marker;
	}

	async function fetchEventSubs(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeEventSubscriptionsCommand({ Marker: reset ? undefined : eventSubsMarker })
		);
		eventSubs = reset
			? (resp.EventSubscriptionsList ?? [])
			: [...eventSubs, ...(resp.EventSubscriptionsList ?? [])];
		eventSubsMarker = resp.Marker;
	}

	async function fetchCertificates(reset: boolean): Promise<void> {
		const resp = await client().send(
			new DescribeCertificatesCommand({ Marker: reset ? undefined : certificatesMarker })
		);
		certificates = reset ? (resp.Certificates ?? []) : [...certificates, ...(resp.Certificates ?? [])];
		certificatesMarker = resp.Marker;
	}

	async function fetchConnections(): Promise<void> {
		const resp = await client().send(new DescribeConnectionsCommand({}));
		connections = resp.Connections ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		instances: () => fetchInstances(true).catch(rethrowDescribed),
		endpoints: () => fetchEndpoints(true).catch(rethrowDescribed),
		tasks: () => fetchTasks(true).catch(rethrowDescribed),
		subnetGroups: () => fetchSubnetGroups(true).catch(rethrowDescribed),
		eventSubscriptions: () => fetchEventSubs(true).catch(rethrowDescribed),
		certificates: () => fetchCertificates(true).catch(rethrowDescribed),
		connections: () => fetchConnections().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		tabLoader.refresh(activeTab);
	});

	// Replication tasks and connections reference endpoints/instances by ARN;
	// endpoint/instance identifiers are resolved for display via
	// endpointLabel/instanceLabel above, which need those lists loaded. Also
	// used to populate the dropdowns in the Create Task / Test Connection
	// modals. Safe to call unconditionally -- tabLoader.load no-ops if
	// already loaded for the current region.
	async function ensureReferenceDataLoaded(): Promise<void> {
		await Promise.all([tabLoader.load('instances'), tabLoader.load('endpoints')]);
	}

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredInstances = $derived(
		instances.filter((i) =>
			matches(searchQuery, i.ReplicationInstanceIdentifier, i.ReplicationInstanceClass, i.ReplicationInstanceStatus)
		)
	);
	const filteredEndpoints = $derived(
		endpoints.filter((e) => matches(searchQuery, e.EndpointIdentifier, e.EngineName, e.ServerName, e.EndpointType))
	);
	const filteredTasks = $derived(
		tasks.filter((t) => matches(searchQuery, t.ReplicationTaskIdentifier, t.MigrationType, t.Status))
	);
	const filteredSubnetGroups = $derived(
		subnetGroups.filter((s) =>
			matches(searchQuery, s.ReplicationSubnetGroupIdentifier, s.ReplicationSubnetGroupDescription, s.VpcId)
		)
	);
	const filteredEventSubs = $derived(
		eventSubs.filter((s) => matches(searchQuery, subscriptionId(s), s.SnsTopicArn, s.SourceType, s.Status))
	);
	const filteredCertificates = $derived(
		certificates.filter((c) => matches(searchQuery, c.CertificateIdentifier, c.CertificateArn))
	);
	const filteredConnections = $derived(
		connections.filter((c) =>
			matches(searchQuery, endpointLabel(c.EndpointArn), instanceLabel(c.ReplicationInstanceArn), c.Status)
		)
	);

	async function loadMoreInstances(): Promise<void> {
		loadingMoreInstances = true;
		try {
			await fetchInstances(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreInstances = false;
		}
	}
	async function loadMoreEndpoints(): Promise<void> {
		loadingMoreEndpoints = true;
		try {
			await fetchEndpoints(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreEndpoints = false;
		}
	}
	async function loadMoreTasks(): Promise<void> {
		loadingMoreTasks = true;
		try {
			await fetchTasks(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTasks = false;
		}
	}
	async function loadMoreSubnetGroups(): Promise<void> {
		loadingMoreSubnetGroups = true;
		try {
			await fetchSubnetGroups(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreSubnetGroups = false;
		}
	}
	async function loadMoreEventSubs(): Promise<void> {
		loadingMoreEventSubs = true;
		try {
			await fetchEventSubs(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreEventSubs = false;
		}
	}
	async function loadMoreCertificates(): Promise<void> {
		loadingMoreCertificates = true;
		try {
			await fetchCertificates(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreCertificates = false;
		}
	}

	function statusClass(active: boolean): string {
		return active
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	// ==================== Replication Instances: create / delete / update / reboot / detail ====================

	let createInstanceModal = $state<Modal | null>(null);
	let creatingInstance = $state(false);
	let createInstanceError = $state<string | null>(null);
	let newInstanceId = $state('');
	let newInstanceClass = $state('dms.t3.micro');
	let newInstanceEngineVersion = $state('');
	let newInstanceAZ = $state('');
	let newInstanceStorage = $state('');
	let newInstanceMultiAZ = $state(false);
	let newInstanceAutoMinorUpgrade = $state(false);
	let newInstancePubliclyAccessible = $state(false);

	function openCreateInstanceModal(): void {
		createInstanceError = null;
		newInstanceId = '';
		newInstanceClass = 'dms.t3.micro';
		newInstanceEngineVersion = '';
		newInstanceAZ = '';
		newInstanceStorage = '';
		newInstanceMultiAZ = false;
		newInstanceAutoMinorUpgrade = false;
		newInstancePubliclyAccessible = false;
		createInstanceModal?.open();
	}

	async function submitCreateInstance(): Promise<void> {
		if (!newInstanceId || !newInstanceClass) {
			createInstanceError = 'Identifier and instance class are required.';
			return;
		}
		creatingInstance = true;
		createInstanceError = null;
		try {
			await client().send(
				new CreateReplicationInstanceCommand({
					ReplicationInstanceIdentifier: newInstanceId,
					ReplicationInstanceClass: newInstanceClass,
					EngineVersion: newInstanceEngineVersion || undefined,
					AvailabilityZone: newInstanceAZ || undefined,
					AllocatedStorage: newInstanceStorage ? parseInt(newInstanceStorage, 10) : undefined,
					MultiAZ: newInstanceMultiAZ,
					AutoMinorVersionUpgrade: newInstanceAutoMinorUpgrade,
					PubliclyAccessible: newInstancePubliclyAccessible
				})
			);
			toast.success('Replication instance created');
			createInstanceModal?.close();
			await tabLoader.refresh('instances');
		} catch (e) {
			const msg = describeError(e);
			createInstanceError = msg;
			toast.error(msg);
		} finally {
			creatingInstance = false;
		}
	}

	let updateInstanceModal = $state<Modal | null>(null);
	let updatingInstance = $state(false);
	let updateInstanceError = $state<string | null>(null);
	let updateInstanceTarget = $state<ReplicationInstance | null>(null);
	let updateInstanceClass = $state('');
	let updateInstanceEngineVersion = $state('');
	let updateInstanceStorage = $state('');
	let updateInstanceMultiAZ = $state(false);
	let updateInstanceAutoMinorUpgrade = $state(false);

	function openUpdateInstanceModal(i: ReplicationInstance): void {
		updateInstanceTarget = i;
		updateInstanceClass = i.ReplicationInstanceClass ?? '';
		updateInstanceEngineVersion = i.EngineVersion ?? '';
		updateInstanceStorage = i.AllocatedStorage ? String(i.AllocatedStorage) : '';
		updateInstanceMultiAZ = i.MultiAZ ?? false;
		updateInstanceAutoMinorUpgrade = i.AutoMinorVersionUpgrade ?? false;
		updateInstanceError = null;
		updateInstanceModal?.open();
	}

	async function submitUpdateInstance(): Promise<void> {
		if (!updateInstanceTarget?.ReplicationInstanceArn) return;
		updatingInstance = true;
		updateInstanceError = null;
		try {
			await client().send(
				new ModifyReplicationInstanceCommand({
					ReplicationInstanceArn: updateInstanceTarget.ReplicationInstanceArn,
					ReplicationInstanceClass: updateInstanceClass || undefined,
					EngineVersion: updateInstanceEngineVersion || undefined,
					AllocatedStorage: updateInstanceStorage ? parseInt(updateInstanceStorage, 10) : undefined,
					MultiAZ: updateInstanceMultiAZ,
					AutoMinorVersionUpgrade: updateInstanceAutoMinorUpgrade
				})
			);
			toast.success('Replication instance updated');
			updateInstanceModal?.close();
			await tabLoader.refresh('instances');
		} catch (e) {
			const msg = describeError(e);
			updateInstanceError = msg;
			toast.error(msg);
		} finally {
			updatingInstance = false;
		}
	}

	async function handleDeleteInstance(i: ReplicationInstance): Promise<void> {
		if (!i.ReplicationInstanceArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete replication instance',
			message: `Delete replication instance ${i.ReplicationInstanceIdentifier}? This fails if any replication task is still attached to it.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteReplicationInstanceCommand({ ReplicationInstanceArn: i.ReplicationInstanceArn }));
			toast.success('Replication instance deleted');
			await tabLoader.refresh('instances');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// RebootReplicationInstance is a benign, non-destructive verb (real AWS
	// causes only a momentary outage, no persistent field changes) -- no
	// confirmation dialog, matching how directoryservice treats VerifyTrust.
	async function handleRebootInstance(i: ReplicationInstance): Promise<void> {
		if (!i.ReplicationInstanceArn) return;
		try {
			await client().send(new RebootReplicationInstanceCommand({ ReplicationInstanceArn: i.ReplicationInstanceArn }));
			toast.success('Reboot initiated');
			await tabLoader.refresh('instances');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let instanceDetailModal = $state<Modal | null>(null);
	let viewedInstance = $state<ReplicationInstance | null>(null);

	function openInstanceDetail(i: ReplicationInstance): void {
		viewedInstance = i;
		instanceDetailModal?.open();
	}

	// ==================== Endpoints: create / delete / update / test connection / detail ====================
	//
	// The real CreateEndpoint/ModifyEndpoint API accepts engine-specific
	// nested settings structures (MySQLSettings, PostgreSQLSettings,
	// S3Settings, ...) plus a Password field. Confirmed against
	// services/dms/handler_endpoints.go's createEndpointInput /
	// ModifyEndpoint backend signature: neither struct has ANY field for
	// these -- encoding/json silently ignores unrecognized JSON members on
	// unmarshal, so if a client sent them today they would be accepted over
	// the wire and then discarded with no error, never stored, never
	// returned. This form intentionally does not offer them (see report).

	let createEndpointModal = $state<Modal | null>(null);
	let creatingEndpoint = $state(false);
	let createEndpointError = $state<string | null>(null);
	let newEndpointId = $state('');
	let newEndpointType = $state<'source' | 'target'>('source');
	let newEndpointEngine = $state('mysql');
	let newEndpointServer = $state('');
	let newEndpointDatabase = $state('');
	let newEndpointUsername = $state('');
	let newEndpointPort = $state('');

	function openCreateEndpointModal(): void {
		createEndpointError = null;
		newEndpointId = '';
		newEndpointType = 'source';
		newEndpointEngine = 'mysql';
		newEndpointServer = '';
		newEndpointDatabase = '';
		newEndpointUsername = '';
		newEndpointPort = '';
		createEndpointModal?.open();
	}

	async function submitCreateEndpoint(): Promise<void> {
		if (!newEndpointId) {
			createEndpointError = 'Endpoint identifier is required.';
			return;
		}
		creatingEndpoint = true;
		createEndpointError = null;
		try {
			await client().send(
				new CreateEndpointCommand({
					EndpointIdentifier: newEndpointId,
					EndpointType: newEndpointType,
					EngineName: newEndpointEngine,
					ServerName: newEndpointServer || undefined,
					DatabaseName: newEndpointDatabase || undefined,
					Username: newEndpointUsername || undefined,
					Port: newEndpointPort ? parseInt(newEndpointPort, 10) : undefined
				})
			);
			toast.success('Endpoint created');
			createEndpointModal?.close();
			await tabLoader.refresh('endpoints');
		} catch (e) {
			const msg = describeError(e);
			createEndpointError = msg;
			toast.error(msg);
		} finally {
			creatingEndpoint = false;
		}
	}

	let updateEndpointModal = $state<Modal | null>(null);
	let updatingEndpoint = $state(false);
	let updateEndpointError = $state<string | null>(null);
	let updateEndpointTarget = $state<Endpoint | null>(null);
	let updateEndpointType = $state<'source' | 'target'>('source');
	let updateEndpointEngine = $state('mysql');
	let updateEndpointServer = $state('');
	let updateEndpointDatabase = $state('');
	let updateEndpointUsername = $state('');
	let updateEndpointPort = $state('');

	function openUpdateEndpointModal(e: Endpoint): void {
		updateEndpointTarget = e;
		updateEndpointType = (e.EndpointType as 'source' | 'target') ?? 'source';
		updateEndpointEngine = e.EngineName ?? 'mysql';
		updateEndpointServer = e.ServerName ?? '';
		updateEndpointDatabase = e.DatabaseName ?? '';
		updateEndpointUsername = e.Username ?? '';
		updateEndpointPort = e.Port ? String(e.Port) : '';
		updateEndpointError = null;
		updateEndpointModal?.open();
	}

	async function submitUpdateEndpoint(): Promise<void> {
		if (!updateEndpointTarget?.EndpointArn) return;
		updatingEndpoint = true;
		updateEndpointError = null;
		try {
			await client().send(
				new ModifyEndpointCommand({
					EndpointArn: updateEndpointTarget.EndpointArn,
					EndpointType: updateEndpointType,
					EngineName: updateEndpointEngine,
					ServerName: updateEndpointServer || undefined,
					DatabaseName: updateEndpointDatabase || undefined,
					Username: updateEndpointUsername || undefined,
					Port: updateEndpointPort ? parseInt(updateEndpointPort, 10) : undefined
				})
			);
			toast.success('Endpoint updated');
			updateEndpointModal?.close();
			await tabLoader.refresh('endpoints');
		} catch (e) {
			const msg = describeError(e);
			updateEndpointError = msg;
			toast.error(msg);
		} finally {
			updatingEndpoint = false;
		}
	}

	async function handleDeleteEndpoint(e: Endpoint): Promise<void> {
		if (!e.EndpointArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete endpoint',
			message: `Delete endpoint ${e.EndpointIdentifier}? This fails if any replication task still references it.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteEndpointCommand({ EndpointArn: e.EndpointArn }));
			toast.success('Endpoint deleted');
			await tabLoader.refresh('endpoints');
		} catch (err) {
			toast.error(describeError(err));
		}
	}

	let testConnModal = $state<Modal | null>(null);
	let testConnTarget = $state<Endpoint | null>(null);
	let testConnInstanceArn = $state('');
	let testConnRunning = $state(false);
	let testConnResult = $state<Connection | null>(null);
	let testConnError = $state<string | null>(null);

	async function openTestConnModal(e: Endpoint): Promise<void> {
		testConnTarget = e;
		testConnResult = null;
		testConnRunning = false;
		testConnError = null;
		await ensureReferenceDataLoaded();
		testConnInstanceArn = instances[0]?.ReplicationInstanceArn ?? '';
		testConnModal?.open();
	}

	async function submitTestConn(): Promise<void> {
		if (!testConnTarget?.EndpointArn || !testConnInstanceArn) return;
		testConnRunning = true;
		testConnResult = null;
		testConnError = null;
		try {
			const resp = await client().send(
				new TestConnectionCommand({
					ReplicationInstanceArn: testConnInstanceArn,
					EndpointArn: testConnTarget.EndpointArn
				})
			);
			testConnResult = resp.Connection ?? null;
			toast.success('Connection test started');
			if (activeTab === 'connections') {
				await tabLoader.refresh('connections');
			}
		} catch (e) {
			const msg = describeError(e);
			testConnError = msg;
			toast.error(msg);
		} finally {
			testConnRunning = false;
		}
	}

	let endpointDetailModal = $state<Modal | null>(null);
	let viewedEndpoint = $state<Endpoint | null>(null);

	function openEndpointDetail(e: Endpoint): void {
		viewedEndpoint = e;
		endpointDetailModal?.open();
	}

	// ==================== Replication Tasks: create / delete / update / start / stop / detail ====================

	let createTaskModal = $state<Modal | null>(null);
	let creatingTask = $state(false);
	let createTaskError = $state<string | null>(null);
	let newTaskId = $state('');
	let newTaskSourceArn = $state('');
	let newTaskTargetArn = $state('');
	let newTaskInstanceArn = $state('');
	let newTaskMigrationType = $state<'full-load' | 'cdc' | 'full-load-and-cdc'>('full-load');
	let newTaskTableMappings = $state('');
	let newTaskSettings = $state('');

	async function openCreateTaskModal(): Promise<void> {
		createTaskError = null;
		newTaskId = '';
		newTaskMigrationType = 'full-load';
		newTaskTableMappings = '';
		newTaskSettings = '';
		await ensureReferenceDataLoaded();
		newTaskSourceArn = endpoints.find((e) => e.EndpointType === 'source')?.EndpointArn ?? '';
		newTaskTargetArn = endpoints.find((e) => e.EndpointType === 'target')?.EndpointArn ?? '';
		newTaskInstanceArn = instances[0]?.ReplicationInstanceArn ?? '';
		createTaskModal?.open();
	}

	async function submitCreateTask(): Promise<void> {
		if (!newTaskId || !newTaskSourceArn || !newTaskTargetArn || !newTaskInstanceArn) {
			createTaskError = 'Identifier, source endpoint, target endpoint, and replication instance are required.';
			return;
		}
		creatingTask = true;
		createTaskError = null;
		try {
			await client().send(
				new CreateReplicationTaskCommand({
					ReplicationTaskIdentifier: newTaskId,
					SourceEndpointArn: newTaskSourceArn,
					TargetEndpointArn: newTaskTargetArn,
					ReplicationInstanceArn: newTaskInstanceArn,
					MigrationType: newTaskMigrationType,
					TableMappings: newTaskTableMappings || undefined,
					ReplicationTaskSettings: newTaskSettings || undefined
				})
			);
			toast.success('Replication task created');
			createTaskModal?.close();
			await tabLoader.refresh('tasks');
		} catch (e) {
			const msg = describeError(e);
			createTaskError = msg;
			toast.error(msg);
		} finally {
			creatingTask = false;
		}
	}

	let updateTaskModal = $state<Modal | null>(null);
	let updatingTask = $state(false);
	let updateTaskError = $state<string | null>(null);
	let updateTaskTarget = $state<ReplicationTask | null>(null);
	let updateTaskMigrationType = $state<'full-load' | 'cdc' | 'full-load-and-cdc'>('full-load');
	let updateTaskTableMappings = $state('');
	let updateTaskSettings = $state('');

	function openUpdateTaskModal(t: ReplicationTask): void {
		updateTaskTarget = t;
		updateTaskMigrationType = (t.MigrationType as 'full-load' | 'cdc' | 'full-load-and-cdc') ?? 'full-load';
		updateTaskTableMappings = t.TableMappings ?? '';
		updateTaskSettings = t.ReplicationTaskSettings ?? '';
		updateTaskError = null;
		updateTaskModal?.open();
	}

	async function submitUpdateTask(): Promise<void> {
		if (!updateTaskTarget?.ReplicationTaskArn) return;
		updatingTask = true;
		updateTaskError = null;
		try {
			await client().send(
				new ModifyReplicationTaskCommand({
					ReplicationTaskArn: updateTaskTarget.ReplicationTaskArn,
					MigrationType: updateTaskMigrationType,
					TableMappings: updateTaskTableMappings || undefined,
					ReplicationTaskSettings: updateTaskSettings || undefined
				})
			);
			toast.success('Replication task updated');
			updateTaskModal?.close();
			await tabLoader.refresh('tasks');
		} catch (e) {
			// Backend rejects Modify while the task is running -- surface that
			// clearly rather than a generic failure.
			const msg = describeError(e);
			updateTaskError = msg;
			toast.error(msg);
		} finally {
			updatingTask = false;
		}
	}

	async function handleDeleteTask(t: ReplicationTask): Promise<void> {
		if (!t.ReplicationTaskArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete replication task',
			message: `Delete replication task ${t.ReplicationTaskIdentifier}? This fails while the task is running.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteReplicationTaskCommand({ ReplicationTaskArn: t.ReplicationTaskArn }));
			toast.success('Replication task deleted');
			await tabLoader.refresh('tasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleStartTask(t: ReplicationTask): Promise<void> {
		if (!t.ReplicationTaskArn) return;
		try {
			await client().send(
				new StartReplicationTaskCommand({
					ReplicationTaskArn: t.ReplicationTaskArn,
					StartReplicationTaskType: 'start-replication'
				})
			);
			toast.success('Replication task started');
			await tabLoader.refresh('tasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	async function handleStopTask(t: ReplicationTask): Promise<void> {
		if (!t.ReplicationTaskArn) return;
		try {
			await client().send(new StopReplicationTaskCommand({ ReplicationTaskArn: t.ReplicationTaskArn }));
			toast.success('Replication task stopped');
			await tabLoader.refresh('tasks');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	function parseTableMappings(tableMappings: string | undefined): { schema: string; table: string }[] {
		if (!tableMappings) return [];
		try {
			const parsed = JSON.parse(tableMappings);
			const rules: { schema: string; table: string }[] = [];
			for (const rule of parsed.rules ?? []) {
				if (rule['rule-type'] === 'selection') {
					rules.push({
						schema: rule['object-locator']?.['schema-name'] ?? '*',
						table: rule['object-locator']?.['table-name'] ?? '*'
					});
				}
			}
			return rules;
		} catch {
			return [];
		}
	}

	let taskDetailModal = $state<Modal | null>(null);
	let viewedTask = $state<ReplicationTask | null>(null);
	let taskStats = $state<TableStatistics[]>([]);
	let taskStatsLoading = $state(false);

	async function openTaskDetail(t: ReplicationTask): Promise<void> {
		viewedTask = t;
		taskStats = [];
		taskDetailModal?.open();
		if (!t.ReplicationTaskArn) return;
		taskStatsLoading = true;
		try {
			const resp = await client().send(
				new DescribeTableStatisticsCommand({ ReplicationTaskArn: t.ReplicationTaskArn })
			);
			taskStats = resp.TableStatistics ?? [];
		} catch (e) {
			toast.error('Failed to load table statistics: ' + describeError(e));
		} finally {
			taskStatsLoading = false;
		}
	}

	// ==================== Replication Subnet Groups: create / delete / update / detail ====================
	//
	// SubnetIds is required by services/dms/handler_replication_subnet_groups.go
	// on both Create and Modify, but accepted-and-discarded: no VPC/subnet
	// emulation exists, so it is never stored or echoed back on
	// ReplicationSubnetGroup (confirmed: replicationSubnetGroupFullJSON has
	// no Subnets field at all). The form still requires it since the backend
	// 400s without it.
	//
	// Separately, replicationSubnetGroupFullJSON also emits a
	// ReplicationSubnetGroupArn field that the real ReplicationSubnetGroup
	// shape does not have at all (confirmed against the installed SDK type --
	// subnet groups are identified by ReplicationSubnetGroupIdentifier only,
	// with no ARN concept in the real API). That field is simply not used
	// here; see report.

	let createSubnetGroupModal = $state<Modal | null>(null);
	let creatingSubnetGroup = $state(false);
	let createSubnetGroupError = $state<string | null>(null);
	let newSubnetGroupId = $state('');
	let newSubnetGroupDescription = $state('');
	let newSubnetGroupSubnetIds = $state('');

	function openCreateSubnetGroupModal(): void {
		createSubnetGroupError = null;
		newSubnetGroupId = '';
		newSubnetGroupDescription = '';
		newSubnetGroupSubnetIds = '';
		createSubnetGroupModal?.open();
	}

	async function submitCreateSubnetGroup(): Promise<void> {
		const subnetIds = parseCommaList(newSubnetGroupSubnetIds);
		if (!newSubnetGroupId || !newSubnetGroupDescription || subnetIds.length === 0) {
			createSubnetGroupError = 'Identifier, description, and at least one subnet ID are required.';
			return;
		}
		creatingSubnetGroup = true;
		createSubnetGroupError = null;
		try {
			await client().send(
				new CreateReplicationSubnetGroupCommand({
					ReplicationSubnetGroupIdentifier: newSubnetGroupId,
					ReplicationSubnetGroupDescription: newSubnetGroupDescription,
					SubnetIds: subnetIds
				})
			);
			toast.success('Replication subnet group created');
			createSubnetGroupModal?.close();
			await tabLoader.refresh('subnetGroups');
		} catch (e) {
			const msg = describeError(e);
			createSubnetGroupError = msg;
			toast.error(msg);
		} finally {
			creatingSubnetGroup = false;
		}
	}

	let updateSubnetGroupModal = $state<Modal | null>(null);
	let updatingSubnetGroup = $state(false);
	let updateSubnetGroupError = $state<string | null>(null);
	let updateSubnetGroupTarget = $state<ReplicationSubnetGroup | null>(null);
	let updateSubnetGroupDescription = $state('');
	let updateSubnetGroupSubnetIds = $state('');

	function openUpdateSubnetGroupModal(sg: ReplicationSubnetGroup): void {
		updateSubnetGroupTarget = sg;
		updateSubnetGroupDescription = sg.ReplicationSubnetGroupDescription ?? '';
		updateSubnetGroupSubnetIds = '';
		updateSubnetGroupError = null;
		updateSubnetGroupModal?.open();
	}

	async function submitUpdateSubnetGroup(): Promise<void> {
		if (!updateSubnetGroupTarget?.ReplicationSubnetGroupIdentifier) return;
		const subnetIds = parseCommaList(updateSubnetGroupSubnetIds);
		if (subnetIds.length === 0) {
			updateSubnetGroupError = 'At least one subnet ID is required.';
			return;
		}
		updatingSubnetGroup = true;
		updateSubnetGroupError = null;
		try {
			await client().send(
				new ModifyReplicationSubnetGroupCommand({
					ReplicationSubnetGroupIdentifier: updateSubnetGroupTarget.ReplicationSubnetGroupIdentifier,
					ReplicationSubnetGroupDescription: updateSubnetGroupDescription || undefined,
					SubnetIds: subnetIds
				})
			);
			toast.success('Replication subnet group updated');
			updateSubnetGroupModal?.close();
			await tabLoader.refresh('subnetGroups');
		} catch (e) {
			const msg = describeError(e);
			updateSubnetGroupError = msg;
			toast.error(msg);
		} finally {
			updatingSubnetGroup = false;
		}
	}

	async function handleDeleteSubnetGroup(sg: ReplicationSubnetGroup): Promise<void> {
		if (!sg.ReplicationSubnetGroupIdentifier) return;
		const confirmed = await confirmDestructive({
			title: 'Delete replication subnet group',
			message: `Delete replication subnet group ${sg.ReplicationSubnetGroupIdentifier}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteReplicationSubnetGroupCommand({
					ReplicationSubnetGroupIdentifier: sg.ReplicationSubnetGroupIdentifier
				})
			);
			toast.success('Replication subnet group deleted');
			await tabLoader.refresh('subnetGroups');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let subnetGroupDetailModal = $state<Modal | null>(null);
	let viewedSubnetGroup = $state<ReplicationSubnetGroup | null>(null);

	function openSubnetGroupDetail(sg: ReplicationSubnetGroup): void {
		viewedSubnetGroup = sg;
		subnetGroupDetailModal?.open();
	}

	// ==================== Event Subscriptions: create / delete / update / detail ====================
	//
	// ModifyEventSubscription only exposes Enabled here: the real API's
	// ModifyEventSubscriptionMessage also allows changing SnsTopicArn,
	// SourceType, and EventCategories, but
	// services/dms/handler_event_subscriptions.go's modifyEventSubscriptionInput
	// struct has only SubscriptionName and Enabled -- the other fields would
	// be silently discarded if sent, so this form doesn't offer them (see
	// report).

	let createEventSubModal = $state<Modal | null>(null);
	let creatingEventSub = $state(false);
	let createEventSubError = $state<string | null>(null);
	let newEventSubName = $state('');
	let newEventSubTopicArn = $state('');
	let newEventSubSourceType = $state<'' | 'replication-instance' | 'replication-task'>('');
	let newEventSubSourceIds = $state('');
	let newEventSubCategories = $state('');
	let newEventSubEnabled = $state(true);

	function openCreateEventSubModal(): void {
		createEventSubError = null;
		newEventSubName = '';
		newEventSubTopicArn = '';
		newEventSubSourceType = '';
		newEventSubSourceIds = '';
		newEventSubCategories = '';
		newEventSubEnabled = true;
		createEventSubModal?.open();
	}

	async function submitCreateEventSub(): Promise<void> {
		if (!newEventSubName || !newEventSubTopicArn) {
			createEventSubError = 'Subscription name and SNS topic ARN are required.';
			return;
		}
		creatingEventSub = true;
		createEventSubError = null;
		try {
			await client().send(
				new CreateEventSubscriptionCommand({
					SubscriptionName: newEventSubName,
					SnsTopicArn: newEventSubTopicArn,
					SourceType: newEventSubSourceType || undefined,
					SourceIds: parseCommaList(newEventSubSourceIds),
					EventCategories: parseCommaList(newEventSubCategories),
					Enabled: newEventSubEnabled
				})
			);
			toast.success('Event subscription created');
			createEventSubModal?.close();
			await tabLoader.refresh('eventSubscriptions');
		} catch (e) {
			const msg = describeError(e);
			createEventSubError = msg;
			toast.error(msg);
		} finally {
			creatingEventSub = false;
		}
	}

	let updateEventSubModal = $state<Modal | null>(null);
	let updatingEventSub = $state(false);
	let updateEventSubError = $state<string | null>(null);
	let updateEventSubTarget = $state<EventSubscriptionWire | null>(null);
	let updateEventSubEnabled = $state(true);

	function openUpdateEventSubModal(es: EventSubscriptionWire): void {
		updateEventSubTarget = es;
		updateEventSubEnabled = es.Enabled ?? true;
		updateEventSubError = null;
		updateEventSubModal?.open();
	}

	async function submitUpdateEventSub(): Promise<void> {
		const name = updateEventSubTarget ? subscriptionId(updateEventSubTarget) : '';
		if (!name) return;
		updatingEventSub = true;
		updateEventSubError = null;
		try {
			await client().send(
				new ModifyEventSubscriptionCommand({ SubscriptionName: name, Enabled: updateEventSubEnabled })
			);
			toast.success('Event subscription updated');
			updateEventSubModal?.close();
			await tabLoader.refresh('eventSubscriptions');
		} catch (e) {
			const msg = describeError(e);
			updateEventSubError = msg;
			toast.error(msg);
		} finally {
			updatingEventSub = false;
		}
	}

	async function handleDeleteEventSub(es: EventSubscriptionWire): Promise<void> {
		const name = subscriptionId(es);
		if (!name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete event subscription',
			message: `Delete event subscription ${name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteEventSubscriptionCommand({ SubscriptionName: name }));
			toast.success('Event subscription deleted');
			await tabLoader.refresh('eventSubscriptions');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let eventSubDetailModal = $state<Modal | null>(null);
	let viewedEventSub = $state<EventSubscriptionWire | null>(null);

	function openEventSubDetail(es: EventSubscriptionWire): void {
		viewedEventSub = es;
		eventSubDetailModal?.open();
	}

	// ==================== Certificates: import / delete / detail ====================
	//
	// No ModifyCertificate exists in the real API -- confirmed absent from
	// the installed SDK's command list -- so there is genuinely no update
	// verb to offer here, only Import (create) / Delete / Describe.

	let importCertModal = $state<Modal | null>(null);
	let importingCert = $state(false);
	let importCertError = $state<string | null>(null);
	let newCertId = $state('');
	let newCertPem = $state('');

	function openImportCertModal(): void {
		importCertError = null;
		newCertId = '';
		newCertPem = '';
		importCertModal?.open();
	}

	async function submitImportCert(): Promise<void> {
		if (!newCertId || !newCertPem) {
			importCertError = 'Certificate identifier and PEM contents are required.';
			return;
		}
		importingCert = true;
		importCertError = null;
		try {
			await client().send(
				new ImportCertificateCommand({ CertificateIdentifier: newCertId, CertificatePem: newCertPem })
			);
			toast.success('Certificate imported');
			importCertModal?.close();
			await tabLoader.refresh('certificates');
		} catch (e) {
			const msg = describeError(e);
			importCertError = msg;
			toast.error(msg);
		} finally {
			importingCert = false;
		}
	}

	async function handleDeleteCert(c: Certificate): Promise<void> {
		if (!c.CertificateArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete certificate',
			message: `Delete certificate ${c.CertificateIdentifier}?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteCertificateCommand({ CertificateArn: c.CertificateArn }));
			toast.success('Certificate deleted');
			await tabLoader.refresh('certificates');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let certDetailModal = $state<Modal | null>(null);
	let viewedCert = $state<Certificate | null>(null);

	function openCertDetail(c: Certificate): void {
		viewedCert = c;
		certDetailModal?.open();
	}

	// ==================== Connections: test / delete / detail ====================
	//
	// Connections have no independent Create -- a Connection row only ever
	// comes into being via TestConnection (which both runs a test AND
	// creates/updates the Connection resource itself), so the header action
	// for this tab opens the same Test Connection modal used from the
	// Endpoints tab rather than inventing a fake "create" operation.

	async function openTestConnFromConnectionsTab(): Promise<void> {
		await ensureReferenceDataLoaded();
		if (endpoints.length === 0) {
			toast.error('Create an endpoint first.');
			return;
		}
		await openTestConnModal(endpoints[0]);
	}

	async function handleDeleteConnection(c: Connection): Promise<void> {
		if (!c.EndpointArn || !c.ReplicationInstanceArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete connection',
			message: `Delete the connection test result between ${endpointLabel(c.EndpointArn)} and ${instanceLabel(c.ReplicationInstanceArn)}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteConnectionCommand({ EndpointArn: c.EndpointArn, ReplicationInstanceArn: c.ReplicationInstanceArn })
			);
			toast.success('Connection deleted');
			await tabLoader.refresh('connections');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let connectionDetailModal = $state<Modal | null>(null);
	let viewedConnection = $state<Connection | null>(null);

	function openConnectionDetail(c: Connection): void {
		viewedConnection = c;
		connectionDetailModal?.open();
	}
</script>

<!-- Replication Instance modals -->
<Modal bind:this={createInstanceModal} title="Create Replication Instance">
	{#snippet children()}
		<div class="space-y-4">
			{#if createInstanceError}
				<div class="text-sm text-red-600 dark:text-red-400">{createInstanceError}</div>
			{/if}
			<div>
				<label for="new-instance-id" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Instance Identifier</label>
				<input id="new-instance-id" bind:value={newInstanceId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="my-replication-instance" />
			</div>
			<div>
				<label for="new-instance-class" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Instance Class</label>
				<select id="new-instance-class" bind:value={newInstanceClass} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each INSTANCE_CLASSES as c (c)}
						<option value={c}>{c}</option>
					{/each}
				</select>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-instance-engine-version" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Engine Version</label>
					<input id="new-instance-engine-version" bind:value={newInstanceEngineVersion} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="3.5.2" />
				</div>
				<div>
					<label for="new-instance-az" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Availability Zone</label>
					<input id="new-instance-az" bind:value={newInstanceAZ} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="us-east-1a" />
				</div>
			</div>
			<div>
				<label for="new-instance-storage" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Allocated Storage (GB)</label>
				<input id="new-instance-storage" bind:value={newInstanceStorage} type="number" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="50" />
			</div>
			<div class="flex flex-wrap gap-4 text-sm text-gray-700 dark:text-gray-300">
				<label class="flex items-center gap-2"><input type="checkbox" bind:checked={newInstanceMultiAZ} /> Multi-AZ</label>
				<label class="flex items-center gap-2"><input type="checkbox" bind:checked={newInstanceAutoMinorUpgrade} /> Auto minor version upgrade</label>
				<label class="flex items-center gap-2"><input type="checkbox" bind:checked={newInstancePubliclyAccessible} /> Publicly accessible</label>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => createInstanceModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateInstance} disabled={creatingInstance} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{creatingInstance ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateInstanceModal} title="Update Replication Instance">
	{#snippet children()}
		<div class="space-y-4">
			{#if updateInstanceError}
				<div class="text-sm text-red-600 dark:text-red-400">{updateInstanceError}</div>
			{/if}
			<div>
				<label for="upd-instance-class" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Instance Class</label>
				<select id="upd-instance-class" bind:value={updateInstanceClass} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					{#each INSTANCE_CLASSES as c (c)}
						<option value={c}>{c}</option>
					{/each}
				</select>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="upd-instance-engine-version" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Engine Version</label>
					<input id="upd-instance-engine-version" bind:value={updateInstanceEngineVersion} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="upd-instance-storage" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Allocated Storage (GB)</label>
					<input id="upd-instance-storage" bind:value={updateInstanceStorage} type="number" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<div class="flex flex-wrap gap-4 text-sm text-gray-700 dark:text-gray-300">
				<label class="flex items-center gap-2"><input type="checkbox" bind:checked={updateInstanceMultiAZ} /> Multi-AZ</label>
				<label class="flex items-center gap-2"><input type="checkbox" bind:checked={updateInstanceAutoMinorUpgrade} /> Auto minor version upgrade</label>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => updateInstanceModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitUpdateInstance} disabled={updatingInstance} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{updatingInstance ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={instanceDetailModal} title="Replication Instance Details">
	{#snippet children()}
		{#if viewedInstance}
			<div class="space-y-2 text-sm">
				{#each [
					['Identifier', viewedInstance.ReplicationInstanceIdentifier],
					['ARN', viewedInstance.ReplicationInstanceArn],
					['Class', viewedInstance.ReplicationInstanceClass],
					['Engine Version', viewedInstance.EngineVersion],
					['Status', viewedInstance.ReplicationInstanceStatus],
					['Availability Zone', viewedInstance.AvailabilityZone],
					['Allocated Storage', viewedInstance.AllocatedStorage ? `${viewedInstance.AllocatedStorage} GB` : '—'],
					['Multi-AZ', viewedInstance.MultiAZ ? 'Yes' : 'No'],
					['Publicly Accessible', viewedInstance.PubliclyAccessible ? 'Yes' : 'No'],
					['Created', formatDate(viewedInstance.InstanceCreateTime)]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => instanceDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Endpoint modals -->
<Modal bind:this={createEndpointModal} title="Create Endpoint">
	{#snippet children()}
		<div class="space-y-4">
			{#if createEndpointError}
				<div class="text-sm text-red-600 dark:text-red-400">{createEndpointError}</div>
			{/if}
			<div>
				<label for="new-endpoint-id" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Endpoint Identifier</label>
				<input id="new-endpoint-id" bind:value={newEndpointId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="my-endpoint" />
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-endpoint-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Type</label>
					<select id="new-endpoint-type" bind:value={newEndpointType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="source">source</option>
						<option value="target">target</option>
					</select>
				</div>
				<div>
					<label for="new-endpoint-engine" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Engine</label>
					<select id="new-endpoint-engine" bind:value={newEndpointEngine} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each ENGINE_NAMES as eng (eng)}
							<option value={eng}>{eng}</option>
						{/each}
					</select>
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-endpoint-server" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Server Name</label>
					<input id="new-endpoint-server" bind:value={newEndpointServer} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="db.example.com" />
				</div>
				<div>
					<label for="new-endpoint-port" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Port</label>
					<input id="new-endpoint-port" bind:value={newEndpointPort} type="number" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="5432" />
				</div>
			</div>
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="new-endpoint-db" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Database Name</label>
					<input id="new-endpoint-db" bind:value={newEndpointDatabase} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
				<div>
					<label for="new-endpoint-user" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Username</label>
					<input id="new-endpoint-user" bind:value={newEndpointUsername} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
				</div>
			</div>
			<p class="text-xs text-gray-500 dark:text-gray-400">
				Password and engine-specific settings (MySQLSettings, S3Settings, ...) are not offered here: the backend accepts no such fields on CreateEndpoint and would silently discard them.
			</p>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => createEndpointModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateEndpoint} disabled={creatingEndpoint} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{creatingEndpoint ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateEndpointModal} title="Update Endpoint">
	{#snippet children()}
		<div class="space-y-4">
			{#if updateEndpointError}
				<div class="text-sm text-red-600 dark:text-red-400">{updateEndpointError}</div>
			{/if}
			<div class="grid grid-cols-2 gap-3">
				<div>
					<label for="upd-endpoint-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Type</label>
					<select id="upd-endpoint-type" bind:value={updateEndpointType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						<option value="source">source</option>
						<option value="target">target</option>
					</select>
				</div>
				<div>
					<label for="upd-endpoint-engine" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Engine</label>
					<select id="upd-endpoint-engine" bind:value={updateEndpointEngine} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each ENGINE_NAMES as eng (eng)}
							<option value={eng}>{eng}</option>
						{/each}
					</select>
				</div>
			</div>
			<div>
				<label for="upd-endpoint-server" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Server Name</label>
				<input id="upd-endpoint-server" bind:value={updateEndpointServer} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="upd-endpoint-db" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Database Name</label>
				<input id="upd-endpoint-db" bind:value={updateEndpointDatabase} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="upd-endpoint-user" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Username</label>
				<input id="upd-endpoint-user" bind:value={updateEndpointUsername} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="upd-endpoint-port" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Port</label>
				<input id="upd-endpoint-port" bind:value={updateEndpointPort} type="number" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => updateEndpointModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitUpdateEndpoint} disabled={updatingEndpoint} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{updatingEndpoint ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={testConnModal} title="Test Connection">
	{#snippet children()}
		<div class="space-y-4">
			{#if testConnError}
				<div class="text-sm text-red-600 dark:text-red-400">{testConnError}</div>
			{/if}
			<div>
				<span class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Endpoint</span>
				<div class="text-sm font-mono">{testConnTarget?.EndpointIdentifier ?? '—'}</div>
			</div>
			<div>
				<label for="test-conn-instance" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Replication Instance</label>
				{#if instances.length === 0}
					<p class="text-sm text-gray-500">No replication instances available to test from.</p>
				{:else}
					<select id="test-conn-instance" bind:value={testConnInstanceArn} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each instances as inst (inst.ReplicationInstanceArn)}
							<option value={inst.ReplicationInstanceArn}>{inst.ReplicationInstanceIdentifier}</option>
						{/each}
					</select>
				{/if}
			</div>
			{#if testConnResult}
				<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-3 space-y-2">
					<div class="flex items-center gap-2">
						<span class="text-xs text-gray-500">Status:</span>
						<span class="px-2 py-0.5 rounded text-xs font-medium {statusClass(testConnResult.Status === 'successful')}">{testConnResult.Status ?? '—'}</span>
					</div>
					{#if testConnResult.LastFailureMessage}
						<div class="text-xs text-red-600 dark:text-red-400 font-mono break-all">{testConnResult.LastFailureMessage}</div>
					{/if}
				</div>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => testConnModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
		<button onclick={submitTestConn} disabled={testConnRunning || !testConnInstanceArn} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{testConnRunning ? 'Testing…' : 'Run Test'}</button>
	{/snippet}
</Modal>

<Modal bind:this={endpointDetailModal} title="Endpoint Details">
	{#snippet children()}
		{#if viewedEndpoint}
			<div class="space-y-2 text-sm">
				{#each [
					['Identifier', viewedEndpoint.EndpointIdentifier],
					['ARN', viewedEndpoint.EndpointArn],
					['Type', viewedEndpoint.EndpointType],
					['Engine', viewedEndpoint.EngineName],
					['Server', viewedEndpoint.ServerName],
					['Port', viewedEndpoint.Port ? String(viewedEndpoint.Port) : '—'],
					['Database', viewedEndpoint.DatabaseName],
					['Username', viewedEndpoint.Username],
					['Status', viewedEndpoint.Status]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => endpointDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Replication Task modals -->
<Modal bind:this={createTaskModal} title="Create Replication Task">
	{#snippet children()}
		<div class="space-y-4">
			{#if createTaskError}
				<div class="text-sm text-red-600 dark:text-red-400">{createTaskError}</div>
			{/if}
			<div>
				<label for="new-task-id" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Task Identifier</label>
				<input id="new-task-id" bind:value={newTaskId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="my-migration-task" />
			</div>
			<div>
				<label for="new-task-source" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Source Endpoint</label>
				{#if endpoints.length === 0}
					<p class="text-sm text-gray-500">Create an endpoint first.</p>
				{:else}
					<select id="new-task-source" bind:value={newTaskSourceArn} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each endpoints as ep (ep.EndpointArn)}
							<option value={ep.EndpointArn}>{ep.EndpointIdentifier} ({ep.EndpointType})</option>
						{/each}
					</select>
				{/if}
			</div>
			<div>
				<label for="new-task-target" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Target Endpoint</label>
				{#if endpoints.length === 0}
					<p class="text-sm text-gray-500">Create an endpoint first.</p>
				{:else}
					<select id="new-task-target" bind:value={newTaskTargetArn} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each endpoints as ep (ep.EndpointArn)}
							<option value={ep.EndpointArn}>{ep.EndpointIdentifier} ({ep.EndpointType})</option>
						{/each}
					</select>
				{/if}
			</div>
			<div>
				<label for="new-task-instance" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Replication Instance</label>
				{#if instances.length === 0}
					<p class="text-sm text-gray-500">Create a replication instance first.</p>
				{:else}
					<select id="new-task-instance" bind:value={newTaskInstanceArn} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
						{#each instances as inst (inst.ReplicationInstanceArn)}
							<option value={inst.ReplicationInstanceArn}>{inst.ReplicationInstanceIdentifier}</option>
						{/each}
					</select>
				{/if}
			</div>
			<div>
				<label for="new-task-migration-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Migration Type</label>
				<select id="new-task-migration-type" bind:value={newTaskMigrationType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="full-load">full-load</option>
					<option value="cdc">cdc</option>
					<option value="full-load-and-cdc">full-load-and-cdc</option>
				</select>
			</div>
			<div>
				<label for="new-task-mappings" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Table Mappings (JSON)</label>
				<textarea id="new-task-mappings" bind:value={newTaskTableMappings} rows="3" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" placeholder={'{"rules": [{"rule-type": "selection", "object-locator": {"schema-name": "public", "table-name": "%"}}]}'}></textarea>
			</div>
			<div>
				<label for="new-task-settings" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Replication Task Settings (JSON)</label>
				<textarea id="new-task-settings" bind:value={newTaskSettings} rows="2" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => createTaskModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateTask} disabled={creatingTask} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{creatingTask ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateTaskModal} title="Update Replication Task">
	{#snippet children()}
		<div class="space-y-4">
			{#if updateTaskError}
				<div class="text-sm text-red-600 dark:text-red-400">{updateTaskError}</div>
			{/if}
			<div>
				<label for="upd-task-migration-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Migration Type</label>
				<select id="upd-task-migration-type" bind:value={updateTaskMigrationType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="full-load">full-load</option>
					<option value="cdc">cdc</option>
					<option value="full-load-and-cdc">full-load-and-cdc</option>
				</select>
			</div>
			<div>
				<label for="upd-task-mappings" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Table Mappings (JSON)</label>
				<textarea id="upd-task-mappings" bind:value={updateTaskTableMappings} rows="3" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<div>
				<label for="upd-task-settings" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Replication Task Settings (JSON)</label>
				<textarea id="upd-task-settings" bind:value={updateTaskSettings} rows="2" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono"></textarea>
			</div>
			<p class="text-xs text-gray-500 dark:text-gray-400">The backend rejects updates while the task is running.</p>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => updateTaskModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitUpdateTask} disabled={updatingTask} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{updatingTask ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={taskDetailModal} title="Replication Task Details">
	{#snippet children()}
		{#if viewedTask}
			<div class="space-y-4 text-sm">
				<div class="space-y-2">
					{#each [
						['Identifier', viewedTask.ReplicationTaskIdentifier],
						['ARN', viewedTask.ReplicationTaskArn],
						['Source Endpoint', endpointLabel(viewedTask.SourceEndpointArn)],
						['Target Endpoint', endpointLabel(viewedTask.TargetEndpointArn)],
						['Replication Instance', instanceLabel(viewedTask.ReplicationInstanceArn)],
						['Migration Type', viewedTask.MigrationType],
						['Status', viewedTask.Status],
						['Created', formatDate(viewedTask.ReplicationTaskCreationDate)]
					] as [label, value] (label)}
						<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
							<span class="text-gray-500">{label}</span>
							<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value ?? '—'}</span>
						</div>
					{/each}
				</div>
				<div>
					<h3 class="text-xs font-semibold uppercase text-gray-500 mb-2">Table Mapping Rules</h3>
					{#if parseTableMappings(viewedTask.TableMappings).length > 0}
						<div class="space-y-1">
							{#each parseTableMappings(viewedTask.TableMappings) as rule, i (i)}
								<div class="flex items-center gap-2 font-mono text-xs">
									<span class="text-blue-600 dark:text-blue-400">{rule.schema}</span>
									<span class="text-gray-400">/</span>
									<span class="text-gray-800 dark:text-gray-200">{rule.table}</span>
								</div>
							{/each}
						</div>
					{:else}
						<p class="text-xs text-gray-500">No table mapping rules configured.</p>
					{/if}
				</div>
				<div>
					<h3 class="text-xs font-semibold uppercase text-gray-500 mb-2 flex items-center gap-2"><Table2 class="w-3.5 h-3.5" /> Table Statistics</h3>
					{#if taskStatsLoading}
						<p class="text-xs text-gray-500">Loading…</p>
					{:else if taskStats.length > 0}
						<div class="overflow-x-auto">
							<table class="w-full text-xs">
								<thead class="text-gray-500">
									<tr>
										<th class="text-left py-1 pr-2">Schema</th>
										<th class="text-left py-1 pr-2">Table</th>
										<th class="text-right py-1 pr-2">Rows</th>
										<th class="text-left py-1">State</th>
									</tr>
								</thead>
								<tbody>
									{#each taskStats as stat, i (i)}
										<tr class="border-t border-gray-100 dark:border-gray-800">
											<td class="py-1 pr-2 font-mono text-blue-600 dark:text-blue-400">{stat.SchemaName ?? '-'}</td>
											<td class="py-1 pr-2 font-mono">{stat.TableName ?? '-'}</td>
											<td class="py-1 pr-2 text-right">{stat.FullLoadRows ?? 0}</td>
											<td class="py-1">{stat.TableState ?? '-'}</td>
										</tr>
									{/each}
								</tbody>
							</table>
						</div>
					{:else}
						<p class="text-xs text-gray-500">No table statistics available.</p>
					{/if}
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => taskDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Replication Subnet Group modals -->
<Modal bind:this={createSubnetGroupModal} title="Create Replication Subnet Group">
	{#snippet children()}
		<div class="space-y-4">
			{#if createSubnetGroupError}
				<div class="text-sm text-red-600 dark:text-red-400">{createSubnetGroupError}</div>
			{/if}
			<div>
				<label for="new-sg-id" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Subnet Group Identifier</label>
				<input id="new-sg-id" bind:value={newSubnetGroupId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="my-subnet-group" />
			</div>
			<div>
				<label for="new-sg-desc" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Description</label>
				<input id="new-sg-desc" bind:value={newSubnetGroupDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="new-sg-subnets" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Subnet IDs (comma-separated)</label>
				<input id="new-sg-subnets" bind:value={newSubnetGroupSubnetIds} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="subnet-abc123, subnet-def456" />
				<p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Required by the API, but not modeled or displayed by this backend (no VPC/subnet emulation).</p>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => createSubnetGroupModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateSubnetGroup} disabled={creatingSubnetGroup} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{creatingSubnetGroup ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateSubnetGroupModal} title="Update Replication Subnet Group">
	{#snippet children()}
		<div class="space-y-4">
			{#if updateSubnetGroupError}
				<div class="text-sm text-red-600 dark:text-red-400">{updateSubnetGroupError}</div>
			{/if}
			<div>
				<label for="upd-sg-desc" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Description</label>
				<input id="upd-sg-desc" bind:value={updateSubnetGroupDescription} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="upd-sg-subnets" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Subnet IDs (comma-separated)</label>
				<input id="upd-sg-subnets" bind:value={updateSubnetGroupSubnetIds} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="subnet-abc123, subnet-def456" />
				<p class="text-xs text-gray-500 dark:text-gray-400 mt-1">Required by the API even to change only the description.</p>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => updateSubnetGroupModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitUpdateSubnetGroup} disabled={updatingSubnetGroup} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{updatingSubnetGroup ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={subnetGroupDetailModal} title="Replication Subnet Group Details">
	{#snippet children()}
		{#if viewedSubnetGroup}
			<div class="space-y-2 text-sm">
				{#each [
					['Identifier', viewedSubnetGroup.ReplicationSubnetGroupIdentifier],
					['Description', viewedSubnetGroup.ReplicationSubnetGroupDescription],
					['VPC ID', viewedSubnetGroup.VpcId]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value || '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => subnetGroupDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Event Subscription modals -->
<Modal bind:this={createEventSubModal} title="Create Event Subscription">
	{#snippet children()}
		<div class="space-y-4">
			{#if createEventSubError}
				<div class="text-sm text-red-600 dark:text-red-400">{createEventSubError}</div>
			{/if}
			<div>
				<label for="new-es-name" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Subscription Name</label>
				<input id="new-es-name" bind:value={newEventSubName} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="new-es-topic" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">SNS Topic ARN</label>
				<input id="new-es-topic" bind:value={newEventSubTopicArn} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" placeholder="arn:aws:sns:us-east-1:123456789012:my-topic" />
			</div>
			<div>
				<label for="new-es-source-type" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Source Type</label>
				<select id="new-es-source-type" bind:value={newEventSubSourceType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="">All sources</option>
					<option value="replication-instance">replication-instance</option>
					<option value="replication-task">replication-task</option>
				</select>
			</div>
			<div>
				<label for="new-es-source-ids" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Source IDs (comma-separated)</label>
				<input id="new-es-source-ids" bind:value={newEventSubSourceIds} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="new-es-categories" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Event Categories (comma-separated)</label>
				<input id="new-es-categories" bind:value={newEventSubCategories} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" placeholder="creation, deletion, failure" />
			</div>
			<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={newEventSubEnabled} /> Enabled</label>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => createEventSubModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitCreateEventSub} disabled={creatingEventSub} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{creatingEventSub ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={updateEventSubModal} title="Update Event Subscription">
	{#snippet children()}
		<div class="space-y-4">
			{#if updateEventSubError}
				<div class="text-sm text-red-600 dark:text-red-400">{updateEventSubError}</div>
			{/if}
			<label class="flex items-center gap-2 text-sm text-gray-700 dark:text-gray-300"><input type="checkbox" bind:checked={updateEventSubEnabled} /> Enabled</label>
			<p class="text-xs text-gray-500 dark:text-gray-400">Only Enabled can be changed against this backend; SNS topic, source type, and event categories are fixed after creation here.</p>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => updateEventSubModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitUpdateEventSub} disabled={updatingEventSub} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{updatingEventSub ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<Modal bind:this={eventSubDetailModal} title="Event Subscription Details">
	{#snippet children()}
		{#if viewedEventSub}
			<div class="space-y-2 text-sm">
				{#each [
					['Name', subscriptionId(viewedEventSub)],
					['SNS Topic ARN', viewedEventSub.SnsTopicArn],
					['Source Type', viewedEventSub.SourceType || 'All sources'],
					['Status', viewedEventSub.Status],
					['Enabled', viewedEventSub.Enabled ? 'Yes' : 'No'],
					['Source IDs', (viewedEventSub.SourceIdsList ?? []).join(', ') || '—'],
					['Event Categories', (viewedEventSub.EventCategoriesList ?? []).join(', ') || '—']
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value || '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => eventSubDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Certificate modals -->
<Modal bind:this={importCertModal} title="Import Certificate">
	{#snippet children()}
		<div class="space-y-4">
			{#if importCertError}
				<div class="text-sm text-red-600 dark:text-red-400">{importCertError}</div>
			{/if}
			<div>
				<label for="new-cert-id" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Certificate Identifier</label>
				<input id="new-cert-id" bind:value={newCertId} type="text" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="new-cert-pem" class="block text-xs font-medium text-gray-600 dark:text-gray-400 mb-1">Certificate PEM</label>
				<textarea id="new-cert-pem" bind:value={newCertPem} rows="6" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm font-mono" placeholder="-----BEGIN CERTIFICATE-----..."></textarea>
			</div>
		</div>
	{/snippet}
	{#snippet footer()}
		<button onclick={() => importCertModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
		<button onclick={submitImportCert} disabled={importingCert} class="px-4 py-2 text-sm rounded-lg bg-blue-600 text-white hover:bg-blue-700 disabled:opacity-50">{importingCert ? 'Importing…' : 'Import'}</button>
	{/snippet}
</Modal>

<Modal bind:this={certDetailModal} title="Certificate Details">
	{#snippet children()}
		{#if viewedCert}
			<div class="space-y-2 text-sm">
				{#each [
					['Identifier', viewedCert.CertificateIdentifier],
					['ARN', viewedCert.CertificateArn]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value || '—'}</span>
					</div>
				{/each}
				<p class="text-xs text-gray-500 dark:text-gray-400 pt-2">
					The imported PEM contents are stored server-side but never returned on the wire by this backend (see report), so they cannot be displayed here.
				</p>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => certDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<!-- Connection detail modal -->
<Modal bind:this={connectionDetailModal} title="Connection Details">
	{#snippet children()}
		{#if viewedConnection}
			<div class="space-y-2 text-sm">
				{#each [
					['Endpoint', endpointLabel(viewedConnection.EndpointArn)],
					['Replication Instance', instanceLabel(viewedConnection.ReplicationInstanceArn)],
					['Status', viewedConnection.Status],
					['Last Failure Message', viewedConnection.LastFailureMessage]
				] as [label, value] (label)}
					<div class="flex justify-between gap-4 border-b border-gray-100 dark:border-gray-800 pb-1">
						<span class="text-gray-500">{label}</span>
						<span class="font-mono text-right break-all text-gray-900 dark:text-white">{value || '—'}</span>
					</div>
				{/each}
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button onclick={() => connectionDetailModal?.close()} class="px-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800">Close</button>
	{/snippet}
</Modal>

<div class="p-6 space-y-6">
	<PageHeader
		icon={GitBranch}
		title="AWS Database Migration Service"
		description="Migrate databases to AWS quickly and securely"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'instances'}
				<button onclick={openCreateInstanceModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create instance
				</button>
			{:else if activeTab === 'endpoints'}
				<button onclick={openCreateEndpointModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create endpoint
				</button>
			{:else if activeTab === 'tasks'}
				<button onclick={openCreateTaskModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create task
				</button>
			{:else if activeTab === 'subnetGroups'}
				<button onclick={openCreateSubnetGroupModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create subnet group
				</button>
			{:else if activeTab === 'eventSubscriptions'}
				<button onclick={openCreateEventSubModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Create subscription
				</button>
			{:else if activeTab === 'certificates'}
				<button onclick={openImportCertModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<Plus class="w-4 h-4" /> Import certificate
				</button>
			{:else if activeTab === 'connections'}
				<button onclick={openTestConnFromConnectionsTab} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm">
					<PlugZap class="w-4 h-4" /> Test connection
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'instances'}
				{#snippet instStatusCell(i: ReplicationInstance)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(i.ReplicationInstanceStatus === 'available')}">{i.ReplicationInstanceStatus ?? '—'}</span>
				{/snippet}
				{#snippet instCreatedCell(i: ReplicationInstance)}
					{formatDate(i.InstanceCreateTime)}
				{/snippet}
				{#snippet instActionsCell(i: ReplicationInstance)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openInstanceDetail(i)} title="View" aria-label="View instance {i.ReplicationInstanceIdentifier}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateInstanceModal(i)} title="Update" aria-label="Update instance {i.ReplicationInstanceIdentifier}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleRebootInstance(i)} title="Reboot" aria-label="Reboot instance {i.ReplicationInstanceIdentifier}" class="text-gray-400 hover:text-blue-500"><RotateCcw class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteInstance(i)} title="Delete" aria-label="Delete instance {i.ReplicationInstanceIdentifier}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const instColumns = defineColumns<ReplicationInstance>([
					{ key: 'ReplicationInstanceIdentifier', label: 'Identifier' },
					{ key: 'ReplicationInstanceClass', label: 'Class' },
					{ key: 'ReplicationInstanceStatus', label: 'Status', render: instStatusCell },
					{ key: 'EngineVersion', label: 'Engine Version' },
					{ key: 'InstanceCreateTime', label: 'Created', render: instCreatedCell },
					{ key: 'actions', label: '', render: instActionsCell }
				])}
				<DataTable
					rows={filteredInstances}
					rowKey={(i) => i.ReplicationInstanceArn ?? ''}
					columns={instColumns}
					loading={tabLoader.isLoading('instances')}
					emptyMessage="No replication instances found"
				/>
				<LoadMore hasMore={!!instancesMarker} loading={loadingMoreInstances} onLoadMore={loadMoreInstances} />
			{:else if activeTab === 'endpoints'}
				{#snippet epTypeCell(e: Endpoint)}
					<span class="px-2 py-0.5 rounded text-xs font-medium {e.EndpointType === 'source' ? 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400' : 'bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400'}">{e.EndpointType ?? '—'}</span>
				{/snippet}
				{#snippet epServerCell(e: Endpoint)}
					<span class="font-mono text-xs">{e.ServerName ?? '—'}{e.Port ? ':' + e.Port : ''}</span>
				{/snippet}
				{#snippet epStatusCell(e: Endpoint)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(e.Status === 'active')}">{e.Status ?? '—'}</span>
				{/snippet}
				{#snippet epActionsCell(e: Endpoint)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openEndpointDetail(e)} title="View" aria-label="View endpoint {e.EndpointIdentifier}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openTestConnModal(e)} title="Test connection" aria-label="Test connection for {e.EndpointIdentifier}" class="text-gray-400 hover:text-blue-500"><PlugZap class="w-4 h-4" /></button>
						<button onclick={() => openUpdateEndpointModal(e)} title="Update" aria-label="Update endpoint {e.EndpointIdentifier}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteEndpoint(e)} title="Delete" aria-label="Delete endpoint {e.EndpointIdentifier}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const epColumns = defineColumns<Endpoint>([
					{ key: 'EndpointIdentifier', label: 'Identifier' },
					{ key: 'EndpointType', label: 'Type', render: epTypeCell },
					{ key: 'EngineName', label: 'Engine' },
					{ key: 'ServerName', label: 'Server', render: epServerCell },
					{ key: 'Status', label: 'Status', render: epStatusCell },
					{ key: 'actions', label: '', render: epActionsCell }
				])}
				<DataTable
					rows={filteredEndpoints}
					rowKey={(e) => e.EndpointArn ?? ''}
					columns={epColumns}
					loading={tabLoader.isLoading('endpoints')}
					emptyMessage="No endpoints found"
				/>
				<LoadMore hasMore={!!endpointsMarker} loading={loadingMoreEndpoints} onLoadMore={loadMoreEndpoints} />
			{:else if activeTab === 'tasks'}
				{#snippet taskStatusCell(t: ReplicationTask)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(t.Status === 'running')}">{t.Status ?? '—'}</span>
				{/snippet}
				{#snippet taskActionsCell(t: ReplicationTask)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openTaskDetail(t)} title="View" aria-label="View task {t.ReplicationTaskIdentifier}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						{#if t.Status === 'running'}
							<button onclick={() => handleStopTask(t)} title="Stop" aria-label="Stop task {t.ReplicationTaskIdentifier}" class="text-gray-400 hover:text-blue-500"><Square class="w-4 h-4" /></button>
						{:else}
							<button onclick={() => handleStartTask(t)} title="Start" aria-label="Start task {t.ReplicationTaskIdentifier}" class="text-gray-400 hover:text-blue-500"><Play class="w-4 h-4" /></button>
						{/if}
						<button onclick={() => openUpdateTaskModal(t)} title="Update" aria-label="Update task {t.ReplicationTaskIdentifier}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteTask(t)} title="Delete" aria-label="Delete task {t.ReplicationTaskIdentifier}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const taskColumns = defineColumns<ReplicationTask>([
					{ key: 'ReplicationTaskIdentifier', label: 'Identifier' },
					{ key: 'Status', label: 'Status', render: taskStatusCell },
					{ key: 'MigrationType', label: 'Migration Type' },
					{ key: 'actions', label: '', render: taskActionsCell }
				])}
				<DataTable
					rows={filteredTasks}
					rowKey={(t) => t.ReplicationTaskArn ?? ''}
					columns={taskColumns}
					loading={tabLoader.isLoading('tasks')}
					emptyMessage="No replication tasks found"
				/>
				<LoadMore hasMore={!!tasksMarker} loading={loadingMoreTasks} onLoadMore={loadMoreTasks} />
			{:else if activeTab === 'subnetGroups'}
				{#snippet sgActionsCell(sg: ReplicationSubnetGroup)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openSubnetGroupDetail(sg)} title="View" aria-label="View subnet group {sg.ReplicationSubnetGroupIdentifier}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateSubnetGroupModal(sg)} title="Update" aria-label="Update subnet group {sg.ReplicationSubnetGroupIdentifier}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteSubnetGroup(sg)} title="Delete" aria-label="Delete subnet group {sg.ReplicationSubnetGroupIdentifier}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const sgColumns = defineColumns<ReplicationSubnetGroup>([
					{ key: 'ReplicationSubnetGroupIdentifier', label: 'Identifier' },
					{ key: 'ReplicationSubnetGroupDescription', label: 'Description' },
					{ key: 'VpcId', label: 'VPC ID' },
					{ key: 'actions', label: '', render: sgActionsCell }
				])}
				<DataTable
					rows={filteredSubnetGroups}
					rowKey={(sg) => sg.ReplicationSubnetGroupIdentifier ?? ''}
					columns={sgColumns}
					loading={tabLoader.isLoading('subnetGroups')}
					emptyMessage="No replication subnet groups found"
				/>
				<LoadMore hasMore={!!subnetGroupsMarker} loading={loadingMoreSubnetGroups} onLoadMore={loadMoreSubnetGroups} />
			{:else if activeTab === 'eventSubscriptions'}
				{#snippet esNameCell(es: EventSubscriptionWire)}
					{subscriptionId(es)}
				{/snippet}
				{#snippet esStatusCell(es: EventSubscriptionWire)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(es.Enabled === true)}">{es.Status ?? '—'}</span>
				{/snippet}
				{#snippet esActionsCell(es: EventSubscriptionWire)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openEventSubDetail(es)} title="View" aria-label="View subscription {subscriptionId(es)}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => openUpdateEventSubModal(es)} title="Update" aria-label="Update subscription {subscriptionId(es)}" class="text-gray-400 hover:text-blue-500"><Pencil class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteEventSub(es)} title="Delete" aria-label="Delete subscription {subscriptionId(es)}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const esColumns = defineColumns<EventSubscriptionWire>([
					{ key: 'SubscriptionName', label: 'Name', render: esNameCell },
					{ key: 'SnsTopicArn', label: 'SNS Topic ARN' },
					{ key: 'SourceType', label: 'Source Type' },
					{ key: 'Status', label: 'Status', render: esStatusCell },
					{ key: 'actions', label: '', render: esActionsCell }
				])}
				<DataTable
					rows={filteredEventSubs}
					rowKey={(es) => subscriptionId(es)}
					columns={esColumns}
					loading={tabLoader.isLoading('eventSubscriptions')}
					emptyMessage="No event subscriptions found"
				/>
				<LoadMore hasMore={!!eventSubsMarker} loading={loadingMoreEventSubs} onLoadMore={loadMoreEventSubs} />
			{:else if activeTab === 'certificates'}
				{#snippet certActionsCell(c: Certificate)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openCertDetail(c)} title="View" aria-label="View certificate {c.CertificateIdentifier}" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteCert(c)} title="Delete" aria-label="Delete certificate {c.CertificateIdentifier}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const certColumns = defineColumns<Certificate>([
					{ key: 'CertificateIdentifier', label: 'Identifier' },
					{ key: 'CertificateArn', label: 'ARN' },
					{ key: 'actions', label: '', render: certActionsCell }
				])}
				<DataTable
					rows={filteredCertificates}
					rowKey={(c) => c.CertificateArn ?? ''}
					columns={certColumns}
					loading={tabLoader.isLoading('certificates')}
					emptyMessage="No certificates found"
				/>
				<LoadMore hasMore={!!certificatesMarker} loading={loadingMoreCertificates} onLoadMore={loadMoreCertificates} />
			{:else if activeTab === 'connections'}
				{#snippet connEndpointCell(c: Connection)}
					{endpointLabel(c.EndpointArn)}
				{/snippet}
				{#snippet connInstanceCell(c: Connection)}
					{instanceLabel(c.ReplicationInstanceArn)}
				{/snippet}
				{#snippet connStatusCell(c: Connection)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(c.Status === 'successful')}">{c.Status ?? '—'}</span>
				{/snippet}
				{#snippet connActionsCell(c: Connection)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openConnectionDetail(c)} title="View" aria-label="View connection" class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => handleDeleteConnection(c)} title="Delete" aria-label="Delete connection" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const connColumns = defineColumns<Connection>([
					{ key: 'EndpointArn', label: 'Endpoint', render: connEndpointCell },
					{ key: 'ReplicationInstanceArn', label: 'Replication Instance', render: connInstanceCell },
					{ key: 'Status', label: 'Status', render: connStatusCell },
					{ key: 'actions', label: '', render: connActionsCell }
				])}
				<DataTable
					rows={filteredConnections}
					rowKey={(c) => `${c.EndpointArn ?? ''}:${c.ReplicationInstanceArn ?? ''}`}
					columns={connColumns}
					loading={tabLoader.isLoading('connections')}
					emptyMessage="No connections found"
				/>
			{/if}
		</div>
	</div>
</div>
