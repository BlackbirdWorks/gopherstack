<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getS3TablesClient } from '$lib/aws-client';
	import {
		ListTableBucketsCommand,
		CreateTableBucketCommand,
		DeleteTableBucketCommand,
		GetTableBucketCommand,
		GetTableBucketPolicyCommand,
		PutTableBucketPolicyCommand,
		DeleteTableBucketPolicyCommand,
		GetTableBucketEncryptionCommand,
		PutTableBucketEncryptionCommand,
		DeleteTableBucketEncryptionCommand,
		GetTableBucketMaintenanceConfigurationCommand,
		PutTableBucketMaintenanceConfigurationCommand,
		GetTableBucketStorageClassCommand,
		PutTableBucketStorageClassCommand,
		GetTableBucketMetricsConfigurationCommand,
		PutTableBucketMetricsConfigurationCommand,
		DeleteTableBucketMetricsConfigurationCommand,
		ListNamespacesCommand,
		CreateNamespaceCommand,
		DeleteNamespaceCommand,
		GetNamespaceCommand,
		ListTablesCommand,
		CreateTableCommand,
		DeleteTableCommand,
		GetTableCommand,
		RenameTableCommand,
		GetTableMetadataLocationCommand,
		UpdateTableMetadataLocationCommand,
		GetTableMaintenanceConfigurationCommand,
		PutTableMaintenanceConfigurationCommand,
		GetTableMaintenanceJobStatusCommand,
		GetTablePolicyCommand,
		PutTablePolicyCommand,
		DeleteTablePolicyCommand,
		GetTableEncryptionCommand,
		GetTableStorageClassCommand,
		GetTableRecordExpirationConfigurationCommand,
		PutTableRecordExpirationConfigurationCommand,
		GetTableRecordExpirationJobStatusCommand,
		type TableBucketSummary,
		type NamespaceSummary,
		type TableSummary,
		type GetTableBucketResponse,
		type GetTableResponse,
		type GetNamespaceResponse,
		type EncryptionConfiguration,
		type TableBucketMaintenanceConfigurationValue,
		type TableMaintenanceConfigurationValue,
		type TableMaintenanceJobStatusValue,
		type TableRecordExpirationConfigurationValue,
		type TableRecordExpirationJobMetrics
	} from '@aws-sdk/client-s3tables';
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
	import { Table2, Plus, Trash2, Eye } from 'lucide-svelte';

	const client = regionalClient(getS3TablesClient);

	type TabId = 'buckets' | 'namespaces' | 'tables';

	const tabs: TabDef[] = [
		{ id: 'buckets', label: 'Table Buckets' },
		{ id: 'namespaces', label: 'Namespaces' },
		{ id: 'tables', label: 'Tables' }
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

	// Bucket policy / encryption 404 (NotFoundException) when never configured
	// -- that is the correct "unconfigured" state, not a page error, so detail
	// sections treat it as such rather than surfacing an error banner.
	function isNotFound(e: unknown): boolean {
		if (!e || typeof e !== 'object') return false;
		const rec = e as { name?: unknown; $metadata?: { httpStatusCode?: number } };
		return rec.name === 'NotFoundException' || rec.$metadata?.httpStatusCode === 404;
	}

	function joinNamespace(ns: string[] | undefined): string {
		return (ns ?? []).join('.');
	}

	let activeTab = $state<TabId>('buckets');
	let searchQuery = $state('');

	let buckets = $state<TableBucketSummary[]>([]);
	let bucketsNextToken = $state<string | undefined>();
	let loadingMoreBuckets = $state(false);

	let namespaces = $state<NamespaceSummary[]>([]);
	let namespacesNextToken = $state<string | undefined>();
	let loadingMoreNamespaces = $state(false);

	let tables = $state<TableSummary[]>([]);
	let tablesNextToken = $state<string | undefined>();
	let loadingMoreTables = $state(false);

	// Namespaces and Tables are both scoped to a selected table bucket, the
	// same shared-selector pattern accessanalyzer uses for its
	// analyzer-scoped tabs -- s3tables' hierarchy is bucket -> namespace ->
	// table, so buckets is the parent resource here.
	let selectedBucketArn = $state('');
	const selectedBucket = $derived(buckets.find((b) => b.arn === selectedBucketArn));
	const selectedBucketName = $derived(selectedBucket?.name ?? '');

	// Tables additionally can be narrowed to one namespace within the
	// selected bucket (ListTables accepts an optional namespace filter).
	let selectedNamespace = $state('');

	async function fetchBuckets(reset: boolean): Promise<void> {
		const resp = await client().send(
			new ListTableBucketsCommand({ continuationToken: reset ? undefined : bucketsNextToken })
		);
		buckets = reset ? (resp.tableBuckets ?? []) : [...buckets, ...(resp.tableBuckets ?? [])];
		bucketsNextToken = resp.continuationToken;
		if (!selectedBucketArn && buckets.length > 0) {
			selectedBucketArn = buckets[0].arn ?? '';
		}
	}

	async function fetchNamespaces(reset: boolean): Promise<void> {
		if (!selectedBucketArn) {
			namespaces = [];
			namespacesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListNamespacesCommand({
				tableBucketARN: selectedBucketArn,
				continuationToken: reset ? undefined : namespacesNextToken
			})
		);
		namespaces = reset ? (resp.namespaces ?? []) : [...namespaces, ...(resp.namespaces ?? [])];
		namespacesNextToken = resp.continuationToken;
	}

	async function fetchTables(reset: boolean): Promise<void> {
		if (!selectedBucketArn) {
			tables = [];
			tablesNextToken = undefined;
			return;
		}
		const resp = await client().send(
			new ListTablesCommand({
				tableBucketARN: selectedBucketArn,
				namespace: selectedNamespace || undefined,
				continuationToken: reset ? undefined : tablesNextToken
			})
		);
		tables = reset ? (resp.tables ?? []) : [...tables, ...(resp.tables ?? [])];
		tablesNextToken = resp.continuationToken;
	}

	// Namespaces are needed for the Create Table dialog's namespace picker
	// even when the user never visited the Namespaces tab.
	async function ensureNamespacesLoaded(): Promise<void> {
		if (!selectedBucketArn || namespaces.length > 0) return;
		try {
			await fetchNamespaces(true);
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	const tabLoader = createTabLoader<TabId>({
		buckets: () => fetchBuckets(true).catch(rethrowDescribed),
		namespaces: () => fetchNamespaces(true).catch(rethrowDescribed),
		tables: () => fetchTables(true).catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
		if (activeTab === 'tables') {
			void ensureNamespacesLoaded();
		}
	}

	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	const bucketScopedTabs: TabId[] = ['namespaces', 'tables'];

	function onBucketSelect(arn: string): void {
		selectedBucketArn = arn;
		selectedNamespace = '';
		namespaces = [];
		namespacesNextToken = undefined;
		if (bucketScopedTabs.includes(activeTab)) {
			tabLoader.refresh(activeTab);
		}
	}

	function onNamespaceFilterSelect(ns: string): void {
		selectedNamespace = ns;
		tabLoader.refresh('tables');
	}

	// Buckets is the parent resource for the two bucket-scoped tabs: on a
	// region change the previously selected bucket ARN belongs to the old
	// region and must not be reused, so reload buckets first (which
	// re-selects a bucket for the new region) before reloading whichever
	// tab is active.
	onRegionChange(() => {
		selectedBucketArn = '';
		selectedNamespace = '';
		buckets = [];
		bucketsNextToken = undefined;
		namespaces = [];
		namespacesNextToken = undefined;
		void tabLoader.refresh('buckets').then(() => {
			if (activeTab !== 'buckets') {
				tabLoader.refresh(activeTab);
			}
		});
	});

	const filteredBuckets = $derived(
		buckets.filter((b) => {
			const q = searchQuery.toLowerCase();
			return (b.name ?? '').toLowerCase().includes(q) || (b.arn ?? '').toLowerCase().includes(q);
		})
	);
	const filteredNamespaces = $derived(
		namespaces.filter((n) => joinNamespace(n.namespace).toLowerCase().includes(searchQuery.toLowerCase()))
	);
	const filteredTables = $derived(
		tables.filter((t) => {
			const q = searchQuery.toLowerCase();
			return (
				(t.name ?? '').toLowerCase().includes(q) || joinNamespace(t.namespace).toLowerCase().includes(q)
			);
		})
	);
	const activeTabError = $derived(tabLoader.getError(activeTab));

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

	async function loadMoreNamespaces(): Promise<void> {
		loadingMoreNamespaces = true;
		try {
			await fetchNamespaces(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreNamespaces = false;
		}
	}

	async function loadMoreTables(): Promise<void> {
		loadingMoreTables = true;
		try {
			await fetchTables(false);
		} catch (e) {
			toast.error(describeError(e));
		} finally {
			loadingMoreTables = false;
		}
	}

	// === Table Buckets: create / delete / detail ===

	let createBucketModal = $state<Modal | null>(null);
	let creatingBucket = $state(false);
	let createBucketError = $state<string | null>(null);
	let newBucketName = $state('');
	let newBucketSSE = $state<'' | 'AES256' | 'aws:kms'>('');
	let newBucketKmsKeyArn = $state('');
	let newBucketStorageClass = $state<'' | 'STANDARD' | 'INTELLIGENT_TIERING'>('');

	function openCreateBucketModal(): void {
		createBucketError = null;
		newBucketName = '';
		newBucketSSE = '';
		newBucketKmsKeyArn = '';
		newBucketStorageClass = '';
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
			const encryptionConfiguration: EncryptionConfiguration | undefined = newBucketSSE
				? { sseAlgorithm: newBucketSSE, kmsKeyArn: newBucketSSE === 'aws:kms' ? newBucketKmsKeyArn : undefined }
				: undefined;
			await client().send(
				new CreateTableBucketCommand({
					name: newBucketName,
					encryptionConfiguration,
					storageClassConfiguration: newBucketStorageClass
						? { storageClass: newBucketStorageClass }
						: undefined
				})
			);
			toast.success('Table bucket created');
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

	async function handleDeleteBucket(b: TableBucketSummary): Promise<void> {
		if (!b.arn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete table bucket',
			message: `Delete table bucket ${b.name}? This also deletes its namespaces and tables.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTableBucketCommand({ tableBucketARN: b.arn }));
			toast.success('Table bucket deleted');
			if (selectedBucketArn === b.arn) {
				selectedBucketArn = '';
			}
			await tabLoader.refresh('buckets');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let bucketDetailModal = $state<Modal | null>(null);
	let viewedBucket = $state<GetTableBucketResponse | TableBucketSummary | null>(null);
	let bucketDetailLoading = $state(false);
	let bucketDetailError = $state<string | null>(null);

	let bucketPolicy = $state<string | null>(null);
	let bucketPolicyDraft = $state('');
	let bucketPolicyLoading = $state(false);
	let bucketPolicySaving = $state(false);
	let bucketPolicyError = $state<string | null>(null);

	let bucketEncryption = $state<EncryptionConfiguration | null>(null);
	let bucketEncryptionDraftSSE = $state<'AES256' | 'aws:kms'>('AES256');
	let bucketEncryptionDraftKms = $state('');
	let bucketEncryptionLoading = $state(false);
	let bucketEncryptionSaving = $state(false);
	let bucketEncryptionError = $state<string | null>(null);

	let bucketMaintenance = $state<TableBucketMaintenanceConfigurationValue | null>(null);
	let bucketMaintenanceDraftStatus = $state<'enabled' | 'disabled'>('enabled');
	let bucketMaintenanceDraftUnreferencedDays = $state(3);
	let bucketMaintenanceDraftNonCurrentDays = $state(10);
	let bucketMaintenanceLoading = $state(false);
	let bucketMaintenanceSaving = $state(false);
	let bucketMaintenanceError = $state<string | null>(null);

	let bucketStorageClass = $state<string | null>(null);
	let bucketStorageClassDraft = $state<'STANDARD' | 'INTELLIGENT_TIERING'>('STANDARD');
	let bucketStorageClassLoading = $state(false);
	let bucketStorageClassSaving = $state(false);
	let bucketStorageClassError = $state<string | null>(null);

	let bucketMetricsId = $state<string | null>(null);
	let bucketMetricsLoading = $state(false);
	let bucketMetricsSaving = $state(false);
	let bucketMetricsError = $state<string | null>(null);

	async function openBucketDetail(b: TableBucketSummary): Promise<void> {
		viewedBucket = b;
		bucketDetailError = null;
		bucketDetailModal?.open();
		if (!b.arn) return;
		bucketDetailLoading = true;
		try {
			const resp = await client().send(new GetTableBucketCommand({ tableBucketARN: b.arn }));
			viewedBucket = resp;
		} catch (e) {
			bucketDetailError = describeError(e);
		} finally {
			bucketDetailLoading = false;
		}
		loadBucketSubResources(b.arn);
	}

	function loadBucketSubResources(arn: string): void {
		bucketPolicy = null;
		bucketPolicyError = null;
		bucketPolicyLoading = true;
		client()
			.send(new GetTableBucketPolicyCommand({ tableBucketARN: arn }))
			.then((resp) => {
				bucketPolicy = resp.resourcePolicy ?? '';
				bucketPolicyDraft = bucketPolicy;
			})
			.catch((e) => {
				if (!isNotFound(e)) bucketPolicyError = describeError(e);
			})
			.finally(() => (bucketPolicyLoading = false));

		bucketEncryption = null;
		bucketEncryptionError = null;
		bucketEncryptionLoading = true;
		client()
			.send(new GetTableBucketEncryptionCommand({ tableBucketARN: arn }))
			.then((resp) => {
				bucketEncryption = resp.encryptionConfiguration ?? null;
				if (bucketEncryption?.sseAlgorithm) bucketEncryptionDraftSSE = bucketEncryption.sseAlgorithm;
				bucketEncryptionDraftKms = bucketEncryption?.kmsKeyArn ?? '';
			})
			.catch((e) => {
				if (!isNotFound(e)) bucketEncryptionError = describeError(e);
			})
			.finally(() => (bucketEncryptionLoading = false));

		bucketMaintenanceError = null;
		bucketMaintenanceLoading = true;
		client()
			.send(new GetTableBucketMaintenanceConfigurationCommand({ tableBucketARN: arn }))
			.then((resp) => {
				const cfg = resp.configuration?.icebergUnreferencedFileRemoval ?? null;
				bucketMaintenance = cfg;
				if (cfg?.status) bucketMaintenanceDraftStatus = cfg.status;
				const settings = cfg?.settings?.icebergUnreferencedFileRemoval;
				bucketMaintenanceDraftUnreferencedDays = settings?.unreferencedDays ?? 3;
				bucketMaintenanceDraftNonCurrentDays = settings?.nonCurrentDays ?? 10;
			})
			.catch((e) => (bucketMaintenanceError = describeError(e)))
			.finally(() => (bucketMaintenanceLoading = false));

		bucketStorageClassError = null;
		bucketStorageClassLoading = true;
		client()
			.send(new GetTableBucketStorageClassCommand({ tableBucketARN: arn }))
			.then((resp) => {
				const sc = resp.storageClassConfiguration?.storageClass ?? 'STANDARD';
				bucketStorageClass = sc;
				bucketStorageClassDraft = sc as 'STANDARD' | 'INTELLIGENT_TIERING';
			})
			.catch((e) => (bucketStorageClassError = describeError(e)))
			.finally(() => (bucketStorageClassLoading = false));

		bucketMetricsError = null;
		bucketMetricsLoading = true;
		client()
			.send(new GetTableBucketMetricsConfigurationCommand({ tableBucketARN: arn }))
			.then((resp) => (bucketMetricsId = resp.id ?? null))
			.catch((e) => (bucketMetricsError = describeError(e)))
			.finally(() => (bucketMetricsLoading = false));
	}

	async function saveBucketPolicy(): Promise<void> {
		if (!selectedBucketArn) return;
		bucketPolicySaving = true;
		bucketPolicyError = null;
		try {
			await client().send(
				new PutTableBucketPolicyCommand({
					tableBucketARN: selectedBucketArn,
					resourcePolicy: bucketPolicyDraft
				})
			);
			bucketPolicy = bucketPolicyDraft;
			toast.success('Bucket policy saved');
		} catch (e) {
			bucketPolicyError = describeError(e);
		} finally {
			bucketPolicySaving = false;
		}
	}

	async function deleteBucketPolicy(): Promise<void> {
		if (!selectedBucketArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete bucket policy',
			message: 'Remove the resource policy from this table bucket?'
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTableBucketPolicyCommand({ tableBucketARN: selectedBucketArn }));
			bucketPolicy = null;
			bucketPolicyDraft = '';
			toast.success('Bucket policy deleted');
		} catch (e) {
			bucketPolicyError = describeError(e);
		}
	}

	async function saveBucketEncryption(): Promise<void> {
		if (!selectedBucketArn) return;
		bucketEncryptionSaving = true;
		bucketEncryptionError = null;
		try {
			await client().send(
				new PutTableBucketEncryptionCommand({
					tableBucketARN: selectedBucketArn,
					encryptionConfiguration: {
						sseAlgorithm: bucketEncryptionDraftSSE,
						kmsKeyArn: bucketEncryptionDraftSSE === 'aws:kms' ? bucketEncryptionDraftKms : undefined
					}
				})
			);
			bucketEncryption = {
				sseAlgorithm: bucketEncryptionDraftSSE,
				kmsKeyArn: bucketEncryptionDraftSSE === 'aws:kms' ? bucketEncryptionDraftKms : undefined
			};
			toast.success('Bucket encryption saved');
		} catch (e) {
			bucketEncryptionError = describeError(e);
		} finally {
			bucketEncryptionSaving = false;
		}
	}

	async function deleteBucketEncryption(): Promise<void> {
		if (!selectedBucketArn) return;
		const confirmed = await confirmDestructive({
			title: 'Delete bucket encryption',
			message: 'Remove the explicit encryption configuration from this table bucket?'
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTableBucketEncryptionCommand({ tableBucketARN: selectedBucketArn }));
			bucketEncryption = null;
			toast.success('Bucket encryption deleted');
		} catch (e) {
			bucketEncryptionError = describeError(e);
		}
	}

	async function saveBucketMaintenance(): Promise<void> {
		if (!selectedBucketArn) return;
		bucketMaintenanceSaving = true;
		bucketMaintenanceError = null;
		try {
			const value: TableBucketMaintenanceConfigurationValue = {
				status: bucketMaintenanceDraftStatus,
				settings: {
					icebergUnreferencedFileRemoval: {
						unreferencedDays: bucketMaintenanceDraftUnreferencedDays,
						nonCurrentDays: bucketMaintenanceDraftNonCurrentDays
					}
				}
			};
			await client().send(
				new PutTableBucketMaintenanceConfigurationCommand({
					tableBucketARN: selectedBucketArn,
					type: 'icebergUnreferencedFileRemoval',
					value
				})
			);
			bucketMaintenance = value;
			toast.success('Bucket maintenance configuration saved');
		} catch (e) {
			bucketMaintenanceError = describeError(e);
		} finally {
			bucketMaintenanceSaving = false;
		}
	}

	async function saveBucketStorageClass(): Promise<void> {
		if (!selectedBucketArn) return;
		bucketStorageClassSaving = true;
		bucketStorageClassError = null;
		try {
			await client().send(
				new PutTableBucketStorageClassCommand({
					tableBucketARN: selectedBucketArn,
					storageClassConfiguration: { storageClass: bucketStorageClassDraft }
				})
			);
			bucketStorageClass = bucketStorageClassDraft;
			toast.success('Bucket storage class saved');
		} catch (e) {
			bucketStorageClassError = describeError(e);
		} finally {
			bucketStorageClassSaving = false;
		}
	}

	async function enableBucketMetrics(): Promise<void> {
		if (!selectedBucketArn) return;
		bucketMetricsSaving = true;
		bucketMetricsError = null;
		try {
			await client().send(new PutTableBucketMetricsConfigurationCommand({ tableBucketARN: selectedBucketArn }));
			const resp = await client().send(
				new GetTableBucketMetricsConfigurationCommand({ tableBucketARN: selectedBucketArn })
			);
			bucketMetricsId = resp.id ?? null;
			toast.success('Bucket metrics enabled');
		} catch (e) {
			bucketMetricsError = describeError(e);
		} finally {
			bucketMetricsSaving = false;
		}
	}

	async function disableBucketMetrics(): Promise<void> {
		if (!selectedBucketArn) return;
		const confirmed = await confirmDestructive({
			title: 'Disable bucket metrics',
			message: 'Disable CloudWatch metrics for this table bucket?'
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteTableBucketMetricsConfigurationCommand({ tableBucketARN: selectedBucketArn })
			);
			bucketMetricsId = null;
			toast.success('Bucket metrics disabled');
		} catch (e) {
			bucketMetricsError = describeError(e);
		}
	}

	// === Namespaces: create / delete / detail ===

	let createNamespaceModal = $state<Modal | null>(null);
	let creatingNamespace = $state(false);
	let createNamespaceError = $state<string | null>(null);
	let newNamespaceName = $state('');

	function openCreateNamespaceModal(): void {
		createNamespaceError = selectedBucketArn ? null : 'Select a table bucket first.';
		newNamespaceName = '';
		createNamespaceModal?.open();
	}

	async function submitCreateNamespace(): Promise<void> {
		if (!selectedBucketArn) {
			createNamespaceError = 'Select a table bucket first.';
			return;
		}
		if (!newNamespaceName) {
			createNamespaceError = 'Namespace name is required.';
			return;
		}
		creatingNamespace = true;
		createNamespaceError = null;
		try {
			await client().send(
				new CreateNamespaceCommand({ tableBucketARN: selectedBucketArn, namespace: [newNamespaceName] })
			);
			toast.success('Namespace created');
			createNamespaceModal?.close();
			await tabLoader.refresh('namespaces');
		} catch (e) {
			const msg = describeError(e);
			createNamespaceError = msg;
			toast.error(msg);
		} finally {
			creatingNamespace = false;
		}
	}

	async function handleDeleteNamespace(n: NamespaceSummary): Promise<void> {
		if (!selectedBucketArn) return;
		const nsName = joinNamespace(n.namespace);
		const confirmed = await confirmDestructive({
			title: 'Delete namespace',
			message: `Delete namespace ${nsName}? The namespace must be empty of tables.`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteNamespaceCommand({ tableBucketARN: selectedBucketArn, namespace: nsName })
			);
			toast.success('Namespace deleted');
			await tabLoader.refresh('namespaces');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let namespaceDetailModal = $state<Modal | null>(null);
	let viewedNamespace = $state<GetNamespaceResponse | NamespaceSummary | null>(null);
	let namespaceDetailLoading = $state(false);
	let namespaceDetailError = $state<string | null>(null);

	async function openNamespaceDetail(n: NamespaceSummary): Promise<void> {
		viewedNamespace = n;
		namespaceDetailError = null;
		namespaceDetailModal?.open();
		if (!selectedBucketArn) return;
		namespaceDetailLoading = true;
		try {
			const resp = await client().send(
				new GetNamespaceCommand({ tableBucketARN: selectedBucketArn, namespace: joinNamespace(n.namespace) })
			);
			viewedNamespace = resp;
		} catch (e) {
			namespaceDetailError = describeError(e);
		} finally {
			namespaceDetailLoading = false;
		}
	}

	// === Tables: create / delete / detail ===

	let createTableModal = $state<Modal | null>(null);
	let creatingTable = $state(false);
	let createTableError = $state<string | null>(null);
	let newTableNamespace = $state('');
	let newTableName = $state('');
	let newTableSSE = $state<'' | 'AES256' | 'aws:kms'>('');
	let newTableKmsKeyArn = $state('');
	let newTableStorageClass = $state<'' | 'STANDARD' | 'INTELLIGENT_TIERING'>('');

	async function openCreateTableModal(): Promise<void> {
		createTableError = selectedBucketArn ? null : 'Select a table bucket first.';
		await ensureNamespacesLoaded();
		newTableNamespace = selectedNamespace || joinNamespace(namespaces[0]?.namespace) || '';
		newTableName = '';
		newTableSSE = '';
		newTableKmsKeyArn = '';
		newTableStorageClass = '';
		createTableModal?.open();
	}

	async function submitCreateTable(): Promise<void> {
		if (!selectedBucketArn) {
			createTableError = 'Select a table bucket first.';
			return;
		}
		if (!newTableNamespace || !newTableName) {
			createTableError = 'Namespace and table name are required.';
			return;
		}
		creatingTable = true;
		createTableError = null;
		try {
			const encryptionConfiguration: EncryptionConfiguration | undefined = newTableSSE
				? { sseAlgorithm: newTableSSE, kmsKeyArn: newTableSSE === 'aws:kms' ? newTableKmsKeyArn : undefined }
				: undefined;
			await client().send(
				new CreateTableCommand({
					tableBucketARN: selectedBucketArn,
					namespace: newTableNamespace,
					name: newTableName,
					format: 'ICEBERG',
					encryptionConfiguration,
					storageClassConfiguration: newTableStorageClass
						? { storageClass: newTableStorageClass }
						: undefined
				})
			);
			toast.success('Table created');
			createTableModal?.close();
			await tabLoader.refresh('tables');
		} catch (e) {
			const msg = describeError(e);
			createTableError = msg;
			toast.error(msg);
		} finally {
			creatingTable = false;
		}
	}

	async function handleDeleteTable(t: TableSummary): Promise<void> {
		if (!selectedBucketArn || !t.name) return;
		const confirmed = await confirmDestructive({
			title: 'Delete table',
			message: `Delete table ${joinNamespace(t.namespace)}.${t.name}?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteTableCommand({
					tableBucketARN: selectedBucketArn,
					namespace: joinNamespace(t.namespace),
					name: t.name
				})
			);
			toast.success('Table deleted');
			await tabLoader.refresh('tables');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	let tableDetailModal = $state<Modal | null>(null);
	let viewedTable = $state<GetTableResponse | TableSummary | null>(null);
	let tableDetailLoading = $state(false);
	let tableDetailError = $state<string | null>(null);

	let tableMetadataLocation = $state<{ versionToken: string; metadataLocation?: string; warehouseLocation: string } | null>(null);
	let tableMetadataLocationDraft = $state('');
	let tableMetadataLocationLoading = $state(false);
	let tableMetadataLocationSaving = $state(false);
	let tableMetadataLocationError = $state<string | null>(null);

	let tableMaintenance = $state<Record<string, TableMaintenanceConfigurationValue>>({});
	let tableMaintenanceDraftType = $state<'icebergCompaction' | 'icebergSnapshotManagement'>('icebergCompaction');
	let tableMaintenanceDraftStatus = $state<'enabled' | 'disabled'>('enabled');
	let tableMaintenanceDraftTargetFileSizeMB = $state(512);
	let tableMaintenanceDraftMinSnapshots = $state(1);
	let tableMaintenanceDraftMaxSnapshotAgeHours = $state(120);
	let tableMaintenanceLoading = $state(false);
	let tableMaintenanceSaving = $state(false);
	let tableMaintenanceError = $state<string | null>(null);

	let tableMaintenanceJobStatus = $state<Record<string, TableMaintenanceJobStatusValue>>({});
	let tableMaintenanceJobStatusLoading = $state(false);
	let tableMaintenanceJobStatusError = $state<string | null>(null);

	let tablePolicy = $state<string | null>(null);
	let tablePolicyDraft = $state('');
	let tablePolicyLoading = $state(false);
	let tablePolicySaving = $state(false);
	let tablePolicyError = $state<string | null>(null);

	let tableEncryption = $state<EncryptionConfiguration | null>(null);
	let tableEncryptionLoading = $state(false);
	let tableEncryptionError = $state<string | null>(null);

	let tableStorageClass = $state<string | null>(null);
	let tableStorageClassLoading = $state(false);
	let tableStorageClassError = $state<string | null>(null);

	let tableRecordExpiration = $state<TableRecordExpirationConfigurationValue | null>(null);
	let tableRecordExpirationDraftStatus = $state<'enabled' | 'disabled'>('disabled');
	let tableRecordExpirationDraftDays = $state(365);
	let tableRecordExpirationLoading = $state(false);
	let tableRecordExpirationSaving = $state(false);
	let tableRecordExpirationError = $state<string | null>(null);

	let tableRecordExpirationJobStatus = $state<{
		status: string;
		lastRunTimestamp?: Date;
		failureMessage?: string;
		metrics?: TableRecordExpirationJobMetrics;
	} | null>(null);
	let tableRecordExpirationJobStatusLoading = $state(false);
	let tableRecordExpirationJobStatusError = $state<string | null>(null);

	let renameDraftNamespace = $state('');
	let renameDraftName = $state('');
	let renaming = $state(false);
	let renameError = $state<string | null>(null);

	function currentTableNamespace(): string {
		return joinNamespace((viewedTable as GetTableResponse | TableSummary | null)?.namespace);
	}

	function currentTableName(): string {
		return (viewedTable as GetTableResponse | TableSummary | null)?.name ?? '';
	}

	async function openTableDetail(t: TableSummary): Promise<void> {
		viewedTable = t;
		tableDetailError = null;
		tableDetailModal?.open();
		if (!selectedBucketArn || !t.name) return;
		const namespace = joinNamespace(t.namespace);
		const name = t.name;
		renameDraftNamespace = namespace;
		renameDraftName = name;
		renameError = null;

		tableDetailLoading = true;
		try {
			const resp = await client().send(
				new GetTableCommand({ tableBucketARN: selectedBucketArn, namespace, name })
			);
			viewedTable = resp;
		} catch (e) {
			tableDetailError = describeError(e);
		} finally {
			tableDetailLoading = false;
		}
		loadTableSubResources(namespace, name);
	}

	function loadTableSubResources(namespace: string, name: string): void {
		const bucketARN = selectedBucketArn;

		tableMetadataLocationError = null;
		tableMetadataLocationLoading = true;
		client()
			.send(new GetTableMetadataLocationCommand({ tableBucketARN: bucketARN, namespace, name }))
			.then((resp) => {
				tableMetadataLocation = {
					versionToken: resp.versionToken ?? '',
					metadataLocation: resp.metadataLocation,
					warehouseLocation: resp.warehouseLocation ?? ''
				};
				tableMetadataLocationDraft = resp.metadataLocation ?? '';
			})
			.catch((e) => (tableMetadataLocationError = describeError(e)))
			.finally(() => (tableMetadataLocationLoading = false));

		tableMaintenanceError = null;
		tableMaintenanceLoading = true;
		client()
			.send(new GetTableMaintenanceConfigurationCommand({ tableBucketARN: bucketARN, namespace, name }))
			.then((resp) => {
				tableMaintenance = (resp.configuration ?? {}) as Record<string, TableMaintenanceConfigurationValue>;
				const compaction = tableMaintenance.icebergCompaction;
				const snapshotMgmt = tableMaintenance.icebergSnapshotManagement;
				if (compaction) {
					tableMaintenanceDraftTargetFileSizeMB =
						compaction.settings?.icebergCompaction?.targetFileSizeMB ?? 512;
				}
				if (snapshotMgmt) {
					tableMaintenanceDraftMinSnapshots =
						snapshotMgmt.settings?.icebergSnapshotManagement?.minSnapshotsToKeep ?? 1;
					tableMaintenanceDraftMaxSnapshotAgeHours =
						snapshotMgmt.settings?.icebergSnapshotManagement?.maxSnapshotAgeHours ?? 120;
				}
			})
			.catch((e) => (tableMaintenanceError = describeError(e)))
			.finally(() => (tableMaintenanceLoading = false));

		tableMaintenanceJobStatusError = null;
		tableMaintenanceJobStatusLoading = true;
		client()
			.send(new GetTableMaintenanceJobStatusCommand({ tableBucketARN: bucketARN, namespace, name }))
			.then((resp) => {
				tableMaintenanceJobStatus = (resp.status ?? {}) as Record<string, TableMaintenanceJobStatusValue>;
			})
			.catch((e) => (tableMaintenanceJobStatusError = describeError(e)))
			.finally(() => (tableMaintenanceJobStatusLoading = false));

		tablePolicy = null;
		tablePolicyError = null;
		tablePolicyLoading = true;
		client()
			.send(new GetTablePolicyCommand({ tableBucketARN: bucketARN, namespace, name }))
			.then((resp) => {
				tablePolicy = resp.resourcePolicy ?? '';
				tablePolicyDraft = tablePolicy;
			})
			.catch((e) => {
				if (!isNotFound(e)) tablePolicyError = describeError(e);
			})
			.finally(() => (tablePolicyLoading = false));

		tableEncryptionError = null;
		tableEncryptionLoading = true;
		client()
			.send(new GetTableEncryptionCommand({ tableBucketARN: bucketARN, namespace, name }))
			.then((resp) => (tableEncryption = resp.encryptionConfiguration ?? null))
			.catch((e) => (tableEncryptionError = describeError(e)))
			.finally(() => (tableEncryptionLoading = false));

		tableStorageClassError = null;
		tableStorageClassLoading = true;
		client()
			.send(new GetTableStorageClassCommand({ tableBucketARN: bucketARN, namespace, name }))
			.then((resp) => (tableStorageClass = resp.storageClassConfiguration?.storageClass ?? 'STANDARD'))
			.catch((e) => (tableStorageClassError = describeError(e)))
			.finally(() => (tableStorageClassLoading = false));

		const tableArn = (viewedTable as GetTableResponse | null)?.tableARN;
		if (tableArn) {
			tableRecordExpirationError = null;
			tableRecordExpirationLoading = true;
			client()
				.send(new GetTableRecordExpirationConfigurationCommand({ tableArn }))
				.then((resp) => {
					tableRecordExpiration = resp.configuration ?? null;
					if (resp.configuration?.status) tableRecordExpirationDraftStatus = resp.configuration.status;
					tableRecordExpirationDraftDays = resp.configuration?.settings?.days ?? 365;
				})
				.catch((e) => (tableRecordExpirationError = describeError(e)))
				.finally(() => (tableRecordExpirationLoading = false));

			tableRecordExpirationJobStatusError = null;
			tableRecordExpirationJobStatusLoading = true;
			client()
				.send(new GetTableRecordExpirationJobStatusCommand({ tableArn }))
				.then((resp) => {
					tableRecordExpirationJobStatus = {
						status: resp.status ?? '',
						lastRunTimestamp: resp.lastRunTimestamp,
						failureMessage: resp.failureMessage,
						metrics: resp.metrics
					};
				})
				.catch((e) => (tableRecordExpirationJobStatusError = describeError(e)))
				.finally(() => (tableRecordExpirationJobStatusLoading = false));
		}
	}

	async function saveTableMetadataLocation(): Promise<void> {
		if (!selectedBucketArn || !tableMetadataLocation) return;
		const namespace = currentTableNamespace();
		const name = currentTableName();
		tableMetadataLocationSaving = true;
		tableMetadataLocationError = null;
		try {
			const resp = await client().send(
				new UpdateTableMetadataLocationCommand({
					tableBucketARN: selectedBucketArn,
					namespace,
					name,
					versionToken: tableMetadataLocation.versionToken,
					metadataLocation: tableMetadataLocationDraft
				})
			);
			tableMetadataLocation = {
				versionToken: resp.versionToken ?? '',
				metadataLocation: resp.metadataLocation,
				warehouseLocation: tableMetadataLocation.warehouseLocation
			};
			toast.success('Table metadata location updated');
		} catch (e) {
			tableMetadataLocationError = describeError(e);
		} finally {
			tableMetadataLocationSaving = false;
		}
	}

	async function saveTableMaintenance(): Promise<void> {
		if (!selectedBucketArn) return;
		const namespace = currentTableNamespace();
		const name = currentTableName();
		tableMaintenanceSaving = true;
		tableMaintenanceError = null;
		try {
			const value: TableMaintenanceConfigurationValue =
				tableMaintenanceDraftType === 'icebergCompaction'
					? {
							status: tableMaintenanceDraftStatus,
							settings: { icebergCompaction: { targetFileSizeMB: tableMaintenanceDraftTargetFileSizeMB } }
						}
					: {
							status: tableMaintenanceDraftStatus,
							settings: {
								icebergSnapshotManagement: {
									minSnapshotsToKeep: tableMaintenanceDraftMinSnapshots,
									maxSnapshotAgeHours: tableMaintenanceDraftMaxSnapshotAgeHours
								}
							}
						};
			await client().send(
				new PutTableMaintenanceConfigurationCommand({
					tableBucketARN: selectedBucketArn,
					namespace,
					name,
					type: tableMaintenanceDraftType,
					value
				})
			);
			tableMaintenance = { ...tableMaintenance, [tableMaintenanceDraftType]: value };
			toast.success('Table maintenance configuration saved');
		} catch (e) {
			tableMaintenanceError = describeError(e);
		} finally {
			tableMaintenanceSaving = false;
		}
	}

	async function saveTablePolicy(): Promise<void> {
		if (!selectedBucketArn) return;
		const namespace = currentTableNamespace();
		const name = currentTableName();
		tablePolicySaving = true;
		tablePolicyError = null;
		try {
			await client().send(
				new PutTablePolicyCommand({ tableBucketARN: selectedBucketArn, namespace, name, resourcePolicy: tablePolicyDraft })
			);
			tablePolicy = tablePolicyDraft;
			toast.success('Table policy saved');
		} catch (e) {
			tablePolicyError = describeError(e);
		} finally {
			tablePolicySaving = false;
		}
	}

	async function deleteTablePolicy(): Promise<void> {
		if (!selectedBucketArn) return;
		const namespace = currentTableNamespace();
		const name = currentTableName();
		const confirmed = await confirmDestructive({
			title: 'Delete table policy',
			message: 'Remove the resource policy from this table?'
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteTablePolicyCommand({ tableBucketARN: selectedBucketArn, namespace, name }));
			tablePolicy = null;
			tablePolicyDraft = '';
			toast.success('Table policy deleted');
		} catch (e) {
			tablePolicyError = describeError(e);
		}
	}

	async function saveTableRecordExpiration(): Promise<void> {
		const tableArn = (viewedTable as GetTableResponse | null)?.tableARN;
		if (!tableArn) return;
		tableRecordExpirationSaving = true;
		tableRecordExpirationError = null;
		try {
			const value: TableRecordExpirationConfigurationValue = {
				status: tableRecordExpirationDraftStatus,
				settings: { days: tableRecordExpirationDraftDays }
			};
			await client().send(new PutTableRecordExpirationConfigurationCommand({ tableArn, value }));
			tableRecordExpiration = value;
			toast.success('Table record expiration configuration saved');
		} catch (e) {
			tableRecordExpirationError = describeError(e);
		} finally {
			tableRecordExpirationSaving = false;
		}
	}

	async function submitRenameTable(): Promise<void> {
		if (!selectedBucketArn || !tableMetadataLocation) return;
		const namespace = currentTableNamespace();
		const name = currentTableName();
		if (!renameDraftNamespace || !renameDraftName) {
			renameError = 'Namespace and name are required.';
			return;
		}
		renaming = true;
		renameError = null;
		try {
			await client().send(
				new RenameTableCommand({
					tableBucketARN: selectedBucketArn,
					namespace,
					name,
					newNamespaceName: renameDraftNamespace === namespace ? undefined : renameDraftNamespace,
					newName: renameDraftName === name ? undefined : renameDraftName,
					versionToken: tableMetadataLocation.versionToken
				})
			);
			toast.success('Table renamed');
			tableDetailModal?.close();
			await tabLoader.refresh('tables');
		} catch (e) {
			const msg = describeError(e);
			renameError = msg;
			toast.error(msg);
		} finally {
			renaming = false;
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Table2}
		title="Amazon S3 Tables"
		description="Managed Apache Iceberg tables optimized for analytics workloads"
		onRefresh={handleRefresh}
		color="blue"
	>
		{#snippet actions()}
			{#if activeTab === 'buckets'}
				<button
					onclick={openCreateBucketModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create table bucket
				</button>
			{:else if activeTab === 'namespaces'}
				<button
					onclick={openCreateNamespaceModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create namespace
				</button>
			{:else if activeTab === 'tables'}
				<button
					onclick={openCreateTableModal}
					class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700 text-sm"
				>
					<Plus class="w-4 h-4" /> Create table
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div
			class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between"
		>
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="blue" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if bucketScopedTabs.includes(activeTab)}
				<div class="flex items-center gap-2 flex-wrap">
					<label for="bucket-select" class="text-sm text-gray-500 dark:text-gray-400">Table bucket</label>
					<select
						id="bucket-select"
						value={selectedBucketArn}
						onchange={(e) => onBucketSelect((e.target as HTMLSelectElement).value)}
						class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-md truncate"
					>
						{#if buckets.length === 0}
							<option value="">No table buckets</option>
						{/if}
						{#each buckets as b (b.arn)}
							<option value={b.arn}>{b.name}</option>
						{/each}
					</select>

					{#if activeTab === 'tables'}
						<label for="namespace-filter" class="text-sm text-gray-500 dark:text-gray-400">Namespace</label>
						<select
							id="namespace-filter"
							value={selectedNamespace}
							onchange={(e) => onNamespaceFilterSelect((e.target as HTMLSelectElement).value)}
							class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white max-w-full sm:max-w-xs truncate"
						>
							<option value="">All namespaces</option>
							{#each namespaces as n (joinNamespace(n.namespace))}
								<option value={joinNamespace(n.namespace)}>{joinNamespace(n.namespace)}</option>
							{/each}
						</select>
					{/if}
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

			{#if activeTab === 'buckets'}
				{#snippet bucketOwnerCell(b: TableBucketSummary)}
					{b.ownerAccountId ?? '—'}
				{/snippet}
				{#snippet bucketCreatedCell(b: TableBucketSummary)}
					{formatDate(b.createdAt)}
				{/snippet}
				{#snippet bucketActionsCell(b: TableBucketSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openBucketDetail(b)}
							title="View"
							aria-label="View table bucket {b.name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteBucket(b)}
							title="Delete"
							aria-label="Delete table bucket {b.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const bucketColumns = defineColumns<TableBucketSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'ownerAccountId', label: 'Owner account', render: bucketOwnerCell },
					{ key: 'createdAt', label: 'Created', render: bucketCreatedCell },
					{ key: 'actions', label: '', render: bucketActionsCell }
				])}
				<DataTable
					rows={filteredBuckets}
					rowKey={(b) => b.arn ?? ''}
					columns={bucketColumns}
					loading={tabLoader.isLoading('buckets')}
					emptyMessage="No table buckets found"
				/>
				<LoadMore hasMore={!!bucketsNextToken} loading={loadingMoreBuckets} onLoadMore={loadMoreBuckets} />
			{:else if activeTab === 'namespaces'}
				{#snippet namespaceNameCell(n: NamespaceSummary)}
					{joinNamespace(n.namespace)}
				{/snippet}
				{#snippet namespaceCreatedCell(n: NamespaceSummary)}
					{formatDate(n.createdAt)}
				{/snippet}
				{#snippet namespaceActionsCell(n: NamespaceSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openNamespaceDetail(n)}
							title="View"
							aria-label="View namespace {joinNamespace(n.namespace)}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteNamespace(n)}
							title="Delete"
							aria-label="Delete namespace {joinNamespace(n.namespace)}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const namespaceColumns = defineColumns<NamespaceSummary>([
					{ key: 'namespace', label: 'Namespace', render: namespaceNameCell },
					{ key: 'createdBy', label: 'Created by' },
					{ key: 'createdAt', label: 'Created', render: namespaceCreatedCell },
					{ key: 'actions', label: '', render: namespaceActionsCell }
				])}
				<DataTable
					rows={filteredNamespaces}
					rowKey={(n) => joinNamespace(n.namespace)}
					columns={namespaceColumns}
					loading={tabLoader.isLoading('namespaces')}
					emptyMessage={selectedBucketName ? 'No namespaces found' : 'Select a table bucket to see its namespaces'}
				/>
				<LoadMore
					hasMore={!!namespacesNextToken}
					loading={loadingMoreNamespaces}
					onLoadMore={loadMoreNamespaces}
				/>
			{:else if activeTab === 'tables'}
				{#snippet tableNamespaceCell(t: TableSummary)}
					{joinNamespace(t.namespace)}
				{/snippet}
				{#snippet tableModifiedCell(t: TableSummary)}
					{formatDate(t.modifiedAt)}
				{/snippet}
				{#snippet tableActionsCell(t: TableSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button
							onclick={() => openTableDetail(t)}
							title="View"
							aria-label="View table {t.name}"
							class="text-gray-400 hover:text-blue-500"><Eye class="w-4 h-4" /></button
						>
						<button
							onclick={() => handleDeleteTable(t)}
							title="Delete"
							aria-label="Delete table {t.name}"
							class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button
						>
					</div>
				{/snippet}
				{@const tableColumns = defineColumns<TableSummary>([
					{ key: 'name', label: 'Name' },
					{ key: 'namespace', label: 'Namespace', render: tableNamespaceCell },
					{ key: 'type', label: 'Type' },
					{ key: 'modifiedAt', label: 'Modified', render: tableModifiedCell },
					{ key: 'actions', label: '', render: tableActionsCell }
				])}
				<DataTable
					rows={filteredTables}
					rowKey={(t) => t.tableARN ?? ''}
					columns={tableColumns}
					loading={tabLoader.isLoading('tables')}
					emptyMessage={selectedBucketName ? 'No tables found' : 'Select a table bucket to see its tables'}
				/>
				<LoadMore hasMore={!!tablesNextToken} loading={loadingMoreTables} onLoadMore={loadMoreTables} />
			{/if}
		</div>
	</div>
</div>

<Modal bind:this={createBucketModal} title="Create Table Bucket">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="bucket-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input
					id="bucket-name"
					bind:value={newBucketName}
					placeholder="my-table-bucket"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<div>
				<label for="bucket-sse" class="text-sm text-slate-600 dark:text-slate-300">Encryption (optional)</label>
				<select
					id="bucket-sse"
					bind:value={newBucketSSE}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Default (AES256)</option>
					<option value="AES256">AES256</option>
					<option value="aws:kms">aws:kms</option>
				</select>
			</div>
			{#if newBucketSSE === 'aws:kms'}
				<div>
					<label for="bucket-kms" class="text-sm text-slate-600 dark:text-slate-300">KMS key ARN</label>
					<input
						id="bucket-kms"
						bind:value={newBucketKmsKeyArn}
						placeholder="arn:aws:kms:..."
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			{/if}
			<div>
				<label for="bucket-storage-class" class="text-sm text-slate-600 dark:text-slate-300"
					>Default storage class (optional)</label
				>
				<select
					id="bucket-storage-class"
					bind:value={newBucketStorageClass}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Service default</option>
					<option value="STANDARD">STANDARD</option>
					<option value="INTELLIGENT_TIERING">INTELLIGENT_TIERING</option>
				</select>
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
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingBucket ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={bucketDetailModal} title="Table Bucket">
	{#snippet children()}
		{#if bucketDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedBucket}
			<div class="max-h-[70vh] overflow-y-auto space-y-5 pr-1">
				<dl class="text-sm space-y-2">
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Name</dt>
						<dd class="text-slate-900 dark:text-white">{viewedBucket.name ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
						<dd class="break-all text-slate-900 dark:text-white">{viewedBucket.arn ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Owner account</dt>
						<dd class="text-slate-900 dark:text-white">{viewedBucket.ownerAccountId ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Created</dt>
						<dd class="text-slate-900 dark:text-white">{formatDate(viewedBucket.createdAt)}</dd>
					</div>
				</dl>
				{#if bucketDetailError}
					<p class="text-sm text-red-600 dark:text-red-400">{bucketDetailError}</p>
				{/if}

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Resource policy</h3>
					{#if bucketPolicyLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<textarea
							bind:value={bucketPolicyDraft}
							rows="3"
							aria-label="Bucket policy JSON"
							placeholder="No policy set"
							class="w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						></textarea>
						<div class="flex gap-2">
							<button
								onclick={saveBucketPolicy}
								disabled={bucketPolicySaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Save policy</button
							>
							{#if bucketPolicy !== null}
								<button
									onclick={deleteBucketPolicy}
									class="rounded-lg border border-red-300 px-3 py-1.5 text-xs font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:text-red-400"
									>Delete policy</button
								>
							{/if}
						</div>
						{#if bucketPolicyError}
							<p class="text-xs text-red-600 dark:text-red-400">{bucketPolicyError}</p>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Encryption</h3>
					{#if bucketEncryptionLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<p class="text-xs text-slate-500 dark:text-slate-400">
							Current: {bucketEncryption?.sseAlgorithm ?? 'Not explicitly configured (AES256 default)'}
						</p>
						<div class="flex gap-2 items-center flex-wrap">
							<select
								bind:value={bucketEncryptionDraftSSE}
								aria-label="Bucket SSE algorithm"
								class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							>
								<option value="AES256">AES256</option>
								<option value="aws:kms">aws:kms</option>
							</select>
							{#if bucketEncryptionDraftSSE === 'aws:kms'}
								<input
									bind:value={bucketEncryptionDraftKms}
									placeholder="KMS key ARN"
									aria-label="Bucket KMS key ARN"
									class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white flex-1 min-w-[10rem]"
								/>
							{/if}
							<button
								onclick={saveBucketEncryption}
								disabled={bucketEncryptionSaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Save</button
							>
							{#if bucketEncryption}
								<button
									onclick={deleteBucketEncryption}
									class="rounded-lg border border-red-300 px-3 py-1.5 text-xs font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:text-red-400"
									>Delete</button
								>
							{/if}
						</div>
						{#if bucketEncryptionError}
							<p class="text-xs text-red-600 dark:text-red-400">{bucketEncryptionError}</p>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">
						Maintenance (unreferenced file removal)
					</h3>
					{#if bucketMaintenanceLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<div class="flex gap-2 items-center flex-wrap">
							<select
								bind:value={bucketMaintenanceDraftStatus}
								aria-label="Bucket maintenance status"
								class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							>
								<option value="enabled">enabled</option>
								<option value="disabled">disabled</option>
							</select>
							<label class="text-xs text-slate-500 dark:text-slate-400" for="bmd-unref"
								>Unreferenced days</label
							>
							<input
								id="bmd-unref"
								type="number"
								bind:value={bucketMaintenanceDraftUnreferencedDays}
								class="w-20 px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
							<label class="text-xs text-slate-500 dark:text-slate-400" for="bmd-noncurrent"
								>Non-current days</label
							>
							<input
								id="bmd-noncurrent"
								type="number"
								bind:value={bucketMaintenanceDraftNonCurrentDays}
								class="w-20 px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
							<button
								onclick={saveBucketMaintenance}
								disabled={bucketMaintenanceSaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Save</button
							>
						</div>
						{#if bucketMaintenanceError}
							<p class="text-xs text-red-600 dark:text-red-400">{bucketMaintenanceError}</p>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Default storage class</h3>
					{#if bucketStorageClassLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<div class="flex gap-2 items-center">
							<select
								bind:value={bucketStorageClassDraft}
								aria-label="Bucket storage class"
								class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							>
								<option value="STANDARD">STANDARD</option>
								<option value="INTELLIGENT_TIERING">INTELLIGENT_TIERING</option>
							</select>
							<button
								onclick={saveBucketStorageClass}
								disabled={bucketStorageClassSaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Save</button
							>
						</div>
						{#if bucketStorageClassError}
							<p class="text-xs text-red-600 dark:text-red-400">{bucketStorageClassError}</p>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Metrics</h3>
					{#if bucketMetricsLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<p class="text-xs text-slate-500 dark:text-slate-400">
							{bucketMetricsId ? `Enabled (id: ${bucketMetricsId})` : 'Disabled'}
						</p>
						{#if bucketMetricsId}
							<button
								onclick={disableBucketMetrics}
								disabled={bucketMetricsSaving}
								class="rounded-lg border border-red-300 px-3 py-1.5 text-xs font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:text-red-400 disabled:opacity-50"
								>Disable</button
							>
						{:else}
							<button
								onclick={enableBucketMetrics}
								disabled={bucketMetricsSaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Enable</button
							>
						{/if}
						{#if bucketMetricsError}
							<p class="text-xs text-red-600 dark:text-red-400">{bucketMetricsError}</p>
						{/if}
					{/if}
				</div>
			</div>
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

<Modal bind:this={createNamespaceModal} title="Create Namespace">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				In table bucket <span class="font-medium">{selectedBucketName || '(none selected)'}</span>.
			</p>
			<div>
				<label for="namespace-name" class="text-sm text-slate-600 dark:text-slate-300">Namespace name</label>
				<input
					id="namespace-name"
					bind:value={newNamespaceName}
					placeholder="my_namespace"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			{#if createNamespaceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createNamespaceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createNamespaceModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateNamespace}
			disabled={creatingNamespace}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingNamespace ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={namespaceDetailModal} title="Namespace">
	{#snippet children()}
		{#if namespaceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedNamespace}
			<dl class="text-sm space-y-2">
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Namespace</dt>
					<dd class="text-slate-900 dark:text-white">{joinNamespace(viewedNamespace.namespace)}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created by</dt>
					<dd class="text-slate-900 dark:text-white">{viewedNamespace.createdBy ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Owner account</dt>
					<dd class="text-slate-900 dark:text-white">{viewedNamespace.ownerAccountId ?? '—'}</dd>
				</div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Created</dt>
					<dd class="text-slate-900 dark:text-white">{formatDate(viewedNamespace.createdAt)}</dd>
				</div>
			</dl>
			{#if namespaceDetailError}
				<p class="mt-2 text-sm text-red-600 dark:text-red-400">{namespaceDetailError}</p>
			{/if}
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => namespaceDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>

<Modal bind:this={createTableModal} title="Create Table">
	{#snippet children()}
		<div class="space-y-3">
			<p class="text-sm text-slate-600 dark:text-slate-300">
				In table bucket <span class="font-medium">{selectedBucketName || '(none selected)'}</span>.
			</p>
			<div>
				<label for="table-namespace" class="text-sm text-slate-600 dark:text-slate-300">Namespace</label>
				<select
					id="table-namespace"
					bind:value={newTableNamespace}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					{#if namespaces.length === 0}
						<option value="">No namespaces -- create one first</option>
					{/if}
					{#each namespaces as n (joinNamespace(n.namespace))}
						<option value={joinNamespace(n.namespace)}>{joinNamespace(n.namespace)}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="table-name" class="text-sm text-slate-600 dark:text-slate-300">Table name</label>
				<input
					id="table-name"
					bind:value={newTableName}
					placeholder="my_table"
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				/>
			</div>
			<p class="text-xs text-slate-500 dark:text-slate-400">Format: ICEBERG (the only format S3 Tables supports)</p>
			<div>
				<label for="table-sse" class="text-sm text-slate-600 dark:text-slate-300">Encryption override (optional)</label>
				<select
					id="table-sse"
					bind:value={newTableSSE}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Inherit from bucket</option>
					<option value="AES256">AES256</option>
					<option value="aws:kms">aws:kms</option>
				</select>
			</div>
			{#if newTableSSE === 'aws:kms'}
				<div>
					<label for="table-kms" class="text-sm text-slate-600 dark:text-slate-300">Table KMS key ARN</label>
					<input
						id="table-kms"
						bind:value={newTableKmsKeyArn}
						placeholder="arn:aws:kms:..."
						class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
					/>
				</div>
			{/if}
			<div>
				<label for="table-storage-class" class="text-sm text-slate-600 dark:text-slate-300"
					>Storage class override (optional)</label
				>
				<select
					id="table-storage-class"
					bind:value={newTableStorageClass}
					class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
				>
					<option value="">Inherit from bucket</option>
					<option value="STANDARD">STANDARD</option>
					<option value="INTELLIGENT_TIERING">INTELLIGENT_TIERING</option>
				</select>
			</div>
			{#if createTableError}
				<p class="text-sm text-red-600 dark:text-red-400">{createTableError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => createTableModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Cancel</button
		>
		<button
			type="button"
			onclick={submitCreateTable}
			disabled={creatingTable}
			class="rounded-lg bg-blue-600 px-4 py-2 text-sm font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
			>{creatingTable ? 'Creating…' : 'Create'}</button
		>
	{/snippet}
</Modal>

<Modal bind:this={tableDetailModal} title="Table">
	{#snippet children()}
		{#if tableDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if viewedTable}
			<div class="max-h-[70vh] overflow-y-auto space-y-5 pr-1">
				<dl class="text-sm space-y-2">
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Name</dt>
						<dd class="text-slate-900 dark:text-white">{viewedTable.name ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Namespace</dt>
						<dd class="text-slate-900 dark:text-white">{joinNamespace(viewedTable.namespace)}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">ARN</dt>
						<dd class="break-all text-slate-900 dark:text-white">
							{(viewedTable as GetTableResponse).tableARN ?? '—'}
						</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Format</dt>
						<dd class="text-slate-900 dark:text-white">{(viewedTable as GetTableResponse).format ?? '—'}</dd>
					</div>
					<div>
						<dt class="text-slate-500 dark:text-slate-400">Modified</dt>
						<dd class="text-slate-900 dark:text-white">{formatDate(viewedTable.modifiedAt)}</dd>
					</div>
				</dl>
				{#if tableDetailError}
					<p class="text-sm text-red-600 dark:text-red-400">{tableDetailError}</p>
				{/if}

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Rename</h3>
					<div class="flex gap-2 items-center flex-wrap">
						<label class="text-xs text-slate-500 dark:text-slate-400" for="rename-ns">New namespace</label>
						<input
							id="rename-ns"
							bind:value={renameDraftNamespace}
							class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<label class="text-xs text-slate-500 dark:text-slate-400" for="rename-name">New name</label>
						<input
							id="rename-name"
							bind:value={renameDraftName}
							class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<button
							onclick={submitRenameTable}
							disabled={renaming}
							class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
							>{renaming ? 'Renaming…' : 'Rename'}</button
						>
					</div>
					{#if renameError}
						<p class="text-xs text-red-600 dark:text-red-400">{renameError}</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Metadata location</h3>
					{#if tableMetadataLocationLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else if tableMetadataLocation}
						<p class="text-xs text-slate-500 dark:text-slate-400 break-all">
							Warehouse: {tableMetadataLocation.warehouseLocation}
						</p>
						<input
							bind:value={tableMetadataLocationDraft}
							aria-label="Metadata location"
							class="w-full px-2 py-1.5 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						/>
						<button
							onclick={saveTableMetadataLocation}
							disabled={tableMetadataLocationSaving}
							class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
							>Save</button
						>
						{#if tableMetadataLocationError}
							<p class="text-xs text-red-600 dark:text-red-400">{tableMetadataLocationError}</p>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Maintenance</h3>
					{#if tableMaintenanceLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<div class="flex gap-2 items-center flex-wrap">
							<select
								bind:value={tableMaintenanceDraftType}
								aria-label="Table maintenance type"
								class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							>
								<option value="icebergCompaction">icebergCompaction</option>
								<option value="icebergSnapshotManagement">icebergSnapshotManagement</option>
							</select>
							<select
								bind:value={tableMaintenanceDraftStatus}
								aria-label="Table maintenance status"
								class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							>
								<option value="enabled">enabled</option>
								<option value="disabled">disabled</option>
							</select>
							{#if tableMaintenanceDraftType === 'icebergCompaction'}
								<label class="text-xs text-slate-500 dark:text-slate-400" for="tmd-filesize"
									>Target file size (MB)</label
								>
								<input
									id="tmd-filesize"
									type="number"
									bind:value={tableMaintenanceDraftTargetFileSizeMB}
									class="w-24 px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
							{:else}
								<label class="text-xs text-slate-500 dark:text-slate-400" for="tmd-minsnap"
									>Min snapshots</label
								>
								<input
									id="tmd-minsnap"
									type="number"
									bind:value={tableMaintenanceDraftMinSnapshots}
									class="w-20 px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
								<label class="text-xs text-slate-500 dark:text-slate-400" for="tmd-maxage"
									>Max age (hrs)</label
								>
								<input
									id="tmd-maxage"
									type="number"
									bind:value={tableMaintenanceDraftMaxSnapshotAgeHours}
									class="w-20 px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
								/>
							{/if}
							<button
								onclick={saveTableMaintenance}
								disabled={tableMaintenanceSaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Save</button
							>
						</div>
						{#if tableMaintenanceError}
							<p class="text-xs text-red-600 dark:text-red-400">{tableMaintenanceError}</p>
						{/if}
						{#if tableMaintenanceJobStatusLoading}
							<p class="text-xs text-slate-500 dark:text-slate-400">Loading job status…</p>
						{:else if Object.keys(tableMaintenanceJobStatus).length > 0}
							<ul class="text-xs text-slate-500 dark:text-slate-400 list-disc list-inside">
								{#each Object.entries(tableMaintenanceJobStatus) as [type, value] (type)}
									<li>{type}: {value.status}</li>
								{/each}
							</ul>
						{/if}
						{#if tableMaintenanceJobStatusError}
							<p class="text-xs text-red-600 dark:text-red-400">{tableMaintenanceJobStatusError}</p>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Resource policy</h3>
					{#if tablePolicyLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<textarea
							bind:value={tablePolicyDraft}
							rows="3"
							aria-label="Table policy JSON"
							placeholder="No policy set"
							class="w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
						></textarea>
						<div class="flex gap-2">
							<button
								onclick={saveTablePolicy}
								disabled={tablePolicySaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Save policy</button
							>
							{#if tablePolicy !== null}
								<button
									onclick={deleteTablePolicy}
									class="rounded-lg border border-red-300 px-3 py-1.5 text-xs font-medium text-red-600 hover:bg-red-50 dark:border-red-800 dark:text-red-400"
									>Delete policy</button
								>
							{/if}
						</div>
						{#if tablePolicyError}
							<p class="text-xs text-red-600 dark:text-red-400">{tablePolicyError}</p>
						{/if}
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Encryption &amp; storage class</h3>
					<p class="text-xs text-slate-500 dark:text-slate-400">
						Set only at table creation or inherited from the bucket -- there is no per-table update operation.
					</p>
					{#if tableEncryptionLoading || tableStorageClassLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<p class="text-xs text-slate-700 dark:text-slate-300">
							Encryption: {tableEncryption?.sseAlgorithm ?? '—'} · Storage class: {tableStorageClass ?? '—'}
						</p>
					{/if}
					{#if tableEncryptionError}
						<p class="text-xs text-red-600 dark:text-red-400">{tableEncryptionError}</p>
					{/if}
					{#if tableStorageClassError}
						<p class="text-xs text-red-600 dark:text-red-400">{tableStorageClassError}</p>
					{/if}
				</div>

				<div class="border-t border-slate-200 dark:border-slate-700 pt-3 space-y-2">
					<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-200">Record expiration</h3>
					{#if tableRecordExpirationLoading}
						<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
					{:else}
						<div class="flex gap-2 items-center flex-wrap">
							<select
								bind:value={tableRecordExpirationDraftStatus}
								aria-label="Record expiration status"
								class="px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							>
								<option value="disabled">disabled</option>
								<option value="enabled">enabled</option>
							</select>
							<label class="text-xs text-slate-500 dark:text-slate-400" for="re-days">Retention days</label>
							<input
								id="re-days"
								type="number"
								bind:value={tableRecordExpirationDraftDays}
								class="w-20 px-2 py-1.5 text-xs rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"
							/>
							<button
								onclick={saveTableRecordExpiration}
								disabled={tableRecordExpirationSaving}
								class="rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-blue-700 disabled:opacity-50"
								>Save</button
							>
						</div>
						{#if tableRecordExpirationError}
							<p class="text-xs text-red-600 dark:text-red-400">{tableRecordExpirationError}</p>
						{/if}
						{#if tableRecordExpirationJobStatusLoading}
							<p class="text-xs text-slate-500 dark:text-slate-400">Loading job status…</p>
						{:else if tableRecordExpirationJobStatus}
							<p class="text-xs text-slate-500 dark:text-slate-400">
								Last job: {tableRecordExpirationJobStatus.status}
							</p>
						{/if}
						{#if tableRecordExpirationJobStatusError}
							<p class="text-xs text-red-600 dark:text-red-400">{tableRecordExpirationJobStatusError}</p>
						{/if}
					{/if}
				</div>
			</div>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button
			type="button"
			onclick={() => tableDetailModal?.close()}
			class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800"
			>Close</button
		>
	{/snippet}
</Modal>
