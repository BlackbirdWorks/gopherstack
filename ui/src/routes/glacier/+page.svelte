<script lang="ts">
	import { onDestroy } from 'svelte';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getGlacierClient } from '$lib/aws-client';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import {
		ListVaultsCommand,
		CreateVaultCommand,
		DeleteVaultCommand,
		DescribeVaultCommand,
		UploadArchiveCommand,
		DeleteArchiveCommand,
		InitiateJobCommand,
		ListJobsCommand,
		DescribeJobCommand,
		GetJobOutputCommand,
		AddTagsToVaultCommand,
		ListTagsForVaultCommand,
		RemoveTagsFromVaultCommand,
		SetVaultAccessPolicyCommand,
		GetVaultAccessPolicyCommand,
		DeleteVaultAccessPolicyCommand,
		SetVaultNotificationsCommand,
		GetVaultNotificationsCommand,
		DeleteVaultNotificationsCommand,
		InitiateVaultLockCommand,
		GetVaultLockCommand,
		AbortVaultLockCommand,
		CompleteVaultLockCommand,
		ListMultipartUploadsCommand,
		AbortMultipartUploadCommand,
		GetDataRetrievalPolicyCommand,
		SetDataRetrievalPolicyCommand,
		type DescribeVaultOutput,
		type GlacierJobDescription,
		type UploadListElement,
		type JobParameters,
		type DataRetrievalPolicy,
	} from '@aws-sdk/client-glacier';
	import { toast } from 'svelte-sonner';
	import {
		Archive,
		Info,
		Database,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Tag,
		Shield,
		Bell,
		Lock,
		Upload,
		Play,
		FileText,
		ChevronRight,
		X,
		Copy,
		Check,
		Settings,
		Filter,
	} from 'lucide-svelte';

	const glacier = regionalClient(getGlacierClient);

	// Known Glacier SNS event types
	const KNOWN_SNS_EVENTS = [
		'ArchiveRetrievalCompleted',
		'InventoryRetrievalCompleted',
	];

	// --- State ---
	let loading = $state(false);
	let vaults = $state<DescribeVaultOutput[]>([]);
	let searchQuery = $state('');
	let selectedVault = $state<DescribeVaultOutput | null>(null);
	let activeTab = $state<'overview' | 'archives' | 'jobs' | 'tags' | 'policy' | 'notifications' | 'lock' | 'multipart'>('overview');

	// Create vault
	let showCreateModal = $state(false);
	let newVaultName = $state('');
	let creating = $state(false);

	// Archive upload
	let showUploadModal = $state(false);
	let uploadDescription = $state('');
	let uploadFile = $state<globalThis.File | null>(null);
	let uploading = $state(false);
	let lastUploadedArchiveId = $state('');

	// Archive delete
	let showDeleteArchiveModal = $state(false);
	let deleteArchiveId = $state('');
	let deletingArchive = $state(false);

	// Jobs
	let jobs = $state<GlacierJobDescription[]>([]);
	let loadingJobs = $state(false);
	let showInitiateJobModal = $state(false);
	let jobType = $state<'archive-retrieval' | 'inventory-retrieval'>('inventory-retrieval');
	let jobArchiveId = $state('');
	let jobTier = $state('Standard');
	let jobDescription = $state('');
	let inventoryFormat = $state<'JSON' | 'CSV'>('JSON');
	let initiatingJob = $state(false);
	let selectedJob = $state<GlacierJobDescription | null>(null);
	let jobDetails = $state<GlacierJobDescription | null>(null);
	let loadingJobDetails = $state(false);
	let jobOutput = $state('');
	let loadingJobOutput = $state(false);
	let jobStatusFilter = $state<'all' | 'completed' | 'in-progress'>('all');
	let autoRefreshJobs = $state(false);
	let autoRefreshInterval: ReturnType<typeof setInterval> | null = null;

	// Tags
	let tags = $state<Record<string, string>>({});
	let loadingTags = $state(false);
	let newTagKey = $state('');
	let newTagValue = $state('');
	let taggingInFlight = $state(false);

	// Policy (vault access policy)
	let accessPolicy = $state('');
	let loadingPolicy = $state(false);
	let editingPolicy = $state(false);
	let policyDraft = $state('');
	let policyError = $state('');
	let savingPolicy = $state(false);

	// Notifications
	let snsTopic = $state('');
	let snsEvents = $state<string[]>([]);
	let loadingNotifications = $state(false);
	let editingNotifications = $state(false);
	let snsTopicDraft = $state('');
	let snsEventsDraft = $state<string[]>([]);
	let savingNotifications = $state(false);

	// Vault lock
	let vaultLock = $state<{ Policy?: string; State?: string; CreationDate?: string; ExpirationDate?: string } | null>(null);
	let loadingLock = $state(false);
	let lockPolicyDraft = $state('');
	let lockIdInput = $state('');
	let lastLockId = $state('');
	let lockActionInFlight = $state(false);

	// Multipart uploads
	let multipartUploads = $state<UploadListElement[]>([]);
	let loadingMultipart = $state(false);

	// Data retrieval policy (account-level)
	let dataRetrievalPolicy = $state<DataRetrievalPolicy | null>(null);
	let loadingDataPolicy = $state(false);
	let editingDataPolicy = $state(false);
	let dataPolicyDraft = $state('');
	let savingDataPolicy = $state(false);

	// Clipboard copy indicator
	let copiedId = $state('');

	const storageClasses = [
		{ name: 'S3 Glacier Instant Retrieval', retrieval: 'Milliseconds', minStorage: '90 days', cost: 'Low', useCase: 'Rarely accessed data requiring immediate retrieval' },
		{ name: 'S3 Glacier Flexible Retrieval', retrieval: '3-5 hours (Standard)', minStorage: '90 days', cost: 'Very Low', useCase: 'Archive data, 1-2 retrievals per year' },
		{ name: 'S3 Glacier Deep Archive', retrieval: '12 hours (Standard)', minStorage: '180 days', cost: 'Lowest', useCase: 'Long-term retention, accessed 1-2x per year' }
	];

	const filteredVaults = $derived(
		vaults.filter(v =>
			(v.VaultName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredJobs = $derived(
		jobs.filter(j => {
			if (jobStatusFilter === 'completed') return j.Completed;
			if (jobStatusFilter === 'in-progress') return !j.Completed;
			return true;
		})
	);

	// --- Helpers ---
	function fmtBytes(b: number | undefined): string {
		if (!b) return '0 B';
		if (b < 1024) return `${b} B`;
		if (b < 1048576) return `${(b / 1024).toFixed(1)} KB`;
		if (b < 1073741824) return `${(b / 1048576).toFixed(1)} MB`;
		return `${(b / 1073741824).toFixed(2)} GB`;
	}

	function fmtDate(d: string | Date | undefined): string {
		if (!d) return '—';
		const dt = typeof d === 'string' ? new Date(d) : d;
		return dt.toLocaleString();
	}

	function extractErrorMessage(err: unknown): string {
		const e = err as { message?: string; name?: string };
		if (e?.name && e?.message) return `${e.name}: ${e.message}`;
		return e?.message ?? String(err);
	}

	async function copyToClipboard(text: string, id: string) {
		try {
			await navigator.clipboard.writeText(text);
			copiedId = id;
			setTimeout(() => { copiedId = ''; }, 2000);
		} catch {
			toast.error('Failed to copy to clipboard');
		}
	}

	function validateJSON(s: string): string {
		if (!s.trim()) return '';
		try {
			JSON.parse(s);
			return '';
		} catch (e) {
			return (e as Error).message;
		}
	}

	function formatJSON(s: string): string {
		try {
			return JSON.stringify(JSON.parse(s), null, 2);
		} catch {
			return s;
		}
	}

	// Escape key closes modals
	function handleKeydown(e: KeyboardEvent) {
		if (e.key === 'Escape') {
			showCreateModal = false;
			showUploadModal = false;
			showInitiateJobModal = false;
			showDeleteArchiveModal = false;
		}
	}

	// --- Vault list ---
	async function loadVaults() {
		loading = true;
		try {
			const res = await glacier().send(new ListVaultsCommand({ accountId: '-' }));
			vaults = res.VaultList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load vaults: ${extractErrorMessage(err)}`);
		} finally {
			loading = false;
		}
	}

	async function createVault() {
		if (!newVaultName.trim()) return;
		creating = true;
		try {
			await glacier().send(new CreateVaultCommand({ accountId: '-', vaultName: newVaultName.trim() }));
			toast.success(`Vault "${newVaultName.trim()}" created`);
			newVaultName = '';
			showCreateModal = false;
			await loadVaults();
		} catch (err: unknown) {
			toast.error(`Failed to create vault: ${extractErrorMessage(err)}`);
		} finally {
			creating = false;
		}
	}

	async function deleteVault(name: string | undefined) {
		if (!name) return;
		const archiveCount = selectedVault?.NumberOfArchives ?? 0;
		const archiveWarning = archiveCount > 0
			? ` It contains ${archiveCount} archive${archiveCount === 1 ? '' : 's'} that must be removed first.`
			: '';
		if (!(await confirmDestructive({
			title: 'Delete Vault',
			message: `Delete vault "${name}"?${archiveWarning} This cannot be undone.`
		}))) return;
		try {
			await glacier().send(new DeleteVaultCommand({ accountId: '-', vaultName: name }));
			toast.success(`Vault "${name}" deleted`);
			if (selectedVault?.VaultName === name) selectedVault = null;
			await loadVaults();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${extractErrorMessage(err)}`);
		}
	}

	async function refreshVaultDetails() {
		if (!selectedVault?.VaultName) return;
		try {
			const res = await glacier().send(new DescribeVaultCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			selectedVault = res;
			// Also update the vault in the list
			vaults = vaults.map(v => v.VaultName === res.VaultName ? res : v);
		} catch (err: unknown) {
			toast.error(`Failed to refresh vault: ${extractErrorMessage(err)}`);
		}
	}

	async function selectVault(v: DescribeVaultOutput) {
		try {
			const res = await glacier().send(new DescribeVaultCommand({ accountId: '-', vaultName: v.VaultName! }));
			selectedVault = res;
		} catch {
			selectedVault = v;
		}
		activeTab = 'overview';
		jobs = [];
		tags = {};
		accessPolicy = '';
		snsTopic = '';
		snsEvents = [];
		vaultLock = null;
		multipartUploads = [];
		lastUploadedArchiveId = '';
		stopAutoRefresh();
	}

	async function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		editingPolicy = false;
		editingNotifications = false;
		selectedJob = null;
		jobOutput = '';
		jobDetails = null;
		stopAutoRefresh();

		if (!selectedVault?.VaultName) return;
		const name = selectedVault.VaultName;

		if (tab === 'jobs') await loadJobs(name);
		else if (tab === 'tags') await loadTags(name);
		else if (tab === 'policy') await loadPolicy(name);
		else if (tab === 'notifications') await loadNotifications(name);
		else if (tab === 'lock') await loadVaultLock(name);
		else if (tab === 'multipart') await loadMultipartUploads(name);
	}

	// --- Archive upload ---
	async function uploadArchive() {
		if (!uploadFile || !selectedVault?.VaultName) return;
		uploading = true;
		try {
			const body = new Uint8Array(await uploadFile.arrayBuffer());
			const res = await glacier().send(new UploadArchiveCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				body,
				archiveDescription: uploadDescription.trim() || undefined,
			}));
			lastUploadedArchiveId = res.archiveId ?? '';
			toast.success(`Archive uploaded — ID: ${res.archiveId}`);
			uploadFile = null;
			uploadDescription = '';
			showUploadModal = false;
			await refreshVaultDetails();
		} catch (err: unknown) {
			toast.error(`Upload failed: ${extractErrorMessage(err)}`);
		} finally {
			uploading = false;
		}
	}

	async function deleteArchive() {
		if (!deleteArchiveId.trim() || !selectedVault?.VaultName) return;
		deletingArchive = true;
		try {
			await glacier().send(new DeleteArchiveCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				archiveId: deleteArchiveId.trim(),
			}));
			toast.success('Archive deleted');
			if (lastUploadedArchiveId === deleteArchiveId.trim()) lastUploadedArchiveId = '';
			deleteArchiveId = '';
			showDeleteArchiveModal = false;
			await refreshVaultDetails();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${extractErrorMessage(err)}`);
		} finally {
			deletingArchive = false;
		}
	}

	// --- Jobs ---
	async function loadJobs(name: string) {
		loadingJobs = true;
		try {
			const res = await glacier().send(new ListJobsCommand({ accountId: '-', vaultName: name }));
			jobs = res.JobList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load jobs: ${extractErrorMessage(err)}`);
		} finally {
			loadingJobs = false;
		}
	}

	function startAutoRefresh() {
		if (autoRefreshInterval) return;
		autoRefreshInterval = setInterval(() => {
			if (selectedVault?.VaultName && activeTab === 'jobs') {
				loadJobs(selectedVault.VaultName);
			}
		}, 15000);
	}

	function stopAutoRefresh() {
		if (autoRefreshInterval) {
			clearInterval(autoRefreshInterval);
			autoRefreshInterval = null;
		}
		autoRefreshJobs = false;
	}

	function toggleAutoRefresh() {
		autoRefreshJobs = !autoRefreshJobs;
		if (autoRefreshJobs) {
			startAutoRefresh();
			toast.success('Auto-refresh enabled (15s)');
		} else {
			stopAutoRefresh();
		}
	}

	async function initiateJob() {
		if (!selectedVault?.VaultName) return;
		initiatingJob = true;
		try {
			const params: JobParameters = { Type: jobType, Tier: jobTier };
			if (jobType === 'archive-retrieval') params.ArchiveId = jobArchiveId.trim();
			if (jobType === 'inventory-retrieval') params.Format = inventoryFormat;
			if (jobDescription.trim()) params.Description = jobDescription.trim();

			await glacier().send(new InitiateJobCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				jobParameters: params,
			}));
			toast.success('Job initiated');
			showInitiateJobModal = false;
			jobArchiveId = '';
			jobDescription = '';
			await loadJobs(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to initiate job: ${extractErrorMessage(err)}`);
		} finally {
			initiatingJob = false;
		}
	}

	async function describeJob(jobId: string) {
		if (!selectedVault?.VaultName) return;
		loadingJobDetails = true;
		jobDetails = null;
		try {
			const res = await glacier().send(new DescribeJobCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				jobId,
			}));
			jobDetails = res as GlacierJobDescription;
		} catch (err: unknown) {
			toast.error(`Failed to describe job: ${extractErrorMessage(err)}`);
		} finally {
			loadingJobDetails = false;
		}
	}

	async function viewJobOutput(job: GlacierJobDescription) {
		selectedJob = job;
		jobOutput = '';
		jobDetails = null;
		if (!job.JobId || !selectedVault?.VaultName) return;
		await describeJob(job.JobId);
		loadingJobOutput = true;
		try {
			const res = await glacier().send(new GetJobOutputCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				jobId: job.JobId,
			}));
			if (res.body) {
				const text = await new Response(res.body as BodyInit).text();
				try {
					jobOutput = JSON.stringify(JSON.parse(text), null, 2);
				} catch {
					jobOutput = text;
				}
			}
		} catch (err: unknown) {
			toast.error(`Failed to get job output: ${extractErrorMessage(err)}`);
		} finally {
			loadingJobOutput = false;
		}
	}

	function initiateArchiveRetrievalForLastUpload() {
		if (!lastUploadedArchiveId) return;
		jobType = 'archive-retrieval';
		jobArchiveId = lastUploadedArchiveId;
		showInitiateJobModal = true;
		switchTab('jobs');
	}

	// --- Tags ---
	async function loadTags(name: string) {
		loadingTags = true;
		try {
			const res = await glacier().send(new ListTagsForVaultCommand({ accountId: '-', vaultName: name }));
			tags = res.Tags ?? {};
		} catch (err: unknown) {
			toast.error(`Failed to load tags: ${extractErrorMessage(err)}`);
		} finally {
			loadingTags = false;
		}
	}

	async function addTag() {
		if (!newTagKey.trim() || !selectedVault?.VaultName) return;
		if (newTagKey.length > 128) { toast.error('Tag key must be ≤ 128 characters'); return; }
		if (newTagValue.length > 256) { toast.error('Tag value must be ≤ 256 characters'); return; }
		taggingInFlight = true;
		try {
			await glacier().send(new AddTagsToVaultCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				Tags: { [newTagKey.trim()]: newTagValue.trim() },
			}));
			newTagKey = '';
			newTagValue = '';
			await loadTags(selectedVault.VaultName);
			toast.success('Tag added');
		} catch (err: unknown) {
			toast.error(`Failed to add tag: ${extractErrorMessage(err)}`);
		} finally {
			taggingInFlight = false;
		}
	}

	async function removeTag(key: string) {
		if (!selectedVault?.VaultName) return;
		taggingInFlight = true;
		try {
			await glacier().send(new RemoveTagsFromVaultCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				TagKeys: [key],
			}));
			await loadTags(selectedVault.VaultName);
			toast.success('Tag removed');
		} catch (err: unknown) {
			toast.error(`Failed to remove tag: ${extractErrorMessage(err)}`);
		} finally {
			taggingInFlight = false;
		}
	}

	// --- Policy ---
	async function loadPolicy(name: string) {
		loadingPolicy = true;
		try {
			const res = await glacier().send(new GetVaultAccessPolicyCommand({ accountId: '-', vaultName: name }));
			accessPolicy = res.policy?.Policy ?? '';
		} catch (err: unknown) {
			const msg = extractErrorMessage(err);
			if (msg.includes('ResourceNotFoundException') || msg.includes('not found')) {
				accessPolicy = '';
			} else {
				toast.error(`Failed to load policy: ${msg}`);
			}
		} finally {
			loadingPolicy = false;
		}
	}

	async function savePolicy() {
		if (!selectedVault?.VaultName) return;
		const validationError = validateJSON(policyDraft);
		if (validationError) { policyError = `Invalid JSON: ${validationError}`; return; }
		policyError = '';
		savingPolicy = true;
		try {
			await glacier().send(new SetVaultAccessPolicyCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				policy: { Policy: policyDraft },
			}));
			accessPolicy = policyDraft;
			editingPolicy = false;
			toast.success('Policy saved');
		} catch (err: unknown) {
			toast.error(`Failed to save policy: ${extractErrorMessage(err)}`);
		} finally {
			savingPolicy = false;
		}
	}

	async function deletePolicy() {
		if (!selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Delete Policy', message: 'Remove the vault access policy?' }))) return;
		try {
			await glacier().send(new DeleteVaultAccessPolicyCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			accessPolicy = '';
			editingPolicy = false;
			toast.success('Policy deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete policy: ${extractErrorMessage(err)}`);
		}
	}

	// --- Notifications ---
	async function loadNotifications(name: string) {
		loadingNotifications = true;
		try {
			const res = await glacier().send(new GetVaultNotificationsCommand({ accountId: '-', vaultName: name }));
			snsTopic = res.vaultNotificationConfig?.SNSTopic ?? '';
			snsEvents = res.vaultNotificationConfig?.Events ?? [];
		} catch (err: unknown) {
			const msg = extractErrorMessage(err);
			if (msg.includes('ResourceNotFoundException') || msg.includes('not found')) {
				snsTopic = '';
				snsEvents = [];
			} else {
				toast.error(`Failed to load notifications: ${msg}`);
			}
		} finally {
			loadingNotifications = false;
		}
	}

	function toggleSnsEvent(ev: string) {
		if (snsEventsDraft.includes(ev)) {
			snsEventsDraft = snsEventsDraft.filter(e => e !== ev);
		} else {
			snsEventsDraft = [...snsEventsDraft, ev];
		}
	}

	async function saveNotifications() {
		if (!selectedVault?.VaultName) return;
		savingNotifications = true;
		try {
			await glacier().send(new SetVaultNotificationsCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				vaultNotificationConfig: { SNSTopic: snsTopicDraft.trim(), Events: snsEventsDraft },
			}));
			snsTopic = snsTopicDraft.trim();
			snsEvents = [...snsEventsDraft];
			editingNotifications = false;
			toast.success('Notifications saved');
		} catch (err: unknown) {
			toast.error(`Failed to save notifications: ${extractErrorMessage(err)}`);
		} finally {
			savingNotifications = false;
		}
	}

	async function deleteNotifications() {
		if (!selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Delete Notifications', message: 'Remove vault notification config?' }))) return;
		try {
			await glacier().send(new DeleteVaultNotificationsCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			snsTopic = '';
			snsEvents = [];
			toast.success('Notifications removed');
		} catch (err: unknown) {
			toast.error(`Failed to remove notifications: ${extractErrorMessage(err)}`);
		}
	}

	// --- Vault Lock ---
	async function loadVaultLock(name: string) {
		loadingLock = true;
		try {
			const res = await glacier().send(new GetVaultLockCommand({ accountId: '-', vaultName: name }));
			vaultLock = {
				Policy: res.Policy,
				State: res.State,
				CreationDate: res.CreationDate,
				ExpirationDate: res.ExpirationDate,
			};
		} catch (err: unknown) {
			const msg = extractErrorMessage(err);
			if (msg.includes('ResourceNotFoundException') || msg.includes('not found')) {
				vaultLock = null;
			} else {
				toast.error(`Failed to load vault lock: ${msg}`);
			}
		} finally {
			loadingLock = false;
		}
	}

	async function initiateLock() {
		if (!selectedVault?.VaultName) return;
		lockActionInFlight = true;
		try {
			const res = await glacier().send(new InitiateVaultLockCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				policy: lockPolicyDraft.trim() ? { Policy: lockPolicyDraft.trim() } : undefined,
			}));
			lastLockId = res.lockId ?? '';
			lockIdInput = lastLockId;
			toast.success(`Lock initiated. Lock ID: ${res.lockId} — save this to complete the lock.`);
			lockPolicyDraft = '';
			await loadVaultLock(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to initiate lock: ${extractErrorMessage(err)}`);
		} finally {
			lockActionInFlight = false;
		}
	}

	async function completeLock() {
		if (!selectedVault?.VaultName || !lockIdInput.trim()) return;
		lockActionInFlight = true;
		try {
			await glacier().send(new CompleteVaultLockCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				lockId: lockIdInput.trim(),
			}));
			toast.success('Vault lock completed — vault is now locked');
			lastLockId = '';
			lockIdInput = '';
			await loadVaultLock(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to complete lock: ${extractErrorMessage(err)}`);
		} finally {
			lockActionInFlight = false;
		}
	}

	async function abortLock() {
		if (!selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Abort Vault Lock', message: 'Abort the in-progress vault lock? The lock ID will be invalidated.' }))) return;
		lockActionInFlight = true;
		try {
			await glacier().send(new AbortVaultLockCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			toast.success('Vault lock aborted');
			lastLockId = '';
			await loadVaultLock(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to abort lock: ${extractErrorMessage(err)}`);
		} finally {
			lockActionInFlight = false;
		}
	}

	// --- Multipart uploads ---
	async function loadMultipartUploads(name: string) {
		loadingMultipart = true;
		try {
			const res = await glacier().send(new ListMultipartUploadsCommand({ accountId: '-', vaultName: name }));
			multipartUploads = res.UploadsList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load multipart uploads: ${extractErrorMessage(err)}`);
		} finally {
			loadingMultipart = false;
		}
	}

	async function abortMultipartUpload(uploadId: string | undefined) {
		if (!uploadId || !selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Abort Upload', message: `Abort multipart upload ${uploadId.slice(0, 16)}…? All uploaded parts will be discarded.` }))) return;
		try {
			await glacier().send(new AbortMultipartUploadCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				uploadId,
			}));
			toast.success('Multipart upload aborted');
			await loadMultipartUploads(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to abort upload: ${extractErrorMessage(err)}`);
		}
	}

	// --- Data Retrieval Policy (account-level) ---
	async function loadDataRetrievalPolicy() {
		loadingDataPolicy = true;
		try {
			const res = await glacier().send(new GetDataRetrievalPolicyCommand({ accountId: '-' }));
			dataRetrievalPolicy = res.Policy ?? null;
		} catch (err: unknown) {
			toast.error(`Failed to load data retrieval policy: ${extractErrorMessage(err)}`);
		} finally {
			loadingDataPolicy = false;
		}
	}

	async function saveDataRetrievalPolicy() {
		const validationError = validateJSON(dataPolicyDraft);
		if (validationError) { toast.error(`Invalid JSON: ${validationError}`); return; }
		savingDataPolicy = true;
		try {
			await glacier().send(new SetDataRetrievalPolicyCommand({
				accountId: '-',
				Policy: JSON.parse(dataPolicyDraft) as DataRetrievalPolicy,
			}));
			toast.success('Data retrieval policy saved');
			editingDataPolicy = false;
			await loadDataRetrievalPolicy();
		} catch (err: unknown) {
			toast.error(`Failed to save policy: ${extractErrorMessage(err)}`);
		} finally {
			savingDataPolicy = false;
		}
	}

	// Vault list, selected vault, and every tab's cached data are region-scoped;
	// a region switch must clear them (not just reload) so stale data from the
	// old region never lingers, and must stop any in-flight auto-refresh timer
	// (it closes over the old vault).
	onRegionChange(() => {
		selectedVault = null;
		vaults = [];
		jobs = [];
		tags = {};
		accessPolicy = '';
		snsTopic = '';
		snsEvents = [];
		vaultLock = null;
		multipartUploads = [];
		lastUploadedArchiveId = '';
		dataRetrievalPolicy = null;
		stopAutoRefresh();
		void loadVaults();
		void loadDataRetrievalPolicy();
	});

	onDestroy(stopAutoRefresh);
</script>

<svelte:window onkeydown={handleKeydown} />

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Archive class="w-7 h-7 text-blue-800 dark:text-blue-300" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon S3 Glacier</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Secure, durable, and extremely low-cost cloud storage for data archiving and long-term backup</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => { showCreateModal = true; newVaultName = ''; }} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-sm font-medium transition-colors">
				<Plus class="w-4 h-4" /> Create Vault
			</button>
			<button onclick={loadVaults} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- Info banner -->
	<div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700 rounded-lg p-4 flex items-start gap-3">
		<Info class="w-5 h-5 text-blue-600 dark:text-blue-400 mt-0.5 shrink-0" />
		<div>
			<p class="text-sm font-medium text-blue-800 dark:text-blue-300">Managed via Amazon S3</p>
			<p class="text-sm text-blue-700 dark:text-blue-400 mt-1">Amazon S3 Glacier storage classes are managed through Amazon S3. Use the S3 console to configure lifecycle policies to automatically archive objects. Direct vault management via Glacier API is also supported below.</p>
		</div>
	</div>

	<!-- Main split layout -->
	<div class="flex gap-4">
		<!-- Vault list -->
		<div class="w-72 flex-shrink-0 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 self-start">
			<div class="p-3 border-b border-slate-200 dark:border-slate-700">
				<div class="relative">
					<Search class="absolute left-2.5 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search vaults…" class="pl-8 pr-3 py-1.5 text-sm rounded-md border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full" />
				</div>
			</div>
			<div class="divide-y divide-slate-100 dark:divide-slate-700 max-h-96 overflow-y-auto">
				{#if loading}
					<div class="p-4 text-center text-sm text-gray-500 dark:text-gray-400 flex items-center justify-center gap-2">
						<RefreshCw class="w-4 h-4 animate-spin" /> Loading…
					</div>
				{:else if filteredVaults.length === 0}
					<div class="p-6 text-center">
						<Database class="w-8 h-8 text-gray-300 mx-auto mb-2" />
						<p class="text-sm text-gray-500 dark:text-gray-400">No vaults found</p>
						<button onclick={() => { showCreateModal = true; newVaultName = ''; }} class="text-xs text-blue-600 hover:underline mt-1">Create your first vault</button>
					</div>
				{:else}
					{#each filteredVaults as vault}
						<button
							onclick={() => selectVault(vault)}
							class="w-full text-left px-3 py-2.5 flex items-center gap-2 hover:bg-gray-50 dark:hover:bg-slate-700/50 transition-colors {selectedVault?.VaultName === vault.VaultName ? 'bg-blue-50 dark:bg-blue-900/20' : ''}"
						>
							<Database class="w-4 h-4 text-blue-500 shrink-0" />
							<div class="flex-1 min-w-0">
								<p class="text-sm font-medium text-gray-900 dark:text-white truncate">{vault.VaultName}</p>
								<p class="text-xs text-gray-500 dark:text-gray-400">{vault.NumberOfArchives ?? 0} archives · {fmtBytes(vault.SizeInBytes ?? 0)}</p>
							</div>
							<ChevronRight class="w-4 h-4 text-gray-400 shrink-0" />
						</button>
					{/each}
				{/if}
			</div>
		</div>

		<!-- Detail panel -->
		{#if selectedVault}
			<div class="flex-1 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
				<!-- Vault header -->
				<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
					<div class="flex-1 min-w-0">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selectedVault.VaultName}</h2>
						<p class="text-xs text-gray-500 dark:text-gray-400 font-mono truncate">{selectedVault.VaultARN}</p>
					</div>
					<div class="flex items-center gap-2 shrink-0 ml-2">
						<button
							onclick={refreshVaultDetails}
							title="Refresh vault details"
							class="p-1.5 rounded text-gray-500 hover:text-gray-700 hover:bg-gray-100 dark:hover:bg-slate-700"
						>
							<RefreshCw class="w-4 h-4" />
						</button>
						<button
							onclick={() => deleteVault(selectedVault?.VaultName)}
							class="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 border border-red-200 dark:border-red-800"
						>
							<Trash2 class="w-4 h-4" /> Delete
						</button>
					</div>
				</div>

				<!-- Tabs -->
				<div class="flex gap-0 border-b border-slate-200 dark:border-slate-700 px-4 overflow-x-auto">
					{#each [
						{ id: 'overview', label: 'Overview', icon: Database },
						{ id: 'archives', label: 'Archives', icon: Archive },
						{ id: 'jobs', label: 'Jobs', icon: Play },
						{ id: 'tags', label: 'Tags', icon: Tag },
						{ id: 'policy', label: 'Policy', icon: Shield },
						{ id: 'notifications', label: 'Notifications', icon: Bell },
						{ id: 'lock', label: 'Lock', icon: Lock },
						{ id: 'multipart', label: 'Multipart', icon: FileText },
					] as tab}
						{@const Icon = tab.icon}
						<button
							onclick={() => switchTab(tab.id as typeof activeTab)}
							class="flex items-center gap-1.5 px-3 py-2.5 text-sm whitespace-nowrap border-b-2 transition-colors
								{activeTab === tab.id
									? 'border-blue-600 text-blue-600 dark:text-blue-400 dark:border-blue-400'
									: 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700 dark:hover:text-gray-300'}"
						>
							<Icon class="w-3.5 h-3.5" />
							{tab.label}
						</button>
					{/each}
				</div>

				<!-- Tab content -->
				<div class="p-4">
					<!-- Overview -->
					{#if activeTab === 'overview'}
						<div class="space-y-4">
							<div class="grid grid-cols-2 gap-3 sm:grid-cols-4">
								{#each [
									{ label: 'Archives', value: String(selectedVault.NumberOfArchives ?? 0) },
									{ label: 'Total Size', value: fmtBytes(selectedVault.SizeInBytes ?? 0) },
									{ label: 'Created', value: selectedVault.CreationDate ? new Date(selectedVault.CreationDate).toLocaleDateString() : '—' },
									{ label: 'Last Inventory', value: selectedVault.LastInventoryDate ? new Date(selectedVault.LastInventoryDate).toLocaleDateString() : 'Never' },
								] as stat}
									<div class="p-3 bg-gray-50 dark:bg-slate-700/50 rounded-lg">
										<p class="text-xs text-gray-500 dark:text-gray-400">{stat.label}</p>
										<p class="text-lg font-semibold text-gray-900 dark:text-white mt-0.5">{stat.value}</p>
									</div>
								{/each}
							</div>

							<!-- Last uploaded archive quick actions -->
							{#if lastUploadedArchiveId}
								<div class="p-3 rounded-lg bg-green-50 dark:bg-green-900/20 border border-green-200 dark:border-green-700">
									<p class="text-xs font-medium text-green-800 dark:text-green-300 mb-1">Last uploaded archive</p>
									<div class="flex items-center gap-2">
										<code class="text-xs font-mono text-green-700 dark:text-green-400 flex-1 truncate">{lastUploadedArchiveId}</code>
										<button onclick={() => copyToClipboard(lastUploadedArchiveId, 'last-upload')} class="text-green-600 hover:text-green-800 shrink-0">
											{#if copiedId === 'last-upload'}
												<Check class="w-4 h-4" />
											{:else}
												<Copy class="w-4 h-4" />
											{/if}
										</button>
										<button onclick={initiateArchiveRetrievalForLastUpload} class="text-xs px-2 py-0.5 rounded bg-green-600 hover:bg-green-700 text-white">
											Retrieve
										</button>
									</div>
								</div>
							{/if}
						</div>

					<!-- Archives -->
					{:else if activeTab === 'archives'}
						<div class="space-y-4">
							<div class="flex items-center justify-between">
								<p class="text-sm text-gray-600 dark:text-gray-400">Upload an archive or delete one by ID.</p>
								<div class="flex gap-2">
									<button onclick={() => { showDeleteArchiveModal = true; deleteArchiveId = ''; }} class="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md border border-red-200 dark:border-red-800 text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20">
										<Trash2 class="w-4 h-4" /> Delete Archive
									</button>
									<button onclick={() => { showUploadModal = true; uploadDescription = ''; uploadFile = null; }} class="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md bg-blue-600 hover:bg-blue-700 text-white">
										<Upload class="w-4 h-4" /> Upload Archive
									</button>
								</div>
							</div>
							<div class="bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded-lg p-4 text-sm">
								<p class="font-medium text-amber-800 dark:text-amber-300 mb-1">About Glacier Archives</p>
								<p class="text-amber-700 dark:text-amber-400">Archive IDs are returned at upload time. To list all archives in this vault, initiate an <button onclick={() => switchTab('jobs')} class="underline">Inventory Retrieval job</button>. Vault currently has <strong>{selectedVault.NumberOfArchives ?? 0}</strong> archive{(selectedVault.NumberOfArchives ?? 0) !== 1 ? 's' : ''} totalling <strong>{fmtBytes(selectedVault.SizeInBytes ?? 0)}</strong>.</p>
							</div>
							{#if lastUploadedArchiveId}
								<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600">
									<p class="text-xs text-gray-500 dark:text-gray-400 mb-1">Last uploaded archive ID</p>
									<div class="flex items-center gap-2">
										<code class="text-xs font-mono text-gray-800 dark:text-gray-200 flex-1 break-all">{lastUploadedArchiveId}</code>
										<button onclick={() => copyToClipboard(lastUploadedArchiveId, 'archive-tab')} class="text-gray-500 hover:text-gray-700 shrink-0">
											{#if copiedId === 'archive-tab'}<Check class="w-4 h-4 text-green-500" />{:else}<Copy class="w-4 h-4" />{/if}
										</button>
									</div>
								</div>
							{/if}
						</div>

					<!-- Jobs -->
					{:else if activeTab === 'jobs'}
						<div class="space-y-3">
							<div class="flex items-center justify-between flex-wrap gap-2">
								<div class="flex items-center gap-2">
									<select bind:value={jobStatusFilter} class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-700 dark:text-gray-300">
										<option value="all">All jobs ({jobs.length})</option>
										<option value="completed">Completed ({jobs.filter(j => j.Completed).length})</option>
										<option value="in-progress">In Progress ({jobs.filter(j => !j.Completed).length})</option>
									</select>
								</div>
								<div class="flex items-center gap-2">
									<button
										onclick={toggleAutoRefresh}
										class="flex items-center gap-1 px-2 py-1.5 text-xs rounded border transition-colors
											{autoRefreshJobs
												? 'border-green-400 bg-green-50 dark:bg-green-900/20 text-green-700 dark:text-green-400'
												: 'border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700'}"
									>
										<RefreshCw class="w-3.5 h-3.5 {autoRefreshJobs ? 'animate-spin' : ''}" />
										{autoRefreshJobs ? 'Auto (15s)' : 'Auto-refresh'}
									</button>
									<button onclick={() => selectedVault?.VaultName && loadJobs(selectedVault.VaultName)} class="flex items-center gap-1 px-2 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">
										<RefreshCw class="w-3.5 h-3.5 {loadingJobs ? 'animate-spin' : ''}" />
									</button>
									<button onclick={() => { showInitiateJobModal = true; }} class="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md bg-blue-600 hover:bg-blue-700 text-white">
										<Play class="w-4 h-4" /> Initiate Job
									</button>
								</div>
							</div>
							{#if loadingJobs}
								<div class="text-center py-8">
									<RefreshCw class="w-6 h-6 animate-spin text-gray-400 mx-auto mb-2" />
									<p class="text-sm text-gray-500">Loading jobs…</p>
								</div>
							{:else if filteredJobs.length === 0}
								<div class="text-center py-8">
									<Play class="w-8 h-8 text-gray-300 mx-auto mb-2" />
									<p class="text-sm text-gray-500 dark:text-gray-400">
										{jobs.length === 0 ? 'No jobs for this vault' : 'No jobs match the filter'}
									</p>
									{#if jobs.length === 0}
										<button onclick={() => { showInitiateJobModal = true; }} class="text-xs text-blue-600 hover:underline mt-1">Initiate your first job</button>
									{/if}
								</div>
							{:else}
								<div class="space-y-2">
									{#each filteredJobs as job}
										<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-700/50">
											<div class="flex items-start justify-between gap-2">
												<div class="flex-1 min-w-0 space-y-0.5">
													<div class="flex items-center gap-2">
														<span class="text-sm font-medium text-gray-900 dark:text-white">{job.Action}</span>
														{#if job.InventoryRetrievalParameters?.Format || (job.Action === 'InventoryRetrieval')}
															<span class="text-xs px-1.5 py-0.5 rounded bg-gray-200 dark:bg-slate-600 text-gray-600 dark:text-gray-300">
																{job.InventoryRetrievalParameters?.Format ?? 'JSON'}
															</span>
														{/if}
														{#if job.Tier}
															<span class="text-xs px-1.5 py-0.5 rounded bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">{job.Tier}</span>
														{/if}
													</div>
													<p class="text-xs text-gray-500 dark:text-gray-400 font-mono truncate">{job.JobId}</p>
													{#if job.ArchiveId}
														<div class="flex items-center gap-1">
															<p class="text-xs text-gray-600 dark:text-gray-400 font-mono truncate">Archive: {job.ArchiveId}</p>
															<button onclick={() => copyToClipboard(job.ArchiveId!, `job-archive-${job.JobId}`)} class="text-gray-400 hover:text-gray-600 shrink-0">
																{#if copiedId === `job-archive-${job.JobId}`}<Check class="w-3 h-3 text-green-500" />{:else}<Copy class="w-3 h-3" />{/if}
															</button>
														</div>
													{/if}
													{#if job.JobDescription}
														<p class="text-xs text-gray-600 dark:text-gray-400">{job.JobDescription}</p>
													{/if}
													<p class="text-xs text-gray-500 dark:text-gray-400">
														Created: {fmtDate(job.CreationDate)}
														{#if job.ArchiveSizeInBytes}· {fmtBytes(job.ArchiveSizeInBytes)}{/if}
													</p>
												</div>
												<div class="flex items-center gap-2 shrink-0">
													<span class="text-xs px-1.5 py-0.5 rounded-full {job.Completed ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}">
														{job.StatusCode}
													</span>
													{#if job.Completed}
														<button onclick={() => viewJobOutput(job)} class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 hover:bg-white dark:hover:bg-slate-700 text-gray-700 dark:text-gray-300">
															Output
														</button>
													{/if}
												</div>
											</div>
										</div>
									{/each}
								</div>
							{/if}

							<!-- Job output viewer -->
							{#if selectedJob}
								<div class="mt-3 p-3 rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-800">
									<div class="flex items-center justify-between mb-2">
										<p class="text-sm font-medium text-gray-900 dark:text-white">
											Job Output: {selectedJob.Action}
											{#if selectedJob.JobId}
												<span class="text-xs text-gray-500 ml-1 font-normal font-mono">{selectedJob.JobId?.slice(0, 16)}…</span>
											{/if}
										</p>
										<div class="flex items-center gap-1">
											{#if jobOutput}
												<button onclick={() => copyToClipboard(jobOutput, 'job-output')} class="text-gray-400 hover:text-gray-600">
													{#if copiedId === 'job-output'}<Check class="w-4 h-4 text-green-500" />{:else}<Copy class="w-4 h-4" />{/if}
												</button>
											{/if}
											<button onclick={() => { selectedJob = null; jobOutput = ''; jobDetails = null; }} class="text-gray-400 hover:text-gray-600"><X class="w-4 h-4" /></button>
										</div>
									</div>
									{#if loadingJobDetails}
										<div class="flex items-center gap-2 text-sm text-gray-500 py-1">
											<RefreshCw class="w-3.5 h-3.5 animate-spin" /> Fetching job details…
										</div>
									{:else if jobDetails}
										<div class="mb-2 grid grid-cols-2 gap-x-4 gap-y-1 text-xs">
											<div>
												<span class="text-gray-500 dark:text-gray-400">Status: </span>
												<span class="font-medium {jobDetails.Completed ? 'text-green-600 dark:text-green-400' : 'text-yellow-600 dark:text-yellow-400'}">{jobDetails.StatusCode}</span>
											</div>
											{#if jobDetails.StatusMessage}
												<div class="col-span-2">
													<span class="text-gray-500 dark:text-gray-400">Message: </span>
													<span class="text-gray-700 dark:text-gray-300">{jobDetails.StatusMessage}</span>
												</div>
											{/if}
											{#if jobDetails.ArchiveSizeInBytes}
												<div>
													<span class="text-gray-500 dark:text-gray-400">Archive Size: </span>
													<span class="text-gray-700 dark:text-gray-300">{fmtBytes(jobDetails.ArchiveSizeInBytes)}</span>
												</div>
											{/if}
											{#if jobDetails.SHA256TreeHash}
												<div class="col-span-2">
													<span class="text-gray-500 dark:text-gray-400">SHA256 Tree Hash: </span>
													<code class="font-mono text-gray-700 dark:text-gray-300 break-all">{jobDetails.SHA256TreeHash}</code>
												</div>
											{/if}
											{#if jobDetails.InventoryRetrievalParameters}
												{@const inv = jobDetails.InventoryRetrievalParameters}
												{#if inv.StartDate || inv.EndDate}
													<div class="col-span-2">
														<span class="text-gray-500 dark:text-gray-400">Date Range: </span>
														<span class="text-gray-700 dark:text-gray-300">{inv.StartDate ?? '—'} → {inv.EndDate ?? '—'}</span>
													</div>
												{/if}
												{#if inv.Limit}
													<div>
														<span class="text-gray-500 dark:text-gray-400">Limit: </span>
														<span class="text-gray-700 dark:text-gray-300">{inv.Limit}</span>
													</div>
												{/if}
											{/if}
										</div>
									{/if}
									{#if loadingJobOutput}
										<div class="flex items-center gap-2 text-sm text-gray-500 py-2">
											<RefreshCw class="w-4 h-4 animate-spin" /> Loading output…
										</div>
									{:else}
										<pre class="text-xs bg-gray-50 dark:bg-slate-900 rounded p-2 overflow-auto max-h-64 text-gray-800 dark:text-gray-200 whitespace-pre-wrap">{jobOutput || '(empty — archive data may not be stored)'}</pre>
									{/if}
								</div>
							{/if}
						</div>

					<!-- Tags -->
					{:else if activeTab === 'tags'}
						<div class="space-y-3">
							{#if loadingTags}
								<div class="flex items-center gap-2 text-sm text-gray-500 py-4 justify-center">
									<RefreshCw class="w-4 h-4 animate-spin" /> Loading tags…
								</div>
							{:else}
								<div class="space-y-1.5">
									{#each Object.entries(tags) as [k, v]}
										<div class="flex items-center gap-2 p-2 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600">
											<span class="text-xs font-mono text-blue-600 dark:text-blue-400 truncate max-w-[120px]" title={k}>{k}</span>
											<span class="text-gray-400 shrink-0">=</span>
											<span class="text-xs text-gray-700 dark:text-gray-300 flex-1 truncate" title={v}>{v}</span>
											<button onclick={() => removeTag(k)} disabled={taggingInFlight} class="text-red-400 hover:text-red-600 shrink-0">
												<X class="w-3.5 h-3.5" />
											</button>
										</div>
									{/each}
									{#if Object.keys(tags).length === 0}
										<div class="py-4 text-center">
											<Tag class="w-6 h-6 text-gray-300 mx-auto mb-1" />
											<p class="text-sm text-gray-500 dark:text-gray-400">No tags — add key-value metadata below</p>
										</div>
									{/if}
								</div>
								<div class="flex gap-2 pt-2 border-t border-gray-100 dark:border-gray-700">
									<div class="flex-1 space-y-1">
										<input bind:value={newTagKey} placeholder="Key (max 128)" maxlength={128} class="w-full px-2 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									</div>
									<div class="flex-1 space-y-1">
										<input bind:value={newTagValue} placeholder="Value (max 256)" maxlength={256} class="w-full px-2 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									</div>
									<button onclick={addTag} disabled={taggingInFlight || !newTagKey.trim()} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50 shrink-0">
										Add
									</button>
								</div>
								<p class="text-xs text-gray-400">Maximum 10 tags per vault. Keys ≤ 128 chars, values ≤ 256 chars.</p>
							{/if}
						</div>

					<!-- Policy -->
					{:else if activeTab === 'policy'}
						<div class="space-y-3">
							{#if loadingPolicy}
								<div class="flex items-center gap-2 text-sm text-gray-500 py-4 justify-center">
									<RefreshCw class="w-4 h-4 animate-spin" /> Loading policy…
								</div>
							{:else if editingPolicy}
								{#if policyError}
									<p class="text-xs text-red-600 dark:text-red-400">{policyError}</p>
								{/if}
								<div class="flex gap-2 mb-1">
									<button onclick={() => { policyDraft = formatJSON(policyDraft); policyError = ''; }} class="text-xs px-2 py-1 rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">Format JSON</button>
								</div>
								<textarea bind:value={policyDraft} rows={12} class="w-full px-3 py-2 text-sm font-mono rounded-lg border {policyError ? 'border-red-400' : 'border-gray-200 dark:border-gray-600'} bg-white dark:bg-slate-700 text-gray-900 dark:text-white resize-y"></textarea>
								<div class="flex gap-2">
									<button onclick={savePolicy} disabled={savingPolicy} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
										{savingPolicy ? 'Saving…' : 'Save Policy'}
									</button>
									<button onclick={() => { editingPolicy = false; policyError = ''; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
										Cancel
									</button>
								</div>
							{:else if accessPolicy}
								<pre class="text-xs bg-gray-50 dark:bg-slate-900 rounded p-3 overflow-auto max-h-64 text-gray-800 dark:text-gray-200">{formatJSON(accessPolicy)}</pre>
								<div class="flex gap-2">
									<button onclick={() => { policyDraft = formatJSON(accessPolicy); policyError = ''; editingPolicy = true; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
										Edit
									</button>
									<button onclick={deletePolicy} class="px-3 py-1.5 text-sm rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 border border-red-200 dark:border-red-800">
										Delete
									</button>
								</div>
							{:else}
								<div class="py-6 text-center">
									<Shield class="w-8 h-8 text-gray-300 mx-auto mb-2" />
									<p class="text-sm text-gray-500 dark:text-gray-400">No access policy configured</p>
									<button onclick={() => { policyDraft = formatJSON('{"Version":"2012-10-17","Statement":[]}'); policyError = ''; editingPolicy = true; }} class="text-xs text-blue-600 hover:underline mt-1">Add a policy</button>
								</div>
							{/if}
						</div>

					<!-- Notifications -->
					{:else if activeTab === 'notifications'}
						<div class="space-y-3">
							{#if loadingNotifications}
								<div class="flex items-center gap-2 text-sm text-gray-500 py-4 justify-center">
									<RefreshCw class="w-4 h-4 animate-spin" /> Loading notifications…
								</div>
							{:else if editingNotifications}
								<div class="space-y-3">
									<div>
										<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">SNS Topic ARN</p>
										<input bind:value={snsTopicDraft} placeholder="arn:aws:sns:us-east-1:123456789012:my-topic" class="w-full px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									</div>
									<div>
										<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-2">Events</p>
										<div class="space-y-1.5">
											{#each KNOWN_SNS_EVENTS as ev}
												<label class="flex items-center gap-2 cursor-pointer">
													<input type="checkbox" checked={snsEventsDraft.includes(ev)} onchange={() => toggleSnsEvent(ev)} class="rounded border-gray-300" />
													<span class="text-sm text-gray-700 dark:text-gray-300">{ev}</span>
												</label>
											{/each}
										</div>
									</div>
								</div>
								<div class="flex gap-2">
									<button onclick={saveNotifications} disabled={savingNotifications} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
										{savingNotifications ? 'Saving…' : 'Save'}
									</button>
									<button onclick={() => { editingNotifications = false; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
										Cancel
									</button>
								</div>
							{:else if snsTopic}
								<div class="space-y-2">
									<div class="p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600">
										<p class="text-xs text-gray-500 dark:text-gray-400">SNS Topic</p>
										<p class="text-sm font-mono text-gray-900 dark:text-white mt-0.5 break-all">{snsTopic}</p>
									</div>
									{#if snsEvents.length > 0}
										<div class="flex flex-wrap gap-1">
											{#each snsEvents as ev}
												<span class="text-xs px-2 py-0.5 bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300 rounded-full">{ev}</span>
											{/each}
										</div>
									{/if}
								</div>
								<div class="flex gap-2">
									<button onclick={() => { snsTopicDraft = snsTopic; snsEventsDraft = [...snsEvents]; editingNotifications = true; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
										Edit
									</button>
									<button onclick={deleteNotifications} class="px-3 py-1.5 text-sm rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 border border-red-200 dark:border-red-800">
										Delete
									</button>
								</div>
							{:else}
								<div class="py-6 text-center">
									<Bell class="w-8 h-8 text-gray-300 mx-auto mb-2" />
									<p class="text-sm text-gray-500 dark:text-gray-400">No notification configuration</p>
									<button onclick={() => { snsTopicDraft = ''; snsEventsDraft = []; editingNotifications = true; }} class="text-xs text-blue-600 hover:underline mt-1">Configure notifications</button>
								</div>
							{/if}
						</div>

					<!-- Vault Lock -->
					{:else if activeTab === 'lock'}
						<div class="space-y-4">
							{#if loadingLock}
								<div class="flex items-center gap-2 text-sm text-gray-500 py-4 justify-center">
									<RefreshCw class="w-4 h-4 animate-spin" /> Loading lock status…
								</div>
							{:else if vaultLock}
								<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-700/50 space-y-2">
									<div class="flex items-center gap-2">
										<Lock class="w-4 h-4 {vaultLock.State === 'Locked' ? 'text-red-500' : 'text-yellow-500'}" />
										<span class="text-xs px-2 py-0.5 rounded-full font-medium
											{vaultLock.State === 'Locked' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}">
											{vaultLock.State}
										</span>
									</div>
									{#if vaultLock.CreationDate}<p class="text-xs text-gray-500 dark:text-gray-400">Created: {fmtDate(vaultLock.CreationDate)}</p>{/if}
									{#if vaultLock.ExpirationDate}<p class="text-xs text-gray-500 dark:text-gray-400">Expires: {fmtDate(vaultLock.ExpirationDate)}</p>{/if}
									{#if vaultLock.Policy}
										<pre class="text-xs bg-white dark:bg-slate-900 rounded p-2 overflow-auto max-h-40 text-gray-800 dark:text-gray-200">{vaultLock.Policy}</pre>
									{/if}
								</div>
								{#if vaultLock.State === 'InProgress'}
									<div class="space-y-2">
										{#if lastLockId}
											<div class="p-2 rounded bg-yellow-50 dark:bg-yellow-900/20 border border-yellow-200 dark:border-yellow-700 flex items-center gap-2">
												<p class="text-xs text-yellow-800 dark:text-yellow-300 flex-1">Lock ID: <code class="font-mono">{lastLockId}</code></p>
												<button onclick={() => copyToClipboard(lastLockId, 'lock-id')} class="text-yellow-600 shrink-0">
													{#if copiedId === 'lock-id'}<Check class="w-4 h-4 text-green-500" />{:else}<Copy class="w-4 h-4" />{/if}
												</button>
											</div>
										{/if}
										<div>
											<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Lock ID (to complete)</p>
											<input bind:value={lockIdInput} placeholder="Lock ID from initiate response" class="w-full px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
										</div>
										<div class="flex gap-2">
											<button onclick={completeLock} disabled={lockActionInFlight || !lockIdInput.trim()} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
												Complete Lock
											</button>
											<button onclick={abortLock} disabled={lockActionInFlight} class="px-3 py-1.5 text-sm rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 border border-red-200 dark:border-red-800">
												Abort
											</button>
										</div>
									</div>
								{/if}
							{:else}
								<div class="py-4 text-center">
									<Lock class="w-8 h-8 text-gray-300 mx-auto mb-2" />
									<p class="text-sm text-gray-500 dark:text-gray-400">No vault lock policy</p>
								</div>
								<div class="space-y-2">
									<div>
										<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Lock Policy (optional JSON)</p>
										<textarea bind:value={lockPolicyDraft} rows={4} placeholder={`{"Version":"2012-10-17","Statement":[]}`} class="w-full px-3 py-1.5 text-sm font-mono rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white resize-y"></textarea>
									</div>
									<button onclick={initiateLock} disabled={lockActionInFlight} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
										{lockActionInFlight ? 'Initiating…' : 'Initiate Lock'}
									</button>
								</div>
							{/if}
						</div>

					<!-- Multipart uploads -->
					{:else if activeTab === 'multipart'}
						<div class="space-y-3">
							<div class="flex items-center justify-between">
								<p class="text-sm text-gray-600 dark:text-gray-400">{multipartUploads.length} in-progress upload{multipartUploads.length !== 1 ? 's' : ''}</p>
								<button onclick={() => selectedVault?.VaultName && loadMultipartUploads(selectedVault.VaultName)} class="flex items-center gap-1 px-2 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">
									<RefreshCw class="w-3.5 h-3.5 {loadingMultipart ? 'animate-spin' : ''}" />
								</button>
							</div>
							{#if loadingMultipart}
								<div class="flex items-center gap-2 text-sm text-gray-500 py-4 justify-center">
									<RefreshCw class="w-4 h-4 animate-spin" /> Loading…
								</div>
							{:else if multipartUploads.length === 0}
								<div class="py-6 text-center">
									<FileText class="w-8 h-8 text-gray-300 mx-auto mb-2" />
									<p class="text-sm text-gray-500 dark:text-gray-400">No in-progress multipart uploads</p>
								</div>
							{:else}
								<div class="space-y-2">
									{#each multipartUploads as up}
										<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-700/50 flex items-start justify-between gap-2">
											<div class="flex-1 min-w-0 space-y-0.5">
												<div class="flex items-center gap-1">
													<p class="text-xs font-mono text-gray-600 dark:text-gray-400 truncate">{up.MultipartUploadId}</p>
													<button onclick={() => copyToClipboard(up.MultipartUploadId!, `mpu-${up.MultipartUploadId}`)} class="text-gray-400 hover:text-gray-600 shrink-0">
														{#if copiedId === `mpu-${up.MultipartUploadId}`}<Check class="w-3 h-3 text-green-500" />{:else}<Copy class="w-3 h-3" />{/if}
													</button>
												</div>
												{#if up.ArchiveDescription}<p class="text-sm text-gray-900 dark:text-white">{up.ArchiveDescription}</p>{/if}
												<p class="text-xs text-gray-500">Part size: {fmtBytes(up.PartSizeInBytes ?? 0)} · Created: {fmtDate(up.CreationDate)}</p>
											</div>
											<button onclick={() => abortMultipartUpload(up.MultipartUploadId)} class="text-red-500 hover:text-red-700 shrink-0">
												<Trash2 class="w-4 h-4" />
											</button>
										</div>
									{/each}
								</div>
							{/if}
						</div>
					{/if}
				</div>
			</div>
		{:else}
			<!-- No vault selected placeholder -->
			<div class="flex-1 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 flex items-center justify-center py-20">
				<div class="text-center text-gray-400">
					<Archive class="w-12 h-12 mx-auto mb-3 opacity-30" />
					<p class="text-sm font-medium">Select a vault to view details</p>
					<p class="text-xs mt-1 opacity-70">or create a new vault to get started</p>
				</div>
			</div>
		{/if}
	</div>

	<!-- Data Retrieval Policy (account-level) -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
			<div class="flex items-center gap-2">
				<Settings class="w-4 h-4 text-gray-500" />
				<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Data Retrieval Policy</h2>
			</div>
			<div class="flex gap-2">
				{#if !editingDataPolicy}
					<button onclick={() => { dataPolicyDraft = formatJSON(JSON.stringify(dataRetrievalPolicy ?? { Rules: [{ Strategy: 'FreeTier' }] })); editingDataPolicy = true; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
						Edit
					</button>
				{/if}
				<button onclick={loadDataRetrievalPolicy} class="p-1.5 rounded text-gray-500 hover:bg-gray-100 dark:hover:bg-slate-700">
					<RefreshCw class="w-4 h-4 {loadingDataPolicy ? 'animate-spin' : ''}" />
				</button>
			</div>
		</div>
		<div class="p-4">
			{#if loadingDataPolicy}
				<div class="flex items-center gap-2 text-sm text-gray-500 py-2">
					<RefreshCw class="w-4 h-4 animate-spin" /> Loading policy…
				</div>
			{:else if editingDataPolicy}
				<div class="space-y-3">
					<p class="text-xs text-gray-500 dark:text-gray-400">Set the account-level data retrieval policy. Use strategy <code class="bg-gray-100 dark:bg-slate-700 px-1 rounded">FreeTier</code>, <code class="bg-gray-100 dark:bg-slate-700 px-1 rounded">MaxRetrievalRate</code>, or <code class="bg-gray-100 dark:bg-slate-700 px-1 rounded">None</code>.</p>
					<textarea bind:value={dataPolicyDraft} rows={8} class="w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white resize-y"></textarea>
					<div class="flex gap-2">
						<button onclick={saveDataRetrievalPolicy} disabled={savingDataPolicy} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
							{savingDataPolicy ? 'Saving…' : 'Save Policy'}
						</button>
						<button onclick={() => { editingDataPolicy = false; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
							Cancel
						</button>
					</div>
				</div>
			{:else if dataRetrievalPolicy}
				<pre class="text-xs bg-gray-50 dark:bg-slate-900 rounded p-3 overflow-auto text-gray-800 dark:text-gray-200">{formatJSON(JSON.stringify(dataRetrievalPolicy))}</pre>
			{:else}
				<p class="text-sm text-gray-500 dark:text-gray-400">No data retrieval policy set (FreeTier applies).</p>
			{/if}
		</div>
	</div>

	<!-- Storage Classes -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Glacier Storage Classes</h2>
		</div>
		<div class="p-4 space-y-4">
			{#each storageClasses as cls}
				<div class="p-4 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600">
					<h3 class="font-semibold text-gray-900 dark:text-white mb-2">{cls.name}</h3>
					<div class="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Retrieval Time</p>
							<p class="text-gray-900 dark:text-white font-medium">{cls.retrieval}</p>
						</div>
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Min Storage Duration</p>
							<p class="text-gray-900 dark:text-white font-medium">{cls.minStorage}</p>
						</div>
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Cost</p>
							<p class="text-gray-900 dark:text-white font-medium">{cls.cost}</p>
						</div>
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Use Case</p>
							<p class="text-gray-700 dark:text-gray-300">{cls.useCase}</p>
						</div>
					</div>
				</div>
			{/each}
		</div>
	</div>
</div>

<!-- Create Vault Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50" role="dialog" aria-modal="true">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-sm mx-4">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Create Vault</h3>
			<div class="space-y-3">
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Vault Name</p>
					<input bind:value={newVaultName} placeholder="my-archive-vault" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					<p class="text-xs text-gray-400 mt-1">Letters, numbers, hyphens, underscores (1-255 chars)</p>
				</div>
			</div>
			<div class="flex gap-2 mt-4">
				<button onclick={createVault} disabled={creating || !newVaultName.trim()} class="flex-1 px-3 py-2 text-sm rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-medium disabled:opacity-50">
					{creating ? 'Creating…' : 'Create'}
				</button>
				<button onclick={() => { showCreateModal = false; }} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Upload Archive Modal -->
{#if showUploadModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50" role="dialog" aria-modal="true">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-sm mx-4">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Upload Archive</h3>
			<div class="space-y-3">
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">File</p>
					<input type="file" onchange={(e) => { uploadFile = (e.currentTarget as HTMLInputElement).files?.[0] ?? null; }} class="w-full text-sm text-gray-700 dark:text-gray-300" />
					{#if uploadFile}
						<p class="text-xs text-gray-500 mt-1">{uploadFile.name} · {fmtBytes(uploadFile.size)}</p>
					{/if}
				</div>
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Description (optional)</p>
					<input bind:value={uploadDescription} placeholder="Archive description" maxlength={1024} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<p class="text-xs text-gray-400">The archive ID will be shown after upload. Save it — it cannot be retrieved later without an inventory job.</p>
			</div>
			<div class="flex gap-2 mt-4">
				<button onclick={uploadArchive} disabled={uploading || !uploadFile} class="flex-1 px-3 py-2 text-sm rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-medium disabled:opacity-50">
					{uploading ? 'Uploading…' : 'Upload'}
				</button>
				<button onclick={() => { showUploadModal = false; }} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Delete Archive Modal -->
{#if showDeleteArchiveModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50" role="dialog" aria-modal="true">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-sm mx-4">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Delete Archive</h3>
			<div class="space-y-3">
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Archive ID</p>
					<input bind:value={deleteArchiveId} placeholder="Archive ID" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono" />
				</div>
				<p class="text-xs text-red-600 dark:text-red-400">This action cannot be undone.</p>
			</div>
			<div class="flex gap-2 mt-4">
				<button onclick={deleteArchive} disabled={deletingArchive || !deleteArchiveId.trim()} class="flex-1 px-3 py-2 text-sm rounded-lg bg-red-600 hover:bg-red-700 text-white font-medium disabled:opacity-50">
					{deletingArchive ? 'Deleting…' : 'Delete Archive'}
				</button>
				<button onclick={() => { showDeleteArchiveModal = false; }} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Initiate Job Modal -->
{#if showInitiateJobModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50" role="dialog" aria-modal="true">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md mx-4">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Initiate Job</h3>
			<div class="space-y-3">
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Job Type</p>
					<select bind:value={jobType} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="inventory-retrieval">Inventory Retrieval — list all archives</option>
						<option value="archive-retrieval">Archive Retrieval — retrieve specific archive</option>
					</select>
				</div>
				{#if jobType === 'archive-retrieval'}
					<div>
						<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Archive ID</p>
						<input bind:value={jobArchiveId} placeholder="Archive ID (required)" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white font-mono" />
					</div>
				{:else}
					<div>
						<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Output Format</p>
						<select bind:value={inventoryFormat} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
							<option value="JSON">JSON</option>
							<option value="CSV">CSV</option>
						</select>
					</div>
				{/if}
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Retrieval Tier</p>
					<select bind:value={jobTier} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="Standard">Standard (3-5 hours)</option>
						<option value="Bulk">Bulk (5-12 hours, lowest cost)</option>
						<option value="Expedited">Expedited (&lt;5 minutes, highest cost)</option>
					</select>
				</div>
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Description (optional)</p>
					<input bind:value={jobDescription} placeholder="Job description" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			<div class="flex gap-2 mt-4">
				<button onclick={initiateJob} disabled={initiatingJob || (jobType === 'archive-retrieval' && !jobArchiveId.trim())} class="flex-1 px-3 py-2 text-sm rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-medium disabled:opacity-50">
					{initiatingJob ? 'Initiating…' : 'Initiate Job'}
				</button>
				<button onclick={() => { showInitiateJobModal = false; }} class="px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
					Cancel
				</button>
			</div>
		</div>
	</div>
{/if}
