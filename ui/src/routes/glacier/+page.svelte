<script lang="ts">
	import { onMount } from 'svelte';
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
		type DescribeVaultOutput,
		type GlacierJobDescription,
		type UploadListElement,
		type JobParameters,
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
	} from 'lucide-svelte';

	const glacier = getGlacierClient();

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

	// Jobs
	let jobs = $state<GlacierJobDescription[]>([]);
	let loadingJobs = $state(false);
	let showInitiateJobModal = $state(false);
	let jobType = $state<'archive-retrieval' | 'inventory-retrieval'>('inventory-retrieval');
	let jobArchiveId = $state('');
	let jobTier = $state('Standard');
	let jobDescription = $state('');
	let initiatingJob = $state(false);
	let selectedJob = $state<GlacierJobDescription | null>(null);
	let jobOutput = $state('');
	let loadingJobOutput = $state(false);

	// Tags
	let tags = $state<Record<string, string>>({});
	let loadingTags = $state(false);
	let newTagKey = $state('');
	let newTagValue = $state('');
	let taggingInFlight = $state(false);

	// Policy
	let accessPolicy = $state('');
	let loadingPolicy = $state(false);
	let editingPolicy = $state(false);
	let policyDraft = $state('');
	let savingPolicy = $state(false);

	// Notifications
	let snsTopic = $state('');
	let snsEvents = $state<string[]>([]);
	let loadingNotifications = $state(false);
	let editingNotifications = $state(false);
	let snsTopicDraft = $state('');
	let snsEventsDraft = $state('');
	let savingNotifications = $state(false);

	// Vault lock
	let vaultLock = $state<{ Policy?: string; State?: string; CreationDate?: string; ExpirationDate?: string } | null>(null);
	let loadingLock = $state(false);
	let lockPolicyDraft = $state('');
	let lockIdInput = $state('');
	let lockActionInFlight = $state(false);

	// Multipart uploads
	let multipartUploads = $state<UploadListElement[]>([]);
	let loadingMultipart = $state(false);

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

	// --- Vault list ---
	async function loadVaults() {
		loading = true;
		try {
			const res = await glacier.send(new ListVaultsCommand({ accountId: '-' }));
			vaults = res.VaultList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load vaults: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function createVault() {
		if (!newVaultName.trim()) return;
		creating = true;
		try {
			await glacier.send(new CreateVaultCommand({ accountId: '-', vaultName: newVaultName.trim() }));
			toast.success(`Vault "${newVaultName.trim()}" created`);
			newVaultName = '';
			showCreateModal = false;
			await loadVaults();
		} catch (err: unknown) {
			toast.error(`Failed to create vault: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteVault(name: string | undefined) {
		if (!name) return;
		if (!(await confirmDestructive({ title: 'Delete Vault', message: `Delete vault "${name}"? All archives must be removed first.` }))) return;
		try {
			await glacier.send(new DeleteVaultCommand({ accountId: '-', vaultName: name }));
			toast.success(`Vault "${name}" deleted`);
			if (selectedVault?.VaultName === name) selectedVault = null;
			await loadVaults();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	async function selectVault(v: DescribeVaultOutput) {
		try {
			const res = await glacier.send(new DescribeVaultCommand({ accountId: '-', vaultName: v.VaultName! }));
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
	}

	async function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		editingPolicy = false;
		editingNotifications = false;
		selectedJob = null;
		jobOutput = '';

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
			const res = await glacier.send(new UploadArchiveCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				body,
				archiveDescription: uploadDescription.trim() || undefined,
			}));
			toast.success(`Archive uploaded: ${res.archiveId}`);
			uploadFile = null;
			uploadDescription = '';
			showUploadModal = false;
			// Refresh vault to update size/count
			const fresh = await glacier.send(new DescribeVaultCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			selectedVault = fresh;
		} catch (err: unknown) {
			toast.error(`Upload failed: ${(err as Error).message}`);
		} finally {
			uploading = false;
		}
	}

	// --- Jobs ---
	async function loadJobs(name: string) {
		loadingJobs = true;
		try {
			const res = await glacier.send(new ListJobsCommand({ accountId: '-', vaultName: name }));
			jobs = res.JobList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load jobs: ${(err as Error).message}`);
		} finally {
			loadingJobs = false;
		}
	}

	async function initiateJob() {
		if (!selectedVault?.VaultName) return;
		initiatingJob = true;
		try {
			const params: JobParameters = { Type: jobType };
			if (jobType === 'archive-retrieval') params.ArchiveId = jobArchiveId.trim();
			if (jobTier) params.Tier = jobTier;
			if (jobDescription.trim()) params.Description = jobDescription.trim();

			await glacier.send(new InitiateJobCommand({
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
			toast.error(`Failed to initiate job: ${(err as Error).message}`);
		} finally {
			initiatingJob = false;
		}
	}

	async function viewJobOutput(job: GlacierJobDescription) {
		selectedJob = job;
		if (!job.JobId || !selectedVault?.VaultName) return;
		loadingJobOutput = true;
		jobOutput = '';
		try {
			const res = await glacier.send(new GetJobOutputCommand({
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
			toast.error(`Failed to get job output: ${(err as Error).message}`);
		} finally {
			loadingJobOutput = false;
		}
	}

	// --- Tags ---
	async function loadTags(name: string) {
		loadingTags = true;
		try {
			const res = await glacier.send(new ListTagsForVaultCommand({ accountId: '-', vaultName: name }));
			tags = res.Tags ?? {};
		} catch (err: unknown) {
			toast.error(`Failed to load tags: ${(err as Error).message}`);
		} finally {
			loadingTags = false;
		}
	}

	async function addTag() {
		if (!newTagKey.trim() || !selectedVault?.VaultName) return;
		taggingInFlight = true;
		try {
			await glacier.send(new AddTagsToVaultCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				Tags: { [newTagKey.trim()]: newTagValue.trim() },
			}));
			newTagKey = '';
			newTagValue = '';
			await loadTags(selectedVault.VaultName);
			toast.success('Tag added');
		} catch (err: unknown) {
			toast.error(`Failed to add tag: ${(err as Error).message}`);
		} finally {
			taggingInFlight = false;
		}
	}

	async function removeTag(key: string) {
		if (!selectedVault?.VaultName) return;
		taggingInFlight = true;
		try {
			await glacier.send(new RemoveTagsFromVaultCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				TagKeys: [key],
			}));
			await loadTags(selectedVault.VaultName);
			toast.success('Tag removed');
		} catch (err: unknown) {
			toast.error(`Failed to remove tag: ${(err as Error).message}`);
		} finally {
			taggingInFlight = false;
		}
	}

	// --- Policy ---
	async function loadPolicy(name: string) {
		loadingPolicy = true;
		try {
			const res = await glacier.send(new GetVaultAccessPolicyCommand({ accountId: '-', vaultName: name }));
			accessPolicy = res.policy?.Policy ?? '';
		} catch (err: unknown) {
			const msg = (err as Error).message;
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
		savingPolicy = true;
		try {
			await glacier.send(new SetVaultAccessPolicyCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				policy: { Policy: policyDraft },
			}));
			accessPolicy = policyDraft;
			editingPolicy = false;
			toast.success('Policy saved');
		} catch (err: unknown) {
			toast.error(`Failed to save policy: ${(err as Error).message}`);
		} finally {
			savingPolicy = false;
		}
	}

	async function deletePolicy() {
		if (!selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Delete Policy', message: 'Remove the vault access policy?' }))) return;
		try {
			await glacier.send(new DeleteVaultAccessPolicyCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			accessPolicy = '';
			editingPolicy = false;
			toast.success('Policy deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete policy: ${(err as Error).message}`);
		}
	}

	// --- Notifications ---
	async function loadNotifications(name: string) {
		loadingNotifications = true;
		try {
			const res = await glacier.send(new GetVaultNotificationsCommand({ accountId: '-', vaultName: name }));
			snsTopic = res.vaultNotificationConfig?.SNSTopic ?? '';
			snsEvents = res.vaultNotificationConfig?.Events ?? [];
		} catch (err: unknown) {
			const msg = (err as Error).message;
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

	async function saveNotifications() {
		if (!selectedVault?.VaultName) return;
		savingNotifications = true;
		try {
			const events = snsEventsDraft.split(',').map(e => e.trim()).filter(Boolean);
			await glacier.send(new SetVaultNotificationsCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				vaultNotificationConfig: { SNSTopic: snsTopicDraft.trim(), Events: events },
			}));
			snsTopic = snsTopicDraft.trim();
			snsEvents = events;
			editingNotifications = false;
			toast.success('Notifications saved');
		} catch (err: unknown) {
			toast.error(`Failed to save notifications: ${(err as Error).message}`);
		} finally {
			savingNotifications = false;
		}
	}

	async function deleteNotifications() {
		if (!selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Delete Notifications', message: 'Remove vault notification config?' }))) return;
		try {
			await glacier.send(new DeleteVaultNotificationsCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			snsTopic = '';
			snsEvents = [];
			toast.success('Notifications removed');
		} catch (err: unknown) {
			toast.error(`Failed to remove notifications: ${(err as Error).message}`);
		}
	}

	// --- Vault Lock ---
	async function loadVaultLock(name: string) {
		loadingLock = true;
		try {
			const res = await glacier.send(new GetVaultLockCommand({ accountId: '-', vaultName: name }));
			vaultLock = {
				Policy: res.Policy,
				State: res.State,
				CreationDate: res.CreationDate,
				ExpirationDate: res.ExpirationDate,
			};
		} catch (err: unknown) {
			const msg = (err as Error).message;
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
			const res = await glacier.send(new InitiateVaultLockCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				policy: lockPolicyDraft.trim() ? { Policy: lockPolicyDraft.trim() } : undefined,
			}));
			toast.success(`Lock initiated. Lock ID: ${res.lockId}`);
			lockIdInput = res.lockId ?? '';
			lockPolicyDraft = '';
			await loadVaultLock(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to initiate lock: ${(err as Error).message}`);
		} finally {
			lockActionInFlight = false;
		}
	}

	async function completeLock() {
		if (!selectedVault?.VaultName || !lockIdInput.trim()) return;
		lockActionInFlight = true;
		try {
			await glacier.send(new CompleteVaultLockCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				lockId: lockIdInput.trim(),
			}));
			toast.success('Vault lock completed');
			lockIdInput = '';
			await loadVaultLock(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to complete lock: ${(err as Error).message}`);
		} finally {
			lockActionInFlight = false;
		}
	}

	async function abortLock() {
		if (!selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Abort Vault Lock', message: 'Abort the in-progress vault lock?' }))) return;
		lockActionInFlight = true;
		try {
			await glacier.send(new AbortVaultLockCommand({ accountId: '-', vaultName: selectedVault.VaultName }));
			toast.success('Vault lock aborted');
			await loadVaultLock(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to abort lock: ${(err as Error).message}`);
		} finally {
			lockActionInFlight = false;
		}
	}

	// --- Multipart uploads ---
	async function loadMultipartUploads(name: string) {
		loadingMultipart = true;
		try {
			const res = await glacier.send(new ListMultipartUploadsCommand({ accountId: '-', vaultName: name }));
			multipartUploads = res.UploadsList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load multipart uploads: ${(err as Error).message}`);
		} finally {
			loadingMultipart = false;
		}
	}

	async function abortMultipartUpload(uploadId: string | undefined) {
		if (!uploadId || !selectedVault?.VaultName) return;
		if (!(await confirmDestructive({ title: 'Abort Upload', message: `Abort multipart upload ${uploadId}?` }))) return;
		try {
			await glacier.send(new AbortMultipartUploadCommand({
				accountId: '-',
				vaultName: selectedVault.VaultName,
				uploadId,
			}));
			toast.success('Multipart upload aborted');
			await loadMultipartUploads(selectedVault.VaultName);
		} catch (err: unknown) {
			toast.error(`Failed to abort upload: ${(err as Error).message}`);
		}
	}

	function fmtBytes(b: number | undefined): string {
		if (!b) return '0 B';
		if (b < 1024) return `${b} B`;
		if (b < 1048576) return `${(b / 1024).toFixed(1)} KB`;
		if (b < 1073741824) return `${(b / 1048576).toFixed(1)} MB`;
		return `${(b / 1073741824).toFixed(2)} GB`;
	}

	onMount(loadVaults);
</script>

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
					<div class="p-4 text-center text-sm text-gray-500 dark:text-gray-400">Loading…</div>
				{:else if filteredVaults.length === 0}
					<div class="p-4 text-center text-sm text-gray-500 dark:text-gray-400">No vaults</div>
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
					<div>
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{selectedVault.VaultName}</h2>
						<p class="text-xs text-gray-500 dark:text-gray-400 font-mono">{selectedVault.VaultARN}</p>
					</div>
					<button
						onclick={() => deleteVault(selectedVault?.VaultName)}
						class="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 border border-red-200 dark:border-red-800"
					>
						<Trash2 class="w-4 h-4" /> Delete
					</button>
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
						<div class="grid grid-cols-2 gap-4 sm:grid-cols-3">
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

					<!-- Archives -->
					{:else if activeTab === 'archives'}
						<div class="space-y-4">
							<div class="flex items-center justify-between">
								<p class="text-sm text-gray-600 dark:text-gray-400">Upload an archive to this vault. To retrieve archives, initiate an inventory retrieval job from the Jobs tab.</p>
								<button onclick={() => { showUploadModal = true; uploadDescription = ''; uploadFile = null; }} class="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md bg-blue-600 hover:bg-blue-700 text-white">
									<Upload class="w-4 h-4" /> Upload Archive
								</button>
							</div>
							<div class="bg-gray-50 dark:bg-slate-700/50 rounded-lg p-4 text-sm text-gray-600 dark:text-gray-400">
								<p class="font-medium text-gray-800 dark:text-gray-200 mb-1">About Glacier Archives</p>
								<p>Archives in Glacier cannot be directly listed — their IDs are returned when uploaded. To retrieve a list of archives in this vault, initiate an <strong>Inventory Retrieval</strong> job from the Jobs tab.</p>
								<p class="mt-2">Vault has <span class="font-semibold text-gray-900 dark:text-white">{selectedVault.NumberOfArchives ?? 0}</span> archives totalling <span class="font-semibold text-gray-900 dark:text-white">{fmtBytes(selectedVault.SizeInBytes ?? 0)}</span>.</p>
							</div>
						</div>

					<!-- Jobs -->
					{:else if activeTab === 'jobs'}
						<div class="space-y-3">
							<div class="flex items-center justify-between">
								<p class="text-sm text-gray-600 dark:text-gray-400">{jobs.length} job{jobs.length !== 1 ? 's' : ''}</p>
								<div class="flex gap-2">
									<button onclick={() => selectedVault?.VaultName && loadJobs(selectedVault.VaultName)} class="flex items-center gap-1 px-2 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-slate-700">
										<RefreshCw class="w-3.5 h-3.5 {loadingJobs ? 'animate-spin' : ''}" />
									</button>
									<button onclick={() => { showInitiateJobModal = true; }} class="flex items-center gap-1.5 px-3 py-1.5 text-sm rounded-md bg-blue-600 hover:bg-blue-700 text-white">
										<Play class="w-4 h-4" /> Initiate Job
									</button>
								</div>
							</div>
							{#if loadingJobs}
								<div class="text-center py-6 text-sm text-gray-500">Loading jobs…</div>
							{:else if jobs.length === 0}
								<div class="text-center py-6 text-sm text-gray-500 dark:text-gray-400">No jobs</div>
							{:else}
								<div class="space-y-2">
									{#each jobs as job}
										<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-700/50">
											<div class="flex items-start justify-between gap-2">
												<div class="flex-1 min-w-0">
													<p class="text-sm font-medium text-gray-900 dark:text-white">{job.Action}</p>
													<p class="text-xs text-gray-500 dark:text-gray-400 font-mono truncate">{job.JobId}</p>
													{#if job.JobDescription}
														<p class="text-xs text-gray-600 dark:text-gray-400 mt-0.5">{job.JobDescription}</p>
													{/if}
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
										<p class="text-sm font-medium text-gray-900 dark:text-white">Job Output: {selectedJob.Action}</p>
										<button onclick={() => { selectedJob = null; jobOutput = ''; }} class="text-gray-400 hover:text-gray-600"><X class="w-4 h-4" /></button>
									</div>
									{#if loadingJobOutput}
										<p class="text-sm text-gray-500">Loading output…</p>
									{:else}
										<pre class="text-xs bg-gray-50 dark:bg-slate-900 rounded p-2 overflow-auto max-h-64 text-gray-800 dark:text-gray-200">{jobOutput || '(empty)'}</pre>
									{/if}
								</div>
							{/if}
						</div>

					<!-- Tags -->
					{:else if activeTab === 'tags'}
						<div class="space-y-3">
							{#if loadingTags}
								<div class="text-sm text-gray-500 py-4 text-center">Loading tags…</div>
							{:else}
								<div class="space-y-1.5">
									{#each Object.entries(tags) as [k, v]}
										<div class="flex items-center gap-2 p-2 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600">
											<span class="text-xs font-mono text-blue-600 dark:text-blue-400">{k}</span>
											<span class="text-gray-400">=</span>
											<span class="text-xs text-gray-700 dark:text-gray-300 flex-1">{v}</span>
											<button onclick={() => removeTag(k)} disabled={taggingInFlight} class="text-red-400 hover:text-red-600">
												<X class="w-3.5 h-3.5" />
											</button>
										</div>
									{/each}
									{#if Object.keys(tags).length === 0}
										<p class="text-sm text-gray-500 dark:text-gray-400 py-2">No tags</p>
									{/if}
								</div>
								<div class="flex gap-2 pt-2 border-t border-gray-100 dark:border-gray-700">
									<input bind:value={newTagKey} placeholder="Key" class="flex-1 px-2 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									<input bind:value={newTagValue} placeholder="Value" class="flex-1 px-2 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									<button onclick={addTag} disabled={taggingInFlight || !newTagKey.trim()} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
										Add
									</button>
								</div>
							{/if}
						</div>

					<!-- Policy -->
					{:else if activeTab === 'policy'}
						<div class="space-y-3">
							{#if loadingPolicy}
								<div class="text-sm text-gray-500 py-4 text-center">Loading policy…</div>
							{:else if editingPolicy}
								<textarea bind:value={policyDraft} rows={10} class="w-full px-3 py-2 text-sm font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white resize-y"></textarea>
								<div class="flex gap-2">
									<button onclick={savePolicy} disabled={savingPolicy} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white disabled:opacity-50">
										{savingPolicy ? 'Saving…' : 'Save'}
									</button>
									<button onclick={() => { editingPolicy = false; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
										Cancel
									</button>
								</div>
							{:else if accessPolicy}
								<pre class="text-xs bg-gray-50 dark:bg-slate-900 rounded p-3 overflow-auto max-h-64 text-gray-800 dark:text-gray-200">{JSON.stringify(JSON.parse(accessPolicy || '{}'), null, 2)}</pre>
								<div class="flex gap-2">
									<button onclick={() => { policyDraft = accessPolicy; editingPolicy = true; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
										Edit
									</button>
									<button onclick={deletePolicy} class="px-3 py-1.5 text-sm rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 border border-red-200 dark:border-red-800">
										Delete
									</button>
								</div>
							{:else}
								<p class="text-sm text-gray-500 dark:text-gray-400">No access policy configured</p>
								<button onclick={() => { policyDraft = '{\n  "Version": "2012-10-17",\n  "Statement": []\n}'; editingPolicy = true; }} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white">
									Set Policy
								</button>
							{/if}
						</div>

					<!-- Notifications -->
					{:else if activeTab === 'notifications'}
						<div class="space-y-3">
							{#if loadingNotifications}
								<div class="text-sm text-gray-500 py-4 text-center">Loading notifications…</div>
							{:else if editingNotifications}
								<div class="space-y-2">
									<div>
										<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">SNS Topic ARN</p>
										<input bind:value={snsTopicDraft} placeholder="arn:aws:sns:us-east-1:…" class="w-full px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
									</div>
									<div>
										<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Events (comma-separated)</p>
										<input bind:value={snsEventsDraft} placeholder="ArchiveRetrievalCompleted, InventoryRetrievalCompleted" class="w-full px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
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
										<p class="text-sm font-mono text-gray-900 dark:text-white mt-0.5">{snsTopic}</p>
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
									<button onclick={() => { snsTopicDraft = snsTopic; snsEventsDraft = snsEvents.join(', '); editingNotifications = true; }} class="px-3 py-1.5 text-sm rounded border border-gray-200 dark:border-gray-600 text-gray-600 dark:text-gray-300">
										Edit
									</button>
									<button onclick={deleteNotifications} class="px-3 py-1.5 text-sm rounded text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 border border-red-200 dark:border-red-800">
										Delete
									</button>
								</div>
							{:else}
								<p class="text-sm text-gray-500 dark:text-gray-400">No notification configuration</p>
								<button onclick={() => { snsTopicDraft = ''; snsEventsDraft = ''; editingNotifications = true; }} class="px-3 py-1.5 text-sm rounded bg-blue-600 hover:bg-blue-700 text-white">
									Configure
								</button>
							{/if}
						</div>

					<!-- Vault Lock -->
					{:else if activeTab === 'lock'}
						<div class="space-y-4">
							{#if loadingLock}
								<div class="text-sm text-gray-500 py-4 text-center">Loading lock status…</div>
							{:else if vaultLock}
								<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-700/50 space-y-2">
									<div class="flex items-center gap-2">
										<span class="text-xs px-2 py-0.5 rounded-full font-medium
											{vaultLock.State === 'Locked' ? 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400' : 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400'}">
											{vaultLock.State}
										</span>
									</div>
									{#if vaultLock.CreationDate}<p class="text-xs text-gray-500 dark:text-gray-400">Created: {vaultLock.CreationDate}</p>{/if}
									{#if vaultLock.ExpirationDate}<p class="text-xs text-gray-500 dark:text-gray-400">Expires: {vaultLock.ExpirationDate}</p>{/if}
									{#if vaultLock.Policy}
										<pre class="text-xs bg-white dark:bg-slate-900 rounded p-2 overflow-auto max-h-40 text-gray-800 dark:text-gray-200">{vaultLock.Policy}</pre>
									{/if}
								</div>
								{#if vaultLock.State === 'InProgress'}
									<div class="space-y-2">
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
								<p class="text-sm text-gray-500 dark:text-gray-400">No vault lock policy</p>
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
								<div class="text-sm text-gray-500 py-4 text-center">Loading…</div>
							{:else if multipartUploads.length === 0}
								<div class="text-sm text-gray-500 dark:text-gray-400 py-4 text-center">No in-progress multipart uploads</div>
							{:else}
								<div class="space-y-2">
									{#each multipartUploads as up}
										<div class="p-3 rounded-lg border border-gray-200 dark:border-gray-600 bg-gray-50 dark:bg-slate-700/50 flex items-start justify-between gap-2">
											<div class="flex-1 min-w-0">
												<p class="text-xs font-mono text-gray-600 dark:text-gray-400 truncate">{up.MultipartUploadId}</p>
												{#if up.ArchiveDescription}<p class="text-sm text-gray-900 dark:text-white mt-0.5">{up.ArchiveDescription}</p>{/if}
												<p class="text-xs text-gray-500 mt-0.5">Part size: {fmtBytes(up.PartSizeInBytes ?? 0)} · Created: {up.CreationDate}</p>
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
			<div class="flex-1 bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 flex items-center justify-center py-16">
				<div class="text-center text-gray-400">
					<Archive class="w-10 h-10 mx-auto mb-3 opacity-40" />
					<p class="text-sm">Select a vault to view details</p>
				</div>
			</div>
		{/if}
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
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-sm mx-4">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Create Vault</h3>
			<div class="space-y-3">
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Vault Name</p>
					<input bind:value={newVaultName} placeholder="my-archive-vault" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
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
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-sm mx-4">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Upload Archive</h3>
			<div class="space-y-3">
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">File</p>
					<input type="file" onchange={(e) => { uploadFile = (e.currentTarget as HTMLInputElement).files?.[0] ?? null; }} class="w-full text-sm text-gray-700 dark:text-gray-300" />
				</div>
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Description (optional)</p>
					<input bind:value={uploadDescription} placeholder="Archive description" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
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

<!-- Initiate Job Modal -->
{#if showInitiateJobModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md mx-4">
			<h3 class="text-lg font-semibold text-gray-900 dark:text-white mb-4">Initiate Job</h3>
			<div class="space-y-3">
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Job Type</p>
					<select bind:value={jobType} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="inventory-retrieval">Inventory Retrieval</option>
						<option value="archive-retrieval">Archive Retrieval</option>
					</select>
				</div>
				{#if jobType === 'archive-retrieval'}
					<div>
						<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Archive ID</p>
						<input bind:value={jobArchiveId} placeholder="Archive ID" class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
					</div>
				{/if}
				<div>
					<p class="text-xs font-medium text-gray-700 dark:text-gray-300 mb-1">Retrieval Tier</p>
					<select bind:value={jobTier} class="w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
						<option value="Standard">Standard (3-5 hours)</option>
						<option value="Bulk">Bulk (5-12 hours)</option>
						<option value="Expedited">Expedited (&lt;5 min)</option>
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
