<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getFirehoseClient } from '$lib/aws-client';
	import {
		ListDeliveryStreamsCommand,
		DescribeDeliveryStreamCommand,
		CreateDeliveryStreamCommand,
		DeleteDeliveryStreamCommand,
		PutRecordCommand,
		PutRecordBatchCommand,
		StartDeliveryStreamEncryptionCommand,
		StopDeliveryStreamEncryptionCommand,
		type DeliveryStreamDescription,
		type DestinationDescription
	} from '@aws-sdk/client-firehose';
	import { toast } from 'svelte-sonner';
	import { Flame, Search, RefreshCw, Plus, Trash2, ChevronRight, Send, Database, Archive, Key, Shield, ShieldOff } from 'lucide-svelte';

	const firehose = getFirehoseClient();

	let loading = $state(false);
	let streamNames = $state<string[]>([]);
	let selectedStream = $state<DeliveryStreamDescription | null>(null);
	let loadingDetail = $state(false);
	let activeTab = $state<'overview' | 'destinations' | 'encryption' | 'put'>('overview');
	let searchQuery = $state('');

	// Create Stream
	let showCreateStream = $state(false);
	let creatingStream = $state(false);
	let newStreamName = $state('');
	let newStreamType = $state<'DirectPut' | 'KinesisStreamAsSource'>('DirectPut');
	let newDestType = $state<'s3' | 'opensearch' | 'redshift'>('s3');
	let newS3Bucket = $state('');
	let newS3Prefix = $state('');
	let newS3RoleArn = $state('');
	let newBufferSize = $state(5);
	let newBufferInterval = $state(300);

	// Put Record
	let putMode = $state<'single' | 'batch'>('single');
	let putData = $state('{"key": "value"}');
	let batchData = $state('{"id": 1, "event": "click"}\n{"id": 2, "event": "view"}\n{"id": 3, "event": "purchase"}');
	let puttingRecord = $state(false);
	let putResult = $state('');
	let batchResult = $state<{ failed: number; total: number; errors: { index: number; code?: string; message?: string }[] } | null>(null);

	const batchLines = $derived(batchData.split('\n').map((l) => l.trim()).filter((l) => l.length > 0));

	// Encryption
	let managingEncryption = $state(false);
	let newKmsKeyArn = $state('');

	const filteredNames = $derived(
		streamNames.filter((n) => !searchQuery || n.toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const statusColor = (status: string | undefined) => {
		if (!status) return 'gray';
		if (status === 'ACTIVE') return 'green';
		if (status === 'CREATING') return 'yellow';
		if (status === 'DELETING') return 'red';
		return 'gray';
	};

	async function loadStreams() {
		loading = true;
		try {
			const resp = await firehose.send(new ListDeliveryStreamsCommand({ Limit: 50 }));
			streamNames = resp.DeliveryStreamNames ?? [];
		} catch (e) {
			toast.error('Failed to load streams: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function selectStream(name: string) {
		loadingDetail = true;
		try {
			const resp = await firehose.send(new DescribeDeliveryStreamCommand({ DeliveryStreamName: name }));
			selectedStream = resp.DeliveryStreamDescription ?? null;
			activeTab = 'overview';
		} catch (e) {
			toast.error('Failed to load stream details: ' + String(e));
		} finally {
			loadingDetail = false;
		}
	}

	async function createStream() {
		if (!newStreamName.trim() || !newS3Bucket.trim()) return;
		creatingStream = true;
		try {
			await firehose.send(new CreateDeliveryStreamCommand({
				DeliveryStreamName: newStreamName.trim(),
				DeliveryStreamType: newStreamType,
				S3DestinationConfiguration: {
					BucketARN: `arn:aws:s3:::${newS3Bucket.trim()}`,
					RoleARN: newS3RoleArn.trim() || 'arn:aws:iam::123456789012:role/firehose-role',
					Prefix: newS3Prefix.trim() || undefined,
					BufferingHints: {
						SizeInMBs: newBufferSize,
						IntervalInSeconds: newBufferInterval
					}
				}
			}));
			toast.success(`Stream "${newStreamName}" created`);
			showCreateStream = false;
			resetCreateForm();
			await loadStreams();
		} catch (e) {
			toast.error('Failed to create stream: ' + String(e));
		} finally {
			creatingStream = false;
		}
	}

	function resetCreateForm() {
		newStreamName = '';
		newStreamType = 'DirectPut';
		newDestType = 's3';
		newS3Bucket = '';
		newS3Prefix = '';
		newS3RoleArn = '';
		newBufferSize = 5;
		newBufferInterval = 300;
	}

	async function deleteStream(name: string) {
		if (!await confirmDestructive({ title: 'Delete Delivery Stream', message: `Delete delivery stream "${name}"? In-flight records may be lost.` })) return;
		try {
			await firehose.send(new DeleteDeliveryStreamCommand({ DeliveryStreamName: name }));
			toast.success(`Stream "${name}" deleted`);
			if (selectedStream?.DeliveryStreamName === name) selectedStream = null;
			await loadStreams();
		} catch (e) {
			toast.error('Failed to delete stream: ' + String(e));
		}
	}

	async function putRecord() {
		if (!selectedStream?.DeliveryStreamName || !putData.trim()) return;
		puttingRecord = true;
		putResult = '';
		try {
			const resp = await firehose.send(new PutRecordCommand({
				DeliveryStreamName: selectedStream.DeliveryStreamName,
				Record: { Data: new TextEncoder().encode(putData.trim()) }
			}));
			putResult = `Record sent! RecordId: ${resp.RecordId ?? 'N/A'}`;
			toast.success('Record put successfully');
		} catch (e) {
			putResult = 'Error: ' + String(e);
			toast.error('Failed to put record: ' + String(e));
		} finally {
			puttingRecord = false;
		}
	}

	async function putRecordBatch() {
		if (!selectedStream?.DeliveryStreamName || batchLines.length === 0) return;
		if (batchLines.length > 500) {
			toast.error('PutRecordBatch accepts at most 500 records per call');
			return;
		}
		puttingRecord = true;
		batchResult = null;
		try {
			const resp = await firehose.send(new PutRecordBatchCommand({
				DeliveryStreamName: selectedStream.DeliveryStreamName,
				Records: batchLines.map((line) => ({ Data: new TextEncoder().encode(line) }))
			}));
			const responses = resp.RequestResponses ?? [];
			const errors = responses
				.map((r, index) => ({ index, code: r.ErrorCode, message: r.ErrorMessage }))
				.filter((r) => r.code);
			batchResult = { failed: resp.FailedPutCount ?? errors.length, total: batchLines.length, errors };
			if ((resp.FailedPutCount ?? 0) === 0) toast.success(`${batchLines.length} records delivered`);
			else toast.error(`${resp.FailedPutCount} of ${batchLines.length} records failed`);
		} catch (e) {
			toast.error('Failed to put record batch: ' + String(e));
		} finally {
			puttingRecord = false;
		}
	}

	async function startEncryption() {
		if (!selectedStream?.DeliveryStreamName) return;
		managingEncryption = true;
		try {
			await firehose.send(new StartDeliveryStreamEncryptionCommand({
				DeliveryStreamName: selectedStream.DeliveryStreamName,
				DeliveryStreamEncryptionConfigurationInput: {
					KeyType: newKmsKeyArn.trim() ? 'CUSTOMER_MANAGED_CMK' : 'AWS_OWNED_CMK',
					KeyARN: newKmsKeyArn.trim() || undefined
				}
			}));
			toast.success('Started delivery stream encryption update');
			newKmsKeyArn = '';
			await selectStream(selectedStream.DeliveryStreamName);
		} catch (e) {
			toast.error('Failed to update encryption: ' + String(e));
		} finally {
			managingEncryption = false;
		}
	}

	async function stopEncryption() {
		if (!selectedStream?.DeliveryStreamName) return;
		if (!await confirmDestructive({ title: 'Stop Encryption', message: `Stop encryption for delivery stream "${selectedStream.DeliveryStreamName}"?` })) return;
		managingEncryption = true;
		try {
			await firehose.send(new StopDeliveryStreamEncryptionCommand({
				DeliveryStreamName: selectedStream.DeliveryStreamName
			}));
			toast.success('Stopped delivery stream encryption');
			await selectStream(selectedStream.DeliveryStreamName);
		} catch (e) {
			toast.error('Failed to stop encryption: ' + String(e));
		} finally {
			managingEncryption = false;
		}
	}

	function formatDate(d: Date | undefined): string {
		if (!d) return '-';
		return d.toLocaleString();
	}

	function getDestinationLabel(dest: DestinationDescription): string {
		if (dest.S3DestinationDescription) return `S3: ${dest.S3DestinationDescription.BucketARN}`;
		if (dest.RedshiftDestinationDescription) return `Redshift: ${dest.RedshiftDestinationDescription.ClusterJDBCURL ?? ''}`;
		if (dest.AmazonopensearchserviceDestinationDescription) return `OpenSearch: ${dest.AmazonopensearchserviceDestinationDescription.DomainARN ?? ''}`;
		return 'Unknown destination';
	}

	function getRetryDuration(dest: DestinationDescription): number | undefined {
		return dest.RedshiftDestinationDescription?.RetryOptions?.DurationInSeconds ??
			dest.AmazonopensearchserviceDestinationDescription?.RetryOptions?.DurationInSeconds;
	}

	onMount(loadStreams);
</script>

<div class="p-6 space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Flame class="w-7 h-7 text-red-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon Data Firehose</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Reliably load real-time streams into data stores</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={loadStreams} class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
				<RefreshCw class="w-4 h-4" /> Refresh
			</button>
			<button onclick={() => (showCreateStream = true)} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium">
				<Plus class="w-4 h-4" /> Create Stream
			</button>
		</div>
	</div>

	<!-- Stat Cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-red-100 dark:bg-red-900/30 rounded-lg">
				<Flame class="w-5 h-5 text-red-600 dark:text-red-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{streamNames.length}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Delivery Streams</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<Database class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{selectedStream ? 1 : 0}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Selected Stream</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Archive class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{selectedStream?.Destinations?.length ?? 0}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Destinations</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Send class="w-5 h-5 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-gray-900 dark:text-white">{selectedStream?.DeliveryStreamStatus ?? '—'}</p>
				<p class="text-sm text-gray-500 dark:text-gray-400">Status</p>
			</div>
		</div>
	</div>

	{#if selectedStream}
		<!-- Stream Detail -->
		<div class="flex items-center gap-2 text-sm">
			<button onclick={() => { selectedStream = null; }} class="text-red-600 hover:underline">Delivery Streams</button>
			<ChevronRight class="w-4 h-4 text-gray-400" />
			<span class="text-gray-600 dark:text-gray-300 font-medium">{selectedStream.DeliveryStreamName}</span>
			<span class={`ml-2 px-2 py-0.5 rounded text-xs font-medium bg-${statusColor(selectedStream.DeliveryStreamStatus)}-100 text-${statusColor(selectedStream.DeliveryStreamStatus)}-700`}>
				{selectedStream.DeliveryStreamStatus}
			</span>
		</div>

		<!-- Tabs -->
		<div class="flex gap-1 border-b border-gray-200 dark:border-gray-700">
			{#each [['overview', 'Overview'], ['destinations', 'Destinations'], ['encryption', 'Encryption'], ['put', 'Put Record']] as [tab, label]}
				<button
					onclick={() => (activeTab = tab as 'overview' | 'destinations' | 'encryption' | 'put')}
					class={`px-4 py-2 text-sm font-medium border-b-2 transition-colors ${activeTab === tab ? 'border-red-500 text-red-600 dark:text-red-400' : 'border-transparent text-gray-500 dark:text-gray-400 hover:text-gray-700'}`}
				>
					{label}
				</button>
			{/each}
			<button onclick={() => deleteStream(selectedStream?.DeliveryStreamName ?? '')} class="ml-auto px-4 py-2 text-sm text-red-500 hover:underline">Delete</button>
		</div>

		{#if activeTab === 'overview'}
			<div class="grid grid-cols-1 md:grid-cols-2 gap-4">
				{#each [
					{ label: 'Status', value: selectedStream.DeliveryStreamStatus ?? '-' },
					{ label: 'Stream Type', value: selectedStream.DeliveryStreamType ?? '-' },
					{ label: 'Version', value: selectedStream.VersionId ?? '-' },
					{ label: 'Created', value: formatDate(selectedStream.CreateTimestamp) },
					{ label: 'Last Updated', value: formatDate(selectedStream.LastUpdateTimestamp) },
					{ label: 'Destinations', value: `${(selectedStream.Destinations ?? []).length} destination(s)` }
				] as row}
					<div class="bg-white dark:bg-gray-900 rounded-lg border border-gray-200 dark:border-gray-700 p-4">
						<div class="text-xs text-gray-500 font-medium">{row.label}</div>
						<div class="text-sm text-gray-900 dark:text-white mt-1 truncate font-mono">{row.value}</div>
					</div>
				{/each}
			</div>
		{/if}

		{#if activeTab === 'destinations'}
			{#if (selectedStream.Destinations ?? []).length === 0}
				<div class="text-center py-12 text-gray-500">
					<Database class="w-10 h-10 mx-auto mb-2 opacity-40" />
					<p>No destinations configured</p>
				</div>
			{:else}
				<div class="space-y-4">
					{#each selectedStream.Destinations ?? [] as dest}
						{@const retryDuration = getRetryDuration(dest)}
						{@const maxRetry = 7200}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
							<div class="flex items-start gap-3">
								<Archive class="w-5 h-5 text-red-500 mt-0.5" />
								<div class="flex-1">
									<div class="font-medium text-sm">{dest.DestinationId}</div>
									<div class="text-xs text-gray-500 mt-1">{getDestinationLabel(dest)}</div>

									{#if dest.S3DestinationDescription}
										<div class="mt-3 grid grid-cols-3 gap-3 text-xs">
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Buffer Size</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{dest.S3DestinationDescription.BufferingHints?.SizeInMBs ?? '-'} MB</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Buffer Interval</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{dest.S3DestinationDescription.BufferingHints?.IntervalInSeconds ?? '-'}s</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Compression</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{dest.S3DestinationDescription.CompressionFormat ?? 'UNCOMPRESSED'}</div>
											</div>
										</div>
										{#if dest.S3DestinationDescription.Prefix}
											<div class="mt-2 text-xs">
												<span class="text-gray-400">Prefix: </span>
												<span class="font-mono text-gray-700 dark:text-gray-300">{dest.S3DestinationDescription.Prefix}</span>
											</div>
										{/if}
									{/if}

									{#if dest.RedshiftDestinationDescription}
										<div class="mt-3 grid grid-cols-2 gap-3 text-xs">
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">JDBC URL</div>
												<div class="font-mono text-gray-800 dark:text-gray-200 truncate">{dest.RedshiftDestinationDescription.ClusterJDBCURL ?? '-'}</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Username</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{dest.RedshiftDestinationDescription.Username ?? '-'}</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Target Table</div>
												<div class="font-mono text-gray-800 dark:text-gray-200">{dest.RedshiftDestinationDescription.CopyCommand?.DataTableName ?? '-'}</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Retry Duration</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{dest.RedshiftDestinationDescription.RetryOptions?.DurationInSeconds ?? '-'}s</div>
											</div>
										</div>
									{/if}

									{#if dest.AmazonopensearchserviceDestinationDescription}
										{@const es = dest.AmazonopensearchserviceDestinationDescription}
										<div class="mt-3 grid grid-cols-2 gap-3 text-xs">
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Domain / Endpoint</div>
												<div class="font-mono text-gray-800 dark:text-gray-200 truncate">{es.ClusterEndpoint ?? es.DomainARN ?? '-'}</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Index Name</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{es.IndexName ?? '-'}</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Index Rotation</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{es.IndexRotationPeriod ?? '-'}</div>
											</div>
											<div class="bg-gray-50 dark:bg-gray-800 rounded p-2">
												<div class="text-gray-400 mb-1">Buffer Size / Interval</div>
												<div class="font-semibold text-gray-800 dark:text-gray-200">{es.BufferingHints?.SizeInMBs ?? '-'} MB / {es.BufferingHints?.IntervalInSeconds ?? '-'}s</div>
											</div>
										</div>
									{/if}

									{#if retryDuration !== undefined}
										<div class="mt-3 border border-orange-200 dark:border-orange-800 rounded-lg p-3 bg-orange-50 dark:bg-orange-900/20">
											<div class="flex items-center justify-between mb-2">
												<span class="text-xs font-medium text-orange-700 dark:text-orange-400">Retry Policy</span>
												<span class="text-xs font-bold text-orange-700 dark:text-orange-400">{retryDuration}s</span>
											</div>
											<div class="w-full bg-orange-100 dark:bg-orange-900/40 rounded-full h-2">
												<div
													class="bg-orange-500 dark:bg-orange-400 h-2 rounded-full transition-all"
													style="width: {Math.min(100, Math.round((retryDuration / maxRetry) * 100))}%"
												></div>
											</div>
											<div class="flex justify-between text-xs text-orange-500 mt-1">
												<span>0s</span>
												<span class="text-orange-600 dark:text-orange-400">
													{retryDuration < 60 ? `${retryDuration}s` : retryDuration < 3600 ? `${Math.round(retryDuration/60)}m` : `${(retryDuration/3600).toFixed(1)}h`}
												</span>
												<span>{maxRetry / 3600}h max</span>
											</div>
										</div>
									{/if}
								</div>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'encryption'}
			<div class="max-w-lg space-y-6">
				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
					<div class="flex items-center gap-3 mb-4">
						{#if selectedStream.DeliveryStreamEncryptionConfiguration?.Status === 'ENABLED' || selectedStream.DeliveryStreamEncryptionConfiguration?.Status === 'ENABLING'}
							<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
								<Shield class="w-6 h-6 text-green-600 dark:text-green-400" />
							</div>
							<div>
								<h3 class="font-medium text-gray-900 dark:text-white text-base">Server-Side Encryption</h3>
								<p class="text-sm text-green-600 dark:text-green-400 font-medium">
									{selectedStream.DeliveryStreamEncryptionConfiguration.Status}
									({selectedStream.DeliveryStreamEncryptionConfiguration.KeyType === 'CUSTOMER_MANAGED_CMK' ? 'Customer Managed Key' : 'AWS Owned Key'})
								</p>
							</div>
						{:else}
							<div class="p-2 bg-gray-100 dark:bg-gray-800 rounded-lg">
								<ShieldOff class="w-6 h-6 text-gray-500 dark:text-gray-400" />
							</div>
							<div>
								<h3 class="font-medium text-gray-900 dark:text-white text-base">Server-Side Encryption</h3>
								<p class="text-sm text-gray-500 dark:text-gray-400">
									{selectedStream.DeliveryStreamEncryptionConfiguration?.Status || 'DISABLED'}
								</p>
							</div>
						{/if}
					</div>

					{#if selectedStream.DeliveryStreamEncryptionConfiguration?.FailureDescription}
						<div class="mt-3 p-3 bg-red-50 text-red-700 text-sm rounded-lg border border-red-100">
							<strong>Encryption Error:</strong> {selectedStream.DeliveryStreamEncryptionConfiguration.FailureDescription.Details}
						</div>
					{/if}

					{#if selectedStream.DeliveryStreamEncryptionConfiguration?.KeyType === 'CUSTOMER_MANAGED_CMK' && selectedStream.DeliveryStreamEncryptionConfiguration.KeyARN}
						<div class="mt-4 pt-4 border-t border-gray-200 dark:border-gray-800">
							<p class="text-xs text-gray-500 font-medium mb-1">Current KMS Key ARN</p>
							<p class="text-sm font-mono text-gray-900 dark:text-white break-all">{selectedStream.DeliveryStreamEncryptionConfiguration.KeyARN}</p>
						</div>
					{/if}
				</div>

				<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-5">
					<h3 class="font-medium text-gray-900 dark:text-white mb-2">Rotate or Enable Encryption</h3>
					<p class="text-sm text-gray-500 dark:text-gray-400 mb-4">
						Update the KMS key used for server-side encryption. Leave the KMS Key ARN blank to use an AWS-owned CMK.
					</p>
					
					<div class="space-y-3">
						<div>
							<label for="kms-key-arn" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">KMS Key ARN (Optional)</label>
							<input id="kms-key-arn" bind:value={newKmsKeyArn} type="text" placeholder="arn:aws:kms:..." class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
						</div>
						
						<div class="flex gap-3 pt-2">
							<button onclick={startEncryption} disabled={managingEncryption} class="flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg bg-blue-600 text-white text-sm font-medium hover:bg-blue-700 disabled:opacity-50">
								<Key class="w-4 h-4" /> {managingEncryption ? 'Updating...' : 'Enable / Rotate Key'}
							</button>
							{#if selectedStream.DeliveryStreamEncryptionConfiguration?.Status === 'ENABLED' || selectedStream.DeliveryStreamEncryptionConfiguration?.Status === 'ENABLING'}
								<button onclick={stopEncryption} disabled={managingEncryption} class="px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-red-600 text-sm font-medium hover:bg-red-50 dark:hover:bg-red-900/20 disabled:opacity-50">
									Stop Encryption
								</button>
							{/if}
						</div>
					</div>
				</div>
			</div>
		{/if}

		{#if activeTab === 'put'}
			<div class="max-w-lg space-y-4">
				<div class="flex gap-2">
					{#each [['single', 'Single Record'], ['batch', 'Batch (PutRecordBatch)']] as [mode, label]}
						<button onclick={() => (putMode = mode as 'single' | 'batch')}
							class="px-3 py-1.5 rounded-lg text-sm font-medium {putMode === mode ? 'bg-red-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
							{label}
						</button>
					{/each}
				</div>

				{#if putMode === 'single'}
					<div>
						<label for="put-data" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Record Data (plain text or JSON)</label>
						<textarea id="put-data" bind:value={putData} rows={8} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono"></textarea>
					</div>
					<button onclick={putRecord} disabled={puttingRecord || !putData.trim()} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium disabled:opacity-50">
						<Send class="w-4 h-4" /> {puttingRecord ? 'Sending...' : 'Put Record'}
					</button>
					{#if putResult}
						<div class={`text-sm p-3 rounded-lg ${putResult.startsWith('Record') ? 'bg-green-50 text-green-700 dark:bg-green-900/20 dark:text-green-400' : 'bg-red-50 text-red-700 dark:bg-red-900/20 dark:text-red-400'}`}>
							{putResult}
						</div>
					{/if}
				{:else}
					<div>
						<label for="batch-data" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Records (one per line, max 500)</label>
						<textarea id="batch-data" bind:value={batchData} rows={8} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm font-mono"></textarea>
					</div>
					<div class="bg-gray-50 dark:bg-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 p-3">
						<div class="text-xs font-medium text-gray-500 mb-2">Preview — {batchLines.length} record(s){batchLines.length > 500 ? ' (over limit)' : ''}</div>
						<div class="space-y-1 max-h-40 overflow-y-auto">
							{#each batchLines.slice(0, 20) as line, i}
								<div class="flex gap-2 text-xs font-mono">
									<span class="text-gray-400 w-6 text-right">{i + 1}</span>
									<span class="text-gray-700 dark:text-gray-300 truncate">{line}</span>
								</div>
							{/each}
							{#if batchLines.length > 20}
								<div class="text-xs text-gray-400 pl-8">…and {batchLines.length - 20} more</div>
							{/if}
						</div>
					</div>
					<button onclick={putRecordBatch} disabled={puttingRecord || batchLines.length === 0} class="flex items-center gap-2 px-4 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm font-medium disabled:opacity-50">
						<Send class="w-4 h-4" /> {puttingRecord ? 'Sending...' : `Put ${batchLines.length} Record(s)`}
					</button>
					{#if batchResult}
						<div class={`text-sm p-3 rounded-lg ${batchResult.failed === 0 ? 'bg-green-50 text-green-700 dark:bg-green-900/20 dark:text-green-400' : 'bg-orange-50 text-orange-700 dark:bg-orange-900/20 dark:text-orange-400'}`}>
							{batchResult.total - batchResult.failed} of {batchResult.total} delivered, {batchResult.failed} failed.
							{#if batchResult.errors.length > 0}
								<ul class="mt-2 space-y-1">
									{#each batchResult.errors as err}
										<li class="text-xs font-mono">#{err.index + 1}: {err.code} — {err.message}</li>
									{/each}
								</ul>
							{/if}
						</div>
					{/if}
				{/if}
			</div>
		{/if}

	{:else}
		<!-- Stream List -->
		<div class="relative">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
			<input bind:value={searchQuery} type="text" placeholder="Search delivery streams..." class="w-full pl-10 pr-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-900 text-sm" />
		</div>

		{#if loading}
			<div class="flex justify-center py-12"><div class="animate-spin w-8 h-8 border-4 border-red-600 border-t-transparent rounded-full"></div></div>
		{:else if filteredNames.length === 0}
			<div class="text-center py-16 text-gray-500 dark:text-gray-400">
				<Flame class="w-12 h-12 mx-auto mb-3 opacity-40" />
				<p class="font-medium">No delivery streams found</p>
				<p class="text-sm mt-1">Create a stream to start delivering data</p>
			</div>
		{:else}
			<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-gray-50 dark:bg-gray-800 text-gray-600 dark:text-gray-400 uppercase text-xs">
						<tr>
							<th class="px-4 py-3 text-left">Stream Name</th>
							<th class="px-4 py-3 text-left">Actions</th>
						</tr>
					</thead>
					<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
						{#each filteredNames as name}
							<tr class="hover:bg-gray-50 dark:hover:bg-gray-800/50 transition-colors">
								<td class="px-4 py-3">
									<button onclick={() => selectStream(name)} class="text-red-600 dark:text-red-400 hover:underline font-medium">{name}</button>
								</td>
								<td class="px-4 py-3">
									<button onclick={() => deleteStream(name)} class="text-red-500 hover:text-red-700 p-1">
										<Trash2 class="w-4 h-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Stream Modal -->
{#if showCreateStream}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
		<div class="bg-white dark:bg-gray-900 rounded-xl shadow-xl w-full max-w-lg p-6 space-y-4 max-h-[90vh] overflow-y-auto">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Create Delivery Stream</h2>
			<div>
				<label for="stream-name" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Stream Name</label>
				<input id="stream-name" bind:value={newStreamName} type="text" placeholder="my-delivery-stream" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
			</div>
			<div>
				<label for="source-type" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Source</label>
				<select id="source-type" bind:value={newStreamType} class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm">
					<option value="DirectPut">Direct Put</option>
					<option value="KinesisStreamAsSource">Kinesis Stream</option>
				</select>
			</div>
			<div class="border-t border-gray-200 dark:border-gray-700 pt-4">
				<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300 mb-3">S3 Destination</h3>
				<div class="space-y-3">
					<div>
						<label for="s3-bucket" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">S3 Bucket Name</label>
						<input id="s3-bucket" bind:value={newS3Bucket} type="text" placeholder="my-bucket" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div>
						<label for="s3-prefix" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Prefix (optional)</label>
						<input id="s3-prefix" bind:value={newS3Prefix} type="text" placeholder="data/year=YYYY/month=MM/" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div>
						<label for="role-arn" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">IAM Role ARN</label>
						<input id="role-arn" bind:value={newS3RoleArn} type="text" placeholder="arn:aws:iam::123456789012:role/firehose-role" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
					</div>
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label for="buffer-size" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Buffer Size (MB)</label>
							<input id="buffer-size" bind:value={newBufferSize} type="number" min="1" max="128" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
						</div>
						<div>
							<label for="buffer-interval" class="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Buffer Interval (s)</label>
							<input id="buffer-interval" bind:value={newBufferInterval} type="number" min="60" max="900" class="w-full px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 bg-white dark:bg-gray-800 text-sm" />
						</div>
					</div>
				</div>
			</div>
			<div class="flex gap-3 pt-2">
				<button onclick={() => { showCreateStream = false; resetCreateForm(); }} class="flex-1 px-4 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-sm hover:bg-gray-50 dark:hover:bg-gray-800">Cancel</button>
				<button onclick={createStream} disabled={creatingStream || !newStreamName.trim() || !newS3Bucket.trim()} class="flex-1 px-4 py-2 rounded-lg bg-red-600 text-white text-sm font-medium hover:bg-red-700 disabled:opacity-50">
					{creatingStream ? 'Creating...' : 'Create Stream'}
				</button>
			</div>
		</div>
	</div>
{/if}
