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
		type DeliveryStreamDescription,
		type DestinationDescription
	} from '@aws-sdk/client-firehose';
	import { toast } from 'svelte-sonner';
	import { Flame, Search, RefreshCw, Plus, Trash2, ChevronRight, Send, Database, Archive } from 'lucide-svelte';

	const firehose = getFirehoseClient();

	let loading = $state(false);
	let streamNames = $state<string[]>([]);
	let selectedStream = $state<DeliveryStreamDescription | null>(null);
	let loadingDetail = $state(false);
	let activeTab = $state<'overview' | 'destinations' | 'put'>('overview');
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
	let putData = $state('{"key": "value"}');
	let puttingRecord = $state(false);
	let putResult = $state('');

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
			{#each [['overview', 'Overview'], ['destinations', 'Destinations'], ['put', 'Put Record']] as [tab, label]}
				<button
					onclick={() => (activeTab = tab as 'overview' | 'destinations' | 'put')}
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
					{ label: 'Stream ARN', value: selectedStream.DeliveryStreamARN },
					{ label: 'Stream Type', value: selectedStream.DeliveryStreamType },
					{ label: 'Version', value: selectedStream.VersionId },
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
				<div class="space-y-3">
					{#each selectedStream.Destinations ?? [] as dest}
						<div class="bg-white dark:bg-gray-900 rounded-xl border border-gray-200 dark:border-gray-700 p-4">
							<div class="flex items-start gap-3">
								<Archive class="w-5 h-5 text-red-500 mt-0.5" />
								<div>
									<div class="font-medium text-sm">{dest.DestinationId}</div>
									<div class="text-xs text-gray-500 mt-1">{getDestinationLabel(dest)}</div>
									{#if dest.S3DestinationDescription}
										<div class="mt-2 grid grid-cols-2 gap-2 text-xs">
											<div class="text-gray-500">Buffer Size: {dest.S3DestinationDescription.BufferingHints?.SizeInMBs ?? '-'} MB</div>
											<div class="text-gray-500">Buffer Interval: {dest.S3DestinationDescription.BufferingHints?.IntervalInSeconds ?? '-'}s</div>
											<div class="text-gray-500">Compression: {dest.S3DestinationDescription.CompressionFormat ?? 'UNCOMPRESSED'}</div>
										</div>
									{/if}
								</div>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		{/if}

		{#if activeTab === 'put'}
			<div class="max-w-lg space-y-4">
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
