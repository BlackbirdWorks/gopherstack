<script lang="ts">
	import { onMount } from 'svelte';
	import { newS3Client } from '$lib/aws/client';
	import {
		ListBucketsCommand,
		CreateBucketCommand,
		DeleteBucketCommand,
		ListObjectsV2Command,
		PutBucketVersioningCommand,
		DeleteObjectCommand,
		PutObjectCommand,
		HeadObjectCommand,
		GetObjectCommand,
		CopyObjectCommand,
		GetBucketVersioningCommand,
		ListObjectVersionsCommand,
		type Bucket,
		type _Object,
		type ObjectVersion
	} from '@aws-sdk/client-s3';
	import { toast } from 'svelte-sonner';

	const s3 = newS3Client();

	let buckets = $state<Bucket[]>([]);
	let loading = $state(true);
	let searchQuery = $state('');
	let showCreateModal = $state(false);
	let newBucketName = $state('');
	let enableVersioning = $state(false);
	let creating = $state(false);
	const bucketPageSize = 10;
	let bucketPage = $state(1);

	// Bucket detail state
	let selectedBucket = $state<string | null>(null);
	let objects = $state<_Object[]>([]);
	let commonPrefixes = $state<string[]>([]);
	let currentPrefix = $state('');
	let loadingObjects = $state(false);
	let showUploadModal = $state(false);
	let uploadKey = $state('');
	let uploadFile = $state<File | null>(null);
	let uploading = $state(false);

	// Object inspection state
	let showObjectModal = $state(false);
	let selectedObject = $state<_Object | null>(null);
	let objectMetadata = $state<Record<string, unknown> | null>(null);
	let objectUserMetadata = $state<Record<string, string>>({});
	let objectVersions = $state<ObjectVersion[]>([]);
	let propertyKey = $state('');
	let propertyValue = $state('');
	let loadingMetadata = $state(false);

	// Versioning state
	let bucketVersioning = $state<string>('');
	let loadingVersioning = $state(false);
	let showVersioningModal = $state(false);
	let bucketVersions = $state<ObjectVersion[]>([]);

	const filteredBuckets = $derived(
		buckets.filter((b) => !searchQuery || (b.Name?.toLowerCase().includes(searchQuery.toLowerCase()) ?? false))
	);

	const totalBucketPages = $derived(Math.max(1, Math.ceil(filteredBuckets.length / bucketPageSize)));

	const pagedBuckets = $derived(
		filteredBuckets.slice((bucketPage - 1) * bucketPageSize, (bucketPage - 1) * bucketPageSize + bucketPageSize)
	);

	async function loadBuckets() {
		loading = true;
		try {
			const res = await s3.send(new ListBucketsCommand({}));
			buckets = res.Buckets ?? [];
			bucketPage = 1;
		} catch (err: unknown) {
			toast.error(`Failed to list buckets: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	function nextBucketPage(): void {
		if (bucketPage < totalBucketPages) {
			bucketPage += 1;
		}
	}

	function previousBucketPage(): void {
		if (bucketPage > 1) {
			bucketPage -= 1;
		}
	}

	async function createBucket() {
		if (!newBucketName.trim()) return;
		creating = true;
		try {
			await s3.send(new CreateBucketCommand({ Bucket: newBucketName.trim() }));
			if (enableVersioning) {
				await s3.send(
					new PutBucketVersioningCommand({
						Bucket: newBucketName.trim(),
						VersioningConfiguration: { Status: 'Enabled' }
					})
				);
			}
			toast.success(`Bucket "${newBucketName.trim()}" created`);
			showCreateModal = false;
			newBucketName = '';
			enableVersioning = false;
			await loadBuckets();
		} catch (err: unknown) {
			toast.error(`Failed to create bucket: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	async function deleteAllObjectsInBucket(bucketName: string): Promise<void> {
		const objs = await s3.send(new ListObjectsV2Command({ Bucket: bucketName }));
		if (!objs.Contents) return;
		for (const obj of objs.Contents) {
			if (obj.Key) {
				await s3.send(new DeleteObjectCommand({ Bucket: bucketName, Key: obj.Key }));
			}
		}
	}

	async function purgeAll() {
		if (!confirm('Are you sure you want to delete ALL buckets? This cannot be undone.')) return;
		try {
			for (const bucket of buckets) {
				if (!bucket.Name) continue;
				// Delete all objects first
				await deleteAllObjectsInBucket(bucket.Name);
				await s3.send(new DeleteBucketCommand({ Bucket: bucket.Name }));
			}
			toast.success('All buckets purged');
			await loadBuckets();
		} catch (err: unknown) {
			toast.error(`Failed to purge: ${(err as Error).message}`);
		}
	}

	async function deleteBucket(name: string) {
		if (!confirm(`Delete bucket "${name}"?`)) return;
		try {
			await deleteAllObjectsInBucket(name);
			await s3.send(new DeleteBucketCommand({ Bucket: name }));
			toast.success(`Bucket "${name}" deleted`);
			if (selectedBucket === name) {
				selectedBucket = null;
			}
			await loadBuckets();
		} catch (err: unknown) {
			toast.error(`Failed to delete bucket: ${(err as Error).message}`);
		}
	}

	async function openBucket(name: string) {
		selectedBucket = name;
		currentPrefix = '';
		await loadObjects(name, '');
		await loadBucketVersioning(name);
	}

	async function loadObjects(bucket: string, prefix: string) {
		loadingObjects = true;
		try {
			const res = await s3.send(
				new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix || undefined, Delimiter: '/' })
			);
			objects = res.Contents ?? [];
			commonPrefixes = (res.CommonPrefixes ?? []).map((p) => p.Prefix ?? '');
			currentPrefix = prefix;
		} catch (err: unknown) {
			toast.error(`Failed to list objects: ${(err as Error).message}`);
		} finally {
			loadingObjects = false;
		}
	}

	async function navigatePrefix(prefix: string) {
		if (selectedBucket) {
			await loadObjects(selectedBucket, prefix);
		}
	}

	async function goBack() {
		if (!currentPrefix) {
			selectedBucket = null;
			return;
		}
		const parts = currentPrefix.replace(/\/$/, '').split('/');
		parts.pop();
		const newPrefix = parts.length > 0 ? parts.join('/') + '/' : '';
		if (selectedBucket) {
			await loadObjects(selectedBucket, newPrefix);
		}
	}

	async function uploadObject() {
		if (!uploadFile || !selectedBucket) return;
		uploading = true;
		try {
			const key = uploadKey.trim() || (currentPrefix + uploadFile.name);
			const arrayBuffer = await uploadFile.arrayBuffer();
			await s3.send(
				new PutObjectCommand({
					Bucket: selectedBucket,
					Key: key,
					Body: new Uint8Array(arrayBuffer),
					ContentType: uploadFile.type || 'application/octet-stream'
				})
			);
			toast.success(`Uploaded "${key}"`);
			showUploadModal = false;
			uploadKey = '';
			uploadFile = null;
			await loadObjects(selectedBucket, currentPrefix);
		} catch (err: unknown) {
			toast.error(`Upload failed: ${(err as Error).message}`);
		} finally {
			uploading = false;
		}
	}

	async function deleteObject(key: string) {
		if (!selectedBucket || !confirm(`Delete "${key}"?`)) return;
		try {
			await s3.send(new DeleteObjectCommand({ Bucket: selectedBucket, Key: key }));
			toast.success(`Deleted "${key}"`);
			await loadObjects(selectedBucket, currentPrefix);
		} catch (err: unknown) {
			toast.error(`Failed to delete: ${(err as Error).message}`);
		}
	}

	function formatDate(date: Date | undefined): string {
		if (!date) return '';
		return date.toLocaleDateString() + ' ' + date.toLocaleTimeString();
	}

	function formatSize(bytes: number | undefined): string {
		if (!bytes) return '0 B';
		const units = ['B', 'KB', 'MB', 'GB', 'TB'];
		let i = 0;
		let size = bytes;
		while (size >= 1024 && i < units.length - 1) {
			size /= 1024;
			i++;
		}
		return `${size.toFixed(1)} ${units[i]}`;
	}

	async function inspectObject(obj: _Object) {
		selectedObject = obj;
		loadingMetadata = true;
		showObjectModal = true;
		try {
			const [res, versionsRes] = await Promise.all([
				s3.send(new HeadObjectCommand({ Bucket: selectedBucket!, Key: obj.Key! })),
				s3.send(new ListObjectVersionsCommand({ Bucket: selectedBucket!, Prefix: obj.Key! }))
			]);
			objectUserMetadata = res.Metadata ?? {};
			objectVersions = (versionsRes.Versions ?? []).filter((version) => version.Key === obj.Key);
			objectMetadata = {
				'Content Type': res.ContentType,
				'Content Length': formatSize(res.ContentLength),
				'Last Modified': formatDate(res.LastModified),
				'ETag': res.ETag,
				'Storage Class': res.StorageClass,
				'Version ID': res.VersionId || '(no versioning)',
				'Metadata Keys': Object.keys(objectUserMetadata)
			};
		} catch (err: unknown) {
			toast.error(`Failed to inspect object: ${(err as Error).message}`);
			objectMetadata = null;
			objectUserMetadata = {};
			objectVersions = [];
		} finally {
			loadingMetadata = false;
		}
	}

	async function downloadObjectVersion(versionId?: string) {
		if (!selectedBucket || !selectedObject?.Key) return;
		try {
			const res = await s3.send(
				new GetObjectCommand({
					Bucket: selectedBucket,
					Key: selectedObject.Key,
					VersionId: versionId
				})
			);
			const bytes = res.Body && 'transformToByteArray' in res.Body
				? await res.Body.transformToByteArray()
				: new TextEncoder().encode(res.Body && 'transformToString' in res.Body ? await res.Body.transformToString() : '');
			const blob = new Blob([bytes], { type: res.ContentType || 'application/octet-stream' });
			const url = URL.createObjectURL(blob);
			const a = document.createElement('a');
			a.href = url;
			a.download = selectedObject.Key.split('/').pop() || selectedObject.Key;
			a.click();
			URL.revokeObjectURL(url);
			toast.success(`Downloaded "${selectedObject.Key}"`);
		} catch (err: unknown) {
			toast.error(`Failed to download object: ${(err as Error).message}`);
		}
	}

	async function deleteObjectVersion(versionId?: string) {
		if (!selectedBucket || !selectedObject?.Key) return;
		if (!confirm(`Delete "${selectedObject.Key}"${versionId ? ` version ${versionId}` : ''}?`)) return;
		try {
			await s3.send(
				new DeleteObjectCommand({
					Bucket: selectedBucket,
					Key: selectedObject.Key,
					VersionId: versionId
				})
			);
			toast.success(`Deleted "${selectedObject.Key}"`);
			await loadObjects(selectedBucket, currentPrefix);
			await inspectObject(selectedObject);
		} catch (err: unknown) {
			toast.error(`Failed to delete object: ${(err as Error).message}`);
		}
	}

	async function updateObjectProperty() {
		if (!selectedBucket || !selectedObject?.Key || !propertyKey.trim()) return;
		try {
			await s3.send(
				new CopyObjectCommand({
					Bucket: selectedBucket,
					Key: selectedObject.Key,
					CopySource: `${selectedBucket}/${selectedObject.Key}`,
					MetadataDirective: 'REPLACE',
					Metadata: { ...objectUserMetadata, [propertyKey.trim()]: propertyValue }
				})
			);
			propertyKey = '';
			propertyValue = '';
			toast.success(`Updated properties for "${selectedObject.Key}"`);
			await inspectObject(selectedObject);
		} catch (err: unknown) {
			toast.error(`Failed to update properties: ${(err as Error).message}`);
		}
	}

	async function loadBucketVersioning(bucket: string) {
		loadingVersioning = true;
		try {
			const res = await s3.send(new GetBucketVersioningCommand({ Bucket: bucket }));
			bucketVersioning = res.Status || 'Disabled';
		} catch (err: unknown) {
			toast.error(`Failed to check versioning: ${(err as Error).message}`);
			bucketVersioning = 'Unknown';
		} finally {
			loadingVersioning = false;
		}
	}

	async function toggleBucketVersioning() {
		if (!selectedBucket) return;
		const newStatus = bucketVersioning === 'Enabled' ? 'Suspended' : 'Enabled';
		try {
			await s3.send(
				new PutBucketVersioningCommand({
					Bucket: selectedBucket,
					VersioningConfiguration: { Status: newStatus as 'Enabled' | 'Suspended' }
				})
			);
			bucketVersioning = newStatus;
			toast.success(`Versioning ${newStatus.toLowerCase()} for "${selectedBucket}"`);
		} catch (err: unknown) {
			toast.error(`Failed to update versioning: ${(err as Error).message}`);
		}
	}

	async function loadBucketVersionsList() {
		if (!selectedBucket) return;
		try {
			const res = await s3.send(
				new ListObjectVersionsCommand({ Bucket: selectedBucket })
			);
			bucketVersions = res.Versions || [];
		} catch (err: unknown) {
			toast.error(`Failed to load versions: ${(err as Error).message}`);
		}
	}

	onMount(() => {
		loadBuckets();
	});

	$effect(() => {
		if (bucketPage > totalBucketPages) {
			bucketPage = totalBucketPages;
		}
	});
</script>

<div class="space-y-6">
	{#if selectedBucket}
		<!-- Bucket Detail View -->
		<nav class="flex" aria-label="Breadcrumb">
			<ol class="inline-flex items-center space-x-1 md:space-x-2">
				<li class="inline-flex items-center">
					<button
						onclick={() => { selectedBucket = null; }}
						class="inline-flex items-center text-sm font-medium text-slate-700 hover:text-blue-600 dark:text-slate-400 dark:hover:text-white"
					>
						<svg class="w-3 h-3 me-2.5" fill="currentColor" viewBox="0 0 20 20"><path d="m19.707 9.293-2-2-7-7a1 1 0 0 0-1.414 0l-7 7-2 2a1 1 0 0 0 1.414 1.414L2 10.414V18a2 2 0 0 0 2 2h3a1 1 0 0 0 1-1v-4a1 1 0 0 1 1-1h2a1 1 0 0 1 1 1v4a1 1 0 0 0 1 1h3a2 2 0 0 0 2-2v-7.586l.293.293a1 1 0 0 0 1.414-1.414Z" /></svg>
						Buckets
					</button>
				</li>
				<li>
					<div class="flex items-center">
						<svg class="w-3 h-3 text-slate-400 mx-1" fill="none" viewBox="0 0 6 10"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 9 4-4-4-4" /></svg>
						<span class="ms-1 text-sm font-medium text-slate-500 dark:text-slate-400">{selectedBucket}</span>
					</div>
				</li>
				{#if currentPrefix}
					{#each currentPrefix
						.replace(/\/$/, '')
						.split('/')
						.filter((p) => p) as part}
						<li>
							<div class="flex items-center">
								<svg class="w-3 h-3 text-slate-400 mx-1" fill="none" viewBox="0 0 6 10"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 9 4-4-4-4" /></svg>
								<span class="ms-1 text-sm font-medium text-slate-500 dark:text-slate-400">{part}</span>
							</div>
						</li>
					{/each}
				{/if}
			</ol>
		</nav>

		<div class="flex justify-between items-center">
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">{selectedBucket}</h1>
			<div class="flex gap-2 flex-wrap">
				<button
					onclick={goBack}
					class="text-slate-700 bg-white hover:bg-slate-100 border border-slate-300 focus:ring-4 focus:ring-slate-200 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700 dark:focus:ring-slate-700"
				>
					Back
				</button>
				<button
					onclick={() => { showVersioningModal = true; loadBucketVersionsList(); }}
					class={`font-medium rounded-lg text-sm px-5 py-2.5 transition-colors ${bucketVersioning === 'Enabled' ? 'text-white bg-green-600 hover:bg-green-700 dark:bg-green-700 dark:hover:bg-green-800' : 'text-white bg-slate-500 hover:bg-slate-600 dark:bg-slate-700 dark:hover:bg-slate-800'}`}
				>
					{loadingVersioning ? 'Loading...' : `Versioning: ${bucketVersioning}`}
				</button>
				<button
					onclick={() => { showUploadModal = true; }}
					class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800"
				>
					Upload File
				</button>
			</div>
		</div>

		<div class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl">
			{#if loadingObjects}
				<div class="flex items-center justify-center p-8">
					<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none">
						<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
						<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
					</svg>
				</div>
			{:else if objects.length === 0 && commonPrefixes.length === 0}
				<div class="text-center py-12 text-slate-500">
					<p class="text-lg font-medium">This bucket is empty</p>
					<p class="text-sm mt-1">Upload files to get started</p>
				</div>
			{:else}
				<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
					<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
						<tr>
							<th class="px-6 py-3">Name</th>
							<th class="px-6 py-3 text-right">Size</th>
							<th class="px-6 py-3 text-right">Last Modified</th>
							<th class="px-6 py-3 text-right">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#each commonPrefixes as prefix}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700 cursor-pointer" onclick={() => navigatePrefix(prefix)}>
								<td class="px-6 py-4 font-medium text-slate-900 dark:text-white flex items-center gap-2">
									<svg class="w-5 h-5 text-yellow-500" fill="currentColor" viewBox="0 0 20 20"><path d="M2 6a2 2 0 012-2h5l2 2h5a2 2 0 012 2v6a2 2 0 01-2 2H4a2 2 0 01-2-2V6z" /></svg>
									{prefix.replace(currentPrefix, '').replace(/\/$/, '')}
								</td>
								<td class="px-6 py-4 text-right">—</td>
								<td class="px-6 py-4 text-right">—</td>
								<td class="px-6 py-4 text-right">—</td>
							</tr>
						{/each}
						{#each objects.filter((o) => o.Key !== currentPrefix) as obj}
							<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
								<td class="px-6 py-4 font-medium text-slate-900 dark:text-white flex items-center gap-2">
									<svg class="w-5 h-5 text-slate-400" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M7 21h10a2 2 0 002-2V9.414a1 1 0 00-.293-.707l-5.414-5.414A1 1 0 0012.586 3H7a2 2 0 00-2 2v14a2 2 0 002 2z" /></svg>
									<button
										type="button"
										onclick={() => inspectObject(obj)}
										class="hover:text-blue-600 dark:hover:text-blue-400 hover:underline text-left"
									>
										{(obj.Key ?? '').replace(currentPrefix, '')}
									</button>
								</td>
								<td class="px-6 py-4 text-right">{formatSize(obj.Size)}</td>
								<td class="px-6 py-4 text-right">{formatDate(obj.LastModified)}</td>
								<td class="px-6 py-4 text-right space-x-2">
									<button
										onclick={() => inspectObject(obj)}
										class="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 text-xs font-medium"
									>
										Inspect
									</button>
									<button
										onclick={() => deleteObject(obj.Key ?? '')}
										class="text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 text-xs font-medium"
									>
										Delete
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			{/if}
		</div>
	{:else}
		<!-- Bucket List View -->
		<div class="bg-white/80 dark:bg-slate-800/80 backdrop-blur-md p-6 rounded-xl border border-slate-200 dark:border-slate-700 shadow-sm flex flex-col sm:flex-row sm:items-center justify-between gap-4 mb-8">
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white flex items-center gap-3">
					<img src="/dashboard/static/icons/s3.svg" class="w-8 h-8 rounded-md shadow-sm" alt="s3" />
					S3 Buckets
				</h1>
				<p class="mt-2 text-sm text-slate-600 dark:text-slate-400">Manage your Object Storage buckets.</p>
			</div>
			<div class="flex gap-2">
				<button
					onclick={purgeAll}
					class="text-white bg-red-700 hover:bg-red-800 focus:ring-4 focus:ring-red-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-red-600 dark:hover:bg-red-700 focus:outline-none dark:focus:ring-red-800"
				>
					Purge All
				</button>
				<button
					onclick={() => { showCreateModal = true; }}
					class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 focus:outline-none dark:focus:ring-blue-800"
				>
					+ Create Bucket
				</button>
			</div>
		</div>

		<div class="flex flex-col md:flex-row justify-between items-end gap-4">
			<div class="w-full max-w-xs">
				<label for="bucket-search" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Search Buckets</label>
				<div class="relative">
					<div class="absolute inset-y-0 start-0 flex items-center ps-3 pointer-events-none">
						<svg class="w-4 h-4 text-slate-500 dark:text-slate-400" fill="none" viewBox="0 0 20 20"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m19 19-4-4m0-7A7 7 0 1 1 1 8a7 7 0 0 1 14 0Z" /></svg>
					</div>
					<input
						type="text"
						id="bucket-search"
						placeholder="Search buckets..."
						bind:value={searchQuery}
						class="block w-full p-2 ps-10 text-sm text-slate-900 border border-slate-300 rounded-lg bg-slate-50 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white dark:focus:ring-blue-500 dark:focus:border-blue-500"
					/>
				</div>
			</div>
		</div>

		{#if loading}
			<div class="flex items-center justify-center p-8">
				<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none">
					<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
					<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
				</svg>
			</div>
		{:else if filteredBuckets.length === 0}
			<div class="text-center py-12 text-slate-500">
				<svg class="w-16 h-16 mx-auto mb-4 text-slate-300 dark:text-slate-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="1.5" d="M20 7l-8-4-8 4m16 0l-8 4m8-4v10l-8 4m0-10L4 7m8 4v10M4 7v10l8 4" /></svg>
				<p class="text-lg font-medium">No buckets found</p>
				<p class="text-sm mt-1">Create your first bucket to get started</p>
			</div>
		{:else}
			<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
				{#each pagedBuckets as bucket}
					<div class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl hover:shadow-md transition-shadow cursor-pointer group">
						<div class="flex justify-between items-start">
							<button onclick={() => openBucket(bucket.Name ?? '')} class="flex-1 text-left">
								<h3 class="text-base font-semibold text-slate-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
									{bucket.Name}
								</h3>
								<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">
									Created: {formatDate(bucket.CreationDate)}
								</p>
							</button>
							<button
								type="button"
								onclick={() => deleteBucket(bucket.Name ?? '')}
								class="text-xs text-slate-500 hover:text-red-500 dark:hover:text-red-400 p-1"
								title="Delete bucket"
							>
								Delete
							</button>
						</div>
					</div>
				{/each}
			</div>
			<div class="mt-4 flex items-center justify-end gap-2">
				<button
					type="button"
					onclick={previousBucketPage}
					disabled={bucketPage === 1}
					class="rounded-lg border border-slate-300 px-3 py-1.5 text-sm text-slate-700 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200"
				>
					Previous
				</button>
				<span class="text-xs text-slate-500 dark:text-slate-400">Page {bucketPage} of {totalBucketPages}</span>
				<button
					type="button"
					onclick={nextBucketPage}
					disabled={bucketPage >= totalBucketPages}
					class="rounded-lg border border-slate-300 px-3 py-1.5 text-sm text-slate-700 disabled:opacity-50 dark:border-slate-600 dark:text-slate-200"
				>
					Next
				</button>
			</div>
		{/if}
	{/if}
</div>

<!-- Create Bucket Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showCreateModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-md" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Bucket</h3>
					<button onclick={() => { showCreateModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
						<svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
					</button>
				</div>
				<div class="p-4 md:p-5">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createBucket(); }}>
						<div>
							<label for="bucketName" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Bucket Name</label>
							<input type="text" id="bucketName" bind:value={newBucketName} placeholder="my-bucket" required
								class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
							<p class="mt-1 text-xs text-slate-500 dark:text-slate-400">Must be unique and follow S3 naming rules</p>
						</div>
						<div class="flex items-center">
							<input id="versioning" type="checkbox" bind:checked={enableVersioning}
								class="w-4 h-4 text-blue-600 bg-slate-100 border-slate-300 rounded focus:ring-blue-500 dark:focus:ring-blue-600 dark:ring-offset-slate-800 focus:ring-2 dark:bg-slate-700 dark:border-slate-600" />
							<label for="versioning" class="ms-2 text-sm font-medium text-slate-900 dark:text-slate-300">Enable Versioning</label>
						</div>
						<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
							<button type="button" onclick={() => { showCreateModal = false; }}
								class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 focus:ring-4 focus:ring-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700 dark:focus:ring-slate-700">
								Cancel
							</button>
							<button type="submit" disabled={creating}
								class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800 disabled:opacity-50">
								{creating ? 'Creating...' : 'Create'}
							</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Upload File Modal -->
{#if showUploadModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showUploadModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-2xl" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Upload File</h3>
					<button onclick={() => { showUploadModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
						<svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
					</button>
				</div>
				<div class="p-4 md:p-5">
					<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); uploadObject(); }}>
						<div>
							<label for="file_input" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">File</label>
							<input id="file_input" type="file" required onchange={(e) => { uploadFile = (e.target as HTMLInputElement).files?.[0] ?? null; }}
								class="block w-full text-sm text-slate-900 border border-slate-300 rounded-lg cursor-pointer bg-slate-50 dark:text-slate-400 dark:bg-slate-700 dark:border-slate-600" />
						</div>
						<div>
							<label for="key" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Key (Optional - defaults to filename)</label>
							<input type="text" id="key" bind:value={uploadKey} placeholder="path/to/file.txt"
								class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white" />
						</div>
						<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
							<button type="button" onclick={() => { showUploadModal = false; }}
								class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 focus:ring-4 focus:ring-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700 dark:focus:ring-slate-700">
								Cancel
							</button>
							<button type="submit" disabled={uploading}
								class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800 disabled:opacity-50">
								{uploading ? 'Uploading...' : 'Upload'}
							</button>
						</div>
					</form>
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Object Inspection Modal -->
{#if showObjectModal && selectedObject}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showObjectModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-2xl" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Object Details</h3>
					<button onclick={() => { showObjectModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
						<svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
					</button>
				</div>
				<div class="p-4 md:p-5">
					{#if loadingMetadata}
						<div class="flex items-center justify-center p-8">
							<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none">
								<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
								<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
							</svg>
						</div>
					{:else}
						<div class="space-y-4">
							<div>
								<h4 class="text-sm font-semibold text-slate-900 dark:text-white mb-1">Object Key</h4>
								<p class="text-sm text-slate-600 dark:text-slate-400 break-all font-mono">{selectedObject.Key}</p>
							</div>
							{#if objectMetadata}
								<div class="grid grid-cols-2 gap-4 pt-4 border-t border-slate-200 dark:border-slate-600">
									{#each Object.entries(objectMetadata) as [key, value]}
										<div>
											<h5 class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase">{key}</h5>
											<p class="text-sm text-slate-900 dark:text-slate-100 break-all">
												{#if Array.isArray(value)}
													{value.join(', ') || '(none)'}
												{:else}
													{value || '(empty)'}
												{/if}
											</p>
										</div>
									{/each}
								</div>
							{/if}

							<div class="pt-4 border-t border-slate-200 dark:border-slate-600">
								<h4 class="text-sm font-semibold text-slate-900 dark:text-white mb-2">Object Versions</h4>
								{#if objectVersions.length === 0}
									<p class="text-sm text-slate-600 dark:text-slate-400">No versions available for this object.</p>
								{:else}
									<div class="space-y-2 max-h-48 overflow-y-auto">
										{#each objectVersions as version}
											<div class="p-3 bg-slate-50 dark:bg-slate-800 rounded border border-slate-200 dark:border-slate-600 text-xs">
												<p class="font-mono text-slate-900 dark:text-white truncate">{version.VersionId || '(current)'}</p>
												<p class="text-slate-600 dark:text-slate-400 mt-1">{formatDate(version.LastModified)}</p>
												<div class="mt-2 flex gap-3">
													<button type="button" onclick={() => downloadObjectVersion(version.VersionId)} class="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 font-medium">Download</button>
													<button type="button" onclick={() => deleteObjectVersion(version.VersionId)} class="text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 font-medium">Delete</button>
												</div>
											</div>
										{/each}
									</div>
								{/if}
							</div>

							<div class="pt-4 border-t border-slate-200 dark:border-slate-600">
								<h4 class="text-sm font-semibold text-slate-900 dark:text-white mb-2">Change Properties</h4>
								<div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
									<input type="text" bind:value={propertyKey} placeholder="Property key" class="border border-slate-300 rounded-lg p-2 text-sm dark:bg-slate-800 dark:border-slate-600" />
									<input type="text" bind:value={propertyValue} placeholder="Property value" class="border border-slate-300 rounded-lg p-2 text-sm dark:bg-slate-800 dark:border-slate-600" />
								</div>
								<div class="mt-3 flex gap-2">
									<button type="button" onclick={() => downloadObjectVersion()} class="py-2 px-3 text-sm font-medium text-white bg-blue-600 rounded-lg hover:bg-blue-700">Download Latest</button>
									<button type="button" onclick={() => deleteObjectVersion()} class="py-2 px-3 text-sm font-medium text-white bg-red-600 rounded-lg hover:bg-red-700">Delete Object</button>
									<button type="button" onclick={updateObjectProperty} class="py-2 px-3 text-sm font-medium text-white bg-emerald-600 rounded-lg hover:bg-emerald-700">Save Properties</button>
								</div>
							</div>
						</div>
						<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600 mt-6">
							<button type="button" onclick={() => { showObjectModal = false; }}
								class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 focus:ring-4 focus:ring-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700 dark:focus:ring-slate-700">
								Close
							</button>
						</div>
					{/if}
				</div>
			</div>
		</div>
	</div>
{/if}

<!-- Versioning Modal -->
{#if showVersioningModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showVersioningModal = false; }} role="dialog" aria-modal="true">
		<div class="relative p-4 w-full max-w-2xl" onclick={(e) => e.stopPropagation()} role="document">
			<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
				<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
					<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Versioning Management</h3>
					<button onclick={() => { showVersioningModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
						<svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
					</button>
				</div>
				<div class="p-4 md:p-5 space-y-4">
					<div class="p-4 bg-slate-50 dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-600">
						<div class="flex items-center justify-between mb-4">
							<div>
								<h4 class="font-semibold text-slate-900 dark:text-white">Current Status</h4>
								<p class="text-sm text-slate-600 dark:text-slate-400 mt-1">Versioning is {bucketVersioning === 'Enabled' ? 'enabled' : 'disabled'} for this bucket</p>
							</div>
							<button onclick={toggleBucketVersioning}
								class={`font-medium rounded-lg text-sm px-4 py-2 transition-colors ${bucketVersioning === 'Enabled' ? 'text-white bg-green-600 hover:bg-green-700' : 'text-white bg-slate-500 hover:bg-slate-600'}`}>
								{bucketVersioning === 'Enabled' ? 'Disable' : 'Enable'} Versioning
							</button>
						</div>
					</div>

					<div>
						<h4 class="font-semibold text-slate-900 dark:text-white mb-2">Version History</h4>
						{#if bucketVersions.length === 0}
							<p class="text-sm text-slate-600 dark:text-slate-400">No versions found. Enable versioning and upload files to track versions.</p>
						{:else}
							<div class="space-y-2 max-h-96 overflow-y-auto">
								{#each bucketVersions.slice(0, 20) as version}
									<div class="p-3 bg-slate-50 dark:bg-slate-800 rounded border border-slate-200 dark:border-slate-600 text-sm">
										<p class="font-mono text-slate-900 dark:text-white truncate">{version.Key}</p>
										<div class="text-xs text-slate-600 dark:text-slate-400 mt-1 space-y-0.5">
											<p>Version ID: {version.VersionId || '(current)'}</p>
											<p>Modified: {version.LastModified ? formatDate(version.LastModified) : '(unknown)'}</p>
											<p>Size: {formatSize(version.Size)}</p>
										</div>
									</div>
								{/each}
								{#if bucketVersions.length > 20}
									<p class="text-xs text-slate-500 text-center py-2">Showing 20 of {bucketVersions.length} versions</p>
								{/if}
							</div>
						{/if}
					</div>

					<div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
						<button type="button" onclick={() => { showVersioningModal = false; }}
							class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 focus:ring-4 focus:ring-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700 dark:focus:ring-slate-700">
							Close
						</button>
					</div>
				</div>
			</div>
		</div>
	</div>
{/if}
