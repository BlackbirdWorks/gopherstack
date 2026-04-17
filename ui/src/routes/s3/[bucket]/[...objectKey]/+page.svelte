<script lang="ts">
	import { onMount } from 'svelte';
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { newS3Client } from '$lib/aws/client';
	import {
		HeadObjectCommand,
		GetObjectCommand,
		DeleteObjectCommand,
		CopyObjectCommand,
		PutObjectAclCommand,
		ListObjectVersionsCommand,
		type ObjectVersion
	} from '@aws-sdk/client-s3';
	import { toast } from 'svelte-sonner';
	import { Download, Trash2, Edit, Copy, ChevronLeft, Eye, Lock, Clock, HardDrive, FileText } from 'lucide-svelte';

	const s3 = newS3Client();
	
	let bucket = $state('');
	let objectKey = $state('');
	let metadata = $state<Record<string, any> | null>(null);
	let versions = $state<ObjectVersion[]>([]);
	let loading = $state(true);
	let error = $state<string | null>(null);
	let showPropertiesModal = $state(false);

	onMount(async () => {
		// Get bucket and object key from URL
		const bucketParam = (page.params as any).bucket;
		const objectKeyParam = String((page.params as any).objectKey || '');
		
		bucket = bucketParam;
		// SvelteKit decodes the rest param automatically for [...objectKey]
		objectKey = objectKeyParam;
		
		await loadObjectMetadata();
		await loadObjectVersions();
	});

	async function loadObjectMetadata() {
		try {
			loading = true;
			const response = await s3.send(new HeadObjectCommand({
				Bucket: bucket,
				Key: objectKey
			}));
			
			metadata = {
				ContentType: response.ContentType,
				ContentLength: response.ContentLength,
				LastModified: response.LastModified,
				ETag: response.ETag,
				StorageClass: response.StorageClass,
				VersionId: response.VersionId,
				Metadata: response.Metadata || {},
				CacheControl: response.CacheControl,
				ContentDisposition: response.ContentDisposition,
				ContentEncoding: response.ContentEncoding
			};
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to load object metadata';
			toast.error(error);
		} finally {
			loading = false;
		}
	}

	async function loadObjectVersions() {
		try {
			const response = await s3.send(new ListObjectVersionsCommand({
				Bucket: bucket,
				Prefix: objectKey,
				MaxKeys: 20
			}));
			
			versions = response.Versions?.filter(v => v.Key === objectKey) || [];
		} catch (e) {
			console.error('Failed to load versions', e);
		}
	}

	async function downloadObject() {
		try {
			const response = await s3.send(new GetObjectCommand({
				Bucket: bucket,
				Key: objectKey
			}));
			
			const blob = await response.Body?.transformToByteArray();
			if (blob) {
				const url = URL.createObjectURL(new Blob([blob]));
				const a = document.createElement('a');
				a.href = url;
				a.download = objectKey.split('/').pop() || 'download';
				a.click();
				URL.revokeObjectURL(url);
			}
			toast.success('Download started');
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to download object';
			toast.error(error);
		}
	}

	async function deleteObject() {
		if (!confirm(`Delete object "${objectKey}"?`)) return;
		
		try {
			await s3.send(new DeleteObjectCommand({
				Bucket: bucket,
				Key: objectKey
			}));
			toast.success('Object deleted');
			goto(`/dashboard/s3`);
		} catch (e) {
			error = e instanceof Error ? e.message : 'Failed to delete object';
			toast.error(error);
		}
	}

	function formatBytes(bytes?: number): string {
		if (!bytes) return '0 B';
		const k = 1024;
		const sizes = ['B', 'KB', 'MB', 'GB'];
		const i = Math.floor(Math.log(bytes) / Math.log(k));
		return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
	}

	function formatDate(date?: Date): string {
		return date ? new Date(date).toLocaleString() : 'N/A';
	}
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between mb-6">
		<div class="flex items-center gap-4">
			<button onclick={() => goto(`/dashboard/s3`)} class="p-2 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg transition-colors">
				<ChevronLeft class="w-5 h-5 text-slate-600 dark:text-slate-400" />
			</button>
			<div>
				<p class="text-sm text-slate-600 dark:text-slate-400">S3 Object</p>
				<h1 class="text-2xl font-bold text-slate-900 dark:text-white truncate">{objectKey}</h1>
				<p class="text-sm text-slate-500 dark:text-slate-500">in <span class="font-mono">{bucket}</span></p>
			</div>
		</div>

		<div class="flex gap-2">
			<button onclick={downloadObject} class="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2">
				<Download class="w-4 h-4" />
				<span class="hidden sm:inline">Download</span>
			</button>
			<button onclick={() => showPropertiesModal = true} class="p-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition-colors flex items-center gap-2">
				<Edit class="w-4 h-4" />
				<span class="hidden sm:inline">Properties</span>
			</button>
			<button onclick={deleteObject} class="p-2 bg-red-600 text-white rounded-lg hover:bg-red-700 transition-colors flex items-center gap-2">
				<Trash2 class="w-4 h-4" />
				<span class="hidden sm:inline">Delete</span>
			</button>
		</div>
	</div>

	{#if loading}
		<div class="text-center py-8 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
			<p>Loading object metadata...</p>
		</div>
	{:else if error}
		<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
			{error}
		</div>
	{:else if metadata}
		<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
			<!-- Metadata -->
			<div class="lg:col-span-2 space-y-4">
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
					<h2 class="text-lg font-semibold text-slate-900 dark:text-white mb-4 flex items-center gap-2">
						<FileText class="w-5 h-5 text-blue-500" />
						Object Details
					</h2>
					<div class="space-y-4">
						<div class="grid grid-cols-2 gap-4">
							<div>
								<p class="text-sm text-slate-600 dark:text-slate-400">Content Type</p>
								<p class="font-mono text-slate-900 dark:text-white break-all">{metadata.ContentType || 'N/A'}</p>
							</div>
							<div>
								<p class="text-sm text-slate-600 dark:text-slate-400">Size</p>
								<p class="font-mono text-slate-900 dark:text-white">{formatBytes(metadata.ContentLength)}</p>
							</div>
							<div>
								<p class="text-sm text-slate-600 dark:text-slate-400">Storage Class</p>
								<p class="font-mono text-slate-900 dark:text-white">{metadata.StorageClass || 'STANDARD'}</p>
							</div>
							<div>
								<p class="text-sm text-slate-600 dark:text-slate-400">Version ID</p>
								<p class="font-mono text-xs text-slate-900 dark:text-white break-all">{metadata.VersionId ? metadata.VersionId.slice(0, 16) + '...' : 'N/A'}</p>
							</div>
						</div>

						<div class="border-t border-slate-200 dark:border-slate-700 pt-4">
							<p class="text-sm text-slate-600 dark:text-slate-400 mb-2">Last Modified</p>
							<p class="text-slate-900 dark:text-white">{formatDate(metadata.LastModified)}</p>
						</div>

						<div class="border-t border-slate-200 dark:border-slate-700 pt-4">
							<p class="text-sm text-slate-600 dark:text-slate-400 mb-2">ETag</p>
							<p class="font-mono text-xs text-slate-900 dark:text-white break-all">{metadata.ETag}</p>
						</div>

						{#if metadata.CacheControl || metadata.ContentDisposition || metadata.ContentEncoding}
							<div class="border-t border-slate-200 dark:border-slate-700 pt-4 space-y-3">
								{#if metadata.CacheControl}
									<div>
										<p class="text-sm text-slate-600 dark:text-slate-400">Cache Control</p>
										<p class="font-mono text-slate-900 dark:text-white">{metadata.CacheControl}</p>
									</div>
								{/if}
								{#if metadata.ContentDisposition}
									<div>
										<p class="text-sm text-slate-600 dark:text-slate-400">Content Disposition</p>
										<p class="font-mono text-slate-900 dark:text-white">{metadata.ContentDisposition}</p>
									</div>
								{/if}
								{#if metadata.ContentEncoding}
									<div>
										<p class="text-sm text-slate-600 dark:text-slate-400">Content Encoding</p>
										<p class="font-mono text-slate-900 dark:text-white">{metadata.ContentEncoding}</p>
									</div>
								{/if}
							</div>
						{/if}

						{#if metadata.Metadata && Object.keys(metadata.Metadata).length > 0}
							<div class="border-t border-slate-200 dark:border-slate-700 pt-4">
								<p class="text-sm font-semibold text-slate-600 dark:text-slate-400 mb-2">Custom Metadata</p>
								<div class="space-y-1 text-sm">
									{#each Object.entries(metadata.Metadata) as [key, value]}
										<p><span class="text-slate-600 dark:text-slate-400">{key}:</span> <span class="text-slate-900 dark:text-white font-mono">{value}</span></p>
									{/each}
								</div>
							</div>
						{/if}
					</div>
				</div>
			</div>

			<!-- Versions Sidebar -->
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 h-fit">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white mb-4 flex items-center gap-2">
					<Clock class="w-5 h-5 text-purple-500" />
					Versions
				</h2>
				{#if versions.length === 0}
					<p class="text-sm text-slate-500 dark:text-slate-500">No versions available</p>
				{:else}
					<div class="space-y-2 max-h-96 overflow-y-auto">
						{#each versions as version}
							<div class="p-3 bg-slate-50 dark:bg-slate-700 rounded text-sm">
								<p class="font-mono text-xs text-slate-600 dark:text-slate-400 break-all">{version.VersionId?.slice(0, 12)}...</p>
								<p class="text-slate-700 dark:text-slate-300 mt-1">{formatDate(version.LastModified)}</p>
								<p class="text-xs text-slate-500 dark:text-slate-500 mt-1">{version.Size ? formatBytes(version.Size) : 'N/A'}</p>
							</div>
						{/each}
					</div>
				{/if}
			</div>
		</div>
	{/if}
</div>
