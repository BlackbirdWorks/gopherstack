<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
import { page } from '$app/state';
import { goto } from '$app/navigation';
import { getS3Client } from '$lib/aws-client';
import { currentRegion } from '$lib/region.svelte';
import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
import {
HeadObjectCommand,
GetObjectCommand,
DeleteObjectCommand,
ListObjectVersionsCommand,
GetObjectTaggingCommand,
PutObjectTaggingCommand,
type ObjectVersion,
type Tag
} from '@aws-sdk/client-s3';
import { toast } from 'svelte-sonner';
import { Download, Trash2, Edit, ChevronLeft, Clock, FileText, Tag as TagIcon, Link, Eye } from 'lucide-svelte';

const s3 = regionalClient(getS3Client);

interface ObjectMetadata {
ContentType?: string;
ContentLength?: number;
LastModified?: Date;
ETag?: string;
StorageClass?: string;
VersionId?: string;
Metadata?: Record<string, string>;
CacheControl?: string;
ContentDisposition?: string;
ContentEncoding?: string;
}

// Read via $derived, not inside loadAll() below -- reading page.params
// directly inside the async function that onRegionChange's $effect calls
// makes that read part of the effect's own dependency tracking, which
// causes the effect to re-run a second time on every mount (every request
// below fired twice). Hoisting the read to a top-level $derived (the same
// pattern the dynamodb table-detail page uses) decouples it from the
// effect's tracked reads.
const bucket = $derived(String(page.params.bucket ?? ''));
const objectKey = $derived(String(page.params.objectKey ?? ''));
let metadata = $state<ObjectMetadata | null>(null);
let versions = $state<ObjectVersion[]>([]);
let loading = $state(true);
let loadError = $state<string | null>(null);
let showPropertiesModal = $state(false);

// Preview state
let previewVisible = $state(false);
let previewLoading = $state(false);
let previewType = $state<'image' | 'json' | 'text' | 'binary' | null>(null);
let previewContent = $state<string>('');
let previewBlobUrl = $state<string | null>(null);
let previewBinarySize = $state(0);

// Tags state
let objectTags = $state<Tag[]>([]);
let newObjTagKey = $state('');
let newObjTagValue = $state('');
let loadingTags = $state(false);

// Presigned URL state
let selectedExpiry = $state(3600);
let presignedUrl = $state('');

const expiryOptions = [
{ label: '15 min', seconds: 900 },
{ label: '1 hour', seconds: 3600 },
{ label: '6 hours', seconds: 21600 },
{ label: '24 hours', seconds: 86400 },
{ label: '7 days', seconds: 604800 }
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

async function loadAll(): Promise<void> {
await loadObjectMetadata();
await loadObjectVersions();
await loadObjectTags();
}

onRegionChange(loadAll);

async function loadObjectMetadata() {
try {
loading = true;
const response = await s3().send(new HeadObjectCommand({
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
loadError = describeError(e);
toast.error(loadError ?? 'Error');
} finally {
loading = false;
}
}

async function loadObjectVersions() {
try {
const response = await s3().send(new ListObjectVersionsCommand({
Bucket: bucket,
Prefix: objectKey,
MaxKeys: 20
}));
versions = response.Versions?.filter((v) => v.Key === objectKey) || [];
} catch (e) {
toast.error(`Failed to load versions: ${describeError(e)}`);
}
}

async function loadObjectTags() {
loadingTags = true;
try {
const res = await s3().send(new GetObjectTaggingCommand({ Bucket: bucket, Key: objectKey }));
objectTags = res.TagSet ?? [];
} catch (e) {
toast.error(`Failed to load tags: ${describeError(e)}`);
} finally {
loadingTags = false;
}
}

function addObjectTag() {
if (!newObjTagKey.trim()) return;
objectTags = [...objectTags.filter((t) => t.Key !== newObjTagKey.trim()), { Key: newObjTagKey.trim(), Value: newObjTagValue.trim() }];
newObjTagKey = '';
newObjTagValue = '';
}

function removeObjectTag(key: string) {
objectTags = objectTags.filter((t) => t.Key !== key);
}

async function saveObjectTags() {
try {
await s3().send(new PutObjectTaggingCommand({ Bucket: bucket, Key: objectKey, Tagging: { TagSet: objectTags } }));
toast.success('Tags saved');
} catch (e) {
toast.error(`Failed to save tags: ${describeError(e)}`);
}
}

// Generates a fake presigned URL with a zeroed SigV4 signature for local dev only.
// The local backend validates structure and expiry but not cryptographic validity.
// Do not use in production environments.
function generatePresignedUrl() {
const now = new Date();
const region = currentRegion();
const dateShort = now.toISOString().slice(0, 10).replaceAll('-', '');
const isoDate = now.toISOString().replaceAll('-', '').replaceAll(':', '').replace(/\.\d+Z$/, 'Z');
const encodedKey = objectKey.split('/').map(seg => encodeURIComponent(seg)).join('/');
presignedUrl = `${window.location.origin}/${encodeURIComponent(bucket)}/${encodedKey}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=test%2F${dateShort}%2F${encodeURIComponent(region)}%2Fs3%2Faws4_request&X-Amz-Date=${isoDate}&X-Amz-Expires=${selectedExpiry}&X-Amz-SignedHeaders=host&X-Amz-Signature=${'0'.repeat(64)}`;
}

function copyPresignedUrl() {
navigator.clipboard.writeText(presignedUrl).then(() => toast.success('URL copied')).catch(() => toast.error('Failed to copy'));
}

const MAX_TEXT_BYTES = 10240;
const MAX_IMAGE_BYTES = 5 * 1024 * 1024;

type PreviewKind = 'image' | 'json' | 'text' | 'binary';

// Classify how an object should be previewed, based on its content type and key.
function classifyPreviewKind(contentType: string, key: string): PreviewKind {
if (contentType.startsWith('image/')) return 'image';
if (contentType === 'application/json' || key.endsWith('.json')) return 'json';
if (contentType.startsWith('text/') || key.endsWith('.txt') || key.endsWith('.log') || key.endsWith('.csv')) return 'text';
return 'binary';
}

async function fetchPreviewTextBytes(size: number): Promise<Uint8Array | undefined> {
// bytesToFetch is the whole object when it fits under the preview cap, else
// the cap itself. The end-of-range index is inclusive (Range: bytes=0-N
// requests N+1 bytes), so it's bytesToFetch - 1, not one less than that --
// the previous formula subtracted an extra byte, silently truncating the
// last byte of every preview whose object fit entirely under the cap (and
// producing an invalid "bytes=0--1" range for 1-byte objects).
const bytesToFetch = size > 0 ? Math.min(MAX_TEXT_BYTES, size) : MAX_TEXT_BYTES;
const rangeEnd = bytesToFetch - 1;
const res = await s3().send(new GetObjectCommand({
Bucket: bucket,
Key: objectKey,
Range: `bytes=0-${rangeEnd}`
}));
return res.Body?.transformToByteArray();
}

function setBinaryPreview(size: number) {
previewType = 'binary';
previewBinarySize = size;
}

async function prepareImagePreview(contentType: string, size: number) {
if (size > MAX_IMAGE_BYTES) {
setBinaryPreview(size);
return;
}
const res = await s3().send(new GetObjectCommand({ Bucket: bucket, Key: objectKey }));
const bytes = await res.Body?.transformToByteArray();
if (!bytes) return;
previewType = 'image';
// The SDK's Uint8Array is always backed by a real ArrayBuffer, never a
	// SharedArrayBuffer -- @types/node's global Uint8Array<ArrayBufferLike>
	// default otherwise makes this look incompatible with DOM's BlobPart.
	previewBlobUrl = URL.createObjectURL(new Blob([bytes as Uint8Array<ArrayBuffer>], { type: contentType }));
}

async function prepareJsonPreview(size: number) {
const bytes = await fetchPreviewTextBytes(size);
if (!bytes) return;
const text = new TextDecoder().decode(bytes);
previewType = 'json';
try {
previewContent = JSON.stringify(JSON.parse(text), null, 2);
} catch {
previewContent = text;
}
}

async function prepareTextPreview(size: number) {
const bytes = await fetchPreviewTextBytes(size);
if (!bytes) return;
const text = new TextDecoder().decode(bytes);
previewType = 'text';
previewContent = text;
if (size > MAX_TEXT_BYTES) {
previewContent += `\n\n... (truncated, showing first 10KB of ${formatBytes(size)})`;
}
}

async function loadPreview() {
previewLoading = true;
previewVisible = true;
if (previewBlobUrl) {
URL.revokeObjectURL(previewBlobUrl);
previewBlobUrl = null;
}
const ct = metadata?.ContentType ?? '';
const size = metadata?.ContentLength ?? 0;
try {
const kind = classifyPreviewKind(ct, objectKey);
if (kind === 'image') {
await prepareImagePreview(ct, size);
} else if (kind === 'json') {
await prepareJsonPreview(size);
} else if (kind === 'text') {
await prepareTextPreview(size);
} else {
setBinaryPreview(size);
}
} catch (e) {
toast.error(`Preview failed: ${describeError(e)}`);
previewVisible = false;
} finally {
previewLoading = false;
}
}

function hidePreview() {
previewVisible = false;
if (previewBlobUrl) {
URL.revokeObjectURL(previewBlobUrl);
previewBlobUrl = null;
}
}

async function downloadObject(versionId?: string) {
try {
const response = await s3().send(new GetObjectCommand({
Bucket: bucket,
Key: objectKey,
VersionId: versionId
}));
const blob = await response.Body?.transformToByteArray();
if (blob) {
const url = URL.createObjectURL(new Blob([blob as Uint8Array<ArrayBuffer>]));
const a = document.createElement('a');
a.href = url;
a.download = objectKey.split('/').pop() || 'download';
a.click();
a.remove();
URL.revokeObjectURL(url);
}
toast.success('Download started');
} catch (e) {
toast.error(`Download failed: ${describeError(e)}`);
}
}

async function deleteVersion(versionId: string) {
if (!await confirmDestructive({ title: 'Delete Version', message: `Delete version ${versionId.slice(0, 12)}…? This cannot be undone.` })) return;
try {
await s3().send(new DeleteObjectCommand({ Bucket: bucket, Key: objectKey, VersionId: versionId }));
toast.success('Version deleted');
await loadObjectVersions();
} catch (e) {
toast.error(`Failed to delete version: ${describeError(e)}`);
}
}

async function deleteObject() {
if (!await confirmDestructive({ title: 'Delete Object', message: `Delete object "${objectKey}"? This cannot be undone.` })) return;
try {
await s3().send(new DeleteObjectCommand({ Bucket: bucket, Key: objectKey }));
toast.success('Object deleted');
goto('/dashboard/s3');
} catch (e) {
toast.error(`Delete failed: ${describeError(e)}`);
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

function storageClassBadge(sc?: string): { label: string; cls: string } {
switch (sc) {
case 'STANDARD_IA': return { label: 'STANDARD_IA', cls: 'bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300' };
case 'GLACIER': return { label: 'GLACIER', cls: 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300' };
case 'INTELLIGENT_TIERING': return { label: 'INT_TIERING', cls: 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300' };
case 'DEEP_ARCHIVE': return { label: 'DEEP_ARCHIVE', cls: 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300' };
default: return { label: 'STANDARD', cls: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300' };
}
}
</script>

<div class="space-y-6">
<!-- Header -->
<div class="flex items-center justify-between mb-6 flex-wrap gap-4">
<div class="flex items-center gap-4">
<button onclick={() => goto('/dashboard/s3')} class="p-2 hover:bg-slate-100 dark:hover:bg-slate-700 rounded-lg transition-colors">
<ChevronLeft class="w-5 h-5 text-slate-600 dark:text-slate-400" />
</button>
<div>
<p class="text-sm text-slate-600 dark:text-slate-400">S3 Object</p>
<h1 class="text-2xl font-bold text-slate-900 dark:text-white truncate">{objectKey}</h1>
<p class="text-sm text-slate-500 dark:text-slate-500">in <span class="font-mono">{bucket}</span></p>
</div>
</div>

<div class="flex gap-2 flex-wrap">
<button onclick={loadPreview} class="p-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 transition-colors flex items-center gap-2">
<Eye class="w-4 h-4" />
<span class="hidden sm:inline">Preview</span>
</button>
<button onclick={() => downloadObject()} class="p-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors flex items-center gap-2">
<Download class="w-4 h-4" />
<span class="hidden sm:inline">Download</span>
</button>
<button onclick={() => showPropertiesModal = true} class="p-2 bg-slate-600 text-white rounded-lg hover:bg-slate-700 transition-colors flex items-center gap-2">
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
{:else if loadError}
<div class="p-4 bg-red-50 dark:bg-red-900/20 border border-red-200 dark:border-red-900/50 rounded-lg text-red-700 dark:text-red-300">
{loadError}
</div>
{:else if metadata}
<!-- Preview Panel -->
{#if previewVisible}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<div class="flex items-center justify-between mb-4">
<h2 class="text-lg font-semibold text-slate-900 dark:text-white flex items-center gap-2">
<Eye class="w-5 h-5 text-indigo-500" />
Preview
</h2>
<button onclick={hidePreview} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 text-sm">Hide</button>
</div>
{#if previewLoading}
<div class="text-center py-8 text-slate-500">Loading preview...</div>
{:else if previewType === 'image' && previewBlobUrl}
<img src={previewBlobUrl} alt={objectKey} class="max-w-full max-h-96 object-contain rounded" />
{:else if previewType === 'json' || previewType === 'text'}
<pre class="bg-slate-50 dark:bg-slate-900 rounded p-4 text-xs font-mono text-slate-800 dark:text-slate-200 overflow-auto max-h-96 whitespace-pre-wrap break-all">{previewContent}</pre>
{:else if previewType === 'binary'}
<div class="text-center py-8 text-slate-500 dark:text-slate-400">
<p class="font-medium">Binary file</p>
<p class="text-sm mt-1">{formatBytes(previewBinarySize)}</p>
</div>
{/if}
</div>
{/if}

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
<span class={`inline-block text-xs font-medium px-2.5 py-0.5 rounded-full ${storageClassBadge(metadata.StorageClass).cls}`}>{storageClassBadge(metadata.StorageClass).label}</span>
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

<!-- Tags -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<h2 class="text-lg font-semibold text-slate-900 dark:text-white mb-4 flex items-center gap-2">
<TagIcon class="w-5 h-5 text-green-500" />
Object Tags
</h2>
{#if loadingTags}
<p class="text-sm text-slate-500">Loading tags...</p>
{:else}
{#if objectTags.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400 mb-4">No tags</p>
{:else}
<table class="w-full text-sm mb-4">
<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
<tr>
<th class="px-3 py-2 text-left">Key</th>
<th class="px-3 py-2 text-left">Value</th>
<th class="px-3 py-2"></th>
</tr>
</thead>
<tbody>
{#each objectTags as tag}
<tr class="border-b dark:border-slate-600">
<td class="px-3 py-2 font-mono text-slate-900 dark:text-white">{tag.Key}</td>
<td class="px-3 py-2 text-slate-700 dark:text-slate-300">{tag.Value}</td>
<td class="px-3 py-2 text-right">
<button onclick={() => removeObjectTag(tag.Key ?? '')} class="text-red-500 hover:text-red-700 text-xs">Remove</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
<div class="flex gap-2 items-end flex-wrap mb-3">
<div>
<label for="s3-tag-key" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Key</label>
<input id="s3-tag-key" type="text" bind:value={newObjTagKey} placeholder="key" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-28" />
</div>
<div>
<label for="s3-tag-value" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Value</label>
<input id="s3-tag-value" type="text" bind:value={newObjTagValue} placeholder="value" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-28" />
</div>
<button onclick={addObjectTag} class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-3 py-2">Add Tag</button>
</div>
<button onclick={saveObjectTags} class="text-white bg-green-600 hover:bg-green-700 font-medium rounded-lg text-sm px-4 py-2">Save Tags</button>
{/if}
</div>

<!-- Presigned URL -->
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<h2 class="text-lg font-semibold text-slate-900 dark:text-white mb-4 flex items-center gap-2">
<Link class="w-5 h-5 text-purple-500" />
Presigned URL
</h2>
<div class="flex gap-2 items-center flex-wrap mb-3">
<select
bind:value={selectedExpiry}
class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white"
>
{#each expiryOptions as opt}
<option value={opt.seconds}>{opt.label}</option>
{/each}
</select>
<button onclick={generatePresignedUrl} class="text-white bg-purple-600 hover:bg-purple-700 font-medium rounded-lg text-sm px-4 py-2">Generate</button>
</div>
{#if presignedUrl}
<div class="flex gap-2 items-center">
<input
type="text"
readonly
value={presignedUrl}
class="flex-1 font-mono text-xs border border-slate-300 dark:border-slate-600 rounded-lg p-2 bg-slate-50 dark:bg-slate-700 dark:text-white min-w-0"
/>
<button onclick={copyPresignedUrl} class="text-white bg-slate-600 hover:bg-slate-700 font-medium rounded-lg text-sm px-3 py-2 shrink-0">Copy</button>
</div>
{/if}
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
<div class="flex gap-2 mt-2">
<button
onclick={() => downloadObject(version.VersionId)}
disabled={!version.VersionId}
class="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 text-xs font-medium disabled:opacity-40 disabled:cursor-not-allowed"
>
Download
</button>
<button
onclick={() => version.VersionId && deleteVersion(version.VersionId)}
disabled={!version.VersionId}
class="text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 text-xs font-medium disabled:opacity-40 disabled:cursor-not-allowed"
>
Delete
</button>
</div>
</div>
{/each}
</div>
{/if}
</div>
</div>
{/if}
</div>

<!-- Properties Modal -->
{#if showPropertiesModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showPropertiesModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showPropertiesModal = false)} role="dialog" aria-modal="true">
<div class="relative p-4 w-full max-w-lg" role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Object Properties</h3>
<button aria-label="Close" onclick={() => { showPropertiesModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
<svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
</button>
</div>
<div class="p-4 space-y-3 text-sm">
{#if metadata}
<div class="grid grid-cols-2 gap-y-2">
<span class="text-slate-500 dark:text-slate-400">Content-Type</span>
<span class="font-mono text-slate-900 dark:text-white break-all">{metadata.ContentType ?? 'N/A'}</span>
<span class="text-slate-500 dark:text-slate-400">Size</span>
<span class="font-mono text-slate-900 dark:text-white">{formatBytes(metadata.ContentLength)}</span>
<span class="text-slate-500 dark:text-slate-400">ETag</span>
<span class="font-mono text-xs text-slate-900 dark:text-white break-all">{metadata.ETag ?? 'N/A'}</span>
<span class="text-slate-500 dark:text-slate-400">Last Modified</span>
<span class="text-slate-900 dark:text-white">{formatDate(metadata.LastModified)}</span>
</div>
{/if}
</div>
<div class="flex justify-end p-4 border-t dark:border-slate-600">
<button onclick={() => { showPropertiesModal = false; }} class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700">
Close
</button>
</div>
</div>
</div>
</div>
{/if}
