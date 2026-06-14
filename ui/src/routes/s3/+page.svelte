<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
import { onMount } from 'svelte';
import { goto } from '$app/navigation';
import { newS3Client, getStoredRegion } from '$lib/aws/client';
import {
ListBucketsCommand,
CreateBucketCommand,
DeleteBucketCommand,
ListObjectsV2Command,
PutBucketVersioningCommand,
DeleteObjectCommand,
PutObjectCommand,
GetBucketVersioningCommand,
ListObjectVersionsCommand,
GetBucketTaggingCommand,
PutBucketTaggingCommand,
DeleteBucketTaggingCommand,
GetBucketPolicyCommand,
PutBucketPolicyCommand,
DeleteBucketPolicyCommand,
GetBucketCorsCommand,
PutBucketCorsCommand,
DeleteBucketCorsCommand,
GetBucketLifecycleConfigurationCommand,
PutBucketLifecycleConfigurationCommand,
DeleteBucketLifecycleCommand,
GetBucketEncryptionCommand,
PutBucketEncryptionCommand,
GetBucketLocationCommand,
GetObjectLockConfigurationCommand,
GetObjectCommand,
CopyObjectCommand,
ListMultipartUploadsCommand,
ListPartsCommand,
AbortMultipartUploadCommand,
GetBucketWebsiteCommand,
PutBucketWebsiteCommand,
DeleteBucketWebsiteCommand,
GetBucketLoggingCommand,
PutBucketLoggingCommand,
type Bucket,
type _Object,
type ObjectVersion,
type Tag,
type CORSRule,
type LifecycleRule
} from '@aws-sdk/client-s3';
import { toast } from 'svelte-sonner';

let s3 = newS3Client();

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
let activeDetailTab = $state<'objects' | 'properties' | 'tagging' | 'permissions' | 'lifecycle' | 'cors' | 'uploads' | 'analytics'>('objects');
type MultipartUploadEntry = { key: string; uploadId: string; initiated?: Date; partsCompleted: number; bytesUploaded: number; };
let multipartUploads = $state<MultipartUploadEntry[]>([]);
let loadingUploads = $state(false);
let objects = $state<_Object[]>([]);
let commonPrefixes = $state<string[]>([]);
let currentPrefix = $state('');
let loadingObjects = $state(false);
let showUploadModal = $state(false);
let uploadKey = $state('');
let uploadFile = $state<File | null>(null);
let uploading = $state(false);
let selectedObjects = $state(new Set<string>());
let filterObjects = $state('');
let isDragging = $state(false);

// Properties tab state
let bucketVersioning = $state<string>('');
let loadingVersioning = $state(false);
let bucketVersions = $state<ObjectVersion[]>([]);
let bucketEncryption = $state<string>('None');
let bucketLocation = $state<string>('');
let objectLockStatus = $state<string>('Disabled');
let loadingProperties = $state(false);

// Tagging tab state
let bucketTags = $state<Tag[]>([]);
let newTagKey = $state('');
let newTagValue = $state('');
let loadingTags = $state(false);

// Permissions tab state
let bucketPolicy = $state<string>('');
let loadingPolicy = $state(false);

// Lifecycle tab state
let lifecycleRules = $state<LifecycleRule[]>([]);
let newLifecycleId = $state('');
let newLifecyclePrefix = $state('');
let newLifecycleDays = $state(30);
let loadingLifecycle = $state(false);

// CORS tab state
let corsRules = $state<CORSRule[]>([]);
let newCorsOrigins = $state('');
let newCorsMethods = $state<string[]>(['GET']);
let newCorsHeaders = $state('');
let newCorsMaxAge = $state(3600);
let loadingCors = $state(false);

// Create folder state
let showCreateFolderModal = $state(false);
let newFolderName = $state('');

// Sort state for objects
let sortField = $state<'name' | 'size' | 'date'>('name');
let sortOrder = $state<'asc' | 'desc'>('asc');

// Bucket sizes (lazy loaded)
let bucketSizes = $state<Map<string, number>>(new Map());

// Rename state
let showRenameModal = $state(false);
let renameOldKey = $state('');
let renameNewKey = $state('');
let showCopyModal = $state(false);
let copySourceKey = $state('');
let copyTargetKey = $state('');

// Website hosting state
let websiteConfig = $state<{ IndexDocument?: string; ErrorDocument?: string } | null>(null);
let loadingWebsite = $state(false);

// Access logging state
let loggingEnabled = $state(false);
let loggingTargetBucket = $state('');
let loggingTargetPrefix = $state('');
let loadingLogging = $state(false);
let savingLogging = $state(false);

// Storage analytics state (size by prefix, computed from listed objects)
let analyticsLoading = $state(false);
type PrefixStat = { prefix: string; count: number; bytes: number };
let analyticsByPrefix = $state<PrefixStat[]>([]);
let analyticsTotalBytes = $state(0);
let analyticsTotalCount = $state(0);
let analyticsTruncated = $state(false);

// Bucket sort state
let bucketSortOrder = $state<'alpha' | 'newest' | 'largest'>('alpha');

const filteredBuckets = $derived(
buckets.filter((b) => !searchQuery || (b.Name?.toLowerCase().includes(searchQuery.toLowerCase()) ?? false))
);

const totalBucketPages = $derived(Math.max(1, Math.ceil(filteredBuckets.length / bucketPageSize)));

const sortedBuckets = $derived(
  [...filteredBuckets].toSorted((a, b) => {
    if (bucketSortOrder === 'newest') return (b.CreationDate?.getTime() ?? 0) - (a.CreationDate?.getTime() ?? 0);
    if (bucketSortOrder === 'largest') return (bucketSizes.get(b.Name ?? '') ?? 0) - (bucketSizes.get(a.Name ?? '') ?? 0);
    return (a.Name ?? '').localeCompare(b.Name ?? '');
  })
);

const pagedBuckets = $derived(
  sortedBuckets.slice((bucketPage - 1) * bucketPageSize, bucketPage * bucketPageSize)
);

const filteredObjects = $derived(
objects.filter((o) => !filterObjects || (o.Key ?? '').toLowerCase().includes(filterObjects.toLowerCase()))
);

const filteredPrefixes = $derived(
commonPrefixes.filter((p) => !filterObjects || p.toLowerCase().includes(filterObjects.toLowerCase()))
);

const sortedObjects = $derived(
  [...filteredObjects].toSorted((a, b) => {
    if (sortField === 'size') {
      const diff = (a.Size ?? 0) - (b.Size ?? 0);
      return sortOrder === 'asc' ? diff : -diff;
    }
    if (sortField === 'date') {
      const diff = (a.LastModified?.getTime() ?? 0) - (b.LastModified?.getTime() ?? 0);
      return sortOrder === 'asc' ? diff : -diff;
    }
    const diff = (a.Key ?? '').localeCompare(b.Key ?? '');
    return sortOrder === 'asc' ? diff : -diff;
  })
);

async function loadBuckets() {
loading = true;
try {
const res = await s3.send(new ListBucketsCommand({}));
buckets = res.Buckets ?? [];
bucketPage = 1;
void loadBucketSizes(buckets);
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
if (!await confirmDestructive({ title: 'Delete All Buckets', message: 'Delete ALL S3 buckets and their contents? This cannot be undone.', confirmLabel: 'Delete All' })) return;
try {
for (const bucket of buckets) {
if (!bucket.Name) continue;
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
if (!await confirmDestructive({ title: 'Delete Bucket', message: `Delete bucket "${name}"? The bucket must be empty before deletion.` })) return;
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
  activeDetailTab = 'objects';
  currentPrefix = '';
  selectedObjects = new Set<string>();
  filterObjects = '';
  await loadObjects();
}

async function loadObjects(reset = true): Promise<void> {
  if (!selectedBucket) return;
  loadingObjects = true;
  if (reset) { objects = []; commonPrefixes = []; }
  try {
    let continuationToken: string | undefined;
    let pageCount = 0;
    const allObjects: _Object[] = [];
    const allPrefixes: string[] = [];
    do {
      const res = await s3.send(new ListObjectsV2Command({
        Bucket: selectedBucket,
        Prefix: currentPrefix || undefined,
        Delimiter: '/',
        ContinuationToken: continuationToken,
      }));
      allObjects.push(...(res.Contents ?? []));
      allPrefixes.push(...(res.CommonPrefixes ?? []).map((p) => p.Prefix ?? '').filter(Boolean));
      continuationToken = res.IsTruncated ? res.NextContinuationToken : undefined;
      pageCount++;
      if (pageCount >= 10) {
        toast.warning(`Showing first 10,000 objects. Use a filter to narrow results.`);
        break;
      }
    } while (continuationToken);
    objects = allObjects;
    commonPrefixes = allPrefixes;
  } catch (err: unknown) {
    toast.error(`Failed to load objects: ${(err as Error).message}`);
  } finally {
    loadingObjects = false;
  }
}

async function navigatePrefix(prefix: string) {
  if (selectedBucket) {
    currentPrefix = prefix;
    await loadObjects();
  }
}

async function goBack() {
  if (!currentPrefix) {
    selectedBucket = null;
    return;
  }
  const parts = currentPrefix.replace(/\/$/, '').split('/');
  parts.pop();
  currentPrefix = parts.length > 0 ? parts.join('/') + '/' : '';
  if (selectedBucket) {
    await loadObjects();
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
await loadObjects();
} catch (err: unknown) {
toast.error(`Upload failed: ${(err as Error).message}`);
} finally {
uploading = false;
}
}

async function handleDrop(e: DragEvent): Promise<void> {
  e.preventDefault();
  isDragging = false;
  const files = e.dataTransfer?.files;
  if (!files || files.length === 0 || !selectedBucket) return;
  let uploaded = 0;
  for (let i = 0; i < files.length; i++) {
    const file = files[i];
    try {
      toast.info(`Uploading ${i + 1} of ${files.length}: ${file.name}`);
      const key = `${currentPrefix}${file.name}`;
      const buf = await file.arrayBuffer();
      await s3.send(new PutObjectCommand({
        Bucket: selectedBucket,
        Key: key,
        Body: new Uint8Array(buf),
        ContentType: file.type || 'application/octet-stream',
      }));
      uploaded++;
    } catch (err: unknown) {
      toast.error(`Failed to upload ${file.name}: ${(err as Error).message}`);
    }
  }
  toast.success(`Uploaded ${uploaded} of ${files.length} files`);
  await loadObjects();
}

async function deleteObject(key: string) {
if (!selectedBucket || !await confirmDestructive({ title: 'Delete Object', message: `Delete "${key}"? This cannot be undone.` })) return;
try {
await s3.send(new DeleteObjectCommand({ Bucket: selectedBucket, Key: key }));
toast.success(`Deleted "${key}"`);
await loadObjects();
} catch (err: unknown) {
toast.error(`Failed to delete: ${(err as Error).message}`);
}
}

async function deleteSelectedObjects() {
if (!selectedBucket || selectedObjects.size === 0) return;
if (!await confirmDestructive({ title: 'Delete Objects', message: `Delete ${selectedObjects.size} selected object(s)? This cannot be undone.` })) return;
try {
for (const key of selectedObjects) {
await s3.send(new DeleteObjectCommand({ Bucket: selectedBucket, Key: key }));
}
toast.success(`Deleted ${selectedObjects.size} object(s)`);
selectedObjects = new Set<string>();
await loadObjects();
} catch (err: unknown) {
toast.error(`Failed to delete: ${(err as Error).message}`);
}
}

async function downloadObject(key: string) {
if (!selectedBucket) return;
try {
const res = await s3.send(new GetObjectCommand({ Bucket: selectedBucket, Key: key }));
const bytes = await res.Body?.transformToByteArray();
if (!bytes) return;
const url = URL.createObjectURL(new Blob([bytes]));
const a = document.createElement('a');
a.href = url;
a.download = key.split('/').pop() ?? 'download';
a.click();
a.remove();
URL.revokeObjectURL(url);
toast.success('Download started');
} catch (err: unknown) {
toast.error(`Download failed: ${(err as Error).message}`);
}
}

function copyObjectUrl(key: string) {
const url = `${window.location.origin}/${selectedBucket}/${key}`;
navigator.clipboard.writeText(url).then(() => toast.success('URL copied')).catch(() => toast.error('Failed to copy URL'));
}

function toggleObjectSelection(key: string) {
const next = new Set(selectedObjects);
if (next.has(key)) {
next.delete(key);
} else {
next.add(key);
}
selectedObjects = next;
}

function inspectObject(obj: _Object) {
if (!selectedBucket || !obj.Key) return;
goto(`/dashboard/s3/${selectedBucket}/${obj.Key}`);
}

async function loadPropertiesTab() {
if (!selectedBucket) return;
loadingProperties = true;
try {
const [vRes, locRes] = await Promise.allSettled([
s3.send(new GetBucketVersioningCommand({ Bucket: selectedBucket })),
s3.send(new GetBucketLocationCommand({ Bucket: selectedBucket }))
]);
if (vRes.status === 'fulfilled') bucketVersioning = vRes.value.Status || 'Disabled';
if (locRes.status === 'fulfilled') bucketLocation = locRes.value.LocationConstraint || 'us-east-1';
try {
const encRes = await s3.send(new GetBucketEncryptionCommand({ Bucket: selectedBucket }));
bucketEncryption = encRes.ServerSideEncryptionConfiguration?.Rules?.[0]?.ApplyServerSideEncryptionByDefault?.SSEAlgorithm || 'None';
} catch {
bucketEncryption = 'None';
}
try {
const lockRes = await s3.send(new GetObjectLockConfigurationCommand({ Bucket: selectedBucket }));
objectLockStatus = lockRes.ObjectLockConfiguration?.ObjectLockEnabled || 'Disabled';
} catch {
objectLockStatus = 'Disabled';
}
try {
const versRes = await s3.send(new ListObjectVersionsCommand({ Bucket: selectedBucket }));
bucketVersions = versRes.Versions || [];
} catch {
bucketVersions = [];
}
} catch (err: unknown) {
toast.error(`Failed to load properties: ${(err as Error).message}`);
} finally {
loadingProperties = false;
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

async function toggleEncryption() {
if (!selectedBucket) return;
try {
if (bucketEncryption === 'None') {
await s3.send(new PutBucketEncryptionCommand({
Bucket: selectedBucket,
ServerSideEncryptionConfiguration: {
Rules: [{ ApplyServerSideEncryptionByDefault: { SSEAlgorithm: 'AES256' } }]
}
}));
bucketEncryption = 'AES256';
toast.success('AES256 encryption enabled');
} else {
await s3.send(new PutBucketEncryptionCommand({
Bucket: selectedBucket,
ServerSideEncryptionConfiguration: { Rules: [] }
}));
bucketEncryption = 'None';
toast.success('Encryption disabled');
}
} catch (err: unknown) {
toast.error(`Failed to update encryption: ${(err as Error).message}`);
}
}

async function loadTagsTab() {
if (!selectedBucket) return;
loadingTags = true;
try {
const res = await s3.send(new GetBucketTaggingCommand({ Bucket: selectedBucket }));
bucketTags = res.TagSet ?? [];
} catch (err: unknown) {
const e = err as { Code?: string; name?: string };
		const code = e.Code ?? e.name;
if (code === 'NoSuchTagSet') {
bucketTags = [];
} else {
toast.error(`Failed to load tags: ${(err as Error).message}`);
}
} finally {
loadingTags = false;
}
}

function addTag() {
if (!newTagKey.trim()) return;
bucketTags = [...bucketTags.filter((t) => t.Key !== newTagKey.trim()), { Key: newTagKey.trim(), Value: newTagValue.trim() }];
newTagKey = '';
newTagValue = '';
}

function removeTag(key: string) {
bucketTags = bucketTags.filter((t) => t.Key !== key);
}

async function saveTags() {
if (!selectedBucket) return;
try {
await s3.send(new PutBucketTaggingCommand({ Bucket: selectedBucket, Tagging: { TagSet: bucketTags } }));
toast.success('Tags saved');
} catch (err: unknown) {
toast.error(`Failed to save tags: ${(err as Error).message}`);
}
}

async function clearAllTags() {
if (!selectedBucket || !await confirmDestructive({ title: 'Delete All Tags', message: 'Remove all tags from this bucket?' })) return;
try {
await s3.send(new DeleteBucketTaggingCommand({ Bucket: selectedBucket }));
bucketTags = [];
toast.success('All tags cleared');
} catch (err: unknown) {
toast.error(`Failed to clear tags: ${(err as Error).message}`);
}
}

async function loadPermissionsTab() {
if (!selectedBucket) return;
loadingPolicy = true;
try {
const res = await s3.send(new GetBucketPolicyCommand({ Bucket: selectedBucket }));
bucketPolicy = res.Policy ?? '';
} catch (err: unknown) {
const e = err as { Code?: string; name?: string };
		const code = e.Code ?? e.name;
if (code === 'NoSuchBucketPolicy') {
bucketPolicy = '';
} else {
toast.error(`Failed to load policy: ${(err as Error).message}`);
}
} finally {
loadingPolicy = false;
}
}

async function savePolicy() {
if (!selectedBucket) return;
try {
await s3.send(new PutBucketPolicyCommand({ Bucket: selectedBucket, Policy: bucketPolicy }));
toast.success('Policy saved');
} catch (err: unknown) {
toast.error(`Failed to save policy: ${(err as Error).message}`);
}
}

async function deletePolicy() {
if (!selectedBucket || !await confirmDestructive({ title: 'Delete Bucket Policy', message: 'Remove the bucket policy? Access control will revert to default bucket settings.' })) return;
try {
await s3.send(new DeleteBucketPolicyCommand({ Bucket: selectedBucket }));
bucketPolicy = '';
toast.success('Policy deleted');
} catch (err: unknown) {
toast.error(`Failed to delete policy: ${(err as Error).message}`);
}
}

async function loadLifecycleTab() {
if (!selectedBucket) return;
loadingLifecycle = true;
try {
const res = await s3.send(new GetBucketLifecycleConfigurationCommand({ Bucket: selectedBucket }));
lifecycleRules = res.Rules ?? [];
} catch (err: unknown) {
const e = err as { Code?: string; name?: string };
		const code = e.Code ?? e.name;
if (code === 'NoSuchLifecycleConfiguration') {
lifecycleRules = [];
} else {
toast.error(`Failed to load lifecycle: ${(err as Error).message}`);
}
} finally {
loadingLifecycle = false;
}
}

async function addLifecycleRule() {
if (!selectedBucket || !newLifecycleId.trim()) return;
const newRule: LifecycleRule = {
ID: newLifecycleId.trim(),
Status: 'Enabled',
Filter: { Prefix: newLifecyclePrefix },
Expiration: { Days: newLifecycleDays }
};
const updatedRules = [...lifecycleRules, newRule];
try {
await s3.send(new PutBucketLifecycleConfigurationCommand({ Bucket: selectedBucket, LifecycleConfiguration: { Rules: updatedRules } }));
lifecycleRules = updatedRules;
newLifecycleId = '';
newLifecyclePrefix = '';
newLifecycleDays = 30;
toast.success('Lifecycle rule added');
} catch (err: unknown) {
toast.error(`Failed to add rule: ${(err as Error).message}`);
}
}

async function deleteLifecycleRule(id: string) {
if (!selectedBucket) return;
const updatedRules = lifecycleRules.filter((r) => r.ID !== id);
try {
if (updatedRules.length === 0) {
await s3.send(new DeleteBucketLifecycleCommand({ Bucket: selectedBucket }));
} else {
await s3.send(new PutBucketLifecycleConfigurationCommand({ Bucket: selectedBucket, LifecycleConfiguration: { Rules: updatedRules } }));
}
lifecycleRules = updatedRules;
toast.success('Lifecycle rule deleted');
} catch (err: unknown) {
toast.error(`Failed to delete rule: ${(err as Error).message}`);
}
}

async function loadCorsTab() {
if (!selectedBucket) return;
loadingCors = true;
try {
const res = await s3.send(new GetBucketCorsCommand({ Bucket: selectedBucket }));
corsRules = res.CORSRules ?? [];
} catch (err: unknown) {
const e = err as { Code?: string; name?: string };
		const code = e.Code ?? e.name;
if (code === 'NoSuchCORSConfiguration') {
corsRules = [];
} else {
toast.error(`Failed to load CORS: ${(err as Error).message}`);
}
} finally {
loadingCors = false;
}
}

async function addCorsRule() {
if (!selectedBucket || !newCorsOrigins.trim()) return;
const newRule: CORSRule = {
AllowedOrigins: newCorsOrigins.split(',').map((o) => o.trim()).filter(Boolean),
AllowedMethods: newCorsMethods,
AllowedHeaders: newCorsHeaders ? newCorsHeaders.split(',').map((h) => h.trim()) : ['*'],
MaxAgeSeconds: newCorsMaxAge
};
const updatedRules = [...corsRules, newRule];
try {
await s3.send(new PutBucketCorsCommand({ Bucket: selectedBucket, CORSConfiguration: { CORSRules: updatedRules } }));
corsRules = updatedRules;
newCorsOrigins = '';
newCorsMethods = ['GET'];
newCorsHeaders = '';
newCorsMaxAge = 3600;
toast.success('CORS rule added');
} catch (err: unknown) {
toast.error(`Failed to add CORS rule: ${(err as Error).message}`);
}
}

async function deleteCorsRule(idx: number) {
if (!selectedBucket) return;
const updatedRules = corsRules.filter((_, i) => i !== idx);
try {
if (updatedRules.length === 0) {
await s3.send(new DeleteBucketCorsCommand({ Bucket: selectedBucket }));
} else {
await s3.send(new PutBucketCorsCommand({ Bucket: selectedBucket, CORSConfiguration: { CORSRules: updatedRules } }));
}
corsRules = updatedRules;
toast.success('CORS rule deleted');
} catch (err: unknown) {
toast.error(`Failed to delete CORS rule: ${(err as Error).message}`);
}
}

async function createFolder(): Promise<void> {
  if (!selectedBucket || !newFolderName.trim()) return;
  const key = `${currentPrefix}${newFolderName.trim().replace(/\/+$/, '')}/`;
  try {
    await s3.send(new PutObjectCommand({
      Bucket: selectedBucket,
      Key: key,
      Body: new Uint8Array(0),
      ContentType: 'application/x-directory',
    }));
    toast.success(`Folder "${newFolderName}" created`);
    showCreateFolderModal = false;
    newFolderName = '';
    await loadObjects();
  } catch (err: unknown) {
    toast.error(`Failed to create folder: ${(err as Error).message}`);
  }
}

async function loadBucketSizes(bucketList: Bucket[]): Promise<void> {
  const results = await Promise.allSettled(
    bucketList.map(async (b) => {
      if (!b.Name) return;
      let total = 0;
      let token: string | undefined;
      do {
        const res = await s3.send(new ListObjectsV2Command({
          Bucket: b.Name,
          ContinuationToken: token,
          MaxKeys: 1000,
        }));
        for (const obj of res.Contents ?? []) total += obj.Size ?? 0;
        token = res.IsTruncated ? res.NextContinuationToken : undefined;
      } while (token);
      bucketSizes = new Map(bucketSizes).set(b.Name, total);
    })
  );
  void results;
}

async function copyObject(): Promise<void> {
  if (!selectedBucket || !copySourceKey || !copyTargetKey.trim()) return;
  if (copyTargetKey.trim() === copySourceKey) {
    toast.error('Target key must differ from source');
    return;
  }
  try {
    await s3.send(new CopyObjectCommand({
      Bucket: selectedBucket,
      CopySource: `${selectedBucket}/${copySourceKey}`,
      Key: copyTargetKey.trim(),
    }));
    toast.success('Object copied');
    showCopyModal = false;
    await loadObjects();
  } catch (err: unknown) {
    toast.error(`Copy failed: ${(err as Error).message}`);
  }
}

async function renameObject(): Promise<void> {
  if (!selectedBucket || !renameOldKey || !renameNewKey.trim()) return;
  try {
    await s3.send(new CopyObjectCommand({
      Bucket: selectedBucket,
      CopySource: `${selectedBucket}/${renameOldKey}`,
      Key: renameNewKey.trim(),
    }));
    await s3.send(new DeleteObjectCommand({ Bucket: selectedBucket, Key: renameOldKey }));
    toast.success('Object renamed');
    showRenameModal = false;
    await loadObjects();
  } catch (err: unknown) {
    toast.error(`Rename failed: ${(err as Error).message}`);
  }
}

async function loadWebsite(): Promise<void> {
  if (!selectedBucket) return;
  loadingWebsite = true;
  try {
    const res = await s3.send(new GetBucketWebsiteCommand({ Bucket: selectedBucket }));
    websiteConfig = {
      IndexDocument: res.IndexDocument?.Suffix,
      ErrorDocument: res.ErrorDocument?.Key,
    };
  } catch (err: unknown) {
    const e = err as { Code?: string; name?: string };
    const code = e.Code ?? e.name;
    if (code === 'NoSuchWebsiteConfiguration') {
      websiteConfig = null;
    } else {
      toast.error(`Failed to load website config: ${(err as Error).message}`);
    }
  } finally {
    loadingWebsite = false;
  }
}

async function saveWebsite(): Promise<void> {
  if (!selectedBucket || !websiteConfig) return;
  try {
    await s3.send(new PutBucketWebsiteCommand({
      Bucket: selectedBucket,
      WebsiteConfiguration: {
        IndexDocument: { Suffix: websiteConfig.IndexDocument ?? 'index.html' },
        ErrorDocument: websiteConfig.ErrorDocument ? { Key: websiteConfig.ErrorDocument } : undefined,
      },
    }));
    toast.success('Website configuration saved');
  } catch (err: unknown) {
    toast.error(`Failed to save website config: ${(err as Error).message}`);
  }
}

async function deleteWebsite(): Promise<void> {
  if (!selectedBucket || !await confirmDestructive({ title: 'Remove Website Configuration', message: 'Remove the static website hosting configuration from this bucket?', confirmLabel: 'Remove' })) return;
  try {
    await s3.send(new DeleteBucketWebsiteCommand({ Bucket: selectedBucket }));
    websiteConfig = null;
    toast.success('Website configuration deleted');
  } catch (err: unknown) {
    toast.error(`Failed to delete website config: ${(err as Error).message}`);
  }
}

function websiteEndpointUrl(): string {
  if (!selectedBucket) return '';
  return `${window.location.origin}/${selectedBucket}/${websiteConfig?.IndexDocument ?? 'index.html'}`;
}

async function loadLogging(): Promise<void> {
  if (!selectedBucket) return;
  loadingLogging = true;
  try {
    const res = await s3.send(new GetBucketLoggingCommand({ Bucket: selectedBucket }));
    const le = res.LoggingEnabled;
    loggingEnabled = !!le;
    loggingTargetBucket = le?.TargetBucket ?? '';
    loggingTargetPrefix = le?.TargetPrefix ?? '';
  } catch (err: unknown) {
    toast.error(`Failed to load access logging: ${(err as Error).message}`);
  } finally {
    loadingLogging = false;
  }
}

async function saveLogging(): Promise<void> {
  if (!selectedBucket) return;
  if (loggingEnabled && !loggingTargetBucket.trim()) {
    toast.error('Target bucket is required to enable access logging');
    return;
  }
  savingLogging = true;
  try {
    await s3.send(new PutBucketLoggingCommand({
      Bucket: selectedBucket,
      BucketLoggingStatus: loggingEnabled
        ? { LoggingEnabled: { TargetBucket: loggingTargetBucket.trim(), TargetPrefix: loggingTargetPrefix.trim() || `${selectedBucket}/` } }
        : {}
    }));
    toast.success(loggingEnabled ? 'Access logging enabled' : 'Access logging disabled');
    await loadLogging();
  } catch (err: unknown) {
    toast.error(`Failed to save access logging: ${(err as Error).message}`);
  } finally {
    savingLogging = false;
  }
}

async function loadAnalytics(): Promise<void> {
  if (!selectedBucket) return;
  analyticsLoading = true;
  analyticsByPrefix = [];
  analyticsTotalBytes = 0;
  analyticsTotalCount = 0;
  analyticsTruncated = false;
  try {
    const stats = new Map<string, PrefixStat>();
    let token: string | undefined;
    let pages = 0;
    do {
      const res = await s3.send(new ListObjectsV2Command({ Bucket: selectedBucket, ContinuationToken: token, MaxKeys: 1000 }));
      for (const obj of res.Contents ?? []) {
        const key = obj.Key ?? '';
        const size = obj.Size ?? 0;
        analyticsTotalBytes += size;
        analyticsTotalCount += 1;
        const topPrefix = key.includes('/') ? key.slice(0, key.indexOf('/') + 1) : '(root)';
        const cur = stats.get(topPrefix) ?? { prefix: topPrefix, count: 0, bytes: 0 };
        cur.count += 1;
        cur.bytes += size;
        stats.set(topPrefix, cur);
      }
      token = res.IsTruncated ? res.NextContinuationToken : undefined;
      pages += 1;
      // Cap at 10k objects (10 pages) to keep the UI responsive on huge buckets.
      if (pages >= 10) {
        analyticsTruncated = !!token;
        break;
      }
    } while (token);
    analyticsByPrefix = Array.from(stats.values()).toSorted((a, b) => b.bytes - a.bytes);
  } catch (err: unknown) {
    toast.error(`Failed to compute analytics: ${(err as Error).message}`);
  } finally {
    analyticsLoading = false;
  }
}

function fileIcon(key: string): string {
  const ext = key.split('.').pop()?.toLowerCase() ?? '';
  if (['jpg', 'jpeg', 'png', 'gif', 'webp', 'svg', 'ico'].includes(ext)) return '🖼️';
  if (['mp4', 'mov', 'avi', 'mkv', 'webm'].includes(ext)) return '🎥';
  if (['mp3', 'wav', 'ogg', 'flac'].includes(ext)) return '🎵';
  if (['pdf'].includes(ext)) return '📄';
  if (['json', 'yaml', 'yml', 'xml', 'toml'].includes(ext)) return '📋';
  if (['js', 'ts', 'jsx', 'tsx', 'py', 'go', 'rs', 'java', 'c', 'cpp'].includes(ext)) return '💻';
  if (['zip', 'tar', 'gz', 'bz2', '7z'].includes(ext)) return '📦';
  if (['txt', 'md', 'log', 'csv'].includes(ext)) return '📝';
  return '📄';
}

async function switchTab(tab: typeof activeDetailTab) {
activeDetailTab = tab;
if (tab === 'properties') { await loadPropertiesTab(); await loadWebsite(); await loadLogging(); }
else if (tab === 'analytics') await loadAnalytics();
else if (tab === 'tagging') await loadTagsTab();
else if (tab === 'permissions') await loadPermissionsTab();
else if (tab === 'lifecycle') await loadLifecycleTab();
else if (tab === 'cors') await loadCorsTab();
else if (tab === 'uploads') await loadMultipartUploads();
}

async function loadMultipartUploads(): Promise<void> {
  if (!selectedBucket) return;
  loadingUploads = true;
  try {
    const res = await s3.send(new ListMultipartUploadsCommand({ Bucket: selectedBucket }));
    const uploads = res.Uploads ?? [];
    // ListParts in parallel to compute per-upload completion stats.
    const entries: MultipartUploadEntry[] = await Promise.all(
      uploads.map(async (u) => {
        let partsCompleted = 0;
        let bytesUploaded = 0;
        if (u.Key && u.UploadId) {
          try {
            const partsRes = await s3.send(new ListPartsCommand({
              Bucket: selectedBucket!,
              Key: u.Key,
              UploadId: u.UploadId,
            }));
            const parts = partsRes.Parts ?? [];
            partsCompleted = parts.length;
            bytesUploaded = parts.reduce((sum, p) => sum + (p.Size ?? 0), 0);
          } catch {
            // Listing parts can race with abort; surface zero progress.
          }
        }
        return {
          key: u.Key ?? '',
          uploadId: u.UploadId ?? '',
          initiated: u.Initiated,
          partsCompleted,
          bytesUploaded,
        };
      }),
    );
    multipartUploads = entries;
  } catch (err: unknown) {
    toast.error(`Failed to list uploads: ${(err as Error).message}`);
    multipartUploads = [];
  } finally {
    loadingUploads = false;
  }
}

async function abortMultipartUpload(key: string, uploadId: string): Promise<void> {
  if (!selectedBucket) return;
  try {
    await s3.send(new AbortMultipartUploadCommand({
      Bucket: selectedBucket,
      Key: key,
      UploadId: uploadId,
    }));
    toast.success('Upload aborted');
    await loadMultipartUploads();
  } catch (err: unknown) {
    toast.error(`Abort failed: ${(err as Error).message}`);
  }
}

function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MB`;
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GB`;
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

function storageClassBadge(sc?: string): { label: string; cls: string } {
switch (sc) {
case 'STANDARD_IA': return { label: 'STANDARD_IA', cls: 'bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300' };
case 'GLACIER': return { label: 'GLACIER', cls: 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300' };
case 'INTELLIGENT_TIERING': return { label: 'INT_TIERING', cls: 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300' };
case 'DEEP_ARCHIVE': return { label: 'DEEP_ARCHIVE', cls: 'bg-indigo-100 text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300' };
default: return { label: 'STANDARD', cls: 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300' };
}
}

function toggleCorsMethod(method: string) {
if (newCorsMethods.includes(method)) {
newCorsMethods = newCorsMethods.filter((m) => m !== method);
} else {
newCorsMethods = [...newCorsMethods, method];
}
}

onMount(() => {
loadBuckets();

function refreshS3Client(region: string | null | undefined): void {
if (!region) return;
s3 = newS3Client(region);
void loadBuckets();
}
const handleStorage = (e: StorageEvent) => {
if (e.key === 'gopherstack_region') refreshS3Client(e.newValue);
};
const handleRegionChange = (e: Event) => {
const region = e instanceof CustomEvent && typeof e.detail === 'string'
? e.detail
: getStoredRegion();
refreshS3Client(region);
};
window.addEventListener('storage', handleStorage);
window.addEventListener('gopherstack:region-change', handleRegionChange);
return () => {
window.removeEventListener('storage', handleStorage);
window.removeEventListener('gopherstack:region-change', handleRegionChange);
};
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
<button onclick={async () => { currentPrefix = ''; await loadObjects(); }} class="ms-1 text-sm font-medium text-slate-500 hover:text-blue-600 dark:text-slate-400 dark:hover:text-white">{selectedBucket}</button>
</div>
</li>
{#if currentPrefix}
{#each currentPrefix.replace(/\/$/, '').split('/').filter((p) => p) as part, i}
{@const partPrefix = currentPrefix.replace(/\/$/, '').split('/').filter((p) => p).slice(0, i + 1).join('/') + '/'}
<li>
<div class="flex items-center">
<svg class="w-3 h-3 text-slate-400 mx-1" fill="none" viewBox="0 0 6 10"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 9 4-4-4-4" /></svg>
{#if i < currentPrefix.replace(/\/$/, '').split('/').filter((p) => p).length - 1}
<button onclick={() => navigatePrefix(partPrefix)} class="ms-1 text-sm font-medium text-slate-500 hover:text-blue-600 dark:text-slate-400 dark:hover:text-white">{part}</button>
{:else}
<span class="ms-1 text-sm font-medium text-slate-500 dark:text-slate-400">{part}</span>
{/if}
</div>
</li>
{/each}
{/if}
</ol>
</nav>

<div class="flex justify-between items-center flex-wrap gap-2">
<h1 class="text-3xl font-bold text-slate-900 dark:text-white">{selectedBucket}</h1>
<div class="flex gap-2 flex-wrap">
<button
onclick={goBack}
class="text-slate-700 bg-white hover:bg-slate-100 border border-slate-300 focus:ring-4 focus:ring-slate-200 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700 dark:focus:ring-slate-700"
>
Back
</button>
{#if activeDetailTab === 'objects'}
<button
id="upload-file-btn"
onclick={() => { showUploadModal = true; }}
class="text-white bg-blue-700 hover:bg-blue-800 focus:ring-4 focus:ring-blue-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-blue-600 dark:hover:bg-blue-700 dark:focus:ring-blue-800"
>
Upload File
</button>
<button
  onclick={() => { showCreateFolderModal = true; }}
  class="text-white bg-green-600 hover:bg-green-700 focus:ring-4 focus:ring-green-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-green-600 dark:hover:bg-green-700 dark:focus:ring-green-800"
>
  + New Folder
</button>
{/if}
</div>
</div>

<!-- Tabs -->
<div class="border-b border-slate-200 dark:border-slate-700">
<ul class="flex flex-wrap -mb-px text-sm font-medium text-center">
{#each [['objects','Objects'],['uploads','Uploads'],['properties','Properties'],['analytics','Analytics'],['tagging','Tags'],['permissions','Permissions'],['lifecycle','Lifecycle'],['cors','CORS']] as [tab, label]}
<li class="me-2">
<button
onclick={() => switchTab(tab as typeof activeDetailTab)}
class={`inline-block p-4 border-b-2 rounded-t-lg transition-colors ${activeDetailTab === tab ? 'text-blue-600 border-blue-600 dark:text-blue-500 dark:border-blue-500' : 'border-transparent hover:text-slate-600 hover:border-slate-300 dark:hover:text-slate-300 text-slate-500 dark:text-slate-400'}`}
>
{label}
</button>
</li>
{/each}
</ul>
</div>

<!-- Tab Content -->
{#if activeDetailTab === 'objects'}
<!-- Objects Tab -->
<div class="flex flex-wrap gap-2 items-center justify-between mb-2">
<div class="flex gap-2 items-center">
<input
type="text"
placeholder="Filter objects..."
bind:value={filterObjects}
class="block p-2 ps-4 text-sm text-slate-900 border border-slate-300 rounded-lg bg-slate-50 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:placeholder-slate-400 dark:text-white w-48"
/>
<span class="text-xs text-slate-500 dark:text-slate-400">
{sortedObjects.length} object{sortedObjects.length !== 1 ? 's' : ''}, {filteredPrefixes.length} folder{filteredPrefixes.length !== 1 ? 's' : ''}{selectedObjects.size > 0 ? ` · ${selectedObjects.size} selected` : ''}
</span>
</div>
{#if selectedObjects.size > 0}
<button
onclick={deleteSelectedObjects}
class="text-white bg-red-600 hover:bg-red-700 font-medium rounded-lg text-sm px-4 py-2"
>
Delete Selected ({selectedObjects.size})
</button>
{/if}
</div>

{#if uploading}
<div class="w-full bg-slate-200 rounded-full h-2 mb-2 dark:bg-slate-700">
<div class="bg-blue-600 h-2 rounded-full animate-pulse" style="width: 60%"></div>
</div>
{/if}

<div
class="p-6 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl relative"
ondragover={(e) => { e.preventDefault(); isDragging = true; }}
ondragleave={() => { isDragging = false; }}
ondrop={handleDrop}
role="region"
aria-label="Object browser"
>
{#if isDragging}
<div class="absolute inset-0 bg-blue-100/80 dark:bg-blue-900/40 border-2 border-dashed border-blue-500 rounded-xl z-10 flex items-center justify-center pointer-events-none">
<p class="text-blue-600 dark:text-blue-400 font-semibold text-lg">Drop files to upload</p>
</div>
{/if}

{#if loadingObjects}
<div class="flex items-center justify-center p-8">
<svg class="w-8 h-8 animate-spin text-slate-200 dark:text-slate-600 fill-blue-600" viewBox="0 0 100 101" fill="none">
<path d="M100 50.5908C100 78.2051 77.6142 100.591 50 100.591C22.3858 100.591 0 78.2051 0 50.5908C0 22.9766 22.3858 0.59082 50 0.59082C77.6142 0.59082 100 22.9766 100 50.5908ZM9.08144 50.5908C9.08144 73.1895 27.4013 91.5094 50 91.5094C72.5987 91.5094 90.9186 73.1895 90.9186 50.5908C90.9186 27.9921 72.5987 9.67226 50 9.67226C27.4013 9.67226 9.08144 27.9921 9.08144 50.5908Z" fill="currentColor" />
<path d="M93.9676 39.0409C96.393 38.4038 97.8624 35.9116 97.0079 33.5539C95.2932 28.8227 92.871 24.3692 89.8167 20.348C85.8452 15.1192 80.8826 10.7238 75.2124 7.41289C69.5422 4.10194 63.2754 1.94025 56.7698 1.05124C51.7666 0.367541 46.6976 0.446843 41.7345 1.27873C39.2613 1.69328 37.813 4.19778 38.4501 6.62326C39.0873 9.04874 41.5694 10.4717 44.0505 10.1071C47.8511 9.54855 51.7191 9.52689 55.5402 10.0491C60.8642 10.7766 65.9928 12.5457 70.6331 15.2552C75.2735 17.9648 79.3347 21.5619 82.5849 25.841C84.9175 28.9121 86.7997 32.2913 88.1811 35.8758C89.083 38.2158 91.5421 39.6781 93.9676 39.0409Z" fill="currentFill" />
</svg>
</div>
{:else if sortedObjects.length === 0 && filteredPrefixes.length === 0}
<div class="text-center py-12 text-slate-500">
<p class="text-lg font-medium">This bucket is empty</p>
<p class="text-sm mt-1">Upload files or drop them here</p>
</div>
{:else}
<table class="w-full text-sm text-left text-slate-500 dark:text-slate-400">
<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
<tr>
  <th class="px-3 py-3 w-8">
    <input type="checkbox"
      checked={selectedObjects.size === sortedObjects.length && sortedObjects.length > 0}
      onchange={(e) => {
        if ((e.currentTarget as HTMLInputElement).checked) {
          selectedObjects = new Set(sortedObjects.map(o => o.Key ?? ''));
        } else {
          selectedObjects = new Set();
        }
      }}
      class="w-4 h-4 text-blue-600 border-slate-300 rounded"
    />
  </th>
  <th class="px-6 py-3 cursor-pointer select-none hover:bg-slate-100 dark:hover:bg-slate-600"
    onclick={() => { if (sortField === 'name') sortOrder = sortOrder === 'asc' ? 'desc' : 'asc'; else { sortField = 'name'; sortOrder = 'asc'; } }}>
    Name {sortField === 'name' ? (sortOrder === 'asc' ? '↑' : '↓') : ''}
  </th>
  <th class="px-6 py-3">Storage Class</th>
  <th class="px-6 py-3 text-right cursor-pointer select-none hover:bg-slate-100 dark:hover:bg-slate-600"
    onclick={() => { if (sortField === 'size') sortOrder = sortOrder === 'asc' ? 'desc' : 'asc'; else { sortField = 'size'; sortOrder = 'asc'; } }}>
    Size {sortField === 'size' ? (sortOrder === 'asc' ? '↑' : '↓') : ''}
  </th>
  <th class="px-6 py-3 text-right cursor-pointer select-none hover:bg-slate-100 dark:hover:bg-slate-600"
    onclick={() => { if (sortField === 'date') sortOrder = sortOrder === 'asc' ? 'desc' : 'asc'; else { sortField = 'date'; sortOrder = 'asc'; } }}>
    Last Modified {sortField === 'date' ? (sortOrder === 'asc' ? '↑' : '↓') : ''}
  </th>
  <th class="px-6 py-3 text-right">Actions</th>
</tr>
</thead>
<tbody>
{#each filteredPrefixes as prefix}
<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700 cursor-pointer" onclick={() => navigatePrefix(prefix)}>
<td class="px-3 py-4"></td>
<td class="px-6 py-4 font-medium text-slate-900 dark:text-white flex items-center gap-2">
<svg class="w-5 h-5 text-yellow-500" fill="currentColor" viewBox="0 0 20 20"><path d="M2 6a2 2 0 012-2h5l2 2h5a2 2 0 012 2v6a2 2 0 01-2 2H4a2 2 0 01-2-2V6z" /></svg>
{prefix.replace(currentPrefix, '')}
</td>
<td class="px-6 py-4">—</td>
<td class="px-6 py-4 text-right">—</td>
<td class="px-6 py-4 text-right">—</td>
<td class="px-6 py-4 text-right">—</td>
</tr>
{/each}
{#each sortedObjects.filter((o) => o.Key !== currentPrefix) as obj}
{@const badge = storageClassBadge(obj.StorageClass)}
<tr class="bg-white border-b dark:bg-slate-800 dark:border-slate-700 hover:bg-slate-50 dark:hover:bg-slate-700">
<td class="px-3 py-4">
<input
type="checkbox"
checked={selectedObjects.has(obj.Key ?? '')}
onchange={() => toggleObjectSelection(obj.Key ?? '')}
class="w-4 h-4 text-blue-600 border-slate-300 rounded"
/>
</td>
<td class="px-6 py-4 font-medium text-slate-900 dark:text-white">
<div class="flex items-center gap-2">
<span class="text-base">{fileIcon(obj.Key ?? '')}</span>
<button
type="button"
onclick={() => inspectObject(obj)}
class="hover:text-blue-600 dark:hover:text-blue-400 hover:underline text-left"
>
{(obj.Key ?? '').replace(currentPrefix, '')}
</button>
</div>
</td>
<td class="px-6 py-4">
<span class={`text-xs font-medium px-2.5 py-0.5 rounded-full ${badge.cls}`}>{badge.label}</span>
</td>
<td class="px-6 py-4 text-right">{formatSize(obj.Size)}</td>
<td class="px-6 py-4 text-right">{formatDate(obj.LastModified)}</td>
<td class="px-6 py-4 text-right">
<div class="flex gap-1 justify-end flex-wrap">
<button
onclick={() => inspectObject(obj)}
class="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 text-xs font-medium px-2 py-1 rounded hover:bg-blue-50 dark:hover:bg-blue-900/20"
>
Inspect
</button>
<button
onclick={() => downloadObject(obj.Key ?? '')}
class="text-green-600 hover:text-green-800 dark:text-green-400 dark:hover:text-green-300 text-xs font-medium px-2 py-1 rounded hover:bg-green-50 dark:hover:bg-green-900/20"
>
Download
</button>
<button
onclick={() => copyObjectUrl(obj.Key ?? '')}
class="text-slate-600 hover:text-slate-800 dark:text-slate-400 dark:hover:text-slate-200 text-xs font-medium px-2 py-1 rounded hover:bg-slate-50 dark:hover:bg-slate-700"
>
Copy URL
</button>
<button
  onclick={() => { copySourceKey = obj.Key ?? ''; copyTargetKey = `${obj.Key ?? ''}-copy`; showCopyModal = true; }}
  class="text-blue-600 hover:text-blue-800 dark:text-blue-400 dark:hover:text-blue-300 text-xs font-medium px-2 py-1 rounded hover:bg-blue-50 dark:hover:bg-blue-900/20"
>
  Copy
</button>
<button
  onclick={() => { renameOldKey = obj.Key ?? ''; renameNewKey = obj.Key ?? ''; showRenameModal = true; }}
  class="text-yellow-600 hover:text-yellow-800 dark:text-yellow-400 dark:hover:text-yellow-300 text-xs font-medium px-2 py-1 rounded hover:bg-yellow-50 dark:hover:bg-yellow-900/20"
>
  Rename
</button>
<button
onclick={() => deleteObject(obj.Key ?? '')}
class="text-red-600 hover:text-red-800 dark:text-red-400 dark:hover:text-red-300 text-xs font-medium px-2 py-1 rounded hover:bg-red-50 dark:hover:bg-red-900/20"
>
Delete
</button>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>

{:else if activeDetailTab === 'properties'}
<!-- Properties Tab -->
<div class="space-y-4">
{#if loadingProperties}
<div class="text-center py-8 text-slate-500">Loading properties...</div>
{:else}
<!-- Versioning -->
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-3">Versioning</h3>
<div class="flex items-center justify-between">
<div>
<p class="text-sm text-slate-600 dark:text-slate-400">Status: <span class="font-medium text-slate-900 dark:text-white">{bucketVersioning || 'Disabled'}</span></p>
</div>
<button
onclick={toggleBucketVersioning}
class={`font-medium rounded-lg text-sm px-4 py-2 transition-colors ${bucketVersioning === 'Enabled' ? 'text-white bg-green-600 hover:bg-green-700' : 'text-white bg-slate-500 hover:bg-slate-600'}`}
>
{bucketVersioning === 'Enabled' ? 'Disable' : 'Enable'} Versioning
</button>
</div>
{#if bucketVersions.length > 0}
<div class="mt-4 space-y-2 max-h-48 overflow-y-auto">
{#each bucketVersions.slice(0, 20) as version}
<div class="p-2 bg-slate-50 dark:bg-slate-700 rounded text-xs">
<span class="font-mono text-slate-700 dark:text-slate-300">{version.Key}</span>
<span class="text-slate-500 ml-2">{version.VersionId?.slice(0, 12)}...</span>
<span class="text-slate-500 ml-2">{formatSize(version.Size)}</span>
</div>
{/each}
</div>
{/if}
</div>

<!-- Encryption -->
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-3">Encryption</h3>
<div class="flex items-center justify-between">
<p class="text-sm text-slate-600 dark:text-slate-400">Algorithm: <span class="font-medium text-slate-900 dark:text-white">{bucketEncryption}</span></p>
<button
onclick={toggleEncryption}
class={`font-medium rounded-lg text-sm px-4 py-2 transition-colors ${bucketEncryption === 'None' ? 'text-white bg-slate-500 hover:bg-slate-600' : 'text-white bg-green-600 hover:bg-green-700'}`}
>
{bucketEncryption === 'None' ? 'Enable AES256' : 'Disable'}
</button>
</div>
</div>

<!-- Location -->
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-3">Location</h3>
<p class="text-sm text-slate-600 dark:text-slate-400">Region: <span class="font-mono font-medium text-slate-900 dark:text-white">{bucketLocation || 'us-east-1'}</span></p>
</div>

<!-- Object Lock -->
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-3">Object Lock</h3>
<p class="text-sm text-slate-600 dark:text-slate-400">Status: <span class={`font-medium ${objectLockStatus === 'Enabled' ? 'text-green-600 dark:text-green-400' : 'text-slate-900 dark:text-white'}`}>{objectLockStatus}</span></p>
</div>

<!-- Static Website Hosting -->
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
  <h3 class="text-base font-semibold text-slate-900 dark:text-white mb-3">Static Website Hosting</h3>
  {#if loadingWebsite}
    <div class="text-sm text-slate-500">Loading...</div>
  {:else if websiteConfig}
    <p class="text-sm text-slate-600 dark:text-slate-400 mb-3">Status: <span class="font-medium text-green-600 dark:text-green-400">Enabled</span></p>
    {#if selectedBucket}
    {@const websiteUrl = `http://${selectedBucket}.s3-website-${getStoredRegion() || 'us-east-1'}.amazonaws.com`}
    <div class="mb-4 p-3 bg-blue-50 dark:bg-blue-900/20 rounded-lg border border-blue-200 dark:border-blue-800">
      <p class="text-xs font-medium text-blue-700 dark:text-blue-300 mb-1">Website Endpoint</p>
      <div class="flex items-center gap-2">
        <a href={websiteUrl} target="_blank" rel="noopener noreferrer" class="text-sm font-mono text-blue-600 dark:text-blue-400 hover:underline break-all flex-1">{websiteUrl}</a>
        <button onclick={() => navigator.clipboard.writeText(websiteUrl).then(() => toast.success('URL copied')).catch(() => {})} class="shrink-0 text-xs text-blue-600 dark:text-blue-400 hover:text-blue-800 border border-blue-300 dark:border-blue-700 rounded px-2 py-0.5">Copy</button>
      </div>
    </div>
    {/if}
    <div class="space-y-2 mb-4">
      <div>
        <label for="website-index-doc" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Index Document</label>
        <input type="text" id="website-index-doc" bind:value={websiteConfig.IndexDocument} placeholder="index.html"
          class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-48" />
      </div>
      <div>
        <label for="website-error-doc" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Error Document</label>
        <input type="text" id="website-error-doc" bind:value={websiteConfig.ErrorDocument} placeholder="error.html"
          class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-48" />
      </div>
    </div>
    <div class="mb-4 p-3 bg-slate-50 dark:bg-slate-900/40 rounded-lg border border-slate-200 dark:border-slate-700">
      <p class="text-xs text-slate-500 dark:text-slate-400 mb-1">Website endpoint</p>
      <div class="flex items-center gap-2">
        <a href={websiteEndpointUrl()} target="_blank" rel="noopener noreferrer" class="text-sm font-mono text-blue-600 dark:text-blue-400 hover:underline break-all">{websiteEndpointUrl()}</a>
        <button onclick={() => { navigator.clipboard.writeText(websiteEndpointUrl()).then(() => toast.success('URL copied')).catch(() => toast.error('Copy failed')); }} class="shrink-0 text-xs text-slate-400 hover:text-blue-500">Copy</button>
      </div>
    </div>
    <div class="flex gap-2">
      <button onclick={saveWebsite} class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-4 py-2">Save</button>
      <button onclick={deleteWebsite} class="text-white bg-red-600 hover:bg-red-700 font-medium rounded-lg text-sm px-4 py-2">Disable</button>
    </div>
  {:else}
    <p class="text-sm text-slate-600 dark:text-slate-400 mb-3">Status: <span class="font-medium text-slate-900 dark:text-white">Disabled</span></p>
    <button
      onclick={() => { websiteConfig = { IndexDocument: 'index.html', ErrorDocument: 'error.html' }; }}
      class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-4 py-2"
    >
      Enable
    </button>
  {/if}
</div>

<!-- Server Access Logging -->
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
  <h3 class="text-base font-semibold text-slate-900 dark:text-white mb-3">Server Access Logging</h3>
  {#if loadingLogging}
    <div class="text-sm text-slate-500">Loading...</div>
  {:else}
    <label class="flex items-center gap-2 text-sm text-slate-700 dark:text-slate-300 mb-3">
      <input type="checkbox" bind:checked={loggingEnabled} />
      Enable access logging
    </label>
    {#if loggingEnabled}
      <div class="space-y-2 mb-4">
        <div>
          <label for="logging-target-bucket" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Target Bucket</label>
          <input type="text" id="logging-target-bucket" bind:value={loggingTargetBucket} placeholder="log-bucket-name"
            class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-64" />
        </div>
        <div>
          <label for="logging-target-prefix" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Target Prefix</label>
          <input type="text" id="logging-target-prefix" bind:value={loggingTargetPrefix} placeholder="logs/"
            class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-64" />
        </div>
      </div>
    {/if}
    <button onclick={saveLogging} disabled={savingLogging} class="text-white bg-blue-600 hover:bg-blue-700 disabled:opacity-50 font-medium rounded-lg text-sm px-4 py-2">{savingLogging ? 'Saving...' : 'Save Logging Config'}</button>
  {/if}
</div>
{/if}
</div>

{:else if activeDetailTab === 'analytics'}
<!-- Storage Analytics Tab -->
<div class="space-y-4">
{#if analyticsLoading}
<div class="text-center py-8 text-slate-500">Computing storage analytics...</div>
{:else}
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<div class="flex items-center justify-between mb-4">
<h3 class="text-base font-semibold text-slate-900 dark:text-white">Storage Analytics</h3>
<button onclick={loadAnalytics} class="text-sm text-blue-600 hover:text-blue-700">Refresh</button>
</div>
<div class="grid grid-cols-2 sm:grid-cols-3 gap-3 mb-4">
<div class="p-3 bg-slate-50 dark:bg-slate-900/40 rounded-lg">
<p class="text-xs text-slate-500 dark:text-slate-400">Total Size</p>
<p class="text-lg font-bold text-slate-900 dark:text-white">{formatBytes(analyticsTotalBytes)}</p>
</div>
<div class="p-3 bg-slate-50 dark:bg-slate-900/40 rounded-lg">
<p class="text-xs text-slate-500 dark:text-slate-400">Object Count</p>
<p class="text-lg font-bold text-slate-900 dark:text-white">{analyticsTotalCount.toLocaleString()}</p>
</div>
<div class="p-3 bg-slate-50 dark:bg-slate-900/40 rounded-lg">
<p class="text-xs text-slate-500 dark:text-slate-400">Top-level Prefixes</p>
<p class="text-lg font-bold text-slate-900 dark:text-white">{analyticsByPrefix.length}</p>
</div>
</div>
{#if analyticsTruncated}
<p class="text-xs text-amber-600 dark:text-amber-400 mb-2">Showing first 10,000 objects; totals are partial for very large buckets.</p>
{/if}
{#if analyticsByPrefix.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No objects in this bucket.</p>
{:else}
<table class="w-full text-sm">
<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
<tr>
<th class="px-4 py-2 text-left">Prefix</th>
<th class="px-4 py-2 text-right">Objects</th>
<th class="px-4 py-2 text-right">Size</th>
<th class="px-4 py-2 text-left">Share</th>
</tr>
</thead>
<tbody>
{#each analyticsByPrefix as ps}
<tr class="border-b dark:border-slate-600">
<td class="px-4 py-2 font-mono text-slate-900 dark:text-white">{ps.prefix}</td>
<td class="px-4 py-2 text-right text-slate-700 dark:text-slate-300">{ps.count.toLocaleString()}</td>
<td class="px-4 py-2 text-right text-slate-700 dark:text-slate-300">{formatBytes(ps.bytes)}</td>
<td class="px-4 py-2">
<div class="w-full bg-slate-100 dark:bg-slate-700 rounded-full h-2">
<div class="bg-blue-500 h-2 rounded-full" style={`width: ${analyticsTotalBytes > 0 ? Math.max(2, Math.round((ps.bytes / analyticsTotalBytes) * 100)) : 0}%`}></div>
</div>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
</div>
{/if}
</div>

{:else if activeDetailTab === 'tagging'}
<!-- Tagging Tab -->
<div class="space-y-4">
{#if loadingTags}
<div class="text-center py-8 text-slate-500">Loading tags...</div>
{:else}
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-4">Bucket Tags</h3>
{#if bucketTags.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400 mb-4">No tags set</p>
{:else}
<table class="w-full text-sm mb-4">
<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
<tr>
<th class="px-4 py-2 text-left">Key</th>
<th class="px-4 py-2 text-left">Value</th>
<th class="px-4 py-2"></th>
</tr>
</thead>
<tbody>
{#each bucketTags as tag}
<tr class="border-b dark:border-slate-600">
<td class="px-4 py-2 font-mono text-slate-900 dark:text-white">{tag.Key}</td>
<td class="px-4 py-2 text-slate-700 dark:text-slate-300">{tag.Value}</td>
<td class="px-4 py-2 text-right">
<button onclick={() => removeTag(tag.Key ?? '')} class="text-red-500 hover:text-red-700 text-xs">Remove</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
<div class="flex gap-2 items-end flex-wrap">
<div>
<label for="tag-key" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Key</label>
<input type="text" id="tag-key" bind:value={newTagKey} placeholder="key" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-32" />
</div>
<div>
<label for="tag-value" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Value</label>
<input type="text" id="tag-value" bind:value={newTagValue} placeholder="value" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-32" />
</div>
<button onclick={addTag} class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-4 py-2">Add</button>
</div>
<div class="flex gap-2 mt-4 pt-4 border-t dark:border-slate-600">
<button onclick={saveTags} class="text-white bg-green-600 hover:bg-green-700 font-medium rounded-lg text-sm px-4 py-2">Save Tags</button>
<button onclick={clearAllTags} class="text-white bg-red-600 hover:bg-red-700 font-medium rounded-lg text-sm px-4 py-2">Clear All Tags</button>
</div>
</div>
{/if}
</div>

{:else if activeDetailTab === 'permissions'}
<!-- Permissions Tab -->
<div class="space-y-4">
{#if loadingPolicy}
<div class="text-center py-8 text-slate-500">Loading policy...</div>
{:else}
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-4">Bucket Policy</h3>
<textarea
bind:value={bucketPolicy}
rows="10"
placeholder={`{"Version":"2012-10-17","Statement":[]}`}
class="w-full font-mono text-xs p-3 border border-slate-300 dark:border-slate-600 rounded-lg bg-slate-50 dark:bg-slate-700 dark:text-white focus:ring-blue-500 focus:border-blue-500"
></textarea>
<div class="flex gap-2 mt-3">
<button onclick={savePolicy} class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-4 py-2">Save Policy</button>
<button onclick={deletePolicy} class="text-white bg-red-600 hover:bg-red-700 font-medium rounded-lg text-sm px-4 py-2">Delete Policy</button>
</div>
</div>
<div class="p-6 bg-amber-50 dark:bg-amber-900/20 border border-amber-200 dark:border-amber-700 rounded-xl">
<h3 class="text-base font-semibold text-amber-800 dark:text-amber-300 mb-2">Public Access Block</h3>
<p class="text-sm text-amber-700 dark:text-amber-400">Public access block settings are not enforced in local development environments. In production, these settings restrict public access to bucket contents regardless of bucket policy.</p>
</div>
{/if}
</div>

{:else if activeDetailTab === 'lifecycle'}
<!-- Lifecycle Tab -->
<div class="space-y-4">
{#if loadingLifecycle}
<div class="text-center py-8 text-slate-500">Loading lifecycle rules...</div>
{:else}
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-4">Lifecycle Rules</h3>
{#if lifecycleRules.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400 mb-4">No lifecycle rules configured</p>
{:else}
<table class="w-full text-sm mb-4">
<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
<tr>
<th class="px-4 py-2 text-left">ID</th>
<th class="px-4 py-2 text-left">Status</th>
<th class="px-4 py-2 text-left">Prefix</th>
<th class="px-4 py-2 text-left">Expiration</th>
<th class="px-4 py-2"></th>
</tr>
</thead>
<tbody>
{#each lifecycleRules as rule}
<tr class="border-b dark:border-slate-600">
<td class="px-4 py-2 font-mono text-slate-900 dark:text-white">{rule.ID}</td>
<td class="px-4 py-2">
<span class={`text-xs px-2 py-0.5 rounded-full ${rule.Status === 'Enabled' ? 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300' : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300'}`}>{rule.Status}</span>
</td>
<td class="px-4 py-2 font-mono text-slate-700 dark:text-slate-300">{rule.Filter?.Prefix ?? ''}</td>
<td class="px-4 py-2 text-slate-700 dark:text-slate-300">{rule.Expiration?.Days ? `${rule.Expiration.Days} days` : '—'}</td>
<td class="px-4 py-2 text-right">
<button onclick={() => deleteLifecycleRule(rule.ID ?? '')} class="text-red-500 hover:text-red-700 text-xs">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
{/if}
<div class="border-t dark:border-slate-600 pt-4">
<h4 class="text-sm font-semibold text-slate-900 dark:text-white mb-3">Add Rule</h4>
<div class="flex gap-2 flex-wrap items-end">
<div>
<label for="lifecycle-rule-id" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Rule ID</label>
<input type="text" id="lifecycle-rule-id" bind:value={newLifecycleId} placeholder="delete-old-logs" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-36" />
</div>
<div>
<label for="lifecycle-prefix" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Prefix</label>
<input type="text" id="lifecycle-prefix" bind:value={newLifecyclePrefix} placeholder="logs/" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-28" />
</div>
<div>
<label for="lifecycle-days" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Expiry (days)</label>
<input type="number" id="lifecycle-days" bind:value={newLifecycleDays} min="1" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-24" />
</div>
<button onclick={addLifecycleRule} class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-4 py-2">Add Rule</button>
</div>
</div>
</div>
{/if}
</div>

{:else if activeDetailTab === 'uploads'}
<!-- Multipart Uploads Tab -->
<div class="space-y-4">
{#if loadingUploads}
<div class="text-center py-8 text-slate-500">Loading multipart uploads...</div>
{:else}
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<div class="flex items-center justify-between mb-4">
<h3 class="text-base font-semibold text-slate-900 dark:text-white">In-Flight Multipart Uploads</h3>
<button onclick={loadMultipartUploads} class="text-sm text-blue-600 hover:text-blue-800 dark:text-blue-400">Refresh</button>
</div>
{#if multipartUploads.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400">No multipart uploads in progress.</p>
{:else}
<div class="overflow-x-auto">
<table class="w-full text-sm">
<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
<tr>
<th class="px-4 py-2 text-left">Key</th>
<th class="px-4 py-2 text-left">Upload ID</th>
<th class="px-4 py-2 text-left">Initiated</th>
<th class="px-4 py-2 text-right">Parts</th>
<th class="px-4 py-2 text-right">Bytes uploaded</th>
<th class="px-4 py-2"></th>
</tr>
</thead>
<tbody>
{#each multipartUploads as u (u.uploadId)}
<tr class="border-b dark:border-slate-600">
<td class="px-4 py-2 font-mono text-xs text-slate-700 dark:text-slate-300 break-all">{u.key}</td>
<td class="px-4 py-2 font-mono text-xs text-slate-500 dark:text-slate-400">{u.uploadId.slice(0, 16)}…</td>
<td class="px-4 py-2 text-xs text-slate-700 dark:text-slate-300">{u.initiated ? formatDate(u.initiated) : '—'}</td>
<td class="px-4 py-2 text-right tabular-nums text-slate-700 dark:text-slate-300">{u.partsCompleted}</td>
<td class="px-4 py-2 text-right tabular-nums text-slate-700 dark:text-slate-300">{formatBytes(u.bytesUploaded)}</td>
<td class="px-4 py-2 text-right">
<button onclick={() => abortMultipartUpload(u.key, u.uploadId)} class="text-red-600 hover:text-red-800 dark:text-red-400 text-xs font-medium px-2 py-1 rounded hover:bg-red-50 dark:hover:bg-red-900/20">Abort</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
<p class="mt-4 text-xs text-slate-500 dark:text-slate-400">Stale uploads are automatically aborted after 24 hours by the janitor. Add an AbortIncompleteMultipartUpload lifecycle rule for a shorter window.</p>
</div>
{/if}
</div>

{:else if activeDetailTab === 'cors'}
<!-- CORS Tab -->
<div class="space-y-4">
{#if loadingCors}
<div class="text-center py-8 text-slate-500">Loading CORS rules...</div>
{:else}
<div class="p-6 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-xl">
<h3 class="text-base font-semibold text-slate-900 dark:text-white mb-4">CORS Rules</h3>
{#if corsRules.length === 0}
<p class="text-sm text-slate-500 dark:text-slate-400 mb-4">No CORS rules configured</p>
{:else}
<div class="overflow-x-auto mb-4">
<table class="w-full text-sm">
<thead class="text-xs text-slate-700 uppercase bg-slate-50 dark:bg-slate-700 dark:text-slate-400">
<tr>
<th class="px-4 py-2 text-left">Allowed Origins</th>
<th class="px-4 py-2 text-left">Methods</th>
<th class="px-4 py-2 text-left">Headers</th>
<th class="px-4 py-2 text-left">Max Age</th>
<th class="px-4 py-2"></th>
</tr>
</thead>
<tbody>
{#each corsRules as rule, idx}
<tr class="border-b dark:border-slate-600">
<td class="px-4 py-2 font-mono text-xs text-slate-700 dark:text-slate-300">{(rule.AllowedOrigins ?? []).join(', ')}</td>
<td class="px-4 py-2">
<div class="flex gap-1 flex-wrap">
{#each (rule.AllowedMethods ?? []) as method}
<span class="text-xs bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 px-1.5 py-0.5 rounded">{method}</span>
{/each}
</div>
</td>
<td class="px-4 py-2 text-xs text-slate-700 dark:text-slate-300">{(rule.AllowedHeaders ?? []).join(', ')}</td>
<td class="px-4 py-2 text-slate-700 dark:text-slate-300">{rule.MaxAgeSeconds ?? '—'}s</td>
<td class="px-4 py-2 text-right">
<button onclick={() => deleteCorsRule(idx)} class="text-red-500 hover:text-red-700 text-xs">Delete</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
<div class="border-t dark:border-slate-600 pt-4">
<h4 class="text-sm font-semibold text-slate-900 dark:text-white mb-3">Add CORS Rule</h4>
<div class="space-y-3">
<div>
<label for="cors-origins" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Allowed Origins (comma separated)</label>
<input type="text" id="cors-origins" bind:value={newCorsOrigins} placeholder="https://example.com, *" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-full" />
</div>
<div>
<p class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Allowed Methods</p>
<div class="flex gap-3 flex-wrap">
{#each ['GET', 'PUT', 'POST', 'DELETE', 'HEAD'] as method}
<label class="flex items-center gap-1.5 text-sm text-slate-700 dark:text-slate-300">
<input
type="checkbox"
checked={newCorsMethods.includes(method)}
onchange={() => toggleCorsMethod(method)}
class="w-4 h-4 text-blue-600"
/>
{method}
</label>
{/each}
</div>
</div>
<div class="flex gap-2 flex-wrap items-end">
<div>
<label for="cors-headers" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Allowed Headers</label>
<input type="text" id="cors-headers" bind:value={newCorsHeaders} placeholder="Content-Type, Authorization" class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-48" />
</div>
<div>
<label for="cors-max-age" class="block text-xs text-slate-600 dark:text-slate-400 mb-1">Max Age (s)</label>
<input type="number" id="cors-max-age" bind:value={newCorsMaxAge} class="border border-slate-300 dark:border-slate-600 rounded-lg p-2 text-sm bg-slate-50 dark:bg-slate-700 dark:text-white w-24" />
</div>
<button onclick={addCorsRule} class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-4 py-2">Add Rule</button>
</div>
</div>
</div>
</div>
{/if}
</div>
{/if}

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
<div class="flex gap-2 flex-wrap">
<button
onclick={loadBuckets}
class="text-slate-700 bg-white hover:bg-slate-100 border border-slate-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-slate-800 dark:text-slate-300 dark:border-slate-600 dark:hover:bg-slate-700"
>
Refresh
</button>
<button
id="purge-all-btn"
onclick={purgeAll}
class="text-white bg-red-700 hover:bg-red-800 focus:ring-4 focus:ring-red-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-red-600 dark:hover:bg-red-700 focus:outline-none dark:focus:ring-red-800"
>
Purge All
</button>
<button
id="create-bucket-btn"
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
<div>
  <label for="bucket-sort-order" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Sort by</label>
  <select id="bucket-sort-order" bind:value={bucketSortOrder}
    class="p-2 text-sm text-slate-900 border border-slate-300 rounded-lg bg-slate-50 focus:ring-blue-500 focus:border-blue-500 dark:bg-slate-700 dark:border-slate-600 dark:text-white">
    <option value="alpha">Alphabetical</option>
    <option value="newest">Newest First</option>
    <option value="largest">Largest First</option>
  </select>
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
<div id="bucket-{bucket.Name}" class="p-5 bg-white/80 dark:bg-slate-800/80 backdrop-blur-md border border-slate-200 dark:border-slate-700 shadow-sm rounded-xl hover:shadow-md transition-shadow cursor-pointer group">
<div class="flex justify-between items-start">
<button onclick={() => openBucket(bucket.Name ?? '')} class="flex-1 text-left">
<h3 class="text-base font-semibold text-slate-900 dark:text-white group-hover:text-blue-600 dark:group-hover:text-blue-400 transition-colors">
{bucket.Name}
</h3>
<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">
Created: {formatDate(bucket.CreationDate)}
</p>
<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">
  Size: {formatSize(bucketSizes.get(bucket.Name ?? '') ?? 0)}
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
<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showCreateModal = false; }}></button>
<div class="relative p-4 w-full max-w-md z-10" role="dialog" aria-modal="true" tabindex="-1">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Bucket</h3>
<button onclick={() => { showCreateModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
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
<button id="confirm-create-bucket-btn" type="submit" disabled={creating}
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

<!-- Create Folder Modal -->
{#if showCreateFolderModal}
<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showCreateFolderModal = false; }}></button>
  <div class="relative p-4 w-full max-w-md z-10" role="dialog" aria-modal="true" tabindex="-1">
    <div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
      <div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
        <h3 class="text-xl font-semibold text-slate-900 dark:text-white">New Folder</h3>
        <button onclick={() => { showCreateFolderModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
          <svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
        </button>
      </div>
      <div class="p-4 md:p-5">
        <form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createFolder(); }}>
          <div>
            <label for="folderName" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Folder Name</label>
            <input type="text" id="folderName" bind:value={newFolderName} placeholder="my-folder" required
              class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
            {#if currentPrefix}
            <p class="mt-1 text-xs text-slate-500 dark:text-slate-400">Will be created under: {currentPrefix}</p>
            {/if}
          </div>
          <div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
            <button type="button" onclick={() => { showCreateFolderModal = false; }}
              class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 focus:ring-4 focus:ring-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700 dark:focus:ring-slate-700">
              Cancel
            </button>
            <button type="submit"
              class="text-white bg-green-600 hover:bg-green-700 focus:ring-4 focus:ring-green-300 font-medium rounded-lg text-sm px-5 py-2.5 dark:bg-green-600 dark:hover:bg-green-700 dark:focus:ring-green-800">
              Create Folder
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</div>
{/if}

<!-- Copy Object Modal -->
{#if showCopyModal}
<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showCopyModal = false; }}></button>
  <div class="relative p-4 w-full max-w-md z-10" role="dialog" aria-modal="true" tabindex="-1">
    <div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
      <div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
        <h3 class="text-xl font-semibold text-slate-900 dark:text-white">Copy Object</h3>
        <button onclick={() => { showCopyModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
          <svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
        </button>
      </div>
      <div class="p-4 md:p-5">
        <form class="space-y-4" onsubmit={(e) => { e.preventDefault(); copyObject(); }}>
          <div>
            <p class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Source Key</p>
            <p class="text-sm font-mono text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-800 rounded p-2">{copySourceKey}</p>
          </div>
          <div>
            <label for="copyTargetKey" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Target Key</label>
            <input type="text" id="copyTargetKey" bind:value={copyTargetKey} required
              class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
            <p class="mt-1 text-xs text-slate-500 dark:text-slate-400">The source object stays in place.</p>
          </div>
          <div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
            <button type="button" onclick={() => { showCopyModal = false; }}
              class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700">
              Cancel
            </button>
            <button type="submit"
              class="text-white bg-blue-600 hover:bg-blue-700 font-medium rounded-lg text-sm px-5 py-2.5">
              Copy
            </button>
          </div>
        </form>
      </div>
    </div>
  </div>
</div>
{/if}

<!-- Rename Object Modal -->
{#if showRenameModal}
<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showRenameModal = false; }}></button>
  <div class="relative p-4 w-full max-w-md z-10" role="dialog" aria-modal="true" tabindex="-1">
    <div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
      <div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
        <h3 class="text-xl font-semibold text-slate-900 dark:text-white">Rename Object</h3>
        <button onclick={() => { showRenameModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
          <svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg>
        </button>
      </div>
      <div class="p-4 md:p-5">
        <form class="space-y-4" onsubmit={(e) => { e.preventDefault(); renameObject(); }}>
          <div>
            <p class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Current Key</p>
            <p class="text-sm font-mono text-slate-700 dark:text-slate-300 bg-slate-50 dark:bg-slate-800 rounded p-2">{renameOldKey}</p>
          </div>
          <div>
            <label for="renameNewKey" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">New Key</label>
            <input type="text" id="renameNewKey" bind:value={renameNewKey} required
              class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-blue-500 focus:border-blue-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
          </div>
          <div class="flex justify-end gap-2 pt-4 border-t dark:border-slate-600">
            <button type="button" onclick={() => { showRenameModal = false; }}
              class="py-2.5 px-5 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 hover:text-blue-700 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:text-white dark:hover:bg-slate-700">
              Cancel
            </button>
            <button type="submit"
              class="text-white bg-yellow-600 hover:bg-yellow-700 font-medium rounded-lg text-sm px-5 py-2.5">
              Rename
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
<div class="fixed inset-0 z-50 flex items-center justify-center" role="presentation"><button type="button" class="absolute inset-0 bg-black/50 backdrop-blur-sm" aria-label="Close" onclick={() => { showUploadModal = false; }}></button>
<div class="relative p-4 w-full max-w-2xl z-10" role="dialog" aria-modal="true" tabindex="-1">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 md:p-5 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Upload File</h3>
<button onclick={() => { showUploadModal = false; }} aria-label="Close" class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white">
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
