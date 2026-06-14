<script lang="ts">
import { onMount } from 'svelte';
import { getKMSClient } from '$lib/aws-client';
import {
	ListKeysCommand, DescribeKeyCommand, DisableKeyCommand, EnableKeyCommand,
	ListAliasesCommand, CreateKeyCommand, EncryptCommand, DecryptCommand, ReEncryptCommand,
	SignCommand, VerifyCommand, GetPublicKeyCommand,
	EnableKeyRotationCommand, DisableKeyRotationCommand, GetKeyRotationStatusCommand, RotateKeyOnDemandCommand, ListKeyRotationsCommand,
	ScheduleKeyDeletionCommand, CancelKeyDeletionCommand,
	CreateAliasCommand, UpdateAliasCommand, DeleteAliasCommand,
	TagResourceCommand, UntagResourceCommand, ListResourceTagsCommand,
	GetKeyPolicyCommand, PutKeyPolicyCommand,
	CreateGrantCommand, ListGrantsCommand, RevokeGrantCommand,
	type AliasListEntry, type KeySpec, type KeyUsageType, type SigningAlgorithmSpec, type GrantOperation
} from '@aws-sdk/client-kms';
import { toast } from 'svelte-sonner';
import { Key, RefreshCw, Search, Lock, Unlock, Copy, Tag, Plus, BarChart3, BookOpen, Shield, RotateCcw, Trash2, CheckCircle, XCircle, Edit2, Database } from 'lucide-svelte';

const kms = getKMSClient();

type KMSKey = {
	CreationDate?: Date | string;
	DeletionDate?: Date | string;
	Description?: string;
	KeyArn?: string;
	KeyId?: string;
	KeyManager?: string;
	KeyState?: string;
	KeyUsage?: string;
	CustomerMasterKeySpec?: string;
	KeySpec?: string;
	Origin?: string;
	MultiRegion?: boolean;
	Enabled?: boolean;
	PendingWindowInDays?: number;
};

type TagEntry = { TagKey: string; TagValue: string };
type Grant = { GrantId: string; GranteePrincipal: string; Operations: GrantOperation[]; Name?: string };

let keys = $state<KMSKey[]>([]);
let aliases = $state<AliasListEntry[]>([]);
let loading = $state(true);
let search = $state('');
let stateFilter = $state('all');
let usageFilter = $state('all');
let selectedKey = $state<KMSKey | null>(null);
let activeTab = $state<'keys' | 'aliases' | 'grants' | 'metrics' | 'docs'>('keys');
let aliasSearch = $state('');
let showCreateModal = $state(false);
let creatingKey = $state(false);
let newKeyDescription = $state('');
let newKeySpec = $state('SYMMETRIC_DEFAULT');
let newKeyUsage = $state('ENCRYPT_DECRYPT');
let newKeyMultiRegion = $state(false);

let showCryptoModal = $state(false);
let cryptoKeyId = $state('');
let plaintext = $state('');
let ciphertext = $state('');
let decryptedText = $state('');
let encryptAlgorithm = $state('');
let decryptAlgorithm = $state('');
let encrypting = $state(false);
let decrypting = $state(false);

// ReEncrypt state
let reEncryptDestKeyId = $state('');
let reEncryptedCiphertext = $state('');
let reEncryptSrcAlgo = $state('');
let reEncryptDstAlgo = $state('');
let reEncrypting = $state(false);

let showSignModal = $state(false);
let signKeyId = $state('');
let signMessage = $state('');
let signAlgorithm = $state('RSASSA_PSS_SHA_256');
let signMessageType = $state<'RAW' | 'DIGEST'>('RAW');
let signature = $state('');
let verifyMessage = $state('');
let verifySig = $state('');
let verifyResult = $state<boolean | null>(null);
let signing = $state(false);
let verifying = $state(false);

let showRotationModal = $state(false);
let rotationKeyId = $state('');
let rotationEnabled = $state(false);
let rotationPeriodDays = $state(365);
let rotationStatus = $state<{ KeyRotationEnabled?: boolean; RotationPeriodInDays?: number; NextRotationDate?: number; OnDemandRotationStartDate?: number } | null>(null);
let rotationHistory = $state<{ RotationDate?: number; RotationType?: string; KeyId?: string }[]>([]);
let togglingRotation = $state(false);
let rotatingOnDemand = $state(false);

let showDeletionModal = $state(false);
let deletionKeyId = $state('');
let deletionKeyState = $state('');
let pendingWindowDays = $state(30);
let schedulingDeletion = $state(false);

let showAliasCreateModal = $state(false);
let newAliasName = $state('');
let newAliasKeyId = $state('');
let creatingAlias = $state(false);
let showAliasUpdateModal = $state(false);
let editAliasName = $state('');
let editAliasTargetKeyId = $state('');
let updatingAlias = $state(false);

let showTagModal = $state(false);
let tagKeyId = $state('');
let keyTags = $state<TagEntry[]>([]);
let newTagKey = $state('');
let newTagValue = $state('');
let savingTags = $state(false);

let showPolicyModal = $state(false);
let policyKeyId = $state('');
let policyContent = $state('');
let savingPolicy = $state(false);

let showGrantModal = $state(false);
let grantKeyId = $state('');
let keyGrants = $state<Grant[]>([]);
let newGrantPrincipal = $state('');
let newGrantOps = $state('Decrypt,Encrypt');
let creatingGrant = $state(false);

// Grants tab state
const ALL_GRANT_OPERATIONS: GrantOperation[] = [
	'Decrypt', 'Encrypt', 'GenerateDataKey', 'GenerateDataKeyWithoutPlaintext',
	'ReEncryptFrom', 'ReEncryptTo', 'Sign', 'Verify', 'GetPublicKey',
	'CreateGrant', 'RetireGrant', 'DescribeKey', 'GenerateDataKeyPair',
	'GenerateDataKeyPairWithoutPlaintext', 'GenerateMac', 'VerifyMac', 'DeriveSharedSecret'
];
let grantsTabKeyId = $state('');
let grantsTabGrants = $state<Grant[]>([]);
let grantsTabLoading = $state(false);
let grantsTabPrincipal = $state('');
let grantsTabSelectedOps = $state<Set<GrantOperation>>(new Set(['Decrypt', 'Encrypt']));
let grantsTabName = $state('');
let grantsTabCreating = $state(false);
// Policy viewer/editor in grants tab
let policyTabContent = $state('');
let policyTabLoading = $state(false);
let policyTabSaving = $state(false);

let originFilter = $state('all');
let seedingDemo = $state(false);

onMount(async () => { await loadKeys(); });

async function loadKeys() {
	try {
		loading = true;
		const listData = await kms.send(new ListKeysCommand({}));
		const rawKeys = listData.Keys || [];
		const details = await Promise.all(
			rawKeys.slice(0, 50).map(async (k) => {
				try {
					const d = await kms.send(new DescribeKeyCommand({ KeyId: k.KeyId }));
					return d.KeyMetadata;
				} catch {
					return { KeyId: k.KeyId, KeyArn: k.KeyArn, Description: '', KeyState: 'Unknown', KeyUsage: 'Unknown' };
				}
			})
		);
		keys = details.filter(Boolean) as KMSKey[];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load keys');
	} finally {
		loading = false;
	}
}

async function loadAliases() {
	try {
		loading = true;
		const data = await kms.send(new ListAliasesCommand({ Limit: 100 }));
		aliases = data.Aliases || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load aliases');
	} finally {
		loading = false;
	}
}

async function selectTab(t: typeof activeTab) {
	activeTab = t;
	if (t === 'keys' && keys.length === 0) await loadKeys();
	else if (t === 'aliases') await loadAliases();
	else if (t === 'grants') {
		if (keys.length === 0) await loadKeys();
		if (grantsTabKeyId && keys.length > 0) await loadGrantsForTab();
	}
}

async function refresh() {
	if (activeTab === 'keys') { keys = []; await loadKeys(); }
	else if (activeTab === 'aliases') { aliases = []; await loadAliases(); }
	else if (activeTab === 'grants' && grantsTabKeyId) await loadGrantsForTab();
}

async function loadGrantsForTab() {
	if (!grantsTabKeyId) return;
	grantsTabLoading = true;
	try {
		const res = await kms.send(new ListGrantsCommand({ KeyId: grantsTabKeyId }));
		grantsTabGrants = (res.Grants ?? []) as Grant[];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load grants');
	} finally {
		grantsTabLoading = false;
	}
}

async function loadPolicyForTab() {
	if (!grantsTabKeyId) return;
	policyTabLoading = true;
	try {
		const res = await kms.send(new GetKeyPolicyCommand({ KeyId: grantsTabKeyId, PolicyName: 'default' }));
		policyTabContent = res.Policy ? JSON.stringify(JSON.parse(res.Policy), null, 2) : '{}';
	} catch (e) {
		policyTabContent = '{}';
		toast.error(e instanceof Error ? e.message : 'Failed to load policy');
	} finally {
		policyTabLoading = false;
	}
}

async function savePolicyForTab() {
	if (!grantsTabKeyId) return;
	policyTabSaving = true;
	try {
		JSON.parse(policyTabContent);
	} catch {
		toast.error('Policy is not valid JSON');
		policyTabSaving = false;
		return;
	}
	try {
		await kms.send(new PutKeyPolicyCommand({ KeyId: grantsTabKeyId, PolicyName: 'default', Policy: policyTabContent }));
		toast.success('Key policy saved');
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to save policy');
	} finally {
		policyTabSaving = false;
	}
}

async function createGrantInTab() {
	if (!grantsTabKeyId || !grantsTabPrincipal.trim() || grantsTabSelectedOps.size === 0) {
		toast.error('Key, grantee principal, and at least one operation are required');
		return;
	}
	grantsTabCreating = true;
	try {
		await kms.send(new CreateGrantCommand({
			KeyId: grantsTabKeyId,
			GranteePrincipal: grantsTabPrincipal.trim(),
			Operations: [...grantsTabSelectedOps],
			Name: grantsTabName.trim() || undefined
		}));
		toast.success('Grant created');
		grantsTabPrincipal = '';
		grantsTabName = '';
		grantsTabSelectedOps = new Set(['Decrypt', 'Encrypt']);
		await loadGrantsForTab();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to create grant');
	} finally {
		grantsTabCreating = false;
	}
}

async function revokeGrantInTab(grantId: string) {
	if (!grantsTabKeyId) return;
	try {
		await kms.send(new RevokeGrantCommand({ KeyId: grantsTabKeyId, GrantId: grantId }));
		toast.success('Grant revoked');
		grantsTabGrants = grantsTabGrants.filter(g => g.GrantId !== grantId);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to revoke grant');
	}
}

async function copyId(id: string) {
	await navigator.clipboard.writeText(id);
	toast.success('Copied to clipboard');
}

async function disableKey(keyId: string) {
	try {
		await kms.send(new DisableKeyCommand({ KeyId: keyId }));
		toast.success(`Key ${keyId} disabled`);
		await loadKeys();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to disable key');
	}
}

async function enableKey(keyId: string) {
	try {
		await kms.send(new EnableKeyCommand({ KeyId: keyId }));
		toast.success(`Key ${keyId} enabled`);
		await loadKeys();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to enable key');
	}
}

async function createKey() {
	try {
		creatingKey = true;
		await kms.send(new CreateKeyCommand({
			Description: newKeyDescription,
			KeySpec: newKeySpec as KeySpec,
			KeyUsage: newKeyUsage as KeyUsageType,
			MultiRegion: newKeyMultiRegion || undefined
		}));
		toast.success('Key created');
		showCreateModal = false;
		newKeyDescription = '';
		newKeySpec = 'SYMMETRIC_DEFAULT';
		newKeyUsage = 'ENCRYPT_DECRYPT';
		newKeyMultiRegion = false;
		await loadKeys();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to create key');
	} finally {
		creatingKey = false;
	}
}

async function encrypt() {
	if (!plaintext) return;
	try {
		encrypting = true;
		const res = await kms.send(new EncryptCommand({ KeyId: cryptoKeyId, Plaintext: new TextEncoder().encode(plaintext) }));
		if (res.CiphertextBlob) {
			ciphertext = btoa(String.fromCodePoint(...res.CiphertextBlob));
		}
		encryptAlgorithm = res.EncryptionAlgorithm ?? '';
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Encryption failed');
	} finally {
		encrypting = false;
	}
}

async function decrypt() {
	if (!ciphertext) return;
	try {
		decrypting = true;
		const res = await kms.send(new DecryptCommand({ CiphertextBlob: Uint8Array.from(atob(ciphertext), c => c.codePointAt(0)!) }));
		if (res.Plaintext) {
			decryptedText = new TextDecoder().decode(res.Plaintext);
		}
		decryptAlgorithm = res.EncryptionAlgorithm ?? '';
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Decryption failed');
	} finally {
		decrypting = false;
	}
}

function openCrypto(keyId: string) {
	cryptoKeyId = keyId;
	plaintext = '';
	ciphertext = '';
	decryptedText = '';
	encryptAlgorithm = '';
	decryptAlgorithm = '';
	reEncryptDestKeyId = '';
	reEncryptedCiphertext = '';
	reEncryptSrcAlgo = '';
	reEncryptDstAlgo = '';
	showCryptoModal = true;
}

async function reEncrypt() {
	if (!ciphertext || !reEncryptDestKeyId) return;
	try {
		reEncrypting = true;
		const res = await kms.send(new ReEncryptCommand({
			CiphertextBlob: Uint8Array.from(atob(ciphertext), c => c.codePointAt(0)!),
			DestinationKeyId: reEncryptDestKeyId
		}));
		if (res.CiphertextBlob) {
			reEncryptedCiphertext = btoa(String.fromCodePoint(...res.CiphertextBlob));
		}
		reEncryptSrcAlgo = res.SourceEncryptionAlgorithm ?? '';
		reEncryptDstAlgo = res.DestinationEncryptionAlgorithm ?? '';
		toast.success('Re-encrypted successfully');
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Re-encryption failed');
	} finally {
		reEncrypting = false;
	}
}

async function openSignModal(key: KMSKey) {
	signKeyId = key.KeyId ?? '';
	signMessage = '';
	signature = '';
	verifyMessage = '';
	verifySig = '';
	verifyResult = null;
	try {
		const pub = await kms.send(new GetPublicKeyCommand({ KeyId: signKeyId }));
		if (pub.SigningAlgorithms && pub.SigningAlgorithms.length > 0) {
			signAlgorithm = pub.SigningAlgorithms[0];
		}
	} catch { signAlgorithm = 'RSASSA_PSS_SHA_256'; }
	showSignModal = true;
}

async function signData() {
	if (!signMessage) return;
	try {
		signing = true;
		const res = await kms.send(new SignCommand({
			KeyId: signKeyId,
			Message: new TextEncoder().encode(signMessage),
			MessageType: signMessageType,
			SigningAlgorithm: signAlgorithm as SigningAlgorithmSpec
		}));
		if (res.Signature) signature = btoa(String.fromCodePoint(...res.Signature));
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Sign failed');
	} finally { signing = false; }
}

async function verifyData() {
	if (!verifyMessage || !verifySig) return;
	try {
		verifying = true;
		const res = await kms.send(new VerifyCommand({
			KeyId: signKeyId,
			Message: new TextEncoder().encode(verifyMessage),
			MessageType: signMessageType,
			SigningAlgorithm: signAlgorithm as SigningAlgorithmSpec,
			Signature: Uint8Array.from(atob(verifySig), c => c.codePointAt(0)!)
		}));
		verifyResult = res.SignatureValid ?? false;
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Verify failed');
	} finally { verifying = false; }
}

async function openRotationModal(key: KMSKey) {
	rotationKeyId = key.KeyId ?? '';
	rotationStatus = null;
	rotationHistory = [];
	try {
		const res = await kms.send(new GetKeyRotationStatusCommand({ KeyId: rotationKeyId }));
		rotationEnabled = res.KeyRotationEnabled ?? false;
		rotationPeriodDays = res.RotationPeriodInDays ?? 365;
		rotationStatus = {
			KeyRotationEnabled: res.KeyRotationEnabled,
			RotationPeriodInDays: res.RotationPeriodInDays,
			NextRotationDate: res.NextRotationDate ? res.NextRotationDate.getTime() / 1000 : undefined,
			OnDemandRotationStartDate: (res as { OnDemandRotationStartDate?: Date }).OnDemandRotationStartDate
				? ((res as { OnDemandRotationStartDate?: Date }).OnDemandRotationStartDate!.getTime() / 1000)
				: undefined
		};
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load rotation status');
	}
	try {
		const hist = await kms.send(new ListKeyRotationsCommand({ KeyId: rotationKeyId }));
		rotationHistory = (hist.Rotations ?? []).map(r => ({
			RotationDate: r.RotationDate ? r.RotationDate.getTime() / 1000 : undefined,
			RotationType: (r as { RotationType?: string }).RotationType,
			KeyId: (r as { KeyId?: string }).KeyId
		}));
	} catch { rotationHistory = []; }
	showRotationModal = true;
}

async function toggleRotation() {
	try {
		togglingRotation = true;
		if (rotationEnabled) {
			await kms.send(new DisableKeyRotationCommand({ KeyId: rotationKeyId }));
			rotationEnabled = false;
			toast.success('Rotation disabled');
		} else {
			await kms.send(new EnableKeyRotationCommand({ KeyId: rotationKeyId, RotationPeriodInDays: rotationPeriodDays }));
			rotationEnabled = true;
			toast.success('Rotation enabled');
		}
		const s = await kms.send(new GetKeyRotationStatusCommand({ KeyId: rotationKeyId }));
		rotationStatus = {
			KeyRotationEnabled: s.KeyRotationEnabled,
			RotationPeriodInDays: s.RotationPeriodInDays,
			NextRotationDate: s.NextRotationDate ? s.NextRotationDate.getTime() / 1000 : undefined,
			OnDemandRotationStartDate: (s as { OnDemandRotationStartDate?: Date }).OnDemandRotationStartDate
				? ((s as { OnDemandRotationStartDate?: Date }).OnDemandRotationStartDate!.getTime() / 1000)
				: undefined
		};
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to toggle rotation');
	} finally { togglingRotation = false; }
}

async function rotateOnDemand() {
	try {
		rotatingOnDemand = true;
		await kms.send(new RotateKeyOnDemandCommand({ KeyId: rotationKeyId }));
		toast.success('Key material rotated on demand');
		const s = await kms.send(new GetKeyRotationStatusCommand({ KeyId: rotationKeyId }));
		rotationStatus = {
			KeyRotationEnabled: s.KeyRotationEnabled,
			RotationPeriodInDays: s.RotationPeriodInDays,
			NextRotationDate: s.NextRotationDate ? s.NextRotationDate.getTime() / 1000 : undefined,
			OnDemandRotationStartDate: (s as { OnDemandRotationStartDate?: Date }).OnDemandRotationStartDate
				? ((s as { OnDemandRotationStartDate?: Date }).OnDemandRotationStartDate!.getTime() / 1000)
				: undefined
		};
		const hist = await kms.send(new ListKeyRotationsCommand({ KeyId: rotationKeyId }));
		rotationHistory = (hist.Rotations ?? []).map(r => ({
			RotationDate: r.RotationDate ? r.RotationDate.getTime() / 1000 : undefined,
			RotationType: (r as { RotationType?: string }).RotationType,
			KeyId: (r as { KeyId?: string }).KeyId
		}));
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'On-demand rotation failed');
	} finally { rotatingOnDemand = false; }
}

function openDeletionModal(key: KMSKey) {
	deletionKeyId = key.KeyId ?? '';
	deletionKeyState = key.KeyState ?? '';
	pendingWindowDays = 30;
	showDeletionModal = true;
}

async function scheduleOrCancelDeletion() {
	try {
		schedulingDeletion = true;
		if (deletionKeyState === 'PendingDeletion') {
			const res = await kms.send(new CancelKeyDeletionCommand({ KeyId: deletionKeyId }));
			// Update the key state from the response if available.
			const newState = (res as { KeyState?: string }).KeyState;
			if (newState) { deletionKeyState = newState; }
			toast.success('Key deletion cancelled');
		} else {
			await kms.send(new ScheduleKeyDeletionCommand({ KeyId: deletionKeyId, PendingWindowInDays: pendingWindowDays }));
			toast.success(`Key scheduled for deletion in ${pendingWindowDays} days`);
		}
		showDeletionModal = false;
		await loadKeys();
		if (selectedKey?.KeyId === deletionKeyId) selectedKey = null;
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Deletion operation failed');
	} finally { schedulingDeletion = false; }
}

async function seedDemoData() {
	try {
		seedingDemo = true;
		const demoKeys: Array<{ Description: string; KeySpec: KeySpec; KeyUsage: KeyUsageType }> = [
			{ Description: 'Demo: S3 Encryption Key', KeySpec: 'SYMMETRIC_DEFAULT', KeyUsage: 'ENCRYPT_DECRYPT' },
			{ Description: 'Demo: RDS Database Key', KeySpec: 'SYMMETRIC_DEFAULT', KeyUsage: 'ENCRYPT_DECRYPT' },
			{ Description: 'Demo: Code Signing Key', KeySpec: 'RSA_2048', KeyUsage: 'SIGN_VERIFY' },
			{ Description: 'Demo: HMAC Auth Key', KeySpec: 'HMAC_256', KeyUsage: 'GENERATE_VERIFY_MAC' },
			{ Description: 'Demo: ECC Sign Key', KeySpec: 'ECC_NIST_P256', KeyUsage: 'SIGN_VERIFY' },
		];
		for (const k of demoKeys) {
			await kms.send(new CreateKeyCommand(k));
		}
		toast.success(`Seeded ${demoKeys.length} demo KMS keys`);
		await loadKeys();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Demo seeding failed');
	} finally { seedingDemo = false; }
}

async function createAlias() {
	if (!newAliasName || !newAliasKeyId) return;
	try {
		creatingAlias = true;
		await kms.send(new CreateAliasCommand({ AliasName: newAliasName, TargetKeyId: newAliasKeyId }));
		toast.success(`Alias created`);
		showAliasCreateModal = false;
		newAliasName = '';
		newAliasKeyId = '';
		await loadAliases();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to create alias');
	} finally { creatingAlias = false; }
}

function openUpdateAlias(alias: AliasListEntry) {
	editAliasName = alias.AliasName ?? '';
	editAliasTargetKeyId = alias.TargetKeyId ?? '';
	showAliasUpdateModal = true;
}

async function updateAlias() {
	if (!editAliasName || !editAliasTargetKeyId) return;
	try {
		updatingAlias = true;
		await kms.send(new UpdateAliasCommand({ AliasName: editAliasName, TargetKeyId: editAliasTargetKeyId }));
		toast.success('Alias updated');
		showAliasUpdateModal = false;
		await loadAliases();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to update alias');
	} finally { updatingAlias = false; }
}

async function deleteAlias(aliasName: string) {
	try {
		await kms.send(new DeleteAliasCommand({ AliasName: aliasName }));
		toast.success('Alias deleted');
		await loadAliases();
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to delete alias');
	}
}

async function openTagModal(key: KMSKey) {
	tagKeyId = key.KeyId ?? '';
	keyTags = [];
	newTagKey = '';
	newTagValue = '';
	try {
		const res = await kms.send(new ListResourceTagsCommand({ KeyId: tagKeyId }));
		keyTags = (res.Tags ?? []).map(t => ({ TagKey: t.TagKey ?? '', TagValue: t.TagValue ?? '' }));
	} catch { keyTags = []; }
	showTagModal = true;
}

async function addTag() {
	if (!newTagKey) return;
	try {
		savingTags = true;
		await kms.send(new TagResourceCommand({ KeyId: tagKeyId, Tags: [{ TagKey: newTagKey, TagValue: newTagValue }] }));
		keyTags = [...keyTags, { TagKey: newTagKey, TagValue: newTagValue }];
		newTagKey = '';
		newTagValue = '';
		toast.success('Tag added');
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to add tag');
	} finally { savingTags = false; }
}

async function removeTag(tagKey: string) {
	try {
		await kms.send(new UntagResourceCommand({ KeyId: tagKeyId, TagKeys: [tagKey] }));
		keyTags = keyTags.filter(t => t.TagKey !== tagKey);
		toast.success('Tag removed');
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to remove tag');
	}
}

async function openPolicyModal(key: KMSKey) {
	policyKeyId = key.KeyId ?? '';
	policyContent = '';
	try {
		const res = await kms.send(new GetKeyPolicyCommand({ KeyId: policyKeyId, PolicyName: 'default' }));
		policyContent = res.Policy ? JSON.stringify(JSON.parse(res.Policy), null, 2) : '{}';
	} catch { policyContent = '{}'; }
	showPolicyModal = true;
}

async function savePolicy() {
	try {
		savingPolicy = true;
		await kms.send(new PutKeyPolicyCommand({ KeyId: policyKeyId, PolicyName: 'default', Policy: policyContent }));
		toast.success('Key policy updated');
		showPolicyModal = false;
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to save policy');
	} finally { savingPolicy = false; }
}

async function openGrantModal(key: KMSKey) {
	grantKeyId = key.KeyId ?? '';
	keyGrants = [];
	newGrantPrincipal = '';
	newGrantOps = 'Decrypt,Encrypt';
	try {
		const res = await kms.send(new ListGrantsCommand({ KeyId: grantKeyId }));
		keyGrants = (res.Grants ?? []) as Grant[];
	} catch { keyGrants = []; }
	showGrantModal = true;
}

async function createGrant() {
	if (!newGrantPrincipal) return;
	try {
		creatingGrant = true;
		const ops = newGrantOps.split(',').map((s: string) => s.trim()).filter((s): s is GrantOperation => s.length > 0);
		await kms.send(new CreateGrantCommand({ KeyId: grantKeyId, GranteePrincipal: newGrantPrincipal, Operations: ops }));
		toast.success('Grant created');
		const res = await kms.send(new ListGrantsCommand({ KeyId: grantKeyId }));
		keyGrants = (res.Grants ?? []) as Grant[];
		newGrantPrincipal = '';
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to create grant');
	} finally { creatingGrant = false; }
}

async function revokeGrant(grantId: string) {
	try {
		await kms.send(new RevokeGrantCommand({ KeyId: grantKeyId, GrantId: grantId }));
		toast.success('Grant revoked');
		keyGrants = keyGrants.filter(g => g.GrantId !== grantId);
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to revoke grant');
	}
}

function getStateColor(state: string) {
	if (state === 'Enabled') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
	if (state === 'Disabled') return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	if (state === 'PendingDeletion') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
	return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
}

function getKeySpec(key: KMSKey) {
	return key.CustomerMasterKeySpec || key.KeySpec || 'SYMMETRIC_DEFAULT';
}

function getKeyManager(key: KMSKey) {
	return key.KeyManager || 'CUSTOMER';
}

function isSymmetric(key: KMSKey) { return (key.CustomerMasterKeySpec || key.KeySpec || '').includes('SYMMETRIC'); }
function isSignVerify(key: KMSKey) { return key.KeyUsage === 'SIGN_VERIFY'; }

let enabledCount = $derived(keys.filter(k => k.KeyState === 'Enabled').length);
let disabledCount = $derived(keys.filter(k => k.KeyState === 'Disabled').length);
let pendingCount = $derived(keys.filter(k => k.KeyState === 'PendingDeletion').length);
let pendingImportCount = $derived(keys.filter(k => k.KeyState === 'PendingImport').length);
let symmetricCount = $derived(keys.filter(k => isSymmetric(k)).length);
let asymmetricCount = $derived(keys.filter(k => !isSymmetric(k) && !isHMAC(k)).length);
let hmacCount = $derived(keys.filter(k => isHMAC(k)).length);
let multiRegionCount = $derived(keys.filter(k => k.MultiRegion).length);

function isHMAC(key: KMSKey) { return (key.CustomerMasterKeySpec || key.KeySpec || '').startsWith('HMAC'); }
let uniqueUsages = $derived([...new Set(keys.map(k => k.KeyUsage).filter((u): u is string => u !== null))]);
let uniqueOrigins = $derived([...new Set(keys.map(k => k.Origin ?? 'AWS_KMS').filter(Boolean))]);
let uniqueSpecs = $derived([...new Set(keys.map(k => getKeySpec(k)).filter(Boolean))]);

let filtered = $derived(keys.filter(k => {
	const desc = (k.Description || '').toLowerCase();
	const id = (k.KeyId || '').toLowerCase();
	const matchSearch = !search || desc.includes(search.toLowerCase()) || id.includes(search.toLowerCase());
	const matchState = stateFilter === 'all' || k.KeyState === stateFilter;
	const matchUsage = usageFilter === 'all' || k.KeyUsage === usageFilter;
	const matchOrigin = originFilter === 'all' || (k.Origin ?? 'AWS_KMS') === originFilter;
	return matchSearch && matchState && matchUsage && matchOrigin;
}));

let filteredAliases = $derived(aliases.filter(a =>
	!aliasSearch || (a.AliasName ?? '').toLowerCase().includes(aliasSearch.toLowerCase())
));

let awsAliasCount = $derived(aliases.filter(a => (a.AliasName ?? '').startsWith('alias/aws/')).length);
let customerAliasCount = $derived(aliases.filter(a => !(a.AliasName ?? '').startsWith('alias/aws/')).length);

const KEY_SPECS = ['SYMMETRIC_DEFAULT','RSA_2048','RSA_3072','RSA_4096','ECC_NIST_P256','ECC_NIST_P384','ECC_NIST_P521','HMAC_256','HMAC_384','HMAC_512'];
const KEY_USAGES = ['ENCRYPT_DECRYPT','SIGN_VERIFY','GENERATE_VERIFY_MAC','KEY_AGREEMENT'];
const SIGN_ALGS = ['RSASSA_PSS_SHA_256','RSASSA_PSS_SHA_384','RSASSA_PSS_SHA_512','RSASSA_PKCS1_V1_5_SHA_256','RSASSA_PKCS1_V1_5_SHA_384','RSASSA_PKCS1_V1_5_SHA_512','ECDSA_SHA_256','ECDSA_SHA_384','ECDSA_SHA_512'];
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
				<Key class="w-6 h-6 text-amber-600 dark:text-amber-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">KMS Keys</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm">Key Management Service</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button onclick={refresh} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700" title="Refresh">
				<RefreshCw class="w-4 h-4" />
			</button>
			<button onclick={seedDemoData} disabled={seedingDemo} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-600 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700 flex items-center gap-2 text-sm" title="Seed demo keys">
				<Database class="w-4 h-4" /> {seedingDemo ? 'Seeding...' : 'Demo Data'}
			</button>
			<button id="create-key-btn" onclick={() => showCreateModal = true} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 flex items-center gap-2">
				<Plus class="w-4 h-4" /> Create Key
			</button>
		</div>
	</div>

	<!-- Stats -->
	{#if !loading}
	<div class="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-8 gap-3">
		{#each [
			{ label: 'Enabled', count: enabledCount, color: 'text-green-500' },
			{ label: 'Disabled', count: disabledCount, color: 'text-slate-400' },
			{ label: 'Pending Del.', count: pendingCount, color: 'text-red-500' },
			{ label: 'Pending Import', count: pendingImportCount, color: 'text-orange-500' },
			{ label: 'Symmetric', count: symmetricCount, color: 'text-amber-500' },
			{ label: 'Asymmetric', count: asymmetricCount, color: 'text-indigo-500' },
			{ label: 'HMAC', count: hmacCount, color: 'text-teal-500' },
			{ label: 'Multi-Region', count: multiRegionCount, color: 'text-purple-500' }
		] as stat}
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-2xl font-bold {stat.color}">{stat.count}</p>
				<p class="text-xs text-slate-500 dark:text-slate-400 mt-1">{stat.label}</p>
			</div>
		{/each}
	</div>
	{/if}

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700">
		<nav class="flex gap-1 -mb-px overflow-x-auto">
			<button onclick={() => selectTab('keys')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'keys' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Key class="w-4 h-4" /> Keys {#if keys.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{keys.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('aliases')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'aliases' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Tag class="w-4 h-4" /> Aliases {#if aliases.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{aliases.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('grants')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'grants' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Shield class="w-4 h-4" /> Grants {#if grantsTabGrants.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{grantsTabGrants.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('metrics')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'metrics' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<BarChart3 class="w-4 h-4" /> Metrics
			</button>
			<button onclick={() => selectTab('docs')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors whitespace-nowrap {activeTab === 'docs' ? 'border-amber-500 text-amber-600 dark:text-amber-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<BookOpen class="w-4 h-4" /> Docs
			</button>
		</nav>
	</div>

	<!-- Keys Tab -->
	{#if activeTab === 'keys'}
	<!-- Filters -->
	<div class="flex flex-wrap gap-3">
		<div class="relative flex-1 min-w-48">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input type="text" placeholder="Search keys by description or ID..." bind:value={search}
				class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
		</div>
		<select bind:value={stateFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
			<option value="all">All States</option>
			<option value="Enabled">Enabled</option>
			<option value="Disabled">Disabled</option>
			<option value="PendingDeletion">Pending Deletion</option>
		</select>
		<select bind:value={usageFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
			<option value="all">All Usages</option>
			{#each uniqueUsages as u}<option value={u}>{u.replace(/_/g, ' ')}</option>{/each}
		</select>
		<select bind:value={originFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
			<option value="all">All Origins</option>
			{#each uniqueOrigins as o}<option value={o}>{o}</option>{/each}
		</select>
	</div>

	<!-- List + detail panel -->
	<div class="flex gap-4">
		<div class="flex-1 min-w-0">
			{#if loading}
				<div class="text-center py-12 text-slate-500">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-amber-500 mb-3"></div>
					<p>Loading keys...</p>
				</div>
			{:else if filtered.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Key class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
					<p class="text-slate-500 dark:text-slate-400 font-medium">No KMS keys found</p>
				</div>
			{:else}
				<div class="space-y-3">
					{#each filtered as key}
						<div
							class="bg-white dark:bg-slate-800 rounded-xl border cursor-pointer transition-all
								{selectedKey?.KeyId === key.KeyId
									? 'border-amber-400 dark:border-amber-500 ring-1 ring-amber-400/30'
									: 'border-slate-200 dark:border-slate-700 hover:border-slate-300 dark:hover:border-slate-600'}
								p-5"
							onclick={() => selectedKey = selectedKey?.KeyId === key.KeyId ? null : key}
							role="button"
							tabindex="0"
							onkeydown={(e) => e.key === 'Enter' && (selectedKey = selectedKey?.KeyId === key.KeyId ? null : key)}
						>
							<div class="flex items-start justify-between mb-3">
								<div class="min-w-0 flex-1">
									<h3 class="font-semibold text-slate-900 dark:text-white truncate">
										{key.Description || '(no description)'}
									</h3>
									<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mt-0.5 truncate">{key.KeyId}</p>
								</div>
								<div class="flex items-center gap-2 ml-3 shrink-0">
									{#if key.MultiRegion}<span class="px-2 py-0.5 rounded-full text-xs font-medium bg-purple-100 dark:bg-purple-900/30 text-purple-700 dark:text-purple-300">Multi-Region</span>{/if}
									{#if isHMAC(key)}<span class="px-2 py-0.5 rounded-full text-xs font-medium bg-teal-100 dark:bg-teal-900/30 text-teal-700 dark:text-teal-300">HMAC</span>{/if}
									{#if (key.Origin ?? 'AWS_KMS') !== 'AWS_KMS'}<span class="px-2 py-0.5 rounded-full text-xs font-medium bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300">{key.Origin}</span>{/if}
									<span class={`px-3 py-1 rounded-full text-xs font-medium ${getStateColor(key.KeyState ?? '')}`}>{key.KeyState}</span>
								</div>
							</div>
							<div class="grid grid-cols-3 gap-3 text-sm mb-4">
								<div>
									<p class="text-xs text-slate-400 uppercase tracking-wide">Usage</p>
									<p class="font-medium text-slate-900 dark:text-white text-xs mt-0.5">{key.KeyUsage}</p>
								</div>
								<div>
									<p class="text-xs text-slate-400 uppercase tracking-wide">Spec</p>
									<p class="font-medium text-slate-900 dark:text-white text-xs mt-0.5">{getKeySpec(key)}</p>
								</div>
								<div>
									<p class="text-xs text-slate-400 uppercase tracking-wide">Origin</p>
									<p class="font-medium text-slate-900 dark:text-white text-xs mt-0.5">{key.Origin || 'AWS_KMS'}</p>
								</div>
							</div>
							<div class="flex flex-wrap gap-2">
								{#if key.KeyState === 'Enabled'}
									<button onclick={(e) => { e.stopPropagation(); disableKey(key.KeyId ?? ''); }} class="px-3 py-1.5 bg-yellow-600 text-white rounded-lg text-xs hover:bg-yellow-700 flex items-center gap-1"><Lock class="w-3 h-3" /> Disable</button>
								{:else if key.KeyState === 'Disabled'}
									<button onclick={(e) => { e.stopPropagation(); enableKey(key.KeyId ?? ''); }} class="px-3 py-1.5 bg-green-600 text-white rounded-lg text-xs hover:bg-green-700 flex items-center gap-1"><Unlock class="w-3 h-3" /> Enable</button>
								{/if}
								{#if isSymmetric(key) && key.KeyState === 'Enabled'}
									<button id="crypto-btn-{key.KeyId}" onclick={(e) => { e.stopPropagation(); openCrypto(key.KeyId ?? ''); }} class="px-3 py-1.5 bg-indigo-600 text-white rounded-lg text-xs hover:bg-indigo-700 flex items-center gap-1"><Lock class="w-3 h-3" /> Crypto</button>
								{/if}
								{#if isHMAC(key) && key.KeyState === 'Enabled'}
									<button onclick={(e) => { e.stopPropagation(); openGrantModal(key); }} class="px-3 py-1.5 bg-teal-600 text-white rounded-lg text-xs hover:bg-teal-700 flex items-center gap-1"><Shield class="w-3 h-3" /> MAC Grants</button>
								{/if}
								{#if isSignVerify(key) && key.KeyState === 'Enabled'}
									<button onclick={(e) => { e.stopPropagation(); openSignModal(key); }} class="px-3 py-1.5 bg-violet-600 text-white rounded-lg text-xs hover:bg-violet-700 flex items-center gap-1"><Shield class="w-3 h-3" /> Sign/Verify</button>
								{/if}
								{#if isSymmetric(key)}
									<button onclick={(e) => { e.stopPropagation(); openRotationModal(key); }} class="px-3 py-1.5 bg-teal-600 text-white rounded-lg text-xs hover:bg-teal-700 flex items-center gap-1"><RotateCcw class="w-3 h-3" /> Rotation</button>
								{/if}
								<button onclick={(e) => { e.stopPropagation(); openTagModal(key); }} class="px-3 py-1.5 bg-slate-600 text-white rounded-lg text-xs hover:bg-slate-700 flex items-center gap-1"><Tag class="w-3 h-3" /> Tags</button>
								<button onclick={(e) => { e.stopPropagation(); openGrantModal(key); }} class="px-3 py-1.5 bg-orange-600 text-white rounded-lg text-xs hover:bg-orange-700 flex items-center gap-1"><Shield class="w-3 h-3" /> Grants</button>
								<button onclick={(e) => { e.stopPropagation(); openPolicyModal(key); }} class="px-3 py-1.5 bg-pink-600 text-white rounded-lg text-xs hover:bg-pink-700 flex items-center gap-1"><Edit2 class="w-3 h-3" /> Policy</button>
								<button onclick={(e) => { e.stopPropagation(); openDeletionModal(key); }} class="px-3 py-1.5 {key.KeyState === 'PendingDeletion' ? 'bg-amber-600 hover:bg-amber-700' : 'bg-red-600 hover:bg-red-700'} text-white rounded-lg text-xs flex items-center gap-1"><Trash2 class="w-3 h-3" /> {key.KeyState === 'PendingDeletion' ? 'Cancel Delete' : 'Delete'}</button>
								<button onclick={(e) => { e.stopPropagation(); copyId(key.KeyId ?? ''); }} class="px-3 py-1.5 bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 rounded-lg text-xs hover:bg-slate-200 dark:hover:bg-slate-600 flex items-center gap-1"><Copy class="w-3 h-3" /> Copy ID</button>
								{#if key.KeyArn}
									<button onclick={(e) => { e.stopPropagation(); copyId(key.KeyArn ?? ''); }} class="px-3 py-1.5 bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 rounded-lg text-xs hover:bg-slate-200 dark:hover:bg-slate-600 flex items-center gap-1"><Copy class="w-3 h-3" /> Copy ARN</button>
								{/if}
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>

		<!-- Detail panel -->
		{#if selectedKey}
			<div class="w-80 shrink-0">
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5 space-y-4 sticky top-4">
					<div class="flex items-start justify-between">
						<h3 class="font-bold text-slate-900 dark:text-white">Key Details</h3>
						<button onclick={() => selectedKey = null} class="text-slate-400 hover:text-slate-600 dark:hover:text-slate-200 text-xl leading-none">&times;</button>
					</div>
					<div class="space-y-3 text-sm">
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">Key ID</p>
							<div class="flex items-center gap-1">
								<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all flex-1">{selectedKey.KeyId}</p>
								<button onclick={() => copyId(selectedKey!.KeyId ?? '')} class="p-1 text-slate-400 hover:text-slate-600 shrink-0" title="Copy"><Copy class="w-3.5 h-3.5" /></button>
							</div>
						</div>
						{#if selectedKey.KeyArn}
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 uppercase tracking-wide mb-1">ARN</p>
							<div class="flex items-center gap-1">
								<p class="text-slate-700 dark:text-slate-300 font-mono text-xs break-all flex-1">{selectedKey.KeyArn}</p>
								<button onclick={() => copyId(selectedKey!.KeyArn ?? '')} class="p-1 text-slate-400 hover:text-slate-600 shrink-0" title="Copy ARN"><Copy class="w-3.5 h-3.5" /></button>
							</div>
						</div>
						{/if}
						<div class="grid grid-cols-2 gap-3">
							<div>
								<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">State</p>
								<span class={`px-2 py-0.5 rounded text-xs font-medium ${getStateColor(selectedKey.KeyState ?? '')}`}>{selectedKey.KeyState}</span>
							</div>
							<div>
								<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Manager</p>
								<p class="text-slate-700 dark:text-slate-300 text-xs">{getKeyManager(selectedKey)}</p>
							</div>
						</div>
						<div class="grid grid-cols-2 gap-3">
							<div>
								<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Spec</p>
								<p class="text-slate-700 dark:text-slate-300 text-xs">{getKeySpec(selectedKey)}</p>
							</div>
							<div>
								<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Usage</p>
								<p class="text-slate-700 dark:text-slate-300 text-xs">{selectedKey.KeyUsage}</p>
							</div>
						</div>
						{#if selectedKey.MultiRegion}
						<div>
							<p class="text-xs font-medium text-purple-500 mb-1">Multi-Region</p>
							<p class="text-slate-700 dark:text-slate-300 text-xs">Enabled</p>
						</div>
						{/if}
						{#if selectedKey.CreationDate}
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Created</p>
							<p class="text-slate-700 dark:text-slate-300 text-xs">{new Date(selectedKey.CreationDate).toLocaleString()}</p>
						</div>
						{/if}
						{#if selectedKey.DeletionDate}
						<div>
							<p class="text-xs font-medium text-red-500 mb-1">Deletion Date</p>
							<p class="text-red-600 dark:text-red-400 text-xs">{new Date(selectedKey.DeletionDate).toLocaleString()}</p>
						</div>
						{/if}
						{#if selectedKey.PendingWindowInDays}
						<div>
							<p class="text-xs font-medium text-red-500 mb-1">Pending Window</p>
							<p class="text-red-600 dark:text-red-400 text-xs">{selectedKey.PendingWindowInDays} days</p>
						</div>
						{/if}
						<div>
							<p class="text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Origin</p>
							<p class="text-slate-700 dark:text-slate-300 text-xs">{selectedKey.Origin ?? 'AWS_KMS'}</p>
						</div>
					</div>
				</div>
			</div>
		{/if}
	</div>

	<!-- End Keys Tab -->
	{/if}

	<!-- Aliases Tab -->
	{#if activeTab === 'aliases'}
		<div>
			<div class="flex items-center gap-3 mb-4">
				<div class="relative flex-1">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
					<input type="text" placeholder="Search aliases..." bind:value={aliasSearch}
						class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm" />
				</div>
				<span class="px-2.5 py-1 rounded-full bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 text-sm shrink-0">{customerAliasCount} customer</span>
				<span class="px-2.5 py-1 rounded-full bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 text-sm shrink-0">{awsAliasCount} AWS</span>
				<button onclick={() => showAliasCreateModal = true} class="px-3 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 flex items-center gap-2 text-sm shrink-0"><Plus class="w-4 h-4" /> New Alias</button>
			</div>
			{#if loading}
				<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-amber-500"></div></div>
			{:else if filteredAliases.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Tag class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
					<p class="text-slate-500 dark:text-slate-400">No aliases found</p>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
					<table class="w-full text-sm">
						<thead class="bg-slate-50 dark:bg-slate-700">
							<tr>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Alias Name</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">ARN</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Target Key ID</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Type</th>
								<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Actions</th>
							</tr>
						</thead>
						<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each filteredAliases as alias}
								{@const isAws = (alias.AliasName || '').startsWith('alias/aws/')}
								<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
									<td class="px-4 py-3 font-mono text-xs text-slate-900 dark:text-white">{alias.AliasName}</td>
									<td class="px-4 py-3 font-mono text-xs text-slate-400 dark:text-slate-500 max-w-xs truncate" title={alias.AliasArn ?? ''}>{alias.AliasArn ?? '—'}</td>
									<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{alias.TargetKeyId || '—'}</td>
									<td class="px-4 py-3">
										<span class="px-2 py-0.5 text-xs rounded-full {isAws ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-600 dark:text-blue-400' : 'bg-green-100 dark:bg-green-900/30 text-green-600 dark:text-green-400'}">
											{isAws ? 'AWS Managed' : 'Customer'}
										</span>
									</td>
									<td class="px-4 py-3">
										<div class="flex gap-1">
											{#if alias.TargetKeyId}
												<button onclick={() => copyId(alias.TargetKeyId ?? '')} class="p-1 text-slate-400 hover:text-slate-600" title="Copy Key ID"><Copy class="w-3.5 h-3.5" /></button>
											{/if}
											{#if alias.AliasArn}
												<button onclick={() => copyId(alias.AliasArn ?? '')} class="p-1 text-slate-400 hover:text-slate-600" title="Copy ARN"><Copy class="w-3.5 h-3.5" /></button>
											{/if}
											{#if !isAws}
												<button onclick={() => openUpdateAlias(alias)} class="p-1 text-blue-400 hover:text-blue-600" title="Update"><Edit2 class="w-3.5 h-3.5" /></button>
												<button onclick={() => deleteAlias(alias.AliasName ?? '')} class="p-1 text-red-400 hover:text-red-600" title="Delete"><Trash2 class="w-3.5 h-3.5" /></button>
											{/if}
										</div>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{/if}
		</div>
	{/if}

	<!-- Grants Tab -->
	{#if activeTab === 'grants'}
	<div class="space-y-6">
		<!-- Key selector -->
		<div class="flex items-center gap-3">
			<label class="text-sm font-medium text-slate-700 dark:text-slate-300 whitespace-nowrap">Key:</label>
			<select bind:value={grantsTabKeyId} onchange={async () => { grantsTabGrants = []; policyTabContent = ''; if (grantsTabKeyId) { await loadGrantsForTab(); await loadPolicyForTab(); } }} class="flex-1 px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white text-sm">
				<option value="">— select a key —</option>
				{#each keys as k}
					<option value={k.KeyId ?? ''}>{k.Description || k.KeyId} ({(k.KeyId ?? '').slice(0, 8)}...)</option>
				{/each}
			</select>
			{#if grantsTabKeyId}
			<button onclick={loadGrantsForTab} class="p-2 text-slate-400 hover:text-slate-600"><RefreshCw class="w-4 h-4" /></button>
			{/if}
		</div>

		{#if grantsTabKeyId}
		<div class="grid grid-cols-1 xl:grid-cols-2 gap-6">
			<!-- Left: Grants list + create form -->
			<div class="space-y-4">
				<!-- Grants list -->
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden">
					<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
						<h3 class="text-sm font-semibold text-slate-900 dark:text-white flex items-center gap-2"><Shield class="w-4 h-4 text-amber-500" /> Grants ({grantsTabGrants.length})</h3>
					</div>
					{#if grantsTabLoading}
						<div class="p-8 text-center"><div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-amber-500"></div></div>
					{:else if grantsTabGrants.length === 0}
						<p class="p-6 text-sm text-slate-400 italic text-center">No grants for this key.</p>
					{:else}
						<div class="divide-y divide-slate-100 dark:divide-slate-700">
							{#each grantsTabGrants as grant}
							<div class="px-4 py-3 space-y-1">
								<div class="flex items-start justify-between gap-2">
									<div class="min-w-0 flex-1">
										{#if grant.Name}<p class="text-xs font-semibold text-slate-900 dark:text-white truncate">{grant.Name}</p>{/if}
										<p class="text-xs font-mono text-slate-500 dark:text-slate-400 truncate">{grant.GranteePrincipal}</p>
										<div class="flex flex-wrap gap-1 mt-1">
											{#each grant.Operations ?? [] as op}
												<span class="px-1.5 py-0.5 text-[10px] bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-300 rounded">{op}</span>
											{/each}
										</div>
										<p class="text-[10px] text-slate-400 font-mono mt-1 truncate">ID: {grant.GrantId}</p>
									</div>
									<button onclick={() => revokeGrantInTab(grant.GrantId)} class="flex-shrink-0 p-1.5 text-red-400 hover:text-red-600 hover:bg-red-50 dark:hover:bg-red-900/20 rounded" title="Revoke grant">
										<Trash2 class="w-3.5 h-3.5" />
									</button>
								</div>
							</div>
							{/each}
						</div>
					{/if}
				</div>

				<!-- Create grant form -->
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-4 space-y-4">
					<h3 class="text-sm font-semibold text-slate-900 dark:text-white flex items-center gap-2"><Plus class="w-4 h-4 text-amber-500" /> Create Grant</h3>
					<div>
						<label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Grantee Principal (ARN)</label>
						<input type="text" bind:value={grantsTabPrincipal} placeholder="arn:aws:iam::123456789012:role/my-role" class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-1 focus:ring-amber-500" />
					</div>
					<div>
						<label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Grant Name (optional)</label>
						<input type="text" bind:value={grantsTabName} placeholder="my-grant" class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm focus:outline-none focus:ring-1 focus:ring-amber-500" />
					</div>
					<div>
						<label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-2">Operations</label>
						<div class="grid grid-cols-2 gap-1.5">
							{#each ALL_GRANT_OPERATIONS as op}
							<label class="flex items-center gap-2 cursor-pointer select-none">
								<input type="checkbox" checked={grantsTabSelectedOps.has(op)} onchange={() => {
									const s = new Set(grantsTabSelectedOps);
									if (s.has(op)) s.delete(op); else s.add(op);
									grantsTabSelectedOps = s;
								}} class="w-3.5 h-3.5 rounded border-slate-300 text-amber-500 focus:ring-amber-500" />
								<span class="text-xs text-slate-700 dark:text-slate-300">{op}</span>
							</label>
							{/each}
						</div>
					</div>
					<button onclick={createGrantInTab} disabled={grantsTabCreating || !grantsTabPrincipal.trim() || grantsTabSelectedOps.size === 0} class="w-full px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 disabled:opacity-50 text-sm font-medium flex items-center justify-center gap-2">
						{#if grantsTabCreating}<div class="animate-spin rounded-full h-3.5 w-3.5 border-b-2 border-white"></div>{/if}
						{grantsTabCreating ? 'Creating...' : 'Create Grant'}
					</button>
				</div>
			</div>

			<!-- Right: Key policy viewer/editor -->
			<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 overflow-hidden flex flex-col">
				<div class="px-4 py-3 border-b border-slate-200 dark:border-slate-700 flex items-center justify-between">
					<h3 class="text-sm font-semibold text-slate-900 dark:text-white flex items-center gap-2"><BookOpen class="w-4 h-4 text-amber-500" /> Key Policy</h3>
					<button onclick={loadPolicyForTab} class="p-1.5 text-slate-400 hover:text-slate-600" title="Reload policy"><RefreshCw class="w-3.5 h-3.5" /></button>
				</div>
				{#if policyTabLoading}
					<div class="p-8 text-center flex-1"><div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-amber-500"></div></div>
				{:else}
					<div class="flex flex-col flex-1 p-4 gap-3">
						<textarea bind:value={policyTabContent} rows={20} spellcheck={false} class="flex-1 w-full font-mono text-xs border border-slate-200 dark:border-slate-700 rounded-lg p-3 bg-slate-50 dark:bg-slate-900 text-slate-900 dark:text-white resize-none focus:outline-none focus:ring-1 focus:ring-amber-500" placeholder="Loading policy..."></textarea>
						<button onclick={savePolicyForTab} disabled={policyTabSaving || !policyTabContent} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 disabled:opacity-50 text-sm font-medium flex items-center justify-center gap-2">
							{#if policyTabSaving}<div class="animate-spin rounded-full h-3.5 w-3.5 border-b-2 border-white"></div>{/if}
							{policyTabSaving ? 'Saving...' : 'Save Policy'}
						</button>
					</div>
				{/if}
			</div>
		</div>
		{:else}
			<p class="text-sm text-slate-400 italic text-center py-12">Select a key above to manage grants and view its key policy.</p>
		{/if}
	</div>
	{/if}

	<!-- Metrics Tab -->
	{#if activeTab === 'metrics'}
		<div class="space-y-6">
			<div class="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-7 gap-4">
				{#each [
					{ label: 'Total Keys', value: keys.length, color: 'text-amber-600 dark:text-amber-400' },
					{ label: 'Enabled', value: enabledCount, color: 'text-green-600 dark:text-green-400' },
					{ label: 'Disabled', value: disabledCount, color: 'text-slate-500' },
					{ label: 'Pending Del.', value: pendingCount, color: 'text-red-600 dark:text-red-400' },
					{ label: 'HMAC', value: hmacCount, color: 'text-teal-600 dark:text-teal-400' },
					{ label: 'Multi-Region', value: multiRegionCount, color: 'text-purple-600 dark:text-purple-400' },
					{ label: 'Total Aliases', value: aliases.length, color: 'text-blue-600 dark:text-blue-400' }
				] as m}
					<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5 text-center">
						<p class="text-3xl font-bold {m.color}">{m.value}</p>
						<p class="text-xs text-slate-500 dark:text-slate-400 mt-2">{m.label}</p>
					</div>
				{/each}
			</div>
			<div class="grid grid-cols-1 md:grid-cols-3 gap-4">
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5">
					<h3 class="font-semibold text-slate-900 dark:text-white mb-4">Key Usage Breakdown</h3>
					{#each uniqueUsages as usage}
						{@const count = keys.filter(k => k.KeyUsage === usage).length}
						{@const pct = keys.length > 0 ? Math.round((count / keys.length) * 100) : 0}
						<div class="flex items-center gap-3 mb-3">
							<span class="text-xs text-slate-500 dark:text-slate-400 w-44 shrink-0">{usage.replace(/_/g,' ')}</span>
							<div class="flex-1 bg-slate-100 dark:bg-slate-700 rounded-full h-2"><div class="bg-amber-500 h-2 rounded-full" style="width: {pct}%"></div></div>
							<span class="text-xs font-medium text-slate-700 dark:text-slate-300 w-8 text-right">{count}</span>
						</div>
					{/each}
				</div>
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5">
					<h3 class="font-semibold text-slate-900 dark:text-white mb-4">Key State Distribution</h3>
					{#each [['Enabled', enabledCount, 'bg-green-500'], ['Disabled', disabledCount, 'bg-slate-400'], ['PendingDeletion', pendingCount, 'bg-red-500'], ['Other', Math.max(0, keys.length - enabledCount - disabledCount - pendingCount), 'bg-yellow-500']] as [label, count, color]}
						{@const pct = keys.length > 0 ? Math.round(((count as number) / keys.length) * 100) : 0}
						<div class="flex items-center gap-3 mb-3">
							<span class="text-xs text-slate-500 dark:text-slate-400 w-32 shrink-0">{label}</span>
							<div class="flex-1 bg-slate-100 dark:bg-slate-700 rounded-full h-2"><div class="{color} h-2 rounded-full" style="width: {pct}%"></div></div>
							<span class="text-xs font-medium text-slate-700 dark:text-slate-300 w-8 text-right">{count}</span>
						</div>
					{/each}
				</div>
				<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-5">
					<h3 class="font-semibold text-slate-900 dark:text-white mb-4">Key Spec Breakdown</h3>
					{#each uniqueSpecs as spec}
						{@const count = keys.filter(k => getKeySpec(k) === spec).length}
						{@const pct = keys.length > 0 ? Math.round((count / keys.length) * 100) : 0}
						<div class="flex items-center gap-3 mb-3">
							<span class="text-xs text-slate-500 dark:text-slate-400 w-40 shrink-0 truncate">{spec}</span>
							<div class="flex-1 bg-slate-100 dark:bg-slate-700 rounded-full h-2"><div class="bg-indigo-500 h-2 rounded-full" style="width: {pct}%"></div></div>
							<span class="text-xs font-medium text-slate-700 dark:text-slate-300 w-8 text-right">{count}</span>
						</div>
					{/each}
				</div>
			</div>
		</div>
	{/if}

	<!-- Docs Tab -->
	{#if activeTab === 'docs'}
		<div class="bg-white dark:bg-slate-800 rounded-xl border border-slate-200 dark:border-slate-700 p-6 space-y-3">
			<p class="text-sm text-slate-500 dark:text-slate-400">Supported KMS operations in GopherStack.</p>
			{#each ([
				['CreateKey', 'Create symmetric (SYMMETRIC_DEFAULT), asymmetric (RSA/ECC), HMAC, or KEY_AGREEMENT keys. Supports MultiRegion and Tags at creation.'],
				['DescribeKey', 'Get key metadata including spec, usage, state, multi-region config, deletion date, and pending window.'],
				['ListKeys', 'Paginated list of all keys in the account/region.'],
				['EnableKey / DisableKey', 'Enable or disable a key. Disabled keys reject all cryptographic operations.'],
				['ScheduleKeyDeletion', 'Schedule deletion with a 7-30 day window (default 30). Returns DeletionDate and PendingWindowInDays.'],
				['CancelKeyDeletion', 'Cancel pending deletion. Key returns to Disabled state.'],
				['Encrypt / Decrypt', 'Encrypt plaintext (up to 4096 bytes) or decrypt ciphertext. Supports EncryptionContext.'],
				['GenerateDataKey / GenerateDataKeyWithoutPlaintext', 'Generate AES-128/256 data keys.'],
				['ReEncrypt', 'Re-encrypt ciphertext under a different key without exposing plaintext.'],
				['Sign / Verify', 'Sign messages and verify signatures with RSA (PSS/PKCS1) or ECDSA keys.'],
				['GetPublicKey', 'Retrieve DER-encoded public key for an asymmetric KMS key.'],
				['CreateAlias / UpdateAlias / DeleteAlias', 'Manage aliases. Names must start with alias/ (not alias/aws/).'],
				['ListAliases', 'List all aliases, optionally filtered by key.'],
				['EnableKeyRotation / DisableKeyRotation', 'Toggle automatic rotation. Supports RotationPeriodInDays (1-2560, default 365).'],
				['GetKeyRotationStatus', 'Returns rotation state, RotationPeriodInDays, and NextRotationDate.'],
				['RotateKeyOnDemand', 'Immediately rotate key material on demand.'],
				['ListKeyRotations', 'List history of key material rotation dates.'],
				['CreateGrant / ListGrants / RevokeGrant / RetireGrant', 'Manage grants for delegating key usage to other principals.'],
				['GetKeyPolicy / PutKeyPolicy / ListKeyPolicies', 'Read, write, or list key resource policies.'],
				['TagResource / UntagResource / ListResourceTags', 'Manage key tags with pagination.'],
				['ImportKeyMaterial / DeleteImportedKeyMaterial', 'Import or delete external key material.'],
				['GetParametersForImport', 'Retrieve import token and wrapping key for key material import.'],
				['ReplicateKey', 'Create a multi-region replica. Only Enabled keys can be replicated.'],
				['UpdatePrimaryRegion / UpdateKeyDescription', 'Update primary region marker or key description.'],
				['GenerateMac / VerifyMac', 'Generate or verify HMAC tags using HMAC keys.'],
				['GenerateDataKeyPair / GenerateDataKeyPairWithoutPlaintext', 'Generate RSA or ECC data key pairs.'],
				['DeriveSharedSecret', 'Compute ECDH shared secrets using KEY_AGREEMENT keys.'],
				['GenerateRandom', 'Generate cryptographically random bytes (up to 1024).'],
				['ConnectCustomKeyStore / DisconnectCustomKeyStore / CreateCustomKeyStore / DeleteCustomKeyStore', 'Custom key store lifecycle management.'],
			] satisfies [string, string][]) as [opname, opdesc]}
				<div class="border-b border-slate-100 dark:border-slate-700 pb-3 last:border-0">
					<p class="text-sm font-semibold text-slate-800 dark:text-white">{opname}</p>
					<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{opdesc}</p>
				</div>
			{/each}
		</div>
	{/if}
</div>

<!-- Create Modal -->
{#if showCreateModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create KMS Key</h2>
			<form onsubmit={(e) => { e.preventDefault(); createKey(); }} class="space-y-4">
				<div>
					<label for="new-key-description" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description</label>
					<input id="new-key-description" name="description" type="text" bind:value={newKeyDescription} placeholder="e.g. Test Key" class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
				</div>
				<div>
					<label for="new-key-spec" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Key Spec</label>
					<select id="new-key-spec" bind:value={newKeySpec} class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white">
						{#each KEY_SPECS as s}<option value={s}>{s}</option>{/each}
					</select>
				</div>
				<div>
					<label for="new-key-usage" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Key Usage</label>
					<select id="new-key-usage" bind:value={newKeyUsage} class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white">
						{#each KEY_USAGES as u}<option value={u}>{u.replace(/_/g,' ')}</option>{/each}
					</select>
				</div>
				<div class="flex items-center gap-2">
					<input id="new-key-multiregion" type="checkbox" bind:checked={newKeyMultiRegion} class="rounded" />
					<label for="new-key-multiregion" class="text-sm text-slate-700 dark:text-slate-300">Multi-Region Key</label>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => showCreateModal = false} class="px-4 py-2 text-slate-500">Cancel</button>
					<button type="submit" disabled={creatingKey} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700">
						{creatingKey ? 'Creating...' : 'Create'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Crypto Modal -->
{#if showCryptoModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Encrypt / Decrypt / Re-Encrypt</h2>
			<div class="space-y-6">
				<!-- Encrypt -->
				<div class="space-y-2">
					<label for="plaintext-input" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Plaintext</label>
					<div class="flex gap-2">
						<input id="plaintext-input" name="plaintext" type="text" bind:value={plaintext} class="flex-1 px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
						<button id="encrypt-submit" onclick={encrypt} disabled={encrypting} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
							<Lock class="w-4 h-4" /> Encrypt
						</button>
					</div>
					{#if ciphertext}
						<div class="p-3 bg-slate-100 dark:bg-slate-900 rounded-lg">
							<div class="flex items-center justify-between mb-1">
								<p class="text-xs text-slate-400 leading-none uppercase tracking-wider font-semibold">Ciphertext (Base64)</p>
								{#if encryptAlgorithm}<span class="text-xs px-1.5 py-0.5 bg-indigo-100 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400 rounded font-mono">{encryptAlgorithm}</span>{/if}
							</div>
							<p id="encrypt-result" class="text-xs font-mono break-all text-slate-700 dark:text-slate-300">{ciphertext}</p>
						</div>
					{/if}
				</div>

				<!-- Decrypt -->
				<div class="space-y-2">
					<label for="ciphertext-input" class="block text-sm font-medium text-slate-700 dark:text-slate-300">Ciphertext (Base64)</label>
					<div class="flex gap-2">
						<input id="ciphertext-input" name="ciphertext" type="text" bind:value={ciphertext} class="flex-1 px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
						<button id="decrypt-submit" onclick={decrypt} disabled={decrypting} class="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 flex items-center gap-2">
							<Unlock class="w-4 h-4" /> Decrypt
						</button>
					</div>
					{#if decryptedText}
						<div class="p-3 bg-slate-100 dark:bg-slate-900 rounded-lg">
							<div class="flex items-center justify-between mb-1">
								<p class="text-xs text-slate-400 leading-none uppercase tracking-wider font-semibold">Decrypted Text</p>
								{#if decryptAlgorithm}<span class="text-xs px-1.5 py-0.5 bg-green-100 dark:bg-green-900/30 text-green-600 dark:text-green-400 rounded font-mono">{decryptAlgorithm}</span>{/if}
							</div>
							<p id="decrypt-result" class="text-sm font-medium text-slate-900 dark:text-white">{decryptedText}</p>
						</div>
					{/if}
				</div>

				<!-- ReEncrypt -->
				<div class="border-t border-slate-200 dark:border-slate-700 pt-4 space-y-2">
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Re-Encrypt to Different Key</label>
					<div class="flex gap-2">
						<select bind:value={reEncryptDestKeyId} class="flex-1 px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm">
							<option value="">Select destination key...</option>
							{#each keys.filter(k => k.KeyState === 'Enabled' && isSymmetric(k)) as k}
								<option value={k.KeyId}>{k.Description || k.KeyId}</option>
							{/each}
						</select>
						<button onclick={reEncrypt} disabled={reEncrypting || !ciphertext || !reEncryptDestKeyId} class="px-4 py-2 bg-violet-600 text-white rounded-lg hover:bg-violet-700 flex items-center gap-2 text-sm">
							<RotateCcw class="w-4 h-4" /> Re-Encrypt
						</button>
					</div>
					{#if reEncryptedCiphertext}
						<div class="p-3 bg-slate-100 dark:bg-slate-900 rounded-lg">
							<div class="flex items-center gap-2 mb-1">
								<p class="text-xs text-slate-400 leading-none uppercase tracking-wider font-semibold flex-1">Re-Encrypted Ciphertext</p>
								{#if reEncryptSrcAlgo}<span class="text-xs px-1 py-0.5 bg-slate-200 dark:bg-slate-700 text-slate-500 rounded font-mono">{reEncryptSrcAlgo}</span>{/if}
								{#if reEncryptDstAlgo}<span class="text-xs">→</span><span class="text-xs px-1 py-0.5 bg-violet-100 dark:bg-violet-900/30 text-violet-600 dark:text-violet-400 rounded font-mono">{reEncryptDstAlgo}</span>{/if}
							</div>
							<p class="text-xs font-mono break-all text-slate-700 dark:text-slate-300">{reEncryptedCiphertext}</p>
						</div>
					{/if}
				</div>
			</div>
			<div class="flex justify-end mt-6">
				<button onclick={() => showCryptoModal = false} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 rounded-lg">Close</button>
			</div>
		</div>
	</div>
{/if}

<!-- Sign/Verify Modal -->
{#if showSignModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Sign / Verify</h2>
			<div class="grid grid-cols-2 gap-3 mb-4">
				<div>
					<label class="block text-xs font-medium text-slate-500 mb-1">Signing Algorithm</label>
					<select bind:value={signAlgorithm} class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm">
						{#each SIGN_ALGS as alg}<option value={alg}>{alg}</option>{/each}
					</select>
				</div>
				<div>
					<label class="block text-xs font-medium text-slate-500 mb-1">Message Type</label>
					<select bind:value={signMessageType} class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm">
						<option value="RAW">RAW</option>
						<option value="DIGEST">DIGEST</option>
					</select>
				</div>
			</div>
			<div class="space-y-4">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message to Sign</label>
					<div class="flex gap-2">
						<input type="text" bind:value={signMessage} placeholder="Enter message..." class="flex-1 px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
						<button onclick={signData} disabled={signing} class="px-4 py-2 bg-violet-600 text-white rounded-lg hover:bg-violet-700 text-sm flex items-center gap-1"><Shield class="w-4 h-4" /> Sign</button>
					</div>
					{#if signature}
						<div class="mt-2 p-3 bg-slate-100 dark:bg-slate-900 rounded-lg">
							<p class="text-xs text-slate-400 mb-1 uppercase tracking-wider font-semibold">Signature (Base64)</p>
							<p class="text-xs font-mono break-all text-slate-700 dark:text-slate-300">{signature}</p>
						</div>
					{/if}
				</div>
				<hr class="border-slate-200 dark:border-slate-700" />
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Message to Verify</label>
					<input type="text" bind:value={verifyMessage} placeholder="Enter original message..." class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm mb-2" />
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Signature (Base64)</label>
					<div class="flex gap-2">
						<input type="text" bind:value={verifySig} placeholder="Paste signature..." class="flex-1 px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
						<button onclick={verifyData} disabled={verifying} class="px-4 py-2 bg-teal-600 text-white rounded-lg hover:bg-teal-700 text-sm flex items-center gap-1"><CheckCircle class="w-4 h-4" /> Verify</button>
					</div>
					{#if verifyResult !== null}
						<div class="mt-2 p-3 rounded-lg {verifyResult ? 'bg-green-50 dark:bg-green-900/20' : 'bg-red-50 dark:bg-red-900/20'}">
							<div class="flex items-center gap-2">
								{#if verifyResult}<CheckCircle class="w-4 h-4 text-green-600" /><span class="text-sm font-medium text-green-700 dark:text-green-400">Signature valid</span>
								{:else}<XCircle class="w-4 h-4 text-red-600" /><span class="text-sm font-medium text-red-700 dark:text-red-400">Signature invalid</span>{/if}
							</div>
						</div>
					{/if}
				</div>
			</div>
			<div class="flex justify-end mt-6">
				<button onclick={() => showSignModal = false} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300 rounded-lg">Close</button>
			</div>
		</div>
	</div>
{/if}

<!-- Rotation Modal -->
{#if showRotationModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Key Rotation</h2>
			<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mb-4">{rotationKeyId}</p>
			{#if rotationStatus}
				<div class="space-y-2 mb-5">
					<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-700 rounded-lg">
						<span class="text-sm text-slate-700 dark:text-slate-300">Automatic Rotation</span>
						<span class="px-2 py-0.5 rounded-full text-xs font-medium {rotationEnabled ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-600 text-slate-600 dark:text-slate-400'}">{rotationEnabled ? 'Enabled' : 'Disabled'}</span>
					</div>
					{#if rotationStatus.RotationPeriodInDays}
						<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-700 rounded-lg">
							<span class="text-sm text-slate-700 dark:text-slate-300">Rotation Period</span>
							<span class="text-sm font-medium text-slate-900 dark:text-white">{rotationStatus.RotationPeriodInDays} days</span>
						</div>
					{/if}
					{#if rotationStatus.NextRotationDate}
						<div class="flex items-center justify-between p-3 bg-slate-50 dark:bg-slate-700 rounded-lg">
							<span class="text-sm text-slate-700 dark:text-slate-300">Next Rotation</span>
							<span class="text-sm font-medium text-slate-900 dark:text-white">{new Date(rotationStatus.NextRotationDate * 1000).toLocaleDateString()}</span>
						</div>
					{/if}
					{#if rotationStatus.OnDemandRotationStartDate}
						<div class="flex items-center justify-between p-3 bg-indigo-50 dark:bg-indigo-900/20 rounded-lg">
							<span class="text-sm text-indigo-700 dark:text-indigo-300">Last On-Demand Rotation</span>
							<span class="text-sm font-medium text-indigo-900 dark:text-indigo-200">{new Date(rotationStatus.OnDemandRotationStartDate * 1000).toLocaleString()}</span>
						</div>
					{/if}
				</div>
			{/if}
			{#if !rotationEnabled}
				<div class="mb-4">
					<label class="block text-xs font-medium text-slate-500 dark:text-slate-400 mb-1">Rotation Period (days, 1-2560)</label>
					<input type="number" min="1" max="2560" bind:value={rotationPeriodDays}
						class="w-full px-3 py-2 border border-slate-200 dark:border-slate-600 rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm" />
				</div>
			{/if}
			<div class="flex gap-3 mb-5">
				<button onclick={toggleRotation} disabled={togglingRotation} class="flex-1 px-4 py-2 {rotationEnabled ? 'bg-yellow-600 hover:bg-yellow-700' : 'bg-teal-600 hover:bg-teal-700'} text-white rounded-lg text-sm font-medium">
					{togglingRotation ? 'Updating...' : (rotationEnabled ? 'Disable Rotation' : 'Enable Rotation')}
				</button>
				<button onclick={rotateOnDemand} disabled={rotatingOnDemand} class="flex-1 px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 text-sm font-medium flex items-center justify-center gap-1">
					<RotateCcw class="w-3.5 h-3.5" /> {rotatingOnDemand ? 'Rotating...' : 'Rotate Now'}
				</button>
			</div>
			{#if rotationHistory.length > 0}
				<div class="border-t border-slate-200 dark:border-slate-700 pt-4">
					<h4 class="text-sm font-semibold text-slate-700 dark:text-slate-300 mb-3">Rotation History</h4>
					<div class="space-y-2 max-h-48 overflow-y-auto">
						{#each rotationHistory as entry}
							<div class="flex items-center justify-between p-2 bg-slate-50 dark:bg-slate-700 rounded text-xs">
								<span class="font-mono text-slate-500 dark:text-slate-400">{entry.RotationDate ? new Date(entry.RotationDate * 1000).toLocaleString() : '—'}</span>
								<span class="px-2 py-0.5 rounded-full {entry.RotationType === 'IMPORTED' ? 'bg-indigo-100 dark:bg-indigo-900/30 text-indigo-600 dark:text-indigo-400' : 'bg-teal-100 dark:bg-teal-900/30 text-teal-600 dark:text-teal-400'}">{entry.RotationType ?? '—'}</span>
							</div>
						{/each}
					</div>
				</div>
			{/if}
			<div class="flex justify-end mt-4">
				<button onclick={() => showRotationModal = false} class="px-4 py-2 text-slate-500">Close</button>
			</div>
		</div>
	</div>
{/if}

<!-- Deletion Modal -->
{#if showDeletionModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">{deletionKeyState === 'PendingDeletion' ? 'Cancel Key Deletion' : 'Schedule Key Deletion'}</h2>
			<p class="text-xs text-slate-500 dark:text-slate-400 font-mono mb-4">{deletionKeyId}</p>
			{#if deletionKeyState !== 'PendingDeletion'}
				<div class="mb-4">
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Waiting Period (days)</label>
					<input type="number" min="7" max="30" bind:value={pendingWindowDays} class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
					<p class="text-xs text-slate-400 mt-1">Must be between 7 and 30 days.</p>
				</div>
				<div class="p-3 bg-red-50 dark:bg-red-900/20 rounded-lg mb-4">
					<p class="text-sm text-red-700 dark:text-red-400">Warning: all data encrypted with this key will be permanently inaccessible after deletion.</p>
				</div>
			{:else}
				<div class="p-3 bg-amber-50 dark:bg-amber-900/20 rounded-lg mb-4">
					<p class="text-sm text-amber-700 dark:text-amber-400">This will cancel the pending deletion and return the key to Disabled state.</p>
				</div>
			{/if}
			<div class="flex justify-end gap-3">
				<button onclick={() => showDeletionModal = false} class="px-4 py-2 text-slate-500">Cancel</button>
				<button onclick={scheduleOrCancelDeletion} disabled={schedulingDeletion} class="px-4 py-2 {deletionKeyState === 'PendingDeletion' ? 'bg-amber-600 hover:bg-amber-700' : 'bg-red-600 hover:bg-red-700'} text-white rounded-lg">
					{schedulingDeletion ? 'Processing...' : (deletionKeyState === 'PendingDeletion' ? 'Cancel Deletion' : 'Schedule Deletion')}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Alias Modal -->
{#if showAliasCreateModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Alias</h2>
			<form onsubmit={(e) => { e.preventDefault(); createAlias(); }} class="space-y-4">
				<div>
					<label for="new-alias-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Alias Name</label>
					<input id="new-alias-name" type="text" bind:value={newAliasName} placeholder="alias/my-key" class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white" />
					<p class="text-xs text-slate-400 mt-1">Must start with alias/ (not alias/aws/)</p>
				</div>
				<div>
					<label for="new-alias-keyid" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Target Key</label>
					<select id="new-alias-keyid" bind:value={newAliasKeyId} class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white">
						<option value="">Select a key...</option>
						{#each keys as k}<option value={k.KeyId}>{k.Description || k.KeyId}</option>{/each}
					</select>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => showAliasCreateModal = false} class="px-4 py-2 text-slate-500">Cancel</button>
					<button type="submit" disabled={creatingAlias} class="px-4 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700">{creatingAlias ? 'Creating...' : 'Create Alias'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Update Alias Modal -->
{#if showAliasUpdateModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-md">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Update Alias</h2>
			<p class="text-sm font-mono text-slate-500 mb-4">{editAliasName}</p>
			<form onsubmit={(e) => { e.preventDefault(); updateAlias(); }} class="space-y-4">
				<div>
					<label for="edit-alias-keyid" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">New Target Key</label>
					<select id="edit-alias-keyid" bind:value={editAliasTargetKeyId} class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white">
						{#each keys as k}<option value={k.KeyId}>{k.Description || k.KeyId}</option>{/each}
					</select>
				</div>
				<div class="flex justify-end gap-3">
					<button type="button" onclick={() => showAliasUpdateModal = false} class="px-4 py-2 text-slate-500">Cancel</button>
					<button type="submit" disabled={updatingAlias} class="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">{updatingAlias ? 'Updating...' : 'Update'}</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Tag Modal -->
{#if showTagModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Manage Tags</h2>
			<p class="text-xs font-mono text-slate-500 mb-4">{tagKeyId}</p>
			{#if keyTags.length > 0}
				<div class="bg-slate-50 dark:bg-slate-700 rounded-lg overflow-hidden mb-4">
					<table class="w-full text-sm">
						<thead><tr class="bg-slate-100 dark:bg-slate-600"><th class="text-left px-3 py-2 font-medium text-slate-600 dark:text-slate-300">Key</th><th class="text-left px-3 py-2 font-medium text-slate-600 dark:text-slate-300">Value</th><th class="px-3 py-2"></th></tr></thead>
						<tbody class="divide-y divide-slate-200 dark:divide-slate-600">
							{#each keyTags as tag}
								<tr>
									<td class="px-3 py-2 text-xs font-mono text-slate-700 dark:text-slate-300">{tag.TagKey}</td>
									<td class="px-3 py-2 text-xs font-mono text-slate-600 dark:text-slate-400">{tag.TagValue}</td>
									<td class="px-3 py-2 text-right"><button onclick={() => removeTag(tag.TagKey)} class="text-red-400 hover:text-red-600"><Trash2 class="w-3.5 h-3.5" /></button></td>
								</tr>
							{/each}
						</tbody>
					</table>
				</div>
			{:else}
				<p class="text-sm text-slate-400 mb-4">No tags.</p>
			{/if}
			<div class="flex gap-2 items-end">
				<div class="flex-1"><label class="block text-xs font-medium text-slate-500 mb-1">Tag Key</label><input type="text" bind:value={newTagKey} placeholder="Environment" class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm" /></div>
				<div class="flex-1"><label class="block text-xs font-medium text-slate-500 mb-1">Tag Value</label><input type="text" bind:value={newTagValue} placeholder="production" class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm" /></div>
				<button onclick={addTag} disabled={savingTags || !newTagKey} class="px-3 py-2 bg-amber-600 text-white rounded-lg hover:bg-amber-700 text-sm shrink-0"><Plus class="w-4 h-4" /></button>
			</div>
			<div class="flex justify-end mt-4"><button onclick={() => showTagModal = false} class="px-4 py-2 text-slate-500">Done</button></div>
		</div>
	</div>
{/if}

<!-- Policy Modal -->
{#if showPolicyModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-2xl max-h-[90vh] flex flex-col">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Key Policy</h2>
			<p class="text-xs font-mono text-slate-500 mb-4">{policyKeyId}</p>
			<textarea bind:value={policyContent} rows="16" class="flex-1 w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-900 text-slate-900 dark:text-white text-xs font-mono resize-y" />
			<div class="flex justify-end gap-3 mt-4">
				<button onclick={() => showPolicyModal = false} class="px-4 py-2 text-slate-500">Cancel</button>
				<button onclick={savePolicy} disabled={savingPolicy} class="px-4 py-2 bg-pink-600 text-white rounded-lg hover:bg-pink-700">{savingPolicy ? 'Saving...' : 'Save Policy'}</button>
			</div>
		</div>
	</div>
{/if}

<!-- Grant Modal -->
{#if showGrantModal}
	<div role="dialog" class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg max-h-[90vh] overflow-y-auto">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-2">Manage Grants</h2>
			<p class="text-xs font-mono text-slate-500 mb-4">{grantKeyId}</p>
			{#if keyGrants.length > 0}
				<div class="space-y-2 mb-4">
					{#each keyGrants as grant}
						<div class="border border-slate-200 dark:border-slate-700 rounded-lg p-3">
							<div class="flex items-start justify-between">
								<div class="min-w-0 flex-1">
									<div class="flex items-center gap-1 mb-0.5">
										<p class="text-xs font-mono text-slate-700 dark:text-slate-300 truncate">{grant.GrantId}</p>
										<button onclick={() => copyId(grant.GrantId)} class="p-0.5 text-slate-400 hover:text-slate-600 shrink-0" title="Copy Grant ID"><Copy class="w-3 h-3" /></button>
									</div>
									<p class="text-xs text-slate-500 mt-0.5">Principal: {grant.GranteePrincipal}</p>
									<p class="text-xs text-slate-400 mt-0.5">Ops: {grant.Operations?.join(', ')}</p>
								</div>
								<button onclick={() => revokeGrant(grant.GrantId)} class="px-2 py-1 bg-red-100 dark:bg-red-900/30 text-red-600 dark:text-red-400 rounded text-xs hover:bg-red-200 shrink-0 ml-2">Revoke</button>
							</div>
						</div>
					{/each}
				</div>
			{:else}
				<p class="text-sm text-slate-400 mb-4">No grants.</p>
			{/if}
			<div class="space-y-3 border-t border-slate-200 dark:border-slate-700 pt-4">
				<h3 class="text-sm font-semibold text-slate-700 dark:text-slate-300">Create Grant</h3>
				<div><label class="block text-xs font-medium text-slate-500 mb-1">Grantee Principal (ARN)</label><input type="text" bind:value={newGrantPrincipal} placeholder="arn:aws:iam::123456789012:role/my-role" class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm" /></div>
				<div><label class="block text-xs font-medium text-slate-500 mb-1">Operations (comma-separated)</label><input type="text" bind:value={newGrantOps} placeholder="Decrypt,Encrypt" class="w-full px-3 py-2 border rounded-lg bg-slate-50 dark:bg-slate-700 text-slate-900 dark:text-white text-sm" /></div>
				<button onclick={createGrant} disabled={creatingGrant || !newGrantPrincipal} class="w-full px-4 py-2 bg-orange-600 text-white rounded-lg hover:bg-orange-700 text-sm font-medium">{creatingGrant ? 'Creating...' : 'Create Grant'}</button>
			</div>
			<div class="flex justify-end mt-4"><button onclick={() => showGrantModal = false} class="px-4 py-2 text-slate-500">Done</button></div>
		</div>
	</div>
{/if}

