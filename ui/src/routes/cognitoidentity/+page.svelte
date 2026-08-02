<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getCognitoIdentityClient } from '$lib/aws-client';
	import {
		ListIdentityPoolsCommand,
		DescribeIdentityPoolCommand,
		CreateIdentityPoolCommand,
		DeleteIdentityPoolCommand,
		GetIdentityPoolRolesCommand,
		SetIdentityPoolRolesCommand,
		GetPrincipalTagAttributeMapCommand,
		LookupDeveloperIdentityCommand,
		MergeDeveloperIdentitiesCommand,
		SetPrincipalTagAttributeMapCommand,
		ListIdentitiesCommand,
		UnlinkIdentityCommand,
		UnlinkDeveloperIdentityCommand,
		GetIdCommand,
		GetCredentialsForIdentityCommand,
		GetOpenIdTokenCommand,
		GetOpenIdTokenForDeveloperIdentityCommand,
		DeleteIdentitiesCommand,
		DescribeIdentityCommand,
		TagResourceCommand,
		UntagResourceCommand,
		ListTagsForResourceCommand,
		type IdentityPoolShortDescription,
		type IdentityPool,
		type IdentityDescription
	} from '@aws-sdk/client-cognito-identity';
	import { toast } from 'svelte-sonner';
	import {
		Users,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		CheckCircle,
		XCircle,
		KeyRound,
		Tag,
		LinkIcon,
		Link2Off,
		List,
		Copy,
		ShieldCheck,
		Fingerprint,
		Key
	} from 'lucide-svelte';

	const cognitoId = regionalClient(getCognitoIdentityClient);

	// The mock backend extends IdentityPool with the ARN field.
	interface IdentityPoolWithArn extends IdentityPool {
		IdentityPoolArn?: string;
	}

	function poolArn(pool: IdentityPool | null): string {
		return (pool as IdentityPoolWithArn)?.IdentityPoolArn ?? '';
	}

	let loading = $state(false);
	let identityPools = $state<IdentityPoolShortDescription[]>([]);
	let selectedPool = $state<IdentityPool | null>(null);
	let poolRoles = $state<Record<string, string>>({});
	let loadingDetail = $state(false);
	let searchQuery = $state('');

	// identities within selected pool
	let poolIdentities = $state<IdentityDescription[]>([]);
	let loadingIdentities = $state(false);

	// unlink identity
	let unlinkIdentityId = $state('');
	let unlinkProvider = $state('');
	let unlinkToken = $state('');
	let unlinkingIdentity = $state(false);

	// unlink developer identity
	let unlinkDevIdentityId = $state('');
	let unlinkDevProviderName = $state('');
	let unlinkDevUserIdentifier = $state('');
	let unlinkingDevIdentity = $state(false);

	// set identity pool roles
	let setRoleAuth = $state('');
	let setRoleUnauth = $state('');
	let settingRoles = $state(false);

	// get id / credentials / token quick-test
	let getIdLogins = $state('');
	let getIdResult = $state('');
	let gettingId = $state(false);
	let credIdentityId = $state('');
	let credResult = $state<Record<string, string> | null>(null);
	let gettingCredentials = $state(false);
	let oidcIdentityId = $state('');
	let oidcTokenResult = $state('');
	let gettingOidcToken = $state(false);

	// GetOpenIdTokenForDeveloperIdentity
	let devTokenLogins = $state('');
	let devTokenDuration = $state(0);
	let devTokenResult = $state('');
	let gettingDevToken = $state(false);

	// TagResource / UntagResource
	let tagKey = $state('');
	let tagValue = $state('');
	let taggingResource = $state(false);
	let untagKey = $state('');
	let untaggingResource = $state(false);
	let resourceTags = $state<Record<string, string> | null>(null);
	let loadingResourceTags = $state(false);

	// describe identity inline
	let describeIdentityId = $state('');
	let describeIdentityResult = $state<{ logins: string[]; created: string; modified: string } | null>(null);
	let describingIdentity = $state(false);

	// merge result
	let mergeResultIdentityId = $state('');

	let showCreateModal = $state(false);
	let creating = $state(false);
	let newPoolName = $state('');
	let newPoolUnauthenticated = $state(false);
	let newPoolCognitoProvider = $state('');
	let newPoolDeveloperProviderName = $state('');
	let principalProviderName = $state('');
	let principalUseDefaults = $state(true);
	let principalTagsText = $state('');
	let savingPrincipalTags = $state(false);
	let lookingUpDeveloperIdentity = $state(false);
	let mergingDeveloperIdentities = $state(false);
	let lookupIdentityID = $state('');
	let lookupDeveloperUserIdentifier = $state('');
	let developerProviderName = $state('');
	let lookupResultIdentityID = $state('');
	let lookupResultDeveloperUsers = $state<string[]>([]);
	let mergeSourceUserIdentifier = $state('');
	let mergeDestinationUserIdentifier = $state('');

	const filteredPools = $derived(
		identityPools.filter(
			(p) =>
				(p.IdentityPoolName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(p.IdentityPoolId ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	async function loadPools() {
		loading = true;
		try {
			const res = await cognitoId().send(new ListIdentityPoolsCommand({ MaxResults: 60 }));
			identityPools = res.IdentityPools ?? [];
		} catch (e) {
			toast.error(`Failed to load identity pools: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewPool(pool: IdentityPoolShortDescription) {
		if (!pool.IdentityPoolId) return;
		loadingDetail = true;
		selectedPool = null;
		poolRoles = {};
		poolIdentities = [];
		principalProviderName = '';
		principalUseDefaults = true;
		principalTagsText = '';
		lookupIdentityID = '';
		lookupDeveloperUserIdentifier = '';
		developerProviderName = '';
		lookupResultIdentityID = '';
		lookupResultDeveloperUsers = [];
		mergeSourceUserIdentifier = '';
		mergeDestinationUserIdentifier = '';
		mergeResultIdentityId = '';
		unlinkIdentityId = '';
		unlinkProvider = '';
		unlinkToken = '';
		unlinkDevIdentityId = '';
		unlinkDevProviderName = '';
		unlinkDevUserIdentifier = '';
		setRoleAuth = '';
		setRoleUnauth = '';
		getIdLogins = '';
		getIdResult = '';
		credIdentityId = '';
		credResult = null;
		oidcIdentityId = '';
		oidcTokenResult = '';
		devTokenLogins = '';
		devTokenDuration = 0;
		devTokenResult = '';
		tagKey = '';
		tagValue = '';
		untagKey = '';
		resourceTags = null;
		describeIdentityResult = null;
		describeIdentityId = '';
		try {
			const [detailRes, rolesRes] = await Promise.all([
				cognitoId().send(new DescribeIdentityPoolCommand({ IdentityPoolId: pool.IdentityPoolId })),
				cognitoId().send(new GetIdentityPoolRolesCommand({ IdentityPoolId: pool.IdentityPoolId }))
			]);
			selectedPool = detailRes ?? null;
			poolRoles = rolesRes.Roles ?? {};
		} catch (e) {
			toast.error(`Failed to load pool details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function deletePool(pool: IdentityPoolShortDescription) {
		if (!pool.IdentityPoolId || !await confirmDestructive({ title: 'Delete Identity Pool', message: `Delete identity pool "${pool.IdentityPoolName}"? All identities associated with this pool will be removed.` })) return;
		try {
			await cognitoId().send(
				new DeleteIdentityPoolCommand({ IdentityPoolId: pool.IdentityPoolId })
			);
			toast.success(`Identity pool deleted`);
			if (selectedPool?.IdentityPoolId === pool.IdentityPoolId) selectedPool = null;
			await loadPools();
		} catch (e) {
			toast.error(`Failed to delete pool: ${e}`);
		}
	}

	async function createPool() {
		if (!newPoolName.trim()) return;
		creating = true;
		try {
			await cognitoId().send(
				new CreateIdentityPoolCommand({
					IdentityPoolName: newPoolName.trim(),
					AllowUnauthenticatedIdentities: newPoolUnauthenticated,
					DeveloperProviderName: newPoolDeveloperProviderName.trim() || undefined,
					CognitoIdentityProviders:
						newPoolCognitoProvider.trim()
							? [{ ProviderName: newPoolCognitoProvider.trim() }]
							: undefined
				})
			);
			toast.success(`Identity pool "${newPoolName}" created`);
			showCreateModal = false;
			newPoolName = '';
			newPoolCognitoProvider = '';
			newPoolDeveloperProviderName = '';
			newPoolUnauthenticated = false;
			await loadPools();
		} catch (e) {
			toast.error(`Failed to create pool: ${e}`);
		} finally {
			creating = false;
		}
	}

	function parsePrincipalTags(text: string): Record<string, string> {
		const trimmed = text.trim();
		if (!trimmed) return {};

		try {
			const parsed = JSON.parse(trimmed);
			if (parsed && typeof parsed === 'object') {
				return Object.fromEntries(
					Object.entries(parsed).map(([k, v]) => [k, String(v ?? '')])
				);
			}
		} catch {
			// Fallback to line-based "key=value" parsing.
		}

		return Object.fromEntries(
			trimmed
				.split('\n')
				.map((line) => line.trim())
				.filter(Boolean)
				.map((line) => {
					const index = line.indexOf('=');
					if (index < 0) return [line, ''];
					return [line.slice(0, index).trim(), line.slice(index + 1).trim()];
				})
				.filter(([k]) => k.length > 0)
		);
	}

	async function loadPrincipalTagMap() {
		if (!selectedPool?.IdentityPoolId || !principalProviderName.trim()) return;
		try {
			const out = await cognitoId().send(
				new GetPrincipalTagAttributeMapCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					IdentityProviderName: principalProviderName.trim()
				})
			);
			principalUseDefaults = out.UseDefaults ?? true;
			principalTagsText = JSON.stringify(out.PrincipalTags ?? {}, null, 2);
			toast.success('Principal tag map loaded');
		} catch (e) {
			toast.error(`Failed to load principal tag map: ${e}`);
		}
	}

	async function savePrincipalTagMap() {
		if (!selectedPool?.IdentityPoolId || !principalProviderName.trim()) return;
		savingPrincipalTags = true;
		try {
			await cognitoId().send(
				new SetPrincipalTagAttributeMapCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					IdentityProviderName: principalProviderName.trim(),
					UseDefaults: principalUseDefaults,
					PrincipalTags: parsePrincipalTags(principalTagsText)
				})
			);
			toast.success('Principal tag map saved');
		} catch (e) {
			toast.error(`Failed to save principal tag map: ${e}`);
		} finally {
			savingPrincipalTags = false;
		}
	}

	async function runDeveloperIdentityLookup() {
		if (!selectedPool?.IdentityPoolId || !developerProviderName.trim()) return;
		if (!lookupIdentityID.trim() && !lookupDeveloperUserIdentifier.trim()) return;

		lookingUpDeveloperIdentity = true;
		lookupResultIdentityID = '';
		lookupResultDeveloperUsers = [];
		try {
			const out = await cognitoId().send(
				new LookupDeveloperIdentityCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					IdentityId: lookupIdentityID.trim() || undefined,
					DeveloperUserIdentifier: lookupDeveloperUserIdentifier.trim() || undefined
				})
			);
			lookupResultIdentityID = out.IdentityId ?? '';
			lookupResultDeveloperUsers = out.DeveloperUserIdentifierList ?? [];
			toast.success('Developer identity lookup complete');
		} catch (e) {
			toast.error(`Failed to lookup developer identity: ${e}`);
		} finally {
			lookingUpDeveloperIdentity = false;
		}
	}

	async function mergeDeveloperIdentityUsers() {
		if (!selectedPool?.IdentityPoolId || !developerProviderName.trim()) return;
		if (!mergeSourceUserIdentifier.trim() || !mergeDestinationUserIdentifier.trim()) return;

		mergingDeveloperIdentities = true;
		mergeResultIdentityId = '';
		try {
			const out = await cognitoId().send(
				new MergeDeveloperIdentitiesCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					SourceUserIdentifier: mergeSourceUserIdentifier.trim(),
					DestinationUserIdentifier: mergeDestinationUserIdentifier.trim(),
					DeveloperProviderName: developerProviderName.trim()
				})
			);
			mergeResultIdentityId = out.IdentityId ?? '';
			toast.success(`Developer identities merged → ${mergeResultIdentityId}`);
		} catch (e) {
			toast.error(`Failed to merge developer identities: ${e}`);
		} finally {
			mergingDeveloperIdentities = false;
		}
	}

	async function loadPoolIdentities() {
		if (!selectedPool?.IdentityPoolId) return;
		loadingIdentities = true;
		try {
			const out = await cognitoId().send(
				new ListIdentitiesCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					MaxResults: 20,
					HideDisabled: false
				})
			);
			poolIdentities = out.Identities ?? [];
			toast.success(`Loaded ${poolIdentities.length} identit${poolIdentities.length === 1 ? 'y' : 'ies'}`);
		} catch (e) {
			toast.error(`Failed to load identities: ${e}`);
		} finally {
			loadingIdentities = false;
		}
	}

	async function runUnlinkIdentity() {
		if (!unlinkIdentityId.trim() || !unlinkProvider.trim()) return;
		unlinkingIdentity = true;
		try {
			await cognitoId().send(
				new UnlinkIdentityCommand({
					IdentityId: unlinkIdentityId.trim(),
					Logins: { [unlinkProvider.trim()]: unlinkToken.trim() },
					LoginsToRemove: [unlinkProvider.trim()]
				})
			);
			toast.success(`Provider "${unlinkProvider}" unlinked from identity`);
			unlinkIdentityId = '';
			unlinkProvider = '';
			unlinkToken = '';
		} catch (e) {
			toast.error(`Failed to unlink identity: ${e}`);
		} finally {
			unlinkingIdentity = false;
		}
	}

	async function runUnlinkDeveloperIdentity() {
		if (!selectedPool?.IdentityPoolId || !unlinkDevIdentityId.trim() || !unlinkDevProviderName.trim() || !unlinkDevUserIdentifier.trim()) return;
		unlinkingDevIdentity = true;
		try {
			await cognitoId().send(
				new UnlinkDeveloperIdentityCommand({
					IdentityId: unlinkDevIdentityId.trim(),
					IdentityPoolId: selectedPool.IdentityPoolId,
					DeveloperProviderName: unlinkDevProviderName.trim(),
					DeveloperUserIdentifier: unlinkDevUserIdentifier.trim()
				})
			);
			toast.success(`Developer identity unlinked`);
			unlinkDevIdentityId = '';
			unlinkDevProviderName = '';
			unlinkDevUserIdentifier = '';
		} catch (e) {
			toast.error(`Failed to unlink developer identity: ${e}`);
		} finally {
			unlinkingDevIdentity = false;
		}
	}

	async function setIdentityPoolRoles() {
		if (!selectedPool?.IdentityPoolId) return;
		if (!setRoleAuth.trim() && !setRoleUnauth.trim()) return;
		settingRoles = true;
		try {
			const roles: Record<string, string> = {};
			if (setRoleAuth.trim()) roles['authenticated'] = setRoleAuth.trim();
			if (setRoleUnauth.trim()) roles['unauthenticated'] = setRoleUnauth.trim();
			await cognitoId().send(
				new SetIdentityPoolRolesCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					Roles: roles
				})
			);
			toast.success('Identity pool roles saved');
			// Refresh the displayed roles.
			const rolesRes = await cognitoId().send(
				new GetIdentityPoolRolesCommand({ IdentityPoolId: selectedPool.IdentityPoolId })
			);
			poolRoles = rolesRes.Roles ?? {};
			setRoleAuth = '';
			setRoleUnauth = '';
		} catch (e) {
			toast.error(`Failed to set roles: ${e}`);
		} finally {
			settingRoles = false;
		}
	}

	async function quickGetId() {
		if (!selectedPool?.IdentityPoolId) return;
		gettingId = true;
		getIdResult = '';
		try {
			let logins: Record<string, string> | undefined;
			if (getIdLogins.trim()) {
				try {
					logins = JSON.parse(getIdLogins.trim());
				} catch {
					toast.error('Logins must be valid JSON e.g. {"provider": "token"}');
					return;
				}
			}
			const out = await cognitoId().send(
				new GetIdCommand({ IdentityPoolId: selectedPool.IdentityPoolId, Logins: logins })
			);
			getIdResult = out.IdentityId ?? '';
			toast.success('Got identity ID');
		} catch (e) {
			toast.error(`GetId failed: ${e}`);
		} finally {
			gettingId = false;
		}
	}

	async function quickGetCredentials() {
		if (!credIdentityId.trim()) return;
		gettingCredentials = true;
		credResult = null;
		try {
			const out = await cognitoId().send(
				new GetCredentialsForIdentityCommand({ IdentityId: credIdentityId.trim() })
			);
			credResult = {
				AccessKeyId: out.Credentials?.AccessKeyId ?? '',
				SecretKey: out.Credentials?.SecretKey ?? '',
				SessionToken: out.Credentials?.SessionToken ?? '',
				Expiration: out.Credentials?.Expiration?.toISOString() ?? ''
			};
			toast.success('Got temporary credentials');
		} catch (e) {
			toast.error(`GetCredentialsForIdentity failed: ${e}`);
		} finally {
			gettingCredentials = false;
		}
	}

	async function quickGetOpenIdToken() {
		if (!oidcIdentityId.trim()) return;
		gettingOidcToken = true;
		oidcTokenResult = '';
		try {
			const out = await cognitoId().send(
				new GetOpenIdTokenCommand({ IdentityId: oidcIdentityId.trim() })
			);
			oidcTokenResult = out.Token ?? '';
			toast.success('Got OpenID token');
		} catch (e) {
			toast.error(`GetOpenIdToken failed: ${e}`);
		} finally {
			gettingOidcToken = false;
		}
	}

	async function quickGetDevToken() {
		if (!selectedPool?.IdentityPoolId) return;
		gettingDevToken = true;
		devTokenResult = '';
		try {
			let logins: Record<string, string> | undefined;
			if (devTokenLogins.trim()) {
				try {
					logins = JSON.parse(devTokenLogins.trim());
				} catch {
					toast.error('Logins must be valid JSON');
					return;
				}
			}
			const out = await cognitoId().send(
				new GetOpenIdTokenForDeveloperIdentityCommand({
					IdentityPoolId: selectedPool.IdentityPoolId,
					Logins: logins ?? {},
					TokenDuration: devTokenDuration > 0 ? devTokenDuration : undefined
				})
			);
			devTokenResult = out.Token ?? '';
			toast.success('Got developer OpenID token');
		} catch (e) {
			toast.error(`GetOpenIdTokenForDeveloperIdentity failed: ${e}`);
		} finally {
			gettingDevToken = false;
		}
	}

	async function loadResourceTags() {
		if (!selectedPool?.IdentityPoolId) return;
		// ARN is exposed via DescribeIdentityPool but not in the short description.
		// Use the ARN from the detailed pool object.
		const arn = poolArn(selectedPool) ?? '';
		if (!arn) { toast.error('Pool ARN not available'); return; }
		loadingResourceTags = true;
		try {
			const out = await cognitoId().send(new ListTagsForResourceCommand({ ResourceArn: arn }));
			resourceTags = out.Tags ?? {};
		} catch (e) {
			toast.error(`ListTagsForResource failed: ${e}`);
		} finally {
			loadingResourceTags = false;
		}
	}

	async function addTag() {
		if (!tagKey.trim() || !tagValue.trim()) return;
		const arn = poolArn(selectedPool) ?? '';
		if (!arn) { toast.error('Pool ARN not available'); return; }
		taggingResource = true;
		try {
			await cognitoId().send(new TagResourceCommand({ ResourceArn: arn, Tags: { [tagKey.trim()]: tagValue.trim() } }));
			toast.success(`Tag "${tagKey}" added`);
			tagKey = '';
			tagValue = '';
			await loadResourceTags();
		} catch (e) {
			toast.error(`TagResource failed: ${e}`);
		} finally {
			taggingResource = false;
		}
	}

	async function removeTag() {
		if (!untagKey.trim()) return;
		const arn = poolArn(selectedPool) ?? '';
		if (!arn) { toast.error('Pool ARN not available'); return; }
		untaggingResource = true;
		try {
			await cognitoId().send(new UntagResourceCommand({ ResourceArn: arn, TagKeys: [untagKey.trim()] }));
			toast.success(`Tag "${untagKey}" removed`);
			untagKey = '';
			await loadResourceTags();
		} catch (e) {
			toast.error(`UntagResource failed: ${e}`);
		} finally {
			untaggingResource = false;
		}
	}

	async function deleteIdentity(identityId: string) {
		if (!await confirmDestructive({ title: 'Delete Identity', message: `Delete identity ${identityId}?` })) return;
		try {
			await cognitoId().send(new DeleteIdentitiesCommand({ IdentityIdsToDelete: [identityId] }));
			toast.success('Identity deleted');
			poolIdentities = poolIdentities.filter((i) => i.IdentityId !== identityId);
		} catch (e) {
			toast.error(`Failed to delete identity: ${e}`);
		}
	}

	async function describeIdentity(identityId: string) {
		describingIdentity = true;
		describeIdentityId = identityId;
		describeIdentityResult = null;
		try {
			const out = await cognitoId().send(
				new DescribeIdentityCommand({ IdentityId: identityId })
			);
			describeIdentityResult = {
				logins: out.Logins ?? [],
				created: out.CreationDate ? new Date(out.CreationDate).toLocaleString() : '',
				modified: out.LastModifiedDate ? new Date(out.LastModifiedDate).toLocaleString() : ''
			};
		} catch (e) {
			toast.error(`DescribeIdentity failed: ${e}`);
		} finally {
			describingIdentity = false;
		}
	}

	function copyToClipboard(text: string, label: string) {
		navigator.clipboard.writeText(text).then(
			() => toast.success(`${label} copied to clipboard`),
			() => toast.error('Failed to copy to clipboard')
		);
	}

	// selectedPool and its detail state (roles, identities) are region-scoped:
	// a pool ID from the old region is meaningless in the new one.
	onRegionChange(() => {
		selectedPool = null;
		identityPools = [];
		poolRoles = {};
		poolIdentities = [];
		void loadPools();
	});
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg">
				<Users class="h-6 w-6 text-pink-600 dark:text-pink-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Cognito Identity</h1>
				<p class="text-slate-600 dark:text-slate-300">Federated identity pools for AWS service access</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button
				onclick={loadPools}
				class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300"
				title="Refresh"
			>
				<RefreshCw class="h-4 w-4" />
			</button>
			<button
				onclick={() => (showCreateModal = true)}
				class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2"
			>
				<Plus class="h-4 w-4" />
				Create Pool
			</button>
		</div>
	</div>

	<!-- Stats cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-pink-100 dark:bg-pink-900/30 rounded-lg">
				<Users class="w-5 h-5 text-pink-600 dark:text-pink-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{identityPools.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Identity Pools</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<KeyRound class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{filteredPools.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Filtered Results</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg">
				<List class="w-5 h-5 text-green-600 dark:text-green-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{poolIdentities.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Loaded Identities</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg">
				<Eye class="w-5 h-5 text-purple-600 dark:text-purple-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{selectedPool ? 1 : 0}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Selected</p>
			</div>
		</div>
	</div>

	<!-- Filter -->
	<div class="flex items-center gap-3">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-slate-400" />
			<input
				type="text"
				placeholder="Search identity pools..."
				bind:value={searchQuery}
				class="w-full rounded-lg border border-slate-200 dark:border-slate-700 bg-white dark:bg-slate-900 pl-9 pr-4 py-2 text-sm text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
			/>
		</div>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if filteredPools.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Users class="h-12 w-12 mb-3 opacity-30" />
			<p>No identity pools found</p>
			<p class="text-sm">Create a pool to enable federated identities</p>
		</div>
	{:else}
		<div class="rounded-lg border overflow-hidden">
			<table class="w-full text-sm">
				<thead class="bg-muted/50">
					<tr>
						<th class="px-4 py-3 text-left font-medium">Pool Name</th>
						<th class="px-4 py-3 text-left font-medium">Pool ID</th>
						<th class="px-4 py-3 text-right font-medium">Actions</th>
					</tr>
				</thead>
				<tbody class="divide-y">
					{#each filteredPools as pool}
						<tr class="hover:bg-muted/30 cursor-pointer" onclick={() => viewPool(pool)}>
							<td class="px-4 py-3 font-medium">{pool.IdentityPoolName}</td>
							<td class="px-4 py-3 font-mono text-xs text-muted-foreground">{pool.IdentityPoolId}</td>
							<td class="px-4 py-3 text-right flex justify-end gap-1">
								<button
									onclick={(e) => { e.stopPropagation(); viewPool(pool); }}
									class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
									title="View details"
								>
									<Eye class="h-4 w-4" />
								</button>
								<button
									onclick={(e) => { e.stopPropagation(); deletePool(pool); }}
									class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
									title="Delete pool"
								>
									<Trash2 class="h-4 w-4" />
								</button>
							</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>
	{/if}

	<!-- Pool Detail -->
	{#if loadingDetail}
		<div class="flex justify-center py-4">
			<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
		</div>
	{:else if selectedPool}
		<div class="rounded-lg border p-5 space-y-4">
			<div class="flex items-center justify-between">
				<h3 class="font-semibold">{selectedPool.IdentityPoolName}</h3>
				<button onclick={() => { selectedPool = null; poolIdentities = []; }} class="text-xs text-muted-foreground hover:text-foreground">
					Close
				</button>
			</div>

			<div class="grid grid-cols-2 gap-3 text-sm">
				<div>
					<p class="text-muted-foreground">Pool ID</p>
					<div class="flex items-center gap-1">
						<p class="font-mono text-xs">{selectedPool.IdentityPoolId}</p>
						<button onclick={() => copyToClipboard(selectedPool?.IdentityPoolId ?? '', 'Pool ID')} class="ml-1 text-muted-foreground hover:text-foreground" title="Copy Pool ID">
							<Copy class="h-3 w-3" />
						</button>
					</div>
				</div>
				{#if poolArn(selectedPool)}
					<div>
						<p class="text-muted-foreground">Pool ARN</p>
						<div class="flex items-center gap-1">
							<p class="font-mono text-xs truncate max-w-xs">{poolArn(selectedPool)}</p>
							<button onclick={() => copyToClipboard(poolArn(selectedPool), 'Pool ARN')} class="ml-1 text-muted-foreground hover:text-foreground flex-shrink-0" title="Copy ARN">
								<Copy class="h-3 w-3" />
							</button>
						</div>
					</div>
				{/if}
				<div>
					<p class="text-muted-foreground">Unauthenticated Identities</p>
					<div class="flex items-center gap-1">
						{#if selectedPool.AllowUnauthenticatedIdentities}
							<CheckCircle class="h-4 w-4 text-green-500" />
							<span>Allowed</span>
						{:else}
							<XCircle class="h-4 w-4 text-muted-foreground" />
							<span>Not allowed</span>
						{/if}
					</div>
				</div>
				{#if selectedPool.DeveloperProviderName}
					<div>
						<p class="text-muted-foreground">Developer Provider Name</p>
						<p class="font-mono text-xs">{selectedPool.DeveloperProviderName}</p>
					</div>
				{/if}
				{#if selectedPool.AllowClassicFlow}
					<div>
						<p class="text-muted-foreground">Classic Flow</p>
						<div class="flex items-center gap-1">
							<CheckCircle class="h-4 w-4 text-green-500" />
							<span>Enabled</span>
						</div>
					</div>
				{/if}
			</div>

			{#if (selectedPool.CognitoIdentityProviders ?? []).length > 0}
				<div>
					<p class="text-sm font-medium mb-2">Cognito User Pool Providers</p>
					<div class="divide-y rounded border overflow-hidden">
						{#each selectedPool.CognitoIdentityProviders ?? [] as provider}
							<div class="px-3 py-2 text-xs">
								<span class="font-mono">{provider.ProviderName}</span>
								{#if provider.ClientId}
									<span class="ml-2 text-muted-foreground">Client: {provider.ClientId}</span>
								{/if}
							</div>
						{/each}
					</div>
				</div>
			{/if}

			{#if Object.keys(poolRoles).length > 0}
				<div>
					<p class="text-sm font-medium mb-2 flex items-center gap-1">
						<KeyRound class="h-4 w-4" />
						IAM Roles
					</p>
					<div class="space-y-1">
						{#each Object.entries(poolRoles) as [roleType, roleArn]}
							<div class="text-xs">
								<span class="font-medium capitalize">{roleType}:</span>
								<span class="ml-1 font-mono text-muted-foreground truncate block">{roleArn}</span>
							</div>
						{/each}
					</div>
				</div>
			{/if}

			<!-- Set Identity Pool Roles -->
			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium flex items-center gap-1">
					<ShieldCheck class="h-4 w-4" />
					Set Identity Pool Roles
				</p>
				<p class="text-xs text-muted-foreground">Assign IAM role ARNs for authenticated and/or unauthenticated access. Leave a field blank to preserve the existing role.</p>
				<input
					type="text"
					bind:value={setRoleAuth}
					placeholder="Authenticated role ARN (optional)"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<input
					type="text"
					bind:value={setRoleUnauth}
					placeholder="Unauthenticated role ARN (optional)"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<button
					onclick={setIdentityPoolRoles}
					disabled={settingRoles || (!setRoleAuth.trim() && !setRoleUnauth.trim())}
					class="rounded-md bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{settingRoles ? 'Saving...' : 'Save Roles'}
				</button>
			</div>

			<!-- Tag Management -->
			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium flex items-center gap-1">
					<Tag class="h-4 w-4" />
					Resource Tags (via ARN)
				</p>
				{#if poolArn(selectedPool)}
					<div class="flex gap-2">
						<button
							onclick={loadResourceTags}
							disabled={loadingResourceTags}
							class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
						>
							{loadingResourceTags ? 'Loading...' : 'List Tags'}
						</button>
					</div>
					{#if resourceTags !== null}
						<div class="rounded border bg-muted/20 p-2 text-xs space-y-1">
							{#if Object.keys(resourceTags).length === 0}
								<p class="text-muted-foreground italic">No tags</p>
							{:else}
								{#each Object.entries(resourceTags) as [k, v]}
									<div class="flex items-center justify-between">
										<span><span class="font-semibold">{k}</span> = {v}</span>
									</div>
								{/each}
							{/if}
						</div>
					{/if}
					<div class="grid grid-cols-2 gap-2">
						<input type="text" bind:value={tagKey} placeholder="Tag key" class="rounded-md border bg-background px-2 py-1 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-primary" />
						<input type="text" bind:value={tagValue} placeholder="Tag value" class="rounded-md border bg-background px-2 py-1 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-primary" />
					</div>
					<button
						onclick={addTag}
						disabled={taggingResource || !tagKey.trim() || !tagValue.trim()}
						class="rounded-md bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
					>
						{taggingResource ? 'Tagging...' : 'Add / Update Tag'}
					</button>
					<div class="flex gap-2">
						<input type="text" bind:value={untagKey} placeholder="Tag key to remove" class="flex-1 rounded-md border bg-background px-2 py-1 text-xs font-mono focus:outline-none focus:ring-1 focus:ring-primary" />
						<button
							onclick={removeTag}
							disabled={untaggingResource || !untagKey.trim()}
							class="rounded-md bg-destructive px-3 py-1 text-xs text-destructive-foreground hover:bg-destructive/90 disabled:opacity-50"
						>
							{untaggingResource ? 'Removing...' : 'Remove Tag'}
						</button>
					</div>
				{:else}
					<p class="text-xs text-muted-foreground">Pool ARN not available — re-describe the pool or use a newer SDK response.</p>
				{/if}
			</div>

			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium">Principal Tag Attribute Map</p>
				<input
					type="text"
					bind:value={principalProviderName}
					placeholder="Identity provider name"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<label class="flex items-center gap-2 text-xs">
					<input type="checkbox" bind:checked={principalUseDefaults} class="rounded" />
					Use defaults
				</label>
				<textarea
					bind:value={principalTagsText}
					rows="4"
					placeholder="JSON map or key=value lines"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				></textarea>
				<div class="flex gap-2">
					<button
						onclick={loadPrincipalTagMap}
						disabled={!principalProviderName.trim()}
						class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
					>
						Load
					</button>
					<button
						onclick={savePrincipalTagMap}
						disabled={savingPrincipalTags || !principalProviderName.trim()}
						class="rounded-md bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
					>
						{savingPrincipalTags ? 'Saving...' : 'Save'}
					</button>
				</div>
			</div>

			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium">Developer Identity Tools</p>
				<input
					type="text"
					bind:value={developerProviderName}
					placeholder="Developer provider name"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
					<input
						type="text"
						bind:value={lookupIdentityID}
						placeholder="Lookup by identity ID"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<input
						type="text"
						bind:value={lookupDeveloperUserIdentifier}
						placeholder="Lookup by developer user"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<button
					onclick={runDeveloperIdentityLookup}
					disabled={!developerProviderName.trim() || (!lookupIdentityID.trim() && !lookupDeveloperUserIdentifier.trim()) || lookingUpDeveloperIdentity}
					class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
				>
					{lookingUpDeveloperIdentity ? 'Looking up...' : 'Lookup'}
				</button>

				{#if lookupResultIdentityID}
					<div class="rounded border bg-muted/30 p-2 text-xs">
						<p><span class="font-medium">Identity:</span> <span class="font-mono">{lookupResultIdentityID}</span></p>
						<p><span class="font-medium">Developer users:</span> {lookupResultDeveloperUsers.join(', ') || 'none'}</p>
					</div>
				{/if}

				<div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
					<input
						type="text"
						bind:value={mergeSourceUserIdentifier}
						placeholder="Source user ID"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<input
						type="text"
						bind:value={mergeDestinationUserIdentifier}
						placeholder="Destination user ID"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<button
					onclick={mergeDeveloperIdentityUsers}
					disabled={!developerProviderName.trim() || !mergeSourceUserIdentifier.trim() || !mergeDestinationUserIdentifier.trim() || mergingDeveloperIdentities}
					class="rounded-md bg-primary px-3 py-1 text-xs text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{mergingDeveloperIdentities ? 'Merging...' : 'Merge Developer Users'}
				</button>

				{#if mergeResultIdentityId}
					<div class="rounded border bg-green-50 dark:bg-green-950/20 p-2 text-xs">
						<p class="font-medium text-green-700 dark:text-green-400">Merged → destination identity:</p>
						<div class="flex items-center gap-1 mt-1">
							<span class="font-mono">{mergeResultIdentityId}</span>
							<button onclick={() => copyToClipboard(mergeResultIdentityId, 'Identity ID')} class="text-muted-foreground hover:text-foreground" title="Copy">
								<Copy class="h-3 w-3" />
							</button>
						</div>
					</div>
				{/if}

				<hr class="border-border my-1" />
				<p class="text-xs font-medium">Get OpenID Token for Developer Identity</p>
				<textarea
					bind:value={devTokenLogins}
					rows="2"
					placeholder="Logins JSON e.g. developer.example.com → user-id"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				></textarea>
				<input
					type="number"
					bind:value={devTokenDuration}
					min="0"
					max="86400"
					placeholder="Token duration in seconds (0 = default)"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<button
					onclick={quickGetDevToken}
					disabled={gettingDevToken || !developerProviderName.trim()}
					class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
				>
					{gettingDevToken ? 'Getting...' : 'Get Developer Token'}
				</button>
				{#if devTokenResult}
					<div class="rounded border bg-muted/30 p-2 text-xs">
						<div class="flex items-center gap-1">
							<span class="font-mono break-all">{devTokenResult}</span>
							<button onclick={() => copyToClipboard(devTokenResult, 'Token')} class="flex-shrink-0 text-muted-foreground hover:text-foreground" title="Copy token">
								<Copy class="h-3 w-3" />
							</button>
						</div>
					</div>
				{/if}
			</div>

			<!-- Quick Test: GetId / GetCredentialsForIdentity / GetOpenIdToken -->
			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium flex items-center gap-1">
					<Fingerprint class="h-4 w-4" />
					Identity Quick Test
				</p>
				<p class="text-xs text-muted-foreground">Register or resolve an identity, then exchange it for temporary credentials or an OpenID token.</p>

				<p class="text-xs font-medium">GetId</p>
				<textarea
					bind:value={getIdLogins}
					rows="2"
					placeholder="Logins JSON (optional) e.g. provider → token"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				></textarea>
				<div class="flex gap-2">
					<button
						onclick={quickGetId}
						disabled={gettingId}
						class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
					>
						{gettingId ? 'Getting...' : 'Get Identity ID'}
					</button>
					{#if getIdResult}
						<div class="flex items-center gap-1 rounded border bg-muted/30 px-2 py-1 text-xs font-mono flex-1 min-w-0">
							<span class="truncate">{getIdResult}</span>
							<button onclick={() => copyToClipboard(getIdResult, 'Identity ID')} class="flex-shrink-0 text-muted-foreground hover:text-foreground" title="Copy ID">
								<Copy class="h-3 w-3" />
							</button>
						</div>
					{/if}
				</div>

				<p class="text-xs font-medium mt-2">GetCredentialsForIdentity</p>
				<div class="flex gap-2">
					<input
						type="text"
						bind:value={credIdentityId}
						placeholder="Identity ID"
						class="flex-1 rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<button
						onclick={quickGetCredentials}
						disabled={gettingCredentials || !credIdentityId.trim()}
						class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
					>
						{gettingCredentials ? 'Getting...' : 'Get Credentials'}
					</button>
				</div>
				{#if credResult}
					<div class="rounded border bg-muted/20 p-2 text-xs space-y-1">
						{#each Object.entries(credResult) as [k, v]}
							<div class="flex items-center gap-1">
								<span class="font-medium min-w-20">{k}:</span>
								<span class="font-mono truncate flex-1">{v}</span>
								{#if v}
									<button onclick={() => copyToClipboard(v, k)} class="flex-shrink-0 text-muted-foreground hover:text-foreground" title="Copy">
										<Copy class="h-3 w-3" />
									</button>
								{/if}
							</div>
						{/each}
					</div>
				{/if}

				<p class="text-xs font-medium mt-2">GetOpenIdToken</p>
				<div class="flex gap-2">
					<input
						type="text"
						bind:value={oidcIdentityId}
						placeholder="Identity ID"
						class="flex-1 rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<button
						onclick={quickGetOpenIdToken}
						disabled={gettingOidcToken || !oidcIdentityId.trim()}
						class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50"
					>
						{gettingOidcToken ? 'Getting...' : 'Get Token'}
					</button>
				</div>
				{#if oidcTokenResult}
					<div class="rounded border bg-muted/30 p-2 text-xs flex items-center gap-1">
						<span class="font-mono break-all flex-1">{oidcTokenResult}</span>
						<button onclick={() => copyToClipboard(oidcTokenResult, 'Token')} class="flex-shrink-0 text-muted-foreground hover:text-foreground" title="Copy token">
							<Copy class="h-3 w-3" />
						</button>
					</div>
				{/if}
			</div>

			<!-- Supported Login Providers -->
			{#if Object.keys(selectedPool.SupportedLoginProviders ?? {}).length > 0}
				<div>
					<p class="text-sm font-medium mb-2 flex items-center gap-1">
						<LinkIcon class="h-4 w-4" />
						Supported Login Providers
					</p>
					<div class="divide-y rounded border overflow-hidden">
						{#each Object.entries(selectedPool.SupportedLoginProviders ?? {}) as [provider, appId]}
							<div class="px-3 py-2 text-xs flex items-center justify-between">
								<span class="font-mono">{provider}</span>
								<span class="text-muted-foreground ml-2 truncate">{appId}</span>
							</div>
						{/each}
					</div>
				</div>
			{/if}

			<!-- Pool Tags -->
			{#if Object.keys(selectedPool.IdentityPoolTags ?? {}).length > 0}
				<div>
					<p class="text-sm font-medium mb-2 flex items-center gap-1">
						<Tag class="h-4 w-4" />
						Tags
					</p>
					<div class="flex flex-wrap gap-2">
						{#each Object.entries(selectedPool.IdentityPoolTags ?? {}) as [k, v]}
							<span class="inline-flex items-center gap-1 rounded-full border px-2.5 py-0.5 text-xs font-medium">
								<span class="font-semibold">{k}</span>
								<span class="text-muted-foreground">= {v}</span>
							</span>
						{/each}
					</div>
				</div>
			{/if}

			<!-- Identity List -->
			<div class="space-y-2 rounded-lg border p-3">
				<div class="flex items-center justify-between">
					<p class="text-sm font-medium flex items-center gap-1">
						<List class="h-4 w-4" />
						Identities in Pool
					</p>
					<button
						onclick={loadPoolIdentities}
						disabled={loadingIdentities}
						class="rounded-md border px-3 py-1 text-xs hover:bg-accent disabled:opacity-50 flex items-center gap-1"
					>
						{#if loadingIdentities}
							<RefreshCw class="h-3 w-3 animate-spin" />
						{/if}
						{loadingIdentities ? 'Loading...' : 'Load Identities'}
					</button>
				</div>
				{#if poolIdentities.length > 0}
					<div class="divide-y rounded border overflow-hidden mt-2">
						{#each poolIdentities as identity}
							<div class="px-3 py-2 text-xs">
								<div class="flex items-center justify-between gap-2">
									<div class="flex items-center gap-1 min-w-0">
										<span class="font-mono text-muted-foreground truncate">{identity.IdentityId}</span>
										<button onclick={() => copyToClipboard(identity.IdentityId ?? '', 'Identity ID')} class="flex-shrink-0 text-muted-foreground hover:text-foreground" title="Copy ID">
											<Copy class="h-3 w-3" />
										</button>
									</div>
									<div class="flex gap-1 flex-shrink-0">
										<button
											onclick={() => describeIdentity(identity.IdentityId ?? '')}
											class="rounded p-0.5 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
											title="Describe identity"
										>
											<Eye class="h-3 w-3" />
										</button>
										<button
											onclick={() => deleteIdentity(identity.IdentityId ?? '')}
											class="rounded p-0.5 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
											title="Delete identity"
										>
											<Trash2 class="h-3 w-3" />
										</button>
									</div>
								</div>
								{#if (identity.Logins ?? []).length > 0}
									<div class="mt-1 flex flex-wrap gap-1">
										{#each identity.Logins ?? [] as login}
											<span class="rounded-full bg-muted px-2 py-0.5 text-xs">{login}</span>
										{/each}
									</div>
								{/if}
								{#if describeIdentityId === identity.IdentityId && describeIdentityResult}
									<div class="mt-2 rounded border bg-muted/20 p-2 space-y-1">
										{#if describingIdentity}
											<RefreshCw class="h-3 w-3 animate-spin" />
										{:else}
											<p><span class="font-medium">Created:</span> {describeIdentityResult.created}</p>
											<p><span class="font-medium">Modified:</span> {describeIdentityResult.modified}</p>
											{#if describeIdentityResult.logins.length > 0}
												<p class="font-medium">Linked providers:</p>
												<div class="flex flex-wrap gap-1">
													{#each describeIdentityResult.logins as p}
														<span class="rounded-full bg-primary/10 px-2 py-0.5 text-xs">{p}</span>
													{/each}
												</div>
											{:else}
												<p class="text-muted-foreground italic">No linked providers</p>
											{/if}
										{/if}
									</div>
								{/if}
							</div>
						{/each}
					</div>
				{:else if !loadingIdentities}
					<p class="text-xs text-muted-foreground">Click "Load Identities" to list identities in this pool (up to 20).</p>
				{/if}
			</div>

			<!-- Unlink Identity -->
			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium flex items-center gap-1">
					<Link2Off class="h-4 w-4" />
					Unlink Identity Provider
				</p>
				<p class="text-xs text-muted-foreground">Remove a specific login provider from an identity after validating the login token.</p>
				<input
					type="text"
					bind:value={unlinkIdentityId}
					placeholder="Identity ID (e.g. us-east-1:...)"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
					<input
						type="text"
						bind:value={unlinkProvider}
						placeholder="Provider (e.g. accounts.google.com)"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<input
						type="text"
						bind:value={unlinkToken}
						placeholder="Login token"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<button
					onclick={runUnlinkIdentity}
					disabled={!unlinkIdentityId.trim() || !unlinkProvider.trim() || unlinkingIdentity}
					class="rounded-md bg-destructive px-3 py-1 text-xs text-destructive-foreground hover:bg-destructive/90 disabled:opacity-50"
				>
					{unlinkingIdentity ? 'Unlinking...' : 'Unlink Provider'}
				</button>
			</div>

			<!-- Unlink Developer Identity -->
			<div class="space-y-2 rounded-lg border p-3">
				<p class="text-sm font-medium flex items-center gap-1">
					<Link2Off class="h-4 w-4" />
					Unlink Developer Identity
				</p>
				<p class="text-xs text-muted-foreground">Remove a developer-provider association from an identity.</p>
				<input
					type="text"
					bind:value={unlinkDevIdentityId}
					placeholder="Identity ID (e.g. us-east-1:...)"
					class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
				/>
				<div class="grid grid-cols-1 gap-2 sm:grid-cols-2">
					<input
						type="text"
						bind:value={unlinkDevProviderName}
						placeholder="Developer provider name"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
					<input
						type="text"
						bind:value={unlinkDevUserIdentifier}
						placeholder="Developer user identifier"
						class="w-full rounded-md border bg-background px-3 py-2 text-xs font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<button
					onclick={runUnlinkDeveloperIdentity}
					disabled={!unlinkDevIdentityId.trim() || !unlinkDevProviderName.trim() || !unlinkDevUserIdentifier.trim() || unlinkingDevIdentity}
					class="rounded-md bg-destructive px-3 py-1 text-xs text-destructive-foreground hover:bg-destructive/90 disabled:opacity-50"
				>
					{unlinkingDevIdentity ? 'Unlinking...' : 'Unlink Developer Identity'}
				</button>
			</div>
		</div>
	{/if}
</div>

<!-- Create Pool Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create Identity Pool</h2>
			<div class="space-y-3">
				<div>
					<label for="pool-name" class="block text-sm font-medium mb-1">Pool Name *</label>
					<input
						id="pool-name"
						type="text"
						bind:value={newPoolName}
						placeholder="my-identity-pool"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="pool-provider" class="block text-sm font-medium mb-1"
						>Cognito User Pool Provider (optional)</label
					>
					<input
						id="pool-provider"
						type="text"
						bind:value={newPoolCognitoProvider}
						placeholder="cognito-idp.us-east-1.amazonaws.com/us-east-1_xxxxx"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary font-mono"
					/>
				</div>
				<div>
					<label for="pool-dev-provider" class="block text-sm font-medium mb-1"
						>Developer Provider Name (optional)</label
					>
					<input
						id="pool-dev-provider"
						type="text"
						bind:value={newPoolDeveloperProviderName}
						placeholder="developer.myapp.com"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary font-mono"
					/>
				</div>
				<div class="flex items-center gap-2">
					<input
						id="pool-unauth"
						type="checkbox"
						bind:checked={newPoolUnauthenticated}
						class="rounded"
					/>
					<label for="pool-unauth" class="text-sm">Allow unauthenticated identities</label>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createPool}
					disabled={creating || !newPoolName.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Pool'}
				</button>
			</div>
		</div>
	</div>
{/if}
