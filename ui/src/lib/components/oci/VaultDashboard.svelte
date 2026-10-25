<script lang="ts">
	import { onMount } from 'svelte';
	import { Eye, EyeOff, KeyRound, RefreshCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { copyText, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		listKeys,
		listKeyVersions,
		listSecrets,
		listSecretVersions,
		listVaults,
		revealSecretVersion,
		shortOcid,
		type KeyInfo,
		type KeyVersionInfo,
		type RevealedSecret,
		type SecretInfo,
		type SecretVersionInfo,
		type VaultInfo
	} from '#lib/ocivault.ts';

	const TABS = [
		{ id: 'secrets', label: 'Secrets' },
		{ id: 'keys', label: 'Keys' }
	];

	let vaults = $state<VaultInfo[]>([]);
	let loading = $state(true);
	let error = $state('');
	let vaultId = $state('');
	let tab = $state('secrets');
	let secrets = $state<SecretInfo[]>([]);
	let keys = $state<KeyInfo[]>([]);
	let childLoading = $state(false);
	let childError = $state('');
	let secretId = $state('');
	let secretVersions = $state<SecretVersionInfo[]>([]);
	let keyId = $state('');
	let keyVersions = $state<KeyVersionInfo[]>([]);
	let revealed = $state<Record<number, RevealedSecret>>({});

	async function load() {
		try {
			vaults = await listVaults();
			error = '';
			if (!vaults.some((v) => v.id === vaultId)) vaultId = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	async function loadChildren() {
		if (!vaultId) return;
		childLoading = true;
		childError = '';
		secretId = '';
		keyId = '';
		revealed = {};
		try {
			[secrets, keys] = await Promise.all([listSecrets(vaultId), listKeys(vaultId)]);
		} catch (e) {
			childError = describeGcpError(e);
			secrets = [];
			keys = [];
		} finally {
			childLoading = false;
		}
	}

	function selectVault(row: ResourceRow) {
		vaultId = vaultId === row.id ? '' : row.id;
		tab = 'secrets';
		void loadChildren();
	}

	async function selectSecret(row: ResourceRow) {
		revealed = {};
		secretId = secretId === row.id ? '' : row.id;
		secretVersions = [];
		if (!secretId) return;
		try {
			secretVersions = await listSecretVersions(secretId);
			childError = '';
		} catch (e) {
			childError = describeGcpError(e);
		}
	}

	async function selectKey(row: ResourceRow) {
		keyId = keyId === row.id ? '' : row.id;
		keyVersions = [];
		if (!keyId) return;
		try {
			keyVersions = await listKeyVersions(vaultId, keyId);
			childError = '';
		} catch (e) {
			childError = describeGcpError(e);
		}
	}

	async function toggleReveal(n: number) {
		if (n in revealed) {
			const { [n]: _drop, ...rest } = revealed;
			revealed = rest;
			return;
		}
		try {
			revealed = { ...revealed, [n]: await revealSecretVersion(secretId, n) };
			childError = '';
		} catch (e) {
			childError = describeGcpError(e);
		}
	}

	const vaultRows = $derived(vaults.map((v) => ({ id: v.id, cells: [v.displayName, v.state, v.vaultType, shortOcid(v.compartmentId), v.timeCreated], raw: v.raw })));
	const secretRows = $derived(secrets.map((s) => ({ id: s.id, cells: [s.name, s.state, s.description, s.timeCreated], raw: s.raw })));
	const keyRows = $derived(keys.map((k) => ({ id: k.id, cells: [k.displayName, k.state, k.algorithm, k.protectionMode, k.timeCreated], raw: k.raw })));
	const cell = 'px-4 py-2';

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="oci-vault-dashboard">
	<PageHeader icon={KeyRound} title="Vault" description="Oracle Cloud Vault: vaults, master encryption keys and secrets. Secret contents stay hidden until you reveal them." color="red" onRefresh={load} />
	<GcpResourceTable testId="oci-vault" noun="vaults" columns={['Vault', 'State', 'Type', 'Compartment', 'Created']} rows={vaultRows} {loading} {error} selected={vaultId} onRetry={load} onSelect={selectVault} />

	{#if vaultId}
		<div class="space-y-3" data-testid="oci-vault-detail">
			<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="red" />
			{#if childError}<p class="text-sm text-red-600" role="alert" data-testid="oci-vault-error">{childError}</p>{/if}
			{#if childLoading}
				<div class="flex justify-center py-6"><RefreshCw class="h-5 w-5 animate-spin text-gray-400" /></div>
			{:else if tab === 'secrets'}
				<GcpResourceTable testId="oci-secret" noun="secrets" columns={['Secret', 'State', 'Description', 'Created']} rows={secretRows} loading={false} error="" selected={secretId} onRetry={loadChildren} onSelect={selectSecret} />
				{#if secretId}
					<table class="w-full text-sm" data-testid="oci-secret-versions">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Version</th><th class={cell}>Stages</th><th class={cell}>Created</th><th class={cell}>Expires</th><th class="{cell} text-right">Content</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each secretVersions as v (v.versionNumber)}
								<tr data-testid="oci-secret-version">
									<td class="{cell} text-xs">{v.versionNumber}</td>
									<td class="{cell} text-xs">{v.stages.join(', ') || '—'}</td>
									<td class="{cell} text-xs">{v.timeCreated}</td>
									<td class="{cell} text-xs">{v.timeOfExpiry || '—'}</td>
									<td class="{cell} text-right text-xs">
										{#if v.versionNumber in revealed}
											{@const r = revealed[v.versionNumber]}
											<code class="font-mono break-all" data-testid="oci-secret-value">{r.text ?? `base64:${r.base64}`}</code>
											<button class={GCP_MINI} onclick={() => copyText(r.text ?? r.base64)}>Copy</button>
										{:else}
											<span class="font-mono text-gray-400" data-testid="oci-secret-masked">••••••••</span>
										{/if}
										<button class={GCP_MINI} onclick={() => toggleReveal(v.versionNumber)} aria-label="{v.versionNumber in revealed ? 'Hide' : 'Reveal'} version {v.versionNumber}" data-testid="oci-secret-reveal">
											{#if v.versionNumber in revealed}<EyeOff class="inline h-3.5 w-3.5" /> Hide{:else}<Eye class="inline h-3.5 w-3.5" /> Reveal{/if}
										</button>
									</td>
								</tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{:else}
				<GcpResourceTable testId="oci-key" noun="keys" columns={['Key', 'State', 'Algorithm', 'Protection', 'Created']} rows={keyRows} loading={false} error="" selected={keyId} onRetry={loadChildren} onSelect={selectKey} />
				{#if keyId}
					<table class="w-full text-sm" data-testid="oci-key-versions">
						<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Version</th><th class={cell}>State</th><th class={cell}>Origin</th><th class={cell}>Created</th></tr></thead>
						<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
							{#each keyVersions as v (v.id)}
								<tr data-testid="oci-key-version"><td class="{cell} font-mono text-xs break-all">{shortOcid(v.id)}</td><td class="{cell} text-xs">{v.state}</td><td class="{cell} text-xs">{v.origin}</td><td class="{cell} text-xs">{v.timeCreated}</td></tr>
							{/each}
						</tbody>
					</table>
				{/if}
			{/if}
		</div>
	{/if}
</div>
