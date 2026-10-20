<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { KeyRound, Plus, RefreshCw, Copy, X, Star, Trash2, Pencil, Search } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { metadataToRows, rowsToMap, validateRows, type MetaRow } from '#lib/gcpstorage.ts';
	import {
		ALGORITHMS,
		PROTECTION_LEVELS,
		PURPOSES,
		createKey,
		createKeyRing,
		asymmetricDecryptText,
		createVersion,
		deleteKey,
		deleteVersion,
		decryptText,
		encryptText,
		getKey,
		getPublicKey,
		listKeyRings,
		listKeys,
		listRetired,
		listVersions,
		macSignText,
		macVerifyText,
		readStoredKmsLocation,
		readStoredKmsProject,
		rotationDaysInput,
		rsaEncryptText,
		updateKey,
		rotationFields,
		setPrimary,
		signText,
		validateKmsId,
		validateLabelKey,
		versionAction,
		writeStoredKmsScope,
		type KeyInfo,
		type KeyRef,
		type KeyRingInfo,
		type RetiredInfo,
		type Purpose,
		type VersionAction,
		type VersionInfo
	} from '#lib/gcpkms.ts';

	let project = $state(readStoredKmsProject());
	let location = $state(readStoredKmsLocation());
	let projectInput = $state(readStoredKmsProject());
	let locationInput = $state(readStoredKmsLocation());
	let rings = $state<KeyRingInfo[]>([]);
	let ring = $state<KeyRingInfo | null>(null);
	let keys = $state<KeyInfo[]>([]);
	let current = $state<KeyInfo | null>(null);
	let versions = $state<VersionInfo[]>([]);
	let loading = $state(true);
	let loadError = $state('');
	let busy = $state('');

	let ringModal = $state<Modal | null>(null);
	let fRingId = $state('');
	let ringTouched = $state(false);
	const ringIdError = $derived(validateKmsId(fRingId.trim()));

	let keyModal = $state<Modal | null>(null);
	let fKeyId = $state('');
	let fPurpose = $state<Purpose>('ENCRYPT_DECRYPT');
	let fAlgorithm = $state('GOOGLE_SYMMETRIC_ENCRYPTION');
	let fLevel = $state('SOFTWARE');
	let fRotation = $state('');
	let fLabels = $state<MetaRow[]>([]);
	let keyTouched = $state(false);
	const keyIdError = $derived(validateKmsId(fKeyId.trim()));

	let keyFilter = $state('');
	let keyOrder = $state('');
	let retired = $state<RetiredInfo[]>([]);
	let showRetired = $state(false);

	let editModal = $state<Modal | null>(null);
	let eLabels = $state<MetaRow[]>([]);
	let eRotation = $state('');

	let tryVersion = $state('');
	let plaintext = $state('');
	let aad = $state('');
	let cipherIn = $state('');
	let cipherOut = $state('');
	let decrypted = $state<{ text: string | null; base64: string } | null>(null);
	let signInput = $state('');
	let signature = $state('');
	let macInput = $state('');
	let macOut = $state('');
	let macCheck = $state('');
	let macResult = $state<boolean | null>(null);
	let rsaPlain = $state('');
	let rsaCipher = $state('');
	let rsaDecrypted = $state<{ text: string | null; base64: string } | null>(null);
	let publicKey = $state<{ pem: string; algorithm: string } | null>(null);

	const describe = describeGcpError;
	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-amber-500';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const btn = 'rounded-md bg-amber-600 px-3 py-1 text-sm text-white hover:bg-amber-700 disabled:opacity-50';
	const mini = 'rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const cell = 'px-4 py-3';
	const out = 'whitespace-pre-wrap break-all rounded-md bg-gray-50 dark:bg-slate-800/60 p-2 font-mono text-xs';

	const enabledVersions = $derived(versions.filter((v) => v.state === 'ENABLED'));
	const ref = $derived<KeyRef | null>(ring && current ? { project, location, ring: ring.id, key: current.id } : null);

	async function loadRings() {
		loading = true;
		loadError = '';
		try {
			rings = (await listKeyRings(project, location)).toSorted((a, b) => a.id.localeCompare(b.id));
			if (ring && !rings.some((r) => r.name === ring?.name)) {
				ring = null;
				keys = [];
				current = null;
			}
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load key rings: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function loadKeys() {
		const r = ring;
		if (!r) return;
		try {
			keys = (await listKeys(project, location, r.id, fetch, { filter: keyFilter, orderBy: keyOrder })).toSorted((a, b) => a.id.localeCompare(b.id));
			if (current && !keys.some((k) => k.name === current?.name)) current = null;
		} catch (e) {
			toast.error(`Failed to load keys: ${describe(e)}`);
		}
	}

	async function run(label: string, action: () => Promise<void>, success: string, failure: string): Promise<boolean> {
		busy = label;
		try {
			await action();
			if (success) toast.success(success);
			return true;
		} catch (e) {
			toast.error(`${failure}: ${describe(e)}`);
			return false;
		} finally {
			busy = '';
		}
	}

	async function selectRing(r: KeyRingInfo) {
		ring = r;
		current = null;
		keys = [];
		keyFilter = '';
		keyOrder = '';
		await loadKeys();
	}

	async function loadRetired() {
		try {
			retired = (await listRetired(project, location)).toSorted((a, b) => b.deleteTime.localeCompare(a.deleteTime));
		} catch (e) {
			toast.error(`Failed to load retired resources: ${describe(e)}`);
		}
	}

	async function toggleRetired() {
		showRetired = !showRetired;
		if (showRetired) await loadRetired();
	}

	async function applyFilter(event: Event) {
		event.preventDefault();
		await loadKeys();
	}

	function resetTry() {
		plaintext = '';
		aad = '';
		cipherIn = '';
		cipherOut = '';
		decrypted = null;
		signInput = '';
		signature = '';
		macInput = '';
		macOut = '';
		macCheck = '';
		macResult = null;
		rsaPlain = '';
		rsaCipher = '';
		rsaDecrypted = null;
		publicKey = null;
	}

	async function loadDetail(k: KeyInfo) {
		const r = ring;
		if (!r) return;
		try {
			const [fresh, vs] = await Promise.all([getKey(project, location, r.id, k.id), listVersions(project, location, r.id, k.id)]);
			current = fresh;
			versions = vs.toSorted((a, b) => Number(b.id) - Number(a.id));
			const pick = versions.find((v) => v.id === fresh.primaryId && v.state === 'ENABLED') ?? versions.find((v) => v.state === 'ENABLED');
			if (!versions.some((v) => v.id === tryVersion && v.state === 'ENABLED')) tryVersion = pick?.id ?? '';
		} catch (e) {
			toast.error(`Failed to read key: ${describe(e)}`);
		}
	}

	async function selectKey(k: KeyInfo) {
		current = k;
		versions = [];
		tryVersion = '';
		resetTry();
		await loadDetail(k);
	}

	function openRing() {
		fRingId = '';
		ringTouched = false;
		ringModal?.open();
	}

	async function submitRing(event: Event) {
		event.preventDefault();
		ringTouched = true;
		if (ringIdError) return;
		const id = fRingId.trim();
		const ok = await run('create-ring', async () => { await createKeyRing(project, location, id); }, `Created key ring ${id}`, `Failed to create ${id}`);
		if (ok) {
			ringModal?.close('created');
			await loadRings();
			const made = rings.find((r) => r.id === id);
			if (made) await selectRing(made);
		}
	}

	function openKey() {
		fKeyId = '';
		fPurpose = 'ENCRYPT_DECRYPT';
		fAlgorithm = ALGORITHMS.ENCRYPT_DECRYPT[0];
		fLevel = 'SOFTWARE';
		fRotation = '';
		fLabels = [];
		keyTouched = false;
		keyModal?.open();
	}

	function onPurposeChange() {
		fAlgorithm = ALGORITHMS[fPurpose][0];
	}

	async function submitKey(event: Event) {
		event.preventDefault();
		keyTouched = true;
		const r = ring;
		if (keyIdError || !r) return;
		const bad = validateRows(fLabels, validateLabelKey);
		if (bad) return void toast.error(`Invalid label ${bad}`);
		const rot = rotationFields(fRotation);
		if (rot && 'error' in rot) return void toast.error(rot.error);
		const id = fKeyId.trim();
		const ok = await run('create-key', async () => {
			await createKey(project, location, r.id, id, {
				purpose: fPurpose,
				algorithm: fAlgorithm,
				protectionLevel: fLevel,
				rotationDays: fRotation,
				labels: rowsToMap(fLabels)
			});
		}, `Created key ${id}`, `Failed to create ${id}`);
		if (ok) {
			keyModal?.close('created');
			await loadKeys();
			const made = keys.find((k) => k.id === id);
			if (made) await selectKey(made);
		}
	}

	async function addVersion() {
		const k = current;
		const r = ring;
		if (!k || !r) return;
		const ok = await run('add-version', async () => {
			const v = await createVersion(project, location, r.id, k.id);
			toast.success(`Created version ${v.id}`);
		}, '', 'Failed to create version');
		if (ok) await loadDetail(k);
	}

	async function act(v: VersionInfo, action: VersionAction) {
		const k = current;
		const r = ring;
		if (!k || !r) return;
		if (action === 'destroy' && !(await confirmDestructive({ title: 'Destroy version', message: `Schedule version ${v.id} of "${k.id}" for destruction? It can be restored until the scheduled time.` }))) return;
		const ok = await run(`${action}-${v.id}`, async () => { await versionAction(project, location, r.id, k.id, v.id, action); }, `Version ${v.id}: ${action} done`, `Failed to ${action} version ${v.id}`);
		if (ok) await loadDetail(k);
	}

	async function makePrimary(v: VersionInfo) {
		const k = current;
		const r = ring;
		if (!k || !r) return;
		const ok = await run(`primary-${v.id}`, async () => { await setPrimary(project, location, r.id, k.id, v.id); }, `Version ${v.id} is now primary`, `Failed to set primary ${v.id}`);
		if (ok) {
			await loadKeys();
			await loadDetail(k);
		}
	}

	async function doEncrypt() {
		const r = ref;
		if (!r || !plaintext) return;
		await run('encrypt', async () => {
			const res = await encryptText(r, plaintext, aad);
			cipherOut = res.ciphertext;
			cipherIn = res.ciphertext;
			decrypted = null;
		}, 'Encrypted', 'Encrypt failed');
	}

	async function doDecrypt() {
		const r = ref;
		if (!r || !cipherIn.trim()) return;
		await run('decrypt', async () => { decrypted = await decryptText(r, cipherIn, aad); }, 'Decrypted', 'Decrypt failed');
	}

	async function doSign() {
		const r = ref;
		if (!r || !signInput || !tryVersion) return;
		await run('sign', async () => { signature = await signText(r, tryVersion, signInput); }, 'Signed', 'Sign failed');
	}

	async function doMac() {
		const r = ref;
		if (!r || !macInput || !tryVersion) return;
		await run('mac', async () => { macOut = await macSignText(r, tryVersion, macInput); }, 'MAC computed', 'MAC sign failed');
	}

	async function doMacVerify() {
		const r = ref;
		if (!r || !macInput || !tryVersion || !macCheck.trim()) return;
		await run('mac-verify', async () => { macResult = await macVerifyText(r, tryVersion, macInput, macCheck); }, '', 'MAC verify failed');
	}

	async function doRsaEncrypt() {
		const r = ref;
		if (!r || !rsaPlain || !tryVersion) return;
		await run('rsa-encrypt', async () => {
			const pk = await getPublicKey(r, tryVersion);
			publicKey = pk;
			rsaCipher = await rsaEncryptText(pk.pem, pk.algorithm, rsaPlain);
			rsaDecrypted = null;
		}, 'Encrypted in the browser with the public key', 'Encrypt failed');
	}

	async function doRsaDecrypt() {
		const r = ref;
		if (!r || !rsaCipher.trim() || !tryVersion) return;
		await run('rsa-decrypt', async () => { rsaDecrypted = await asymmetricDecryptText(r, tryVersion, rsaCipher); }, 'Decrypted', 'Decrypt failed');
	}

	function openEdit() {
		const k = current;
		if (!k) return;
		eLabels = metadataToRows(k.labels);
		eRotation = rotationDaysInput(k.rotationPeriod);
		editModal?.open();
	}

	async function submitEdit(event: Event) {
		event.preventDefault();
		const r = ref;
		const k = current;
		if (!r || !k) return;
		const bad = validateRows(eLabels, validateLabelKey);
		if (bad) return void toast.error(`Invalid label ${bad}`);
		const rot = rotationFields(eRotation);
		if (rot && 'error' in rot) return void toast.error(rot.error);
		const ok = await run('update-key', async () => { await updateKey(r, { labels: rowsToMap(eLabels), rotationDays: eRotation }); }, `Updated ${k.id}`, `Failed to update ${k.id}`);
		if (ok) {
			editModal?.close('updated');
			await loadKeys();
			await loadDetail(k);
		}
	}

	async function removeVersion(v: VersionInfo) {
		const r = ref;
		const k = current;
		if (!r || !k) return;
		if (!(await confirmDestructive({ title: 'Delete version', message: `Permanently delete destroyed version ${v.id} of "${k.id}"? Its id is never reused.` }))) return;
		const ok = await run(`delete-${v.id}`, async () => { await deleteVersion(r, v.id); }, `Deleted version ${v.id}`, `Failed to delete version ${v.id}`);
		if (ok) await loadDetail(k);
	}

	async function removeKey() {
		const r = ref;
		const k = current;
		if (!r || !k) return;
		if (!(await confirmDestructive({ title: 'Delete key', message: `Permanently delete key "${k.id}"? Its name can never be reused and it moves to Retired.` }))) return;
		const ok = await run('delete-key', async () => { await deleteKey(r); }, `Deleted key ${k.id}`, `Failed to delete ${k.id}`);
		if (ok) {
			current = null;
			await loadKeys();
			if (showRetired) await loadRetired();
		}
	}

	async function doPublicKey() {
		const r = ref;
		if (!r || !tryVersion) return;
		await run('pubkey', async () => { publicKey = await getPublicKey(r, tryVersion); }, '', 'Get public key failed');
	}

	async function copy(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	function applyScope(event: Event) {
		event.preventDefault();
		const p = projectInput.trim();
		const l = locationInput.trim();
		if (!p || !l) return;
		project = p;
		location = l;
		writeStoredKmsScope(p, l);
		ring = null;
		current = null;
		keys = [];
		void loadRings();
	}

	const stateClass = (st: string) =>
		st === 'ENABLED' ? 'text-green-700 dark:text-green-400' : st === 'DISABLED' ? 'text-amber-700 dark:text-amber-400' : 'text-gray-500 dark:text-gray-400';

	onMount(loadRings);
</script>

<div class="space-y-6 pb-20" data-testid="kms-dashboard">
	<PageHeader icon={KeyRound} title="Cloud KMS" description="Google Cloud KMS: key rings, keys and versions" color="amber" onRefresh={async () => { await loadRings(); if (ring) await loadKeys(); if (current) await loadDetail(current); }}>
		{#snippet actions()}
			<button class="flex items-center gap-2 rounded-lg bg-amber-600 px-3 py-2 text-sm text-white hover:bg-amber-700 disabled:opacity-50" onclick={openRing} disabled={loading || Boolean(loadError)} data-testid="kms-new-ring">
				<Plus class="h-4 w-4" /> New key ring
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyScope}>
		<label for="kms-project">Project</label>
		<input id="kms-project" class="w-48 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="kms-project" />
		<label for="kms-location">Location</label>
		<input id="kms-location" class="w-32 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={locationInput} data-testid="kms-location" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || !locationInput.trim() || (projectInput.trim() === project && locationInput.trim() === location)}>Switch</button>
		<span>Key rings are listed per project and location.</span>
	</form>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="kms-error">
			<p class="font-medium">Could not reach Cloud KMS</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadRings}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<div class="space-y-6">
				<section aria-label="Key rings" class="space-y-2">
					<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Key rings</h2>
					{#if loading}
						<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
					{:else if rings.length === 0}
						<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="kms-no-rings">No key rings in {location}</p>
					{:else}
						<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
							{#each rings as r (r.name)}
								<li class={ring?.name === r.name ? 'bg-amber-50 dark:bg-amber-950/30' : ''} data-testid="kms-ring-row">
									<button class="w-full min-w-0 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectRing(r)}>
										<span class="block truncate">{r.id}</span>
									</button>
								</li>
							{/each}
						</ul>
					{/if}
				</section>

				{#if ring}
					<section aria-label="Keys" class="space-y-2">
						<div class="flex items-center justify-between">
							<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Keys in {ring.id}</h2>
							<button class={mini} onclick={openKey} data-testid="kms-new-key"><Plus class="inline h-3.5 w-3.5" /> New key</button>
						</div>
						<form class="flex gap-1" onsubmit={applyFilter}>
							<input class="{input} !py-1 text-xs" bind:value={keyFilter} placeholder="filter, e.g. labels.env=dev" aria-label="Filter keys" data-testid="kms-key-filter" />
							<select class="{input} !w-auto !py-1 text-xs" bind:value={keyOrder} onchange={loadKeys} aria-label="Order keys" data-testid="kms-key-order">
								<option value="">name</option>
								<option value="name desc">name desc</option>
								<option value="create_time desc">newest</option>
								<option value="create_time">oldest</option>
							</select>
							<button type="submit" class={mini} aria-label="Apply filter" data-testid="kms-key-filter-apply"><Search class="h-3.5 w-3.5" /></button>
						</form>
						{#if keys.length === 0}
							<p class="py-6 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="kms-no-keys">{keyFilter.trim() ? 'No keys match the filter' : 'No keys in this key ring'}</p>
						{:else}
							<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
								{#each keys as k (k.name)}
									<li class={current?.name === k.name ? 'bg-amber-50 dark:bg-amber-950/30' : ''} data-testid="kms-key-row">
										<button class="w-full min-w-0 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => selectKey(k)}>
											<span class="block truncate">{k.id}</span>
											<span class="block truncate text-[11px] font-normal text-gray-500 dark:text-gray-400">{k.purpose} · {k.algorithm}</span>
										</button>
									</li>
								{/each}
							</ul>
						{/if}
					</section>
				{/if}

				<section aria-label="Retired" class="space-y-2">
					<button class="text-sm font-semibold text-gray-700 dark:text-gray-300 hover:underline" onclick={toggleRetired} data-testid="kms-retired-toggle">
						Retired {showRetired ? `(${retired.length})` : ''}
					</button>
					{#if showRetired}
						{#if retired.length === 0}
							<p class="text-xs text-gray-500 dark:text-gray-400" data-testid="kms-no-retired">No retired resources</p>
						{:else}
							<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
								{#each retired as r (r.name)}
									<li class="px-3 py-2 text-xs" data-testid="kms-retired-row">
										<span class="block truncate font-medium text-gray-900 dark:text-gray-100">{r.originalId}</span>
										<span class="block truncate text-gray-500 dark:text-gray-400">{r.resourceType} · {r.deleteTime}</span>
										<span class="block break-all font-mono text-[10px] text-gray-500 dark:text-gray-400">{r.original}</span>
									</li>
								{/each}
							</ul>
						{/if}
					{/if}
				</section>
			</div>

			<section aria-label="Key details" class="min-w-0">
				{#if !current}
					<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="kms-select-hint">{ring ? 'Select a key to manage its versions' : 'Select a key ring to see its keys'}</p>
				{:else}
					{@const k = current}
					<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-5" data-testid="kms-detail">
						<div class="flex items-start justify-between gap-4">
							<div class="min-w-0"><h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{k.id}</h3><p class="font-mono text-xs text-gray-500 dark:text-gray-400 break-all">{k.name}</p></div>
							<div class="flex items-center gap-1">
								<button class={mini} onclick={openEdit} data-testid="kms-edit-key"><Pencil class="inline h-3.5 w-3.5" /> Edit</button>
								<button class="{mini} text-red-600" disabled={busy === 'delete-key'} onclick={removeKey} data-testid="kms-delete-key"><Trash2 class="inline h-3.5 w-3.5" /> Delete</button>
								<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (current = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
							</div>
						</div>
						<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
							<dt class={dt}>Purpose</dt><dd class={dd} data-testid="kms-purpose">{k.purpose}</dd>
							<dt class={dt}>Algorithm</dt><dd class={dd}>{k.algorithm || '—'}</dd>
							<dt class={dt}>Protection level</dt><dd class={dd}>{k.protectionLevel || '—'}</dd>
							<dt class={dt}>Primary version</dt><dd class={dd} data-testid="kms-primary">{k.primaryId || '—'}</dd>
							<dt class={dt}>Rotation</dt><dd class={dd}>{k.rotationPeriod ? `every ${k.rotationPeriod}${k.nextRotation ? `, next ${k.nextRotation}` : ''}` : 'none'}</dd>
							<dt class={dt}>Created</dt><dd class={dd}>{k.createTime || '—'}</dd>
							<dt class={dt}>Labels</dt><dd class={dd}>{Object.entries(k.labels).map(([lk, lv]) => `${lk}=${lv}`).join(', ') || '—'}</dd>
						</dl>

						<div class="space-y-2" data-testid="kms-versions">
							<div class="flex items-center justify-between">
								<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Versions ({versions.length})</h4>
								<button class={mini} disabled={busy === 'add-version'} onclick={addVersion} data-testid="kms-add-version"><Plus class="inline h-3.5 w-3.5" /> New version</button>
							</div>
							{#if versions.length === 0}
								<p class="text-xs text-gray-500 dark:text-gray-400">No versions yet</p>
							{:else}
								<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
									<table class="w-full text-sm">
										<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Version</th><th class={cell}>State</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each versions as v (v.id)}
												<tr data-testid="kms-version-row">
													<td class="{cell} font-mono">{v.id}{#if v.id === k.primaryId} <Star class="inline h-3.5 w-3.5 text-amber-500" aria-label="primary" />{/if}</td>
													<td class="{cell} font-medium {stateClass(v.state)}" data-testid="kms-version-state">{v.state}{#if v.destroyTime}<span class="block text-[11px] font-normal text-gray-500 dark:text-gray-400">{v.destroyTime}</span>{/if}</td>
													<td class="{cell} whitespace-nowrap text-xs">{v.createTime}</td>
													<td class="{cell} text-right whitespace-nowrap space-x-1">
														{#if v.state === 'ENABLED'}
															{#if k.purpose === 'ENCRYPT_DECRYPT' && v.id !== k.primaryId}
																<button class={mini} disabled={busy === `primary-${v.id}`} onclick={() => makePrimary(v)} data-testid="kms-primary-{v.id}">Make primary</button>
															{/if}
															<button class={mini} disabled={busy === `disable-${v.id}`} onclick={() => act(v, 'disable')} data-testid="kms-disable-{v.id}">Disable</button>
														{:else if v.state === 'DISABLED'}
															<button class={mini} disabled={busy === `enable-${v.id}`} onclick={() => act(v, 'enable')} data-testid="kms-enable-{v.id}">Enable</button>
														{/if}
														{#if v.state === 'ENABLED' || v.state === 'DISABLED'}
															<button class="{mini} text-red-600" disabled={busy === `destroy-${v.id}`} onclick={() => act(v, 'destroy')} data-testid="kms-destroy-{v.id}">Destroy</button>
														{:else if v.state === 'DESTROY_SCHEDULED'}
															<button class={mini} disabled={busy === `restore-${v.id}`} onclick={() => act(v, 'restore')} data-testid="kms-restore-{v.id}">Restore</button>
														{:else if v.state === 'DESTROYED' && v.id !== k.primaryId}
															<button class="{mini} text-red-600" disabled={busy === `delete-${v.id}`} onclick={() => removeVersion(v)} data-testid="kms-delete-{v.id}">Delete</button>
														{/if}
													</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
						</div>

						<div class="space-y-4 border-t border-gray-200 dark:border-gray-700 pt-4" data-testid="kms-try">
							<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Try it</h4>

							{#if k.purpose === 'ENCRYPT_DECRYPT'}
								<div class="grid gap-6 md:grid-cols-2">
									<div class="space-y-2">
										<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-plaintext">Plaintext</label>
										<textarea id="kms-plaintext" class="{input} font-mono" rows="3" bind:value={plaintext} placeholder="text to encrypt" data-testid="kms-plaintext"></textarea>
										<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-aad">Additional authenticated data (optional)</label>
										<input id="kms-aad" class={input} bind:value={aad} data-testid="kms-aad" />
										<button class={btn} disabled={busy === 'encrypt' || !plaintext} onclick={doEncrypt} data-testid="kms-encrypt">Encrypt</button>
										{#if cipherOut}
											<div class="flex items-start gap-2"><pre class={out + ' flex-1'} data-testid="kms-ciphertext">{cipherOut}</pre><button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy ciphertext" onclick={() => copy(cipherOut)}><Copy class="h-4 w-4" /></button></div>
										{/if}
									</div>
									<div class="space-y-2">
										<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-cipher-in">Ciphertext (base64)</label>
										<textarea id="kms-cipher-in" class="{input} font-mono" rows="3" bind:value={cipherIn} placeholder="paste ciphertext" data-testid="kms-cipher-in"></textarea>
										<button class={btn} disabled={busy === 'decrypt' || !cipherIn.trim()} onclick={doDecrypt} data-testid="kms-decrypt">Decrypt</button>
										{#if decrypted}
											{#if decrypted.text !== null}
												<pre class={out} data-testid="kms-decrypted">{decrypted.text}</pre>
											{:else}
												<p class="text-[11px] text-gray-500 dark:text-gray-400">Binary plaintext (not valid UTF-8)</p>
											{/if}
											<p class="font-mono text-[11px] text-gray-500 dark:text-gray-400 break-all">base64: {decrypted.base64}</p>
										{/if}
									</div>
								</div>
							{:else}
								<div class="flex items-center gap-2 text-sm">
									<label class="text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-try-version">Version</label>
									<select id="kms-try-version" class="rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 text-sm" bind:value={tryVersion} data-testid="kms-try-version">
										{#each enabledVersions as v (v.id)}<option value={v.id}>{v.id}</option>{/each}
									</select>
									{#if enabledVersions.length === 0}<span class="text-xs text-gray-500 dark:text-gray-400">No enabled version</span>{/if}
								</div>
							{/if}

							{#if k.purpose === 'ASYMMETRIC_SIGN'}
								<div class="space-y-2">
									<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-sign-in">Data to sign</label>
									<textarea id="kms-sign-in" class="{input} font-mono" rows="3" bind:value={signInput} placeholder="message" data-testid="kms-sign-input"></textarea>
									<div class="flex gap-2">
										<button class={btn} disabled={busy === 'sign' || !signInput || !tryVersion} onclick={doSign} data-testid="kms-sign">Sign</button>
										<button class={mini} disabled={busy === 'pubkey' || !tryVersion} onclick={doPublicKey} data-testid="kms-get-pubkey">Get public key</button>
									</div>
									{#if signature}
										<p class="text-xs font-medium text-gray-600 dark:text-gray-400">Signature (base64)</p>
										<div class="flex items-start gap-2"><pre class={out + ' flex-1'} data-testid="kms-signature">{signature}</pre><button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy signature" onclick={() => copy(signature)}><Copy class="h-4 w-4" /></button></div>
									{/if}
								</div>
							{:else if k.purpose === 'ASYMMETRIC_DECRYPT'}
								<button class={mini} disabled={busy === 'pubkey' || !tryVersion} onclick={doPublicKey} data-testid="kms-get-pubkey">Get public key</button>
								<div class="space-y-2">
									<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-rsa-plain">Plaintext (encrypted in this browser with the public key)</label>
									<textarea id="kms-rsa-plain" class="{input} font-mono" rows="3" bind:value={rsaPlain} placeholder="text to encrypt" data-testid="kms-rsa-plain"></textarea>
									<button class={btn} disabled={busy === 'rsa-encrypt' || !rsaPlain || !tryVersion} onclick={doRsaEncrypt} data-testid="kms-rsa-encrypt">Encrypt in browser</button>
									<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-rsa-cipher">Ciphertext (base64)</label>
									<textarea id="kms-rsa-cipher" class="{input} font-mono" rows="3" bind:value={rsaCipher} placeholder="encrypt above or paste ciphertext" data-testid="kms-rsa-cipher"></textarea>
									<button class={btn} disabled={busy === 'rsa-decrypt' || !rsaCipher.trim() || !tryVersion} onclick={doRsaDecrypt} data-testid="kms-rsa-decrypt">Decrypt with KMS</button>
									{#if rsaDecrypted}
										{#if rsaDecrypted.text !== null}
											<pre class={out} data-testid="kms-rsa-decrypted">{rsaDecrypted.text}</pre>
										{:else}
											<p class="text-[11px] text-gray-500 dark:text-gray-400">Binary plaintext (not valid UTF-8)</p>
										{/if}
										<p class="font-mono text-[11px] text-gray-500 dark:text-gray-400 break-all">base64: {rsaDecrypted.base64}</p>
									{/if}
								</div>
							{:else if k.purpose === 'MAC'}
								<div class="space-y-2">
									<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-mac-in">Data to MAC</label>
									<textarea id="kms-mac-in" class="{input} font-mono" rows="3" bind:value={macInput} placeholder="message" data-testid="kms-mac-input"></textarea>
									<button class={btn} disabled={busy === 'mac' || !macInput || !tryVersion} onclick={doMac} data-testid="kms-mac-sign">MAC sign</button>
									{#if macOut}
										<p class="text-xs font-medium text-gray-600 dark:text-gray-400">MAC (base64)</p>
										<div class="flex items-start gap-2"><pre class={out + ' flex-1'} data-testid="kms-mac">{macOut}</pre><button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy MAC" onclick={() => copy(macOut)}><Copy class="h-4 w-4" /></button></div>
									<button class={mini} onclick={() => (macCheck = macOut)} data-testid="kms-mac-use">Use for verify</button>
									{/if}
									<label class="block text-xs font-medium text-gray-600 dark:text-gray-400" for="kms-mac-check">MAC to verify (base64, checked against the data above)</label>
									<input id="kms-mac-check" class="{input} font-mono" bind:value={macCheck} oninput={() => (macResult = null)} data-testid="kms-mac-check" />
									<button class={btn} disabled={busy === 'mac-verify' || !macInput || !tryVersion || !macCheck.trim()} onclick={doMacVerify} data-testid="kms-mac-verify">MAC verify</button>
									{#if macResult !== null}
										<p class="text-sm font-medium {macResult ? 'text-green-700 dark:text-green-400' : 'text-red-600 dark:text-red-400'}" data-testid="kms-mac-result">{macResult ? 'Valid: the MAC matches' : 'Invalid: the MAC does not match'}</p>
									{/if}
								</div>
							{/if}

							{#if publicKey}
								<div class="space-y-1" data-testid="kms-pubkey">
									<p class="text-xs font-medium text-gray-600 dark:text-gray-400">Public key ({publicKey.algorithm})</p>
									<div class="flex items-start gap-2"><pre class={out + ' flex-1'} data-testid="kms-pem">{publicKey.pem}</pre><button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy public key" onclick={() => copy(publicKey?.pem ?? '')}><Copy class="h-4 w-4" /></button></div>
								</div>
							{/if}
						</div>
					</div>
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={ringModal} title="New key ring" titleId="kms-ring-title">
	{#snippet children()}
		<form id="kms-ring-form" class="space-y-3" onsubmit={submitRing} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Key ring ID</span>
				<input class={input} bind:value={fRingId} placeholder="my-key-ring" autocomplete="off" aria-label="Key ring ID" />
				{#if ringTouched && ringIdError}<span class="block text-xs text-red-600 dark:text-red-400">{ringIdError}</span>{/if}
			</label>
			<p class="text-xs text-gray-500 dark:text-gray-400">Created in {project} / {location}. Key rings cannot be deleted.</p>
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => ringModal?.close()}>Cancel</button>
		<button type="submit" form="kms-ring-form" class="rounded-md bg-amber-600 px-4 py-2 text-sm text-white hover:bg-amber-700 disabled:opacity-50" disabled={busy === 'create-ring'} data-testid="kms-ring-submit">{busy === 'create-ring' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={keyModal} title="New key" titleId="kms-key-title">
	{#snippet children()}
		<form id="kms-key-form" class="space-y-3" onsubmit={submitKey} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Key ID</span>
				<input class={input} bind:value={fKeyId} placeholder="my-key" autocomplete="off" aria-label="Key ID" />
				{#if keyTouched && keyIdError}<span class="block text-xs text-red-600 dark:text-red-400">{keyIdError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Purpose</span>
				<select class={input} bind:value={fPurpose} onchange={onPurposeChange} aria-label="Purpose">
					{#each PURPOSES as p (p.id)}<option value={p.id}>{p.label}</option>{/each}
				</select>
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Algorithm</span>
				<select class={input} bind:value={fAlgorithm} aria-label="Algorithm">
					{#each ALGORITHMS[fPurpose] as a (a)}<option value={a}>{a}</option>{/each}
				</select>
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Protection level</span>
				<select class={input} bind:value={fLevel} aria-label="Protection level">
					{#each PROTECTION_LEVELS as l (l)}<option value={l}>{l}</option>{/each}
				</select>
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Rotation period (days, optional)</span>
				<input class={input} bind:value={fRotation} placeholder="90" aria-label="Rotation period days" />
			</label>
			<KeyValueEditor bind:rows={fLabels} label="Labels" testId="kms-key-labels" validateKey={validateLabelKey} hideSave />
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => keyModal?.close()}>Cancel</button>
		<button type="submit" form="kms-key-form" class="rounded-md bg-amber-600 px-4 py-2 text-sm text-white hover:bg-amber-700 disabled:opacity-50" disabled={busy === 'create-key'} data-testid="kms-key-submit">{busy === 'create-key' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<Modal bind:this={editModal} title="Edit key" titleId="kms-edit-title">
	{#snippet children()}
		<form id="kms-edit-form" class="space-y-3" onsubmit={submitEdit} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Rotation period (days, blank for none)</span>
				<input class={input} bind:value={eRotation} placeholder="90" aria-label="Rotation period days" data-testid="kms-edit-rotation" />
				<span class="block text-xs text-gray-500 dark:text-gray-400">Next rotation is set to that many days from now.</span>
			</label>
			<KeyValueEditor bind:rows={eLabels} label="Labels" testId="kms-edit-labels" validateKey={validateLabelKey} hideSave />
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => editModal?.close()}>Cancel</button>
		<button type="submit" form="kms-edit-form" class="rounded-md bg-amber-600 px-4 py-2 text-sm text-white hover:bg-amber-700 disabled:opacity-50" disabled={busy === 'update-key'} data-testid="kms-edit-submit">{busy === 'update-key' ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
