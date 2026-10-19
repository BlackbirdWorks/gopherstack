<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { KeyRound, Plus, Trash2, RefreshCw, Inbox, Copy, Eye, EyeOff, Undo2, Flame, RotateCw } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import {
		KvError,
		algsFor,
		certPem,
		createCert,
		createKey,
		createVault,
		decryptText,
		deleteItem,
		encryptText,
		formatEpoch,
		getCert,
		getKey,
		listItems,
		listVaults,
		listVersions,
		purgeItem,
		recoverItem,
		revealSecret,
		rotateKey,
		setSecret,
		signText,
		thumbprintHex,
		validateMonths,
		validateObjectName,
		validateVaultName,
		verifyText,
		type CertDetail,
		type Kind,
		type KeyDetail,
		type KvItem
	} from '#lib/azurekeyvault.ts';

	type CreateKind = 'vault' | 'secret' | 'key' | 'cert';

	const TABS = [
		{ id: 'secrets', label: 'Secrets' },
		{ id: 'keys', label: 'Keys' },
		{ id: 'certificates', label: 'Certificates' }
	];

	let vaults = $state<string[]>([]);
	let vault = $state('');
	let tab = $state<Kind>('secrets');
	let loading = $state(true);
	let loadError = $state('');
	let listLoading = $state(false);
	let items = $state<KvItem[]>([]);
	let showDeleted = $state(false);
	let selected = $state<string | null>(null);
	let versions = $state<KvItem[]>([]);
	let keyDetail = $state<KeyDetail | null>(null);
	let certDetail = $state<CertDetail | null>(null);
	let revealed = $state<Record<string, string>>({});

	let createModal = $state<Modal | null>(null);
	let createKind = $state<CreateKind>('secret');
	let fName = $state('');
	let fValue = $state('');
	let fContentType = $state('');
	let fKty = $state<'RSA' | 'EC'>('RSA');
	let fSize = $state('2048');
	let fCrv = $state('P-256');
	let fSubject = $state('');
	let fMonths = $state('12');
	let touched = $state(false);
	let creating = $state(false);

	let tryAlg = $state('');
	let tryText = $state('hello gopherstack');
	let tryCipher = $state('');
	let tryPlain = $state('');
	let trySig = $state('');
	let tryVerify = $state<boolean | null>(null);
	let tryBusy = $state(false);

	const nameError = $derived(createKind === 'vault' ? validateVaultName(fName.trim()) : validateObjectName(fName.trim()));
	const monthsError = $derived(validateMonths(fMonths));
	const subjectError = $derived(fSubject.trim() ? '' : 'Subject is required, e.g. CN=example.com');
	const valueError = $derived(createKind === 'secret' && fValue === '' ? 'Value is required' : '');
	const formError = $derived(
		nameError || (createKind === 'secret' ? valueError : '') || (createKind === 'cert' ? monthsError || subjectError : '')
	);
	const algs = $derived(keyDetail ? algsFor(keyDetail.kty, keyDetail.crv) : { encrypt: [], sign: [] });
	const selectedItem = $derived(items.find((i) => i.name === selected) ?? null);
	const kindLabel = $derived(tab === 'secrets' ? 'secret' : tab === 'keys' ? 'key' : 'certificate');

	function describe(e: unknown): string {
		if (e instanceof KvError) return e.code ? `${e.code}: ${e.message}` : e.message;
		return e instanceof Error ? e.message : String(e);
	}

	async function loadVaults(keep = vault) {
		loading = true;
		loadError = '';
		try {
			vaults = await listVaults();
			vault = vaults.includes(keep) ? keep : (vaults[0] ?? '');
			await loadItems();
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load Key Vault: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	function resetSelection() {
		selected = null;
		versions = [];
		keyDetail = null;
		certDetail = null;
		revealed = {};
		resetTry();
	}

	function resetTry() {
		tryCipher = '';
		tryPlain = '';
		trySig = '';
		tryVerify = null;
	}

	async function loadItems() {
		if (!vault) {
			items = [];
			return;
		}
		listLoading = true;
		try {
			items = await listItems(vault, tab, showDeleted);
			if (selected && !items.some((i) => i.name === selected)) resetSelection();
		} catch (e) {
			items = [];
			toast.error(`Failed to list ${tab}: ${describe(e)}`);
		} finally {
			listLoading = false;
		}
	}

	async function selectVault(name: string) {
		vault = name;
		resetSelection();
		await loadItems();
	}

	async function selectTab(next: string) {
		tab = next as Kind;
		showDeleted = false;
		resetSelection();
		await loadItems();
	}

	async function setDeletedView(on: boolean) {
		showDeleted = on;
		resetSelection();
		await loadItems();
	}

	async function select(name: string) {
		resetSelection();
		selected = name;
		if (showDeleted) return;
		try {
			versions = await listVersions(vault, tab, name);
			if (tab === 'keys') {
				keyDetail = await getKey(vault, name);
				const a = algsFor(keyDetail.kty, keyDetail.crv);
				tryAlg = a.encrypt[0] ?? a.sign[0] ?? '';
			} else if (tab === 'certificates') {
				certDetail = await getCert(vault, name);
			}
		} catch (e) {
			toast.error(`Failed to load ${name}: ${describe(e)}`);
		}
	}

	function openCreate(kind: CreateKind) {
		createKind = kind;
		fName = '';
		fValue = '';
		fContentType = '';
		fKty = 'RSA';
		fSize = '2048';
		fCrv = 'P-256';
		fSubject = '';
		fMonths = '12';
		touched = false;
		createModal?.open();
	}

	async function submitCreate(event: Event) {
		event.preventDefault();
		touched = true;
		if (formError || creating) return;
		creating = true;
		const name = fName.trim();
		try {
			if (createKind === 'vault') {
				await createVault(name);
				createModal?.close('created');
				toast.success(`Created vault ${name}`);
				await loadVaults(name);
				return;
			}
			if (createKind === 'secret') await setSecret(vault, name, fValue, fContentType.trim());
			else if (createKind === 'key') await createKey(vault, { name, kty: fKty, size: Number(fSize), crv: fCrv });
			else await createCert(vault, { name, subject: fSubject.trim(), validityMonths: Number(fMonths) });
			createModal?.close('created');
			toast.success(`Created ${name}`);
			fValue = '';
			await loadItems();
			await select(name);
		} catch (e) {
			toast.error(`Failed to create ${name}: ${describe(e)}`);
		} finally {
			creating = false;
		}
	}

	async function remove(name: string) {
		const ok = await confirmDestructive({
			title: `Delete ${kindLabel}`,
			message: `Delete ${kindLabel} "${name}"? It moves to the deleted list and can be recovered until purged.`
		});
		if (!ok) return;
		try {
			await deleteItem(vault, tab, name);
			toast.success(`Deleted ${name}`);
			await loadItems();
		} catch (e) {
			toast.error(`Failed to delete ${name}: ${describe(e)}`);
		}
	}

	async function recover(name: string) {
		try {
			await recoverItem(vault, tab, name);
			toast.success(`Recovered ${name}`);
			await loadItems();
		} catch (e) {
			toast.error(`Failed to recover ${name}: ${describe(e)}`);
		}
	}

	async function purge(name: string) {
		const ok = await confirmDestructive({
			title: `Purge ${kindLabel}`,
			message: `Permanently purge ${kindLabel} "${name}"? This cannot be undone.`,
			confirmLabel: 'Purge'
		});
		if (!ok) return;
		try {
			await purgeItem(vault, tab, name);
			toast.success(`Purged ${name}`);
			await loadItems();
		} catch (e) {
			toast.error(`Failed to purge ${name}: ${describe(e)}`);
		}
	}

	async function toggleReveal(version: string) {
		if (!selected) return;
		if (version in revealed) {
			const { [version]: _drop, ...rest } = revealed;
			revealed = rest;
			return;
		}
		try {
			revealed = { ...revealed, [version]: await revealSecret(vault, selected, version) };
		} catch (e) {
			toast.error(`Failed to read secret: ${describe(e)}`);
		}
	}

	async function copy(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	async function rotate() {
		if (!selected) return;
		try {
			await rotateKey(vault, selected);
			toast.success(`Rotated ${selected}`);
			await select(selected);
		} catch (e) {
			toast.error(`Failed to rotate: ${describe(e)}`);
		}
	}

	async function runTry(op: 'encrypt' | 'decrypt' | 'sign' | 'verify') {
		if (!selected || !keyDetail) return;
		tryBusy = true;
		try {
			const name = selected;
			if (op === 'encrypt') tryCipher = await encryptText(vault, name, '', tryAlg, tryText);
			else if (op === 'decrypt') tryPlain = await decryptText(vault, name, '', tryAlg, tryCipher);
			else if (op === 'sign') {
				trySig = await signText(vault, name, '', tryAlg, tryText);
				tryVerify = null;
			} else tryVerify = await verifyText(vault, name, '', tryAlg, tryText, trySig);
		} catch (e) {
			toast.error(`Failed to ${op}: ${describe(e)}`);
		} finally {
			tryBusy = false;
		}
	}

	function setTryMode(next: 'encrypt' | 'sign') {
		tryAlg = algs[next][0] ?? '';
		resetTry();
	}

	const signAlg = $derived(algs.sign.includes(tryAlg));

	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const th = 'px-4 py-2 font-medium';
	const td = 'px-4 py-2 text-gray-600 dark:text-gray-400';
	const iconBtn = 'rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800';
	const primaryBtn = 'flex items-center gap-2 rounded-md bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50';
	const ghostBtn = 'rounded-md border border-gray-300 dark:border-gray-600 px-3 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const rowBtn = 'flex-1 truncate px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50';

	onMount(() => loadVaults());
</script>

<div class="space-y-6 pb-20" data-testid="keyvault-dashboard">
	<PageHeader icon={KeyRound} title="Key Vault" description="Azure Key Vault: secrets, keys and certificates" color="blue" onRefresh={() => loadVaults()}>
		{#snippet actions()}
			<button class={primaryBtn} onclick={() => openCreate('vault')} disabled={loading || Boolean(loadError)} data-testid="keyvault-new-vault">
				<Plus class="h-4 w-4" /> New vault
			</button>
		{/snippet}
	</PageHeader>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="keyvault-error">
			<p class="font-medium">Could not reach Azure Key Vault</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={() => loadVaults()}>Retry</button>
		</div>
	{:else if loading}
		<div class="flex justify-center py-16"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
	{:else if vaults.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="keyvault-no-vaults">
			<Inbox class="h-10 w-10 mb-3 opacity-30" />
			<p>No vaults yet</p>
			<button class="mt-3 {primaryBtn}" onclick={() => openCreate('vault')}><Plus class="h-4 w-4" /> New vault</button>
		</div>
	{:else}
		<div class="flex flex-wrap items-center gap-3">
			<label class="flex items-center gap-2 text-sm font-medium text-gray-700 dark:text-gray-300">
				Vault
				<select class="{input} w-64" value={vault} onchange={(e) => selectVault(e.currentTarget.value)} aria-label="Vault" data-testid="keyvault-vault-select">
					{#each vaults as v (v)}<option value={v}>{v}</option>{/each}
				</select>
			</label>
			<span class="text-xs text-gray-500 dark:text-gray-400">Vaults are shared with Resource Manager; a new vault is created on first use.</span>
		</div>

		<Tabs tabs={TABS} active={tab} onSelect={selectTab} color="blue" />

		<div class="grid gap-6 lg:grid-cols-[22rem_1fr]" id="tabpanel-{tab}" role="tabpanel" aria-labelledby="tab-{tab}">
			<section aria-label="{kindLabel} list" class="space-y-3">
				<div class="flex flex-wrap items-center justify-between gap-2">
					<div class="inline-flex rounded-md border border-gray-300 dark:border-gray-600 text-sm" role="group" aria-label="View">
						<button class="px-3 py-1 rounded-l-md {!showDeleted ? 'bg-blue-600 text-white' : 'hover:bg-gray-50 dark:hover:bg-slate-800'}" aria-pressed={!showDeleted} onclick={() => setDeletedView(false)}>Active</button>
						<button class="px-3 py-1 rounded-r-md {showDeleted ? 'bg-blue-600 text-white' : 'hover:bg-gray-50 dark:hover:bg-slate-800'}" aria-pressed={showDeleted} onclick={() => setDeletedView(true)} data-testid="keyvault-deleted-toggle">Deleted</button>
					</div>
					{#if !showDeleted}
						<button class={primaryBtn} onclick={() => openCreate(tab === 'secrets' ? 'secret' : tab === 'keys' ? 'key' : 'cert')} data-testid="keyvault-new">
							<Plus class="h-4 w-4" /> New {kindLabel}
						</button>
					{/if}
				</div>

				{#if listLoading}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if items.length === 0}
					<div class="flex flex-col items-center py-8 text-gray-500 dark:text-gray-400" data-testid="keyvault-empty">
						<Inbox class="h-8 w-8 mb-2 opacity-30" />
						<p class="text-sm">{showDeleted ? `No deleted ${tab}` : `No ${tab}`}</p>
					</div>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700" data-testid="keyvault-list">
						{#each items as it (it.name)}
							<li class="flex items-center {selected === it.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="keyvault-row">
								<button class={rowBtn} onclick={() => select(it.name)}>
									{it.name}
									{#if !it.enabled}<span class="ml-2 rounded bg-gray-100 dark:bg-slate-800 px-1.5 py-0.5 text-xs font-normal text-gray-500">disabled</span>{/if}
								</button>
								{#if showDeleted}
									<button class="{iconBtn} text-emerald-600" title="Recover {it.name}" aria-label="Recover {it.name}" onclick={() => recover(it.name)}><Undo2 class="h-4 w-4" /></button>
									<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Purge {it.name}" aria-label="Purge {it.name}" onclick={() => purge(it.name)}><Flame class="h-4 w-4" /></button>
								{:else}
									<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" title="Delete {it.name}" aria-label="Delete {it.name}" onclick={() => remove(it.name)}><Trash2 class="h-4 w-4" /></button>
								{/if}
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<section aria-label="Details" class="space-y-4 min-w-0">
				{#if !selected || !selectedItem}
					<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="keyvault-select-hint">
						<KeyRound class="h-12 w-12 mb-3 opacity-30" />
						<p>Select a {kindLabel}</p>
					</div>
				{:else if showDeleted}
					<h2 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{selectedItem.name}</h2>
					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 rounded-lg border border-gray-200 dark:border-gray-700 p-4 text-sm" data-testid="keyvault-props">
						<dt class={dt}>Deleted</dt><dd class={dd}>{formatEpoch(selectedItem.deletedDate)}</dd>
						<dt class={dt}>Scheduled purge</dt><dd class={dd}>{formatEpoch(selectedItem.scheduledPurgeDate)}</dd>
					</dl>
					<div class="flex gap-2">
						<button class={primaryBtn} onclick={() => recover(selectedItem.name)}><Undo2 class="h-4 w-4" /> Recover</button>
						<button class="{ghostBtn} text-red-600" onclick={() => purge(selectedItem.name)}><Flame class="inline h-4 w-4" /> Purge</button>
					</div>
				{:else}
					<div class="flex flex-wrap items-center justify-between gap-2">
						<h2 class="text-lg font-semibold text-gray-900 dark:text-white break-all" data-testid="keyvault-current">{selectedItem.name}</h2>
						{#if tab === 'keys'}
							<button class={ghostBtn} onclick={rotate} data-testid="keyvault-rotate"><RotateCw class="inline h-4 w-4" /> Rotate</button>
						{/if}
					</div>

					<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 rounded-lg border border-gray-200 dark:border-gray-700 p-4 text-sm" data-testid="keyvault-props">
						<dt class={dt}>Status</dt><dd class={dd}>{selectedItem.enabled ? 'Enabled' : 'Disabled'}</dd>
						<dt class={dt}>Created</dt><dd class={dd}>{formatEpoch(selectedItem.created)}</dd>
						<dt class={dt}>Updated</dt><dd class={dd}>{formatEpoch(selectedItem.updated)}</dd>
						{#if selectedItem.contentType}<dt class={dt}>Content type</dt><dd class={dd}>{selectedItem.contentType}</dd>{/if}
						{#if keyDetail}
							<dt class={dt}>Key type</dt><dd class={dd}>{keyDetail.kty}{keyDetail.keySize ? ` ${keyDetail.keySize}` : ''}{keyDetail.crv ? ` ${keyDetail.crv}` : ''}</dd>
							<dt class={dt}>Operations</dt><dd class={dd}>{keyDetail.keyOps.join(', ') || '—'}</dd>
						{/if}
						{#if certDetail}
							<dt class={dt}>Subject</dt><dd class={dd}>{certDetail.subject || '—'}</dd>
							<dt class={dt}>Thumbprint</dt><dd class="{dd} font-mono text-xs" data-testid="keyvault-thumbprint">{thumbprintHex(certDetail.x5t) || '—'}</dd>
						{/if}
					</dl>

					{#if tab !== 'certificates' || versions.length > 0}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700">
							<h3 class="px-4 pt-3 text-sm font-semibold text-gray-700 dark:text-gray-300">Versions</h3>
							<div class="overflow-x-auto">
								<table class="w-full text-left text-sm" data-testid="keyvault-versions">
									<thead class="text-gray-500 dark:text-gray-400"><tr><th class={th}>Version</th><th class={th}>Created</th><th class={th}>Status</th>{#if tab === 'secrets'}<th class={th}>Value</th>{/if}</tr></thead>
									<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
										{#each versions as v (v.version)}
											<tr>
												<td class="{td} font-mono text-xs">{v.version}</td>
												<td class={td}>{formatEpoch(v.created)}</td>
												<td class={td}>{v.enabled ? 'Enabled' : 'Disabled'}</td>
												{#if tab === 'secrets'}
													<td class={td}>
														{#if v.version in revealed}
															<span class="font-mono text-xs break-all" data-testid="keyvault-secret-value">{revealed[v.version]}</span>
															<button class={iconBtn} title="Copy value" aria-label="Copy value" onclick={() => copy(revealed[v.version])}><Copy class="inline h-4 w-4" /></button>
															<button class={iconBtn} title="Hide value" aria-label="Hide value" onclick={() => toggleReveal(v.version)}><EyeOff class="inline h-4 w-4" /></button>
														{:else}
															<button class="inline-flex items-center gap-1 text-blue-600 dark:text-blue-400 hover:underline" onclick={() => toggleReveal(v.version)} data-testid="keyvault-reveal"><Eye class="h-4 w-4" /> Reveal</button>
														{/if}
													</td>
												{/if}
											</tr>
										{/each}
									</tbody>
								</table>
							</div>
						</div>
					{/if}

					{#if certDetail && certDetail.cer}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-4 space-y-2">
							<div class="flex items-center justify-between">
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Certificate (PEM)</h3>
								<button class={iconBtn} title="Copy PEM" aria-label="Copy PEM" onclick={() => copy(certPem(certDetail?.cer ?? ''))}><Copy class="h-4 w-4" /></button>
							</div>
							<pre class="max-h-72 overflow-auto rounded bg-gray-50 dark:bg-slate-900 p-3 text-xs" data-testid="keyvault-pem">{certPem(certDetail.cer)}</pre>
						</div>
					{/if}

					{#if keyDetail && (algs.encrypt.length > 0 || algs.sign.length > 0)}
						<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-4 space-y-3" data-testid="keyvault-tryit">
							<div class="flex flex-wrap items-center justify-between gap-2">
								<h3 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Try it</h3>
								<div class="inline-flex rounded-md border border-gray-300 dark:border-gray-600 text-sm" role="group" aria-label="Operation">
									{#if algs.encrypt.length > 0}
										<button class="px-3 py-1 rounded-l-md {!signAlg ? 'bg-blue-600 text-white' : 'hover:bg-gray-50 dark:hover:bg-slate-800'}" aria-pressed={!signAlg} onclick={() => setTryMode('encrypt')}>Encrypt / decrypt</button>
									{/if}
									{#if algs.sign.length > 0}
										<button class="px-3 py-1 {algs.encrypt.length === 0 ? 'rounded-l-md' : ''} rounded-r-md {signAlg ? 'bg-blue-600 text-white' : 'hover:bg-gray-50 dark:hover:bg-slate-800'}" aria-pressed={signAlg} onclick={() => setTryMode('sign')}>Sign / verify</button>
									{/if}
								</div>
							</div>
							<div class="grid gap-3 sm:grid-cols-[14rem_1fr]">
								<label class="space-y-1 text-sm">
									<span class="block font-medium text-gray-700 dark:text-gray-300">Algorithm</span>
									<select class={input} bind:value={tryAlg} aria-label="Algorithm" onchange={resetTry}>
										{#each signAlg ? algs.sign : algs.encrypt as a (a)}<option value={a}>{a}</option>{/each}
									</select>
								</label>
								<label class="space-y-1 text-sm">
									<span class="block font-medium text-gray-700 dark:text-gray-300">Text</span>
									<textarea class={input} rows="2" bind:value={tryText} aria-label="Text"></textarea>
								</label>
							</div>
							{#if !signAlg}
								<div class="flex flex-wrap gap-2">
									<button class={primaryBtn} disabled={tryBusy || !tryText} onclick={() => runTry('encrypt')} data-testid="keyvault-encrypt">Encrypt</button>
									<button class={ghostBtn} disabled={tryBusy || !tryCipher} onclick={() => runTry('decrypt')} data-testid="keyvault-decrypt">Decrypt</button>
								</div>
								{#if tryCipher}<label class="block space-y-1 text-sm"><span class="font-medium text-gray-700 dark:text-gray-300">Ciphertext (base64url)</span><textarea class="{input} font-mono text-xs" rows="3" bind:value={tryCipher} aria-label="Ciphertext" data-testid="keyvault-cipher"></textarea></label>{/if}
								{#if tryPlain}<p class="text-sm">Decrypted: <span class="font-mono" data-testid="keyvault-plain">{tryPlain}</span></p>{/if}
							{:else}
								<div class="flex flex-wrap gap-2">
									<button class={primaryBtn} disabled={tryBusy || !tryText} onclick={() => runTry('sign')} data-testid="keyvault-sign">Sign</button>
									<button class={ghostBtn} disabled={tryBusy || !trySig} onclick={() => runTry('verify')} data-testid="keyvault-verify">Verify</button>
								</div>
								{#if trySig}<label class="block space-y-1 text-sm"><span class="font-medium text-gray-700 dark:text-gray-300">Signature (base64url)</span><textarea class="{input} font-mono text-xs" rows="3" bind:value={trySig} aria-label="Signature" data-testid="keyvault-signature" oninput={() => (tryVerify = null)}></textarea></label>{/if}
								{#if tryVerify !== null}
									<p class="text-sm font-medium {tryVerify ? 'text-emerald-600 dark:text-emerald-400' : 'text-red-600 dark:text-red-400'}" data-testid="keyvault-verify-result">{tryVerify ? 'Signature is valid' : 'Signature is NOT valid'}</p>
								{/if}
							{/if}
						</div>
					{/if}
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal
	bind:this={createModal}
	title={createKind === 'vault' ? 'New vault' : createKind === 'secret' ? 'New secret' : createKind === 'key' ? 'New key' : 'New self-signed certificate'}
	titleId="keyvault-create-title"
>
	{#snippet children()}
		<form id="keyvault-create-form" class="space-y-3" onsubmit={submitCreate} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Name</span>
				<input class={input} bind:value={fName} autocomplete="off" aria-label="Name" />
				{#if touched && nameError}<span class="block text-xs text-red-600 dark:text-red-400">{nameError}</span>{/if}
			</label>
			{#if createKind === 'secret'}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Value</span>
					<textarea class={input} rows="3" bind:value={fValue} autocomplete="off" aria-label="Value"></textarea>
					{#if touched && valueError}<span class="block text-xs text-red-600 dark:text-red-400">{valueError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Content type (optional)</span>
					<input class={input} bind:value={fContentType} aria-label="Content type" />
				</label>
			{:else if createKind === 'key'}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Key type</span>
					<select class={input} bind:value={fKty} aria-label="Key type"><option value="RSA">RSA</option><option value="EC">EC</option></select>
				</label>
				{#if fKty === 'RSA'}
					<label class="block space-y-1 text-sm">
						<span class="font-medium text-gray-700 dark:text-gray-300">Key size</span>
						<select class={input} bind:value={fSize} aria-label="Key size"><option>2048</option><option>3072</option><option>4096</option></select>
					</label>
				{:else}
					<label class="block space-y-1 text-sm">
						<span class="font-medium text-gray-700 dark:text-gray-300">Curve</span>
						<select class={input} bind:value={fCrv} aria-label="Curve"><option>P-256</option><option>P-384</option><option>P-521</option></select>
					</label>
				{/if}
			{:else if createKind === 'cert'}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Subject</span>
					<input class={input} bind:value={fSubject} placeholder="CN=example.com" autocomplete="off" aria-label="Subject" />
					{#if touched && subjectError}<span class="block text-xs text-red-600 dark:text-red-400">{subjectError}</span>{/if}
				</label>
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Validity (months)</span>
					<input class={input} bind:value={fMonths} inputmode="numeric" aria-label="Validity months" />
					{#if monthsError}<span class="block text-xs text-red-600 dark:text-red-400">{monthsError}</span>{/if}
				</label>
			{/if}
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class={ghostBtn} onclick={() => createModal?.close()}>Cancel</button>
		<button type="submit" form="keyvault-create-form" class={primaryBtn} disabled={creating}>{creating ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>
