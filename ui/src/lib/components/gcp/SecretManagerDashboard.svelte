<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { KeyRound, Plus, Trash2, RefreshCw, Eye, EyeOff, Copy, X } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import Modal from '#lib/components/Modal.svelte';
	import KeyValueEditor from '#lib/components/gcp/KeyValueEditor.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { describeGcpError, isBase64 } from '#lib/gcprest.ts';
	import { rowsToMap, validateRows, type MetaRow } from '#lib/gcpstorage.ts';
	import {
		accessVersion,
		addVersion,
		aliasesToRows,
		createSecret,
		deleteSecret,
		getIamPolicy,
		getSecret,
		listSecrets,
		listVersions,
		readStoredSmProject,
		rowsToAliases,
		updateSecret,
		validateAliasKey,
		validateLabelKey,
		validateSecretId,
		versionAction,
		writeStoredSmProject,
		type AccessedPayload,
		type IamPolicy,
		type SecretInfo,
		type VersionAction,
		type VersionInfo
	} from '#lib/gcpsecretmanager.ts';

	let project = $state(readStoredSmProject());
	let projectInput = $state(readStoredSmProject());
	let secrets = $state<SecretInfo[]>([]);
	let current = $state<SecretInfo | null>(null);
	let versions = $state<VersionInfo[]>([]);
	let iam = $state<IamPolicy | null>(null);
	let aliasRows = $state<MetaRow[]>([]);
	let revealed = $state<Record<string, AccessedPayload>>({});
	let loading = $state(true);
	let loadError = $state('');
	let busy = $state('');

	let newMode = $state<'text' | 'base64'>('text');
	let newValue = $state('');

	let modal = $state<Modal | null>(null);
	let fId = $state('');
	let fReplication = $state<'automatic' | 'user'>('automatic');
	let fLocations = $state('us-east1');
	let fExpiry = $state<'none' | 'ttl' | 'time'>('none');
	let fTtl = $state('');
	let fTime = $state('');
	let fLabels = $state<MetaRow[]>([]);
	let fTouched = $state(false);

	const idError = $derived(validateSecretId(fId.trim()));
	const payloadError = $derived(newMode === 'base64' && newValue.trim() && !isBase64(newValue.trim()) ? 'Not valid base64' : null);

	const describe = describeGcpError;
	const input =
		'w-full rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500';
	const dt = 'font-medium text-gray-500 dark:text-gray-400';
	const dd = 'text-gray-800 dark:text-gray-200 break-all';
	const btn = 'rounded-md bg-blue-600 px-3 py-1 text-sm text-white hover:bg-blue-700 disabled:opacity-50';
	const mini = 'rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 text-xs hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50';
	const cell = 'px-4 py-3';

	async function loadSecrets() {
		loading = true;
		loadError = '';
		try {
			secrets = (await listSecrets(project)).toSorted((a, b) => a.id.localeCompare(b.id));
			if (current && !secrets.some((s) => s.name === current?.name)) current = null;
		} catch (e) {
			loadError = describe(e);
			toast.error(`Failed to load secrets: ${loadError}`);
		} finally {
			loading = false;
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

	async function loadDetail(id: string) {
		try {
			const [s, vs, policy] = await Promise.all([getSecret(project, id), listVersions(project, id), getIamPolicy(project, id)]);
			current = s;
			versions = vs;
			iam = policy;
			aliasRows = aliasesToRows(s.aliases);
		} catch (e) {
			toast.error(`Failed to read secret: ${describe(e)}`);
		}
	}

	async function select(s: SecretInfo) {
		current = s;
		versions = [];
		iam = null;
		revealed = {};
		newValue = '';
		newMode = 'text';
		aliasRows = aliasesToRows(s.aliases);
		await loadDetail(s.id);
	}

	function openNew() {
		fId = '';
		fReplication = 'automatic';
		fLocations = 'us-east1';
		fExpiry = 'none';
		fTtl = '';
		fTime = '';
		fLabels = [];
		fTouched = false;
		modal?.open();
	}

	async function submit(event: Event) {
		event.preventDefault();
		fTouched = true;
		if (idError) return;
		const bad = validateRows(fLabels, validateLabelKey);
		if (bad) return void toast.error(`Invalid label ${bad}`);
		const locations = fLocations.split(',').map((l) => l.trim()).filter(Boolean);
		if (fReplication === 'user' && locations.length === 0) return void toast.error('Enter at least one location');
		if (fExpiry === 'ttl' && !/^\d+$/.test(fTtl.trim())) return void toast.error('TTL must be a whole number of seconds');
		if (fExpiry === 'time' && !fTime) return void toast.error('Choose an expiry time');
		const id = fId.trim();
		const ok = await run('create', async () => {
			await createSecret(project, id, {
				replication: fReplication === 'automatic' ? { kind: 'automatic' } : { kind: 'user', locations },
				labels: rowsToMap(fLabels),
				ttlSeconds: fExpiry === 'ttl' ? fTtl : '',
				expireTime: fExpiry === 'time' ? fTime : ''
			});
		}, `Created secret ${id}`, `Failed to create ${id}`);
		if (ok) {
			modal?.close('created');
			await loadSecrets();
			const made = secrets.find((s) => s.id === id);
			if (made) await select(made);
		}
	}

	async function removeSecret(s: SecretInfo) {
		if (!(await confirmDestructive({ title: 'Delete secret', message: `Delete secret "${s.id}" and all of its versions? This cannot be undone.` }))) return;
		const ok = await run('delete', () => deleteSecret(project, s.id), `Deleted secret ${s.id}`, `Failed to delete ${s.id}`);
		if (ok) {
			if (current?.name === s.name) current = null;
			await loadSecrets();
		}
	}

	async function doAddVersion() {
		const s = current;
		if (!s || payloadError) return;
		if (!newValue) return void toast.error('Enter a payload');
		const ok = await run('add-version', async () => {
			const v = await addVersion(project, s.id, { mode: newMode, value: newValue });
			toast.success(`Added version ${v.id}`);
		}, '', 'Failed to add version');
		if (ok) {
			newValue = '';
			await loadDetail(s.id);
		}
	}

	async function reveal(v: VersionInfo) {
		const s = current;
		if (!s) return;
		try {
			revealed = { ...revealed, [v.id]: await accessVersion(project, s.id, v.id) };
		} catch (e) {
			toast.error(`Failed to access version ${v.id}: ${describe(e)}`);
		}
	}

	function hide(v: VersionInfo) {
		const { [v.id]: _drop, ...rest } = revealed;
		revealed = rest;
	}

	async function act(v: VersionInfo, action: VersionAction) {
		const s = current;
		if (!s) return;
		if (action === 'destroy' && !(await confirmDestructive({ title: 'Destroy version', message: `Permanently destroy version ${v.id} of "${s.id}"? Its payload cannot be recovered.` }))) return;
		const ok = await run(`${action}-${v.id}`, async () => { await versionAction(project, s.id, v.id, action); }, `Version ${v.id}: ${action} done`, `Failed to ${action} version ${v.id}`);
		if (ok) {
			hide(v);
			await loadDetail(s.id);
		}
	}

	async function saveAliases() {
		const s = current;
		if (!s) return;
		const parsed = rowsToAliases(aliasRows);
		if ('error' in parsed) return void toast.error(parsed.error);
		const ok = await run('aliases', async () => { await updateSecret(project, s.id, { aliases: parsed.aliases }); }, 'Saved aliases', 'Failed to save aliases');
		if (ok) await loadDetail(s.id);
	}

	async function copy(text: string) {
		try {
			await navigator.clipboard.writeText(text);
			toast.success('Copied to clipboard');
		} catch {
			toast.error('Clipboard is unavailable');
		}
	}

	function applyProject(event: Event) {
		event.preventDefault();
		const next = projectInput.trim();
		if (!next) return;
		project = next;
		writeStoredSmProject(next);
		current = null;
		void loadSecrets();
	}

	const stateClass = (st: string) =>
		st === 'ENABLED' ? 'text-green-700 dark:text-green-400' : st === 'DISABLED' ? 'text-amber-700 dark:text-amber-400' : 'text-gray-500 dark:text-gray-400';

	onMount(loadSecrets);
</script>

<div class="space-y-6 pb-20" data-testid="sm-dashboard">
	<PageHeader icon={KeyRound} title="Secret Manager" description="Google Secret Manager: secrets and versions" color="blue" onRefresh={loadSecrets}>
		{#snippet actions()}
			<button class="flex items-center gap-2 rounded-lg bg-blue-600 px-3 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" onclick={openNew} disabled={loading || Boolean(loadError)} data-testid="sm-new-secret">
				<Plus class="h-4 w-4" /> New secret
			</button>
		{/snippet}
	</PageHeader>

	<form class="flex flex-wrap items-center gap-2 text-xs text-gray-500 dark:text-gray-400" onsubmit={applyProject}>
		<label for="sm-project">Project</label>
		<input id="sm-project" class="w-56 rounded-md border border-gray-300 dark:border-gray-600 bg-white dark:bg-slate-800 px-2 py-1 font-mono text-xs" bind:value={projectInput} data-testid="sm-project" />
		<button type="submit" class="rounded-md border border-gray-200 dark:border-gray-700 px-2 py-1 hover:bg-gray-50 dark:hover:bg-slate-800 disabled:opacity-50" disabled={!projectInput.trim() || projectInput.trim() === project}>Switch</button>
		<span>Secrets are listed per project.</span>
	</form>

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="sm-error">
			<p class="font-medium">Could not reach Secret Manager</p>
			<p>{loadError}</p>
			<button class="mt-2 underline" onclick={loadSecrets}>Retry</button>
		</div>
	{:else}
		<div class="grid gap-6 lg:grid-cols-[18rem_1fr]">
			<section aria-label="Secrets" class="space-y-2">
				<h2 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Secrets</h2>
				{#if loading}
					<div class="flex justify-center py-8"><RefreshCw class="h-6 w-6 animate-spin text-gray-400" /></div>
				{:else if secrets.length === 0}
					<p class="py-8 text-center text-sm text-gray-500 dark:text-gray-400" data-testid="sm-no-secrets">No secrets in {project}</p>
				{:else}
					<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700">
						{#each secrets as s (s.name)}
							<li class="flex items-center {current?.name === s.name ? 'bg-blue-50 dark:bg-blue-950/40' : ''}" data-testid="sm-secret-row">
								<button class="flex-1 min-w-0 px-3 py-2 text-left text-sm font-medium text-gray-900 dark:text-gray-100 hover:bg-gray-50 dark:hover:bg-slate-800/50" onclick={() => select(s)}>
									<span class="block truncate">{s.id}</span>
									<span class="block text-[11px] font-normal text-gray-500 dark:text-gray-400">{s.replication}{s.expiry ? ` · ${s.expiry}` : ''}</span>
								</button>
								<button class="mr-1 rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950" aria-label="Delete secret {s.id}" onclick={() => removeSecret(s)}><Trash2 class="h-4 w-4" /></button>
							</li>
						{/each}
					</ul>
				{/if}
			</section>

			<section aria-label="Secret details" class="min-w-0">
				{#if !current}
					<p class="py-16 text-center text-gray-500 dark:text-gray-400" data-testid="sm-select-hint">Select a secret to manage its versions</p>
				{:else}
					{@const s = current}
					<div class="rounded-lg border border-gray-200 dark:border-gray-700 p-5 space-y-5" data-testid="sm-detail">
						<div class="flex items-start justify-between gap-4">
							<div class="min-w-0"><h3 class="text-lg font-semibold text-gray-900 dark:text-white break-all">{s.id}</h3><p class="font-mono text-xs text-gray-500 dark:text-gray-400 break-all">{s.name}</p></div>
							<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" onclick={() => (current = null)} aria-label="Close details"><X class="h-4 w-4" /></button>
						</div>
						<dl class="grid grid-cols-[max-content_1fr] gap-x-6 gap-y-2 text-sm">
							<dt class={dt}>Replication</dt><dd class={dd}>{s.replication || '—'}</dd>
							<dt class={dt}>Created</dt><dd class={dd}>{s.createTime || '—'}</dd>
							<dt class={dt}>Expires</dt><dd class={dd}>{s.expiry || 'never'}</dd>
							<dt class={dt}>Labels</dt><dd class={dd}>{Object.entries(s.labels).map(([k, v]) => `${k}=${v}`).join(', ') || '—'}</dd>
							<dt class={dt}>Aliases</dt><dd class={dd} data-testid="sm-aliases-view">{Object.entries(s.aliases).map(([k, v]) => `${k} → ${v}`).join(', ') || '—'}</dd>
						</dl>

						<div class="space-y-2" data-testid="sm-versions">
							<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Versions ({versions.length})</h4>
							{#if versions.length === 0}
								<p class="text-xs text-gray-500 dark:text-gray-400">No versions yet</p>
							{:else}
								<div class="rounded-lg border border-gray-200 dark:border-gray-700 overflow-x-auto">
									<table class="w-full text-sm">
										<thead class="bg-gray-50 dark:bg-slate-800/60 text-left text-gray-500 dark:text-gray-400"><tr><th class={cell}>Version</th><th class={cell}>State</th><th class={cell}>Created</th><th class="{cell} text-right">Actions</th></tr></thead>
										<tbody class="divide-y divide-gray-100 dark:divide-gray-800">
											{#each versions as v (v.id)}
												<tr data-testid="sm-version-row">
													<td class="{cell} font-mono">{v.id}</td>
													<td class="{cell} font-medium {stateClass(v.state)}" data-testid="sm-version-state">{v.state}</td>
													<td class="{cell} whitespace-nowrap text-xs">{v.createTime}</td>
													<td class="{cell} text-right whitespace-nowrap space-x-1">
														{#if v.state === 'ENABLED'}
															{#if revealed[v.id]}
																<button class={mini} onclick={() => hide(v)} data-testid="sm-hide-{v.id}"><EyeOff class="inline h-3.5 w-3.5" /> Hide</button>
															{:else}
																<button class={mini} onclick={() => reveal(v)} data-testid="sm-reveal-{v.id}"><Eye class="inline h-3.5 w-3.5" /> Reveal</button>
															{/if}
															<button class={mini} disabled={busy === `disable-${v.id}`} onclick={() => act(v, 'disable')} data-testid="sm-disable-{v.id}">Disable</button>
														{:else if v.state === 'DISABLED'}
															<button class={mini} disabled={busy === `enable-${v.id}`} onclick={() => act(v, 'enable')} data-testid="sm-enable-{v.id}">Enable</button>
														{/if}
														{#if v.state !== 'DESTROYED'}
															<button class="{mini} text-red-600" disabled={busy === `destroy-${v.id}`} onclick={() => act(v, 'destroy')} data-testid="sm-destroy-{v.id}">Destroy</button>
														{/if}
													</td>
												</tr>
												{#if revealed[v.id]}
													{@const p = revealed[v.id]}
													<tr class="bg-amber-50 dark:bg-amber-950/20" data-testid="sm-payload-row">
														<td class={cell} colspan="4">
															<div class="flex items-start justify-between gap-3">
																<div class="min-w-0 space-y-1">
																	{#if p.text !== null}
																		<pre class="whitespace-pre-wrap break-all font-mono text-xs" data-testid="sm-payload-text">{p.text}</pre>
																	{:else}
																		<p class="text-[11px] text-gray-500 dark:text-gray-400">Binary payload (not valid UTF-8)</p>
																	{/if}
																	<p class="font-mono text-[11px] text-gray-500 dark:text-gray-400 break-all">base64: <span data-testid="sm-payload-b64">{p.base64}</span></p>
																</div>
																<button class="rounded p-1 hover:bg-gray-100 dark:hover:bg-slate-800" aria-label="Copy payload" onclick={() => copy(p.text ?? p.base64)}><Copy class="h-4 w-4" /></button>
															</div>
														</td>
													</tr>
												{/if}
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
						</div>

						<div class="grid gap-6 md:grid-cols-2">
							<div class="space-y-3" data-testid="sm-add-version">
								<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">Add version</h4>
								<div class="flex gap-4 text-sm">
									<label class="flex items-center gap-1"><input type="radio" bind:group={newMode} value="text" /> Text</label>
									<label class="flex items-center gap-1"><input type="radio" bind:group={newMode} value="base64" /> Base64</label>
								</div>
								<textarea class="{input} font-mono" rows="3" bind:value={newValue} aria-label="Secret payload" placeholder={newMode === 'text' ? 'secret value' : 'c2VjcmV0'} data-testid="sm-payload-input"></textarea>
								{#if payloadError}<p class="text-xs text-red-600 dark:text-red-400">{payloadError}</p>{/if}
								<button type="button" class={btn} disabled={busy === 'add-version' || Boolean(payloadError) || !newValue} onclick={doAddVersion} data-testid="sm-add-version-submit">Add version</button>
							</div>
							<div class="space-y-3">
								<KeyValueEditor bind:rows={aliasRows} label="Version aliases" testId="sm-aliases" validateKey={validateAliasKey} saving={busy === 'aliases'} onsave={saveAliases} />
								<p class="text-[11px] text-gray-500 dark:text-gray-400">Value is the version number the alias points to.</p>
							</div>
						</div>

						<div class="space-y-2" data-testid="sm-iam">
							<h4 class="text-sm font-semibold text-gray-700 dark:text-gray-300">IAM policy (read-only)</h4>
							{#if !iam || iam.bindings.length === 0}
								<p class="text-xs text-gray-500 dark:text-gray-400">No bindings</p>
							{:else}
								<ul class="divide-y divide-gray-100 dark:divide-gray-800 rounded-lg border border-gray-200 dark:border-gray-700 text-xs">
									{#each iam.bindings as b (b.role)}
										<li class="px-3 py-2" data-testid="sm-iam-binding"><p class="font-mono font-medium">{b.role}</p><p class="font-mono text-gray-500 dark:text-gray-400 break-all">{b.members.join(', ')}</p></li>
									{/each}
								</ul>
							{/if}
							{#if iam}<p class="text-[11px] text-gray-500 dark:text-gray-400">version {iam.version}</p>{/if}
						</div>
					</div>
				{/if}
			</section>
		</div>
	{/if}
</div>

<Modal bind:this={modal} title="New secret" titleId="sm-create-title">
	{#snippet children()}
		<form id="sm-create-form" class="space-y-3" onsubmit={submit} novalidate>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Secret ID</span>
				<input class={input} bind:value={fId} placeholder="my-secret" autocomplete="off" aria-label="Secret ID" />
				{#if fTouched && idError}<span class="block text-xs text-red-600 dark:text-red-400">{idError}</span>{/if}
			</label>
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Replication</span>
				<select class={input} bind:value={fReplication} aria-label="Replication"><option value="automatic">Automatic</option><option value="user">User-managed</option></select>
			</label>
			{#if fReplication === 'user'}
				<label class="block space-y-1 text-sm">
					<span class="font-medium text-gray-700 dark:text-gray-300">Locations (comma separated)</span>
					<input class={input} bind:value={fLocations} aria-label="Locations" />
				</label>
			{/if}
			<label class="block space-y-1 text-sm">
				<span class="font-medium text-gray-700 dark:text-gray-300">Expiration</span>
				<select class={input} bind:value={fExpiry} aria-label="Expiration"><option value="none">Never</option><option value="ttl">TTL (seconds)</option><option value="time">At a time</option></select>
			</label>
			{#if fExpiry === 'ttl'}
				<input class={input} bind:value={fTtl} placeholder="86400" aria-label="TTL seconds" />
			{:else if fExpiry === 'time'}
				<input type="datetime-local" class={input} bind:value={fTime} aria-label="Expire time" />
			{/if}
			<KeyValueEditor bind:rows={fLabels} label="Labels" testId="sm-create-labels" validateKey={validateLabelKey} hideSave />
		</form>
	{/snippet}
	{#snippet footer()}
		<button type="button" class="rounded-md border border-gray-300 dark:border-gray-600 px-4 py-2 text-sm hover:bg-gray-50 dark:hover:bg-slate-800" onclick={() => modal?.close()}>Cancel</button>
		<button type="submit" form="sm-create-form" class="rounded-md bg-blue-600 px-4 py-2 text-sm text-white hover:bg-blue-700 disabled:opacity-50" disabled={busy === 'create'} data-testid="sm-create-submit">{busy === 'create' ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>
