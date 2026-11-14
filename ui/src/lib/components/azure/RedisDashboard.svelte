<script lang="ts">
	import { onMount } from 'svelte';
	import { toast } from 'svelte-sonner';
	import { Database, Inbox } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import RevealKeys from '#lib/components/azure/RevealKeys.svelte';
	import { attempt, GCP_GHOST, GCP_INPUT, GCP_MINI } from '#lib/gcp-action.ts';
	import { describeGcpError } from '#lib/gcprest.ts';
	import {
		DATABASES,
		getValue,
		listCaches,
		revealKeys,
		scanKeys,
		ttlLabel,
		validatePattern,
		type RedisCache,
		type RedisKey,
		type RedisValue
	} from '#lib/azureredis.ts';

	let caches = $state<RedisCache[]>([]);
	let selectedId = $state('');
	let loading = $state(true);
	let loadError = $state('');
	let db = $state(0);
	let match = $state('*');
	let keys = $state<RedisKey[]>([]);
	let next = $state('');
	let scanned = $state(false);
	let value = $state<RedisValue | undefined>();

	const cache = $derived(caches.find((c) => c.id === selectedId));
	const patternError = $derived(validatePattern(match));

	async function load() {
		loading = true;
		loadError = '';
		try {
			caches = await listCaches();
			if (!caches.some((c) => c.id === selectedId)) selectedId = caches[0]?.id ?? '';
		} catch (e) {
			loadError = describeGcpError(e);
			toast.error(`Failed to load Cache for Redis: ${loadError}`);
		} finally {
			loading = false;
		}
	}

	async function scan(more = false) {
		if (!cache || patternError) return;
		value = undefined;
		await attempt(async () => {
			const page = await scanKeys(cache.id, db, match.trim(), more ? next : '');
			keys = more ? [...keys, ...page.keys] : page.keys;
			next = page.next;
			scanned = true;
		}, '', 'Failed to scan keys');
	}

	function pick(id: string) {
		selectedId = id;
		keys = [];
		next = '';
		scanned = false;
		value = undefined;
	}

	async function view(k: RedisKey) {
		if (!cache) return;
		await attempt(async () => {
			value = await getValue(cache.id, db, k.key);
		}, '', `Failed to read ${k.key}`);
	}

	onMount(() => void load());
</script>

<div class="space-y-6 pb-20" data-testid="redis-dashboard">
	<PageHeader icon={Database} title="Cache for Redis" description="Azure Cache for Redis: caches, a read-only key browser and access keys" color="red" onRefresh={load} />

	{#if loadError}
		<div class="rounded-lg border border-red-200 bg-red-50 p-4 text-sm text-red-700 dark:border-red-900 dark:bg-red-950/40 dark:text-red-300" role="alert" data-testid="redis-error">
			<p class="font-medium">Could not reach Azure Cache for Redis</p>
			<p>{loadError}</p>
		</div>
	{:else if loading}
		<p class="text-gray-500">Loading...</p>
	{:else if caches.length === 0}
		<div class="flex flex-col items-center py-16 text-gray-500 dark:text-gray-400" data-testid="redis-empty">
			<Inbox class="mb-3 h-10 w-10 opacity-30" />
			<p>No caches. Create one with the Azure CLI, Terraform or ARM.</p>
		</div>
	{:else}
		<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
			<table class="w-full text-sm" data-testid="redis-table">
				<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Name</th><th class="px-4 py-2">SKU</th><th class="px-4 py-2">TLS port</th><th class="px-4 py-2">Non-TLS port</th><th class="px-4 py-2">Min TLS</th><th class="px-4 py-2">Persistence</th><th class="px-4 py-2">Keys (db0)</th></tr></thead>
				<tbody>
					{#each caches as c (c.id)}
						<tr class="border-t border-gray-100 dark:border-gray-800 {c.id === selectedId ? 'bg-red-50/60 dark:bg-red-950/20' : ''}" data-testid="redis-cache">
							<td class="px-4 py-2 font-mono"><button class="underline decoration-dotted" onclick={() => pick(c.id)} data-testid="redis-select">{c.name}</button></td>
							<td class="px-4 py-2" data-testid="redis-sku">{c.sku}</td>
							<td class="px-4 py-2 font-mono" data-testid="redis-ssl-port">{c.host}:{c.sslPort}</td>
							<td class="px-4 py-2 font-mono">{c.enableNonSslPort && c.port ? `${c.host}:${c.port}` : 'disabled'}</td>
							<td class="px-4 py-2">{c.minTlsVersion || '-'}</td>
							<td class="px-4 py-2">{c.persistence ? 'On' : 'Off'}</td>
							<td class="px-4 py-2">{c.keyCount}</td>
						</tr>
					{/each}
				</tbody>
			</table>
		</div>

		{#if cache}
			<section class="space-y-2" data-testid="redis-access-keys">
				<h2 class="text-lg font-semibold">Access keys</h2>
				{#if cache.authNotRequired}<p class="text-sm text-gray-500">Authentication is not required for this cache.</p>{/if}
				{#key cache.id}
					<RevealKeys testid="redis-keys" fetchKeys={async () => {
						const k = await revealKeys(cache.id);
						return [{ label: 'Primary', value: k.primary }, { label: 'Secondary', value: k.secondary }];
					}} />
				{/key}
			</section>

			<section class="space-y-3" data-testid="redis-browser">
				<h2 class="text-lg font-semibold">Keys</h2>
				<form class="flex flex-wrap items-end gap-3" onsubmit={(e) => { e.preventDefault(); void scan(); }}>
					<label class="text-sm">Database
						<select class="{GCP_INPUT} !w-28" bind:value={db} data-testid="redis-db">
							{#each Array.from({ length: DATABASES }, (_, i) => i) as i (i)}<option value={i}>db{i}</option>{/each}
						</select>
					</label>
					<label class="text-sm">Pattern<input class="{GCP_INPUT} !w-64 font-mono" bind:value={match} placeholder="user:*" data-testid="redis-pattern" /></label>
					<button type="submit" class={GCP_GHOST} disabled={Boolean(patternError)} data-testid="redis-scan">Scan</button>
					{#if patternError}<span class="text-sm text-red-600">{patternError}</span>{/if}
				</form>

				{#if scanned}
					<div class="overflow-x-auto rounded-lg border border-gray-200 dark:border-gray-700">
						<table class="w-full text-sm" data-testid="redis-keys-table">
							<thead class="text-left text-gray-500 dark:text-gray-400"><tr><th class="px-4 py-2">Key</th><th class="px-4 py-2">Type</th><th class="px-4 py-2">TTL</th><th class="px-4 py-2"></th></tr></thead>
							<tbody>
								{#each keys as k (k.key)}
									<tr class="border-t border-gray-100 dark:border-gray-800" data-testid="redis-key">
										<td class="px-4 py-2 font-mono">{k.key}</td>
										<td class="px-4 py-2">{k.type}</td>
										<td class="px-4 py-2">{ttlLabel(k.ttlSeconds)}</td>
										<td class="px-4 py-2 text-right"><button class={GCP_MINI} onclick={() => view(k)} data-testid="redis-view">View</button></td>
									</tr>
								{:else}
									<tr><td class="px-4 py-6 text-center text-gray-500" colspan="4" data-testid="redis-keys-empty">No keys match.</td></tr>
								{/each}
							</tbody>
						</table>
					</div>
					{#if next}<button class={GCP_GHOST} onclick={() => scan(true)} data-testid="redis-more">Load more</button>{/if}
				{/if}

				{#if value}
					<div class="rounded-lg border border-gray-200 p-3 dark:border-gray-700" data-testid="redis-value">
						<p class="text-sm"><span class="font-mono font-semibold">{value.key}</span> <span class="text-gray-500">({value.type}, {ttlLabel(value.ttlSeconds)}, {value.total} {value.type === 'string' ? 'bytes' : 'elements'})</span></p>
						{#if value.elements.length > 0}
							<ol class="mt-2 list-decimal pl-6 font-mono text-xs break-all">{#each value.elements as e, i (i)}<li>{e}</li>{/each}</ol>
						{:else}
							<pre class="mt-2 max-h-72 overflow-auto font-mono text-xs break-all whitespace-pre-wrap" data-testid="redis-value-text">{value.text}</pre>
						{/if}
						{#if value.truncated}<p class="mt-2 text-xs text-amber-600" data-testid="redis-truncated">Value truncated for display.</p>{/if}
					</div>
				{/if}
			</section>
		{/if}
	{/if}
</div>
