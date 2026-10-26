<script lang="ts">
	import { onMount } from 'svelte';
	import { Database } from 'lucide-svelte';
	import PageHeader from '#lib/components/PageHeader.svelte';
	import GcpScopeBar from '#lib/components/gcp/GcpScopeBar.svelte';
	import GcpResourceTable, { type ResourceRow } from '#lib/components/gcp/GcpResourceTable.svelte';
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { attempt, copyText, GCP_MINI } from '#lib/gcp-action.ts';
	import {
		deleteInstance,
		endpointOf,
		getAuthString,
		listInstances,
		readStoredRedisLocation,
		readStoredRedisProject,
		writeStoredRedisScope,
		type RedisInstance
	} from '#lib/gcpredis.ts';
	import { describeGcpError } from '#lib/gcprest.ts';

	let project = $state(readStoredRedisProject());
	let location = $state(readStoredRedisLocation());
	let instances = $state<RedisInstance[]>([]);
	let loading = $state(true);
	let error = $state('');
	let secrets = $state<Record<string, string>>({});
	let secretError = $state('');

	async function load() {
		try {
			instances = (await listInstances({ project, location })).toSorted((a, b) => a.id.localeCompare(b.id));
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		} finally {
			loading = false;
		}
	}

	function applyScope(p: string, l: string) {
		project = p;
		location = l;
		writeStoredRedisScope(p, l);
		secrets = {};
		loading = true;
		void load();
	}

	async function remove(row: ResourceRow) {
		if (!(await confirmDestructive({ title: 'Delete instance', message: `Delete Redis instance "${row.cells[0]}"? Its data is lost.` }))) return;
		if (await attempt(() => deleteInstance(row.id), `Deleted ${row.cells[0]}`, `Failed to delete ${row.cells[0]}`)) await load();
	}

	async function reveal(i: RedisInstance) {
		secretError = '';
		try {
			secrets = { ...secrets, [i.name]: await getAuthString(i.name) };
		} catch (e) {
			secretError = describeGcpError(e);
		}
	}

	const byName = (name: string) => instances.find((i) => i.name === name);

	const rows = $derived(
		instances.map((i) => ({
			id: i.name,
			cells: [i.id, i.state, `${i.tier} ${i.memorySizeGb} GB`, endpointOf(i.host, i.port), i.authEnabled ? 'enabled' : 'disabled'],
			raw: i.raw
		}))
	);

	onMount(load);
</script>

<div class="space-y-6 pb-20" data-testid="redis-dashboard">
	<PageHeader icon={Database} title="Memorystore Redis" description="Google Memorystore for Redis: instances and endpoints" color="red" onRefresh={load} />
	<GcpScopeBar prefix="redis" {project} {location} hint="Instances are listed per project and location." onApply={applyScope} />
	<GcpResourceTable testId="redis-instance" noun="instances" columns={['Instance', 'State', 'Tier / size', 'Endpoint', 'AUTH']} {rows} {loading} {error} onRetry={load} onDelete={remove}>
		{#snippet detail(row)}
			{@const i = byName(row.id)}
			{#if i}
				<dl class="grid grid-cols-[max-content_1fr] gap-x-4 gap-y-1" data-testid="redis-info">
					<dt class="font-medium text-gray-500 dark:text-gray-400">Version</dt><dd>{i.redisVersion || '—'}</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">Primary endpoint</dt>
					<dd class="font-mono">{endpointOf(i.host, i.port) || '—'} {#if i.host}<button class={GCP_MINI} onclick={() => copyText(endpointOf(i.host, i.port))}>Copy</button>{/if}</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">Read endpoint</dt><dd class="font-mono">{endpointOf(i.readEndpoint, i.readEndpointPort) || '—'}</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">Location</dt><dd>{i.locationId || '—'}</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">Network</dt><dd class="font-mono break-all">{i.authorizedNetwork || '—'} ({i.connectMode || '—'})</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">Transit encryption</dt><dd>{i.transitEncryption} ({i.caCerts} CA certificate{i.caCerts === 1 ? '' : 's'})</dd>
					<dt class="font-medium text-gray-500 dark:text-gray-400">AUTH</dt>
					<dd>
						{#if !i.authEnabled}
							disabled
						{:else if i.name in secrets}
							<code class="font-mono break-all" data-testid="redis-auth-value">{secrets[i.name]}</code> <button class={GCP_MINI} onclick={() => copyText(secrets[i.name])}>Copy</button>
						{:else}
							enabled <button class={GCP_MINI} onclick={() => reveal(i)} data-testid="redis-auth-reveal">Show AUTH string</button>
						{/if}
						{#if secretError}<span class="text-red-600" role="alert">{secretError}</span>{/if}
					</dd>
				</dl>
			{/if}
		{/snippet}
	</GcpResourceTable>
</div>
