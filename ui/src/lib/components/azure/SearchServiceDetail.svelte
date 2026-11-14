<script lang="ts">
	import { onMount } from 'svelte';
	import SearchExplorer from './SearchExplorer.svelte';
	import SearchIndexers from './SearchIndexers.svelte';
	import SearchIndexes from './SearchIndexes.svelte';
	import SearchKeys from './SearchKeys.svelte';
	import SearchObjects from './SearchObjects.svelte';
	import Tabs from '#lib/components/Tabs.svelte';
	import { describeGcpError } from '#lib/gcprest.ts';
	import { formatBytes, getServiceStats, type SearchService, type SearchTarget, type ServiceStats } from '#lib/azuresearch.ts';

	const TABS = [
		{ id: 'overview', label: 'Overview' },
		{ id: 'keys', label: 'Keys' },
		{ id: 'indexes', label: 'Indexes' },
		{ id: 'explorer', label: 'Search explorer' },
		{ id: 'indexers', label: 'Indexers' },
		{ id: 'objects', label: 'Data sources & skills' }
	];

	let { service, target }: { service: SearchService; target: SearchTarget } = $props();

	let tab = $state('overview');
	let stats = $state<ServiceStats>();
	let error = $state('');

	async function loadStats() {
		try {
			stats = await getServiceStats(target);
			error = '';
		} catch (e) {
			error = describeGcpError(e);
		}
	}

	onMount(loadStats);

	const tile = 'rounded-md border border-gray-200 p-3 dark:border-gray-700';
</script>

<div class="space-y-4 rounded-lg border border-gray-200 p-4 dark:border-gray-700" data-testid="search-detail">
	<h2 class="text-lg font-semibold text-gray-900 dark:text-white">{service.name}</h2>
	<Tabs tabs={TABS} active={tab} onSelect={(id) => (tab = id)} color="sky" />
	{#if tab === 'overview'}
		{#if error}<p class="text-sm text-red-600" role="alert" data-testid="search-stats-error">{error}</p>{/if}
		<dl class="grid grid-cols-2 gap-3 text-sm md:grid-cols-4" data-testid="search-overview">
			<div class={tile}><dt class="text-xs text-gray-500">Documents</dt><dd class="text-lg font-semibold" data-testid="search-stat-documents">{stats?.documents ?? '—'}</dd></div>
			<div class={tile}><dt class="text-xs text-gray-500">Storage</dt><dd class="text-lg font-semibold" data-testid="search-stat-storage">{stats ? formatBytes(stats.storage) : '—'}</dd></div>
			<div class={tile}><dt class="text-xs text-gray-500">Indexes</dt><dd class="text-lg font-semibold" data-testid="search-stat-indexes">{stats?.indexes ?? '—'}{stats?.indexesQuota !== undefined ? ` / ${stats.indexesQuota}` : ''}</dd></div>
			<div class={tile}><dt class="text-xs text-gray-500">Indexers</dt><dd class="text-lg font-semibold">{stats?.indexers ?? '—'}</dd></div>
			<div class={tile}><dt class="text-xs text-gray-500">Data sources</dt><dd class="text-lg font-semibold">{stats?.dataSources ?? '—'}</dd></div>
			<div class={tile}><dt class="text-xs text-gray-500">Skillsets</dt><dd class="text-lg font-semibold">{stats?.skillsets ?? '—'}</dd></div>
			<div class={tile}><dt class="text-xs text-gray-500">Synonym maps</dt><dd class="text-lg font-semibold">{stats?.synonymMaps ?? '—'}</dd></div>
			<div class={tile}><dt class="text-xs text-gray-500">Hosting / access</dt><dd class="text-sm">{service.hostingMode || 'default'} · {service.publicAccess || 'enabled'}</dd></div>
		</dl>
	{:else if tab === 'keys'}
		<SearchKeys {target} />
	{:else if tab === 'indexes'}
		<SearchIndexes {target} />
	{:else if tab === 'explorer'}
		<SearchExplorer {target} />
	{:else if tab === 'indexers'}
		<SearchIndexers {target} />
	{:else}
		<SearchObjects {target} />
	{/if}
</div>
