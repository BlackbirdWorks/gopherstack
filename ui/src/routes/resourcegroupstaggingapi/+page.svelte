<script lang="ts">
	import { onMount } from 'svelte';
	import { getResourceGroupsTaggingAPIClient } from '$lib/aws-client';
	import {
		GetResourcesCommand,
		GetTagKeysCommand,
		GetTagValuesCommand,
		GetComplianceSummaryCommand,
		TagResourcesCommand,
		UntagResourcesCommand
	} from '@aws-sdk/client-resource-groups-tagging-api';
	import { toast } from 'svelte-sonner';
	import { Tag, Filter, RefreshCw, Search, ChevronDown, ChevronRight, BarChart3, X, Plus, Trash2 } from 'lucide-svelte';

	const tagging = getResourceGroupsTaggingAPIClient();

	type Resource = { ResourceARN?: string; Tags?: Array<{ Key?: string; Value?: string }> };
	type ComplianceSummaryItem = { NonCompliantResources?: number; Region?: string; ResourceType?: string };

	// Resources
	let resources = $state<Resource[]>([]);
	let loading = $state(false);

	// Filters
	let filterKey = $state('');
	let filterValue = $state('');
	let filterTypeInput = $state('');
	let tagFilters = $state<Array<{ key: string; values: string[] }>>([]);
	let typeFilters = $state<string[]>([]);
	let availableKeys = $state<string[]>([]);
	let availableValues = $state<string[]>([]);

	// Compliance
	let complianceSummary = $state<ComplianceSummaryItem[]>([]);
	let loadingCompliance = $state(false);
	let showCompliance = $state(false);

	// Tag/Untag modal
	let showTagModal = $state(false);
	let tagModalARN = $state('');
	let newTagKey = $state('');
	let newTagValue = $state('');
	let tagging2 = $state(false);

	// Expanded resource ARNs
	let expanded = $state(new Set<string>());

	async function loadResources() {
		loading = true;
		try {
			const awsTagFilters = tagFilters.map((f) => ({
				Key: f.key,
				Values: f.values.length > 0 ? f.values : undefined
			}));
			const out = await tagging.send(
				new GetResourcesCommand({
					TagFilters: awsTagFilters.length > 0 ? awsTagFilters : undefined,
					ResourceTypeFilters: typeFilters.length > 0 ? typeFilters : undefined
				})
			);
			resources = out.ResourceTagMappingList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load tagged resources: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadTagKeys() {
		try {
			const out = await tagging.send(new GetTagKeysCommand({}));
			availableKeys = out.TagKeys ?? [];
		} catch {
			// non-critical
		}
	}

	async function loadTagValues(key: string) {
		if (!key) {
			availableValues = [];
			return;
		}
		try {
			const out = await tagging.send(new GetTagValuesCommand({ Key: key }));
			availableValues = out.TagValues ?? [];
		} catch {
			availableValues = [];
		}
	}

	async function loadCompliance() {
		loadingCompliance = true;
		try {
			const out = await tagging.send(new GetComplianceSummaryCommand({ GroupBy: ['REGION', 'RESOURCE_TYPE'] }));
			complianceSummary = (out.SummaryList ?? []) as ComplianceSummaryItem[];
		} catch (err: unknown) {
			toast.error(`Failed to load compliance: ${(err as Error).message}`);
		} finally {
			loadingCompliance = false;
		}
	}

	function addTagFilter() {
		if (!filterKey.trim()) return;
		const vals = filterValue.trim() ? [filterValue.trim()] : [];
		tagFilters = [...tagFilters, { key: filterKey.trim(), values: vals }];
		filterKey = '';
		filterValue = '';
		availableValues = [];
	}

	function removeTagFilter(idx: number) {
		tagFilters = tagFilters.filter((_, i) => i !== idx);
	}

	function addTypeFilter() {
		const t = filterTypeInput.trim();
		if (!t || typeFilters.includes(t)) return;
		typeFilters = [...typeFilters, t];
		filterTypeInput = '';
	}

	function removeTypeFilter(t: string) {
		typeFilters = typeFilters.filter((x) => x !== t);
	}

	function toggleExpanded(arn: string) {
		const next = new Set(expanded);
		if (next.has(arn)) {
			next.delete(arn);
		} else {
			next.add(arn);
		}
		expanded = next;
	}

	async function applyTagToResource() {
		if (!tagModalARN || !newTagKey.trim()) return;
		tagging2 = true;
		try {
			await tagging.send(
				new TagResourcesCommand({
					ResourceARNList: [tagModalARN],
					Tags: { [newTagKey.trim()]: newTagValue.trim() }
				})
			);
			toast.success('Tag applied');
			showTagModal = false;
			newTagKey = '';
			newTagValue = '';
			await loadResources();
		} catch (err: unknown) {
			toast.error(`Failed to tag resource: ${(err as Error).message}`);
		} finally {
			tagging2 = false;
		}
	}

	async function removeTag(arn: string, key: string) {
		try {
			await tagging.send(
				new UntagResourcesCommand({
					ResourceARNList: [arn],
					TagKeys: [key]
				})
			);
			toast.success(`Removed tag "${key}"`);
			await loadResources();
		} catch (err: unknown) {
			toast.error(`Failed to remove tag: ${(err as Error).message}`);
		}
	}

	$effect(() => {
		void loadTagValues(filterKey);
	});

	onMount(() => {
		void loadResources();
		void loadTagKeys();
	});

	let totalNonCompliant = $derived(
		complianceSummary.reduce((sum, s) => sum + (s.NonCompliantResources ?? 0), 0)
	);
</script>

<div class="space-y-6 p-6">
	<!-- Header -->
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="flex items-center justify-between">
			<div class="flex items-center gap-3">
				<Tag class="h-8 w-8 text-indigo-500" />
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Resource Groups Tagging API</h1>
			</div>
			<div class="flex items-center gap-2">
				<button
					onclick={() => { showCompliance = !showCompliance; if (showCompliance && complianceSummary.length === 0) void loadCompliance(); }}
					class="flex items-center gap-2 rounded-lg border border-slate-200 bg-slate-50 px-3 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:bg-slate-700 dark:text-slate-200 dark:hover:bg-slate-600"
				>
					<BarChart3 class="h-4 w-4" />
					Compliance
				</button>
				<button
					onclick={() => void loadResources()}
					class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700"
					disabled={loading}
				>
					<RefreshCw class="h-4 w-4 {loading ? 'animate-spin' : ''}" />
					Refresh
				</button>
			</div>
		</div>
	</div>

	<!-- Compliance Summary -->
	{#if showCompliance}
		<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<div class="mb-4 flex items-center justify-between">
				<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Compliance Summary</h2>
				{#if loadingCompliance}
					<RefreshCw class="h-4 w-4 animate-spin text-slate-400" />
				{/if}
			</div>
			{#if complianceSummary.length === 0}
				<p class="text-sm text-slate-500 dark:text-slate-400">
					{loadingCompliance ? 'Loading...' : 'No non-compliant resources found.'}
				</p>
			{:else}
				<div class="mb-4 rounded-lg bg-red-50 p-3 dark:bg-red-900/20">
					<p class="text-sm font-medium text-red-700 dark:text-red-400">
						{totalNonCompliant} total non-compliant resource{totalNonCompliant === 1 ? '' : 's'}
					</p>
				</div>
				<div class="space-y-2">
					{#each complianceSummary as item}
						<div class="flex items-center justify-between rounded-lg border border-slate-100 px-3 py-2 dark:border-slate-700">
							<div class="flex items-center gap-4">
								{#if item.Region}
									<span class="rounded bg-slate-100 px-2 py-0.5 text-xs font-mono text-slate-600 dark:bg-slate-700 dark:text-slate-300">{item.Region}</span>
								{/if}
								{#if item.ResourceType}
									<span class="text-sm text-slate-700 dark:text-slate-300">{item.ResourceType}</span>
								{/if}
							</div>
							<span class="rounded-full bg-red-100 px-2.5 py-0.5 text-xs font-semibold text-red-700 dark:bg-red-900/30 dark:text-red-400">
								{item.NonCompliantResources ?? 0} non-compliant
							</span>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	{/if}

	<!-- Filters -->
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<h2 class="mb-4 flex items-center gap-2 text-lg font-semibold text-slate-900 dark:text-white">
			<Filter class="h-5 w-5 text-slate-500" />
			Filters
		</h2>

		<!-- Tag filters -->
		<div class="mb-4">
			<p class="mb-2 text-sm font-medium text-slate-700 dark:text-slate-300">Tag Filters</p>
			<div class="flex flex-wrap gap-2">
				{#each tagFilters as f, idx}
					<span class="flex items-center gap-1 rounded-full bg-indigo-100 px-3 py-1 text-xs font-medium text-indigo-800 dark:bg-indigo-900/30 dark:text-indigo-300">
						{f.key}{f.values.length > 0 ? '=' + f.values.join(',') : ''}
						<button onclick={() => removeTagFilter(idx)} class="ml-1 hover:text-indigo-600">
							<X class="h-3 w-3" />
						</button>
					</span>
				{/each}
			</div>
			<div class="mt-2 flex items-center gap-2">
				<div class="relative flex-1">
					<input
						type="text"
						list="tag-keys"
						bind:value={filterKey}
						placeholder="Tag key"
						class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm focus:border-indigo-400 focus:outline-none dark:border-slate-600 dark:bg-slate-700 dark:text-white"
					/>
					<datalist id="tag-keys">
						{#each availableKeys as k}<option value={k}></option>{/each}
					</datalist>
				</div>
				<div class="relative flex-1">
					<input
						type="text"
						list="tag-values"
						bind:value={filterValue}
						placeholder="Value (optional)"
						class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm focus:border-indigo-400 focus:outline-none dark:border-slate-600 dark:bg-slate-700 dark:text-white"
					/>
					<datalist id="tag-values">
						{#each availableValues as v}<option value={v}></option>{/each}
					</datalist>
				</div>
				<button
					onclick={addTagFilter}
					class="flex items-center gap-1 rounded-lg bg-indigo-600 px-3 py-2 text-sm font-medium text-white hover:bg-indigo-700"
				>
					<Plus class="h-4 w-4" />
					Add
				</button>
			</div>
		</div>

		<!-- Resource type filters -->
		<div class="mb-4">
			<p class="mb-2 text-sm font-medium text-slate-700 dark:text-slate-300">Resource Type Filters</p>
			<div class="flex flex-wrap gap-2">
				{#each typeFilters as t}
					<span class="flex items-center gap-1 rounded-full bg-emerald-100 px-3 py-1 text-xs font-medium text-emerald-800 dark:bg-emerald-900/30 dark:text-emerald-300">
						{t}
						<button onclick={() => removeTypeFilter(t)} class="ml-1 hover:text-emerald-600">
							<X class="h-3 w-3" />
						</button>
					</span>
				{/each}
			</div>
			<div class="mt-2 flex items-center gap-2">
				<input
					type="text"
					bind:value={filterTypeInput}
					placeholder="e.g. ec2:instance"
					class="flex-1 rounded-lg border border-slate-200 px-3 py-2 text-sm focus:border-indigo-400 focus:outline-none dark:border-slate-600 dark:bg-slate-700 dark:text-white"
					onkeydown={(e) => { if (e.key === 'Enter') addTypeFilter(); }}
				/>
				<button
					onclick={addTypeFilter}
					class="flex items-center gap-1 rounded-lg bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-700"
				>
					<Plus class="h-4 w-4" />
					Add
				</button>
			</div>
		</div>

		<button
			onclick={() => void loadResources()}
			class="flex items-center gap-2 rounded-lg bg-slate-800 px-4 py-2 text-sm font-medium text-white hover:bg-slate-700 dark:bg-slate-600 dark:hover:bg-slate-500"
		>
			<Search class="h-4 w-4" />
			Search
		</button>
	</div>

	<!-- Resources List -->
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="mb-4 flex items-center justify-between">
			<h2 class="text-lg font-semibold text-slate-900 dark:text-white">
				Tagged Resources
				{#if resources.length > 0}
					<span class="ml-2 rounded-full bg-slate-100 px-2.5 py-0.5 text-sm font-medium text-slate-600 dark:bg-slate-700 dark:text-slate-300">{resources.length}</span>
				{/if}
			</h2>
			<button
				onclick={() => { showTagModal = true; tagModalARN = ''; newTagKey = ''; newTagValue = ''; }}
				class="flex items-center gap-2 rounded-lg border border-slate-200 px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700"
			>
				<Tag class="h-4 w-4" />
				Tag Resource
			</button>
		</div>

		{#if loading}
			<div class="flex items-center gap-2 text-sm text-slate-500">
				<RefreshCw class="h-4 w-4 animate-spin" />
				Loading...
			</div>
		{:else if resources.length === 0}
			<p class="text-sm text-slate-500 dark:text-slate-400">No tagged resources found</p>
		{:else}
			<div class="space-y-2">
				{#each resources as resource}
					{@const arn = resource.ResourceARN ?? ''}
					{@const isExpanded = expanded.has(arn)}
					<div class="rounded-lg border border-slate-200 dark:border-slate-700">
						<button
							class="flex w-full items-center justify-between p-3 text-left hover:bg-slate-50 dark:hover:bg-slate-700/50"
							onclick={() => toggleExpanded(arn)}
						>
							<div class="min-w-0 flex-1">
								<div class="truncate font-medium text-slate-900 dark:text-white">{arn}</div>
								<div class="mt-0.5 flex flex-wrap gap-1">
									{#each resource.Tags ?? [] as tag}
										<span class="rounded bg-slate-100 px-1.5 py-0.5 text-xs text-slate-600 dark:bg-slate-700 dark:text-slate-300">
											{tag.Key}={tag.Value}
										</span>
									{/each}
								</div>
							</div>
							{#if isExpanded}
								<ChevronDown class="ml-2 h-4 w-4 flex-shrink-0 text-slate-400" />
							{:else}
								<ChevronRight class="ml-2 h-4 w-4 flex-shrink-0 text-slate-400" />
							{/if}
						</button>
						{#if isExpanded}
							<div class="border-t border-slate-100 p-3 dark:border-slate-700">
								<div class="mb-2 flex items-center justify-between">
									<p class="text-xs font-semibold uppercase tracking-wide text-slate-500">Tags</p>
									<button
										onclick={() => { showTagModal = true; tagModalARN = arn; newTagKey = ''; newTagValue = ''; }}
										class="flex items-center gap-1 rounded px-2 py-1 text-xs text-indigo-600 hover:bg-indigo-50 dark:text-indigo-400 dark:hover:bg-indigo-900/20"
									>
										<Plus class="h-3 w-3" />
										Add tag
									</button>
								</div>
								{#if (resource.Tags ?? []).length === 0}
									<p class="text-xs text-slate-400">No tags</p>
								{:else}
									<div class="space-y-1">
										{#each resource.Tags ?? [] as tag}
											<div class="flex items-center justify-between rounded bg-slate-50 px-2 py-1 dark:bg-slate-700">
												<span class="text-xs font-mono">
													<span class="text-slate-700 dark:text-slate-200">{tag.Key}</span>
													<span class="text-slate-400 dark:text-slate-500"> = </span>
													<span class="text-slate-700 dark:text-slate-200">{tag.Value}</span>
												</span>
												<button
													onclick={() => void removeTag(arn, tag.Key ?? '')}
													class="ml-2 text-slate-400 hover:text-red-500"
												>
													<Trash2 class="h-3 w-3" />
												</button>
											</div>
										{/each}
									</div>
								{/if}
							</div>
						{/if}
					</div>
				{/each}
			</div>
		{/if}
	</div>
</div>

<!-- Tag Resource Modal -->
{#if showTagModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h3 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Tag Resource</h3>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300">Resource ARN</label>
					<input
						type="text"
						bind:value={tagModalARN}
						placeholder="arn:aws:..."
						class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm focus:border-indigo-400 focus:outline-none dark:border-slate-600 dark:bg-slate-700 dark:text-white"
					/>
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300">Tag Key</label>
					<input
						type="text"
						list="tag-keys-modal"
						bind:value={newTagKey}
						placeholder="Key"
						class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm focus:border-indigo-400 focus:outline-none dark:border-slate-600 dark:bg-slate-700 dark:text-white"
					/>
					<datalist id="tag-keys-modal">
						{#each availableKeys as k}<option value={k}></option>{/each}
					</datalist>
				</div>
				<div>
					<label class="mb-1 block text-sm font-medium text-slate-700 dark:text-slate-300">Tag Value</label>
					<input
						type="text"
						bind:value={newTagValue}
						placeholder="Value"
						class="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm focus:border-indigo-400 focus:outline-none dark:border-slate-600 dark:bg-slate-700 dark:text-white"
					/>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => { showTagModal = false; }}
					class="rounded-lg border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700"
				>
					Cancel
				</button>
				<button
					onclick={() => void applyTagToResource()}
					disabled={tagging2 || !tagModalARN || !newTagKey.trim()}
					class="flex items-center gap-2 rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
				>
					{#if tagging2}
						<RefreshCw class="h-4 w-4 animate-spin" />
					{/if}
					Apply Tag
				</button>
			</div>
		</div>
	</div>
{/if}
