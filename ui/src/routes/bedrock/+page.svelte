<script lang="ts">
	import { onMount } from 'svelte';
	import { getBedrockClient } from '$lib/aws-client';
	import {
		ListFoundationModelsCommand,
		ListCustomModelsCommand,
		GetFoundationModelCommand,
		type FoundationModelSummary,
		type CustomModelSummary
	} from '@aws-sdk/client-bedrock';
	import { toast } from 'svelte-sonner';
	import {
		Brain,
		Search,
		RefreshCw,
		Eye,
		Sparkles,
		Cpu,
		CheckCircle,
		XCircle,
		Filter
	} from 'lucide-svelte';

	const bedrock = getBedrockClient();

	let loading = $state(false);
	let activeTab = $state<'foundation' | 'custom'>('foundation');
	let searchQuery = $state('');
	let modalityFilter = $state('all');
	let providerFilter = $state('all');

	// Foundation Models
	let foundationModels = $state<FoundationModelSummary[]>([]);
	let selectedModel = $state<FoundationModelSummary | null>(null);
	let modelDetail = $state<object | null>(null);
	let loadingDetail = $state(false);

	// Custom Models
	let customModels = $state<CustomModelSummary[]>([]);

	const providers = $derived([...new Set(foundationModels.map((m) => m.providerName ?? 'Unknown'))].sort());
	const modalities = $derived([...new Set(foundationModels.flatMap((m) => m.inputModalities ?? []))].sort());

	const filteredFoundation = $derived(
		foundationModels.filter((m) => {
			const nameMatch =
				(m.modelName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(m.providerName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(m.modelId ?? '').toLowerCase().includes(searchQuery.toLowerCase());
			const providerMatch = providerFilter === 'all' || m.providerName === providerFilter;
			const modalityMatch =
				modalityFilter === 'all' || (m.inputModalities as string[] ?? []).includes(modalityFilter);
			return nameMatch && providerMatch && modalityMatch;
		})
	);

	const filteredCustom = $derived(
		customModels.filter(
			(m) =>
				(m.modelName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(m.baseModelArn ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function statusBadge(status?: string) {
		if (status === 'ACTIVE') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (status === 'LEGACY') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadFoundationModels() {
		loading = true;
		try {
			const res = await bedrock.send(new ListFoundationModelsCommand({}));
			foundationModels = res.modelSummaries ?? [];
		} catch (e) {
			toast.error(`Failed to load foundation models: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function loadCustomModels() {
		loading = true;
		try {
			const res = await bedrock.send(new ListCustomModelsCommand({ maxResults: 100 }));
			customModels = res.modelSummaries ?? [];
		} catch (e) {
			toast.error(`Failed to load custom models: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewModelDetail(model: FoundationModelSummary) {
		selectedModel = model;
		if (!model.modelArn) return;
		loadingDetail = true;
		modelDetail = null;
		try {
			const res = await bedrock.send(
				new GetFoundationModelCommand({ modelIdentifier: model.modelId ?? '' })
			);
			modelDetail = res.modelDetails ?? null;
		} catch (e) {
			toast.error(`Failed to load model details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function onTabChange(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedModel = null;
		if (tab === 'foundation') await loadFoundationModels();
		else await loadCustomModels();
	}

	onMount(() => loadFoundationModels());
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Brain class="h-8 w-8 text-violet-600" />
			<div>
				<h1 class="text-2xl font-bold">Amazon Bedrock</h1>
				<p class="text-sm text-muted-foreground">
					Foundation models and generative AI capabilities
				</p>
			</div>
		</div>
		<button
			onclick={() => onTabChange(activeTab)}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<!-- Summary Cards -->
	{#if foundationModels.length > 0}
		<div class="grid gap-4 sm:grid-cols-3">
			<div class="rounded-lg border p-4 text-center">
				<div class="text-3xl font-bold text-violet-600">{foundationModels.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Foundation Models</div>
			</div>
			<div class="rounded-lg border p-4 text-center">
				<div class="text-3xl font-bold text-violet-600">{providers.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Providers</div>
			</div>
			<div class="rounded-lg border p-4 text-center">
				<div class="text-3xl font-bold text-violet-600">{customModels.length}</div>
				<div class="text-sm text-muted-foreground mt-1">Custom Models</div>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="flex border-b">
		{#each [{ id: 'foundation', label: 'Foundation Models', icon: Sparkles }, { id: 'custom', label: 'Custom Models', icon: Cpu }] as tab}
			<button
				onclick={() => onTabChange(tab.id as typeof activeTab)}
				class="flex items-center gap-2 px-4 py-2 text-sm font-medium border-b-2 transition-colors {activeTab === tab.id ? 'border-primary text-primary' : 'border-transparent text-muted-foreground hover:text-foreground'}"
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Foundation Models Tab -->
	{#if activeTab === 'foundation'}
		<div class="flex flex-wrap gap-3">
			<div class="relative flex-1 min-w-[200px]">
				<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
				<input
					type="text"
					placeholder="Search models..."
					bind:value={searchQuery}
					class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
				/>
			</div>
			<select
				bind:value={providerFilter}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Providers</option>
				{#each providers as provider}
					<option value={provider}>{provider}</option>
				{/each}
			</select>
			<select
				bind:value={modalityFilter}
				class="rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			>
				<option value="all">All Modalities</option>
				{#each modalities as modality}
					<option value={modality}>{modality}</option>
				{/each}
			</select>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredFoundation.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Brain class="h-12 w-12 mb-3 opacity-30" />
				<p>No foundation models found</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Model Name</th>
							<th class="px-4 py-3 text-left font-medium">Provider</th>
							<th class="px-4 py-3 text-left font-medium">Input Modalities</th>
							<th class="px-4 py-3 text-left font-medium">Status</th>
							<th class="px-4 py-3 text-right font-medium">Details</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredFoundation as model}
							<tr
								class="hover:bg-muted/30 cursor-pointer"
								onclick={() => viewModelDetail(model)}
							>
								<td class="px-4 py-3">
									<div class="font-medium">{model.modelName}</div>
									<div class="text-xs text-muted-foreground font-mono">{model.modelId}</div>
								</td>
								<td class="px-4 py-3 text-muted-foreground">{model.providerName}</td>
								<td class="px-4 py-3">
									<div class="flex flex-wrap gap-1">
										{#each model.inputModalities ?? [] as modality}
											<span class="rounded bg-muted px-1.5 py-0.5 text-xs">{modality}</span>
										{/each}
									</div>
								</td>
								<td class="px-4 py-3">
									<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(model.modelLifecycle?.status)}">
										{model.modelLifecycle?.status ?? 'ACTIVE'}
									</span>
								</td>
								<td class="px-4 py-3 text-right">
									<button
										onclick={(e) => { e.stopPropagation(); viewModelDetail(model); }}
										class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
										title="View details"
									>
										<Eye class="h-4 w-4" />
									</button>
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>

			<!-- Model Details -->
			{#if selectedModel}
				<div class="rounded-lg border p-5 space-y-4">
					<div class="flex items-center justify-between">
						<div class="flex items-center gap-2">
							<Sparkles class="h-5 w-5 text-violet-500" />
							<h3 class="font-semibold">{selectedModel.modelName}</h3>
						</div>
						<button onclick={() => (selectedModel = null)} class="text-xs text-muted-foreground hover:text-foreground">
							Close
						</button>
					</div>
					<div class="grid grid-cols-2 gap-3 text-sm">
						<div>
							<p class="text-muted-foreground">Model ID</p>
							<p class="font-mono text-xs">{selectedModel.modelId}</p>
						</div>
						<div>
							<p class="text-muted-foreground">Provider</p>
							<p>{selectedModel.providerName}</p>
						</div>
						<div>
							<p class="text-muted-foreground">Customizations</p>
							<div class="flex flex-wrap gap-1 mt-1">
								{#each selectedModel.customizationsSupported ?? [] as c}
									<span class="rounded bg-muted px-1.5 py-0.5 text-xs">{c}</span>
								{:else}
									<span class="text-muted-foreground text-xs">None</span>
								{/each}
							</div>
						</div>
						<div>
							<p class="text-muted-foreground">Inference Types</p>
							<div class="flex flex-wrap gap-1 mt-1">
								{#each selectedModel.inferenceTypesSupported ?? [] as t}
									<span class="rounded bg-muted px-1.5 py-0.5 text-xs">{t}</span>
								{:else}
									<span class="text-muted-foreground text-xs">None</span>
								{/each}
							</div>
						</div>
					</div>
					<div class="flex items-center gap-4 text-sm">
						<div class="flex items-center gap-1">
							{#if selectedModel.responseStreamingSupported}
								<CheckCircle class="h-4 w-4 text-green-500" />
								<span>Streaming supported</span>
							{:else}
								<XCircle class="h-4 w-4 text-muted-foreground" />
								<span class="text-muted-foreground">No streaming</span>
							{/if}
						</div>
					</div>
				</div>
			{/if}
		{/if}
	{/if}

	<!-- Custom Models Tab -->
	{#if activeTab === 'custom'}
		<div class="relative">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search custom models..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>

		{#if loading}
			<div class="flex justify-center py-12">
				<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
			</div>
		{:else if filteredCustom.length === 0}
			<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
				<Cpu class="h-12 w-12 mb-3 opacity-30" />
				<p>No custom models found</p>
				<p class="text-sm">Fine-tune a foundation model to create a custom model</p>
			</div>
		{:else}
			<div class="rounded-lg border overflow-hidden">
				<table class="w-full text-sm">
					<thead class="bg-muted/50">
						<tr>
							<th class="px-4 py-3 text-left font-medium">Model Name</th>
							<th class="px-4 py-3 text-left font-medium">Base Model</th>
							<th class="px-4 py-3 text-left font-medium">Created</th>
						</tr>
					</thead>
					<tbody class="divide-y">
						{#each filteredCustom as model}
							<tr class="hover:bg-muted/30">
								<td class="px-4 py-3 font-medium">{model.modelName}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs font-mono">{model.baseModelArn}</td>
								<td class="px-4 py-3 text-muted-foreground text-xs">
									{model.creationTime ? new Date(model.creationTime).toLocaleDateString() : '—'}
								</td>
							</tr>
						{/each}
					</tbody>
				</table>
			</div>
		{/if}
	{/if}
</div>
