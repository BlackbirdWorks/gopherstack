<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onMount } from 'svelte';
	import { getSSMClient } from '$lib/aws-client';
	import {
		DescribeParametersCommand,
		GetParameterCommand,
		PutParameterCommand,
		DeleteParameterCommand,
		GetParametersByPathCommand,
		type ParameterMetadata
	} from '@aws-sdk/client-ssm';
	import { toast } from 'svelte-sonner';
	import { Settings, Search, RefreshCw, Plus, Trash2, Eye, EyeOff, Edit2, Check, X } from 'lucide-svelte';

	const ssm = getSSMClient();

	let loading = $state(false);
	let parameters = $state<ParameterMetadata[]>([]);
	let searchPath = $state('');
	let selectedParam = $state<ParameterMetadata | null>(null);
	let selectedValue = $state<string | null>(null);
	let loadingValue = $state(false);
	let showValue = $state(false);

	// Create/Edit modal
	let showModal = $state(false);
	let saving = $state(false);
	let isEditing = $state(false);
	let modalName = $state('');
	let modalValue = $state('');
	let modalType = $state<'String' | 'StringList' | 'SecureString'>('String');
	let modalDescription = $state('');
	let modalTier = $state<'Standard' | 'Advanced'>('Standard');
	let modalOverwrite = $state(false);

	const typeColors: Record<string, string> = {
		String: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300',
		StringList: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300',
		SecureString: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300'
	};

	async function loadParameters() {
		loading = true;
		try {
			if (searchPath.trim().startsWith('/')) {
				// Path-based search
				const res = await ssm.send(new GetParametersByPathCommand({
					Path: searchPath.trim(),
					Recursive: true,
					WithDecryption: false,
					MaxResults: 50
				}));
				parameters = (res.Parameters ?? []).map((p) => ({
					Name: p.Name,
					Type: p.Type,
					LastModifiedDate: p.LastModifiedDate,
					ARN: p.ARN,
					DataType: p.DataType,
					Version: p.Version
				}));
			} else {
				const filters = searchPath.trim()
					? [{ Key: 'Name', Option: 'Contains', Values: [searchPath.trim()] }]
					: undefined;
				const res = await ssm.send(new DescribeParametersCommand({
					ParameterFilters: filters,
					MaxResults: 50
				}));
				parameters = res.Parameters ?? [];
			}
		} catch (err: unknown) {
			toast.error(`Failed to load parameters: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function getValue(param: ParameterMetadata) {
		selectedParam = param;
		selectedValue = null;
		showValue = false;
		loadingValue = true;
		try {
			const res = await ssm.send(new GetParameterCommand({
				Name: param.Name,
				WithDecryption: true
			}));
			selectedValue = res.Parameter?.Value ?? '';
		} catch (err: unknown) {
			toast.error(`Failed to get value: ${(err as Error).message}`);
		} finally {
			loadingValue = false;
		}
	}

	function openCreate() {
		isEditing = false;
		modalName = '';
		modalValue = '';
		modalType = 'String';
		modalDescription = '';
		modalTier = 'Standard';
		modalOverwrite = false;
		showModal = true;
	}

	function openEdit(param: ParameterMetadata) {
		isEditing = true;
		modalName = param.Name ?? '';
		modalValue = selectedValue ?? '';
		modalType = (param.Type as 'String' | 'StringList' | 'SecureString') ?? 'String';
		modalDescription = '';
		modalTier = 'Standard';
		modalOverwrite = true;
		showModal = true;
	}

	async function save() {
		if (!modalName.trim() || !modalValue.trim()) return;
		saving = true;
		try {
			await ssm.send(new PutParameterCommand({
				Name: modalName.trim(),
				Value: modalValue,
				Type: modalType,
				Description: modalDescription || undefined,
				Tier: modalTier,
				Overwrite: modalOverwrite
			}));
			toast.success(`Parameter "${modalName.trim()}" ${isEditing ? 'updated' : 'created'}`);
			showModal = false;
			await loadParameters();
		} catch (err: unknown) {
			toast.error(`Save failed: ${(err as Error).message}`);
		} finally {
			saving = false;
		}
	}

	async function deleteParameter(name: string) {
		if (!await confirmDestructive(`Delete parameter "${name}"?`)) return;
		try {
			await ssm.send(new DeleteParameterCommand({ Name: name }));
			toast.success(`Parameter "${name}" deleted`);
			if (selectedParam?.Name === name) { selectedParam = null; selectedValue = null; }
			await loadParameters();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${(err as Error).message}`);
		}
	}

	function formatDate(d?: Date): string {
		return d ? new Date(d).toLocaleString() : 'N/A';
	}

	onMount(() => { loadParameters(); });
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-teal-100 dark:bg-teal-900/30 rounded-lg">
				<Settings class="w-6 h-6 text-teal-600 dark:text-teal-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">SSM Parameter Store</h1>
				<p class="text-slate-600 dark:text-slate-300">Secure configuration and secrets management</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button onclick={() => loadParameters()} class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white" title="Refresh">
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
			<button onclick={openCreate} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Create Parameter
			</button>
		</div>
	</div>

	<!-- Search -->
	<div class="flex gap-2">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input
				type="text"
				bind:value={searchPath}
				placeholder="Search by name or /path/prefix..."
				class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500"
			/>
		</div>
		<button onclick={() => loadParameters()} class="px-4 py-2 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-lg hover:bg-slate-200 dark:hover:bg-slate-600">
			Search
		</button>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- Parameter List -->
		<div class="lg:col-span-1">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading parameters...</p>
				</div>
			{:else if parameters.length === 0}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
					<Settings class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
					<p class="text-slate-500 dark:text-slate-400">No parameters found</p>
				</div>
			{:else}
				<div class="space-y-2">
					{#each parameters as param}
						<button
							onclick={() => getValue(param)}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-indigo-400 transition-colors {selectedParam?.Name === param.Name ? 'border-indigo-500 ring-1 ring-indigo-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-start justify-between">
								<div class="min-w-0 flex-1">
									<p class="font-medium text-slate-900 dark:text-white text-sm truncate">{param.Name}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{formatDate(param.LastModifiedDate)}</p>
								</div>
								<span class="ml-2 flex-shrink-0 px-2 py-0.5 text-xs rounded-full {typeColors[param.Type ?? 'String'] ?? typeColors.String}">{param.Type}</span>
							</div>
						</button>
					{/each}
				</div>
			{/if}
		</div>

		<!-- Parameter Detail -->
		<div class="lg:col-span-2">
			{#if selectedParam}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-lg font-bold text-slate-900 dark:text-white font-mono break-all">{selectedParam.Name}</h2>
							<span class="mt-1 inline-block px-2 py-0.5 text-xs rounded-full {typeColors[selectedParam.Type ?? 'String'] ?? typeColors.String}">{selectedParam.Type}</span>
						</div>
						<div class="flex gap-2">
							<button
								onclick={() => openEdit(selectedParam!)}
								class="px-3 py-1.5 bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-300 rounded-lg hover:bg-indigo-200 dark:hover:bg-indigo-900/50 flex items-center gap-1.5 text-sm"
							>
								<Edit2 class="w-4 h-4" />Edit
							</button>
							<button
								onclick={() => deleteParameter(selectedParam?.Name ?? '')}
								class="px-3 py-1.5 bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300 rounded-lg hover:bg-red-200 flex items-center gap-1.5 text-sm"
							>
								<Trash2 class="w-4 h-4" />Delete
							</button>
						</div>
					</div>

					<div class="grid grid-cols-2 gap-3">
						<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
							<p class="text-xs text-slate-500 dark:text-slate-400">Last Modified</p>
							<p class="text-sm font-medium text-slate-900 dark:text-white mt-0.5">{formatDate(selectedParam.LastModifiedDate)}</p>
						</div>
						<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
							<p class="text-xs text-slate-500 dark:text-slate-400">Version</p>
							<p class="text-sm font-medium text-slate-900 dark:text-white mt-0.5">{selectedParam.Version ?? 'N/A'}</p>
						</div>
					</div>

					<!-- Value -->
					<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-4">
						<div class="flex items-center justify-between mb-2">
							<p class="text-sm font-medium text-slate-700 dark:text-slate-300">Value</p>
							{#if selectedParam.Type === 'SecureString'}
								<button onclick={() => { showValue = !showValue; }} class="flex items-center gap-1 text-xs text-slate-500 dark:text-slate-400 hover:text-indigo-500">
									{#if showValue}
										<EyeOff class="w-4 h-4" />Hide
									{:else}
										<Eye class="w-4 h-4" />Show
									{/if}
								</button>
							{/if}
						</div>
						{#if loadingValue}
							<div class="inline-block animate-spin rounded-full h-4 w-4 border-b-2 border-indigo-500"></div>
						{:else if selectedValue !== null}
							{#if selectedParam.Type === 'SecureString' && !showValue}
								<p class="font-mono text-sm text-slate-500 dark:text-slate-400">••••••••••••</p>
							{:else}
								<pre class="font-mono text-sm text-slate-900 dark:text-white whitespace-pre-wrap break-all">{selectedValue}</pre>
							{/if}
						{/if}
					</div>
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<Settings class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a parameter to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create/Edit Modal -->
{#if showModal}
	<div class="fixed inset-0 bg-black/50 flex items-center justify-center z-50">
		<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl p-6 w-full max-w-lg">
			<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">{isEditing ? 'Edit Parameter' : 'Create Parameter'}</h2>
			<form onsubmit={(e) => { e.preventDefault(); save(); }} class="space-y-4">
				<div>
					<label for="ssm-param-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Name</label>
					<input
						id="ssm-param-name"
						type="text"
						bind:value={modalName}
						placeholder="e.g. /myapp/database/password"
						disabled={isEditing}
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-60"
						required
					/>
				</div>
				<div>
					<label for="ssm-param-value" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Value</label>
					<textarea
						id="ssm-param-value"
						bind:value={modalValue}
						rows={3}
						placeholder="Parameter value..."
						class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 font-mono text-sm resize-none"
						required
					></textarea>
				</div>
				<div class="grid grid-cols-2 gap-4">
					<div>
						<label for="ssm-param-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Type</label>
						<select id="ssm-param-type" bind:value={modalType} disabled={isEditing} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500 disabled:opacity-60">
							<option value="String">String</option>
							<option value="StringList">StringList</option>
							<option value="SecureString">SecureString</option>
						</select>
					</div>
					<div>
						<label for="ssm-param-tier" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Tier</label>
						<select id="ssm-param-tier" bind:value={modalTier} class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500">
							<option value="Standard">Standard</option>
							<option value="Advanced">Advanced</option>
						</select>
					</div>
				</div>
				<div>
					<label for="ssm-param-desc" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Description (optional)</label>
					<input id="ssm-param-desc" type="text" bind:value={modalDescription} placeholder="Parameter description..." class="w-full px-3 py-2 bg-slate-50 dark:bg-slate-700 border border-slate-200 dark:border-slate-600 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-indigo-500" />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => { showModal = false; }} class="px-4 py-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white flex items-center gap-1.5">
						<X class="w-4 h-4" />Cancel
					</button>
					<button type="submit" disabled={saving} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 disabled:opacity-50 flex items-center gap-2">
						<Check class="w-4 h-4" />
						{saving ? 'Saving...' : (isEditing ? 'Update' : 'Create')}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
