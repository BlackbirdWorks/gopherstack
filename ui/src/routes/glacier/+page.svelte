<script lang="ts">
	import { onMount } from 'svelte';
	import { getGlacierClient } from '$lib/aws-client';
	import { ListVaultsCommand, type DescribeVaultOutput } from '@aws-sdk/client-glacier';
	import { toast } from 'svelte-sonner';
	import { Archive, Info, CheckCircle, Database, Search, RefreshCw, Box } from 'lucide-svelte';
	
	const glacier = getGlacierClient();

	let loading = $state(false);
	let vaults = $state<DescribeVaultOutput[]>([]);
	let searchQuery = $state('');

	const filteredVaults = $derived(
		vaults.filter(v => 
			(v.VaultName ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const storageClasses = [
		{ name: 'S3 Glacier Instant Retrieval', retrieval: 'Milliseconds', minStorage: '90 days', cost: 'Low', useCase: 'Rarely accessed data requiring immediate retrieval' },
		{ name: 'S3 Glacier Flexible Retrieval', retrieval: '3-5 hours (Standard)', minStorage: '90 days', cost: 'Very Low', useCase: 'Archive data, 1-2 retrievals per year' },
		{ name: 'S3 Glacier Deep Archive', retrieval: '12 hours (Standard)', minStorage: '180 days', cost: 'Lowest', useCase: 'Long-term retention, accessed 1-2x per year' }
	];

	async function loadVaults() {
		loading = true;
		try {
			// Note: SDK uses "-" for accountId to mean current account
			const res = await glacier.send(new ListVaultsCommand({ accountId: '-' }));
			vaults = res.VaultList ?? [];
		} catch (err: any) {
			toast.error(`Failed to load vaults: ${err.message}`);
		} finally {
			loading = false;
		}
	}

	onMount(loadVaults);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Archive class="w-7 h-7 text-blue-800 dark:text-blue-300" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">Amazon S3 Glacier</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Secure, durable, and extremely low-cost cloud storage for data archiving and long-term backup</p>
			</div>
		</div>
		<button onclick={loadVaults} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4 {loading ? 'animate-spin' : ''}" /> Refresh
		</button>
	</div>

	<div class="bg-blue-50 dark:bg-blue-900/20 border border-blue-200 dark:border-blue-700 rounded-lg p-4 flex items-start gap-3">
		<Info class="w-5 h-5 text-blue-600 dark:text-blue-400 mt-0.5 shrink-0" />
		<div>
			<p class="text-sm font-medium text-blue-800 dark:text-blue-300">Managed via Amazon S3</p>
			<p class="text-sm text-blue-700 dark:text-blue-400 mt-1">Amazon S3 Glacier storage classes are managed through Amazon S3. Use the S3 console to configure lifecycle policies to automatically archive objects. Direct vault management via Glacier API is also supported below.</p>
		</div>
	</div>

	<!-- Vaults List -->
	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex justify-between items-center">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Vaults</h2>
			<div class="relative">
				<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
				<input bind:value={searchQuery} placeholder="Search vaults..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-64" />
			</div>
		</div>
		<div class="p-4">
			{#if loading && !vaults.length}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading vaults...</div>
			{:else if filteredVaults.length === 0}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">No vaults found</div>
			{:else}
				<div class="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
					{#each filteredVaults as vault}
						<div class="p-4 rounded-xl bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600 flex items-start gap-3 group">
							<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
								<Database class="w-5 h-5 text-blue-600 dark:text-blue-400" />
							</div>
							<div class="flex-1 min-w-0">
								<h3 class="font-bold text-gray-900 dark:text-white truncate" title={vault.VaultName}>{vault.VaultName}</h3>
								<div class="mt-2 space-y-1">
									<p class="text-[10px] text-gray-500 dark:text-gray-400 uppercase tracking-wider font-mono truncate">{vault.VaultARN}</p>
									<p class="text-xs text-gray-600 dark:text-gray-300">Archives: {vault.NumberOfArchives ?? 0} · Size: {Math.round((vault.SizeInBytes ?? 0) / 1024 / 1024)} MB</p>
								</div>
							</div>
						</div>
					{/each}
				</div>
			{/if}
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700">
			<h2 class="text-lg font-semibold text-gray-900 dark:text-white">Glacier Storage Classes</h2>
		</div>
		<div class="p-4 space-y-4">
			{#each storageClasses as cls}
				<div class="p-4 rounded-lg bg-gray-50 dark:bg-slate-700/50 border border-gray-200 dark:border-gray-600">
					<h3 class="font-semibold text-gray-900 dark:text-white mb-2">{cls.name}</h3>
					<div class="grid grid-cols-2 sm:grid-cols-4 gap-3 text-sm">
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Retrieval Time</p>
							<p class="text-gray-900 dark:text-white font-medium">{cls.retrieval}</p>
						</div>
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Min Storage Duration</p>
							<p class="text-gray-900 dark:text-white font-medium">{cls.minStorage}</p>
						</div>
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Cost</p>
							<p class="text-gray-900 dark:text-white font-medium">{cls.cost}</p>
						</div>
						<div>
							<p class="text-gray-500 dark:text-gray-400 text-xs">Use Case</p>
							<p class="text-gray-700 dark:text-gray-300">{cls.useCase}</p>
						</div>
					</div>
				</div>
			{/each}
		</div>
	</div>
</div>
