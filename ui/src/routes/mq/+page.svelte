<script lang="ts">
	import { onMount } from 'svelte';
	import { getMQClient } from '$lib/aws-client';
	import {
		ListBrokersCommand,
		CreateBrokerCommand,
		DescribeBrokerCommand,
		ListConfigurationsCommand,
		type BrokerSummary,
		type DescribeBrokerResponse,
		type Configuration,
		DeploymentMode,
		EngineType
	} from '@aws-sdk/client-mq';
	import { toast } from 'svelte-sonner';
	import {
		MessageSquare,
		Search,
		RefreshCw,
		Settings,
		Server,
		CheckCircle,
		Clock,
		XCircle,
		ChevronRight,
		Tag,
		Plus
	} from 'lucide-svelte';

	const mq = getMQClient();

	let loading = $state(false);
	let activeTab = $state<'brokers' | 'configurations'>('brokers');
	let searchQuery = $state('');
	let brokers = $state<BrokerSummary[]>([]);
	let configurations = $state<Configuration[]>([]);
	let selectedBroker = $state<DescribeBrokerResponse | null>(null);
	let loadingDetail = $state(false);
	let showCreateBrokerModal = $state(false);
	let creating = $state(false);
	let newBrokerName = $state('');
	let newBrokerEngine = $state<'ACTIVEMQ' | 'RABBITMQ'>('ACTIVEMQ');
	let newBrokerVersion = $state('5.15.14');
	let newBrokerDeployment = $state(DeploymentMode.SINGLE_INSTANCE);
	let newBrokerInstance = $state('mq.m5.large');

	const filteredBrokers = $derived(
		brokers.filter(
			(b) =>
				(b.BrokerName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(b.BrokerState ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredConfigurations = $derived(
		configurations.filter(
			(c) =>
				(c.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(c.EngineType ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const activeMQCount = $derived(brokers.filter((b) => b.EngineType === 'ACTIVEMQ').length);
	const rabbitMQCount = $derived(brokers.filter((b) => b.EngineType === 'RABBITMQ').length);

	function statusColor(status: string | undefined): string {
		if (status === 'RUNNING') return 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300';
		if (status === 'CREATION_IN_PROGRESS' || status === 'REBOOT_IN_PROGRESS')
			return 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300';
		if (status === 'DELETION_IN_PROGRESS' || status === 'CRITICAL_ACTION_REQUIRED')
			return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	function engineColor(engine: string | undefined): string {
		if (engine === 'ACTIVEMQ') return 'bg-orange-100 dark:bg-orange-900/30 text-orange-700 dark:text-orange-300';
		if (engine === 'RABBITMQ') return 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300';
		return 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-400';
	}

	async function loadBrokers() {
		loading = true;
		try {
			const res = await mq.send(new ListBrokersCommand({ MaxResults: 100 }));
			brokers = res.BrokerSummaries ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load brokers: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function loadConfigurations() {
		loading = true;
		try {
			const res = await mq.send(new ListConfigurationsCommand({ MaxResults: 100 }));
			configurations = res.Configurations ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load configurations: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	async function selectBroker(brokerId: string) {
		loadingDetail = true;
		selectedBroker = null;
		try {
			const res = await mq.send(new DescribeBrokerCommand({ BrokerId: brokerId }));
			selectedBroker = res;
		} catch (err: unknown) {
			toast.error(`Failed to describe broker: ${(err as Error).message}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function selectTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
		selectedBroker = null;
		if (tab === 'brokers' && brokers.length === 0) await loadBrokers();
		else if (tab === 'configurations' && configurations.length === 0) await loadConfigurations();
	}

	async function refresh() {
		if (activeTab === 'brokers') { brokers = []; await loadBrokers(); }
		else { configurations = []; await loadConfigurations(); }
	}

	async function createBroker() {
		if (!newBrokerName.trim()) return;
		creating = true;
		try {
			await mq.send(new CreateBrokerCommand({
				BrokerName: newBrokerName.trim(),
				DeploymentMode: newBrokerDeployment,
				EngineType: newBrokerEngine as EngineType,
				EngineVersion: newBrokerVersion,
				HostInstanceType: newBrokerInstance,
				PubliclyAccessible: false,
				AutoMinorVersionUpgrade: true,
			}));
			toast.success(`Broker "${newBrokerName.trim()}" created`);
			showCreateBrokerModal = false;
			newBrokerName = '';
			await loadBrokers();
		} catch (err: unknown) {
			toast.error(`Failed to create broker: ${(err as Error).message}`);
		} finally {
			creating = false;
		}
	}

	onMount(() => {
		loadBrokers();
	});
</script>

<div class="space-y-6">
	<!-- Header -->
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<MessageSquare class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Amazon MQ</h1>
				<p class="text-slate-600 dark:text-slate-300">Managed message broker service</p>
			</div>
		</div>
		<div class="flex items-center gap-2">
			<button
				onclick={() => { showCreateBrokerModal = true; }}
				class="flex items-center gap-1.5 px-3 py-2 text-sm font-medium text-white bg-orange-600 hover:bg-orange-700 rounded-lg transition-colors"
			>
				<Plus class="w-4 h-4" />
				Create Broker
			</button>
			<button
				onclick={() => refresh()}
				class="p-2 text-slate-600 dark:text-slate-400 hover:text-slate-900 dark:hover:text-white"
				title="Refresh"
			>
				<RefreshCw class="w-5 h-5 {loading ? 'animate-spin' : ''}" />
			</button>
		</div>
	</div>

	<!-- Stat cards -->
	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Server class="w-5 h-5 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{brokers.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Brokers</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg">
				<Settings class="w-5 h-5 text-blue-600 dark:text-blue-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{configurations.length}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">Configurations</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-amber-100 dark:bg-amber-900/30 rounded-lg">
				<Tag class="w-5 h-5 text-amber-600 dark:text-amber-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{activeMQCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">ActiveMQ</p>
			</div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-sky-100 dark:bg-sky-900/30 rounded-lg">
				<Tag class="w-5 h-5 text-sky-600 dark:text-sky-400" />
			</div>
			<div>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{rabbitMQCount}</p>
				<p class="text-sm text-slate-500 dark:text-slate-400">RabbitMQ</p>
			</div>
		</div>
	</div>

	<!-- Tab navigation -->
	<div class="flex items-center gap-1 border-b border-slate-200 dark:border-slate-700">
		<button
			onclick={() => selectTab('brokers')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'brokers' ? 'border-orange-500 text-orange-600 dark:text-orange-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Server class="w-4 h-4" />Brokers</span>
		</button>
		<button
			onclick={() => selectTab('configurations')}
			class="px-4 py-2.5 text-sm font-medium transition-colors border-b-2 -mb-px {activeTab === 'configurations' ? 'border-orange-500 text-orange-600 dark:text-orange-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 dark:hover:text-slate-200'}"
		>
			<span class="flex items-center gap-1.5"><Settings class="w-4 h-4" />Configurations</span>
		</button>
	</div>

	<!-- Search -->
	<div class="relative">
		<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
		<input
			type="text"
			bind:value={searchQuery}
			placeholder="Search {activeTab}..."
			class="w-full pl-10 pr-4 py-2 bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-900 dark:text-white focus:outline-none focus:ring-2 focus:ring-orange-500"
		/>
	</div>

	<!-- Content -->
	<div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
		<!-- List -->
		<div class="lg:col-span-1 space-y-2">
			{#if loading}
				<div class="text-center py-12">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-orange-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading {activeTab}...</p>
				</div>
			{:else if activeTab === 'brokers'}
				{#if filteredBrokers.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Server class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No brokers found</p>
					</div>
				{:else}
					{#each filteredBrokers as broker}
						<div
							role="button"
							tabindex="0"
							onclick={() => { if (broker.BrokerId) selectBroker(broker.BrokerId); }}
							onkeypress={(e) => { if (e.key === 'Enter' && broker.BrokerId) selectBroker(broker.BrokerId); }}
							class="w-full text-left bg-white dark:bg-slate-800 rounded-lg border p-3 hover:border-orange-400 transition-colors cursor-pointer {selectedBroker?.BrokerId === broker.BrokerId ? 'border-orange-500 ring-1 ring-orange-500' : 'border-slate-200 dark:border-slate-700'}"
						>
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{broker.BrokerName}</p>
									<div class="flex items-center gap-1.5 mt-1">
										<span class="px-1.5 py-0.5 text-xs rounded-full {engineColor(broker.EngineType)}">{broker.EngineType}</span>
									</div>
								</div>
								<div class="flex items-center gap-1.5 ml-2 flex-shrink-0">
									<span class="px-2 py-0.5 text-xs rounded-full {statusColor(broker.BrokerState)}">{broker.BrokerState}</span>
									<ChevronRight class="w-4 h-4 text-slate-400" />
								</div>
							</div>
						</div>
					{/each}
				{/if}
			{:else}
				{#if filteredConfigurations.length === 0}
					<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-8 text-center">
						<Settings class="w-12 h-12 mx-auto text-slate-300 dark:text-slate-600 mb-3" />
						<p class="text-slate-500 dark:text-slate-400">No configurations found</p>
					</div>
				{:else}
					{#each filteredConfigurations as config}
						<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-3">
							<div class="flex items-center justify-between">
								<div class="min-w-0">
									<p class="font-medium text-slate-900 dark:text-white truncate">{config.Name}</p>
									<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">{config.EngineType} · v{config.LatestRevision?.Revision ?? 1}</p>
								</div>
								<span class="px-2 py-0.5 text-xs rounded-full {engineColor(config.EngineType)} flex-shrink-0 ml-2">{config.EngineType}</span>
							</div>
						</div>
					{/each}
				{/if}
			{/if}
		</div>

		<!-- Detail panel -->
		<div class="lg:col-span-2">
			{#if loadingDetail}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-orange-500 mb-2"></div>
					<p class="text-slate-500 dark:text-slate-400">Loading broker details...</p>
				</div>
			{:else if selectedBroker}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6 space-y-4">
					<div class="flex items-start justify-between">
						<div>
							<h2 class="text-xl font-bold text-slate-900 dark:text-white">{selectedBroker.BrokerName}</h2>
							<div class="flex items-center gap-2 mt-1">
								<span class="px-2 py-0.5 text-xs rounded-full {statusColor(selectedBroker.BrokerState)}">{selectedBroker.BrokerState}</span>
								<span class="px-2 py-0.5 text-xs rounded-full {engineColor(selectedBroker.EngineType)}">{selectedBroker.EngineType}</span>
							</div>
						</div>
						<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
							<MessageSquare class="w-5 h-5 text-orange-600 dark:text-orange-400" />
						</div>
					</div>

					<div class="grid grid-cols-2 sm:grid-cols-3 gap-3">
						{#each [
							['Broker ARN', selectedBroker.BrokerArn ?? 'N/A'],
							['Broker ID', selectedBroker.BrokerId ?? 'N/A'],
							['Engine Version', selectedBroker.EngineVersion ?? 'N/A'],
							['Deployment Mode', selectedBroker.DeploymentMode ?? 'N/A'],
							['Host Instance Type', selectedBroker.HostInstanceType ?? 'N/A'],
							['Auto Minor Upgrade', String(selectedBroker.AutoMinorVersionUpgrade ?? false)],
							['Publicly Accessible', String(selectedBroker.PubliclyAccessible ?? false)],
							['Storage Type', selectedBroker.StorageType ?? 'N/A'],
							['Created', selectedBroker.Created ? new Date(selectedBroker.Created).toLocaleDateString() : 'N/A']
						] as [label, value]}
							<div class="bg-slate-50 dark:bg-slate-700/50 rounded-lg p-3">
								<p class="text-xs text-slate-500 dark:text-slate-400">{label}</p>
								<p class="text-sm font-semibold text-slate-900 dark:text-white mt-0.5 truncate" title={value}>{value}</p>
							</div>
						{/each}
					</div>

					{#if (selectedBroker.BrokerInstances ?? []).length > 0}
						<div>
							<h3 class="font-semibold text-slate-900 dark:text-white mb-2">Broker Instances</h3>
							<div class="space-y-2">
								{#each selectedBroker.BrokerInstances ?? [] as instance}
									<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2">
										<p class="text-sm font-medium text-slate-900 dark:text-white">{instance.ConsoleURL ?? 'N/A'}</p>
										<p class="text-xs text-slate-500 dark:text-slate-400 mt-0.5">IP: {instance.IpAddress ?? 'N/A'}</p>
									</div>
								{/each}
							</div>
						</div>
					{/if}
				</div>
			{:else}
				<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
					<MessageSquare class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4" />
					<p class="text-slate-500 dark:text-slate-400">Select a broker to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>


{#if showCreateBrokerModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" onclick={(e) => { if (e.target === e.currentTarget) showCreateBrokerModal = false; }} role="dialog" aria-modal="true">
<div class="relative p-4 w-full max-w-md" onclick={(e) => e.stopPropagation()} role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Broker</h3>
<button onclick={() => { showCreateBrokerModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><svg class="w-3 h-3" fill="none" viewBox="0 0 14 14"><path stroke="currentColor" stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="m1 1 6 6m0 0 6 6M7 7l6-6M7 7l-6 6" /></svg></button>
</div>
<div class="p-4">
<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createBroker(); }}>
<div>
<label for="broker-name" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Broker Name</label>
<input type="text" id="broker-name" bind:value={newBrokerName} placeholder="my-broker" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div>
<label for="broker-engine" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Engine Type</label>
<select id="broker-engine" bind:value={newBrokerEngine} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
<option value="ACTIVEMQ">ActiveMQ</option>
<option value="RABBITMQ">RabbitMQ</option>
</select>
</div>
<div>
<label for="broker-deployment" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Deployment Mode</label>
<select id="broker-deployment" bind:value={newBrokerDeployment} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
<option value="SINGLE_INSTANCE">Single Instance</option>
<option value="ACTIVE_STANDBY_MULTI_AZ">Active/Standby Multi-AZ</option>
</select>
</div>
<div class="flex gap-3 justify-end pt-2">
<button type="button" onclick={() => { showCreateBrokerModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:bg-slate-700">Cancel</button>
<button type="submit" disabled={creating} class="text-white bg-orange-600 hover:bg-orange-700 focus:ring-4 focus:ring-orange-300 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">
{creating ? 'Creating...' : 'Create Broker'}
</button>
</div>
</form>
</div>
</div>
</div>
</div>
{/if}
