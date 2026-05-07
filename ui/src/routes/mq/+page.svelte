<script lang="ts">
	import { onMount } from 'svelte';
	import { getMQClient } from '$lib/aws-client';
	import {
		ListBrokersCommand,
		CreateBrokerCommand,
		DescribeBrokerCommand,
		ListConfigurationsCommand,
		DeleteBrokerCommand,
		RebootBrokerCommand,
		UpdateBrokerCommand,
		ListUsersCommand,
		CreateUserCommand,
		DeleteUserCommand,
		UpdateUserCommand,
		type BrokerSummary,
		type DescribeBrokerResponse,
		type Configuration,
		type UserSummary,
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
		ChevronRight,
		Tag,
		Plus,
		Trash2,
		RotateCcw,
		Pencil,
		Users,
		UserPlus,
		X
	} from 'lucide-svelte';

	const mq = getMQClient();

	let loading = $state(false);
	let activeTab = $state<'brokers' | 'configurations'>('brokers');
	let searchQuery = $state('');
	let brokers = $state<BrokerSummary[]>([]);
	let configurations = $state<Configuration[]>([]);
	let selectedBroker = $state<DescribeBrokerResponse | null>(null);
	let loadingDetail = $state(false);

	// Create broker modal
	let showCreateBrokerModal = $state(false);
	let creating = $state(false);
	let newBrokerName = $state('');
	let newBrokerEngine = $state<'ACTIVEMQ' | 'RABBITMQ'>('ACTIVEMQ');
	let newBrokerVersion = $state('5.15.14');
	let newBrokerDeployment = $state(DeploymentMode.SINGLE_INSTANCE);
	let newBrokerInstance = $state('mq.m5.large');

	// Delete broker modal
	let showDeleteBrokerModal = $state(false);
	let brokerToDelete = $state<BrokerSummary | null>(null);
	let deleting = $state(false);

	// Update broker modal
	let showUpdateBrokerModal = $state(false);
	let updating = $state(false);
	let updateEngineVersion = $state('');
	let updateHostInstanceType = $state('');
	let updateAutoMinorUpgrade = $state(false);

	// Reboot broker
	let rebooting = $state(false);

	// User management
	let brokerUsers = $state<UserSummary[]>([]);
	let loadingUsers = $state(false);
	let showUsersPanel = $state(false);

	// Create user modal
	let showCreateUserModal = $state(false);
	let creatingUser = $state(false);
	let newUsername = $state('');
	let newUserPassword = $state('');
	let newUserConsole = $state(false);
	let newUserGroups = $state('');

	// Update user modal
	let showUpdateUserModal = $state(false);
	let updatingUser = $state(false);
	let userToEdit = $state<UserSummary | null>(null);
	let editUserPassword = $state('');
	let editUserConsole = $state(false);
	let editUserGroups = $state('');

	// Delete user
	let deletingUser = $state<string | null>(null);

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
		showUsersPanel = false;
		brokerUsers = [];
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
		showUsersPanel = false;
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

	function openDeleteBroker(broker: BrokerSummary, e: Event) {
		e.stopPropagation();
		brokerToDelete = broker;
		showDeleteBrokerModal = true;
	}

	async function deleteBroker() {
		if (!brokerToDelete?.BrokerId) return;
		deleting = true;
		try {
			await mq.send(new DeleteBrokerCommand({ BrokerId: brokerToDelete.BrokerId }));
			toast.success(`Broker "${brokerToDelete.BrokerName}" deleted`);
			showDeleteBrokerModal = false;
			if (selectedBroker?.BrokerId === brokerToDelete.BrokerId) {
				selectedBroker = null;
				showUsersPanel = false;
			}
			brokerToDelete = null;
			await loadBrokers();
		} catch (err: unknown) {
			toast.error(`Failed to delete broker: ${(err as Error).message}`);
		} finally {
			deleting = false;
		}
	}

	async function rebootBroker(e: Event) {
		e.stopPropagation();
		if (!selectedBroker?.BrokerId) return;
		rebooting = true;
		try {
			await mq.send(new RebootBrokerCommand({ BrokerId: selectedBroker.BrokerId }));
			toast.success(`Broker "${selectedBroker.BrokerName}" rebooted`);
		} catch (err: unknown) {
			toast.error(`Failed to reboot broker: ${(err as Error).message}`);
		} finally {
			rebooting = false;
		}
	}

	function openUpdateBroker() {
		if (!selectedBroker) return;
		updateEngineVersion = selectedBroker.EngineVersion ?? '';
		updateHostInstanceType = selectedBroker.HostInstanceType ?? '';
		updateAutoMinorUpgrade = selectedBroker.AutoMinorVersionUpgrade ?? false;
		showUpdateBrokerModal = true;
	}

	async function updateBroker() {
		if (!selectedBroker?.BrokerId) return;
		updating = true;
		try {
			await mq.send(new UpdateBrokerCommand({
				BrokerId: selectedBroker.BrokerId,
				EngineVersion: updateEngineVersion || undefined,
				HostInstanceType: updateHostInstanceType || undefined,
				AutoMinorVersionUpgrade: updateAutoMinorUpgrade,
			}));
			toast.success(`Broker "${selectedBroker.BrokerName}" updated`);
			showUpdateBrokerModal = false;
			await selectBroker(selectedBroker.BrokerId);
		} catch (err: unknown) {
			toast.error(`Failed to update broker: ${(err as Error).message}`);
		} finally {
			updating = false;
		}
	}

	async function loadUsers() {
		if (!selectedBroker?.BrokerId) return;
		loadingUsers = true;
		try {
			const res = await mq.send(new ListUsersCommand({ BrokerId: selectedBroker.BrokerId }));
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			brokerUsers = ((res as any).Users ?? []) as UserSummary[];
		} catch (err: unknown) {
			toast.error(`Failed to load users: ${(err as Error).message}`);
		} finally {
			loadingUsers = false;
		}
	}

	async function toggleUsersPanel() {
		showUsersPanel = !showUsersPanel;
		if (showUsersPanel && brokerUsers.length === 0) {
			await loadUsers();
		}
	}

	function openCreateUser() {
		newUsername = '';
		newUserPassword = '';
		newUserConsole = false;
		newUserGroups = '';
		showCreateUserModal = true;
	}

	async function createUser() {
		if (!selectedBroker?.BrokerId || !newUsername.trim()) return;
		creatingUser = true;
		try {
			await mq.send(new CreateUserCommand({
				BrokerId: selectedBroker.BrokerId,
				Username: newUsername.trim(),
				Password: newUserPassword,
				ConsoleAccess: newUserConsole,
				Groups: newUserGroups ? newUserGroups.split(',').map((g) => g.trim()).filter(Boolean) : undefined,
			}));
			toast.success(`User "${newUsername.trim()}" created`);
			showCreateUserModal = false;
			await loadUsers();
		} catch (err: unknown) {
			toast.error(`Failed to create user: ${(err as Error).message}`);
		} finally {
			creatingUser = false;
		}
	}

	function openEditUser(user: UserSummary) {
		userToEdit = user;
		editUserPassword = '';
		// eslint-disable-next-line @typescript-eslint/no-explicit-any
		editUserConsole = (user as any).ConsoleAccess ?? false;
		editUserGroups = '';
		showUpdateUserModal = true;
	}

	async function updateUser() {
		if (!selectedBroker?.BrokerId || !userToEdit?.Username) return;
		updatingUser = true;
		try {
			await mq.send(new UpdateUserCommand({
				BrokerId: selectedBroker.BrokerId,
				Username: userToEdit.Username,
				Password: editUserPassword || undefined,
				ConsoleAccess: editUserConsole,
				Groups: editUserGroups ? editUserGroups.split(',').map((g) => g.trim()).filter(Boolean) : undefined,
			}));
			toast.success(`User "${userToEdit.Username}" updated`);
			showUpdateUserModal = false;
			userToEdit = null;
			await loadUsers();
		} catch (err: unknown) {
			toast.error(`Failed to update user: ${(err as Error).message}`);
		} finally {
			updatingUser = false;
		}
	}

	async function deleteUser(username: string) {
		if (!selectedBroker?.BrokerId) return;
		deletingUser = username;
		try {
			await mq.send(new DeleteUserCommand({
				BrokerId: selectedBroker.BrokerId,
				Username: username,
			}));
			toast.success(`User "${username}" deleted`);
			await loadUsers();
		} catch (err: unknown) {
			toast.error(`Failed to delete user: ${(err as Error).message}`);
		} finally {
			deletingUser = null;
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
									<button
										onclick={(e) => openDeleteBroker(broker, e)}
										class="p-1 text-slate-400 hover:text-red-600 dark:hover:text-red-400 transition-colors"
										title="Delete broker"
										aria-label="Delete broker"
									>
										<Trash2 class="w-3.5 h-3.5" />
									</button>
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
		<div class="lg:col-span-2 space-y-4">
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
						<div class="flex items-center gap-2">
							<button
								onclick={rebootBroker}
								disabled={rebooting}
								class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-300 bg-slate-100 dark:bg-slate-700 hover:bg-slate-200 dark:hover:bg-slate-600 rounded-lg transition-colors disabled:opacity-50"
								title="Reboot broker"
							>
								<RotateCcw class="w-3.5 h-3.5 {rebooting ? 'animate-spin' : ''}" />
								{rebooting ? 'Rebooting...' : 'Reboot'}
							</button>
							<button
								onclick={openUpdateBroker}
								class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-slate-700 dark:text-slate-300 bg-slate-100 dark:bg-slate-700 hover:bg-slate-200 dark:hover:bg-slate-600 rounded-lg transition-colors"
								title="Update broker"
							>
								<Pencil class="w-3.5 h-3.5" />
								Update
							</button>
							<button
								onclick={() => { if (selectedBroker) { const s = brokers.find(b => b.BrokerId === selectedBroker!.BrokerId); if (s) openDeleteBroker(s, new Event('click')); } }}
								class="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-red-600 dark:text-red-400 bg-red-50 dark:bg-red-900/20 hover:bg-red-100 dark:hover:bg-red-900/30 rounded-lg transition-colors"
								title="Delete broker"
							>
								<Trash2 class="w-3.5 h-3.5" />
								Delete
							</button>
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

					<!-- Users section -->
					<div>
						<div class="flex items-center justify-between mb-2">
							<button
								onclick={toggleUsersPanel}
								class="flex items-center gap-2 font-semibold text-slate-900 dark:text-white hover:text-orange-600 dark:hover:text-orange-400 transition-colors"
							>
								<Users class="w-4 h-4" />
								Users
								<ChevronRight class="w-4 h-4 transition-transform {showUsersPanel ? 'rotate-90' : ''}" />
							</button>
							{#if showUsersPanel}
								<button
									onclick={openCreateUser}
									class="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-medium text-white bg-orange-600 hover:bg-orange-700 rounded-lg transition-colors"
								>
									<UserPlus class="w-3.5 h-3.5" />
									Add User
								</button>
							{/if}
						</div>

						{#if showUsersPanel}
							{#if loadingUsers}
								<div class="text-center py-6">
									<div class="inline-block animate-spin rounded-full h-6 w-6 border-b-2 border-orange-500 mb-2"></div>
									<p class="text-sm text-slate-500 dark:text-slate-400">Loading users...</p>
								</div>
							{:else if brokerUsers.length === 0}
								<div class="bg-slate-50 dark:bg-slate-700/30 rounded-lg p-6 text-center">
									<Users class="w-8 h-8 mx-auto text-slate-300 dark:text-slate-600 mb-2" />
									<p class="text-sm text-slate-500 dark:text-slate-400">No users found</p>
								</div>
							{:else}
								<div class="space-y-2">
									{#each brokerUsers as user}
										<div class="flex items-center justify-between bg-slate-50 dark:bg-slate-700/30 rounded-lg px-3 py-2">
											<div>
												<p class="text-sm font-medium text-slate-900 dark:text-white">{user.Username}</p>
												{#if (user as any).ConsoleAccess}
													<span class="text-xs text-green-600 dark:text-green-400">Console access</span>
												{/if}
											</div>
											<div class="flex items-center gap-1.5">
												<button
													onclick={() => openEditUser(user)}
													class="p-1 text-slate-400 hover:text-orange-600 dark:hover:text-orange-400 transition-colors"
													title="Edit user"
													aria-label="Edit user"
												>
													<Pencil class="w-3.5 h-3.5" />
												</button>
												<button
													onclick={() => deleteUser(user.Username ?? '')}
													disabled={deletingUser === user.Username}
													class="p-1 text-slate-400 hover:text-red-600 dark:hover:text-red-400 transition-colors disabled:opacity-50"
													title="Delete user"
													aria-label="Delete user"
												>
													{#if deletingUser === user.Username}
														<div class="w-3.5 h-3.5 animate-spin rounded-full border-b-2 border-red-500"></div>
													{:else}
														<Trash2 class="w-3.5 h-3.5" />
													{/if}
												</button>
											</div>
										</div>
									{/each}
								</div>
							{/if}
						{/if}
					</div>
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


<!-- Create Broker Modal -->
{#if showCreateBrokerModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showCreateBrokerModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showCreateBrokerModal = false)} role="dialog" aria-modal="true">
<div class="relative p-4 w-full max-w-md" role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Create Broker</h3>
<button aria-label="Close" onclick={() => { showCreateBrokerModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><X class="w-4 h-4" /></button>
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


<!-- Delete Broker Modal -->
{#if showDeleteBrokerModal && brokerToDelete}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showDeleteBrokerModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showDeleteBrokerModal = false)} role="dialog" aria-modal="true">
<div class="relative p-4 w-full max-w-md" role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Delete Broker</h3>
<button aria-label="Close" onclick={() => { showDeleteBrokerModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><X class="w-4 h-4" /></button>
</div>
<div class="p-4 space-y-4">
<p class="text-slate-700 dark:text-slate-300">Are you sure you want to delete broker <span class="font-semibold text-slate-900 dark:text-white">{brokerToDelete.BrokerName}</span>? This action cannot be undone.</p>
<div class="flex gap-3 justify-end">
<button type="button" onclick={() => { showDeleteBrokerModal = false; brokerToDelete = null; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:bg-slate-700">Cancel</button>
<button type="button" onclick={deleteBroker} disabled={deleting} class="text-white bg-red-600 hover:bg-red-700 focus:ring-4 focus:ring-red-300 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">
{deleting ? 'Deleting...' : 'Delete Broker'}
</button>
</div>
</div>
</div>
</div>
</div>
{/if}


<!-- Update Broker Modal -->
{#if showUpdateBrokerModal && selectedBroker}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showUpdateBrokerModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showUpdateBrokerModal = false)} role="dialog" aria-modal="true">
<div class="relative p-4 w-full max-w-md" role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Update Broker</h3>
<button aria-label="Close" onclick={() => { showUpdateBrokerModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><X class="w-4 h-4" /></button>
</div>
<div class="p-4">
<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); updateBroker(); }}>
<div>
<label for="update-engine-version" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Engine Version</label>
<input type="text" id="update-engine-version" bind:value={updateEngineVersion} placeholder="5.18.3" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div>
<label for="update-host-instance" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Host Instance Type</label>
<select id="update-host-instance" bind:value={updateHostInstanceType} class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:text-white">
<option value="mq.m5.large">mq.m5.large</option>
<option value="mq.m5.xlarge">mq.m5.xlarge</option>
</select>
</div>
<div class="flex items-center gap-2">
<input type="checkbox" id="update-auto-minor" bind:checked={updateAutoMinorUpgrade} class="w-4 h-4 text-orange-600 rounded border-slate-300 focus:ring-orange-500" />
<label for="update-auto-minor" class="text-sm font-medium text-slate-900 dark:text-white">Auto Minor Version Upgrade</label>
</div>
<div class="flex gap-3 justify-end pt-2">
<button type="button" onclick={() => { showUpdateBrokerModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:bg-slate-700">Cancel</button>
<button type="submit" disabled={updating} class="text-white bg-orange-600 hover:bg-orange-700 focus:ring-4 focus:ring-orange-300 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">
{updating ? 'Updating...' : 'Update Broker'}
</button>
</div>
</form>
</div>
</div>
</div>
</div>
{/if}


<!-- Create User Modal -->
{#if showCreateUserModal}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showCreateUserModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showCreateUserModal = false)} role="dialog" aria-modal="true">
<div class="relative p-4 w-full max-w-md" role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Add User</h3>
<button aria-label="Close" onclick={() => { showCreateUserModal = false; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><X class="w-4 h-4" /></button>
</div>
<div class="p-4">
<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); createUser(); }}>
<div>
<label for="user-username" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Username</label>
<input type="text" id="user-username" bind:value={newUsername} placeholder="admin" required class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div>
<label for="user-password" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Password</label>
<input type="password" id="user-password" bind:value={newUserPassword} placeholder="••••••••" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div>
<label for="user-groups" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Groups <span class="text-slate-400 font-normal">(comma-separated)</span></label>
<input type="text" id="user-groups" bind:value={newUserGroups} placeholder="admins, developers" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div class="flex items-center gap-2">
<input type="checkbox" id="user-console" bind:checked={newUserConsole} class="w-4 h-4 text-orange-600 rounded border-slate-300 focus:ring-orange-500" />
<label for="user-console" class="text-sm font-medium text-slate-900 dark:text-white">Console Access</label>
</div>
<div class="flex gap-3 justify-end pt-2">
<button type="button" onclick={() => { showCreateUserModal = false; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:bg-slate-700">Cancel</button>
<button type="submit" disabled={creatingUser} class="text-white bg-orange-600 hover:bg-orange-700 focus:ring-4 focus:ring-orange-300 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">
{creatingUser ? 'Adding...' : 'Add User'}
</button>
</div>
</form>
</div>
</div>
</div>
</div>
{/if}


<!-- Update User Modal -->
{#if showUpdateUserModal && userToEdit}
<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm" tabindex="-1" onclick={(e) => { if (e.target === e.currentTarget) showUpdateUserModal = false; }} onkeydown={(e) => e.key === 'Escape' && (showUpdateUserModal = false)} role="dialog" aria-modal="true">
<div class="relative p-4 w-full max-w-md" role="document">
<div class="relative bg-white rounded-lg shadow dark:bg-slate-700">
<div class="flex items-center justify-between p-4 border-b dark:border-slate-600">
<h3 class="text-xl font-semibold text-slate-900 dark:text-white">Update User: {userToEdit.Username}</h3>
<button aria-label="Close" onclick={() => { showUpdateUserModal = false; userToEdit = null; }} class="text-slate-400 bg-transparent hover:bg-slate-200 hover:text-slate-900 rounded-lg text-sm w-8 h-8 inline-flex justify-center items-center dark:hover:bg-slate-600 dark:hover:text-white"><X class="w-4 h-4" /></button>
</div>
<div class="p-4">
<form class="space-y-4" onsubmit={(e) => { e.preventDefault(); updateUser(); }}>
<div>
<label for="edit-user-password" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">New Password <span class="text-slate-400 font-normal">(leave blank to keep current)</span></label>
<input type="password" id="edit-user-password" bind:value={editUserPassword} placeholder="••••••••" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div>
<label for="edit-user-groups" class="block mb-2 text-sm font-medium text-slate-900 dark:text-white">Groups <span class="text-slate-400 font-normal">(comma-separated)</span></label>
<input type="text" id="edit-user-groups" bind:value={editUserGroups} placeholder="admins, developers" class="bg-slate-50 border border-slate-300 text-slate-900 text-sm rounded-lg focus:ring-orange-500 focus:border-orange-500 block w-full p-2.5 dark:bg-slate-600 dark:border-slate-500 dark:placeholder-slate-400 dark:text-white" />
</div>
<div class="flex items-center gap-2">
<input type="checkbox" id="edit-user-console" bind:checked={editUserConsole} class="w-4 h-4 text-orange-600 rounded border-slate-300 focus:ring-orange-500" />
<label for="edit-user-console" class="text-sm font-medium text-slate-900 dark:text-white">Console Access</label>
</div>
<div class="flex gap-3 justify-end pt-2">
<button type="button" onclick={() => { showUpdateUserModal = false; userToEdit = null; }} class="py-2 px-4 text-sm font-medium text-slate-900 bg-white rounded-lg border border-slate-200 hover:bg-slate-100 dark:bg-slate-800 dark:text-slate-400 dark:border-slate-600 dark:hover:bg-slate-700">Cancel</button>
<button type="submit" disabled={updatingUser} class="text-white bg-orange-600 hover:bg-orange-700 focus:ring-4 focus:ring-orange-300 font-medium rounded-lg text-sm px-4 py-2 disabled:opacity-50">
{updatingUser ? 'Updating...' : 'Update User'}
</button>
</div>
</form>
</div>
</div>
</div>
</div>
{/if}
