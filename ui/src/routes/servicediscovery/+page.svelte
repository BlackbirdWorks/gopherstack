<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getServiceDiscoveryClient } from '$lib/aws-client';
	import {
		ListNamespacesCommand,
		ListServicesCommand,
		ListInstancesCommand,
		ListOperationsCommand,
		CreateHttpNamespaceCommand,
		CreatePrivateDnsNamespaceCommand,
		CreatePublicDnsNamespaceCommand,
		CreateServiceCommand,
		RegisterInstanceCommand,
		UpdateInstanceCustomHealthStatusCommand,
		type NamespaceSummary,
		type ServiceSummary,
		type InstanceSummary,
		type OperationSummary
	} from '@aws-sdk/client-servicediscovery';
	import { toast } from 'svelte-sonner';
	import { Network, RefreshCw, Search, Globe, Server, Box, Plus, Activity, CheckCircle, XCircle, Clock } from 'lucide-svelte';

	const sd = regionalClient(getServiceDiscoveryClient);

	let loading = $state(false);
	let activeTab = $state<'namespaces' | 'services' | 'instances' | 'operations'>('namespaces');
	let searchQuery = $state('');
	let namespaces = $state<NamespaceSummary[]>([]);
	let services = $state<ServiceSummary[]>([]);
	let instances = $state<InstanceSummary[]>([]);
	let operations = $state<OperationSummary[]>([]);
	let selectedServiceId = $state('');
	let loadingInstances = $state(false);

	// Create Namespace modal
	let showCreateNamespace = $state(false);
	let nsType = $state<'HTTP' | 'PRIVATE_DNS' | 'PUBLIC_DNS'>('HTTP');
	let nsName = $state('');
	let nsDesc = $state('');
	let nsVpc = $state('');
	let creatingNs = $state(false);

	// Create Service modal
	let showCreateService = $state(false);
	let svcName = $state('');
	let svcDesc = $state('');
	let svcNamespaceId = $state('');
	let creatingService = $state(false);

	// Register Instance modal
	let showRegisterInstance = $state(false);
	let instServiceId = $state('');
	let instId = $state('');
	let instIPv4 = $state('');
	let instPort = $state('');
	let registeringInstance = $state(false);

	// Update Health Status modal
	let showUpdateHealth = $state(false);
	let healthServiceId = $state('');
	let healthInstanceId = $state('');
	let healthStatus = $state<'HEALTHY' | 'UNHEALTHY'>('HEALTHY');
	let updatingHealth = $state(false);

	const filteredNamespaces = $derived(namespaces.filter((n) => (n.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const filteredServices = $derived(services.filter((s) => (s.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())));
	const dnsNamespaces = $derived(namespaces.filter((n) => n.Type?.startsWith('DNS')).length);

	async function loadData() {
		loading = true;
		try {
			const [nsResp, svcResp, opsResp] = await Promise.all([
				sd().send(new ListNamespacesCommand({})),
				sd().send(new ListServicesCommand({})),
				sd().send(new ListOperationsCommand({}))
			]);
			namespaces = nsResp.Namespaces ?? [];
			services = svcResp.Services ?? [];
			operations = opsResp.Operations ?? [];
		} catch (e) {
			toast.error('Failed to load Service Discovery data: ' + String(e));
		} finally {
			loading = false;
		}
	}

	async function loadInstances(serviceId: string) {
		if (!serviceId) return;
		loadingInstances = true;
		try {
			const resp = await sd().send(new ListInstancesCommand({ ServiceId: serviceId }));
			instances = resp.Instances ?? [];
		} catch (e) {
			toast.error('Failed to load instances: ' + String(e));
		} finally {
			loadingInstances = false;
		}
	}

	async function createNamespace() {
		if (!nsName.trim()) { toast.error('Name is required'); return; }
		if (nsType === 'PRIVATE_DNS' && !nsVpc.trim()) { toast.error('VPC ID is required for Private DNS namespace'); return; }
		creatingNs = true;
		try {
			if (nsType === 'HTTP') {
				await sd().send(new CreateHttpNamespaceCommand({ Name: nsName.trim(), Description: nsDesc || undefined }));
			} else if (nsType === 'PRIVATE_DNS') {
				await sd().send(new CreatePrivateDnsNamespaceCommand({ Name: nsName.trim(), Description: nsDesc || undefined, Vpc: nsVpc || undefined }));
			} else {
				await sd().send(new CreatePublicDnsNamespaceCommand({ Name: nsName.trim(), Description: nsDesc || undefined }));
			}
			toast.success('Namespace creation initiated');
			showCreateNamespace = false;
			nsName = ''; nsDesc = ''; nsVpc = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create namespace: ' + String(e));
		} finally {
			creatingNs = false;
		}
	}

	async function createService() {
		if (!svcName.trim()) { toast.error('Name is required'); return; }
		creatingService = true;
		try {
			await sd().send(new CreateServiceCommand({
				Name: svcName.trim(),
				Description: svcDesc || undefined,
				NamespaceId: svcNamespaceId || undefined
			}));
			toast.success('Service created');
			showCreateService = false;
			svcName = ''; svcDesc = ''; svcNamespaceId = '';
			await loadData();
		} catch (e) {
			toast.error('Failed to create service: ' + String(e));
		} finally {
			creatingService = false;
		}
	}

	async function registerInstance() {
		if (!instServiceId.trim()) { toast.error('Service ID is required'); return; }
		if (!instId.trim()) { toast.error('Instance ID is required'); return; }
		registeringInstance = true;
		try {
			const attrs: Record<string, string> = {};
			if (instIPv4.trim()) attrs['AWS_INSTANCE_IPV4'] = instIPv4.trim();
			if (instPort.trim()) attrs['AWS_INSTANCE_PORT'] = instPort.trim();
			await sd().send(new RegisterInstanceCommand({
				ServiceId: instServiceId.trim(),
				InstanceId: instId.trim(),
				Attributes: attrs
			}));
			toast.success('Instance registration initiated');
			showRegisterInstance = false;
			instId = ''; instIPv4 = ''; instPort = '';
			if (selectedServiceId === instServiceId.trim()) await loadInstances(selectedServiceId);
		} catch (e) {
			toast.error('Failed to register instance: ' + String(e));
		} finally {
			registeringInstance = false;
		}
	}

	async function updateHealthStatus() {
		updatingHealth = true;
		try {
			await sd().send(new UpdateInstanceCustomHealthStatusCommand({
				ServiceId: healthServiceId,
				InstanceId: healthInstanceId,
				Status: healthStatus
			}));
			toast.success(`Health status updated to ${healthStatus}`);
			showUpdateHealth = false;
		} catch (e) {
			toast.error('Failed to update health status: ' + String(e));
		} finally {
			updatingHealth = false;
		}
	}

	function openUpdateHealth(serviceId: string, instanceId: string) {
		healthServiceId = serviceId;
		healthInstanceId = instanceId;
		healthStatus = 'HEALTHY';
		showUpdateHealth = true;
	}

	function openRegisterInstance(serviceId?: string) {
		instServiceId = serviceId ?? selectedServiceId ?? '';
		instId = ''; instIPv4 = ''; instPort = '';
		showRegisterInstance = true;
	}

	async function switchToInstances(serviceId: string) {
		selectedServiceId = serviceId;
		activeTab = 'instances';
		searchQuery = '';
		await loadInstances(serviceId);
	}

	function switchTab(tab: typeof activeTab) {
		activeTab = tab;
		searchQuery = '';
	}

	function operationStatusColor(status?: string) {
		switch (status) {
			case 'SUCCESS': return 'text-green-600 dark:text-green-400 bg-green-100 dark:bg-green-900/30';
			case 'FAIL': return 'text-red-600 dark:text-red-400 bg-red-100 dark:bg-red-900/30';
			case 'PENDING': return 'text-yellow-600 dark:text-yellow-400 bg-yellow-100 dark:bg-yellow-900/30';
			default: return 'text-gray-600 dark:text-gray-400 bg-gray-100 dark:bg-gray-700';
		}
	}

	onRegionChange(loadData);
</script>

<div class="p-6 space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Network class="w-7 h-7 text-indigo-500" />
			<div>
				<h1 class="text-2xl font-bold text-gray-900 dark:text-white">AWS Cloud Map</h1>
				<p class="text-sm text-gray-500 dark:text-gray-400">Service discovery for cloud resources</p>
			</div>
		</div>
		<button onclick={loadData} title="Refresh" class="flex items-center gap-2 px-3 py-2 rounded-lg border border-gray-200 dark:border-gray-700 text-gray-600 dark:text-gray-300 hover:bg-gray-50 dark:hover:bg-gray-800 text-sm">
			<RefreshCw class="w-4 h-4" /> Refresh
		</button>
	</div>

	<div class="grid grid-cols-2 sm:grid-cols-4 gap-4">
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-indigo-100 dark:bg-indigo-900/30 rounded-lg"><Globe class="w-5 h-5 text-indigo-600 dark:text-indigo-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{namespaces.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Namespaces</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-blue-100 dark:bg-blue-900/30 rounded-lg"><Server class="w-5 h-5 text-blue-600 dark:text-blue-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{services.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Services</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-green-100 dark:bg-green-900/30 rounded-lg"><Network class="w-5 h-5 text-green-600 dark:text-green-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{dnsNamespaces}</p><p class="text-sm text-gray-500 dark:text-gray-400">DNS Namespaces</p></div>
		</div>
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4 flex items-center gap-3">
			<div class="p-2 bg-purple-100 dark:bg-purple-900/30 rounded-lg"><Activity class="w-5 h-5 text-purple-600 dark:text-purple-400" /></div>
			<div><p class="text-2xl font-bold text-gray-900 dark:text-white">{operations.length}</p><p class="text-sm text-gray-500 dark:text-gray-400">Operations</p></div>
		</div>
	</div>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<div class="flex gap-2 flex-wrap">
				{#each [['namespaces', 'Namespaces'], ['services', 'Services'], ['instances', 'Instances'], ['operations', 'Operations']] as [tab, label]}
					<button onclick={() => switchTab(tab as typeof activeTab)}
						class="px-4 py-2 rounded-lg text-sm font-medium {activeTab === tab ? 'bg-indigo-600 text-white' : 'bg-gray-100 dark:bg-slate-700 text-gray-700 dark:text-gray-300 hover:bg-gray-200 dark:hover:bg-slate-600'}">
						{label}
					</button>
				{/each}
			</div>
			<div class="flex items-center gap-2">
				{#if activeTab === 'namespaces'}
					<button onclick={() => { nsName=''; nsDesc=''; nsVpc=''; nsType='HTTP'; showCreateNamespace=true; }}
						class="flex items-center gap-1.5 px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm font-medium">
						<Plus class="w-4 h-4" /> Create Namespace
					</button>
				{:else if activeTab === 'services'}
					<button onclick={() => { svcName=''; svcDesc=''; svcNamespaceId=''; showCreateService=true; }}
						class="flex items-center gap-1.5 px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm font-medium">
						<Plus class="w-4 h-4" /> Create Service
					</button>
				{:else if activeTab === 'instances'}
					<button onclick={() => openRegisterInstance()}
						class="flex items-center gap-1.5 px-3 py-2 bg-indigo-600 hover:bg-indigo-700 text-white rounded-lg text-sm font-medium">
						<Plus class="w-4 h-4" /> Register Instance
					</button>
				{/if}
				<div class="relative">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
					<input bind:value={searchQuery} placeholder="Search..." class="pl-9 pr-4 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white w-full sm:w-48" />
				</div>
			</div>
		</div>

		<div class="p-4">
			{#if loading}
				<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading...</div>
			{:else if activeTab === 'namespaces'}
				{#if filteredNamespaces.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No namespaces found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredNamespaces as ns}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Globe class="w-5 h-5 text-indigo-500 shrink-0" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{ns.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{ns.Id} · {ns.Type}</p>
									</div>
								</div>
								<span class="text-xs px-2 py-1 rounded-full bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400">{ns.ServiceCount} services</span>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'services'}
				{#if filteredServices.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No services found</div>
				{:else}
					<div class="space-y-2">
						{#each filteredServices as svc}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Server class="w-5 h-5 text-blue-500 shrink-0" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{svc.Name}</p>
										<p class="text-xs text-gray-500 dark:text-gray-400">{svc.Id}</p>
									</div>
								</div>
								<div class="flex items-center gap-2">
									<span class="text-xs text-gray-400">{svc.InstanceCount} instances</span>
									<button onclick={() => switchToInstances(svc.Id ?? '')}
										class="text-xs px-2 py-1 rounded bg-indigo-100 dark:bg-indigo-900/30 text-indigo-700 dark:text-indigo-400 hover:bg-indigo-200 dark:hover:bg-indigo-900/50">
										View Instances
									</button>
								</div>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'instances'}
				<div class="mb-4 flex items-center gap-3">
					<label class="text-sm text-gray-600 dark:text-gray-300 shrink-0">Service:</label>
					<select bind:value={selectedServiceId} onchange={() => loadInstances(selectedServiceId)}
						class="flex-1 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-3 py-1.5">
						<option value="">— select a service —</option>
						{#each services as svc}
							<option value={svc.Id}>{svc.Name} ({svc.Id})</option>
						{/each}
					</select>
				</div>
				{#if !selectedServiceId}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a service to view its instances</div>
				{:else if loadingInstances}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Loading instances...</div>
				{:else if instances.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No instances registered for this service</div>
				{:else}
					<div class="space-y-2">
						{#each instances as inst}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									<Box class="w-5 h-5 text-purple-500 shrink-0" />
									<div>
										<p class="font-medium text-gray-900 dark:text-white">{inst.Id}</p>
										{#if inst.Attributes && Object.keys(inst.Attributes).length > 0}
											<p class="text-xs text-gray-500 dark:text-gray-400">
												{Object.entries(inst.Attributes).slice(0, 3).map(([k, v]) => `${k}=${v}`).join(' · ')}
											</p>
										{/if}
									</div>
								</div>
								<button onclick={() => openUpdateHealth(selectedServiceId, inst.Id ?? '')}
									class="text-xs px-2 py-1 rounded bg-amber-100 dark:bg-amber-900/30 text-amber-700 dark:text-amber-400 hover:bg-amber-200 dark:hover:bg-amber-900/50">
									Update Health
								</button>
							</div>
						{/each}
					</div>
				{/if}
			{:else if activeTab === 'operations'}
				{#if operations.length === 0}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">No operations found</div>
				{:else}
					<div class="space-y-2">
						{#each operations as op}
							<div class="flex items-center justify-between p-3 rounded-lg bg-gray-50 dark:bg-slate-700/50">
								<div class="flex items-center gap-3">
									{#if op.Status === 'SUCCESS'}
										<CheckCircle class="w-5 h-5 text-green-500 shrink-0" />
									{:else if op.Status === 'FAIL'}
										<XCircle class="w-5 h-5 text-red-500 shrink-0" />
									{:else}
										<Clock class="w-5 h-5 text-yellow-500 shrink-0" />
									{/if}
									<p class="text-sm text-gray-900 dark:text-white font-mono">{op.Id}</p>
								</div>
								<span class="text-xs px-2 py-1 rounded-full font-medium {operationStatusColor(op.Status)}">{op.Status}</span>
							</div>
						{/each}
					</div>
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Create Namespace Modal -->
{#if showCreateNamespace}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showCreateNamespace = false)}
		onkeydown={(e) => e.key === 'Escape' && (showCreateNamespace = false)}>
		<div class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="create-ns-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showCreateNamespace = false; }}>
			<h3 id="create-ns-title" class="mb-4 font-semibold text-white">Create Namespace</h3>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-gray-400">Type *</label>
					<select bind:value={nsType} class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white">
						<option value="HTTP">HTTP</option>
						<option value="PRIVATE_DNS">Private DNS</option>
						<option value="PUBLIC_DNS">Public DNS</option>
					</select>
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Name *</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={nsName} placeholder="my-namespace" />
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Description</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={nsDesc} placeholder="Optional" />
				</div>
				{#if nsType === 'PRIVATE_DNS'}
					<div>
						<label class="mb-1 block text-xs text-gray-400">VPC ID</label>
						<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
							bind:value={nsVpc} placeholder="vpc-..." />
					</div>
				{/if}
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showCreateNamespace = false)}>Cancel</button>
				<button class="rounded bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
					onclick={createNamespace} disabled={creatingNs}>
					{creatingNs ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Create Service Modal -->
{#if showCreateService}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showCreateService = false)}
		onkeydown={(e) => e.key === 'Escape' && (showCreateService = false)}>
		<div class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="create-svc-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showCreateService = false; }}>
			<h3 id="create-svc-title" class="mb-4 font-semibold text-white">Create Service</h3>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-gray-400">Name *</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={svcName} placeholder="my-service" />
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Namespace</label>
					<select bind:value={svcNamespaceId} class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white">
						<option value="">— none —</option>
						{#each namespaces as ns}
							<option value={ns.Id}>{ns.Name} ({ns.Type})</option>
						{/each}
					</select>
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Description</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={svcDesc} placeholder="Optional" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showCreateService = false)}>Cancel</button>
				<button class="rounded bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
					onclick={createService} disabled={creatingService}>
					{creatingService ? 'Creating…' : 'Create'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Register Instance Modal -->
{#if showRegisterInstance}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showRegisterInstance = false)}
		onkeydown={(e) => e.key === 'Escape' && (showRegisterInstance = false)}>
		<div class="w-96 rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="register-inst-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showRegisterInstance = false; }}>
			<h3 id="register-inst-title" class="mb-4 font-semibold text-white">Register Instance</h3>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-gray-400">Service *</label>
					<select bind:value={instServiceId} class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white">
						<option value="">— select service —</option>
						{#each services as svc}
							<option value={svc.Id}>{svc.Name} ({svc.Id})</option>
						{/each}
					</select>
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Instance ID *</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={instId} placeholder="my-instance-1" />
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">IPv4 Address</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={instIPv4} placeholder="192.0.2.44" />
				</div>
				<div>
					<label class="mb-1 block text-xs text-gray-400">Port</label>
					<input class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white"
						bind:value={instPort} placeholder="8080" type="number" />
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showRegisterInstance = false)}>Cancel</button>
				<button class="rounded bg-indigo-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-indigo-700 disabled:opacity-50"
					onclick={registerInstance} disabled={registeringInstance}>
					{registeringInstance ? 'Registering…' : 'Register'}
				</button>
			</div>
		</div>
	</div>
{/if}

<!-- Update Health Status Modal -->
{#if showUpdateHealth}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50"
		role="presentation"
		onclick={() => (showUpdateHealth = false)}
		onkeydown={(e) => e.key === 'Escape' && (showUpdateHealth = false)}>
		<div class="w-80 rounded-lg border border-gray-700 bg-gray-800 p-6"
			role="dialog" aria-modal="true" aria-labelledby="update-health-title" tabindex="-1"
			onclick={(e) => e.stopPropagation()}
			onkeydown={(e) => { if (e.key === 'Escape') showUpdateHealth = false; }}>
			<h3 id="update-health-title" class="mb-4 font-semibold text-white">Update Custom Health Status</h3>
			<p class="text-xs text-gray-400 mb-3">Instance: <span class="text-white font-mono">{healthInstanceId}</span></p>
			<div class="space-y-3">
				<div>
					<label class="mb-1 block text-xs text-gray-400">Status *</label>
					<select bind:value={healthStatus} class="w-full rounded border border-gray-600 bg-gray-900 px-3 py-1.5 text-sm text-white">
						<option value="HEALTHY">HEALTHY</option>
						<option value="UNHEALTHY">UNHEALTHY</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button class="rounded px-3 py-1.5 text-sm text-gray-400 hover:text-gray-200" onclick={() => (showUpdateHealth = false)}>Cancel</button>
				<button class="rounded bg-amber-600 px-3 py-1.5 text-sm font-medium text-white hover:bg-amber-700 disabled:opacity-50"
					onclick={updateHealthStatus} disabled={updatingHealth}>
					{updatingHealth ? 'Updating…' : 'Update'}
				</button>
			</div>
		</div>
	</div>
{/if}
