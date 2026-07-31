<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAppRunnerClient } from '$lib/aws-client';
	import {
		ListServicesCommand,
		DescribeServiceCommand,
		CreateServiceCommand,
		DeleteServiceCommand,
		PauseServiceCommand,
		ResumeServiceCommand,
		ListOperationsCommand,
		type ServiceSummary,
		type Service
	} from '@aws-sdk/client-apprunner';
	import { toast } from 'svelte-sonner';
	import {
		Rocket,
		Search,
		RefreshCw,
		Plus,
		Trash2,
		Eye,
		Play,
		Pause,
		ExternalLink
	} from 'lucide-svelte';

	const apprunner = regionalClient(getAppRunnerClient);

	let loading = $state(false);
	let services = $state<ServiceSummary[]>([]);
	let selectedService = $state<Service | null>(null);
	let loadingDetail = $state(false);
	let searchQuery = $state('');
	let showCreateModal = $state(false);
	let creating = $state(false);

	// Create form
	let newServiceName = $state('');
	let newImageUri = $state('');
	let newPort = $state(8080);
	let newCPU = $state('1 vCPU');
	let newMemory = $state('2 GB');

	const filteredServices = $derived(
		services.filter(
			(s) =>
				(s.ServiceName ?? '').toLowerCase().includes(searchQuery.toLowerCase()) ||
				(s.ServiceUrl ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function statusBadge(status?: string) {
		if (status === 'RUNNING') return 'text-green-700 bg-green-100 dark:text-green-300 dark:bg-green-900';
		if (status === 'PAUSED') return 'text-yellow-700 bg-yellow-100 dark:text-yellow-300 dark:bg-yellow-900';
		if (status === 'CREATING' || status === 'DEPLOYING' || status === 'UPDATING')
			return 'text-blue-700 bg-blue-100 dark:text-blue-300 dark:bg-blue-900';
		if (status === 'FAILED' || status === 'DELETING')
			return 'text-red-700 bg-red-100 dark:text-red-300 dark:bg-red-900';
		return 'text-muted-foreground bg-muted';
	}

	async function loadServices() {
		loading = true;
		try {
			const res = await apprunner().send(new ListServicesCommand({}));
			services = res.ServiceSummaryList ?? [];
		} catch (e) {
			toast.error(`Failed to load services: ${e}`);
		} finally {
			loading = false;
		}
	}

	async function viewService(service: ServiceSummary) {
		if (!service.ServiceArn) return;
		loadingDetail = true;
		selectedService = null;
		try {
			const res = await apprunner().send(
				new DescribeServiceCommand({ ServiceArn: service.ServiceArn })
			);
			selectedService = res.Service ?? null;
		} catch (e) {
			toast.error(`Failed to load service details: ${e}`);
		} finally {
			loadingDetail = false;
		}
	}

	async function pauseService(service: ServiceSummary) {
		if (!service.ServiceArn) return;
		try {
			await apprunner().send(new PauseServiceCommand({ ServiceArn: service.ServiceArn }));
			toast.success(`Service "${service.ServiceName}" paused`);
			await loadServices();
		} catch (e) {
			toast.error(`Failed to pause service: ${e}`);
		}
	}

	async function resumeService(service: ServiceSummary) {
		if (!service.ServiceArn) return;
		try {
			await apprunner().send(new ResumeServiceCommand({ ServiceArn: service.ServiceArn }));
			toast.success(`Service "${service.ServiceName}" resumed`);
			await loadServices();
		} catch (e) {
			toast.error(`Failed to resume service: ${e}`);
		}
	}

	async function deleteService(service: ServiceSummary) {
		if (!service.ServiceArn || !await confirmDestructive({ title: 'Delete App Runner Service', message: `Delete service "${service.ServiceName}"? The service will be stopped and all associated resources removed.` })) return;
		try {
			await apprunner().send(new DeleteServiceCommand({ ServiceArn: service.ServiceArn }));
			toast.success(`Service "${service.ServiceName}" deletion initiated`);
			if (selectedService?.ServiceArn === service.ServiceArn) selectedService = null;
			await loadServices();
		} catch (e) {
			toast.error(`Failed to delete service: ${e}`);
		}
	}

	async function createService() {
		if (!newServiceName.trim() || !newImageUri.trim()) {
			toast.error('Service name and image URI are required');
			return;
		}
		creating = true;
		try {
			await apprunner().send(
				new CreateServiceCommand({
					ServiceName: newServiceName.trim(),
					SourceConfiguration: {
						ImageRepository: {
							ImageIdentifier: newImageUri.trim(),
							ImageRepositoryType: 'ECR_PUBLIC',
							ImageConfiguration: {
								Port: String(newPort)
							}
						},
						AutoDeploymentsEnabled: false
					},
					InstanceConfiguration: {
						Cpu: newCPU,
						Memory: newMemory
					}
				})
			);
			toast.success(`Service "${newServiceName}" creation initiated`);
			showCreateModal = false;
			newServiceName = '';
			newImageUri = '';
			await loadServices();
		} catch (e) {
			toast.error(`Failed to create service: ${e}`);
		} finally {
			creating = false;
		}
	}

	onRegionChange(loadServices);
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<Rocket class="h-8 w-8 text-orange-500" />
			<div>
				<h1 class="text-2xl font-bold">App Runner</h1>
				<p class="text-sm text-muted-foreground">
					Fully managed container application service
				</p>
			</div>
		</div>
		<button
			onclick={loadServices}
			class="flex items-center gap-2 rounded-md border px-3 py-2 text-sm hover:bg-accent"
		>
			<RefreshCw class="h-4 w-4" />
			Refresh
		</button>
	</div>

	<div class="flex items-center justify-between gap-4">
		<div class="relative flex-1">
			<Search class="absolute left-3 top-2.5 h-4 w-4 text-muted-foreground" />
			<input
				type="text"
				placeholder="Search services..."
				bind:value={searchQuery}
				class="w-full rounded-md border bg-background pl-9 pr-4 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
			/>
		</div>
		<button
			onclick={() => (showCreateModal = true)}
			class="flex items-center gap-2 rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90"
		>
			<Plus class="h-4 w-4" />
			Create Service
		</button>
	</div>

	{#if loading}
		<div class="flex justify-center py-12">
			<RefreshCw class="h-8 w-8 animate-spin text-muted-foreground" />
		</div>
	{:else if filteredServices.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-muted-foreground">
			<Rocket class="h-12 w-12 mb-3 opacity-30" />
			<p>No App Runner services found</p>
			<p class="text-sm">Create a service to deploy your containerized app</p>
		</div>
	{:else}
		<div class="grid gap-4 sm:grid-cols-2 lg:grid-cols-3">
			{#each filteredServices as service}
				<div
					role="button"
					tabindex="0"
					class="rounded-lg border p-4 space-y-3 cursor-pointer hover:border-primary/50 transition-colors"
					onclick={() => viewService(service)}
					onkeydown={(e) => e.key === 'Enter' && viewService(service)}
				>
					<div class="flex items-start justify-between">
						<div>
							<p class="font-semibold">{service.ServiceName}</p>
							<p class="text-xs text-muted-foreground mt-0.5">{service.ServiceId}</p>
						</div>
						<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(service.Status)}">
							{service.Status ?? '—'}
						</span>
					</div>
					{#if service.ServiceUrl}
						<a
							href="https://{service.ServiceUrl}"
							target="_blank"
							rel="noopener noreferrer"
							onclick={(e) => e.stopPropagation()}
							class="flex items-center gap-1 text-xs text-blue-500 hover:text-blue-600 truncate"
						>
							<ExternalLink class="h-3 w-3 shrink-0" />
							{service.ServiceUrl}
						</a>
					{/if}
					<div class="flex justify-end gap-1">
						{#if service.Status === 'RUNNING'}
							<button
								onclick={(e) => { e.stopPropagation(); pauseService(service); }}
								class="rounded p-1 text-yellow-500 hover:bg-yellow-50 dark:hover:bg-yellow-950"
								title="Pause"
							>
								<Pause class="h-4 w-4" />
							</button>
						{:else if service.Status === 'PAUSED'}
							<button
								onclick={(e) => { e.stopPropagation(); resumeService(service); }}
								class="rounded p-1 text-green-500 hover:bg-green-50 dark:hover:bg-green-950"
								title="Resume"
							>
								<Play class="h-4 w-4" />
							</button>
						{/if}
						<button
							onclick={(e) => { e.stopPropagation(); viewService(service); }}
							class="rounded p-1 text-blue-500 hover:bg-blue-50 dark:hover:bg-blue-950"
							title="View details"
						>
							<Eye class="h-4 w-4" />
						</button>
						<button
							onclick={(e) => { e.stopPropagation(); deleteService(service); }}
							class="rounded p-1 text-red-500 hover:bg-red-50 dark:hover:bg-red-950"
							title="Delete"
						>
							<Trash2 class="h-4 w-4" />
						</button>
					</div>
				</div>
			{/each}
		</div>
	{/if}

	<!-- Service Detail Panel -->
	{#if loadingDetail}
		<div class="flex justify-center py-4">
			<RefreshCw class="h-6 w-6 animate-spin text-muted-foreground" />
		</div>
	{:else if selectedService}
		<div class="rounded-lg border p-5 space-y-4">
			<div class="flex items-center justify-between">
				<h3 class="font-semibold">{selectedService.ServiceName}</h3>
				<button onclick={() => (selectedService = null)} class="text-xs text-muted-foreground hover:text-foreground">
					Close
				</button>
			</div>
			<div class="grid grid-cols-2 gap-3 text-sm">
				<div>
					<p class="text-muted-foreground">Service ARN</p>
					<p class="font-mono text-xs truncate">{selectedService.ServiceArn}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Status</p>
					<span class="rounded-full px-2 py-0.5 text-xs font-medium {statusBadge(selectedService.Status)}">
						{selectedService.Status}
					</span>
				</div>
				<div>
					<p class="text-muted-foreground">CPU</p>
					<p>{selectedService.InstanceConfiguration?.Cpu ?? '—'}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Memory</p>
					<p>{selectedService.InstanceConfiguration?.Memory ?? '—'}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Created</p>
					<p>{selectedService.CreatedAt ? new Date(selectedService.CreatedAt).toLocaleDateString() : '—'}</p>
				</div>
				<div>
					<p class="text-muted-foreground">Last Updated</p>
					<p>{selectedService.UpdatedAt ? new Date(selectedService.UpdatedAt).toLocaleDateString() : '—'}</p>
				</div>
			</div>
		</div>
	{/if}
</div>

<!-- Create Service Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
		<div class="w-full max-w-md rounded-lg bg-background p-6 shadow-xl">
			<h2 class="text-lg font-semibold mb-4">Create App Runner Service</h2>
			<div class="space-y-3">
				<div>
					<label for="svc-name" class="block text-sm font-medium mb-1">Service Name *</label>
					<input
						id="svc-name"
						type="text"
						bind:value={newServiceName}
						placeholder="my-app"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div>
					<label for="svc-image" class="block text-sm font-medium mb-1">Container Image URI *</label>
					<input
						id="svc-image"
						type="text"
						bind:value={newImageUri}
						placeholder="public.ecr.aws/nginx/nginx:latest"
						class="w-full rounded-md border bg-background px-3 py-2 text-sm font-mono focus:outline-none focus:ring-2 focus:ring-primary"
					/>
				</div>
				<div class="grid grid-cols-2 gap-3">
					<div>
						<label for="svc-port" class="block text-sm font-medium mb-1">Port</label>
						<input
							id="svc-port"
							type="number"
							bind:value={newPort}
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						/>
					</div>
					<div>
						<label for="svc-cpu" class="block text-sm font-medium mb-1">CPU</label>
						<select
							id="svc-cpu"
							bind:value={newCPU}
							class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
						>
							<option value="0.25 vCPU">0.25 vCPU</option>
							<option value="0.5 vCPU">0.5 vCPU</option>
							<option value="1 vCPU">1 vCPU</option>
							<option value="2 vCPU">2 vCPU</option>
							<option value="4 vCPU">4 vCPU</option>
						</select>
					</div>
				</div>
				<div>
					<label for="svc-memory" class="block text-sm font-medium mb-1">Memory</label>
					<select
						id="svc-memory"
						bind:value={newMemory}
						class="w-full rounded-md border bg-background px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary"
					>
						<option value="0.5 GB">0.5 GB</option>
						<option value="1 GB">1 GB</option>
						<option value="2 GB">2 GB</option>
						<option value="3 GB">3 GB</option>
						<option value="4 GB">4 GB</option>
					</select>
				</div>
			</div>
			<div class="mt-4 flex justify-end gap-2">
				<button
					onclick={() => (showCreateModal = false)}
					class="rounded-md border px-4 py-2 text-sm hover:bg-accent"
				>
					Cancel
				</button>
				<button
					onclick={createService}
					disabled={creating || !newServiceName.trim() || !newImageUri.trim()}
					class="rounded-md bg-primary px-4 py-2 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
				>
					{creating ? 'Creating...' : 'Create Service'}
				</button>
			</div>
		</div>
	</div>
{/if}
