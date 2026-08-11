<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getIoTWirelessClient } from '$lib/aws-client';
	import {
		ListWirelessDevicesCommand,
		CreateWirelessDeviceCommand,
		DeleteWirelessDeviceCommand,
		ListWirelessGatewaysCommand,
		CreateWirelessGatewayCommand,
		DeleteWirelessGatewayCommand,
		ListServiceProfilesCommand,
		CreateServiceProfileCommand,
		DeleteServiceProfileCommand,
		ListDestinationsCommand,
		CreateDestinationCommand,
		DeleteDestinationCommand,
		ListDeviceProfilesCommand,
		CreateDeviceProfileCommand,
		DeleteDeviceProfileCommand,
		ListFuotaTasksCommand,
		CreateFuotaTaskCommand,
		DeleteFuotaTaskCommand,
		type WirelessDeviceStatistics,
		type WirelessGatewayStatistics,
		type ServiceProfile,
		type Destinations,
		type DeviceProfile,
		type FuotaTask
	} from '@aws-sdk/client-iot-wireless';
	import { toast } from 'svelte-sonner';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { Wifi, Radio, RefreshCw, Plus, Trash2, Search, Server, BookOpen, Layers } from 'lucide-svelte';

	const client = regionalClient(getIoTWirelessClient);

	// The SDK puts the AWS error code on err.name and status on
	// err.$metadata.httpStatusCode; err.message alone is usually just the
	// human-readable text. Combine them so the toast shows the actual code.
	function describeError(e: unknown): string {
		if (e && typeof e === 'object') {
			const rec = e as { name?: unknown; message?: unknown; $metadata?: { httpStatusCode?: number } };
			const name = rec.name ? String(rec.name) : 'Error';
			const message = rec.message ? String(rec.message) : String(e);
			const status = rec.$metadata?.httpStatusCode;
			return status ? `${name} (HTTP ${status}): ${message}` : `${name}: ${message}`;
		}
		return String(e);
	}

	type Tab = 'devices' | 'gateways' | 'service-profiles' | 'destinations' | 'device-profiles' | 'fuota-tasks';
	let activeTab = $state<Tab>('devices');
	let searchQuery = $state('');

	// --- Wireless Devices ---
	let devices = $state<WirelessDeviceStatistics[]>([]);
	let loadingDevices = $state(false);
	let showCreateDevice = $state(false);
	let creatingDevice = $state(false);
	let newDeviceName = $state('');
	let newDeviceType = $state('LoRaWAN');
	let newDeviceDestination = $state('');

	// --- Wireless Gateways ---
	let gateways = $state<WirelessGatewayStatistics[]>([]);
	let loadingGateways = $state(false);
	let showCreateGateway = $state(false);
	let creatingGateway = $state(false);
	let newGatewayName = $state('');
	let newGatewayDescription = $state('');

	// --- Service Profiles ---
	let serviceProfiles = $state<ServiceProfile[]>([]);
	let loadingServiceProfiles = $state(false);
	let showCreateServiceProfile = $state(false);
	let creatingServiceProfile = $state(false);
	let newServiceProfileName = $state('');

	// --- Destinations ---
	let destinations = $state<Destinations[]>([]);
	let loadingDestinations = $state(false);
	let showCreateDestination = $state(false);
	let creatingDestination = $state(false);
	let newDestinationName = $state('');
	let newDestinationExpression = $state('');
	let newDestinationExpressionType = $state('RuleName');
	let newDestinationRoleArn = $state('');

	// --- Device Profiles ---
	let deviceProfiles = $state<DeviceProfile[]>([]);
	let loadingDeviceProfiles = $state(false);
	let showCreateDeviceProfile = $state(false);
	let creatingDeviceProfile = $state(false);
	let newDeviceProfileName = $state('');

	// --- FUOTA Tasks ---
	let fuotaTasks = $state<FuotaTask[]>([]);
	let loadingFuotaTasks = $state(false);
	let showCreateFuotaTask = $state(false);
	let creatingFuotaTask = $state(false);
	let newFuotaTaskName = $state('');
	let newFuotaTaskFirmwareImage = $state('');
	let newFuotaTaskFirmwareRole = $state('');

	// --- Filtered lists ---
	const filteredDevices = $derived(
		devices.filter((d) => (d.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredGateways = $derived(
		gateways.filter((g) => (g.Id ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	const filteredServiceProfiles = $derived(
		serviceProfiles.filter((sp) =>
			(sp.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredDestinations = $derived(
		destinations.filter((d) =>
			(d.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredDeviceProfiles = $derived(
		deviceProfiles.filter((dp) =>
			(dp.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	const filteredFuotaTasks = $derived(
		fuotaTasks.filter((ft) => (ft.Name ?? '').toLowerCase().includes(searchQuery.toLowerCase()))
	);

	// --- Data loaders ---

	async function loadDevices() {
		loadingDevices = true;
		try {
			const out = await client().send(new ListWirelessDevicesCommand({}));
			devices = out.WirelessDeviceList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load devices: ${describeError(err)}`);
		} finally {
			loadingDevices = false;
		}
	}

	async function loadGateways() {
		loadingGateways = true;
		try {
			const out = await client().send(new ListWirelessGatewaysCommand({}));
			gateways = out.WirelessGatewayList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load gateways: ${describeError(err)}`);
		} finally {
			loadingGateways = false;
		}
	}

	async function loadServiceProfiles() {
		loadingServiceProfiles = true;
		try {
			const out = await client().send(new ListServiceProfilesCommand({}));
			serviceProfiles = out.ServiceProfileList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load service profiles: ${describeError(err)}`);
		} finally {
			loadingServiceProfiles = false;
		}
	}

	async function loadDestinations() {
		loadingDestinations = true;
		try {
			const out = await client().send(new ListDestinationsCommand({}));
			destinations = out.DestinationList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load destinations: ${describeError(err)}`);
		} finally {
			loadingDestinations = false;
		}
	}

	async function loadDeviceProfiles() {
		loadingDeviceProfiles = true;
		try {
			const out = await client().send(new ListDeviceProfilesCommand({}));
			deviceProfiles = out.DeviceProfileList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load device profiles: ${describeError(err)}`);
		} finally {
			loadingDeviceProfiles = false;
		}
	}

	async function loadFuotaTasks() {
		loadingFuotaTasks = true;
		try {
			const out = await client().send(new ListFuotaTasksCommand({}));
			fuotaTasks = out.FuotaTaskList ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load FUOTA tasks: ${describeError(err)}`);
		} finally {
			loadingFuotaTasks = false;
		}
	}

	// --- Create handlers ---

	async function createDevice() {
		if (!newDeviceName.trim()) {
			toast.error('Device name is required');
			return;
		}
		creatingDevice = true;
		try {
			await client().send(
				new CreateWirelessDeviceCommand({
					Name: newDeviceName.trim(),
					Type: newDeviceType === 'LoRaWAN' ? 'LoRaWAN' : 'Sidewalk',
					DestinationName: newDeviceDestination.trim() || undefined
				})
			);
			showCreateDevice = false;
			newDeviceName = '';
			newDeviceDestination = '';
			await loadDevices();
			toast.success('Wireless device created');
		} catch (err: unknown) {
			toast.error(`Failed to create device: ${describeError(err)}`);
		} finally {
			creatingDevice = false;
		}
	}

	async function deleteDevice(id: string) {
		if (!(await confirmDestructive({ title: 'Delete Wireless Device', message: `Delete wireless device "${id}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteWirelessDeviceCommand({ Id: id }));
			await loadDevices();
			toast.success('Device deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete device: ${describeError(err)}`);
		}
	}

	async function createGateway() {
		if (!newGatewayName.trim()) {
			toast.error('Gateway name is required');
			return;
		}
		creatingGateway = true;
		try {
			await client().send(
				new CreateWirelessGatewayCommand({
					Name: newGatewayName.trim(),
					Description: newGatewayDescription.trim() || undefined,
					LoRaWAN: {}
				})
			);
			showCreateGateway = false;
			newGatewayName = '';
			newGatewayDescription = '';
			await loadGateways();
			toast.success('Wireless gateway created');
		} catch (err: unknown) {
			toast.error(`Failed to create gateway: ${describeError(err)}`);
		} finally {
			creatingGateway = false;
		}
	}

	async function deleteGateway(id: string) {
		if (!(await confirmDestructive({ title: 'Delete Wireless Gateway', message: `Delete wireless gateway "${id}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteWirelessGatewayCommand({ Id: id }));
			await loadGateways();
			toast.success('Gateway deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete gateway: ${describeError(err)}`);
		}
	}

	async function createServiceProfile() {
		if (!newServiceProfileName.trim()) {
			toast.error('Service profile name is required');
			return;
		}
		creatingServiceProfile = true;
		try {
			await client().send(
				new CreateServiceProfileCommand({ Name: newServiceProfileName.trim() })
			);
			showCreateServiceProfile = false;
			newServiceProfileName = '';
			await loadServiceProfiles();
			toast.success('Service profile created');
		} catch (err: unknown) {
			toast.error(`Failed to create service profile: ${describeError(err)}`);
		} finally {
			creatingServiceProfile = false;
		}
	}

	async function deleteServiceProfile(id: string) {
		if (!(await confirmDestructive({ title: 'Delete Service Profile', message: `Delete service profile "${id}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteServiceProfileCommand({ Id: id }));
			await loadServiceProfiles();
			toast.success('Service profile deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete service profile: ${describeError(err)}`);
		}
	}

	async function createDestination() {
		if (!newDestinationName.trim() || !newDestinationExpression.trim()) {
			toast.error('Name and expression are required');
			return;
		}
		creatingDestination = true;
		try {
			await client().send(
				new CreateDestinationCommand({
					Name: newDestinationName.trim(),
					Expression: newDestinationExpression.trim(),
					ExpressionType: newDestinationExpressionType === 'RuleName' ? 'RuleName' : 'MqttTopic',
					RoleArn: newDestinationRoleArn.trim() || undefined
				})
			);
			showCreateDestination = false;
			newDestinationName = '';
			newDestinationExpression = '';
			newDestinationRoleArn = '';
			await loadDestinations();
			toast.success('Destination created');
		} catch (err: unknown) {
			toast.error(`Failed to create destination: ${describeError(err)}`);
		} finally {
			creatingDestination = false;
		}
	}

	async function deleteDestination(name: string) {
		if (!(await confirmDestructive({ title: 'Delete Destination', message: `Delete destination "${name}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteDestinationCommand({ Name: name }));
			await loadDestinations();
			toast.success('Destination deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete destination: ${describeError(err)}`);
		}
	}

	async function createDeviceProfile() {
		if (!newDeviceProfileName.trim()) {
			toast.error('Device profile name is required');
			return;
		}
		creatingDeviceProfile = true;
		try {
			await client().send(
				new CreateDeviceProfileCommand({ Name: newDeviceProfileName.trim() })
			);
			showCreateDeviceProfile = false;
			newDeviceProfileName = '';
			await loadDeviceProfiles();
			toast.success('Device profile created');
		} catch (err: unknown) {
			toast.error(`Failed to create device profile: ${describeError(err)}`);
		} finally {
			creatingDeviceProfile = false;
		}
	}

	async function deleteDeviceProfile(id: string) {
		if (!(await confirmDestructive({ title: 'Delete Device Profile', message: `Delete device profile "${id}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteDeviceProfileCommand({ Id: id }));
			await loadDeviceProfiles();
			toast.success('Device profile deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete device profile: ${describeError(err)}`);
		}
	}

	async function createFuotaTask() {
		if (!newFuotaTaskName.trim() || !newFuotaTaskFirmwareImage.trim()) {
			toast.error('Name and firmware image are required');
			return;
		}
		creatingFuotaTask = true;
		try {
			await client().send(
				new CreateFuotaTaskCommand({
					Name: newFuotaTaskName.trim(),
					FirmwareUpdateImage: newFuotaTaskFirmwareImage.trim(),
					FirmwareUpdateRole: newFuotaTaskFirmwareRole.trim() || undefined,
					LoRaWAN: {}
				})
			);
			showCreateFuotaTask = false;
			newFuotaTaskName = '';
			newFuotaTaskFirmwareImage = '';
			newFuotaTaskFirmwareRole = '';
			await loadFuotaTasks();
			toast.success('FUOTA task created');
		} catch (err: unknown) {
			toast.error(`Failed to create FUOTA task: ${describeError(err)}`);
		} finally {
			creatingFuotaTask = false;
		}
	}

	async function deleteFuotaTask(id: string) {
		if (!(await confirmDestructive({ title: 'Delete FUOTA Task', message: `Delete FUOTA task "${id}"? This cannot be undone.` }))) return;
		try {
			await client().send(new DeleteFuotaTaskCommand({ Id: id }));
			await loadFuotaTasks();
			toast.success('FUOTA task deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete FUOTA task: ${describeError(err)}`);
		}
	}

	function setTab(tab: Tab) {
		activeTab = tab;
		searchQuery = '';
	}

	onRegionChange(() => {
		loadDevices();
		loadGateways();
		loadServiceProfiles();
		loadDestinations();
		loadDeviceProfiles();
		loadFuotaTasks();
	});
</script>

<div class="space-y-4">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-2">
			<Wifi class="h-6 w-6" />
			<h1 class="text-2xl font-bold">IoT Wireless</h1>
		</div>
	</div>

	<!-- Tabs -->
	<div class="flex gap-2 border-b pb-2 flex-wrap">
		{#each [
			{ id: 'devices' as Tab, label: 'Devices', icon: Radio },
			{ id: 'gateways' as Tab, label: 'Gateways', icon: Server },
			{ id: 'service-profiles' as Tab, label: 'Service Profiles', icon: BookOpen },
			{ id: 'destinations' as Tab, label: 'Destinations', icon: Layers },
			{ id: 'device-profiles' as Tab, label: 'Device Profiles', icon: BookOpen },
			{ id: 'fuota-tasks' as Tab, label: 'FUOTA Tasks', icon: Layers }
		] as tab}
			<button
				class="flex items-center gap-1 rounded px-3 py-1 text-sm font-medium transition-colors {activeTab === tab.id
					? 'bg-primary text-primary-foreground'
					: 'hover:bg-muted'}"
				onclick={() => setTab(tab.id)}
			>
				<tab.icon class="h-4 w-4" />
				{tab.label}
			</button>
		{/each}
	</div>

	<!-- Search bar -->
	<div class="flex items-center gap-2">
		<div class="relative flex-1 max-w-sm">
			<Search class="absolute left-2 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
			<input
				class="w-full rounded border bg-background pl-8 pr-3 py-1.5 text-sm"
				placeholder="Search..."
				bind:value={searchQuery}
			/>
		</div>
	</div>

	<!-- Wireless Devices Tab -->
	{#if activeTab === 'devices'}
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Wireless Devices ({filteredDevices.length})</h2>
				<div class="flex gap-2">
					<button
						class="flex items-center gap-1 rounded border px-3 py-1.5 text-sm hover:bg-muted"
						onclick={loadDevices}
					>
						<RefreshCw class="h-4 w-4 {loadingDevices ? 'animate-spin' : ''}" />
						Refresh
					</button>
					<button
						class="flex items-center gap-1 rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
						onclick={() => (showCreateDevice = true)}
					>
						<Plus class="h-4 w-4" />
						Create Device
					</button>
				</div>
			</div>

			{#if showCreateDevice}
				<div class="rounded border bg-muted/30 p-4 space-y-3">
					<h3 class="font-medium">Create Wireless Device</h3>
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label class="text-sm font-medium" for="new-device-name">Name *</label>
							<input id="new-device-name"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="my-device"
								bind:value={newDeviceName}
							/>
						</div>
						<div>
							<label class="text-sm font-medium" for="new-device-type">Type</label>
							<select id="new-device-type" class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm" bind:value={newDeviceType}>
								<option value="LoRaWAN">LoRaWAN</option>
								<option value="Sidewalk">Sidewalk</option>
							</select>
						</div>
						<div class="col-span-2">
							<label class="text-sm font-medium" for="new-device-destination">Destination Name</label>
							<input id="new-device-destination"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="my-destination"
								bind:value={newDeviceDestination}
							/>
						</div>
					</div>
					<div class="flex gap-2">
						<button
							class="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
							onclick={createDevice}
							disabled={creatingDevice}
						>
							{creatingDevice ? 'Creating...' : 'Create'}
						</button>
						<button
							class="rounded border px-3 py-1.5 text-sm hover:bg-muted"
							onclick={() => (showCreateDevice = false)}
						>
							Cancel
						</button>
					</div>
				</div>
			{/if}

			<div class="rounded border">
				<table class="w-full text-sm">
					<thead class="border-b bg-muted/50">
						<tr>
							<th class="px-3 py-2 text-left font-medium">ID</th>
							<th class="px-3 py-2 text-left font-medium">Type</th>
							<th class="px-3 py-2 text-left font-medium">Destination</th>
							<th class="px-3 py-2 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#if filteredDevices.length === 0}
							<tr><td colspan="4" class="px-3 py-6 text-center text-muted-foreground">No wireless devices found</td></tr>
						{:else}
							{#each filteredDevices as device}
								<tr class="border-b last:border-0 hover:bg-muted/20">
									<td class="px-3 py-2 font-mono text-xs">{device.Id ?? '—'}</td>
									<td class="px-3 py-2">{device.Type ?? '—'}</td>
									<td class="px-3 py-2">{device.DestinationName ?? '—'}</td>
									<td class="px-3 py-2 text-right">
										<button
											class="text-destructive hover:underline text-xs"
											onclick={() => deleteDevice(device.Id ?? '')}
										>
											<Trash2 class="inline h-3 w-3" /> Delete
										</button>
									</td>
								</tr>
							{/each}
						{/if}
					</tbody>
				</table>
			</div>
		</div>
	{/if}

	<!-- Wireless Gateways Tab -->
	{#if activeTab === 'gateways'}
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Wireless Gateways ({filteredGateways.length})</h2>
				<div class="flex gap-2">
					<button
						class="flex items-center gap-1 rounded border px-3 py-1.5 text-sm hover:bg-muted"
						onclick={loadGateways}
					>
						<RefreshCw class="h-4 w-4 {loadingGateways ? 'animate-spin' : ''}" />
						Refresh
					</button>
					<button
						class="flex items-center gap-1 rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
						onclick={() => (showCreateGateway = true)}
					>
						<Plus class="h-4 w-4" />
						Create Gateway
					</button>
				</div>
			</div>

			{#if showCreateGateway}
				<div class="rounded border bg-muted/30 p-4 space-y-3">
					<h3 class="font-medium">Create Wireless Gateway</h3>
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label class="text-sm font-medium" for="new-gateway-name">Name *</label>
							<input id="new-gateway-name"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="my-gateway"
								bind:value={newGatewayName}
							/>
						</div>
						<div>
							<label class="text-sm font-medium" for="new-gateway-description">Description</label>
							<input id="new-gateway-description"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="Optional description"
								bind:value={newGatewayDescription}
							/>
						</div>
					</div>
					<div class="flex gap-2">
						<button
							class="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
							onclick={createGateway}
							disabled={creatingGateway}
						>
							{creatingGateway ? 'Creating...' : 'Create'}
						</button>
						<button
							class="rounded border px-3 py-1.5 text-sm hover:bg-muted"
							onclick={() => (showCreateGateway = false)}
						>
							Cancel
						</button>
					</div>
				</div>
			{/if}

			<div class="rounded border">
				<table class="w-full text-sm">
					<thead class="border-b bg-muted/50">
						<tr>
							<th class="px-3 py-2 text-left font-medium">ID</th>
							<th class="px-3 py-2 text-left font-medium">Name</th>
							<th class="px-3 py-2 text-left font-medium">Description</th>
							<th class="px-3 py-2 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#if filteredGateways.length === 0}
							<tr><td colspan="4" class="px-3 py-6 text-center text-muted-foreground">No gateways found</td></tr>
						{:else}
							{#each filteredGateways as gw}
								<tr class="border-b last:border-0 hover:bg-muted/20">
									<td class="px-3 py-2 font-mono text-xs">{gw.Id ?? '—'}</td>
									<td class="px-3 py-2">{gw.Name ?? '—'}</td>
									<td class="px-3 py-2 text-muted-foreground">{gw.Description ?? '—'}</td>
									<td class="px-3 py-2 text-right">
										<button
											class="text-destructive hover:underline text-xs"
											onclick={() => deleteGateway(gw.Id ?? '')}
										>
											<Trash2 class="inline h-3 w-3" /> Delete
										</button>
									</td>
								</tr>
							{/each}
						{/if}
					</tbody>
				</table>
			</div>
		</div>
	{/if}

	<!-- Service Profiles Tab -->
	{#if activeTab === 'service-profiles'}
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Service Profiles ({filteredServiceProfiles.length})</h2>
				<div class="flex gap-2">
					<button
						class="flex items-center gap-1 rounded border px-3 py-1.5 text-sm hover:bg-muted"
						onclick={loadServiceProfiles}
					>
						<RefreshCw class="h-4 w-4 {loadingServiceProfiles ? 'animate-spin' : ''}" />
						Refresh
					</button>
					<button
						class="flex items-center gap-1 rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
						onclick={() => (showCreateServiceProfile = true)}
					>
						<Plus class="h-4 w-4" />
						Create
					</button>
				</div>
			</div>

			{#if showCreateServiceProfile}
				<div class="rounded border bg-muted/30 p-4 space-y-3">
					<h3 class="font-medium">Create Service Profile</h3>
					<div>
						<label class="text-sm font-medium" for="new-service-profile-name">Name *</label>
						<input id="new-service-profile-name"
							class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm max-w-sm"
							placeholder="my-service-profile"
							bind:value={newServiceProfileName}
						/>
					</div>
					<div class="flex gap-2">
						<button
							class="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
							onclick={createServiceProfile}
							disabled={creatingServiceProfile}
						>
							{creatingServiceProfile ? 'Creating...' : 'Create'}
						</button>
						<button
							class="rounded border px-3 py-1.5 text-sm hover:bg-muted"
							onclick={() => (showCreateServiceProfile = false)}
						>
							Cancel
						</button>
					</div>
				</div>
			{/if}

			<div class="rounded border">
				<table class="w-full text-sm">
					<thead class="border-b bg-muted/50">
						<tr>
							<th class="px-3 py-2 text-left font-medium">ID</th>
							<th class="px-3 py-2 text-left font-medium">Name</th>
							<th class="px-3 py-2 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#if filteredServiceProfiles.length === 0}
							<tr><td colspan="3" class="px-3 py-6 text-center text-muted-foreground">No service profiles found</td></tr>
						{:else}
							{#each filteredServiceProfiles as sp}
								<tr class="border-b last:border-0 hover:bg-muted/20">
									<td class="px-3 py-2 font-mono text-xs">{sp.Id ?? '—'}</td>
									<td class="px-3 py-2">{sp.Name ?? '—'}</td>
									<td class="px-3 py-2 text-right">
										<button
											class="text-destructive hover:underline text-xs"
											onclick={() => deleteServiceProfile(sp.Id ?? '')}
										>
											<Trash2 class="inline h-3 w-3" /> Delete
										</button>
									</td>
								</tr>
							{/each}
						{/if}
					</tbody>
				</table>
			</div>
		</div>
	{/if}

	<!-- Destinations Tab -->
	{#if activeTab === 'destinations'}
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Destinations ({filteredDestinations.length})</h2>
				<div class="flex gap-2">
					<button
						class="flex items-center gap-1 rounded border px-3 py-1.5 text-sm hover:bg-muted"
						onclick={loadDestinations}
					>
						<RefreshCw class="h-4 w-4 {loadingDestinations ? 'animate-spin' : ''}" />
						Refresh
					</button>
					<button
						class="flex items-center gap-1 rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
						onclick={() => (showCreateDestination = true)}
					>
						<Plus class="h-4 w-4" />
						Create
					</button>
				</div>
			</div>

			{#if showCreateDestination}
				<div class="rounded border bg-muted/30 p-4 space-y-3">
					<h3 class="font-medium">Create Destination</h3>
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label class="text-sm font-medium" for="new-destination-name">Name *</label>
							<input id="new-destination-name"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="my-destination"
								bind:value={newDestinationName}
							/>
						</div>
						<div>
							<label class="text-sm font-medium" for="new-destination-expression-type">Expression Type</label>
							<select id="new-destination-expression-type" class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm" bind:value={newDestinationExpressionType}>
								<option value="RuleName">RuleName</option>
								<option value="MqttTopic">MqttTopic</option>
							</select>
						</div>
						<div>
							<label class="text-sm font-medium" for="new-destination-expression">Expression *</label>
							<input id="new-destination-expression"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="my-iot-rule"
								bind:value={newDestinationExpression}
							/>
						</div>
						<div>
							<label class="text-sm font-medium" for="new-destination-role-arn">Role ARN</label>
							<input id="new-destination-role-arn"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="arn:aws:iam::..."
								bind:value={newDestinationRoleArn}
							/>
						</div>
					</div>
					<div class="flex gap-2">
						<button
							class="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
							onclick={createDestination}
							disabled={creatingDestination}
						>
							{creatingDestination ? 'Creating...' : 'Create'}
						</button>
						<button
							class="rounded border px-3 py-1.5 text-sm hover:bg-muted"
							onclick={() => (showCreateDestination = false)}
						>
							Cancel
						</button>
					</div>
				</div>
			{/if}

			<div class="rounded border">
				<table class="w-full text-sm">
					<thead class="border-b bg-muted/50">
						<tr>
							<th class="px-3 py-2 text-left font-medium">Name</th>
							<th class="px-3 py-2 text-left font-medium">Expression</th>
							<th class="px-3 py-2 text-left font-medium">Type</th>
							<th class="px-3 py-2 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#if filteredDestinations.length === 0}
							<tr><td colspan="4" class="px-3 py-6 text-center text-muted-foreground">No destinations found</td></tr>
						{:else}
							{#each filteredDestinations as dest}
								<tr class="border-b last:border-0 hover:bg-muted/20">
									<td class="px-3 py-2 font-medium">{dest.Name ?? '—'}</td>
									<td class="px-3 py-2 font-mono text-xs">{dest.Expression ?? '—'}</td>
									<td class="px-3 py-2">{dest.ExpressionType ?? '—'}</td>
									<td class="px-3 py-2 text-right">
										<button
											class="text-destructive hover:underline text-xs"
											onclick={() => deleteDestination(dest.Name ?? '')}
										>
											<Trash2 class="inline h-3 w-3" /> Delete
										</button>
									</td>
								</tr>
							{/each}
						{/if}
					</tbody>
				</table>
			</div>
		</div>
	{/if}

	<!-- Device Profiles Tab -->
	{#if activeTab === 'device-profiles'}
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">Device Profiles ({filteredDeviceProfiles.length})</h2>
				<div class="flex gap-2">
					<button
						class="flex items-center gap-1 rounded border px-3 py-1.5 text-sm hover:bg-muted"
						onclick={loadDeviceProfiles}
					>
						<RefreshCw class="h-4 w-4 {loadingDeviceProfiles ? 'animate-spin' : ''}" />
						Refresh
					</button>
					<button
						class="flex items-center gap-1 rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
						onclick={() => (showCreateDeviceProfile = true)}
					>
						<Plus class="h-4 w-4" />
						Create
					</button>
				</div>
			</div>

			{#if showCreateDeviceProfile}
				<div class="rounded border bg-muted/30 p-4 space-y-3">
					<h3 class="font-medium">Create Device Profile</h3>
					<div>
						<label class="text-sm font-medium" for="new-device-profile-name">Name *</label>
						<input id="new-device-profile-name"
							class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm max-w-sm"
							placeholder="my-device-profile"
							bind:value={newDeviceProfileName}
						/>
					</div>
					<div class="flex gap-2">
						<button
							class="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
							onclick={createDeviceProfile}
							disabled={creatingDeviceProfile}
						>
							{creatingDeviceProfile ? 'Creating...' : 'Create'}
						</button>
						<button
							class="rounded border px-3 py-1.5 text-sm hover:bg-muted"
							onclick={() => (showCreateDeviceProfile = false)}
						>
							Cancel
						</button>
					</div>
				</div>
			{/if}

			<div class="rounded border">
				<table class="w-full text-sm">
					<thead class="border-b bg-muted/50">
						<tr>
							<th class="px-3 py-2 text-left font-medium">ID</th>
							<th class="px-3 py-2 text-left font-medium">Name</th>
							<th class="px-3 py-2 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#if filteredDeviceProfiles.length === 0}
							<tr><td colspan="3" class="px-3 py-6 text-center text-muted-foreground">No device profiles found</td></tr>
						{:else}
							{#each filteredDeviceProfiles as dp}
								<tr class="border-b last:border-0 hover:bg-muted/20">
									<td class="px-3 py-2 font-mono text-xs">{dp.Id ?? '—'}</td>
									<td class="px-3 py-2">{dp.Name ?? '—'}</td>
									<td class="px-3 py-2 text-right">
										<button
											class="text-destructive hover:underline text-xs"
											onclick={() => deleteDeviceProfile(dp.Id ?? '')}
										>
											<Trash2 class="inline h-3 w-3" /> Delete
										</button>
									</td>
								</tr>
							{/each}
						{/if}
					</tbody>
				</table>
			</div>
		</div>
	{/if}

	<!-- FUOTA Tasks Tab -->
	{#if activeTab === 'fuota-tasks'}
		<div class="space-y-3">
			<div class="flex items-center justify-between">
				<h2 class="text-lg font-semibold">FUOTA Tasks ({filteredFuotaTasks.length})</h2>
				<div class="flex gap-2">
					<button
						class="flex items-center gap-1 rounded border px-3 py-1.5 text-sm hover:bg-muted"
						onclick={loadFuotaTasks}
					>
						<RefreshCw class="h-4 w-4 {loadingFuotaTasks ? 'animate-spin' : ''}" />
						Refresh
					</button>
					<button
						class="flex items-center gap-1 rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90"
						onclick={() => (showCreateFuotaTask = true)}
					>
						<Plus class="h-4 w-4" />
						Create
					</button>
				</div>
			</div>

			{#if showCreateFuotaTask}
				<div class="rounded border bg-muted/30 p-4 space-y-3">
					<h3 class="font-medium">Create FUOTA Task</h3>
					<div class="grid grid-cols-2 gap-3">
						<div>
							<label class="text-sm font-medium" for="new-fuota-task-name">Name *</label>
							<input id="new-fuota-task-name"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="my-fuota-task"
								bind:value={newFuotaTaskName}
							/>
						</div>
						<div>
							<label class="text-sm font-medium" for="new-fuota-task-firmware-role">Firmware Update Role ARN</label>
							<input id="new-fuota-task-firmware-role"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="arn:aws:iam::..."
								bind:value={newFuotaTaskFirmwareRole}
							/>
						</div>
						<div class="col-span-2">
							<label class="text-sm font-medium" for="new-fuota-task-firmware-image">Firmware Image S3 URL *</label>
							<input id="new-fuota-task-firmware-image"
								class="mt-1 w-full rounded border bg-background px-3 py-1.5 text-sm"
								placeholder="s3://my-bucket/firmware.bin"
								bind:value={newFuotaTaskFirmwareImage}
							/>
						</div>
					</div>
					<div class="flex gap-2">
						<button
							class="rounded bg-primary px-3 py-1.5 text-sm text-primary-foreground hover:bg-primary/90 disabled:opacity-50"
							onclick={createFuotaTask}
							disabled={creatingFuotaTask}
						>
							{creatingFuotaTask ? 'Creating...' : 'Create'}
						</button>
						<button
							class="rounded border px-3 py-1.5 text-sm hover:bg-muted"
							onclick={() => (showCreateFuotaTask = false)}
						>
							Cancel
						</button>
					</div>
				</div>
			{/if}

			<div class="rounded border">
				<table class="w-full text-sm">
					<thead class="border-b bg-muted/50">
						<tr>
							<th class="px-3 py-2 text-left font-medium">ID</th>
							<th class="px-3 py-2 text-left font-medium">Name</th>
							<th class="px-3 py-2 text-right font-medium">Actions</th>
						</tr>
					</thead>
					<tbody>
						{#if filteredFuotaTasks.length === 0}
							<tr><td colspan="3" class="px-3 py-6 text-center text-muted-foreground">No FUOTA tasks found</td></tr>
						{:else}
							{#each filteredFuotaTasks as ft}
								<tr class="border-b last:border-0 hover:bg-muted/20">
									<td class="px-3 py-2 font-mono text-xs">{ft.Id ?? '—'}</td>
									<td class="px-3 py-2">{ft.Name ?? '—'}</td>
									<td class="px-3 py-2 text-right">
										<button
											class="text-destructive hover:underline text-xs"
											onclick={() => deleteFuotaTask(ft.Id ?? '')}
										>
											<Trash2 class="inline h-3 w-3" /> Delete
										</button>
									</td>
								</tr>
							{/each}
						{/if}
					</tbody>
				</table>
			</div>
		</div>
	{/if}
</div>
