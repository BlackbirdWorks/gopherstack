<script lang="ts">
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getELBClient } from '$lib/aws-client';
	import {
		DescribeLoadBalancersCommand,
		DescribeLoadBalancerAttributesCommand,
		DescribeLoadBalancerPoliciesCommand,
		CreateLoadBalancerCommand,
		DeleteLoadBalancerCommand,
		CreateLoadBalancerListenersCommand,
		DeleteLoadBalancerListenersCommand,
		RegisterInstancesWithLoadBalancerCommand,
		DeregisterInstancesFromLoadBalancerCommand,
		ConfigureHealthCheckCommand,
		ModifyLoadBalancerAttributesCommand,
		EnableAvailabilityZonesForLoadBalancerCommand,
		DisableAvailabilityZonesForLoadBalancerCommand,
		AttachLoadBalancerToSubnetsCommand,
		DetachLoadBalancerFromSubnetsCommand,
		SetLoadBalancerListenerSSLCertificateCommand,
		SetLoadBalancerPoliciesOfListenerCommand,
		SetLoadBalancerPoliciesForBackendServerCommand,
		CreateAppCookieStickinessPolicyCommand,
		CreateLBCookieStickinessPolicyCommand,
		DeleteLoadBalancerPolicyCommand,
		DescribeInstanceHealthCommand,
		type LoadBalancerDescription,
		type ListenerDescription,
		type PolicyDescription
	} from '@aws-sdk/client-elastic-load-balancing';
	import { toast } from 'svelte-sonner';

	const elb = regionalClient(getELBClient);

	type ActiveTab = 'overview' | 'listeners' | 'healthcheck' | 'attributes' | 'instances' | 'policies';

	let loadBalancers = $state<LoadBalancerDescription[]>([]);
	let selectedLB = $state<LoadBalancerDescription | null>(null);
	let activeTab = $state<ActiveTab>('overview');
	let loading = $state(false);
	let lbSearch = $state('');

	// create modal
	let showCreateModal = $state(false);
	let newName = $state('');
	let newAZ = $state('us-east-1a');
	let newListenerPort = $state(80);
	let newInstancePort = $state(80);
	let newScheme = $state('internet-facing');

	// listener management
	let showAddListenerModal = $state(false);
	let listenerProto = $state('HTTP');
	let listenerLBPort = $state(443);
	let listenerInstPort = $state(8443);
	let listenerCertARN = $state('');

	// health check
	let hcTarget = $state('HTTP:80/health');
	let hcInterval = $state(30);
	let hcTimeout = $state(5);
	let hcHealthy = $state(2);
	let hcUnhealthy = $state(2);

	// attributes
	let attrCrossZone = $state(false);
	let attrDraining = $state(false);
	let attrDrainingTimeout = $state(300);
	let attrIdleTimeout = $state(60);

	// instances
	let newInstanceId = $state('');

	// policies
	let showAddPolicyModal = $state(false);
	let policyType = $state<'app-cookie' | 'lb-cookie'>('lb-cookie');
	let policyName = $state('');
	let policyCookieName = $state('');
	let policyExpiry = $state(0);

	// SSL cert setter
	let showSSLModal = $state(false);
	let sslPort = $state(443);
	let sslCertARN = $state('');

	// policies for listener
	let showListenerPoliciesModal = $state(false);
	let lpPort = $state(80);
	let lpPolicyInput = $state('');

	// backend server policies modal
	let showBackendPoliciesModal = $state(false);
	let bsdPort = $state(0);
	let bsdPolicyInput = $state('');

	// AZ management
	let newAZInput = $state('');

	// subnet management
	let newSubnetInput = $state('');
	// LB policies list
	let lbPolicies = $state<PolicyDescription[]>([]);
	let lbAttributes = $state<{ CrossZoneLoadBalancing?: boolean; ConnectionDraining?: boolean; ConnectionDrainingTimeout?: number; IdleTimeout?: number } | null>(null);

	// instance health state
	let instanceHealthMap = $state<Record<string, string>>({});
	let healthLoading = $state(false);

	async function loadBalancersList() {
		loading = true;
		try {
			const out = await elb().send(new DescribeLoadBalancersCommand({}));
			loadBalancers = out.LoadBalancerDescriptions ?? [];
			if (selectedLB) {
				const updated = loadBalancers.find((lb) => lb.LoadBalancerName === selectedLB!.LoadBalancerName);
				selectedLB = updated ?? null;
				if (selectedLB) await loadLBDetails();
			}
		} catch (err: unknown) {
			toast.error(`Failed to load balancers: ${(err as Error).message}`);
		} finally {
			loading = false;
		}
	}

	function updateHealthCheckState(hc: import('@aws-sdk/client-elastic-load-balancing').HealthCheck | undefined) {
		if (!hc) return;
		hcTarget = hc.Target ?? 'HTTP:80/health';
		hcInterval = hc.Interval ?? 30;
		hcTimeout = hc.Timeout ?? 5;
		hcHealthy = hc.HealthyThreshold ?? 2;
		hcUnhealthy = hc.UnhealthyThreshold ?? 2;
	}

	function updateAttributeState(attrs: import('@aws-sdk/client-elastic-load-balancing').LoadBalancerAttributes | undefined) {
		lbAttributes = {
			CrossZoneLoadBalancing: attrs?.CrossZoneLoadBalancing?.Enabled ?? false,
			ConnectionDraining: attrs?.ConnectionDraining?.Enabled ?? false,
			ConnectionDrainingTimeout: attrs?.ConnectionDraining?.Timeout ?? 300,
			IdleTimeout: attrs?.ConnectionSettings?.IdleTimeout ?? 60
		};
		attrCrossZone = lbAttributes.CrossZoneLoadBalancing ?? false;
		attrDraining = lbAttributes.ConnectionDraining ?? false;
		attrDrainingTimeout = lbAttributes.ConnectionDrainingTimeout ?? 300;
		attrIdleTimeout = lbAttributes.IdleTimeout ?? 60;
	}

	async function loadLBDetails() {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			const [attrOut, polOut] = await Promise.all([
				elb().send(new DescribeLoadBalancerAttributesCommand({ LoadBalancerName: selectedLB.LoadBalancerName })),
				elb().send(new DescribeLoadBalancerPoliciesCommand({ LoadBalancerName: selectedLB.LoadBalancerName }))
			]);
			updateAttributeState(attrOut.LoadBalancerAttributes);
			updateHealthCheckState(selectedLB?.HealthCheck);

			lbPolicies = polOut.PolicyDescriptions ?? [];
		} catch (err: unknown) {
			toast.error(`Failed to load LB details: ${(err as Error).message}`);
		}
	}

	async function selectLB(lb: LoadBalancerDescription) {
		selectedLB = lb;
		activeTab = 'overview';
		await loadLBDetails();
	}

	async function createLoadBalancer() {
		const createdName = newName;
		try {
			await elb().send(new CreateLoadBalancerCommand({
				LoadBalancerName: createdName,
				AvailabilityZones: [newAZ],
				Scheme: newScheme,
				Listeners: [{
					Protocol: 'HTTP',
					LoadBalancerPort: newListenerPort,
					InstanceProtocol: 'HTTP',
					InstancePort: newInstancePort
				}]
			}));
			showCreateModal = false;
			newName = '';
			await loadBalancersList();
			const created = loadBalancers.find((lb) => lb.LoadBalancerName === createdName);
			if (created) await selectLB(created);
			toast.success('Load balancer created');
		} catch (err: unknown) {
			toast.error(`Failed to create: ${(err as Error).message}`);
		}
	}

	async function deleteLoadBalancer(name: string) {
		try {
			await elb().send(new DeleteLoadBalancerCommand({ LoadBalancerName: name }));
			if (selectedLB?.LoadBalancerName === name) selectedLB = null;
			await loadBalancersList();
			toast.success('Load balancer deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete: ${(err as Error).message}`);
		}
	}

	async function addListener() {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new CreateLoadBalancerListenersCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				Listeners: [{
					Protocol: listenerProto,
					LoadBalancerPort: listenerLBPort,
					InstanceProtocol: listenerProto === 'HTTPS' ? 'HTTP' : listenerProto === 'SSL' ? 'TCP' : listenerProto,
					InstancePort: listenerInstPort
				}]
			}));
			if (listenerCertARN && (listenerProto === 'HTTPS' || listenerProto === 'SSL')) {
				await elb().send(new SetLoadBalancerListenerSSLCertificateCommand({
					LoadBalancerName: selectedLB.LoadBalancerName,
					LoadBalancerPort: listenerLBPort,
					SSLCertificateId: listenerCertARN
				}));
			}
			showAddListenerModal = false;
			await loadBalancersList();
			toast.success('Listener added');
		} catch (err: unknown) {
			toast.error(`Failed to add listener: ${(err as Error).message}`);
		}
	}

	async function deleteListener(port: number) {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new DeleteLoadBalancerListenersCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				LoadBalancerPorts: [port]
			}));
			await loadBalancersList();
			toast.success('Listener removed');
		} catch (err: unknown) {
			toast.error(`Failed to remove listener: ${(err as Error).message}`);
		}
	}

	async function saveHealthCheck() {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new ConfigureHealthCheckCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				HealthCheck: {
					Target: hcTarget,
					Interval: hcInterval,
					Timeout: hcTimeout,
					HealthyThreshold: hcHealthy,
					UnhealthyThreshold: hcUnhealthy
				}
			}));
			await loadBalancersList();
			toast.success('Health check updated');
		} catch (err: unknown) {
			toast.error(`Failed to update health check: ${(err as Error).message}`);
		}
	}

	async function saveAttributes() {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new ModifyLoadBalancerAttributesCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				LoadBalancerAttributes: {
					CrossZoneLoadBalancing: { Enabled: attrCrossZone },
					ConnectionDraining: { Enabled: attrDraining, Timeout: attrDrainingTimeout },
					ConnectionSettings: { IdleTimeout: attrIdleTimeout }
				}
			}));
			await loadLBDetails();
			toast.success('Attributes updated');
		} catch (err: unknown) {
			toast.error(`Failed to update attributes: ${(err as Error).message}`);
		}
	}

	async function registerInstance() {
		if (!selectedLB?.LoadBalancerName || !newInstanceId) return;
		try {
			await elb().send(new RegisterInstancesWithLoadBalancerCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				Instances: [{ InstanceId: newInstanceId }]
			}));
			newInstanceId = '';
			await loadBalancersList();
			toast.success('Instance registered');
		} catch (err: unknown) {
			toast.error(`Failed to register instance: ${(err as Error).message}`);
		}
	}

	async function deregisterInstance(instanceId: string) {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new DeregisterInstancesFromLoadBalancerCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				Instances: [{ InstanceId: instanceId }]
			}));
			await loadBalancersList();
			toast.success('Instance deregistered');
		} catch (err: unknown) {
			toast.error(`Failed to deregister: ${(err as Error).message}`);
		}
	}

	async function addPolicy() {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			if (policyType === 'app-cookie') {
				await elb().send(new CreateAppCookieStickinessPolicyCommand({
					LoadBalancerName: selectedLB.LoadBalancerName,
					PolicyName: policyName,
					CookieName: policyCookieName
				}));
			} else {
				await elb().send(new CreateLBCookieStickinessPolicyCommand({
					LoadBalancerName: selectedLB.LoadBalancerName,
					PolicyName: policyName,
					CookieExpirationPeriod: policyExpiry > 0 ? policyExpiry : undefined
				}));
			}
			showAddPolicyModal = false;
			policyName = '';
			policyCookieName = '';
			policyExpiry = 0;
			await loadLBDetails();
			toast.success('Policy created');
		} catch (err: unknown) {
			toast.error(`Failed to create policy: ${(err as Error).message}`);
		}
	}

	async function deletePolicy(name: string) {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new DeleteLoadBalancerPolicyCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				PolicyName: name
			}));
			await loadLBDetails();
			toast.success('Policy deleted');
		} catch (err: unknown) {
			toast.error(`Failed to delete policy: ${(err as Error).message}`);
		}
	}

	async function setSSLCertificate() {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new SetLoadBalancerListenerSSLCertificateCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				LoadBalancerPort: sslPort,
				SSLCertificateId: sslCertARN
			}));
			showSSLModal = false;
			sslCertARN = '';
			await loadBalancersList();
			toast.success('SSL certificate updated');
		} catch (err: unknown) {
			toast.error(`Failed to set SSL cert: ${(err as Error).message}`);
		}
	}

	async function setListenerPolicies() {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			const names = lpPolicyInput.split(',').map((s) => s.trim()).filter(Boolean);
			await elb().send(new SetLoadBalancerPoliciesOfListenerCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				LoadBalancerPort: lpPort,
				PolicyNames: names
			}));
			showListenerPoliciesModal = false;
			lpPolicyInput = '';
			await loadBalancersList();
			toast.success('Listener policies updated');
		} catch (err: unknown) {
			toast.error(`Failed to update listener policies: ${(err as Error).message}`);
		}
	}

	async function enableAZ() {
		if (!selectedLB?.LoadBalancerName || !newAZInput) return;
		try {
			await elb().send(new EnableAvailabilityZonesForLoadBalancerCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				AvailabilityZones: [newAZInput]
			}));
			newAZInput = '';
			await loadBalancersList();
			toast.success('Availability zone enabled');
		} catch (err: unknown) {
			toast.error(`Failed to enable AZ: ${(err as Error).message}`);
		}
	}

	async function disableAZ(az: string) {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new DisableAvailabilityZonesForLoadBalancerCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				AvailabilityZones: [az]
			}));
			await loadBalancersList();
			toast.success('Availability zone disabled');
		} catch (err: unknown) {
			toast.error(`Failed to disable AZ: ${(err as Error).message}`);
		}
	}

	async function detachSubnet(subnet: string) {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			await elb().send(new DetachLoadBalancerFromSubnetsCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				Subnets: [subnet]
			}));
			await loadBalancersList();
			toast.success('Subnet detached');
		} catch (err: unknown) {
			toast.error(`Failed to detach subnet: ${(err as Error).message}`);
		}
	}

	async function attachSubnet() {
		if (!selectedLB?.LoadBalancerName || !newSubnetInput.trim()) return;
		try {
			await elb().send(new AttachLoadBalancerToSubnetsCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				Subnets: [newSubnetInput.trim()]
			}));
			newSubnetInput = '';
			await loadBalancersList();
			toast.success('Subnet attached');
		} catch (err: unknown) {
			toast.error(`Failed to attach subnet: ${(err as Error).message}`);
		}
	}

	async function setBackendPolicies(instancePort: number) {
		if (!selectedLB?.LoadBalancerName) return;
		try {
			const names = bsdPolicyInput.split(',').map((s) => s.trim()).filter(Boolean);
			await elb().send(new SetLoadBalancerPoliciesForBackendServerCommand({
				LoadBalancerName: selectedLB.LoadBalancerName,
				InstancePort: instancePort,
				PolicyNames: names
			}));
			showBackendPoliciesModal = false;
			bsdPolicyInput = '';
			await loadBalancersList();
			toast.success('Backend server policies updated');
		} catch (err: unknown) {
			toast.error(`Failed: ${(err as Error).message}`);
		}
	}

	function openBackendPoliciesModal(port: number) {
		bsdPort = port;
		bsdPolicyInput = '';
		showBackendPoliciesModal = true;
	}

	async function refreshInstanceHealth() {
		if (!selectedLB?.LoadBalancerName) return;
		healthLoading = true;
		try {
			const out = await elb().send(new DescribeInstanceHealthCommand({ LoadBalancerName: selectedLB.LoadBalancerName }));
			const map: Record<string, string> = {};
			for (const s of out.InstanceStates ?? []) {
				if (s.InstanceId) map[s.InstanceId] = s.State ?? 'Unknown';
			}
			instanceHealthMap = map;
		} catch (err: unknown) {
			toast.error(`Failed to fetch health: ${(err as Error).message}`);
		} finally {
			healthLoading = false;
		}
	}

	async function loadDemoData() {
		const name = 'demo-elb';
		const existing = loadBalancers.find((lb) => lb.LoadBalancerName === name);
		if (existing) {
			await selectLB(existing);
			return;
		}
		try {
			await elb().send(new CreateLoadBalancerCommand({
				LoadBalancerName: name,
				AvailabilityZones: ['us-east-1a', 'us-east-1b'],
				Scheme: 'internet-facing',
				Listeners: [
					{ Protocol: 'HTTP', LoadBalancerPort: 80, InstanceProtocol: 'HTTP', InstancePort: 8080 },
					{ Protocol: 'HTTPS', LoadBalancerPort: 443, InstanceProtocol: 'HTTP', InstancePort: 8080 }
				]
			}));
			await elb().send(new ConfigureHealthCheckCommand({
				LoadBalancerName: name,
				HealthCheck: { Target: 'HTTP:8080/health', Interval: 30, Timeout: 5, HealthyThreshold: 2, UnhealthyThreshold: 3 }
			}));
			await elb().send(new CreateLBCookieStickinessPolicyCommand({ LoadBalancerName: name, PolicyName: 'demo-sticky' }));
			await loadBalancersList();
			const created = loadBalancers.find((lb) => lb.LoadBalancerName === name);
			if (created) await selectLB(created);
			toast.success('Demo data loaded');
		} catch (err: unknown) {
			toast.error(`Demo data failed: ${(err as Error).message}`);
		}
	}

	function copyToClipboard(text: string) {
		navigator.clipboard.writeText(text).then(() => toast.success('Copied!')).catch(() => toast.error('Copy failed'));
	}

	function closeModals(e: KeyboardEvent) {
		if (e.key !== 'Escape') return;
		showCreateModal = false;
		showAddListenerModal = false;
		showSSLModal = false;
		showListenerPoliciesModal = false;
		showAddPolicyModal = false;
		showBackendPoliciesModal = false;
	}

	function protocolColor(proto: string | undefined): string {
		switch (proto) {
			case 'HTTP': return 'bg-green-100 text-green-800 dark:bg-green-900/30 dark:text-green-300';
			case 'HTTPS': return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300';
			case 'TCP': return 'bg-slate-100 text-slate-700 dark:bg-slate-700 dark:text-slate-300';
			case 'SSL': return 'bg-purple-100 text-purple-800 dark:bg-purple-900/30 dark:text-purple-300';
			default: return 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
		}
	}

	function policyTypeColor(typeName: string | undefined): string {
		if (typeName?.includes('AppCookie')) return 'bg-amber-100 text-amber-800 dark:bg-amber-900/30 dark:text-amber-300';
		if (typeName?.includes('LBCookie')) return 'bg-teal-100 text-teal-800 dark:bg-teal-900/30 dark:text-teal-300';
		if (typeName?.includes('SSL')) return 'bg-blue-100 text-blue-800 dark:bg-blue-900/30 dark:text-blue-300';
		if (typeName?.includes('Proxy')) return 'bg-orange-100 text-orange-800 dark:bg-orange-900/30 dark:text-orange-300';
		return 'bg-slate-100 text-slate-600 dark:bg-slate-700 dark:text-slate-300';
	}

	// selectedLB and its detail data (policies, attributes, instance health)
	// reference a load balancer name that is only unique within the region
	// it was fetched from; clear the selection and fall back to the list
	// rather than only re-listing load balancers.
	onRegionChange(() => {
		selectedLB = null;
		activeTab = 'overview';
		lbPolicies = [];
		lbAttributes = null;
		instanceHealthMap = {};
		void loadBalancersList();
	});
</script>

<div class="space-y-6 p-6" role="presentation" onkeydown={closeModals}>
	<!-- Header -->
	<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
		<div class="flex items-center justify-between">
			<h1 class="text-3xl font-bold text-slate-900 dark:text-white">Classic Load Balancers (ELB)</h1>
			<div class="flex items-center gap-2">
				<button type="button" onclick={() => loadDemoData()} class="rounded-lg border border-slate-200 px-3 py-2 text-sm font-medium text-slate-600 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700">
					Demo Data
				</button>
				<button type="button" onclick={() => loadBalancersList()} class="rounded-lg border border-slate-200 px-3 py-2 text-sm font-medium text-slate-600 hover:bg-slate-50 dark:border-slate-600 dark:text-slate-300 dark:hover:bg-slate-700">
					↺ Refresh
				</button>
				<button type="button" onclick={() => (showCreateModal = true)} class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">
					+ Create Load Balancer
				</button>
			</div>
		</div>
	</div>

	<div class="grid grid-cols-1 gap-6 lg:grid-cols-3">
		<!-- LB List -->
		<div class="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm dark:border-slate-700 dark:bg-slate-800">
			<h2 class="mb-3 text-sm font-semibold uppercase tracking-wide text-slate-500 dark:text-slate-400">Load Balancers</h2>
			<input
				bind:value={lbSearch}
				placeholder="Filter…"
				class="mb-2 w-full rounded-lg border border-slate-300 px-3 py-1.5 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white"
			/>
			{#if loading}
				<p class="text-sm text-slate-500">Loading…</p>
			{:else if loadBalancers.length === 0}
				<p class="text-sm text-slate-500 dark:text-slate-400">No load balancers found.</p>
			{:else}
				<ul class="space-y-1">
					{#each loadBalancers.filter(lb => !lbSearch || (lb.LoadBalancerName ?? '').toLowerCase().includes(lbSearch.toLowerCase())) as lb}
						<li class="flex items-center gap-1">
							<button
								type="button"
								onclick={() => selectLB(lb)}
								class="flex-1 rounded-lg px-3 py-2 text-left text-sm hover:bg-slate-50 dark:hover:bg-slate-700 {selectedLB?.LoadBalancerName === lb.LoadBalancerName ? 'bg-indigo-50 font-semibold text-indigo-700 dark:bg-indigo-900/30 dark:text-indigo-300' : 'text-slate-700 dark:text-slate-300'}"
							>
								<span class="block truncate">{lb.LoadBalancerName}</span>
								<span class="flex items-center gap-1 text-xs text-slate-400 dark:text-slate-500">
									<span class="rounded-full px-1.5 py-0 text-xs font-medium {(lb.Scheme ?? 'internet-facing') === 'internet-facing' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300'}">{lb.Scheme ?? 'internet-facing'}</span>
									· {lb.ListenerDescriptions?.length ?? 0} listener{(lb.ListenerDescriptions?.length ?? 0) === 1 ? '' : 's'}
								</span>
							</button>
							<button
								type="button"
								onclick={() => deleteLoadBalancer(lb.LoadBalancerName ?? '')}
								class="flex-shrink-0 rounded px-1.5 py-0.5 text-xs text-red-500 hover:bg-red-50 hover:text-red-700 dark:hover:bg-red-900/30"
							>
								Delete
							</button>
						</li>
					{/each}
				</ul>
			{/if}
		</div>

		<!-- Detail Panel -->
		<div class="lg:col-span-2">
			{#if selectedLB}
				<!-- Tabs -->
				<div class="mb-4 flex gap-1 rounded-xl border border-slate-200 bg-white p-1 shadow-sm dark:border-slate-700 dark:bg-slate-800">
					{#each (['overview', 'listeners', 'healthcheck', 'attributes', 'instances', 'policies'] as ActiveTab[]) as tab}
						<button
							type="button"
							onclick={() => (activeTab = tab)}
							class="rounded-lg px-3 py-1.5 text-xs font-medium capitalize transition-colors {activeTab === tab ? 'bg-indigo-600 text-white' : 'text-slate-600 hover:bg-slate-100 dark:text-slate-300 dark:hover:bg-slate-700'}"
						>
							{tab === 'healthcheck' ? 'Health Check' : tab}
						</button>
					{/each}
				</div>

				<div class="rounded-2xl border border-slate-200 bg-white p-6 shadow-sm dark:border-slate-700 dark:bg-slate-800">
					<!-- Overview Tab -->
					{#if activeTab === 'overview'}
						<div class="space-y-4">
							<div class="flex items-start justify-between">
								<h2 class="text-xl font-semibold text-slate-900 dark:text-white">{selectedLB.LoadBalancerName}</h2>
								<button
									type="button"
									onclick={() => deleteLoadBalancer(selectedLB!.LoadBalancerName ?? '')}
									class="rounded-lg border border-red-200 px-3 py-1 text-xs text-red-600 hover:bg-red-50 dark:border-red-800 dark:text-red-400"
								>
									Delete
								</button>
							</div>
							<dl class="grid grid-cols-2 gap-3 text-sm">
								<div>
									<dt class="text-slate-500 dark:text-slate-400">DNS Name</dt>
									<dd class="flex items-center gap-1.5">
										<span class="font-mono text-xs text-slate-700 dark:text-slate-300 break-all">{selectedLB.DNSName}</span>
										<button type="button" onclick={() => copyToClipboard(selectedLB?.DNSName ?? '')} class="flex-shrink-0 rounded px-1 py-0.5 text-xs text-slate-400 hover:bg-slate-100 hover:text-slate-700 dark:hover:bg-slate-700 dark:hover:text-slate-200" title="Copy DNS name">⧉</button>
									</dd>
								</div>
								<div>
									<dt class="text-slate-500 dark:text-slate-400">Scheme</dt>
									<dd>
										<span class="rounded-full px-2 py-0.5 text-xs font-medium {selectedLB.Scheme === 'internal' ? 'bg-orange-100 text-orange-700 dark:bg-orange-900/30 dark:text-orange-300' : 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300'}">{selectedLB.Scheme ?? 'internet-facing'}</span>
									</dd>
								</div>
								<div>
									<dt class="text-slate-500 dark:text-slate-400">VPC ID</dt>
									<dd class="text-slate-700 dark:text-slate-300">{selectedLB.VPCId ?? '—'}</dd>
								</div>
								<div>
									<dt class="text-slate-500 dark:text-slate-400">Created</dt>
									<dd class="text-slate-700 dark:text-slate-300">{selectedLB.CreatedTime ? new Date(selectedLB.CreatedTime).toLocaleString() : '—'}</dd>
								</div>
							</dl>

							<!-- Health Check summary -->
							{#if selectedLB.HealthCheck?.Target}
								<div>
									<h3 class="mb-1.5 text-sm font-semibold text-slate-700 dark:text-slate-300">Health Check</h3>
									<div class="rounded-lg bg-slate-50 px-3 py-2 text-xs dark:bg-slate-900 space-y-0.5">
										<div class="flex gap-2"><span class="text-slate-500 w-24 flex-shrink-0">Target</span><span class="font-mono text-slate-700 dark:text-slate-300">{selectedLB.HealthCheck.Target}</span></div>
										<div class="flex gap-2"><span class="text-slate-500 w-24 flex-shrink-0">Interval</span><span class="text-slate-700 dark:text-slate-300">{selectedLB.HealthCheck.Interval}s</span></div>
										<div class="flex gap-2"><span class="text-slate-500 w-24 flex-shrink-0">Threshold</span><span class="text-slate-700 dark:text-slate-300">{selectedLB.HealthCheck.HealthyThreshold} healthy / {selectedLB.HealthCheck.UnhealthyThreshold} unhealthy</span></div>
									</div>
								</div>
							{/if}

							<!-- Tags -->
							{#if ((selectedLB as unknown as { Tags?: { Key?: string; Value?: string }[] }).Tags ?? []).length > 0}
								{@const tags = selectedLB as unknown as { Tags?: { Key?: string; Value?: string }[] }}
								<div>
									<h3 class="mb-1.5 text-sm font-semibold text-slate-700 dark:text-slate-300">Tags</h3>
									<div class="flex flex-wrap gap-1.5">
										{#each tags.Tags ?? [] as tag}
											<span class="rounded-full border border-slate-200 bg-slate-50 px-2 py-0.5 text-xs dark:border-slate-700 dark:bg-slate-800">
												<span class="text-slate-500">{tag.Key}:</span> <span class="text-slate-700 dark:text-slate-300">{tag.Value}</span>
											</span>
										{/each}
									</div>
								</div>
							{/if}

							<!-- Availability Zones -->
							<div>
								<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-300">Availability Zones</h3>
								<div class="flex flex-wrap gap-2">
									{#each selectedLB.AvailabilityZones ?? [] as az}
										<span class="flex items-center gap-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700">
											{az}
											<button type="button" onclick={() => disableAZ(az)} class="text-red-500 hover:text-red-700">×</button>
										</span>
									{/each}
									<form class="flex gap-1" onsubmit={(e) => { e.preventDefault(); enableAZ(); }}>
										<input bind:value={newAZInput} placeholder="us-east-1c" class="rounded border border-slate-300 px-2 py-0.5 text-xs dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
										<button type="submit" class="rounded bg-indigo-600 px-2 py-0.5 text-xs text-white">+</button>
									</form>
								</div>
							</div>

							<!-- Subnets -->
							<div>
								<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-300">Subnets</h3>
								<div class="flex flex-wrap gap-2">
									{#each selectedLB.Subnets ?? [] as subnet}
										<span class="flex items-center gap-1 rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700">
											{subnet}
											<button type="button" onclick={() => detachSubnet(subnet)} class="text-red-500 hover:text-red-700">×</button>
										</span>
									{/each}
									<form class="flex gap-1" onsubmit={(e) => { e.preventDefault(); attachSubnet(); }}>
										<input bind:value={newSubnetInput} placeholder="subnet-xxxxxxxx" class="rounded border border-slate-300 px-2 py-0.5 text-xs dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
										<button type="submit" class="rounded bg-indigo-600 px-2 py-0.5 text-xs text-white">+</button>
									</form>
								</div>
							</div>

							<!-- Security Groups -->
							{#if (selectedLB.SecurityGroups ?? []).length > 0}
								<div>
									<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-300">Security Groups</h3>
									<div class="flex flex-wrap gap-2">
										{#each selectedLB.SecurityGroups ?? [] as sg}
											<span class="rounded-full bg-slate-100 px-2 py-0.5 text-xs dark:bg-slate-700">{sg}</span>
										{/each}
									</div>
								</div>
							{/if}
						</div>
					{/if}

					<!-- Listeners Tab -->
					{#if activeTab === 'listeners'}
						<div class="space-y-4">
							<div class="flex items-center justify-between">
								<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Listeners</h2>
								<div class="flex gap-2">
									<button type="button" onclick={() => (showSSLModal = true)} class="rounded-lg border px-3 py-1.5 text-xs hover:bg-slate-50 dark:hover:bg-slate-700">Set SSL Cert</button>
									<button type="button" onclick={() => (showListenerPoliciesModal = true)} class="rounded-lg border px-3 py-1.5 text-xs hover:bg-slate-50 dark:hover:bg-slate-700">Set Policies</button>
									<button type="button" onclick={() => (showAddListenerModal = true)} class="rounded-lg bg-indigo-600 px-3 py-1.5 text-xs text-white hover:bg-indigo-700">+ Add</button>
								</div>
							</div>
							{#if (selectedLB.ListenerDescriptions ?? []).length === 0}
								<p class="text-sm text-slate-500">No listeners configured.</p>
							{:else}
								<table class="w-full text-sm">
									<thead>
										<tr class="border-b border-slate-100 text-left text-xs font-medium uppercase text-slate-500 dark:border-slate-700">
											<th class="pb-2">Protocol</th>
											<th class="pb-2">LB Port</th>
											<th class="pb-2">Inst Port</th>
											<th class="pb-2">SSL Cert</th>
											<th class="pb-2">Policies</th>
											<th class="pb-2"></th>
										</tr>
									</thead>
									<tbody>
										{#each [...(selectedLB.ListenerDescriptions ?? [])].sort((a, b) => (a.Listener?.LoadBalancerPort ?? 0) - (b.Listener?.LoadBalancerPort ?? 0)) as ld}
											<tr class="border-b border-slate-50 dark:border-slate-800">
												<td class="py-2"><span class="rounded-full px-2 py-0.5 text-xs font-medium {protocolColor(ld.Listener?.Protocol)}">{ld.Listener?.Protocol}</span></td>
												<td class="py-2">{ld.Listener?.LoadBalancerPort}</td>
												<td class="py-2">{ld.Listener?.InstancePort}</td>
												<td class="py-2 max-w-xs truncate text-xs font-mono">{ld.Listener?.SSLCertificateId ?? '—'}</td>
												<td class="py-2 text-xs">{(ld.PolicyNames ?? []).join(', ') || '—'}</td>
												<td class="py-2">
													<button type="button" onclick={() => deleteListener(ld.Listener?.LoadBalancerPort ?? 0)} class="text-xs text-red-500 hover:text-red-700">Remove</button>
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							{/if}
						</div>
					{/if}

					<!-- Health Check Tab -->
					{#if activeTab === 'healthcheck'}
						<div class="space-y-4">
							<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Health Check</h2>
							<form onsubmit={(e) => { e.preventDefault(); saveHealthCheck(); }} class="space-y-3">
								<div>
									<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Target (e.g. HTTP:80/health)</label>
									<input bind:value={hcTarget} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
								</div>
								<div class="grid grid-cols-2 gap-3">
									<div>
										<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Interval (s)</label>
										<input type="number" bind:value={hcInterval} min="5" max="300" class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
									</div>
									<div>
										<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Timeout (s)</label>
										<input type="number" bind:value={hcTimeout} min="2" max="60" class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
									</div>
									<div>
										<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Healthy Threshold</label>
										<input type="number" bind:value={hcHealthy} min="2" max="10" class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
									</div>
									<div>
										<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Unhealthy Threshold</label>
										<input type="number" bind:value={hcUnhealthy} min="2" max="10" class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
									</div>
								</div>
								<div class="flex justify-end">
									<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Save</button>
								</div>
							</form>
						</div>
					{/if}

					<!-- Attributes Tab -->
					{#if activeTab === 'attributes'}
						<div class="space-y-4">
							<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Load Balancer Attributes</h2>
							<form onsubmit={(e) => { e.preventDefault(); saveAttributes(); }} class="space-y-4">
								<label class="flex items-center gap-3">
									<input type="checkbox" bind:checked={attrCrossZone} class="rounded" />
									<span class="text-sm font-medium text-slate-700 dark:text-slate-300">Cross-Zone Load Balancing</span>
								</label>
								<label class="flex items-center gap-3">
									<input type="checkbox" bind:checked={attrDraining} class="rounded" />
									<span class="text-sm font-medium text-slate-700 dark:text-slate-300">Connection Draining</span>
								</label>
								{#if attrDraining}
									<div class="ml-7">
										<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Draining Timeout (s)</label>
										<input type="number" bind:value={attrDrainingTimeout} min="1" max="3600" class="mt-1 w-32 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
									</div>
								{/if}
								<div>
									<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Idle Timeout (s)</label>
									<input type="number" bind:value={attrIdleTimeout} min="1" max="3600" class="mt-1 w-32 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
								</div>
								<div class="flex justify-end">
									<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Save</button>
								</div>
							</form>
						</div>
					{/if}

					<!-- Instances Tab -->
					{#if activeTab === 'instances'}
						<div class="space-y-4">
							<div class="flex items-center justify-between">
								<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Registered Instances</h2>
								<button type="button" onclick={() => refreshInstanceHealth()} disabled={healthLoading} class="rounded-lg border px-3 py-1.5 text-xs hover:bg-slate-50 dark:hover:bg-slate-700 disabled:opacity-50">
									{healthLoading ? 'Refreshing…' : '↺ Refresh Health'}
								</button>
							</div>
							<form class="flex gap-2" onsubmit={(e) => { e.preventDefault(); registerInstance(); }}>
								<input bind:value={newInstanceId} placeholder="i-0123456789abcdef0" class="flex-1 rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
								<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Register</button>
							</form>
							{#if (selectedLB.Instances ?? []).length === 0}
								<p class="text-sm text-slate-500">No instances registered.</p>
							{:else}
								<table class="w-full text-sm">
									<thead>
										<tr class="border-b border-slate-100 text-left text-xs font-medium uppercase text-slate-500 dark:border-slate-700">
											<th class="pb-2">Instance ID</th>
											<th class="pb-2">Health</th>
											<th class="pb-2"></th>
										</tr>
									</thead>
									<tbody>
										{#each selectedLB.Instances ?? [] as inst}
											{@const health = instanceHealthMap[inst.InstanceId ?? '']}
											<tr class="border-b border-slate-50 dark:border-slate-800">
												<td class="py-2 font-mono text-xs">{inst.InstanceId}</td>
												<td class="py-2 text-xs">
													{#if health}
														<span class="rounded-full px-2 py-0.5 font-medium {health === 'InService' ? 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-300' : 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-300'}">{health}</span>
													{:else}
														<span class="text-slate-400">—</span>
													{/if}
												</td>
												<td class="py-2 text-right">
													<button type="button" onclick={() => deregisterInstance(inst.InstanceId ?? '')} class="text-xs text-red-500 hover:text-red-700">Deregister</button>
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							{/if}
						</div>
					{/if}

					<!-- Policies Tab -->
					{#if activeTab === 'policies'}
						<div class="space-y-4">
							<div class="flex items-center justify-between">
								<h2 class="text-lg font-semibold text-slate-900 dark:text-white">Policies</h2>
								<button type="button" onclick={() => (showAddPolicyModal = true)} class="rounded-lg bg-indigo-600 px-3 py-1.5 text-xs text-white hover:bg-indigo-700">+ Create Policy</button>
							</div>
							{#if lbPolicies.length === 0}
								<p class="text-sm text-slate-500">No policies defined.</p>
							{:else}
								<table class="w-full text-sm">
									<thead>
										<tr class="border-b border-slate-100 text-left text-xs font-medium uppercase text-slate-500 dark:border-slate-700">
											<th class="pb-2">Name</th>
											<th class="pb-2">Type</th>
											<th class="pb-2"></th>
										</tr>
									</thead>
									<tbody>
										{#each [...lbPolicies].sort((a, b) => (a.PolicyName ?? '').localeCompare(b.PolicyName ?? '')) as pol}
											<tr class="border-b border-slate-50 dark:border-slate-800">
												<td class="py-2">{pol.PolicyName}</td>
												<td class="py-2"><span class="rounded-full px-2 py-0.5 text-xs font-medium {policyTypeColor(pol.PolicyTypeName)}">{pol.PolicyTypeName}</span></td>
												<td class="py-2 text-right">
													<button type="button" onclick={() => deletePolicy(pol.PolicyName ?? '')} class="text-xs text-red-500 hover:text-red-700">Delete</button>
												</td>
											</tr>
										{/each}
									</tbody>
								</table>
							{/if}

							<!-- Backend Server Descriptions -->
							{#if (selectedLB.BackendServerDescriptions ?? []).length > 0}
								<div class="mt-4">
									<h3 class="mb-2 text-sm font-semibold text-slate-700 dark:text-slate-300">Backend Server Policies</h3>
									<table class="w-full text-sm">
										<thead>
											<tr class="border-b border-slate-100 text-left text-xs font-medium uppercase text-slate-500 dark:border-slate-700">
												<th class="pb-2">Instance Port</th>
												<th class="pb-2">Policies</th>
												<th class="pb-2"></th>
											</tr>
										</thead>
										<tbody>
											{#each selectedLB.BackendServerDescriptions ?? [] as bsd}
												<tr class="border-b border-slate-50 dark:border-slate-800">
													<td class="py-2">{bsd.InstancePort}</td>
													<td class="py-2 text-xs">{(bsd.PolicyNames ?? []).join(', ') || '—'}</td>
													<td class="py-2">
														<button type="button" onclick={() => openBackendPoliciesModal(bsd.InstancePort ?? 0)} class="text-xs text-indigo-600 hover:underline">Edit</button>
													</td>
												</tr>
											{/each}
										</tbody>
									</table>
								</div>
							{/if}
						</div>
					{/if}
				</div>
			{:else}
				<div class="flex h-48 items-center justify-center rounded-2xl border border-dashed border-slate-200 dark:border-slate-700">
					<p class="text-sm text-slate-400">Select a load balancer to view details</p>
				</div>
			{/if}
		</div>
	</div>
</div>

<!-- Create Load Balancer Modal -->
{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Create Load Balancer</h2>
			<form onsubmit={(e) => { e.preventDefault(); createLoadBalancer(); }} class="space-y-3">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Name</label>
					<input id="name" bind:value={newName} required class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="my-lb" />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Availability Zone</label>
					<input bind:value={newAZ} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
				</div>
				<div class="grid grid-cols-2 gap-3">
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">LB Port</label>
						<input type="number" bind:value={newListenerPort} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Instance Port</label>
						<input type="number" bind:value={newInstancePort} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
					</div>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Scheme</label>
					<select bind:value={newScheme} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white">
						<option value="internet-facing">internet-facing</option>
						<option value="internal">internal</option>
					</select>
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => (showCreateModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-300">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Add Listener Modal -->
{#if showAddListenerModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Add Listener</h2>
			<form onsubmit={(e) => { e.preventDefault(); addListener(); }} class="space-y-3">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Protocol</label>
					<select bind:value={listenerProto} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white">
						<option>HTTP</option>
						<option>HTTPS</option>
						<option>TCP</option>
						<option>SSL</option>
					</select>
				</div>
				<div class="grid grid-cols-2 gap-3">
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">LB Port</label>
						<input type="number" bind:value={listenerLBPort} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
					</div>
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Instance Port</label>
						<input type="number" bind:value={listenerInstPort} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
					</div>
				</div>
				{#if listenerProto === 'HTTPS' || listenerProto === 'SSL'}
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">SSL Certificate ARN</label>
						<input bind:value={listenerCertARN} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="arn:aws:acm:…" />
					</div>
				{/if}
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => (showAddListenerModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-300">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Add</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- SSL Certificate Modal -->
{#if showSSLModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Set SSL Certificate</h2>
			<form onsubmit={(e) => { e.preventDefault(); setSSLCertificate(); }} class="space-y-3">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Listener Port</label>
					<input type="number" bind:value={sslPort} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">SSL Certificate ARN</label>
					<input bind:value={sslCertARN} required class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="arn:aws:acm:…" />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => (showSSLModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-300">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Set</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Listener Policies Modal -->
{#if showListenerPoliciesModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Set Listener Policies</h2>
			<form onsubmit={(e) => { e.preventDefault(); setListenerPolicies(); }} class="space-y-3">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Listener Port</label>
					<input type="number" bind:value={lpPort} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Policy Names (comma-separated)</label>
					<input bind:value={lpPolicyInput} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="pol-1, pol-2" />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => (showListenerPoliciesModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-300">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Set</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Add Policy Modal -->
{#if showAddPolicyModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Create Policy</h2>
			<form onsubmit={(e) => { e.preventDefault(); addPolicy(); }} class="space-y-3">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Type</label>
					<select bind:value={policyType} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white">
						<option value="lb-cookie">LB Cookie Stickiness</option>
						<option value="app-cookie">App Cookie Stickiness</option>
					</select>
				</div>
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Policy Name</label>
					<input bind:value={policyName} required class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="my-policy" />
				</div>
				{#if policyType === 'app-cookie'}
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Cookie Name</label>
						<input bind:value={policyCookieName} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="SESSIONID" />
					</div>
				{:else}
					<div>
						<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Cookie Expiry (s, 0 = browser session)</label>
						<input type="number" bind:value={policyExpiry} min="0" class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" />
					</div>
				{/if}
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => (showAddPolicyModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-300">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Create</button>
				</div>
			</form>
		</div>
	</div>
{/if}

<!-- Backend Server Policies Modal -->
{#if showBackendPoliciesModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
		<div class="w-full max-w-md rounded-2xl bg-white p-6 shadow-xl dark:bg-slate-800">
			<h2 class="mb-4 text-lg font-semibold text-slate-900 dark:text-white">Backend Server Policies (port {bsdPort})</h2>
			<form onsubmit={(e) => { e.preventDefault(); setBackendPolicies(bsdPort); }} class="space-y-3">
				<div>
					<label class="block text-sm font-medium text-slate-700 dark:text-slate-300">Policy Names (comma-separated)</label>
					<input bind:value={bsdPolicyInput} class="mt-1 w-full rounded-lg border border-slate-300 px-3 py-2 text-sm dark:border-slate-600 dark:bg-slate-900 dark:text-white" placeholder="pol-1, pol-2" />
				</div>
				<div class="flex justify-end gap-3 pt-2">
					<button type="button" onclick={() => (showBackendPoliciesModal = false)} class="px-4 py-2 text-sm text-slate-600 dark:text-slate-300">Cancel</button>
					<button type="submit" class="rounded-lg bg-indigo-600 px-4 py-2 text-sm font-medium text-white hover:bg-indigo-700">Save</button>
				</div>
			</form>
		</div>
	</div>
{/if}
