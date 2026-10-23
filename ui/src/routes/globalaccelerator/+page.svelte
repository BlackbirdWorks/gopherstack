<script lang="ts">
	import { confirmDestructive } from '#lib/confirm-dialog.ts';
	import { onRegionChange, regionalClient } from '#lib/region-effect.svelte.ts';
	import { getGlobalAcceleratorClient } from '#lib/aws-client.ts';
	import {
		ListAcceleratorsCommand,
		ListListenersCommand,
		ListEndpointGroupsCommand,
		UpdateAcceleratorCommand,
		DeleteAcceleratorCommand,
		CreateAcceleratorCommand,
		type Accelerator,
		type Listener,
		type EndpointGroup
	} from '@aws-sdk/client-global-accelerator';
	import { toast } from 'svelte-sonner';
	import { Zap, Search, RefreshCw, Plus, Trash2, Power, Globe, Server } from 'lucide-svelte';

	type ListenerView = { listener: Listener; groups: EndpointGroup[] };

	const ga = regionalClient(getGlobalAcceleratorClient);

	let loading = $state(false);
	let searchQuery = $state('');
	let accelerators = $state<Accelerator[]>([]);
	let selected = $state<Accelerator | null>(null);
	let listeners = $state<ListenerView[]>([]);
	let loadingDetails = $state(false);

	let showCreateModal = $state(false);
	let accelName = $state('');
	let ipType = $state<'IPV4' | 'DUAL_STACK'>('IPV4');
	let creating = $state(false);

	const filtered = $derived(
		accelerators.filter(
			(a) =>
				a.Name?.toLowerCase().includes(searchQuery.toLowerCase()) ||
				a.AcceleratorArn?.toLowerCase().includes(searchQuery.toLowerCase())
		)
	);

	function message(err: unknown): string {
		return (err as Error).message;
	}

	function ipsOf(a: Accelerator, family: string): string[] {
		return (a.IpSets ?? []).filter((s) => s.IpAddressFamily === family).flatMap((s) => s.IpAddresses ?? []);
	}

	function portsOf(l: Listener): string {
		return (l.PortRanges ?? []).map((r) => (r.FromPort === r.ToPort ? `${r.FromPort}` : `${r.FromPort}-${r.ToPort}`)).join(', ');
	}

	function statusColor(status: string | undefined): string {
		if (status === 'DEPLOYED') return 'bg-emerald-500';
		if (status === 'IN_PROGRESS') return 'bg-amber-500';
		return 'bg-slate-400';
	}

	function healthColor(state: string | undefined): string {
		if (state === 'HEALTHY') return 'text-emerald-600';
		if (state === 'INITIAL') return 'text-amber-600';
		return 'text-rose-600';
	}

	async function loadAccelerators() {
		loading = true;
		try {
			const res = await ga().send(new ListAcceleratorsCommand({}));
			accelerators = res.Accelerators ?? [];
			if (selected) {
				selected = accelerators.find((a) => a.AcceleratorArn === selected?.AcceleratorArn) ?? null;
			}
		} catch (err: unknown) {
			toast.error(`Failed to load Global Accelerator: ${message(err)}`);
		} finally {
			loading = false;
		}
	}

	async function loadListeners(arn: string | undefined) {
		const res = await ga().send(new ListListenersCommand({ AcceleratorArn: arn }));
		const found = res.Listeners ?? [];
		const groups = await Promise.all(
			found.map((l) => ga().send(new ListEndpointGroupsCommand({ ListenerArn: l.ListenerArn })))
		);
		listeners = found.map((listener, i) => ({ listener, groups: groups[i]?.EndpointGroups ?? [] }));
	}

	async function selectAccelerator(accel: Accelerator) {
		selected = accel;
		loadingDetails = true;
		try {
			await loadListeners(accel.AcceleratorArn);
		} catch (err: unknown) {
			listeners = [];
			toast.error(`Failed to load details: ${message(err)}`);
		} finally {
			loadingDetails = false;
		}
	}

	async function toggleEnabled(accel: Accelerator) {
		try {
			await ga().send(
				new UpdateAcceleratorCommand({ AcceleratorArn: accel.AcceleratorArn, Enabled: !accel.Enabled })
			);
			toast.success(`Accelerator ${accel.Enabled ? 'disabled' : 'enabled'}`);
			await loadAccelerators();
		} catch (err: unknown) {
			toast.error(`Update failed: ${message(err)}`);
		}
	}

	async function deleteAccelerator(accel: Accelerator) {
		const ok = await confirmDestructive({
			title: 'Delete Global Accelerator',
			message: `Delete ${accel.Name}? It is disabled first; its listeners must already be removed.`
		});
		if (!ok) return;
		try {
			if (accel.Enabled) {
				await ga().send(new UpdateAcceleratorCommand({ AcceleratorArn: accel.AcceleratorArn, Enabled: false }));
			}
			await ga().send(new DeleteAcceleratorCommand({ AcceleratorArn: accel.AcceleratorArn }));
			toast.success('Accelerator deleted');
			selected = null;
			listeners = [];
			await loadAccelerators();
		} catch (err: unknown) {
			toast.error(`Delete failed: ${message(err)}`);
			await loadAccelerators();
		}
	}

	async function createAccelerator() {
		if (!accelName.trim()) return;
		creating = true;
		try {
			await ga().send(new CreateAcceleratorCommand({ Name: accelName.trim(), Enabled: true, IpAddressType: ipType }));
			toast.success(`Accelerator "${accelName}" created`);
			showCreateModal = false;
			accelName = '';
			await loadAccelerators();
		} catch (err: unknown) {
			toast.error(`Creation failed: ${message(err)}`);
		} finally {
			creating = false;
		}
	}

	onRegionChange(loadAccelerators);
</script>

<div class="space-y-6">
	<div class="flex flex-col md:flex-row md:items-center justify-between gap-4 p-6 bg-white/40 dark:bg-slate-800/40 backdrop-blur-xl border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl">
		<div class="flex items-center gap-4">
			<div class="p-3 bg-rose-600/20 rounded-xl border border-rose-500/20">
				<Zap class="w-8 h-8 text-rose-600" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white tracking-tight">Global Accelerator Network</h1>
				<p class="text-slate-500 dark:text-slate-400 text-sm mt-1">Accelerators with their static IP addresses, listeners and endpoint groups.</p>
			</div>
		</div>
		<div class="flex items-center gap-3">
			<button
				onclick={loadAccelerators}
				class="p-2.5 rounded-xl bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 shadow-sm"
				title="Refresh"
				aria-label="Refresh"
			>
				<RefreshCw class="w-5 h-5 text-slate-600 dark:text-slate-300 {loading ? 'animate-spin' : ''}" />
			</button>
			<button
				onclick={() => (showCreateModal = true)}
				class="flex items-center gap-2 px-5 py-2.5 bg-rose-600 hover:bg-rose-700 text-white rounded-xl font-bold shadow-lg"
			>
				<Plus class="w-5 h-5" />
				Deploy Global IP
			</button>
		</div>
	</div>

	<div class="grid grid-cols-1 lg:grid-cols-12 gap-6 items-start">
		<div class="lg:col-span-4 bg-white/40 dark:bg-slate-800/40 border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl overflow-hidden">
			<div class="p-4 border-b border-slate-200 dark:border-slate-700/50">
				<div class="relative w-full">
					<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
					<input
						type="text"
						bind:value={searchQuery}
						placeholder="Search ip groups..."
						class="w-full pl-10 pr-4 py-2 bg-white/50 dark:bg-slate-700/50 border border-slate-200 dark:border-slate-600 rounded-xl text-sm outline-none focus:ring-2 focus:ring-rose-500"
					/>
				</div>
			</div>
			<div class="divide-y divide-slate-100 dark:divide-slate-700/50 max-h-[600px] overflow-y-auto">
				{#each filtered as a (a.AcceleratorArn)}
					<div
						role="button"
						tabindex="0"
						onclick={() => selectAccelerator(a)}
						onkeydown={(e) => e.key === 'Enter' && selectAccelerator(a)}
						class="p-4 flex items-center gap-3 cursor-pointer hover:bg-rose-500/5 border-l-4 {selected?.AcceleratorArn === a.AcceleratorArn ? 'bg-rose-500/10 border-rose-500' : 'border-transparent'}"
					>
						<div class="w-2 h-2 rounded-full {statusColor(a.Status)}"></div>
						<div class="min-w-0">
							<div class="font-semibold text-slate-900 dark:text-white truncate">{a.Name}</div>
							<div class="text-xs text-slate-400">{a.IpAddressType}{a.Enabled ? '' : ' (disabled)'}</div>
						</div>
					</div>
				{/each}
				{#if !loading && !accelerators.length}
					<div class="p-12 text-center text-slate-400 text-sm">No global anycast IP sets detected.</div>
				{/if}
			</div>
		</div>

		<div class="lg:col-span-8 space-y-6">
			{#if selected}
				<div class="bg-white/40 dark:bg-slate-800/40 border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-5">
					<div class="flex justify-between items-start gap-4">
						<div>
							<h2 class="text-2xl font-bold text-slate-900 dark:text-white">{selected.Name}</h2>
							<div class="flex items-center gap-2 mt-2 text-xs">
								<span class="px-2 py-1 rounded-lg {statusColor(selected.Status)} text-white font-semibold">{selected.Status}</span>
								<span class="px-2 py-1 rounded-lg bg-slate-200 dark:bg-slate-700">{selected.Enabled ? 'Enabled' : 'Disabled'}</span>
								<span class="px-2 py-1 rounded-lg bg-slate-200 dark:bg-slate-700">{selected.IpAddressType}</span>
							</div>
						</div>
						<div class="flex gap-2">
							<button onclick={() => selected && toggleEnabled(selected)} class="p-2.5 rounded-xl border border-slate-300 dark:border-slate-600" title={selected.Enabled ? 'Disable' : 'Enable'} aria-label="Toggle enabled">
								<Power class="w-4 h-4" />
							</button>
							<button onclick={() => selected && deleteAccelerator(selected)} class="p-2.5 rounded-xl border border-rose-500/40 text-rose-600" title="Delete" aria-label="Delete accelerator">
								<Trash2 class="w-4 h-4" />
							</button>
						</div>
					</div>

					<dl class="grid grid-cols-1 md:grid-cols-2 gap-4 text-sm">
						<div>
							<dt class="text-xs uppercase text-slate-400">IPv4 addresses</dt>
							<dd class="font-mono">{ipsOf(selected, 'IPv4').join(', ') || '-'}</dd>
						</div>
						<div>
							<dt class="text-xs uppercase text-slate-400">IPv6 addresses</dt>
							<dd class="font-mono">{ipsOf(selected, 'IPv6').join(', ') || '-'}</dd>
						</div>
						<div class="md:col-span-2">
							<dt class="text-xs uppercase text-slate-400">DNS name</dt>
							<dd class="font-mono break-all select-all">{selected.DnsName}</dd>
						</div>
						{#if selected.DualStackDnsName}
							<div class="md:col-span-2">
								<dt class="text-xs uppercase text-slate-400">Dual-stack DNS name</dt>
								<dd class="font-mono break-all select-all">{selected.DualStackDnsName}</dd>
							</div>
						{/if}
					</dl>
				</div>

				<div class="bg-white/40 dark:bg-slate-800/40 border border-white/20 dark:border-slate-700/50 rounded-2xl shadow-xl p-6 space-y-4">
					<h3 class="font-semibold text-slate-700 dark:text-slate-200">Listeners ({listeners.length})</h3>
					{#if loadingDetails}
						<div class="text-sm text-slate-400">Loading...</div>
					{/if}
					{#each listeners as view (view.listener.ListenerArn)}
						<div class="p-4 rounded-xl bg-slate-50 dark:bg-slate-900/50 border border-slate-100 dark:border-slate-700/50 space-y-3">
							<div class="text-sm font-semibold">
								{view.listener.Protocol} ports {portsOf(view.listener)}
								<span class="ml-2 text-xs font-normal text-slate-400">affinity {view.listener.ClientAffinity}</span>
							</div>
							{#each view.groups as eg (eg.EndpointGroupArn)}
								<div class="p-3 rounded-lg bg-white/60 dark:bg-slate-800/60 border border-slate-100 dark:border-slate-700/50 space-y-2">
									<div class="flex items-center gap-2 text-sm font-medium">
										<Globe class="w-4 h-4 text-indigo-500" />
										{eg.EndpointGroupRegion}
										<span class="text-xs font-normal text-slate-400">
											traffic dial {eg.TrafficDialPercentage}% - health {eg.HealthCheckProtocol}:{eg.HealthCheckPort}{eg.HealthCheckPath ?? ''}
										</span>
									</div>
									{#each eg.EndpointDescriptions ?? [] as ep (ep.EndpointId)}
										<div class="flex items-center justify-between text-xs pl-6">
											<span class="flex items-center gap-2 font-mono truncate"><Server class="w-3 h-3" />{ep.EndpointId}</span>
											<span>
												weight {ep.Weight}
												<span class="ml-2 font-semibold {healthColor(ep.HealthState)}">{ep.HealthState}</span>
											</span>
										</div>
									{:else}
										<div class="text-xs text-slate-400 pl-6">No endpoints.</div>
									{/each}
								</div>
							{:else}
								<div class="text-xs text-slate-400">No endpoint groups.</div>
							{/each}
						</div>
					{:else}
						{#if !loadingDetails}
							<div class="text-sm text-slate-400">No listeners.</div>
						{/if}
					{/each}
				</div>
			{:else}
				<div class="border-2 border-dashed border-slate-200 dark:border-slate-700/50 rounded-2xl p-16 text-center text-slate-500">
					Select an accelerator to see its listeners and endpoint groups.
				</div>
			{/if}
		</div>
	</div>
</div>

{#if showCreateModal}
	<div class="fixed inset-0 z-50 flex items-center justify-center p-4">
		<div class="absolute inset-0 bg-slate-900/60" onclick={() => (showCreateModal = false)} onkeydown={(e) => { if (e.key === 'Escape') showCreateModal = false; }} role="presentation"></div>
		<div class="relative w-full max-w-md bg-white dark:bg-slate-800 rounded-2xl shadow-2xl p-6">
			<h3 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Provision Anycast Set</h3>
			<form onsubmit={(e) => { e.preventDefault(); createAccelerator(); }} class="space-y-4">
				<div>
					<label for="aName" class="block text-xs font-semibold text-slate-500 uppercase mb-1">Network Identifier</label>
					<input id="aName" type="text" bind:value={accelName} placeholder="e.g. global-entry-prd" class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl outline-none focus:ring-2 focus:ring-rose-500 font-mono text-sm" required />
				</div>
				<div>
					<label for="aType" class="block text-xs font-semibold text-slate-500 uppercase mb-1">IP address type</label>
					<select id="aType" bind:value={ipType} class="w-full px-4 py-3 bg-slate-50 dark:bg-slate-900 border border-slate-200 dark:border-slate-700 rounded-xl text-sm">
						<option value="IPV4">IPV4</option>
						<option value="DUAL_STACK">DUAL_STACK</option>
					</select>
				</div>
				<div class="flex gap-3 pt-2">
					<button type="button" onclick={() => (showCreateModal = false)} class="flex-1 px-4 py-3 bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300 rounded-xl font-semibold">Abort</button>
					<button type="submit" disabled={creating} class="flex-1 px-4 py-3 bg-rose-600 text-white rounded-xl font-semibold disabled:opacity-50">
						{creating ? 'Syncing...' : 'Provision Global IP'}
					</button>
				</div>
			</form>
		</div>
	</div>
{/if}
