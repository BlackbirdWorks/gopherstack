<script lang="ts">
	import { GCP_GHOST, GCP_INPUT, GCP_PRIMARY, attempt } from '#lib/gcp-action.ts';
	import {
		createInstance,
		imageFamilies,
		listImages,
		listMachineTypes,
		listNetworks,
		listSubnetworks,
		regionOfZone,
		validResourceName,
		type ImageFamily,
		type MachineType,
		type Network,
		type Subnetwork
	} from '#lib/gcpcompute.ts';
	import { untrack } from 'svelte';

	type Props = { project: string; zone: string; zones: string[]; onCreated: () => void; onClose: () => void };
	let { project, zone, zones, onCreated, onClose }: Props = $props();

	let formZone = $state(untrack(() => zone || zones[0] || ''));
	let name = $state('');
	let machineType = $state('');
	let family = $state('');
	let diskSize = $state('');
	let network = $state('');
	let subnetwork = $state('');
	let externalIp = $state(false);
	let machineTypes = $state<MachineType[]>([]);
	let families = $state<ImageFamily[]>([]);
	let networks = $state<Network[]>([]);
	let subnets = $state<Subnetwork[]>([]);
	let loadError = $state('');

	const region = $derived(regionOfZone(formZone));
	const subnetChoices = $derived(subnets.filter((s) => s.region === region && (!network || s.network === network)));
	const nameOk = $derived(validResourceName(name));
	const formOk = $derived(nameOk && Boolean(formZone) && Boolean(machineType) && Boolean(family) && (!diskSize || /^\d+$/.test(diskSize)));

	$effect(() => {
		const p = project;
		untrack(() => {
			Promise.all([listImages(p), listNetworks(p), listSubnetworks(p)])
				.then(([imgs, nets, subs]) => {
					families = imageFamilies(imgs);
					family ||= families.find((f) => f.family === 'debian-12')?.source ?? families[0]?.source ?? '';
					networks = nets;
					subnets = subs;
				})
				.catch((e) => (loadError = String(e)));
		});
	});

	$effect(() => {
		const p = project;
		const z = formZone;
		if (!z) return;
		untrack(() => {
			listMachineTypes(p, z)
				.then((m) => {
					machineTypes = m;
					if (!m.some((x) => x.name === machineType)) machineType = m.find((x) => x.name === 'e2-medium')?.name ?? m[0]?.name ?? '';
				})
				.catch((e) => (loadError = String(e)));
		});
	});

	async function submit() {
		if (!formOk) return;
		const ok = await attempt(
			() => createInstance(project, formZone, { name, machineType, sourceImage: family, diskSizeGb: diskSize, network, subnetwork, externalIp }),
			`Creating instance ${name}`,
			`Failed to create ${name}`
		);
		if (ok) onCreated();
	}
</script>

<form class="space-y-3 rounded-lg border border-gray-200 p-4 text-sm dark:border-gray-700" onsubmit={(e) => { e.preventDefault(); void submit(); }} data-testid="compute-instance-form">
	<div class="grid gap-3 sm:grid-cols-3">
		<label>Name<input class="{GCP_INPUT} font-mono" bind:value={name} placeholder="my-vm" data-testid="compute-new-name" />
			{#if name && !nameOk}<span class="text-xs text-red-600">Lowercase letters, digits and dashes, starting with a letter.</span>{/if}
		</label>
		<label>Zone
			<select class="{GCP_INPUT} font-mono" bind:value={formZone} data-testid="compute-new-zone">
				{#each zones as z (z)}<option value={z}>{z}</option>{/each}
			</select>
		</label>
		<label>Machine type
			<select class="{GCP_INPUT} font-mono" bind:value={machineType} data-testid="compute-new-machine">
				{#each machineTypes as m (m.name)}<option value={m.name}>{m.name} ({m.vcpus} vCPU, {Math.round(m.memoryMb / 1024)} GB)</option>{/each}
			</select>
		</label>
		<label>Image family
			<select class="{GCP_INPUT} font-mono" bind:value={family} data-testid="compute-new-image">
				{#each families as f (f.source)}<option value={f.source}>{f.label}</option>{/each}
			</select>
		</label>
		<label>Boot disk size (GB, optional)<input class={GCP_INPUT} inputmode="numeric" bind:value={diskSize} placeholder="10" data-testid="compute-new-disk" /></label>
		<label>Network
			<select class="{GCP_INPUT} font-mono" bind:value={network} onchange={() => (subnetwork = '')} data-testid="compute-new-network">
				<option value="">default</option>
				{#each networks as n (n.name)}<option value={n.name}>{n.name}</option>{/each}
			</select>
		</label>
		<label>Subnetwork
			<select class="{GCP_INPUT} font-mono" bind:value={subnetwork} data-testid="compute-new-subnet">
				<option value="">(auto)</option>
				{#each subnetChoices as s (`${s.region}/${s.name}`)}<option value={`regions/${s.region}/subnetworks/${s.name}`}>{s.name} ({s.cidr})</option>{/each}
			</select>
		</label>
		<label class="flex items-center gap-2 self-end pb-2"><input type="checkbox" bind:checked={externalIp} data-testid="compute-new-external" /> Ephemeral external IP</label>
	</div>
	{#if loadError}<p class="text-xs text-red-600" role="alert">{loadError}</p>{/if}
	<div class="flex gap-2">
		<button type="submit" class={GCP_PRIMARY} disabled={!formOk} data-testid="compute-new-submit">Create</button>
		<button type="button" class={GCP_GHOST} onclick={onClose}>Close</button>
	</div>
</form>
