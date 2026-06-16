<script lang="ts">
	import { confirmDestructive } from '$lib/confirm-dialog';
import { onMount } from 'svelte';
import { getEC2Client } from '$lib/aws-client';
import {
	DescribeInstancesCommand,
	StartInstancesCommand,
	StopInstancesCommand,
	TerminateInstancesCommand,
	RebootInstancesCommand,
	DescribeSecurityGroupsCommand,
	DescribeKeyPairsCommand,
	DescribeImagesCommand,
	DescribeLaunchTemplatesCommand,
	DescribeVpcEndpointsCommand,
	DescribeNetworkAclsCommand,
	DescribeVpcsCommand,
	DescribeSubnetsCommand,
	DescribeVolumesCommand,
	DescribeSnapshotsCommand,
	DescribeAddressesCommand,
	DescribeInternetGatewaysCommand,
	DescribeRouteTablesCommand,
	DescribeNatGatewaysCommand,
	DeleteLaunchTemplateCommand,
	DeleteVpcEndpointsCommand,
	DeleteSnapshotCommand,
	CreateLaunchTemplateCommand,
	RunInstancesCommand,
	type SecurityGroup,
	type KeyPairInfo,
	type Image,
	type LaunchTemplate,
	type VpcEndpoint,
	type NetworkAcl,
	type Vpc,
	type Subnet,
	type Volume,
	type Snapshot,
	type Address,
	type InternetGateway,
	type RouteTable,
	type NatGateway,
	_InstanceType
} from '@aws-sdk/client-ec2';
import { toast } from 'svelte-sonner';
import { Cpu, Play, Square, Trash2, RefreshCw, Plus, Search, RotateCcw, Shield, Key, Layers, Route, FileImage, Network, HardDrive, Camera } from 'lucide-svelte';

const ec2 = getEC2Client();

type EC2Instance = {
	ImageId?: string;
	InstanceId?: string;
	InstanceType?: string;
	LaunchTime?: Date;
	PrivateIpAddress?: string;
	PublicIpAddress?: string;
	State?: { Name?: string };
	SubnetId?: string;
	Tags?: Array<{ Key?: string; Value?: string }>;
	VpcId?: string;
};

type TabName = 'instances' | 'secgroups' | 'keypairs' | 'amis' | 'launchtemplates' | 'vpcendpoints' | 'nacls' | 'vpcs' | 'volumes' | 'snapshots' | 'eips' | 'igws' | 'routetables' | 'natgateways';

let instances = $state<EC2Instance[]>([]);
let loading = $state(true);
let search = $state('');
let stateFilter = $state('all');
let vpcFilter = $state('all');
let showLaunchModal = $state(false);
let showCreateLTModal = $state(false);
let selectedInstance = $state<EC2Instance | null>(null);
let newInstanceType = $state('t3.micro');
let newInstanceAmi = $state('ami-0c55b159cbfafe1f0');
let newInstanceName = $state('');
let newLTName = $state('');
let newLTImageId = $state('ami-0c55b159cbfafe1f0');
let newLTInstanceType = $state('t3.micro');
let activeTab = $state<TabName>('instances');
let securityGroups = $state<SecurityGroup[]>([]);
let keyPairs = $state<KeyPairInfo[]>([]);
let amis = $state<Image[]>([]);
let launchTemplates = $state<LaunchTemplate[]>([]);
let vpcEndpoints = $state<VpcEndpoint[]>([]);
let networkAcls = $state<NetworkAcl[]>([]);
let vpcs = $state<Vpc[]>([]);
let subnets = $state<Subnet[]>([]);
let volumes = $state<Volume[]>([]);
let snapshots = $state<Snapshot[]>([]);
let elasticIPs = $state<Address[]>([]);
let internetGateways = $state<InternetGateway[]>([]);
let routeTables = $state<RouteTable[]>([]);
let natGateways = $state<NatGateway[]>([]);
let sgSearch = $state('');
let kpSearch = $state('');
let amiSearch = $state('');
let ltSearch = $state('');
let vpceSearch = $state('');
let naclSearch = $state('');
let vpcSearch = $state('');
let volSearch = $state('');
let snapSearch = $state('');
let eipSearch = $state('');
let igwSearch = $state('');
let rtSearch = $state('');
let natSearch = $state('');

const instanceTypes = ['t3.micro', 't3.small', 't3.medium', 't3.large', 'm5.large', 'c5.large', 'r5.large'];

onMount(async () => {
	await loadInstances();
});

async function loadInstances() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeInstancesCommand({}));
		instances = data.Reservations?.flatMap((r) => (r.Instances ?? []) as EC2Instance[]) ?? [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load instances');
	} finally {
		loading = false;
	}
}

async function loadSecurityGroups() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeSecurityGroupsCommand({}));
		securityGroups = data.SecurityGroups || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load security groups');
	} finally {
		loading = false;
	}
}

async function loadKeyPairs() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeKeyPairsCommand({}));
		keyPairs = data.KeyPairs || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load key pairs');
	} finally {
		loading = false;
	}
}

async function loadAmis() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeImagesCommand({}));
		amis = data.Images || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load AMIs');
	} finally {
		loading = false;
	}
}

async function loadLaunchTemplates() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeLaunchTemplatesCommand({}));
		launchTemplates = data.LaunchTemplates || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load launch templates');
	} finally {
		loading = false;
	}
}

async function loadVpcEndpoints() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeVpcEndpointsCommand({}));
		vpcEndpoints = data.VpcEndpoints || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load VPC endpoints');
	} finally {
		loading = false;
	}
}

async function loadNetworkAcls() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeNetworkAclsCommand({}));
		networkAcls = data.NetworkAcls || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load network ACLs');
	} finally {
		loading = false;
	}
}

async function loadVpcs() {
	try {
		loading = true;
		const [vpcData, subnetData] = await Promise.all([
			ec2.send(new DescribeVpcsCommand({})),
			ec2.send(new DescribeSubnetsCommand({}))
		]);
		vpcs = vpcData.Vpcs || [];
		subnets = subnetData.Subnets || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load VPCs');
	} finally {
		loading = false;
	}
}

async function loadVolumes() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeVolumesCommand({}));
		volumes = data.Volumes || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load volumes');
	} finally {
		loading = false;
	}
}

async function loadSnapshots() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeSnapshotsCommand({}));
		snapshots = data.Snapshots || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load snapshots');
	} finally {
		loading = false;
	}
}

async function loadElasticIPs() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeAddressesCommand({}));
		elasticIPs = data.Addresses || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load Elastic IPs');
	} finally {
		loading = false;
	}
}

async function loadInternetGateways() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeInternetGatewaysCommand({}));
		internetGateways = data.InternetGateways || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load internet gateways');
	} finally {
		loading = false;
	}
}

async function loadRouteTables() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeRouteTablesCommand({}));
		routeTables = data.RouteTables || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load route tables');
	} finally {
		loading = false;
	}
}

async function loadNatGateways() {
	try {
		loading = true;
		const data = await ec2.send(new DescribeNatGatewaysCommand({}));
		natGateways = data.NatGateways || [];
	} catch (e) {
		toast.error(e instanceof Error ? e.message : 'Failed to load NAT gateways');
	} finally {
		loading = false;
	}
}

async function selectTab(t: TabName) {
	activeTab = t;
	if (t === 'instances' && instances.length === 0) await loadInstances();
	else if (t === 'secgroups' && securityGroups.length === 0) await loadSecurityGroups();
	else if (t === 'keypairs' && keyPairs.length === 0) await loadKeyPairs();
	else if (t === 'amis' && amis.length === 0) await loadAmis();
	else if (t === 'launchtemplates' && launchTemplates.length === 0) await loadLaunchTemplates();
	else if (t === 'vpcendpoints' && vpcEndpoints.length === 0) await loadVpcEndpoints();
	else if (t === 'nacls' && networkAcls.length === 0) await loadNetworkAcls();
	else if (t === 'vpcs' && vpcs.length === 0) await loadVpcs();
	else if (t === 'volumes' && volumes.length === 0) await loadVolumes();
	else if (t === 'snapshots' && snapshots.length === 0) await loadSnapshots();
	else if (t === 'eips' && elasticIPs.length === 0) await loadElasticIPs();
	else if (t === 'igws' && internetGateways.length === 0) await loadInternetGateways();
	else if (t === 'routetables' && routeTables.length === 0) await loadRouteTables();
	else if (t === 'natgateways' && natGateways.length === 0) await loadNatGateways();
}

async function refresh() {
	if (activeTab === 'instances') { instances = []; await loadInstances(); }
	else if (activeTab === 'secgroups') { securityGroups = []; await loadSecurityGroups(); }
	else if (activeTab === 'keypairs') { keyPairs = []; await loadKeyPairs(); }
	else if (activeTab === 'amis') { amis = []; await loadAmis(); }
	else if (activeTab === 'launchtemplates') { launchTemplates = []; await loadLaunchTemplates(); }
	else if (activeTab === 'vpcendpoints') { vpcEndpoints = []; await loadVpcEndpoints(); }
	else if (activeTab === 'nacls') { networkAcls = []; await loadNetworkAcls(); }
	else if (activeTab === 'vpcs') { vpcs = []; subnets = []; await loadVpcs(); }
	else if (activeTab === 'volumes') { volumes = []; await loadVolumes(); }
	else if (activeTab === 'eips') { elasticIPs = []; await loadElasticIPs(); }
	else if (activeTab === 'igws') { internetGateways = []; await loadInternetGateways(); }
	else if (activeTab === 'routetables') { routeTables = []; await loadRouteTables(); }
	else if (activeTab === 'natgateways') { natGateways = []; await loadNatGateways(); }
	else { snapshots = []; await loadSnapshots(); }
}

function getName(instance: EC2Instance): string {
	return instance.Tags?.find((t) => t.Key === 'Name')?.Value || instance.InstanceId || '';
}

function getStatusColor(state: string) {
const map: Record<string, string> = {
running: 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300',
stopped: 'bg-slate-100 dark:bg-slate-700 text-slate-700 dark:text-slate-300',
stopping: 'bg-yellow-100 dark:bg-yellow-900/30 text-yellow-700 dark:text-yellow-300',
pending: 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300',
terminated: 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-300'
};
return map[state] || 'bg-gray-100 dark:bg-gray-700 text-gray-700 dark:text-gray-300';
}

async function startInstance(id: string) {
try {
await ec2.send(new StartInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} starting...`);
await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to start');
}
}

async function stopInstance(id: string) {
try {
await ec2.send(new StopInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} stopping...`);
await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to stop');
}
}

async function rebootInstance(id: string) {
try {
await ec2.send(new RebootInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} rebooting...`);
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to reboot');
}
}

async function terminateInstance(id: string) {
if (!await confirmDestructive({ title: 'Terminate Instance', message: `Terminate instance ${id}? This cannot be undone.`, confirmLabel: 'Terminate' })) return;
try {
await ec2.send(new TerminateInstancesCommand({ InstanceIds: [id] }));
toast.success(`Instance ${id} terminated`);
await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to terminate');
}
}

async function deleteLaunchTemplate(id: string) {
if (!await confirmDestructive({ title: 'Delete Launch Template', message: `Delete launch template ${id}?`, confirmLabel: 'Delete' })) return;
try {
await ec2.send(new DeleteLaunchTemplateCommand({ LaunchTemplateId: id }));
toast.success('Launch template deleted');
launchTemplates = []; await loadLaunchTemplates();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete launch template');
}
}

async function deleteVpcEndpoint(id: string) {
if (!await confirmDestructive({ title: 'Delete VPC Endpoint', message: `Delete endpoint ${id}?`, confirmLabel: 'Delete' })) return;
try {
await ec2.send(new DeleteVpcEndpointsCommand({ VpcEndpointIds: [id] }));
toast.success('VPC endpoint deleted');
vpcEndpoints = []; await loadVpcEndpoints();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete endpoint');
}
}

async function deleteSnapshot(id: string) {
if (!await confirmDestructive({ title: 'Delete Snapshot', message: `Delete snapshot ${id}?`, confirmLabel: 'Delete' })) return;
try {
await ec2.send(new DeleteSnapshotCommand({ SnapshotId: id }));
toast.success('Snapshot deleted');
snapshots = []; await loadSnapshots();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to delete snapshot');
}
}

async function createLaunchTemplate() {
if (!newLTName) { toast.error('Name is required'); return; }
try {
await ec2.send(new CreateLaunchTemplateCommand({
  LaunchTemplateName: newLTName,
  LaunchTemplateData: {
    ImageId: newLTImageId,
    InstanceType: newLTInstanceType as _InstanceType
  }
}));
toast.success('Launch template created');
showCreateLTModal = false;
newLTName = ''; newLTImageId = 'ami-0c55b159cbfafe1f0'; newLTInstanceType = 't3.micro';
launchTemplates = []; await loadLaunchTemplates();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to create launch template');
}
}

async function launchInstance() {
try {
await ec2.send(new RunInstancesCommand({
  ImageId: newInstanceAmi,
  InstanceType: newInstanceType as _InstanceType,
  MinCount: 1,
  MaxCount: 1,
  TagSpecifications: newInstanceName ? [{
    ResourceType: 'instance',
    Tags: [{ Key: 'Name', Value: newInstanceName }]
  }] : undefined
}));
toast.success('Instance launched');
showLaunchModal = false;
newInstanceName = '';
instances = []; await loadInstances();
} catch (e) {
toast.error(e instanceof Error ? e.message : 'Failed to launch instance');
}
}

function getVpcName(vpcId?: string): string {
const vpc = vpcs.find(v => v.VpcId === vpcId);
return vpc?.Tags?.find(t => t.Key === 'Name')?.Value || vpcId || '—';
}

let filtered = $derived(instances.filter(i => {
	const name = getName(i).toLowerCase();
	const id = (i.InstanceId || '').toLowerCase();
	const matchSearch = !search || name.includes(search.toLowerCase()) || id.includes(search.toLowerCase());
	const matchState = stateFilter === 'all' || i.State?.Name === stateFilter;
	const matchVpc = vpcFilter === 'all' || i.VpcId === vpcFilter;
	return matchSearch && matchState && matchVpc;
}));

let runningCount = $derived(instances.filter(i => i.State?.Name === 'running').length);
let stoppedCount = $derived(instances.filter(i => i.State?.Name === 'stopped').length);
let uniqueVPCs = $derived([...new Set(instances.map(i => i.VpcId).filter(Boolean))]);
let filteredSGs = $derived(securityGroups.filter(sg =>
	!sgSearch || sg.GroupName?.toLowerCase().includes(sgSearch.toLowerCase()) || sg.GroupId?.toLowerCase().includes(sgSearch.toLowerCase())
));
let filteredKPs = $derived(keyPairs.filter(kp =>
	!kpSearch || kp.KeyName?.toLowerCase().includes(kpSearch.toLowerCase())
));
let filteredAmis = $derived(amis.filter(ami =>
	!amiSearch || ami.ImageId?.toLowerCase().includes(amiSearch.toLowerCase()) || ami.Name?.toLowerCase().includes(amiSearch.toLowerCase())
));
let filteredLTs = $derived(launchTemplates.filter(lt =>
	!ltSearch || lt.LaunchTemplateName?.toLowerCase().includes(ltSearch.toLowerCase()) || lt.LaunchTemplateId?.toLowerCase().includes(ltSearch.toLowerCase())
));
let filteredVPCEs = $derived(vpcEndpoints.filter(ep =>
	!vpceSearch || ep.VpcEndpointId?.toLowerCase().includes(vpceSearch.toLowerCase()) || ep.ServiceName?.toLowerCase().includes(vpceSearch.toLowerCase())
));
let filteredNacls = $derived(networkAcls.filter(acl =>
	!naclSearch || acl.NetworkAclId?.toLowerCase().includes(naclSearch.toLowerCase()) || acl.VpcId?.toLowerCase().includes(naclSearch.toLowerCase())
));
let filteredVpcs = $derived(vpcs.filter(v =>
	!vpcSearch || v.VpcId?.toLowerCase().includes(vpcSearch.toLowerCase()) || v.CidrBlock?.toLowerCase().includes(vpcSearch.toLowerCase())
));
let filteredVolumes = $derived(volumes.filter(v =>
	!volSearch || v.VolumeId?.toLowerCase().includes(volSearch.toLowerCase()) || v.VolumeType?.toLowerCase().includes(volSearch.toLowerCase())
));
let filteredSnapshots = $derived(snapshots.filter(s =>
	!snapSearch || s.SnapshotId?.toLowerCase().includes(snapSearch.toLowerCase()) || s.Description?.toLowerCase().includes(snapSearch.toLowerCase())
));
let filteredEIPs = $derived(elasticIPs.filter(a =>
	!eipSearch || a.PublicIp?.includes(eipSearch) || a.AllocationId?.includes(eipSearch) || (a.InstanceId || '').includes(eipSearch)
));
let filteredIGWs = $derived(internetGateways.filter(igw =>
	!igwSearch || igw.InternetGatewayId?.includes(igwSearch)
));
let filteredRTs = $derived(routeTables.filter(rt =>
	!rtSearch || rt.RouteTableId?.includes(rtSearch) || rt.VpcId?.includes(rtSearch)
));
let filteredNATs = $derived(natGateways.filter(nat =>
	!natSearch || nat.NatGatewayId?.includes(natSearch) || nat.VpcId?.includes(natSearch)
));
</script>

<div class="space-y-6">
	<div class="flex items-center justify-between">
		<div class="flex items-center gap-3">
			<div class="p-2 bg-orange-100 dark:bg-orange-900/30 rounded-lg">
				<Cpu class="w-6 h-6 text-orange-600 dark:text-orange-400" />
			</div>
			<div>
				<h1 class="text-3xl font-bold text-slate-900 dark:text-white">EC2</h1>
				<p class="text-slate-600 dark:text-slate-300">Elastic Compute Cloud</p>
			</div>
		</div>
		<div class="flex gap-2">
			<button onclick={refresh} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-100 dark:hover:bg-slate-700 text-slate-600 dark:text-slate-300" title="Refresh">
				<RefreshCw class="w-4 h-4" />
			</button>
			{#if activeTab === 'instances'}
			<button onclick={() => showLaunchModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Launch Instance
			</button>
			{:else if activeTab === 'launchtemplates'}
			<button onclick={() => showCreateLTModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 flex items-center gap-2">
				<Plus class="w-4 h-4" />
				Create Template
			</button>
			{/if}
		</div>
	</div>

	<!-- Stats -->
	{#if !loading}
		<div class="grid grid-cols-2 sm:grid-cols-5 gap-4">
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Instances</p>
				<p class="text-2xl font-bold text-slate-900 dark:text-white">{instances.length}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Running</p>
				<p class="text-2xl font-bold text-green-600">{runningCount}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Stopped</p>
				<p class="text-2xl font-bold text-slate-500">{stoppedCount}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Security Groups</p>
				<p class="text-2xl font-bold text-indigo-600 dark:text-indigo-400">{securityGroups.length}</p>
			</div>
			<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
				<p class="text-xs text-slate-500 uppercase tracking-wide mb-1">Volumes</p>
				<p class="text-2xl font-bold text-purple-600 dark:text-purple-400">{volumes.length}</p>
			</div>
		</div>
	{/if}

	<!-- Tabs -->
	<div class="border-b border-slate-200 dark:border-slate-700 overflow-x-auto">
		<nav class="flex gap-1 -mb-px min-w-max">
			<button onclick={() => selectTab('instances')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'instances' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Cpu class="w-4 h-4" /> Instances {#if instances.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{instances.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('secgroups')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'secgroups' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Shield class="w-4 h-4" /> Security Groups {#if securityGroups.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{securityGroups.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('keypairs')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'keypairs' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Key class="w-4 h-4" /> Key Pairs {#if keyPairs.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{keyPairs.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('amis')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'amis' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<FileImage class="w-4 h-4" /> AMIs {#if amis.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{amis.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('launchtemplates')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'launchtemplates' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Layers class="w-4 h-4" /> Launch Templates {#if launchTemplates.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{launchTemplates.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('vpcendpoints')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'vpcendpoints' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Route class="w-4 h-4" /> VPC Endpoints {#if vpcEndpoints.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{vpcEndpoints.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('nacls')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'nacls' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Shield class="w-4 h-4" /> Network ACLs {#if networkAcls.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{networkAcls.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('vpcs')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'vpcs' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Network class="w-4 h-4" /> VPCs {#if vpcs.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{vpcs.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('volumes')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'volumes' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<HardDrive class="w-4 h-4" /> Volumes {#if volumes.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{volumes.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('snapshots')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'snapshots' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Camera class="w-4 h-4" /> Snapshots {#if snapshots.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{snapshots.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('eips')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'eips' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Network class="w-4 h-4" /> Elastic IPs {#if elasticIPs.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{elasticIPs.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('igws')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'igws' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Route class="w-4 h-4" /> Internet Gateways {#if internetGateways.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{internetGateways.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('routetables')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'routetables' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Layers class="w-4 h-4" /> Route Tables {#if routeTables.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{routeTables.length}</span>{/if}
			</button>
			<button onclick={() => selectTab('natgateways')} class="flex items-center gap-1.5 px-4 py-2.5 text-sm font-medium border-b-2 transition-colors {activeTab === 'natgateways' ? 'border-indigo-600 text-indigo-600 dark:border-indigo-400 dark:text-indigo-400' : 'border-transparent text-slate-500 dark:text-slate-400 hover:text-slate-700 hover:border-slate-300'}">
				<Network class="w-4 h-4" /> NAT Gateways {#if natGateways.length > 0}<span class="ml-1 px-1.5 py-0.5 text-xs bg-slate-100 dark:bg-slate-700 rounded-full">{natGateways.length}</span>{/if}
			</button>
		</nav>
	</div>

	<div class="space-y-4">

	<!-- Instances Tab -->
	{#if activeTab === 'instances'}
	<div class="flex gap-3 flex-wrap">
		<div class="relative flex-1 min-w-48">
			<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
			<input
				type="text"
				placeholder="Search instances..."
				bind:value={search}
				class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white"
			/>
		</div>
		<select bind:value={stateFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white">
			<option value="all">All States</option>
			<option value="running">Running</option>
			<option value="stopped">Stopped</option>
			<option value="pending">Pending</option>
			<option value="terminated">Terminated</option>
		</select>
		<select bind:value={vpcFilter} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white">
			<option value="all">All VPCs</option>
			{#each uniqueVPCs as vpcId}
				<option value={vpcId}>{vpcId}</option>
			{/each}
		</select>
	</div>

	{#if loading}
		<div class="text-center py-12 text-slate-500 dark:text-slate-400">
			<div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mb-3"></div>
			<p>Loading instances...</p>
		</div>
	{:else if filtered.length === 0}
		<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
			<Cpu class="w-16 h-16 mx-auto text-slate-300 dark:text-slate-600 mb-4 opacity-50" />
			<p class="text-slate-600 dark:text-slate-300 mb-4">No EC2 instances found</p>
			<button onclick={() => showLaunchModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg">Launch Instance</button>
</div>
{:else}
<div class="grid gap-4">
{#each filtered as instance}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-6">
<div class="flex items-start justify-between mb-4">
<div>
<h3 class="font-semibold text-slate-900 dark:text-white text-lg">{getName(instance)}</h3>
<p class="text-sm text-slate-500 dark:text-slate-400 font-mono">{instance.InstanceId}</p>
</div>
<span class={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(instance.State?.Name ?? '')}`}>
{instance.State?.Name || 'unknown'}
</span>
</div>
<div class="grid grid-cols-2 md:grid-cols-5 gap-4 mb-4 text-sm">
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Type</p>
<p class="font-medium text-slate-900 dark:text-white">{instance.InstanceType || 'N/A'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Public IP</p>
<p class="font-mono text-slate-900 dark:text-white">{instance.PublicIpAddress || '—'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Private IP</p>
<p class="font-mono text-slate-900 dark:text-white">{instance.PrivateIpAddress || '—'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">VPC</p>
<p class="font-mono text-slate-900 dark:text-white text-xs">{instance.VpcId || '—'}</p>
</div>
<div>
<p class="text-slate-500 dark:text-slate-400 text-xs uppercase">Launched</p>
<p class="text-slate-900 dark:text-white text-xs">{instance.LaunchTime ? new Date(instance.LaunchTime).toLocaleString() : '—'}</p>
</div>
</div>
<div class="flex gap-2 flex-wrap">
{#if instance.State?.Name === 'stopped'}
<button onclick={() => startInstance(instance.InstanceId ?? '')} class="px-3 py-2 bg-green-600 text-white rounded text-sm hover:bg-green-700 flex items-center gap-1">
<Play class="w-3 h-3" /> Start
</button>
{:else if instance.State?.Name === 'running'}
<button onclick={() => stopInstance(instance.InstanceId ?? '')} class="px-3 py-2 bg-yellow-600 text-white rounded text-sm hover:bg-yellow-700 flex items-center gap-1">
<Square class="w-3 h-3" /> Stop
</button>
<button onclick={() => rebootInstance(instance.InstanceId ?? '')} class="px-3 py-2 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 flex items-center gap-1">
<RotateCcw class="w-3 h-3" /> Reboot
</button>
{/if}
<button onclick={() => selectedInstance = instance} class="px-3 py-2 border border-slate-200 dark:border-slate-700 rounded text-sm text-slate-700 dark:text-slate-300 hover:bg-slate-100 dark:hover:bg-slate-700">
Details
</button>
{#if instance.State?.Name !== 'terminated'}
<button onclick={() => terminateInstance(instance.InstanceId ?? '')} class="ml-auto px-3 py-2 bg-red-600 text-white rounded text-sm hover:bg-red-700 flex items-center gap-1">
<Trash2 class="w-3 h-3" /> Terminate
</button>
{/if}
</div>
</div>
{/each}
</div>
{/if}

<!-- End Instances Tab -->
{/if}

<!-- Security Groups Tab -->
{#if activeTab === 'secgroups'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search security groups..." bind:value={sgSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredSGs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Shield class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No security groups found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Name</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Group ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">VPC</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Rules</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredSGs as sg}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3">
<p class="font-medium text-slate-900 dark:text-white">{sg.GroupName}</p>
{#if sg.Description}<p class="text-xs text-slate-500 dark:text-slate-400">{sg.Description}</p>{/if}
</td>
<td class="px-4 py-3 font-mono text-xs text-slate-600 dark:text-slate-300">{sg.GroupId}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{sg.VpcId || '—'}</td>
<td class="px-4 py-3 text-slate-600 dark:text-slate-300">
<span class="text-green-600 dark:text-green-400">{sg.IpPermissions?.length ?? 0} in</span>
·
<span class="text-red-600 dark:text-red-400">{sg.IpPermissionsEgress?.length ?? 0} out</span>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- Key Pairs Tab -->
{#if activeTab === 'keypairs'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search key pairs..." bind:value={kpSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredKPs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Key class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No key pairs found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Key Name</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Key ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Fingerprint</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Created</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredKPs as kp}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-medium text-slate-900 dark:text-white">{kp.KeyName}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{kp.KeyPairId || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400 max-w-xs truncate">{kp.KeyFingerprint || '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{kp.CreateTime ? new Date(kp.CreateTime).toLocaleDateString() : '—'}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

{#if activeTab === 'amis'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search AMIs..." bind:value={amiSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredAmis.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<FileImage class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No AMIs found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">AMI ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Name</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Architecture</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Platform</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">State</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredAmis as ami}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{ami.ImageId}</td>
<td class="px-4 py-3 text-slate-900 dark:text-white">{ami.Name || '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{ami.Architecture || 'x86_64'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{ami.Platform || 'Linux'}</td>
<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">{ami.State || 'available'}</span></td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

{#if activeTab === 'launchtemplates'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search launch templates..." bind:value={ltSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredLTs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Layers class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400 mb-4">No launch templates found</p>
<button onclick={() => showCreateLTModal = true} class="px-4 py-2 bg-indigo-600 text-white rounded-lg text-sm">Create Template</button>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Template ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Name</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Default Ver</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Latest Ver</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Created</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300"></th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredLTs as lt}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{lt.LaunchTemplateId}</td>
<td class="px-4 py-3 text-slate-900 dark:text-white font-medium">{lt.LaunchTemplateName || '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{lt.DefaultVersionNumber ?? '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{lt.LatestVersionNumber ?? '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{lt.CreateTime ? new Date(lt.CreateTime).toLocaleDateString() : '—'}</td>
<td class="px-4 py-3">
<button onclick={() => deleteLaunchTemplate(lt.LaunchTemplateId ?? '')} class="text-red-500 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Delete">
<Trash2 class="w-4 h-4" />
</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

{#if activeTab === 'vpcendpoints'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search VPC endpoints..." bind:value={vpceSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredVPCEs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Route class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No VPC endpoints found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Endpoint ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Service</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Type</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">VPC</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">State</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300"></th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredVPCEs as ep}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{ep.VpcEndpointId}</td>
<td class="px-4 py-3 text-slate-900 dark:text-white text-xs">{ep.ServiceName || '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{ep.VpcEndpointType || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{ep.VpcId || '—'}</td>
<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">{ep.State || '—'}</span></td>
<td class="px-4 py-3">
<button onclick={() => deleteVpcEndpoint(ep.VpcEndpointId ?? '')} class="text-red-500 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Delete">
<Trash2 class="w-4 h-4" />
</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

{#if activeTab === 'nacls'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search network ACLs..." bind:value={naclSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredNacls.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Shield class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No network ACLs found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">ACL ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">VPC</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Entries</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Associations</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Default</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredNacls as acl}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{acl.NetworkAclId}</td>
<td class="px-4 py-3 text-slate-900 dark:text-white">{acl.VpcId || '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{acl.Entries?.length ?? 0}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{acl.Associations?.length ?? 0}</td>
<td class="px-4 py-3">
{#if acl.IsDefault}
<span class="px-2 py-0.5 rounded text-xs bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">Default</span>
{:else}
<span class="text-slate-400 dark:text-slate-500">No</span>
{/if}
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- VPCs Tab -->
{#if activeTab === 'vpcs'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search VPCs..." bind:value={vpcSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredVpcs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Network class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No VPCs found</p>
</div>
{:else}
<div class="space-y-4">
{#each filteredVpcs as vpc}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-4">
<div class="flex items-start justify-between mb-3">
<div>
<p class="font-mono text-sm font-medium text-slate-900 dark:text-white">{vpc.VpcId}</p>
<p class="text-xs text-slate-500 dark:text-slate-400">{vpc.CidrBlock}</p>
</div>
<div class="flex items-center gap-2">
{#if vpc.IsDefault}
<span class="px-2 py-0.5 rounded text-xs bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">Default</span>
{/if}
<span class="px-2 py-0.5 rounded text-xs bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">{vpc.State || 'available'}</span>
</div>
</div>
<!-- Subnets for this VPC -->
{#if subnets.filter(s => s.VpcId === vpc.VpcId).length > 0}
<div class="mt-2 border-t border-slate-100 dark:border-slate-700 pt-2">
<p class="text-xs text-slate-500 dark:text-slate-400 mb-2">Subnets ({subnets.filter(s => s.VpcId === vpc.VpcId).length})</p>
<div class="grid grid-cols-1 sm:grid-cols-2 gap-2">
{#each subnets.filter(s => s.VpcId === vpc.VpcId) as subnet}
<div class="bg-slate-50 dark:bg-slate-700/50 rounded px-3 py-2 text-xs">
<p class="font-mono text-slate-900 dark:text-white">{subnet.SubnetId}</p>
<p class="text-slate-500 dark:text-slate-400">{subnet.CidrBlock} · {subnet.AvailabilityZone}</p>
{#if subnet.DefaultForAz}<span class="text-blue-600 dark:text-blue-400">Default</span>{/if}
</div>
{/each}
</div>
</div>
{/if}
</div>
{/each}
</div>
{/if}
{/if}

<!-- Volumes Tab -->
{#if activeTab === 'volumes'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search volumes..." bind:value={volSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredVolumes.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<HardDrive class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No EBS volumes found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Volume ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Size (GiB)</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Type</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">AZ</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Attached To</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">State</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredVolumes as vol}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{vol.VolumeId}</td>
<td class="px-4 py-3 text-slate-900 dark:text-white">{vol.Size ?? '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{vol.VolumeType || 'gp2'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{vol.AvailabilityZone || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{vol.Attachments?.[0]?.InstanceId || '—'}</td>
<td class="px-4 py-3">
<span class={`px-2 py-0.5 rounded text-xs ${vol.State === 'available' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : vol.State === 'in-use' ? 'bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-600 dark:text-slate-300'}`}>
{vol.State || '—'}
</span>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- Snapshots Tab -->
{#if activeTab === 'snapshots'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search snapshots..." bind:value={snapSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredSnapshots.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Camera class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No EBS snapshots found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Snapshot ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Volume ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Size (GiB)</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Description</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Progress</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Started</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300"></th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredSnapshots as snap}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{snap.SnapshotId}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{snap.VolumeId || '—'}</td>
<td class="px-4 py-3 text-slate-900 dark:text-white">{snap.VolumeSize ?? '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400 max-w-xs truncate">{snap.Description || '—'}</td>
<td class="px-4 py-3">
<span class="px-2 py-0.5 rounded text-xs bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300">{snap.Progress || '100%'}</span>
</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400 text-xs">{snap.StartTime ? new Date(snap.StartTime).toLocaleString() : '—'}</td>
<td class="px-4 py-3">
<button onclick={() => deleteSnapshot(snap.SnapshotId ?? '')} class="text-red-500 hover:text-red-700 dark:text-red-400 dark:hover:text-red-300" title="Delete">
<Trash2 class="w-4 h-4" />
</button>
</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- Elastic IPs Tab -->
{#if activeTab === 'eips'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search Elastic IPs..." bind:value={eipSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredEIPs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Network class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No Elastic IPs allocated</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Allocation ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Public IP</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Associated Instance</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Association ID</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredEIPs as addr}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{addr.AllocationId || '—'}</td>
<td class="px-4 py-3 text-slate-900 dark:text-white font-mono">{addr.PublicIp || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{#if addr.InstanceId}{addr.InstanceId}{:else}<span class="italic text-slate-400">unassociated</span>{/if}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{addr.AssociationId || '—'}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- Internet Gateways Tab -->
{#if activeTab === 'igws'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search Internet Gateways..." bind:value={igwSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredIGWs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Route class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No Internet Gateways found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Gateway ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Attached VPC</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">State</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredIGWs as igw}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{igw.InternetGatewayId || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{#if igw.Attachments?.[0]?.VpcId}{igw.Attachments[0].VpcId}{:else}<span class="italic text-slate-400">detached</span>{/if}</td>
<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs {(igw.Attachments?.length ?? 0) > 0 ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-500'}">{(igw.Attachments?.length ?? 0) > 0 ? 'attached' : 'detached'}</span></td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- Route Tables Tab -->
{#if activeTab === 'routetables'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search Route Tables..." bind:value={rtSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredRTs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Layers class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No Route Tables found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Route Table ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">VPC</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Routes</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Associations</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Main</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredRTs as rt}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{rt.RouteTableId || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{rt.VpcId || '—'}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{rt.Routes?.length ?? 0}</td>
<td class="px-4 py-3 text-slate-500 dark:text-slate-400">{rt.Associations?.length ?? 0}</td>
<td class="px-4 py-3">{#if rt.Associations?.some(a => a.Main)}<span class="px-2 py-0.5 rounded text-xs bg-blue-100 dark:bg-blue-900/30 text-blue-700 dark:text-blue-300">Main</span>{:else}—{/if}</td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

<!-- NAT Gateways Tab -->
{#if activeTab === 'natgateways'}
<div class="relative">
<Search class="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
<input type="text" placeholder="Search NAT Gateways..." bind:value={natSearch}
class="w-full pl-9 pr-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-800 text-slate-900 dark:text-white" />
</div>
{#if loading}
<div class="text-center py-12"><div class="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500"></div></div>
{:else if filteredNATs.length === 0}
<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700 p-12 text-center">
<Network class="w-16 h-16 mx-auto text-slate-300 mb-4 opacity-50" />
<p class="text-slate-500 dark:text-slate-400">No NAT Gateways found</p>
</div>
{:else}
<div class="bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg overflow-hidden">
<table class="w-full text-sm">
<thead class="bg-slate-50 dark:bg-slate-700">
<tr>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Gateway ID</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">VPC</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Subnet</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">Public IP</th>
<th class="text-left px-4 py-3 font-medium text-slate-600 dark:text-slate-300">State</th>
</tr>
</thead>
<tbody class="divide-y divide-slate-100 dark:divide-slate-700">
{#each filteredNATs as nat}
<tr class="hover:bg-slate-50 dark:hover:bg-slate-700/50">
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{nat.NatGatewayId || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{nat.VpcId || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{nat.SubnetId || '—'}</td>
<td class="px-4 py-3 font-mono text-xs text-slate-500 dark:text-slate-400">{nat.NatGatewayAddresses?.[0]?.PublicIp || '—'}</td>
<td class="px-4 py-3"><span class="px-2 py-0.5 rounded text-xs {nat.State === 'available' ? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-300' : 'bg-slate-100 dark:bg-slate-700 text-slate-500'}">{nat.State || '—'}</span></td>
</tr>
{/each}
</tbody>
</table>
</div>
{/if}
{/if}

</div>

<!-- Create Launch Template Modal -->
{#if showCreateLTModal}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md p-6">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Create Launch Template</h2>
<div class="space-y-4">
<div>
<label for="lt-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Template Name</label>
<input id="lt-name" type="text" bind:value={newLTName} placeholder="e.g. web-server-template"
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
</div>
<div>
<label for="lt-ami" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">AMI ID</label>
<input id="lt-ami" type="text" bind:value={newLTImageId} placeholder="ami-..."
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
</div>
<div>
<label for="lt-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Type</label>
<select id="lt-type" bind:value={newLTInstanceType} class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white">
{#each instanceTypes as t}<option value={t}>{t}</option>{/each}
</select>
</div>
</div>
<div class="flex justify-end gap-3 mt-6">
<button onclick={() => showCreateLTModal = false} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-700 dark:text-slate-300">Cancel</button>
<button onclick={createLaunchTemplate} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700">Create</button>
</div>
</div>
</div>
{/if}

{#if showLaunchModal}
<div class="fixed inset-0 bg-black/50 z-50 flex items-center justify-center p-4">
<div class="bg-white dark:bg-slate-800 rounded-xl shadow-xl w-full max-w-md p-6">
<h2 class="text-xl font-bold text-slate-900 dark:text-white mb-4">Launch EC2 Instance</h2>
<div class="space-y-4">
<div>
<label for="launch-instance-name" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Name</label>
<input id="launch-instance-name" type="text" bind:value={newInstanceName} placeholder="e.g. web-server-01"
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
</div>
<div>
<label for="launch-instance-ami" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">AMI ID</label>
<input id="launch-instance-ami" type="text" bind:value={newInstanceAmi} placeholder="ami-..."
class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white" />
</div>
<div>
<label for="launch-instance-type" class="block text-sm font-medium text-slate-700 dark:text-slate-300 mb-1">Instance Type</label>
<select id="launch-instance-type" bind:value={newInstanceType} class="w-full px-3 py-2 border border-slate-200 dark:border-slate-700 rounded-lg bg-white dark:bg-slate-900 text-slate-900 dark:text-white">
{#each instanceTypes as t}<option value={t}>{t}</option>{/each}
</select>
</div>
</div>
<div class="flex justify-end gap-3 mt-6">
<button onclick={() => showLaunchModal = false} class="px-4 py-2 border border-slate-200 dark:border-slate-700 rounded-lg text-slate-700 dark:text-slate-300">Cancel</button>
<button onclick={launchInstance} class="px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700">Launch</button>
</div>
</div>
</div>
{/if}

{#if selectedInstance}
<div class="fixed inset-0 bg-black/50 z-50 flex items-end justify-end">
<div class="bg-white dark:bg-slate-800 w-full max-w-lg h-full overflow-y-auto p-6">
<div class="flex items-center justify-between mb-6">
<h2 class="text-xl font-bold text-slate-900 dark:text-white">{getName(selectedInstance)}</h2>
<button onclick={() => selectedInstance = null} class="text-slate-400 hover:text-slate-600 text-2xl">&times;</button>
</div>
<div class="grid grid-cols-2 gap-4 text-sm">
{#each [['Instance ID', selectedInstance.InstanceId], ['Type', selectedInstance.InstanceType], ['State', selectedInstance.State?.Name], ['Public IP', selectedInstance.PublicIpAddress || '—'], ['Private IP', selectedInstance.PrivateIpAddress || '—'], ['VPC', selectedInstance.VpcId || '—'], ['Subnet', selectedInstance.SubnetId || '—'], ['AMI', selectedInstance.ImageId || '—'], ['Launch Time', selectedInstance.LaunchTime ? new Date(selectedInstance.LaunchTime).toLocaleString() : '—']] as [label, val]}
<div>
<p class="text-slate-500 text-xs uppercase">{label}</p>
<p class="font-mono text-slate-900 dark:text-white text-xs break-all">{val}</p>
</div>
{/each}
</div>
</div>
</div>
{/if}
</div>