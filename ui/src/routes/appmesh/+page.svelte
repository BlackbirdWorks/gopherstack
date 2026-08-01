<script lang="ts">
	// App Mesh is a nested hierarchy: a mesh contains virtual nodes, virtual
	// routers, virtual services and virtual gateways; virtual routers contain
	// routes and virtual gateways contain gateway routes. Every top-level tab
	// below is therefore mesh-scoped (via `selectedMeshName`), and routes /
	// gateway routes are managed inline inside their parent router's / gateway's
	// detail modal rather than as their own top-level tab, matching the real
	// resource hierarchy instead of flattening it.
	//
	// Spec bodies (MeshSpec/VirtualNodeSpec/VirtualRouterSpec/RouteSpec/
	// VirtualServiceSpec/VirtualGatewaySpec/GatewayRouteSpec) are deeply nested
	// AWS shapes that this backend stores and echoes back as opaque JSON with
	// no schema validation (see services/appmesh/PARITY.md). A structured
	// sub-form per spec type would imply validation the emulator doesn't do,
	// so specs are edited as raw JSON here, the same pattern already used by
	// mediapackage's HlsPackage field.
	import { untrack } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getAppMeshClient } from '$lib/aws-client';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import { formatDate } from '$lib/format';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import {
		ListMeshesCommand,
		CreateMeshCommand,
		DescribeMeshCommand,
		UpdateMeshCommand,
		DeleteMeshCommand,
		ListVirtualNodesCommand,
		CreateVirtualNodeCommand,
		DescribeVirtualNodeCommand,
		UpdateVirtualNodeCommand,
		DeleteVirtualNodeCommand,
		ListVirtualRoutersCommand,
		CreateVirtualRouterCommand,
		DescribeVirtualRouterCommand,
		UpdateVirtualRouterCommand,
		DeleteVirtualRouterCommand,
		ListRoutesCommand,
		CreateRouteCommand,
		DescribeRouteCommand,
		DeleteRouteCommand,
		ListVirtualServicesCommand,
		CreateVirtualServiceCommand,
		DescribeVirtualServiceCommand,
		UpdateVirtualServiceCommand,
		DeleteVirtualServiceCommand,
		ListVirtualGatewaysCommand,
		CreateVirtualGatewayCommand,
		DescribeVirtualGatewayCommand,
		UpdateVirtualGatewayCommand,
		DeleteVirtualGatewayCommand,
		ListGatewayRoutesCommand,
		CreateGatewayRouteCommand,
		DescribeGatewayRouteCommand,
		DeleteGatewayRouteCommand,
		type MeshRef,
		type MeshData,
		type VirtualNodeRef,
		type VirtualNodeData,
		type VirtualRouterRef,
		type VirtualRouterData,
		type RouteRef,
		type RouteData,
		type VirtualServiceRef,
		type VirtualServiceData,
		type VirtualGatewayRef,
		type VirtualGatewayData,
		type GatewayRouteRef,
		type GatewayRouteData
	} from '@aws-sdk/client-app-mesh';
	import { toast } from 'svelte-sonner';
	import { Share2, Plus, Trash2, Eye, Pencil } from 'lucide-svelte';

	const client = regionalClient(getAppMeshClient);

	type TabId = 'meshes' | 'nodes' | 'routers' | 'services' | 'gateways';

	const tabs: TabDef[] = [
		{ id: 'meshes', label: 'Meshes' },
		{ id: 'nodes', label: 'Virtual Nodes' },
		{ id: 'routers', label: 'Virtual Routers' },
		{ id: 'services', label: 'Virtual Services' },
		{ id: 'gateways', label: 'Virtual Gateways' }
	];

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
	function rethrowDescribed(e: unknown): never {
		throw new Error(describeError(e));
	}

	function parseSpec(json: string): Record<string, unknown> {
		const trimmed = json.trim();
		return trimmed ? JSON.parse(trimmed) : {};
	}

	let activeTab = $state<TabId>('meshes');
	let searchQuery = $state('');
	let selectedMeshName = $state('');

	// --- Meshes ---
	let meshes = $state<MeshRef[]>([]);
	async function fetchMeshes(): Promise<void> {
		const resp = await client().send(new ListMeshesCommand({}));
		meshes = resp.meshes ?? [];
		if (!untrack(() => selectedMeshName) && meshes.length > 0) {
			selectedMeshName = meshes[0].meshName ?? '';
		}
	}

	// --- Virtual Nodes ---
	let nodes = $state<VirtualNodeRef[]>([]);
	async function fetchNodes(): Promise<void> {
		const meshName = untrack(() => selectedMeshName);
		nodes = meshName ? (await client().send(new ListVirtualNodesCommand({ meshName }))).virtualNodes ?? [] : [];
	}

	// --- Virtual Routers ---
	let routers = $state<VirtualRouterRef[]>([]);
	async function fetchRouters(): Promise<void> {
		const meshName = untrack(() => selectedMeshName);
		routers = meshName ? (await client().send(new ListVirtualRoutersCommand({ meshName }))).virtualRouters ?? [] : [];
	}

	// --- Virtual Services ---
	let services = $state<VirtualServiceRef[]>([]);
	async function fetchServices(): Promise<void> {
		const meshName = untrack(() => selectedMeshName);
		services = meshName ? (await client().send(new ListVirtualServicesCommand({ meshName }))).virtualServices ?? [] : [];
	}

	// --- Virtual Gateways ---
	let gateways = $state<VirtualGatewayRef[]>([]);
	async function fetchGateways(): Promise<void> {
		const meshName = untrack(() => selectedMeshName);
		gateways = meshName ? (await client().send(new ListVirtualGatewaysCommand({ meshName }))).virtualGateways ?? [] : [];
	}

	const tabLoader = createTabLoader<TabId>({
		meshes: () => fetchMeshes().catch(rethrowDescribed),
		nodes: () => fetchNodes().catch(rethrowDescribed),
		routers: () => fetchRouters().catch(rethrowDescribed),
		services: () => fetchServices().catch(rethrowDescribed),
		gateways: () => fetchGateways().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}
	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}
	function changeScopeMesh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		selectedMeshName = '';
		viewedMesh = null;
		viewedNode = null;
		viewedRouter = null;
		viewedService = null;
		viewedGateway = null;
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	function matches(q: string, ...parts: (string | undefined)[]): boolean {
		if (!q) return true;
		return parts.some((p) => (p ?? '').toLowerCase().includes(q.toLowerCase()));
	}

	const filteredMeshes = $derived(meshes.filter((m) => matches(searchQuery, m.meshName)));
	const filteredNodes = $derived(nodes.filter((n) => matches(searchQuery, n.virtualNodeName)));
	const filteredRouters = $derived(routers.filter((r) => matches(searchQuery, r.virtualRouterName)));
	const filteredServices = $derived(services.filter((s) => matches(searchQuery, s.virtualServiceName)));
	const filteredGateways = $derived(gateways.filter((g) => matches(searchQuery, g.virtualGatewayName)));

	// ── Meshes: create / detail / edit / delete ─────────────────────────────
	let createMeshModal = $state<Modal | null>(null);
	let creatingMesh = $state(false);
	let createMeshError = $state<string | null>(null);
	let newMeshName = $state('');
	let newMeshSpecJson = $state('');

	function openCreateMeshModal(): void {
		createMeshError = null;
		newMeshName = '';
		newMeshSpecJson = '';
		createMeshModal?.open();
	}

	async function submitCreateMesh(): Promise<void> {
		if (!newMeshName) {
			createMeshError = 'Mesh name is required.';
			return;
		}
		let spec: Record<string, unknown> | undefined;
		try {
			spec = newMeshSpecJson.trim() ? parseSpec(newMeshSpecJson) : undefined;
		} catch {
			createMeshError = 'Spec must be valid JSON.';
			return;
		}
		creatingMesh = true;
		createMeshError = null;
		try {
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			await client().send(new CreateMeshCommand({ meshName: newMeshName, spec: spec as any }));
			toast.success('Mesh created');
			createMeshModal?.close();
			await tabLoader.refresh('meshes');
		} catch (e) {
			const msg = describeError(e);
			createMeshError = msg;
			toast.error(msg);
		} finally {
			creatingMesh = false;
		}
	}

	let meshDetailModal = $state<Modal | null>(null);
	let viewedMesh = $state<MeshData | null>(null);
	let meshDetailLoading = $state(false);
	let meshDetailError = $state<string | null>(null);

	async function openMeshDetail(m: MeshRef): Promise<void> {
		viewedMesh = null;
		meshDetailError = null;
		meshDetailModal?.open();
		if (!m.meshName) return;
		meshDetailLoading = true;
		try {
			const resp = await client().send(new DescribeMeshCommand({ meshName: m.meshName }));
			viewedMesh = resp.mesh ?? null;
		} catch (e) {
			meshDetailError = describeError(e);
		} finally {
			meshDetailLoading = false;
		}
	}

	let editMeshModal = $state<Modal | null>(null);
	let editingMesh = $state(false);
	let editMeshError = $state<string | null>(null);
	let editMeshName = $state('');
	let editMeshSpecJson = $state('');

	function openEditMeshModal(m: MeshData): void {
		editMeshError = null;
		editMeshName = m.meshName ?? '';
		editMeshSpecJson = m.spec ? JSON.stringify(m.spec, null, 2) : '';
		editMeshModal?.open();
	}

	async function submitEditMesh(): Promise<void> {
		if (!editMeshName) return;
		let spec: Record<string, unknown> | undefined;
		try {
			spec = editMeshSpecJson.trim() ? parseSpec(editMeshSpecJson) : undefined;
		} catch {
			editMeshError = 'Spec must be valid JSON.';
			return;
		}
		editingMesh = true;
		editMeshError = null;
		try {
			// eslint-disable-next-line @typescript-eslint/no-explicit-any
			const resp = await client().send(new UpdateMeshCommand({ meshName: editMeshName, spec: spec as any }));
			toast.success('Mesh updated');
			editMeshModal?.close();
			await tabLoader.refresh('meshes');
			viewedMesh = resp.mesh ?? viewedMesh;
		} catch (e) {
			const msg = describeError(e);
			editMeshError = msg;
			toast.error(msg);
		} finally {
			editingMesh = false;
		}
	}

	async function deleteMesh(m: MeshRef | MeshData): Promise<void> {
		if (!m.meshName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete mesh',
			message: `Delete mesh "${m.meshName}"? All virtual nodes, routers, services and gateways inside it must be empty first.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteMeshCommand({ meshName: m.meshName }));
			toast.success('Mesh deleted');
			meshDetailModal?.close();
			await tabLoader.refresh('meshes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Virtual Nodes: create / detail / edit / delete ──────────────────────
	let createNodeModal = $state<Modal | null>(null);
	let creatingNode = $state(false);
	let createNodeError = $state<string | null>(null);
	let newNodeName = $state('');
	let newNodeSpecJson = $state('');

	function openCreateNodeModal(): void {
		createNodeError = null;
		newNodeName = '';
		newNodeSpecJson = '';
		createNodeModal?.open();
	}

	async function submitCreateNode(): Promise<void> {
		if (!newNodeName || !selectedMeshName) {
			createNodeError = 'Virtual node name is required, and a mesh must be selected.';
			return;
		}
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(newNodeSpecJson);
		} catch {
			createNodeError = 'Spec must be valid JSON.';
			return;
		}
		creatingNode = true;
		createNodeError = null;
		try {
			await client().send(
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				new CreateVirtualNodeCommand({ meshName: selectedMeshName, virtualNodeName: newNodeName, spec: spec as any })
			);
			toast.success('Virtual node created');
			createNodeModal?.close();
			await tabLoader.refresh('nodes');
		} catch (e) {
			const msg = describeError(e);
			createNodeError = msg;
			toast.error(msg);
		} finally {
			creatingNode = false;
		}
	}

	let nodeDetailModal = $state<Modal | null>(null);
	let viewedNode = $state<VirtualNodeData | null>(null);
	let nodeDetailLoading = $state(false);
	let nodeDetailError = $state<string | null>(null);

	async function openNodeDetail(n: VirtualNodeRef): Promise<void> {
		viewedNode = null;
		nodeDetailError = null;
		nodeDetailModal?.open();
		if (!n.meshName || !n.virtualNodeName) return;
		nodeDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVirtualNodeCommand({ meshName: n.meshName, virtualNodeName: n.virtualNodeName })
			);
			viewedNode = resp.virtualNode ?? null;
		} catch (e) {
			nodeDetailError = describeError(e);
		} finally {
			nodeDetailLoading = false;
		}
	}

	let editNodeModal = $state<Modal | null>(null);
	let editingNode = $state(false);
	let editNodeError = $state<string | null>(null);
	let editNodeName = $state('');
	let editNodeSpecJson = $state('');

	function openEditNodeModal(n: VirtualNodeData): void {
		editNodeError = null;
		editNodeName = n.virtualNodeName ?? '';
		editNodeSpecJson = n.spec ? JSON.stringify(n.spec, null, 2) : '';
		editNodeModal?.open();
	}

	async function submitEditNode(): Promise<void> {
		if (!editNodeName || !selectedMeshName) return;
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(editNodeSpecJson);
		} catch {
			editNodeError = 'Spec must be valid JSON.';
			return;
		}
		editingNode = true;
		editNodeError = null;
		try {
			const resp = await client().send(
				// eslint-disable-next-line @typescript-eslint/no-explicit-any
				new UpdateVirtualNodeCommand({ meshName: selectedMeshName, virtualNodeName: editNodeName, spec: spec as any })
			);
			toast.success('Virtual node updated');
			editNodeModal?.close();
			await tabLoader.refresh('nodes');
			viewedNode = resp.virtualNode ?? viewedNode;
		} catch (e) {
			const msg = describeError(e);
			editNodeError = msg;
			toast.error(msg);
		} finally {
			editingNode = false;
		}
	}

	async function deleteNode(n: VirtualNodeRef | VirtualNodeData): Promise<void> {
		if (!n.meshName || !n.virtualNodeName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete virtual node',
			message: `Delete virtual node "${n.virtualNodeName}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteVirtualNodeCommand({ meshName: n.meshName, virtualNodeName: n.virtualNodeName }));
			toast.success('Virtual node deleted');
			nodeDetailModal?.close();
			await tabLoader.refresh('nodes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Virtual Services: create / detail / edit / delete ───────────────────
	let createServiceModal = $state<Modal | null>(null);
	let creatingService = $state(false);
	let createServiceError = $state<string | null>(null);
	let newServiceName = $state('');
	let newServiceSpecJson = $state('');

	function openCreateServiceModal(): void {
		createServiceError = null;
		newServiceName = '';
		newServiceSpecJson = '';
		createServiceModal?.open();
	}

	async function submitCreateService(): Promise<void> {
		if (!newServiceName || !selectedMeshName) {
			createServiceError = 'Virtual service name is required, and a mesh must be selected.';
			return;
		}
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(newServiceSpecJson);
		} catch {
			createServiceError = 'Spec must be valid JSON.';
			return;
		}
		creatingService = true;
		createServiceError = null;
		try {
			await client().send(
				new CreateVirtualServiceCommand({
					meshName: selectedMeshName,
					virtualServiceName: newServiceName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Virtual service created');
			createServiceModal?.close();
			await tabLoader.refresh('services');
		} catch (e) {
			const msg = describeError(e);
			createServiceError = msg;
			toast.error(msg);
		} finally {
			creatingService = false;
		}
	}

	let serviceDetailModal = $state<Modal | null>(null);
	let viewedService = $state<VirtualServiceData | null>(null);
	let serviceDetailLoading = $state(false);
	let serviceDetailError = $state<string | null>(null);

	async function openServiceDetail(s: VirtualServiceRef): Promise<void> {
		viewedService = null;
		serviceDetailError = null;
		serviceDetailModal?.open();
		if (!s.meshName || !s.virtualServiceName) return;
		serviceDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVirtualServiceCommand({ meshName: s.meshName, virtualServiceName: s.virtualServiceName })
			);
			viewedService = resp.virtualService ?? null;
		} catch (e) {
			serviceDetailError = describeError(e);
		} finally {
			serviceDetailLoading = false;
		}
	}

	let editServiceModal = $state<Modal | null>(null);
	let editingService = $state(false);
	let editServiceError = $state<string | null>(null);
	let editServiceName = $state('');
	let editServiceSpecJson = $state('');

	function openEditServiceModal(s: VirtualServiceData): void {
		editServiceError = null;
		editServiceName = s.virtualServiceName ?? '';
		editServiceSpecJson = s.spec ? JSON.stringify(s.spec, null, 2) : '';
		editServiceModal?.open();
	}

	async function submitEditService(): Promise<void> {
		if (!editServiceName || !selectedMeshName) return;
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(editServiceSpecJson);
		} catch {
			editServiceError = 'Spec must be valid JSON.';
			return;
		}
		editingService = true;
		editServiceError = null;
		try {
			const resp = await client().send(
				new UpdateVirtualServiceCommand({
					meshName: selectedMeshName,
					virtualServiceName: editServiceName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Virtual service updated');
			editServiceModal?.close();
			await tabLoader.refresh('services');
			viewedService = resp.virtualService ?? viewedService;
		} catch (e) {
			const msg = describeError(e);
			editServiceError = msg;
			toast.error(msg);
		} finally {
			editingService = false;
		}
	}

	async function deleteService(s: VirtualServiceRef | VirtualServiceData): Promise<void> {
		if (!s.meshName || !s.virtualServiceName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete virtual service',
			message: `Delete virtual service "${s.virtualServiceName}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteVirtualServiceCommand({ meshName: s.meshName, virtualServiceName: s.virtualServiceName })
			);
			toast.success('Virtual service deleted');
			serviceDetailModal?.close();
			await tabLoader.refresh('services');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Virtual Routers: create / detail (+ nested Routes) / edit / delete ──
	let createRouterModal = $state<Modal | null>(null);
	let creatingRouter = $state(false);
	let createRouterError = $state<string | null>(null);
	let newRouterName = $state('');
	let newRouterSpecJson = $state('');

	function openCreateRouterModal(): void {
		createRouterError = null;
		newRouterName = '';
		newRouterSpecJson = '';
		createRouterModal?.open();
	}

	async function submitCreateRouter(): Promise<void> {
		if (!newRouterName || !selectedMeshName) {
			createRouterError = 'Virtual router name is required, and a mesh must be selected.';
			return;
		}
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(newRouterSpecJson);
		} catch {
			createRouterError = 'Spec must be valid JSON.';
			return;
		}
		creatingRouter = true;
		createRouterError = null;
		try {
			await client().send(
				new CreateVirtualRouterCommand({
					meshName: selectedMeshName,
					virtualRouterName: newRouterName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Virtual router created');
			createRouterModal?.close();
			await tabLoader.refresh('routers');
		} catch (e) {
			const msg = describeError(e);
			createRouterError = msg;
			toast.error(msg);
		} finally {
			creatingRouter = false;
		}
	}

	let routerDetailModal = $state<Modal | null>(null);
	let viewedRouter = $state<VirtualRouterData | null>(null);
	let routerDetailLoading = $state(false);
	let routerDetailError = $state<string | null>(null);
	let routerRoutes = $state<RouteRef[]>([]);
	let routerRoutesLoading = $state(false);

	async function loadRouterRoutes(): Promise<void> {
		if (!viewedRouter?.meshName || !viewedRouter.virtualRouterName) return;
		routerRoutesLoading = true;
		try {
			const resp = await client().send(
				new ListRoutesCommand({ meshName: viewedRouter.meshName, virtualRouterName: viewedRouter.virtualRouterName })
			);
			routerRoutes = resp.routes ?? [];
		} catch (e) {
			toast.error('Failed to load routes: ' + describeError(e));
		} finally {
			routerRoutesLoading = false;
		}
	}

	async function openRouterDetail(r: VirtualRouterRef): Promise<void> {
		viewedRouter = null;
		routerDetailError = null;
		routerRoutes = [];
		showAddRoute = false;
		expandedRoute = null;
		routerDetailModal?.open();
		if (!r.meshName || !r.virtualRouterName) return;
		routerDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVirtualRouterCommand({ meshName: r.meshName, virtualRouterName: r.virtualRouterName })
			);
			viewedRouter = resp.virtualRouter ?? null;
			await loadRouterRoutes();
		} catch (e) {
			routerDetailError = describeError(e);
		} finally {
			routerDetailLoading = false;
		}
	}

	let editRouterModal = $state<Modal | null>(null);
	let editingRouter = $state(false);
	let editRouterError = $state<string | null>(null);
	let editRouterName = $state('');
	let editRouterSpecJson = $state('');

	function openEditRouterModal(r: VirtualRouterData): void {
		editRouterError = null;
		editRouterName = r.virtualRouterName ?? '';
		editRouterSpecJson = r.spec ? JSON.stringify(r.spec, null, 2) : '';
		editRouterModal?.open();
	}

	async function submitEditRouter(): Promise<void> {
		if (!editRouterName || !selectedMeshName) return;
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(editRouterSpecJson);
		} catch {
			editRouterError = 'Spec must be valid JSON.';
			return;
		}
		editingRouter = true;
		editRouterError = null;
		try {
			const resp = await client().send(
				new UpdateVirtualRouterCommand({
					meshName: selectedMeshName,
					virtualRouterName: editRouterName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Virtual router updated');
			editRouterModal?.close();
			await tabLoader.refresh('routers');
			viewedRouter = resp.virtualRouter ?? viewedRouter;
		} catch (e) {
			const msg = describeError(e);
			editRouterError = msg;
			toast.error(msg);
		} finally {
			editingRouter = false;
		}
	}

	async function deleteRouter(r: VirtualRouterRef | VirtualRouterData): Promise<void> {
		if (!r.meshName || !r.virtualRouterName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete virtual router',
			message: `Delete virtual router "${r.virtualRouterName}"? Its routes must be deleted first.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteVirtualRouterCommand({ meshName: r.meshName, virtualRouterName: r.virtualRouterName }));
			toast.success('Virtual router deleted');
			routerDetailModal?.close();
			await tabLoader.refresh('routers');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// -- Routes, nested inside the virtual router detail modal --
	let showAddRoute = $state(false);
	let creatingRoute = $state(false);
	let createRouteError = $state<string | null>(null);
	let newRouteName = $state('');
	let newRouteSpecJson = $state('');
	let expandedRoute = $state<string | null>(null);
	let expandedRouteData = $state<RouteData | null>(null);

	function openAddRoute(): void {
		createRouteError = null;
		newRouteName = '';
		newRouteSpecJson = '';
		showAddRoute = true;
	}

	async function submitCreateRoute(): Promise<void> {
		if (!viewedRouter?.meshName || !viewedRouter.virtualRouterName) return;
		if (!newRouteName) {
			createRouteError = 'Route name is required.';
			return;
		}
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(newRouteSpecJson);
		} catch {
			createRouteError = 'Spec must be valid JSON.';
			return;
		}
		creatingRoute = true;
		createRouteError = null;
		try {
			await client().send(
				new CreateRouteCommand({
					meshName: viewedRouter.meshName,
					virtualRouterName: viewedRouter.virtualRouterName,
					routeName: newRouteName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Route created');
			showAddRoute = false;
			await loadRouterRoutes();
		} catch (e) {
			const msg = describeError(e);
			createRouteError = msg;
			toast.error(msg);
		} finally {
			creatingRoute = false;
		}
	}

	async function toggleRouteDetail(r: RouteRef): Promise<void> {
		const key = r.routeName ?? '';
		if (expandedRoute === key) {
			expandedRoute = null;
			expandedRouteData = null;
			return;
		}
		expandedRoute = key;
		expandedRouteData = null;
		if (!r.meshName || !r.virtualRouterName || !r.routeName) return;
		try {
			const resp = await client().send(
				new DescribeRouteCommand({ meshName: r.meshName, virtualRouterName: r.virtualRouterName, routeName: r.routeName })
			);
			expandedRouteData = resp.route ?? null;
		} catch (e) {
			toast.error('Failed to load route: ' + describeError(e));
		}
	}

	async function deleteRoute(r: RouteRef): Promise<void> {
		if (!r.meshName || !r.virtualRouterName || !r.routeName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete route',
			message: `Delete route "${r.routeName}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteRouteCommand({ meshName: r.meshName, virtualRouterName: r.virtualRouterName, routeName: r.routeName })
			);
			toast.success('Route deleted');
			if (expandedRoute === r.routeName) {
				expandedRoute = null;
				expandedRouteData = null;
			}
			await loadRouterRoutes();
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Virtual Gateways: create / detail (+ nested Gateway Routes) / edit / delete ──
	let createGatewayModal = $state<Modal | null>(null);
	let creatingGateway = $state(false);
	let createGatewayError = $state<string | null>(null);
	let newGatewayName = $state('');
	let newGatewaySpecJson = $state('');

	function openCreateGatewayModal(): void {
		createGatewayError = null;
		newGatewayName = '';
		newGatewaySpecJson = '';
		createGatewayModal?.open();
	}

	async function submitCreateGateway(): Promise<void> {
		if (!newGatewayName || !selectedMeshName) {
			createGatewayError = 'Virtual gateway name is required, and a mesh must be selected.';
			return;
		}
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(newGatewaySpecJson);
		} catch {
			createGatewayError = 'Spec must be valid JSON.';
			return;
		}
		creatingGateway = true;
		createGatewayError = null;
		try {
			await client().send(
				new CreateVirtualGatewayCommand({
					meshName: selectedMeshName,
					virtualGatewayName: newGatewayName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Virtual gateway created');
			createGatewayModal?.close();
			await tabLoader.refresh('gateways');
		} catch (e) {
			const msg = describeError(e);
			createGatewayError = msg;
			toast.error(msg);
		} finally {
			creatingGateway = false;
		}
	}

	let gatewayDetailModal = $state<Modal | null>(null);
	let viewedGateway = $state<VirtualGatewayData | null>(null);
	let gatewayDetailLoading = $state(false);
	let gatewayDetailError = $state<string | null>(null);
	let gatewayRoutes = $state<GatewayRouteRef[]>([]);
	let gatewayRoutesLoading = $state(false);

	async function loadGatewayRoutes(): Promise<void> {
		if (!viewedGateway?.meshName || !viewedGateway.virtualGatewayName) return;
		gatewayRoutesLoading = true;
		try {
			const resp = await client().send(
				new ListGatewayRoutesCommand({ meshName: viewedGateway.meshName, virtualGatewayName: viewedGateway.virtualGatewayName })
			);
			gatewayRoutes = resp.gatewayRoutes ?? [];
		} catch (e) {
			toast.error('Failed to load gateway routes: ' + describeError(e));
		} finally {
			gatewayRoutesLoading = false;
		}
	}

	async function openGatewayDetail(g: VirtualGatewayRef): Promise<void> {
		viewedGateway = null;
		gatewayDetailError = null;
		gatewayRoutes = [];
		showAddGatewayRoute = false;
		expandedGatewayRoute = null;
		gatewayDetailModal?.open();
		if (!g.meshName || !g.virtualGatewayName) return;
		gatewayDetailLoading = true;
		try {
			const resp = await client().send(
				new DescribeVirtualGatewayCommand({ meshName: g.meshName, virtualGatewayName: g.virtualGatewayName })
			);
			viewedGateway = resp.virtualGateway ?? null;
			await loadGatewayRoutes();
		} catch (e) {
			gatewayDetailError = describeError(e);
		} finally {
			gatewayDetailLoading = false;
		}
	}

	let editGatewayModal = $state<Modal | null>(null);
	let editingGateway = $state(false);
	let editGatewayError = $state<string | null>(null);
	let editGatewayName = $state('');
	let editGatewaySpecJson = $state('');

	function openEditGatewayModal(g: VirtualGatewayData): void {
		editGatewayError = null;
		editGatewayName = g.virtualGatewayName ?? '';
		editGatewaySpecJson = g.spec ? JSON.stringify(g.spec, null, 2) : '';
		editGatewayModal?.open();
	}

	async function submitEditGateway(): Promise<void> {
		if (!editGatewayName || !selectedMeshName) return;
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(editGatewaySpecJson);
		} catch {
			editGatewayError = 'Spec must be valid JSON.';
			return;
		}
		editingGateway = true;
		editGatewayError = null;
		try {
			const resp = await client().send(
				new UpdateVirtualGatewayCommand({
					meshName: selectedMeshName,
					virtualGatewayName: editGatewayName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Virtual gateway updated');
			editGatewayModal?.close();
			await tabLoader.refresh('gateways');
			viewedGateway = resp.virtualGateway ?? viewedGateway;
		} catch (e) {
			const msg = describeError(e);
			editGatewayError = msg;
			toast.error(msg);
		} finally {
			editingGateway = false;
		}
	}

	async function deleteGateway(g: VirtualGatewayRef | VirtualGatewayData): Promise<void> {
		if (!g.meshName || !g.virtualGatewayName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete virtual gateway',
			message: `Delete virtual gateway "${g.virtualGatewayName}"? Its gateway routes must be deleted first.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteVirtualGatewayCommand({ meshName: g.meshName, virtualGatewayName: g.virtualGatewayName }));
			toast.success('Virtual gateway deleted');
			gatewayDetailModal?.close();
			await tabLoader.refresh('gateways');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// -- Gateway Routes, nested inside the virtual gateway detail modal --
	let showAddGatewayRoute = $state(false);
	let creatingGatewayRoute = $state(false);
	let createGatewayRouteError = $state<string | null>(null);
	let newGatewayRouteName = $state('');
	let newGatewayRouteSpecJson = $state('');
	let expandedGatewayRoute = $state<string | null>(null);
	let expandedGatewayRouteData = $state<GatewayRouteData | null>(null);

	function openAddGatewayRoute(): void {
		createGatewayRouteError = null;
		newGatewayRouteName = '';
		newGatewayRouteSpecJson = '';
		showAddGatewayRoute = true;
	}

	async function submitCreateGatewayRoute(): Promise<void> {
		if (!viewedGateway?.meshName || !viewedGateway.virtualGatewayName) return;
		if (!newGatewayRouteName) {
			createGatewayRouteError = 'Gateway route name is required.';
			return;
		}
		let spec: Record<string, unknown>;
		try {
			spec = parseSpec(newGatewayRouteSpecJson);
		} catch {
			createGatewayRouteError = 'Spec must be valid JSON.';
			return;
		}
		creatingGatewayRoute = true;
		createGatewayRouteError = null;
		try {
			await client().send(
				new CreateGatewayRouteCommand({
					meshName: viewedGateway.meshName,
					virtualGatewayName: viewedGateway.virtualGatewayName,
					gatewayRouteName: newGatewayRouteName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					spec: spec as any
				})
			);
			toast.success('Gateway route created');
			showAddGatewayRoute = false;
			await loadGatewayRoutes();
		} catch (e) {
			const msg = describeError(e);
			createGatewayRouteError = msg;
			toast.error(msg);
		} finally {
			creatingGatewayRoute = false;
		}
	}

	async function toggleGatewayRouteDetail(g: GatewayRouteRef): Promise<void> {
		const key = g.gatewayRouteName ?? '';
		if (expandedGatewayRoute === key) {
			expandedGatewayRoute = null;
			expandedGatewayRouteData = null;
			return;
		}
		expandedGatewayRoute = key;
		expandedGatewayRouteData = null;
		if (!g.meshName || !g.virtualGatewayName || !g.gatewayRouteName) return;
		try {
			const resp = await client().send(
				new DescribeGatewayRouteCommand({
					meshName: g.meshName,
					virtualGatewayName: g.virtualGatewayName,
					gatewayRouteName: g.gatewayRouteName
				})
			);
			expandedGatewayRouteData = resp.gatewayRoute ?? null;
		} catch (e) {
			toast.error('Failed to load gateway route: ' + describeError(e));
		}
	}

	async function deleteGatewayRoute(g: GatewayRouteRef): Promise<void> {
		if (!g.meshName || !g.virtualGatewayName || !g.gatewayRouteName) return;
		const confirmed = await confirmDestructive({
			title: 'Delete gateway route',
			message: `Delete gateway route "${g.gatewayRouteName}"?`
		});
		if (!confirmed) return;
		try {
			await client().send(
				new DeleteGatewayRouteCommand({
					meshName: g.meshName,
					virtualGatewayName: g.virtualGatewayName,
					gatewayRouteName: g.gatewayRouteName
				})
			);
			toast.success('Gateway route deleted');
			if (expandedGatewayRoute === g.gatewayRouteName) {
				expandedGatewayRoute = null;
				expandedGatewayRouteData = null;
			}
			await loadGatewayRoutes();
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Share2}
		title="AWS App Mesh"
		description="Service mesh for microservice networking"
		onRefresh={handleRefresh}
		color="purple"
	>
		{#snippet actions()}
			{#if activeTab === 'meshes'}
				<button onclick={openCreateMeshModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 text-sm">
					<Plus class="w-4 h-4" /> Create mesh
				</button>
			{:else if activeTab === 'nodes'}
				<button onclick={openCreateNodeModal} disabled={!selectedMeshName} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50 text-sm">
					<Plus class="w-4 h-4" /> Create virtual node
				</button>
			{:else if activeTab === 'routers'}
				<button onclick={openCreateRouterModal} disabled={!selectedMeshName} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50 text-sm">
					<Plus class="w-4 h-4" /> Create virtual router
				</button>
			{:else if activeTab === 'services'}
				<button onclick={openCreateServiceModal} disabled={!selectedMeshName} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50 text-sm">
					<Plus class="w-4 h-4" /> Create virtual service
				</button>
			{:else if activeTab === 'gateways'}
				<button onclick={openCreateGatewayModal} disabled={!selectedMeshName} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50 text-sm">
					<Plus class="w-4 h-4" /> Create virtual gateway
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="purple" />
			<div class="flex items-center gap-3 flex-wrap">
				{#if activeTab !== 'meshes'}
					<label class="text-xs text-gray-500 dark:text-gray-400 flex items-center gap-2">
						Mesh:
						<select
							bind:value={selectedMeshName}
							onchange={changeScopeMesh}
							class="text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white px-2 py-1.5"
						>
							<option value="">Select a mesh…</option>
							{#each meshes as m (m.meshName)}
								<option value={m.meshName}>{m.meshName}</option>
							{/each}
						</select>
					</label>
				{/if}
				<SearchInput bind:value={searchQuery} />
			</div>
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'meshes'}
				{#snippet meshActionsCell(m: MeshRef)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openMeshDetail(m)} title="View" aria-label="View mesh {m.meshName}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteMesh(m)} title="Delete" aria-label="Delete mesh {m.meshName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const meshColumns = defineColumns<MeshRef>([
					{ key: 'meshName', label: 'Name' },
					{ key: 'resourceOwner', label: 'Owner' },
					{ key: 'version', label: 'Version' },
					{ key: 'actions', label: '', render: meshActionsCell }
				])}
				<DataTable rows={filteredMeshes} rowKey={(m) => m.meshName ?? ''} columns={meshColumns} loading={tabLoader.isLoading('meshes')} emptyMessage="No meshes found" />
			{:else if activeTab === 'nodes'}
				{#if !selectedMeshName}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a mesh above to view its virtual nodes</div>
				{:else}
					{#snippet nodeActionsCell(n: VirtualNodeRef)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openNodeDetail(n)} title="View" aria-label="View virtual node {n.virtualNodeName}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => deleteNode(n)} title="Delete" aria-label="Delete virtual node {n.virtualNodeName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const nodeColumns = defineColumns<VirtualNodeRef>([
						{ key: 'virtualNodeName', label: 'Name' },
						{ key: 'meshName', label: 'Mesh' },
						{ key: 'actions', label: '', render: nodeActionsCell }
					])}
					<DataTable rows={filteredNodes} rowKey={(n) => n.virtualNodeName ?? ''} columns={nodeColumns} loading={tabLoader.isLoading('nodes')} emptyMessage="No virtual nodes found" />
				{/if}
			{:else if activeTab === 'routers'}
				{#if !selectedMeshName}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a mesh above to view its virtual routers</div>
				{:else}
					{#snippet routerActionsCell(r: VirtualRouterRef)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openRouterDetail(r)} title="View" aria-label="View virtual router {r.virtualRouterName}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => deleteRouter(r)} title="Delete" aria-label="Delete virtual router {r.virtualRouterName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const routerColumns = defineColumns<VirtualRouterRef>([
						{ key: 'virtualRouterName', label: 'Name' },
						{ key: 'meshName', label: 'Mesh' },
						{ key: 'actions', label: '', render: routerActionsCell }
					])}
					<DataTable rows={filteredRouters} rowKey={(r) => r.virtualRouterName ?? ''} columns={routerColumns} loading={tabLoader.isLoading('routers')} emptyMessage="No virtual routers found" />
				{/if}
			{:else if activeTab === 'services'}
				{#if !selectedMeshName}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a mesh above to view its virtual services</div>
				{:else}
					{#snippet serviceActionsCell(s: VirtualServiceRef)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openServiceDetail(s)} title="View" aria-label="View virtual service {s.virtualServiceName}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => deleteService(s)} title="Delete" aria-label="Delete virtual service {s.virtualServiceName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const serviceColumns = defineColumns<VirtualServiceRef>([
						{ key: 'virtualServiceName', label: 'Name' },
						{ key: 'meshName', label: 'Mesh' },
						{ key: 'actions', label: '', render: serviceActionsCell }
					])}
					<DataTable rows={filteredServices} rowKey={(s) => s.virtualServiceName ?? ''} columns={serviceColumns} loading={tabLoader.isLoading('services')} emptyMessage="No virtual services found" />
				{/if}
			{:else if activeTab === 'gateways'}
				{#if !selectedMeshName}
					<div class="text-center py-8 text-gray-500 dark:text-gray-400">Select a mesh above to view its virtual gateways</div>
				{:else}
					{#snippet gatewayActionsCell(g: VirtualGatewayRef)}
						<div class="flex items-center gap-2 justify-end">
							<button onclick={() => openGatewayDetail(g)} title="View" aria-label="View virtual gateway {g.virtualGatewayName}" class="text-gray-400 hover:text-purple-500"><Eye class="w-4 h-4" /></button>
							<button onclick={() => deleteGateway(g)} title="Delete" aria-label="Delete virtual gateway {g.virtualGatewayName}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
						</div>
					{/snippet}
					{@const gatewayColumns = defineColumns<VirtualGatewayRef>([
						{ key: 'virtualGatewayName', label: 'Name' },
						{ key: 'meshName', label: 'Mesh' },
						{ key: 'actions', label: '', render: gatewayActionsCell }
					])}
					<DataTable rows={filteredGateways} rowKey={(g) => g.virtualGatewayName ?? ''} columns={gatewayColumns} loading={tabLoader.isLoading('gateways')} emptyMessage="No virtual gateways found" />
				{/if}
			{/if}
		</div>
	</div>
</div>

<!-- Create Mesh -->
<Modal bind:this={createMeshModal} title="Create Mesh">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mesh-name" class="text-sm text-slate-600 dark:text-slate-300">Mesh name</label>
				<input id="mesh-name" bind:value={newMeshName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mesh-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON, optional)</label>
				<textarea id="mesh-spec" bind:value={newMeshSpecJson} rows={4} placeholder={'{\n  "egressFilter": { "type": "ALLOW_ALL" }\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createMeshError}
				<p class="text-sm text-red-600 dark:text-red-400">{createMeshError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createMeshModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateMesh} disabled={creatingMesh} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingMesh ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Mesh detail -->
<Modal bind:this={meshDetailModal} title="Mesh">
	{#snippet children()}
		{#if meshDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if meshDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{meshDetailError}</p>
		{:else if viewedMesh}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedMesh.meshName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedMesh.metadata?.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedMesh.status?.status ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Version</dt><dd class="text-slate-900 dark:text-white">{viewedMesh.metadata?.version ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Created</dt><dd class="text-slate-900 dark:text-white">{formatDate(viewedMesh.metadata?.createdAt)}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Spec</dt>
					<dd class="text-slate-900 dark:text-white"><pre class="mt-1 max-h-40 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedMesh.spec ?? {}, null, 2)}</pre></dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => meshDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedMesh}
			<button type="button" onclick={() => viewedMesh && openEditMeshModal(viewedMesh)} class="flex items-center gap-2 rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedMesh && deleteMesh(viewedMesh)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Mesh -->
<Modal bind:this={editMeshModal} title="Edit Mesh">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mesh-edit-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="mesh-edit-spec" bind:value={editMeshSpecJson} rows={5} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if editMeshError}
				<p class="text-sm text-red-600 dark:text-red-400">{editMeshError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editMeshModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditMesh} disabled={editingMesh} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{editingMesh ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Virtual Node -->
<Modal bind:this={createNodeModal} title="Create Virtual Node">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="node-name" class="text-sm text-slate-600 dark:text-slate-300">Virtual node name</label>
				<input id="node-name" bind:value={newNodeName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="node-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="node-spec" bind:value={newNodeSpecJson} rows={5} placeholder={'{\n  "listeners": [{ "portMapping": { "port": 8080, "protocol": "http" } }]\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createNodeError}
				<p class="text-sm text-red-600 dark:text-red-400">{createNodeError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createNodeModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateNode} disabled={creatingNode} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingNode ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Virtual Node detail -->
<Modal bind:this={nodeDetailModal} title="Virtual Node">
	{#snippet children()}
		{#if nodeDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if nodeDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{nodeDetailError}</p>
		{:else if viewedNode}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedNode.virtualNodeName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Mesh</dt><dd class="text-slate-900 dark:text-white">{viewedNode.meshName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedNode.metadata?.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedNode.status?.status ?? '—'}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Spec</dt>
					<dd class="text-slate-900 dark:text-white"><pre class="mt-1 max-h-40 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedNode.spec ?? {}, null, 2)}</pre></dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => nodeDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedNode}
			<button type="button" onclick={() => viewedNode && openEditNodeModal(viewedNode)} class="flex items-center gap-2 rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedNode && deleteNode(viewedNode)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Virtual Node -->
<Modal bind:this={editNodeModal} title="Edit Virtual Node">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="node-edit-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="node-edit-spec" bind:value={editNodeSpecJson} rows={5} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if editNodeError}
				<p class="text-sm text-red-600 dark:text-red-400">{editNodeError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editNodeModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditNode} disabled={editingNode} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{editingNode ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Virtual Service -->
<Modal bind:this={createServiceModal} title="Create Virtual Service">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="service-name" class="text-sm text-slate-600 dark:text-slate-300">Virtual service name</label>
				<input id="service-name" bind:value={newServiceName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="service-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="service-spec" bind:value={newServiceSpecJson} rows={5} placeholder={'{\n  "provider": { "virtualRouter": { "virtualRouterName": "router-a" } }\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createServiceError}
				<p class="text-sm text-red-600 dark:text-red-400">{createServiceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createServiceModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateService} disabled={creatingService} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingService ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Virtual Service detail -->
<Modal bind:this={serviceDetailModal} title="Virtual Service">
	{#snippet children()}
		{#if serviceDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if serviceDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{serviceDetailError}</p>
		{:else if viewedService}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedService.virtualServiceName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Mesh</dt><dd class="text-slate-900 dark:text-white">{viewedService.meshName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedService.metadata?.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedService.status?.status ?? '—'}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Spec</dt>
					<dd class="text-slate-900 dark:text-white"><pre class="mt-1 max-h-40 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedService.spec ?? {}, null, 2)}</pre></dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => serviceDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedService}
			<button type="button" onclick={() => viewedService && openEditServiceModal(viewedService)} class="flex items-center gap-2 rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedService && deleteService(viewedService)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Virtual Service -->
<Modal bind:this={editServiceModal} title="Edit Virtual Service">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="service-edit-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="service-edit-spec" bind:value={editServiceSpecJson} rows={5} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if editServiceError}
				<p class="text-sm text-red-600 dark:text-red-400">{editServiceError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editServiceModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditService} disabled={editingService} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{editingService ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Virtual Router -->
<Modal bind:this={createRouterModal} title="Create Virtual Router">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="router-name" class="text-sm text-slate-600 dark:text-slate-300">Virtual router name</label>
				<input id="router-name" bind:value={newRouterName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="router-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="router-spec" bind:value={newRouterSpecJson} rows={5} placeholder={'{\n  "listeners": [{ "portMapping": { "port": 8080, "protocol": "http" } }]\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createRouterError}
				<p class="text-sm text-red-600 dark:text-red-400">{createRouterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createRouterModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateRouter} disabled={creatingRouter} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingRouter ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Virtual Router detail (with nested Routes) -->
<Modal bind:this={routerDetailModal} title="Virtual Router">
	{#snippet children()}
		{#if routerDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if routerDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{routerDetailError}</p>
		{:else if viewedRouter}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedRouter.virtualRouterName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Mesh</dt><dd class="text-slate-900 dark:text-white">{viewedRouter.meshName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedRouter.metadata?.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedRouter.status?.status ?? '—'}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Spec</dt>
					<dd class="text-slate-900 dark:text-white"><pre class="mt-1 max-h-32 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedRouter.spec ?? {}, null, 2)}</pre></dd>
				</div>
				<div>
					<div class="flex items-center justify-between">
						<dt class="text-slate-500 dark:text-slate-400">Routes</dt>
						<button type="button" onclick={openAddRoute} class="flex items-center gap-1 text-xs text-purple-600 hover:text-purple-800 dark:text-purple-400"><Plus class="w-3 h-3" /> Add route</button>
					</div>
					<dd class="text-slate-900 dark:text-white mt-1">
						{#if showAddRoute}
							<div class="mb-3 space-y-2 rounded-lg border border-slate-200 dark:border-slate-600 p-3">
								<div>
									<label for="route-name" class="text-xs text-slate-600 dark:text-slate-300">Route name</label>
									<input id="route-name" bind:value={newRouteName} class="mt-1 w-full px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
								</div>
								<div>
									<label for="route-spec" class="text-xs text-slate-600 dark:text-slate-300">Spec (JSON)</label>
									<textarea id="route-spec" bind:value={newRouteSpecJson} rows={3} placeholder={'{\n  "httpRoute": { "action": { "weightedTargets": [{ "virtualNode": "node-a", "weight": 1 }] }, "match": { "prefix": "/" } }\n}'} class="mt-1 w-full px-2 py-1.5 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
								</div>
								{#if createRouteError}
									<p class="text-xs text-red-600 dark:text-red-400">{createRouteError}</p>
								{/if}
								<div class="flex justify-end gap-2">
									<button type="button" onclick={() => (showAddRoute = false)} class="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
									<button type="button" onclick={submitCreateRoute} disabled={creatingRoute} class="rounded-lg bg-purple-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingRoute ? 'Creating…' : 'Create route'}</button>
								</div>
							</div>
						{/if}
						{#if routerRoutesLoading}
							<p class="text-xs text-slate-500 dark:text-slate-400">Loading routes…</p>
						{:else if routerRoutes.length === 0}
							<p class="text-xs text-slate-500 dark:text-slate-400">No routes</p>
						{:else}
							<ul class="space-y-1">
								{#each routerRoutes as route (route.routeName)}
									<li class="rounded-lg bg-gray-50 dark:bg-slate-700/50">
										<div class="flex items-center justify-between gap-2 p-2">
											<button type="button" onclick={() => toggleRouteDetail(route)} class="text-left text-sm font-mono truncate">{route.routeName}</button>
											<button type="button" onclick={() => deleteRoute(route)} title="Delete route" aria-label="Delete route {route.routeName}" class="shrink-0 text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
										</div>
										{#if expandedRoute === route.routeName}
											<div class="px-2 pb-2">
												{#if expandedRouteData}
													<pre class="max-h-32 overflow-auto rounded-lg bg-gray-100 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(expandedRouteData.spec ?? {}, null, 2)}</pre>
												{:else}
													<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
												{/if}
											</div>
										{/if}
									</li>
								{/each}
							</ul>
						{/if}
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => routerDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedRouter}
			<button type="button" onclick={() => viewedRouter && openEditRouterModal(viewedRouter)} class="flex items-center gap-2 rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedRouter && deleteRouter(viewedRouter)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Virtual Router -->
<Modal bind:this={editRouterModal} title="Edit Virtual Router">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="router-edit-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="router-edit-spec" bind:value={editRouterSpecJson} rows={5} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if editRouterError}
				<p class="text-sm text-red-600 dark:text-red-400">{editRouterError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editRouterModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditRouter} disabled={editingRouter} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{editingRouter ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Virtual Gateway -->
<Modal bind:this={createGatewayModal} title="Create Virtual Gateway">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="gateway-name" class="text-sm text-slate-600 dark:text-slate-300">Virtual gateway name</label>
				<input id="gateway-name" bind:value={newGatewayName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="gateway-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="gateway-spec" bind:value={newGatewaySpecJson} rows={5} placeholder={'{\n  "listeners": [{ "portMapping": { "port": 8080, "protocol": "http" } }]\n}'} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createGatewayError}
				<p class="text-sm text-red-600 dark:text-red-400">{createGatewayError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createGatewayModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateGateway} disabled={creatingGateway} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingGateway ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Virtual Gateway detail (with nested Gateway Routes) -->
<Modal bind:this={gatewayDetailModal} title="Virtual Gateway">
	{#snippet children()}
		{#if gatewayDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if gatewayDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{gatewayDetailError}</p>
		{:else if viewedGateway}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedGateway.virtualGatewayName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Mesh</dt><dd class="text-slate-900 dark:text-white">{viewedGateway.meshName ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedGateway.metadata?.arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Status</dt><dd class="text-slate-900 dark:text-white">{viewedGateway.status?.status ?? '—'}</dd></div>
				<div>
					<dt class="text-slate-500 dark:text-slate-400">Spec</dt>
					<dd class="text-slate-900 dark:text-white"><pre class="mt-1 max-h-32 overflow-auto rounded-lg bg-gray-50 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(viewedGateway.spec ?? {}, null, 2)}</pre></dd>
				</div>
				<div>
					<div class="flex items-center justify-between">
						<dt class="text-slate-500 dark:text-slate-400">Gateway routes</dt>
						<button type="button" onclick={openAddGatewayRoute} class="flex items-center gap-1 text-xs text-purple-600 hover:text-purple-800 dark:text-purple-400"><Plus class="w-3 h-3" /> Add gateway route</button>
					</div>
					<dd class="text-slate-900 dark:text-white mt-1">
						{#if showAddGatewayRoute}
							<div class="mb-3 space-y-2 rounded-lg border border-slate-200 dark:border-slate-600 p-3">
								<div>
									<label for="gw-route-name" class="text-xs text-slate-600 dark:text-slate-300">Gateway route name</label>
									<input id="gw-route-name" bind:value={newGatewayRouteName} class="mt-1 w-full px-2 py-1.5 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
								</div>
								<div>
									<label for="gw-route-spec" class="text-xs text-slate-600 dark:text-slate-300">Spec (JSON)</label>
									<textarea id="gw-route-spec" bind:value={newGatewayRouteSpecJson} rows={3} placeholder={'{\n  "httpRoute": { "action": { "target": { "virtualService": { "virtualServiceName": "svc-a" } } }, "match": { "prefix": { "defaultPrefix": "ENABLED" } } }\n}'} class="mt-1 w-full px-2 py-1.5 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
								</div>
								{#if createGatewayRouteError}
									<p class="text-xs text-red-600 dark:text-red-400">{createGatewayRouteError}</p>
								{/if}
								<div class="flex justify-end gap-2">
									<button type="button" onclick={() => (showAddGatewayRoute = false)} class="rounded-lg border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
									<button type="button" onclick={submitCreateGatewayRoute} disabled={creatingGatewayRoute} class="rounded-lg bg-purple-600 px-3 py-1.5 text-xs font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{creatingGatewayRoute ? 'Creating…' : 'Create gateway route'}</button>
								</div>
							</div>
						{/if}
						{#if gatewayRoutesLoading}
							<p class="text-xs text-slate-500 dark:text-slate-400">Loading gateway routes…</p>
						{:else if gatewayRoutes.length === 0}
							<p class="text-xs text-slate-500 dark:text-slate-400">No gateway routes</p>
						{:else}
							<ul class="space-y-1">
								{#each gatewayRoutes as gr (gr.gatewayRouteName)}
									<li class="rounded-lg bg-gray-50 dark:bg-slate-700/50">
										<div class="flex items-center justify-between gap-2 p-2">
											<button type="button" onclick={() => toggleGatewayRouteDetail(gr)} class="text-left text-sm font-mono truncate">{gr.gatewayRouteName}</button>
											<button type="button" onclick={() => deleteGatewayRoute(gr)} title="Delete gateway route" aria-label="Delete gateway route {gr.gatewayRouteName}" class="shrink-0 text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
										</div>
										{#if expandedGatewayRoute === gr.gatewayRouteName}
											<div class="px-2 pb-2">
												{#if expandedGatewayRouteData}
													<pre class="max-h-32 overflow-auto rounded-lg bg-gray-100 dark:bg-slate-900 p-2 text-xs">{JSON.stringify(expandedGatewayRouteData.spec ?? {}, null, 2)}</pre>
												{:else}
													<p class="text-xs text-slate-500 dark:text-slate-400">Loading…</p>
												{/if}
											</div>
										{/if}
									</li>
								{/each}
							</ul>
						{/if}
					</dd>
				</div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => gatewayDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedGateway}
			<button type="button" onclick={() => viewedGateway && openEditGatewayModal(viewedGateway)} class="flex items-center gap-2 rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedGateway && deleteGateway(viewedGateway)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Virtual Gateway -->
<Modal bind:this={editGatewayModal} title="Edit Virtual Gateway">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="gateway-edit-spec" class="text-sm text-slate-600 dark:text-slate-300">Spec (JSON)</label>
				<textarea id="gateway-edit-spec" bind:value={editGatewaySpecJson} rows={5} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if editGatewayError}
				<p class="text-sm text-red-600 dark:text-red-400">{editGatewayError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editGatewayModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditGateway} disabled={editingGateway} class="rounded-lg bg-purple-600 px-4 py-2 text-sm font-semibold text-white hover:bg-purple-700 disabled:opacity-50">{editingGateway ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>
