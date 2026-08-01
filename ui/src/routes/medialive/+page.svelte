<script lang="ts">
	// MediaLive's floor keeps the four tabs the read-only baseline already
	// had (Channels, Inputs, Input Security Groups, Multiplexes) and gives
	// each full CRUD plus its real lifecycle actions. Channels and
	// Multiplexes aren't just create/delete -- they have a Start/Stop
	// lifecycle (StartChannel/StopChannel, StartMultiplex/StopMultiplex)
	// that only applies from specific states (IDLE->start, RUNNING->stop),
	// modeled here as detail-view buttons gated on the resource's current
	// State rather than always-available actions.
	//
	// EncoderSettings/Destinations/InputAttachments (Channel) and
	// Sources/Destinations (Input) are deeply nested SDK types the real
	// client itself validates before sending (AudioDescriptions/
	// VideoDescriptions/OutputGroups/TimecodeConfig etc, confirmed against
	// medialive's own validators.go) -- modeling a structured sub-form for
	// them would imply a level of validation this dashboard doesn't do
	// anywhere else. They're exposed as optional raw-JSON fields instead,
	// consistent with how this dashboard treats other opaque nested
	// AWS config blocks (see mediapackage's HlsPackage editor).
	import { untrack } from 'svelte';
	import { confirmDestructive } from '$lib/confirm-dialog';
	import { onRegionChange, regionalClient } from '$lib/region-effect.svelte';
	import { getMediaLiveClient } from '$lib/aws-client';
	import { createTabLoader } from '$lib/tab-loader.svelte';
	import PageHeader from '$lib/components/PageHeader.svelte';
	import Tabs from '$lib/components/Tabs.svelte';
	import type { Tab as TabDef } from '$lib/components/Tabs.svelte';
	import SearchInput from '$lib/components/SearchInput.svelte';
	import DataTable from '$lib/components/DataTable.svelte';
	import { defineColumns } from '$lib/components/data-table';
	import Modal from '$lib/components/Modal.svelte';
	import {
		ListChannelsCommand,
		DescribeChannelCommand,
		CreateChannelCommand,
		UpdateChannelCommand,
		DeleteChannelCommand,
		StartChannelCommand,
		StopChannelCommand,
		ListInputsCommand,
		DescribeInputCommand,
		CreateInputCommand,
		UpdateInputCommand,
		DeleteInputCommand,
		ListInputSecurityGroupsCommand,
		DescribeInputSecurityGroupCommand,
		CreateInputSecurityGroupCommand,
		UpdateInputSecurityGroupCommand,
		DeleteInputSecurityGroupCommand,
		ListMultiplexesCommand,
		DescribeMultiplexCommand,
		CreateMultiplexCommand,
		UpdateMultiplexCommand,
		DeleteMultiplexCommand,
		StartMultiplexCommand,
		StopMultiplexCommand,
		ChannelClass,
		InputType,
		type ChannelSummary,
		type DescribeChannelResponse,
		type Input,
		type DescribeInputResponse,
		type InputSecurityGroup,
		type MultiplexSummary,
		type DescribeMultiplexResponse
	} from '@aws-sdk/client-medialive';
	import { toast } from 'svelte-sonner';
	import { Radio, Plus, Trash2, Eye, Pencil, Play, Square } from 'lucide-svelte';

	const client = regionalClient(getMediaLiveClient);

	type TabId = 'channels' | 'inputs' | 'inputsg' | 'multiplexes';

	const tabs: TabDef[] = [
		{ id: 'channels', label: 'Channels' },
		{ id: 'inputs', label: 'Inputs' },
		{ id: 'inputsg', label: 'Input Security Groups' },
		{ id: 'multiplexes', label: 'Multiplexes' }
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

	const activeStates = new Set(['RUNNING', 'IDLE', 'IN_USE']);
	function statusClass(s: unknown): string {
		if (s === 'CREATE_FAILED' || s === 'UPDATE_FAILED') return 'bg-red-100 dark:bg-red-900/30 text-red-700 dark:text-red-400';
		return activeStates.has(String(s))
			? 'bg-green-100 dark:bg-green-900/30 text-green-700 dark:text-green-400'
			: 'bg-gray-100 dark:bg-gray-700 text-gray-600 dark:text-gray-400';
	}

	function parseJsonField(json: string): unknown {
		if (!json.trim()) return undefined;
		return JSON.parse(json);
	}

	let activeTab = $state<TabId>('channels');
	let searchQuery = $state('');

	// --- Channels ---
	let channels = $state<ChannelSummary[]>([]);
	async function fetchChannels(): Promise<void> {
		const resp = await client().send(new ListChannelsCommand({}));
		channels = resp.Channels ?? [];
	}

	// --- Inputs ---
	let inputs = $state<Input[]>([]);
	async function fetchInputs(): Promise<void> {
		const resp = await client().send(new ListInputsCommand({}));
		inputs = resp.Inputs ?? [];
	}

	// --- Input Security Groups ---
	let inputSecurityGroups = $state<InputSecurityGroup[]>([]);
	async function fetchInputSecurityGroups(): Promise<void> {
		const resp = await client().send(new ListInputSecurityGroupsCommand({}));
		inputSecurityGroups = resp.InputSecurityGroups ?? [];
	}

	// --- Multiplexes ---
	let multiplexes = $state<MultiplexSummary[]>([]);
	async function fetchMultiplexes(): Promise<void> {
		const resp = await client().send(new ListMultiplexesCommand({}));
		multiplexes = resp.Multiplexes ?? [];
	}

	const tabLoader = createTabLoader<TabId>({
		channels: () => fetchChannels().catch(rethrowDescribed),
		inputs: () => fetchInputs().catch(rethrowDescribed),
		inputsg: () => fetchInputSecurityGroups().catch(rethrowDescribed),
		multiplexes: () => fetchMultiplexes().catch(rethrowDescribed)
	});

	function switchTab(id: string): void {
		activeTab = id as TabId;
		searchQuery = '';
		tabLoader.load(activeTab);
	}
	function handleRefresh(): void {
		tabLoader.refresh(activeTab);
	}

	onRegionChange(() => {
		viewedChannel = null;
		viewedInput = null;
		viewedInputSg = null;
		viewedMultiplex = null;
		const tab = untrack(() => activeTab);
		tabLoader.refresh(tab);
	});

	const activeTabError = $derived(tabLoader.getError(activeTab));

	const filteredChannels = $derived(
		channels.filter((c) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (c.Name ?? '').toLowerCase().includes(q) || (c.Id ?? '').toLowerCase().includes(q);
		})
	);
	const filteredInputs = $derived(
		inputs.filter((i) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (i.Name ?? '').toLowerCase().includes(q) || (i.Id ?? '').toLowerCase().includes(q) || (i.Type ?? '').toLowerCase().includes(q);
		})
	);
	const filteredInputSgs = $derived(
		inputSecurityGroups.filter((g) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (g.Id ?? '').toLowerCase().includes(q) || (g.Arn ?? '').toLowerCase().includes(q);
		})
	);
	const filteredMultiplexes = $derived(
		multiplexes.filter((m) => {
			const q = searchQuery.toLowerCase();
			if (!q) return true;
			return (m.Name ?? '').toLowerCase().includes(q) || (m.Id ?? '').toLowerCase().includes(q);
		})
	);

	// ── Channels: create / detail / edit / delete / start / stop ────────────
	let createChannelModal = $state<Modal | null>(null);
	let creatingChannel = $state(false);
	let createChannelError = $state<string | null>(null);
	let newChannelName = $state('');
	let newChannelClass = $state<'STANDARD' | 'SINGLE_PIPELINE'>('STANDARD');
	let newChannelRoleArn = $state('');
	let newChannelEncoderSettingsJson = $state('');

	function openCreateChannelModal(): void {
		createChannelError = null;
		newChannelName = '';
		newChannelClass = 'STANDARD';
		newChannelRoleArn = '';
		newChannelEncoderSettingsJson = '';
		createChannelModal?.open();
	}

	async function submitCreateChannel(): Promise<void> {
		if (!newChannelName) {
			createChannelError = 'Name is required.';
			return;
		}
		let encoderSettings: unknown;
		try {
			encoderSettings = parseJsonField(newChannelEncoderSettingsJson);
		} catch {
			createChannelError = 'Encoder settings must be valid JSON.';
			return;
		}
		creatingChannel = true;
		createChannelError = null;
		try {
			await client().send(
				new CreateChannelCommand({
					Name: newChannelName,
					ChannelClass: newChannelClass,
					RoleArn: newChannelRoleArn || undefined,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					EncoderSettings: encoderSettings as any
				})
			);
			toast.success('Channel created');
			createChannelModal?.close();
			await tabLoader.refresh('channels');
		} catch (e) {
			const msg = describeError(e);
			createChannelError = msg;
			toast.error(msg);
		} finally {
			creatingChannel = false;
		}
	}

	let channelDetailModal = $state<Modal | null>(null);
	let viewedChannel = $state<DescribeChannelResponse | null>(null);
	let channelDetailLoading = $state(false);
	let channelDetailError = $state<string | null>(null);
	let channelActionError = $state<string | null>(null);

	async function openChannelDetail(c: ChannelSummary): Promise<void> {
		viewedChannel = null;
		channelDetailError = null;
		channelActionError = null;
		channelDetailModal?.open();
		if (!c.Id) return;
		channelDetailLoading = true;
		try {
			const resp = await client().send(new DescribeChannelCommand({ ChannelId: c.Id }));
			viewedChannel = resp;
		} catch (e) {
			channelDetailError = describeError(e);
		} finally {
			channelDetailLoading = false;
		}
	}

	async function refreshChannelDetail(): Promise<void> {
		if (!viewedChannel?.Id) return;
		const resp = await client().send(new DescribeChannelCommand({ ChannelId: viewedChannel.Id }));
		viewedChannel = resp;
	}

	let editChannelModal = $state<Modal | null>(null);
	let editingChannel = $state(false);
	let editChannelError = $state<string | null>(null);
	let editChannelId = $state('');
	let editChannelName = $state('');
	let editChannelRoleArn = $state('');

	function openEditChannelModal(c: DescribeChannelResponse): void {
		editChannelError = null;
		editChannelId = c.Id ?? '';
		editChannelName = c.Name ?? '';
		editChannelRoleArn = c.RoleArn ?? '';
		editChannelModal?.open();
	}

	async function submitEditChannel(): Promise<void> {
		if (!editChannelId || !editChannelName) {
			editChannelError = 'Name is required.';
			return;
		}
		editingChannel = true;
		editChannelError = null;
		try {
			await client().send(
				new UpdateChannelCommand({ ChannelId: editChannelId, Name: editChannelName, RoleArn: editChannelRoleArn || undefined })
			);
			toast.success('Channel updated');
			editChannelModal?.close();
			await tabLoader.refresh('channels');
			await refreshChannelDetail();
		} catch (e) {
			const msg = describeError(e);
			editChannelError = msg;
			toast.error(msg);
		} finally {
			editingChannel = false;
		}
	}

	async function startChannel(): Promise<void> {
		if (!viewedChannel?.Id) return;
		channelActionError = null;
		try {
			await client().send(new StartChannelCommand({ ChannelId: viewedChannel.Id }));
			toast.success('Channel starting');
			await refreshChannelDetail();
			await tabLoader.refresh('channels');
		} catch (e) {
			const msg = describeError(e);
			channelActionError = msg;
			toast.error(msg);
		}
	}

	async function stopChannel(): Promise<void> {
		if (!viewedChannel?.Id) return;
		channelActionError = null;
		try {
			await client().send(new StopChannelCommand({ ChannelId: viewedChannel.Id }));
			toast.success('Channel stopping');
			await refreshChannelDetail();
			await tabLoader.refresh('channels');
		} catch (e) {
			const msg = describeError(e);
			channelActionError = msg;
			toast.error(msg);
		}
	}

	async function deleteChannel(c: ChannelSummary): Promise<void> {
		if (!c.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete channel',
			message: `Delete channel "${c.Name ?? c.Id}"? The channel must be stopped first.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteChannelCommand({ ChannelId: c.Id }));
			toast.success('Channel deleted');
			channelDetailModal?.close();
			await tabLoader.refresh('channels');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Inputs: create / detail / edit / delete ─────────────────────────────
	let createInputModal = $state<Modal | null>(null);
	let creatingInput = $state(false);
	let createInputError = $state<string | null>(null);
	let newInputName = $state('');
	let newInputType = $state<string>('URL_PULL');
	let newInputSecurityGroups = $state('');
	let newInputSourceUrl = $state('');

	function openCreateInputModal(): void {
		createInputError = null;
		newInputName = '';
		newInputType = 'URL_PULL';
		newInputSecurityGroups = '';
		newInputSourceUrl = '';
		createInputModal?.open();
	}

	async function submitCreateInput(): Promise<void> {
		if (!newInputName) {
			createInputError = 'Name is required.';
			return;
		}
		creatingInput = true;
		createInputError = null;
		try {
			const isPull = newInputType === 'URL_PULL' || newInputType === 'RTMP_PULL';
			await client().send(
				new CreateInputCommand({
					Name: newInputName,
					// eslint-disable-next-line @typescript-eslint/no-explicit-any
					Type: newInputType as any,
					InputSecurityGroups: newInputSecurityGroups
						? newInputSecurityGroups.split(',').map((s) => s.trim()).filter(Boolean)
						: undefined,
					Sources: isPull && newInputSourceUrl ? [{ Url: newInputSourceUrl }] : undefined,
					Destinations: !isPull && newInputSourceUrl ? [{ StreamName: newInputSourceUrl }] : undefined
				})
			);
			toast.success('Input created');
			createInputModal?.close();
			await tabLoader.refresh('inputs');
		} catch (e) {
			const msg = describeError(e);
			createInputError = msg;
			toast.error(msg);
		} finally {
			creatingInput = false;
		}
	}

	let inputDetailModal = $state<Modal | null>(null);
	let viewedInput = $state<DescribeInputResponse | null>(null);
	let inputDetailLoading = $state(false);
	let inputDetailError = $state<string | null>(null);

	async function openInputDetail(i: Input): Promise<void> {
		viewedInput = null;
		inputDetailError = null;
		inputDetailModal?.open();
		if (!i.Id) return;
		inputDetailLoading = true;
		try {
			const resp = await client().send(new DescribeInputCommand({ InputId: i.Id }));
			viewedInput = resp;
		} catch (e) {
			inputDetailError = describeError(e);
		} finally {
			inputDetailLoading = false;
		}
	}

	let editInputModal = $state<Modal | null>(null);
	let editingInput = $state(false);
	let editInputError = $state<string | null>(null);
	let editInputId = $state('');
	let editInputName = $state('');
	let editInputSecurityGroups = $state('');

	function openEditInputModal(i: DescribeInputResponse): void {
		editInputError = null;
		editInputId = i.Id ?? '';
		editInputName = i.Name ?? '';
		editInputSecurityGroups = (i.SecurityGroups ?? []).join(', ');
		editInputModal?.open();
	}

	async function submitEditInput(): Promise<void> {
		if (!editInputId || !editInputName) {
			editInputError = 'Name is required.';
			return;
		}
		editingInput = true;
		editInputError = null;
		try {
			await client().send(
				new UpdateInputCommand({
					InputId: editInputId,
					Name: editInputName,
					InputSecurityGroups: editInputSecurityGroups
						? editInputSecurityGroups.split(',').map((s) => s.trim()).filter(Boolean)
						: undefined
				})
			);
			toast.success('Input updated');
			editInputModal?.close();
			await tabLoader.refresh('inputs');
			const resp = await client().send(new DescribeInputCommand({ InputId: editInputId }));
			viewedInput = resp;
		} catch (e) {
			const msg = describeError(e);
			editInputError = msg;
			toast.error(msg);
		} finally {
			editingInput = false;
		}
	}

	async function deleteInput(i: Input): Promise<void> {
		if (!i.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete input',
			message: `Delete input "${i.Name ?? i.Id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteInputCommand({ InputId: i.Id }));
			toast.success('Input deleted');
			inputDetailModal?.close();
			await tabLoader.refresh('inputs');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Input Security Groups: create / detail / edit / delete ─────────────
	let createInputSgModal = $state<Modal | null>(null);
	let creatingInputSg = $state(false);
	let createInputSgError = $state<string | null>(null);
	let newInputSgCidrs = $state('');

	function openCreateInputSgModal(): void {
		createInputSgError = null;
		newInputSgCidrs = '';
		createInputSgModal?.open();
	}

	async function submitCreateInputSg(): Promise<void> {
		if (!newInputSgCidrs) {
			createInputSgError = 'At least one CIDR is required.';
			return;
		}
		creatingInputSg = true;
		createInputSgError = null;
		try {
			await client().send(
				new CreateInputSecurityGroupCommand({
					WhitelistRules: newInputSgCidrs.split(',').map((c) => ({ Cidr: c.trim() })).filter((r) => r.Cidr)
				})
			);
			toast.success('Input security group created');
			createInputSgModal?.close();
			await tabLoader.refresh('inputsg');
		} catch (e) {
			const msg = describeError(e);
			createInputSgError = msg;
			toast.error(msg);
		} finally {
			creatingInputSg = false;
		}
	}

	let inputSgDetailModal = $state<Modal | null>(null);
	let viewedInputSg = $state<InputSecurityGroup | null>(null);
	let inputSgDetailLoading = $state(false);
	let inputSgDetailError = $state<string | null>(null);

	async function openInputSgDetail(g: InputSecurityGroup): Promise<void> {
		viewedInputSg = null;
		inputSgDetailError = null;
		inputSgDetailModal?.open();
		if (!g.Id) return;
		inputSgDetailLoading = true;
		try {
			const resp = await client().send(new DescribeInputSecurityGroupCommand({ InputSecurityGroupId: g.Id }));
			viewedInputSg = resp;
		} catch (e) {
			inputSgDetailError = describeError(e);
		} finally {
			inputSgDetailLoading = false;
		}
	}

	let editInputSgModal = $state<Modal | null>(null);
	let editingInputSg = $state(false);
	let editInputSgError = $state<string | null>(null);
	let editInputSgId = $state('');
	let editInputSgCidrs = $state('');

	function openEditInputSgModal(g: InputSecurityGroup): void {
		editInputSgError = null;
		editInputSgId = g.Id ?? '';
		editInputSgCidrs = (g.WhitelistRules ?? []).map((r) => r.Cidr).filter(Boolean).join(', ');
		editInputSgModal?.open();
	}

	async function submitEditInputSg(): Promise<void> {
		if (!editInputSgId || !editInputSgCidrs) {
			editInputSgError = 'At least one CIDR is required.';
			return;
		}
		editingInputSg = true;
		editInputSgError = null;
		try {
			await client().send(
				new UpdateInputSecurityGroupCommand({
					InputSecurityGroupId: editInputSgId,
					WhitelistRules: editInputSgCidrs.split(',').map((c) => ({ Cidr: c.trim() })).filter((r) => r.Cidr)
				})
			);
			toast.success('Input security group updated');
			editInputSgModal?.close();
			await tabLoader.refresh('inputsg');
			const resp = await client().send(new DescribeInputSecurityGroupCommand({ InputSecurityGroupId: editInputSgId }));
			viewedInputSg = resp;
		} catch (e) {
			const msg = describeError(e);
			editInputSgError = msg;
			toast.error(msg);
		} finally {
			editingInputSg = false;
		}
	}

	async function deleteInputSg(g: InputSecurityGroup): Promise<void> {
		if (!g.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete input security group',
			message: `Delete input security group "${g.Id}"? This cannot be undone.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteInputSecurityGroupCommand({ InputSecurityGroupId: g.Id }));
			toast.success('Input security group deleted');
			inputSgDetailModal?.close();
			await tabLoader.refresh('inputsg');
		} catch (e) {
			toast.error(describeError(e));
		}
	}

	// ── Multiplexes: create / detail / edit / delete / start / stop ────────
	let createMultiplexModal = $state<Modal | null>(null);
	let creatingMultiplex = $state(false);
	let createMultiplexError = $state<string | null>(null);
	let newMultiplexName = $state('');
	let newMultiplexAzs = $state('');
	let newMultiplexBitrate = $state(0);
	let newMultiplexTsId = $state(0);

	function openCreateMultiplexModal(): void {
		createMultiplexError = null;
		newMultiplexName = '';
		newMultiplexAzs = '';
		newMultiplexBitrate = 0;
		newMultiplexTsId = 0;
		createMultiplexModal?.open();
	}

	async function submitCreateMultiplex(): Promise<void> {
		const azs = newMultiplexAzs.split(',').map((a) => a.trim()).filter(Boolean);
		if (!newMultiplexName || azs.length !== 2 || !newMultiplexBitrate || !newMultiplexTsId) {
			createMultiplexError = 'Name, exactly two availability zones, a bitrate, and a transport stream ID are required.';
			return;
		}
		creatingMultiplex = true;
		createMultiplexError = null;
		try {
			await client().send(
				new CreateMultiplexCommand({
					Name: newMultiplexName,
					AvailabilityZones: azs,
					MultiplexSettings: { TransportStreamBitrate: newMultiplexBitrate, TransportStreamId: newMultiplexTsId }
				})
			);
			toast.success('Multiplex created');
			createMultiplexModal?.close();
			await tabLoader.refresh('multiplexes');
		} catch (e) {
			const msg = describeError(e);
			createMultiplexError = msg;
			toast.error(msg);
		} finally {
			creatingMultiplex = false;
		}
	}

	let multiplexDetailModal = $state<Modal | null>(null);
	let viewedMultiplex = $state<DescribeMultiplexResponse | null>(null);
	let multiplexDetailLoading = $state(false);
	let multiplexDetailError = $state<string | null>(null);
	let multiplexActionError = $state<string | null>(null);

	async function openMultiplexDetail(m: MultiplexSummary): Promise<void> {
		viewedMultiplex = null;
		multiplexDetailError = null;
		multiplexActionError = null;
		multiplexDetailModal?.open();
		if (!m.Id) return;
		multiplexDetailLoading = true;
		try {
			const resp = await client().send(new DescribeMultiplexCommand({ MultiplexId: m.Id }));
			viewedMultiplex = resp;
		} catch (e) {
			multiplexDetailError = describeError(e);
		} finally {
			multiplexDetailLoading = false;
		}
	}

	async function refreshMultiplexDetail(): Promise<void> {
		if (!viewedMultiplex?.Id) return;
		const resp = await client().send(new DescribeMultiplexCommand({ MultiplexId: viewedMultiplex.Id }));
		viewedMultiplex = resp;
	}

	let editMultiplexModal = $state<Modal | null>(null);
	let editingMultiplex = $state(false);
	let editMultiplexError = $state<string | null>(null);
	let editMultiplexId = $state('');
	let editMultiplexName = $state('');
	let editMultiplexBitrate = $state(0);
	let editMultiplexTsId = $state(0);

	function openEditMultiplexModal(m: DescribeMultiplexResponse): void {
		editMultiplexError = null;
		editMultiplexId = m.Id ?? '';
		editMultiplexName = m.Name ?? '';
		editMultiplexBitrate = m.MultiplexSettings?.TransportStreamBitrate ?? 0;
		editMultiplexTsId = m.MultiplexSettings?.TransportStreamId ?? 0;
		editMultiplexModal?.open();
	}

	async function submitEditMultiplex(): Promise<void> {
		if (!editMultiplexId || !editMultiplexName) {
			editMultiplexError = 'Name is required.';
			return;
		}
		editingMultiplex = true;
		editMultiplexError = null;
		try {
			await client().send(
				new UpdateMultiplexCommand({
					MultiplexId: editMultiplexId,
					Name: editMultiplexName,
					MultiplexSettings: { TransportStreamBitrate: editMultiplexBitrate, TransportStreamId: editMultiplexTsId }
				})
			);
			toast.success('Multiplex updated');
			editMultiplexModal?.close();
			await tabLoader.refresh('multiplexes');
			await refreshMultiplexDetail();
		} catch (e) {
			const msg = describeError(e);
			editMultiplexError = msg;
			toast.error(msg);
		} finally {
			editingMultiplex = false;
		}
	}

	async function startMultiplex(): Promise<void> {
		if (!viewedMultiplex?.Id) return;
		multiplexActionError = null;
		try {
			await client().send(new StartMultiplexCommand({ MultiplexId: viewedMultiplex.Id }));
			toast.success('Multiplex starting');
			await refreshMultiplexDetail();
			await tabLoader.refresh('multiplexes');
		} catch (e) {
			const msg = describeError(e);
			multiplexActionError = msg;
			toast.error(msg);
		}
	}

	async function stopMultiplex(): Promise<void> {
		if (!viewedMultiplex?.Id) return;
		multiplexActionError = null;
		try {
			await client().send(new StopMultiplexCommand({ MultiplexId: viewedMultiplex.Id }));
			toast.success('Multiplex stopping');
			await refreshMultiplexDetail();
			await tabLoader.refresh('multiplexes');
		} catch (e) {
			const msg = describeError(e);
			multiplexActionError = msg;
			toast.error(msg);
		}
	}

	async function deleteMultiplex(m: MultiplexSummary): Promise<void> {
		if (!m.Id) return;
		const confirmed = await confirmDestructive({
			title: 'Delete multiplex',
			message: `Delete multiplex "${m.Name ?? m.Id}"? The multiplex must be stopped first.`
		});
		if (!confirmed) return;
		try {
			await client().send(new DeleteMultiplexCommand({ MultiplexId: m.Id }));
			toast.success('Multiplex deleted');
			multiplexDetailModal?.close();
			await tabLoader.refresh('multiplexes');
		} catch (e) {
			toast.error(describeError(e));
		}
	}
</script>

<div class="p-6 space-y-6">
	<PageHeader
		icon={Radio}
		title="AWS Elemental MediaLive"
		description="Live video processing"
		onRefresh={handleRefresh}
		color="red"
	>
		{#snippet actions()}
			{#if activeTab === 'channels'}
				<button onclick={openCreateChannelModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm">
					<Plus class="w-4 h-4" /> Create channel
				</button>
			{:else if activeTab === 'inputs'}
				<button onclick={openCreateInputModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm">
					<Plus class="w-4 h-4" /> Create input
				</button>
			{:else if activeTab === 'inputsg'}
				<button onclick={openCreateInputSgModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm">
					<Plus class="w-4 h-4" /> Create security group
				</button>
			{:else if activeTab === 'multiplexes'}
				<button onclick={openCreateMultiplexModal} class="flex items-center gap-2 px-3 py-2 rounded-lg bg-red-600 text-white hover:bg-red-700 text-sm">
					<Plus class="w-4 h-4" /> Create multiplex
				</button>
			{/if}
		{/snippet}
	</PageHeader>

	<div class="bg-white dark:bg-slate-800 rounded-lg border border-slate-200 dark:border-slate-700">
		<div class="p-4 border-b border-slate-200 dark:border-slate-700 flex flex-col sm:flex-row gap-3 justify-between">
			<Tabs {tabs} active={activeTab} onSelect={switchTab} color="red" />
			<SearchInput bind:value={searchQuery} />
		</div>

		<div class="p-4 space-y-4">
			{#if activeTabError}
				<div role="alert" class="rounded-lg border border-red-300 bg-red-50 dark:bg-red-900/20 dark:border-red-800 px-4 py-3 text-sm text-red-700 dark:text-red-300">
					<p class="font-medium">Failed to load data</p>
					<p>{activeTabError}</p>
				</div>
			{/if}

			{#if activeTab === 'channels'}
				{#snippet channelStateCell(c: ChannelSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(c.State)}">{c.State ?? '—'}</span>
				{/snippet}
				{#snippet channelActionsCell(c: ChannelSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openChannelDetail(c)} title="View" aria-label="View channel {c.Id}" class="text-gray-400 hover:text-red-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteChannel(c)} title="Delete" aria-label="Delete channel {c.Id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const channelColumns = defineColumns<ChannelSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Id', label: 'ID' },
					{ key: 'ChannelClass', label: 'Class' },
					{ key: 'State', label: 'State', render: channelStateCell },
					{ key: 'actions', label: '', render: channelActionsCell }
				])}
				<DataTable
					rows={filteredChannels}
					rowKey={(c) => c.Id ?? ''}
					columns={channelColumns}
					loading={tabLoader.isLoading('channels')}
					emptyMessage="No channels found"
				/>
			{:else if activeTab === 'inputs'}
				{#snippet inputStateCell(i: Input)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(i.State)}">{i.State ?? '—'}</span>
				{/snippet}
				{#snippet inputActionsCell(i: Input)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openInputDetail(i)} title="View" aria-label="View input {i.Id}" class="text-gray-400 hover:text-red-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteInput(i)} title="Delete" aria-label="Delete input {i.Id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const inputColumns = defineColumns<Input>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Id', label: 'ID' },
					{ key: 'Type', label: 'Type' },
					{ key: 'State', label: 'State', render: inputStateCell },
					{ key: 'actions', label: '', render: inputActionsCell }
				])}
				<DataTable
					rows={filteredInputs}
					rowKey={(i) => i.Id ?? ''}
					columns={inputColumns}
					loading={tabLoader.isLoading('inputs')}
					emptyMessage="No inputs found"
				/>
			{:else if activeTab === 'inputsg'}
				{#snippet inputSgStateCell(g: InputSecurityGroup)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(g.State)}">{g.State ?? '—'}</span>
				{/snippet}
				{#snippet inputSgActionsCell(g: InputSecurityGroup)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openInputSgDetail(g)} title="View" aria-label="View input security group {g.Id}" class="text-gray-400 hover:text-red-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteInputSg(g)} title="Delete" aria-label="Delete input security group {g.Id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const inputSgColumns = defineColumns<InputSecurityGroup>([
					{ key: 'Id', label: 'ID' },
					{ key: 'Arn', label: 'ARN' },
					{ key: 'State', label: 'State', render: inputSgStateCell },
					{ key: 'actions', label: '', render: inputSgActionsCell }
				])}
				<DataTable
					rows={filteredInputSgs}
					rowKey={(g) => g.Id ?? ''}
					columns={inputSgColumns}
					loading={tabLoader.isLoading('inputsg')}
					emptyMessage="No input security groups found"
				/>
			{:else if activeTab === 'multiplexes'}
				{#snippet multiplexStateCell(m: MultiplexSummary)}
					<span class="text-xs px-2 py-1 rounded-full {statusClass(m.State)}">{m.State ?? '—'}</span>
				{/snippet}
				{#snippet multiplexActionsCell(m: MultiplexSummary)}
					<div class="flex items-center gap-2 justify-end">
						<button onclick={() => openMultiplexDetail(m)} title="View" aria-label="View multiplex {m.Id}" class="text-gray-400 hover:text-red-500"><Eye class="w-4 h-4" /></button>
						<button onclick={() => deleteMultiplex(m)} title="Delete" aria-label="Delete multiplex {m.Id}" class="text-gray-400 hover:text-red-500"><Trash2 class="w-4 h-4" /></button>
					</div>
				{/snippet}
				{@const multiplexColumns = defineColumns<MultiplexSummary>([
					{ key: 'Name', label: 'Name' },
					{ key: 'Id', label: 'ID' },
					{ key: 'State', label: 'State', render: multiplexStateCell },
					{ key: 'actions', label: '', render: multiplexActionsCell }
				])}
				<DataTable
					rows={filteredMultiplexes}
					rowKey={(m) => m.Id ?? ''}
					columns={multiplexColumns}
					loading={tabLoader.isLoading('multiplexes')}
					emptyMessage="No multiplexes found"
				/>
			{/if}
		</div>
	</div>
</div>

<!-- Create Channel -->
<Modal bind:this={createChannelModal} title="Create Channel">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ch-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ch-name" bind:value={newChannelName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ch-class" class="text-sm text-slate-600 dark:text-slate-300">Channel class</label>
				<select id="ch-class" bind:value={newChannelClass} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					<option value={ChannelClass.STANDARD}>STANDARD</option>
					<option value={ChannelClass.SINGLE_PIPELINE}>SINGLE_PIPELINE</option>
				</select>
			</div>
			<div>
				<label for="ch-role-arn" class="text-sm text-slate-600 dark:text-slate-300">Role ARN</label>
				<input id="ch-role-arn" bind:value={newChannelRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ch-encoder" class="text-sm text-slate-600 dark:text-slate-300">Encoder settings (JSON, optional)</label>
				<textarea id="ch-encoder" bind:value={newChannelEncoderSettingsJson} rows={4} class="mt-1 w-full px-3 py-2 text-xs font-mono rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white"></textarea>
			</div>
			{#if createChannelError}
				<p class="text-sm text-red-600 dark:text-red-400">{createChannelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createChannelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateChannel} disabled={creatingChannel} class="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{creatingChannel ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Channel detail -->
<Modal bind:this={channelDetailModal} title="Channel">
	{#snippet children()}
		{#if channelDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if channelDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{channelDetailError}</p>
		{:else if viewedChannel}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedChannel.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Class</dt><dd class="text-slate-900 dark:text-white">{viewedChannel.ChannelClass ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Role ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedChannel.RoleArn ?? '—'}</dd></div>
				{#if channelActionError}
					<p class="text-sm text-red-600 dark:text-red-400">{channelActionError}</p>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => channelDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedChannel}
			{#if viewedChannel.State === 'IDLE'}
				<button type="button" onclick={startChannel} class="flex items-center gap-2 rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white hover:bg-green-700"><Play class="w-4 h-4" /> Start</button>
			{:else if viewedChannel.State === 'RUNNING'}
				<button type="button" onclick={stopChannel} class="flex items-center gap-2 rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700"><Square class="w-4 h-4" /> Stop</button>
			{/if}
			<button type="button" onclick={() => viewedChannel && openEditChannelModal(viewedChannel)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedChannel && deleteChannel(viewedChannel)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Channel -->
<Modal bind:this={editChannelModal} title="Edit Channel">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="ch-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="ch-edit-name" bind:value={editChannelName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="ch-edit-role-arn" class="text-sm text-slate-600 dark:text-slate-300">Role ARN</label>
				<input id="ch-edit-role-arn" bind:value={editChannelRoleArn} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editChannelError}
				<p class="text-sm text-red-600 dark:text-red-400">{editChannelError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editChannelModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditChannel} disabled={editingChannel} class="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{editingChannel ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Input -->
<Modal bind:this={createInputModal} title="Create Input">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="in-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="in-name" bind:value={newInputName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="in-type" class="text-sm text-slate-600 dark:text-slate-300">Type</label>
				<select id="in-type" bind:value={newInputType} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white">
					{#each Object.values(InputType) as t (t)}
						<option value={t}>{t}</option>
					{/each}
				</select>
			</div>
			<div>
				<label for="in-source" class="text-sm text-slate-600 dark:text-slate-300">
					{newInputType === 'URL_PULL' || newInputType === 'RTMP_PULL' ? 'Source URL' : 'Destination stream name'}
				</label>
				<input id="in-source" bind:value={newInputSourceUrl} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="in-sg" class="text-sm text-slate-600 dark:text-slate-300">Input security group IDs (comma-separated)</label>
				<input id="in-sg" bind:value={newInputSecurityGroups} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createInputError}
				<p class="text-sm text-red-600 dark:text-red-400">{createInputError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createInputModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateInput} disabled={creatingInput} class="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{creatingInput ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Input detail -->
<Modal bind:this={inputDetailModal} title="Input">
	{#snippet children()}
		{#if inputDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if inputDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{inputDetailError}</p>
		{:else if viewedInput}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedInput.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedInput.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Type</dt><dd class="text-slate-900 dark:text-white">{viewedInput.Type ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedInput.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Security groups</dt><dd class="text-slate-900 dark:text-white">{(viewedInput.SecurityGroups ?? []).join(', ') || '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => inputDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedInput}
			<button type="button" onclick={() => viewedInput && openEditInputModal(viewedInput)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedInput && deleteInput(viewedInput)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Input -->
<Modal bind:this={editInputModal} title="Edit Input">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="in-edit-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="in-edit-name" bind:value={editInputName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="in-edit-sg" class="text-sm text-slate-600 dark:text-slate-300">Input security group IDs (comma-separated)</label>
				<input id="in-edit-sg" bind:value={editInputSecurityGroups} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editInputError}
				<p class="text-sm text-red-600 dark:text-red-400">{editInputError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editInputModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditInput} disabled={editingInput} class="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{editingInput ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Input Security Group -->
<Modal bind:this={createInputSgModal} title="Create Input Security Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="sg-cidrs" class="text-sm text-slate-600 dark:text-slate-300">Whitelisted CIDRs (comma-separated)</label>
				<input id="sg-cidrs" bind:value={newInputSgCidrs} placeholder="10.0.0.0/24, 192.168.1.0/24" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if createInputSgError}
				<p class="text-sm text-red-600 dark:text-red-400">{createInputSgError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createInputSgModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateInputSg} disabled={creatingInputSg} class="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{creatingInputSg ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Input Security Group detail -->
<Modal bind:this={inputSgDetailModal} title="Input Security Group">
	{#snippet children()}
		{#if inputSgDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if inputSgDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{inputSgDetailError}</p>
		{:else if viewedInputSg}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedInputSg.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ARN</dt><dd class="text-slate-900 dark:text-white break-all">{viewedInputSg.Arn ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedInputSg.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Whitelisted CIDRs</dt><dd class="text-slate-900 dark:text-white">{(viewedInputSg.WhitelistRules ?? []).map((r) => r.Cidr).join(', ') || '—'}</dd></div>
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => inputSgDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedInputSg}
			<button type="button" onclick={() => viewedInputSg && openEditInputSgModal(viewedInputSg)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedInputSg && deleteInputSg(viewedInputSg)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>

<!-- Edit Input Security Group -->
<Modal bind:this={editInputSgModal} title="Edit Input Security Group">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="sg-edit-cidrs" class="text-sm text-slate-600 dark:text-slate-300">Whitelisted CIDRs (comma-separated)</label>
				<input id="sg-edit-cidrs" bind:value={editInputSgCidrs} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			{#if editInputSgError}
				<p class="text-sm text-red-600 dark:text-red-400">{editInputSgError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => editInputSgModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitEditInputSg} disabled={editingInputSg} class="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{editingInputSg ? 'Saving…' : 'Save'}</button>
	{/snippet}
</Modal>

<!-- Create Multiplex -->
<Modal bind:this={createMultiplexModal} title="Create Multiplex">
	{#snippet children()}
		<div class="space-y-3">
			<div>
				<label for="mx-name" class="text-sm text-slate-600 dark:text-slate-300">Name</label>
				<input id="mx-name" bind:value={newMultiplexName} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div>
				<label for="mx-azs" class="text-sm text-slate-600 dark:text-slate-300">Availability zones (exactly two, comma-separated)</label>
				<input id="mx-azs" bind:value={newMultiplexAzs} placeholder="us-east-1a, us-east-1b" class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
			</div>
			<div class="grid grid-cols-2 gap-2">
				<div>
					<label for="mx-bitrate" class="text-sm text-slate-600 dark:text-slate-300">Transport stream bitrate</label>
					<input id="mx-bitrate" type="number" bind:value={newMultiplexBitrate} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
				<div>
					<label for="mx-tsid" class="text-sm text-slate-600 dark:text-slate-300">Transport stream ID</label>
					<input id="mx-tsid" type="number" bind:value={newMultiplexTsId} class="mt-1 w-full px-3 py-2 text-sm rounded-lg border border-gray-200 dark:border-gray-600 bg-white dark:bg-slate-700 text-gray-900 dark:text-white" />
				</div>
			</div>
			{#if createMultiplexError}
				<p class="text-sm text-red-600 dark:text-red-400">{createMultiplexError}</p>
			{/if}
		</div>
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => createMultiplexModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Cancel</button>
		<button type="button" onclick={submitCreateMultiplex} disabled={creatingMultiplex} class="rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700 disabled:opacity-50">{creatingMultiplex ? 'Creating…' : 'Create'}</button>
	{/snippet}
</Modal>

<!-- Multiplex detail -->
<Modal bind:this={multiplexDetailModal} title="Multiplex">
	{#snippet children()}
		{#if multiplexDetailLoading}
			<p class="text-sm text-slate-500 dark:text-slate-400">Loading…</p>
		{:else if multiplexDetailError}
			<p class="text-sm text-red-600 dark:text-red-400">{multiplexDetailError}</p>
		{:else if viewedMultiplex}
			<dl class="text-sm space-y-2">
				<div><dt class="text-slate-500 dark:text-slate-400">Name</dt><dd class="text-slate-900 dark:text-white">{viewedMultiplex.Name ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">ID</dt><dd class="text-slate-900 dark:text-white">{viewedMultiplex.Id ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">State</dt><dd class="text-slate-900 dark:text-white">{viewedMultiplex.State ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Availability zones</dt><dd class="text-slate-900 dark:text-white">{(viewedMultiplex.AvailabilityZones ?? []).join(', ') || '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Transport stream bitrate</dt><dd class="text-slate-900 dark:text-white">{viewedMultiplex.MultiplexSettings?.TransportStreamBitrate ?? '—'}</dd></div>
				<div><dt class="text-slate-500 dark:text-slate-400">Transport stream ID</dt><dd class="text-slate-900 dark:text-white">{viewedMultiplex.MultiplexSettings?.TransportStreamId ?? '—'}</dd></div>
				{#if multiplexActionError}
					<p class="text-sm text-red-600 dark:text-red-400">{multiplexActionError}</p>
				{/if}
			</dl>
		{/if}
	{/snippet}
	{#snippet footer()}
		<button type="button" onclick={() => multiplexDetailModal?.close()} class="rounded-lg border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-600 dark:text-slate-200 dark:hover:bg-slate-800">Close</button>
		{#if viewedMultiplex}
			{#if viewedMultiplex.State === 'IDLE'}
				<button type="button" onclick={startMultiplex} class="flex items-center gap-2 rounded-lg bg-green-600 px-4 py-2 text-sm font-semibold text-white hover:bg-green-700"><Play class="w-4 h-4" /> Start</button>
			{:else if viewedMultiplex.State === 'RUNNING'}
				<button type="button" onclick={stopMultiplex} class="flex items-center gap-2 rounded-lg bg-amber-600 px-4 py-2 text-sm font-semibold text-white hover:bg-amber-700"><Square class="w-4 h-4" /> Stop</button>
			{/if}
			<button type="button" onclick={() => viewedMultiplex && openEditMultiplexModal(viewedMultiplex)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Pencil class="w-4 h-4" /> Edit</button>
			<button type="button" onclick={() => viewedMultiplex && deleteMultiplex(viewedMultiplex)} class="flex items-center gap-2 rounded-lg bg-red-600 px-4 py-2 text-sm font-semibold text-white hover:bg-red-700"><Trash2 class="w-4 h-4" /> Delete</button>
		{/if}
	{/snippet}
</Modal>
